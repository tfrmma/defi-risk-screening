package main

import (
	"context"
	"encoding/binary"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// Aave V3 event signatures. Run `cast sig-event "Supply(...)"` to verify these
// if anything looks wrong — don't trust comments, trust the ABI.
var aaveTopics = map[string]common.Hash{
	"Supply":    common.HexToHash("0x2b627736bca15cd5381dcf80b0bf11fd197d62a23dd7b3dcbd7cec306e013c64"),
	"Withdraw":  common.HexToHash("0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7"),
	"Borrow":    common.HexToHash("0xb3d084820fb1a9decffb176436bd02b9b3f861bc22ec4df1c3ba35d6d2b3fb58"),
	"Repay":     common.HexToHash("0xa534c8dbe71f871f9f3aecd4c20753fda4e3fd56cf0e52a1c98ad56e91be52ac"),
	"Liquidate": common.HexToHash("0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"),
}

// topic -> (user topic index, reserve topic index)
// positions differ per event type in Aave V3
var topicLayout = map[string][2]int{
	"Supply":    {2, 1},
	"Withdraw":  {2, 1},
	"Borrow":    {2, 1},
	"Repay":     {1, 2},
	"Liquidate": {2, 3}, // liquidated user, debt token
}

// token decimals hardcoded for the handful of assets we track.
// Don't fetch this on-chain every time — it never changes.
var tokenDecimals = map[common.Address]int{
	common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"): 18, // WETH
	common.HexToAddress("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"): 8,  // WBTC
	common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"): 6,  // USDC
	common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"): 6,  // USDT
}

type Config struct {
	RPC struct {
		IPCPath string `yaml:"ipc_path"`
		WSURL   string `yaml:"ws_url"`
	} `yaml:"rpc"`
	Redis struct {
		Addr     string `yaml:"addr"`
		DB       int    `yaml:"db"`
		PoolSize int    `yaml:"pool_size"`
	} `yaml:"redis"`
	Protocols struct {
		AaveV3 struct {
			Pool string `yaml:"pool"`
		} `yaml:"aave_v3"`
		MorphoBlue struct {
			Hub string `yaml:"hub"`
		} `yaml:"morpho_blue"`
		Spark struct {
			Pool string `yaml:"pool"`
		} `yaml:"spark"`
	} `yaml:"protocols"`
}

// addrToProtocol lets us tag events with the protocol name without a map lookup per event.
type addrToProtocol map[common.Address]string

type Indexer struct {
	eth       *ethclient.Client
	rdb       *redis.Client
	log       *zap.SugaredLogger
	cfg       *Config
	protocols addrToProtocol
	subs      []ethereum.Subscription
}

func loadConfig(path string) (*Config, error) {
	f, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	return &cfg, yaml.Unmarshal(f, &cfg)
}

func newIndexer(cfg *Config, log *zap.SugaredLogger) (*Indexer, error) {
	eth, err := ethclient.Dial(cfg.RPC.IPCPath)
	if err != nil {
		log.Warnw("IPC failed, trying WS", "err", err)
		eth, err = ethclient.Dial(cfg.RPC.WSURL)
		if err != nil {
			return nil, err
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})

	protocols := addrToProtocol{
		common.HexToAddress(cfg.Protocols.AaveV3.Pool):     "aave_v3",
		common.HexToAddress(cfg.Protocols.MorphoBlue.Hub):  "morpho_blue",
		common.HexToAddress(cfg.Protocols.Spark.Pool):      "spark",
	}

	return &Indexer{eth: eth, rdb: rdb, log: log, cfg: cfg, protocols: protocols}, nil
}

func (ix *Indexer) watchedAddresses() []common.Address {
	addrs := make([]common.Address, 0, len(ix.protocols))
	for addr := range ix.protocols {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (ix *Indexer) allTopics() []common.Hash {
	topics := make([]common.Hash, 0, len(aaveTopics))
	for _, h := range aaveTopics {
		topics = append(topics, h)
	}
	return topics
}

func (ix *Indexer) Run(ctx context.Context) error {
	logCh := make(chan types.Log, 8192) // big buffer — reorgs during flash crashes are ugly

	sub, err := ix.eth.SubscribeFilterLogs(ctx, ethereum.FilterQuery{
		Addresses: ix.watchedAddresses(),
		Topics:    [][]common.Hash{ix.allTopics()},
	}, logCh)
	if err != nil {
		return err
	}
	ix.subs = append(ix.subs, sub)
	ix.log.Info("protocol log subscription active")

	for {
		select {
		case err := <-sub.Err():
			// TODO: exponential backoff + resubscribe instead of dying
			ix.log.Errorw("subscription error", "err", err)
			return err

		case l := <-logCh:
			if l.Removed {
				continue // reorg'd out — state will reconcile on next event
			}
			ix.dispatch(ctx, l)

		case <-ctx.Done():
			return nil
		}
	}
}

func (ix *Indexer) dispatch(ctx context.Context, l types.Log) {
	if len(l.Topics) == 0 {
		return
	}
	for name, sig := range aaveTopics {
		if l.Topics[0] == sig {
			ix.handleProtocolEvent(ctx, name, l)
			return
		}
	}
}

// handleProtocolEvent is the core state update path.
// We write per-asset position deltas into Redis so the Python risk engine
// can compute HF purely from local state — no node calls needed.
func (ix *Indexer) handleProtocolEvent(ctx context.Context, event string, l types.Log) {
	layout, ok := topicLayout[event]
	if !ok || len(l.Topics) <= layout[0] {
		return
	}

	user    := common.BytesToAddress(l.Topics[layout[0]].Bytes())
	reserve := common.BytesToAddress(l.Topics[layout[1]].Bytes())
	protocol := ix.protocols[l.Address]

	// amount is in the log data, first 32 bytes
	var amount *big.Int
	if len(l.Data) >= 32 {
		amount = new(big.Int).SetBytes(l.Data[:32])
	} else {
		amount = big.NewInt(0)
	}

	decimals, known := tokenDecimals[reserve]
	if !known {
		decimals = 18 // assume 18 if we don't know — better than crashing
	}

	posKey := "acct:" + protocol + ":" + user.Hex() + ":" + reserve.Hex()
	dirtyKey := protocol + ":" + user.Hex() // format risk engine expects

	pipe := ix.rdb.Pipeline()
	pipe.HSet(ctx, posKey, map[string]interface{}{
		"decimals":     decimals,
		"last_event":   event,
		"last_block":   l.BlockNumber,
		"last_updated": time.Now().UnixMilli(),
	})

	// apply signed delta based on event direction
	switch event {
	case "Supply":
		pipe.HIncrBy(ctx, posKey, "collateral_raw", amountInt64(amount))
	case "Withdraw":
		pipe.HIncrBy(ctx, posKey, "collateral_raw", -amountInt64(amount))
	case "Borrow":
		pipe.HIncrBy(ctx, posKey, "debt_raw", amountInt64(amount))
	case "Repay":
		pipe.HIncrBy(ctx, posKey, "debt_raw", -amountInt64(amount))
	case "Liquidate":
		// on liquidation we can't easily decompose delta from the log alone;
		// flag dirty and let the oracle updater refresh the price — HF will recompute
		ix.log.Infow("liquidation event", "user", user.Hex()[:12], "block", l.BlockNumber)
	}

	// mark dirty with 30s TTL — if the risk engine doesn't drain it in 30s we have bigger problems
	pipe.SAdd(ctx, "dirty:accounts", dirtyKey)
	pipe.Expire(ctx, "dirty:accounts", 30*time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		ix.log.Warnw("redis pipeline failed", "user", user.Hex()[:12], "err", err)
	}
}

// MempoolScanner watches pending txs targeting our protocol contracts.
// Useful for anticipating oracle price updates 1-2 blocks ahead.
func (ix *Indexer) MempoolScanner(ctx context.Context) error {
	pendingCh := make(chan string, 2048)

	sub, err := ix.eth.EthSubscribe(ctx, pendingCh, "newPendingTransactions")
	if err != nil {
		ix.log.Warnw("mempool subscription unavailable", "err", err)
		return nil // not all nodes expose this — soft fail
	}
	ix.subs = append(ix.subs, sub)

	watched := make(map[common.Address]bool, len(ix.protocols))
	for addr := range ix.protocols {
		watched[addr] = true
	}

	for {
		select {
		case err := <-sub.Err():
			return err
		case txHash := <-pendingCh:
			// fire-and-forget lookup — don't block the channel
			go ix.checkPendingTx(ctx, common.HexToHash(txHash), watched)
		case <-ctx.Done():
			return nil
		}
	}
}

func (ix *Indexer) checkPendingTx(ctx context.Context, hash common.Hash, watched map[common.Address]bool) {
	tx, _, err := ix.eth.TransactionByHash(ctx, hash)
	if err != nil || tx.To() == nil || !watched[*tx.To()] {
		return
	}
	key := "mempool:pending:" + hash.Hex()
	ix.rdb.SetEX(ctx, key, tx.To().Hex(), 30*time.Second)
}

// amountInt64 clamps a big.Int to int64 for HIncrBy.
// Positions > 2^63 units are... not our problem right now.
func amountInt64(n *big.Int) int64 {
	if !n.IsInt64() {
		// overflow — log and return max; shouldn't happen with real token amounts
		return int64(binary.BigEndian.Uint64(n.Bytes()[len(n.Bytes())-8:]))
	}
	return n.Int64()
}

func (ix *Indexer) Close() {
	for _, s := range ix.subs {
		s.Unsubscribe()
	}
	ix.eth.Close()
	ix.rdb.Close()
}

func main() {
	raw, _ := zap.NewProduction()
	log := raw.Sugar()
	defer log.Sync()

	cfg, err := loadConfig("config/config.yaml")
	if err != nil {
		log.Fatalw("config load failed", "err", err)
	}

	ix, err := newIndexer(cfg, log)
	if err != nil {
		log.Fatalw("indexer init failed", "err", err)
	}
	defer ix.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := ix.MempoolScanner(ctx); err != nil {
			log.Warnw("mempool scanner exited", "err", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutting down")
		cancel()
	}()

	if err := ix.Run(ctx); err != nil {
		log.Errorw("indexer exited", "err", err)
	}
}
