package main

import (
	"context"
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

// protocol event signatures — don't hardcode these inline, it's a nightmare to debug
var aaveTopics = map[string]common.Hash{
	"Supply":    common.HexToHash("0x2b627736bca15cd5381dcf80b0bf11fd197d62a23dd7b3dcbd7cec306e013c64"),
	"Withdraw":  common.HexToHash("0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7"),
	"Borrow":    common.HexToHash("0xb3d084820fb1a9decffb176436bd02b9b3f861bc22ec4df1c3ba35d6d2b3fb58"),
	"Repay":     common.HexToHash("0xa534c8dbe71f871f9f3aecd4c20753fda4e3fd56cf0e52a1c98ad56e91be52ac"),
	"Liquidate": common.HexToHash("0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"),
}

type Config struct {
	RPC struct {
		IPCPATH string `yaml:"ipc_path"`
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

type Indexer struct {
	eth    *ethclient.Client
	rdb    *redis.Client
	log    *zap.SugaredLogger
	cfg    *Config
	subs   []ethereum.Subscription
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
	// prefer IPC when running on same host — ws adds ~200µs of garbage
	ethClient, err := ethclient.Dial(cfg.RPC.IPCPATH)
	if err != nil {
		log.Warnw("IPC dial failed, falling back to ws", "err", err)
		ethClient, err = ethclient.Dial(cfg.RPC.WSURL)
		if err != nil {
			return nil, err
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})

	return &Indexer{eth: ethClient, rdb: rdb, log: log, cfg: cfg}, nil
}

func (ix *Indexer) watchedAddresses() []common.Address {
	return []common.Address{
		common.HexToAddress(ix.cfg.Protocols.AaveV3.Pool),
		common.HexToAddress(ix.cfg.Protocols.MorphoBlue.Hub),
		common.HexToAddress(ix.cfg.Protocols.Spark.Pool),
	}
}

func (ix *Indexer) buildFilter() ethereum.FilterQuery {
	topics := make([]common.Hash, 0, len(aaveTopics))
	for _, h := range aaveTopics {
		topics = append(topics, h)
	}
	return ethereum.FilterQuery{
		Addresses: ix.watchedAddresses(),
		Topics:    [][]common.Hash{topics},
	}
}

func (ix *Indexer) Run(ctx context.Context) error {
	logCh := make(chan types.Log, 4096) // deep buffer — reorgs during high vol are brutal

	sub, err := ix.eth.SubscribeFilterLogs(ctx, ix.buildFilter(), logCh)
	if err != nil {
		return err
	}
	ix.subs = append(ix.subs, sub)
	ix.log.Info("subscribed to protocol logs")

	for {
		select {
		case err := <-sub.Err():
			// TODO: add exponential backoff resubscribe instead of dying here
			ix.log.Errorw("subscription dropped", "err", err)
			return err

		case log := <-logCh:
			if log.Removed {
				continue // reorg — ignore, state will reconcile
			}
			ix.dispatch(ctx, log)

		case <-ctx.Done():
			return nil
		}
	}
}

func (ix *Indexer) dispatch(ctx context.Context, log types.Log) {
	if len(log.Topics) == 0 {
		return
	}

	for name, sig := range aaveTopics {
		if log.Topics[0] == sig {
			ix.handleProtocolEvent(ctx, name, log)
			return
		}
	}
}

// handleProtocolEvent pushes state deltas to Redis. The structure is flat by design —
// no nested hashes, just raw keys. Fast to read, easy to pipeline.
func (ix *Indexer) handleProtocolEvent(ctx context.Context, eventName string, log types.Log) {
	if len(log.Topics) < 2 {
		return
	}

	user := common.BytesToAddress(log.Topics[1].Bytes())
	pipe := ix.rdb.Pipeline()

	pipe.HSet(ctx, "account:"+user.Hex(), map[string]interface{}{
		"last_event":    eventName,
		"last_block":    log.BlockNumber,
		"last_tx":       log.TxHash.Hex(),
		"last_updated":  time.Now().UnixMilli(),
	})

	// mark as dirty — risk engine will recalc HF on next tick
	pipe.SAdd(ctx, "dirty:accounts", user.Hex())

	if _, err := pipe.Exec(ctx); err != nil {
		ix.log.Warnw("redis pipeline failed", "user", user.Hex(), "err", err)
	}

	if eventName == "Liquidate" {
		ix.log.Infow("liquidation detected", "user", user.Hex(), "block", log.BlockNumber)
	}
}

// MempoolScanner watches pending txs for front-running ops against our target contracts.
// Rough, but useful for anticipating oracle updates ~1-2 blocks ahead.
func (ix *Indexer) MempoolScanner(ctx context.Context) error {
	pendingCh := make(chan *types.Transaction, 2048)

	sub, err := ix.eth.SubscribePendingTransactions(ctx, pendingCh)
	if err != nil {
		ix.log.Warnw("mempool subscription not supported on this node", "err", err)
		return nil // soft fail — not all nodes expose this
	}
	ix.subs = append(ix.subs, sub)

	watched := make(map[common.Address]bool)
	for _, addr := range ix.watchedAddresses() {
		watched[addr] = true
	}

	for {
		select {
		case err := <-sub.Err():
			return err
		case tx := <-pendingCh:
			if tx.To() != nil && watched[*tx.To()] {
				ix.flagMempoolHit(ctx, tx)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (ix *Indexer) flagMempoolHit(ctx context.Context, tx *types.Transaction) {
	if tx.Value().Cmp(big.NewInt(0)) == 0 && len(tx.Data()) < 4 {
		return
	}
	// shove it in redis with a short TTL — risk engine reads this
	key := "mempool:pending:" + tx.Hash().Hex()
	ix.rdb.SetEx(ctx, key, tx.To().Hex(), 30*time.Second)
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
		log.Errorw("indexer stopped", "err", err)
	}
}
