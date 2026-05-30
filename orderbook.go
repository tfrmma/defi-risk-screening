package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Level is a single price level in an order book.
type Level struct {
	Price float64
	Size  float64
}

// Book is a local L2 order book. Lock it before touching bids/asks.
type Book struct {
	mu       sync.RWMutex
	Exchange string
	Symbol   string
	Bids     map[float64]float64 // price -> qty
	Asks     map[float64]float64
	UpdatedAt time.Time
}

func newBook(exchange, symbol string) *Book {
	return &Book{
		Exchange: exchange,
		Symbol:   symbol,
		Bids:     make(map[float64]float64, 512),
		Asks:     make(map[float64]float64, 512),
	}
}

func (b *Book) applyDelta(side string, price, qty float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.Bids
	if side == "ask" {
		m = b.Asks
	}
	if qty == 0 {
		delete(m, price)
	} else {
		m[price] = qty
	}
	b.UpdatedAt = time.Now()
}

// MarketImpact estimates price impact for a given notional (USD).
// This is the core function — called by the cascade sim before every position decision.
func (b *Book) MarketImpact(side string, notionalUSD float64) (avgPrice, slippage float64) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var levels []Level
	if side == "sell" {
		for p, q := range b.Bids {
			levels = append(levels, Level{p, q})
		}
		sort.Slice(levels, func(i, j int) bool { return levels[i].Price > levels[j].Price })
	} else {
		for p, q := range b.Asks {
			levels = append(levels, Level{p, q})
		}
		sort.Slice(levels, func(i, j int) bool { return levels[i].Price < levels[j].Price })
	}

	if len(levels) == 0 {
		return 0, 1.0
	}

	topPrice := levels[0].Price
	remaining := notionalUSD
	totalQty := 0.0
	totalCost := 0.0

	for _, lvl := range levels {
		fillQty := math.Min(remaining/lvl.Price, lvl.Size)
		totalQty += fillQty
		totalCost += fillQty * lvl.Price
		remaining -= fillQty * lvl.Price
		if remaining <= 0 {
			break
		}
	}

	if totalQty == 0 {
		return 0, 1.0
	}

	avgPrice = totalCost / totalQty
	if side == "sell" {
		slippage = (topPrice - avgPrice) / topPrice
	} else {
		slippage = (avgPrice - topPrice) / topPrice
	}
	return avgPrice, slippage
}

// BinanceHandler feeds the ETH/USDT book from Binance depth stream.
type BinanceHandler struct {
	book *Book
	log  *zap.SugaredLogger
}

func (h *BinanceHandler) Connect(ctx context.Context) error {
	url := "wss://stream.binance.com:9443/ws/ethusdt@depth@100ms"
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("binance ws dial: %w", err)
	}
	defer conn.Close()

	h.log.Info("binance book feed connected")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("binance read: %w", err)
		}
		h.handleDepthMsg(msg)
	}
}

type binanceDepthMsg struct {
	Bids [][]json.RawMessage `json:"b"`
	Asks [][]json.RawMessage `json:"a"`
}

func (h *BinanceHandler) handleDepthMsg(raw []byte) {
	var msg binanceDepthMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	for _, lvl := range msg.Bids {
		p, q := parseLevel(lvl)
		h.book.applyDelta("bid", p, q)
	}
	for _, lvl := range msg.Asks {
		p, q := parseLevel(lvl)
		h.book.applyDelta("ask", p, q)
	}
}

// BybitHandler feeds the same symbol from Bybit for cross-venue slippage estimation.
type BybitHandler struct {
	book *Book
	log  *zap.SugaredLogger
}

func (h *BybitHandler) Connect(ctx context.Context) error {
	url := "wss://stream.bybit.com/v5/public/linear"
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("bybit ws dial: %w", err)
	}
	defer conn.Close()

	// subscribe to orderbook.50
	sub := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{"orderbook.50.ETHUSDT"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("bybit subscribe: %w", err)
	}

	h.log.Info("bybit book feed connected")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("bybit read: %w", err)
		}
		h.handleMsg(msg)
	}
}

type bybitBookMsg struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  struct {
		Bids [][]string `json:"b"`
		Asks [][]string `json:"a"`
	} `json:"data"`
}

func (h *BybitHandler) handleMsg(raw []byte) {
	var msg bybitBookMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.Topic == "" {
		return // ping/pong or sub confirmation
	}
	for _, lvl := range msg.Data.Bids {
		if len(lvl) != 2 {
			continue
		}
		var p, q float64
		fmt.Sscanf(lvl[0], "%f", &p)
		fmt.Sscanf(lvl[1], "%f", &q)
		h.book.applyDelta("bid", p, q)
	}
	for _, lvl := range msg.Data.Asks {
		if len(lvl) != 2 {
			continue
		}
		var p, q float64
		fmt.Sscanf(lvl[0], "%f", &p)
		fmt.Sscanf(lvl[1], "%f", &q)
		h.book.applyDelta("ask", p, q)
	}
}

// AggregatedBook merges multiple venue books for cross-exchange impact.
// Simple approach: sweep the combined level set in price order.
type AggregatedBook struct {
	books []*Book
}

func newAggregatedBook(books ...*Book) *AggregatedBook {
	return &AggregatedBook{books: books}
}

func (ab *AggregatedBook) SlippageForNotional(side string, notionalUSD float64) map[string]float64 {
	result := make(map[string]float64, len(ab.books)+1)

	// per-venue slippage
	for _, b := range ab.books {
		_, slip := b.MarketImpact(side, notionalUSD)
		result[b.Exchange] = slip
	}

	// combined — just average for now
	// TODO: proper NBBO sweep across merged level sets
	var total float64
	for _, v := range result {
		total += v
	}
	if len(ab.books) > 0 {
		result["combined"] = total / float64(len(ab.books))
	}
	return result
}

func parseLevel(raw []json.RawMessage) (float64, float64) {
	if len(raw) < 2 {
		return 0, 0
	}
	var ps, qs string
	json.Unmarshal(raw[0], &ps)
	json.Unmarshal(raw[1], &qs)
	var p, q float64
	fmt.Sscanf(ps, "%f", &p)
	fmt.Sscanf(qs, "%f", &q)
	return p, q
}

// SlippageServer runs in the background and serves slippage queries over ZMQ.
// Risk engine and frontend both hit this.
type SlippageServer struct {
	agg *AggregatedBook
	log *zap.SugaredLogger
}

func (s *SlippageServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: replace with gRPC once the proto is settled
	notional := float64(10_000_000) // default $10M
	slippage := s.agg.SlippageForNotional("sell", notional)
	json.NewEncoder(w).Encode(slippage)
}

func main() {
	raw, _ := zap.NewProduction()
	log := raw.Sugar()
	defer log.Sync()

	binanceBook := newBook("binance", "ETH/USDT")
	bybitBook   := newBook("bybit", "ETH/USDT")
	agg         := newAggregatedBook(binanceBook, bybitBook)

	binanceH := &BinanceHandler{book: binanceBook, log: log}
	bybitH   := &BybitHandler{book: bybitBook, log: log}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			if err := binanceH.Connect(ctx); err != nil && ctx.Err() == nil {
				log.Warnw("binance reconnecting", "err", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	go func() {
		for {
			if err := bybitH.Connect(ctx); err != nil && ctx.Err() == nil {
				log.Warnw("bybit reconnecting", "err", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	// periodic slippage log — useful for sanity checks
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				for _, notional := range []float64{1e6, 5e6, 10e6} {
					slips := agg.SlippageForNotional("sell", notional)
					log.Infow("slippage snapshot",
						"notional", notional,
						"slippage", slips,
					)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	_ = agg // used by slippage server above — compiler stop complaining

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Info("shutting down aggregator")
}
