package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/btree"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// priceTick is the btree item. floats as keys are fine here — we control
// the precision coming from the exchange (they send strings, we parse once).
type priceTick struct {
	price float64
	qty   float64
}

func (a priceTick) Less(b btree.Item) bool {
	return a.price < b.(*priceTick).price
}

// sideBook is one side of the order book backed by a B-Tree.
// Writes are O(log n), reads sweep O(k) — no sort on the hot path.
type sideBook struct {
	mu   sync.RWMutex
	tree *btree.BTreeG[*priceTick]
	asc  bool // true = asks (price asc), false = bids (price desc)
}

func newSideBook(asc bool) *sideBook {
	cmp := func(a, b *priceTick) bool {
		if asc {
			return a.price < b.price
		}
		return a.price > b.price
	}
	return &sideBook{tree: btree.NewG(32, cmp), asc: asc}
}

func (s *sideBook) update(price, qty float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if qty == 0 {
		s.tree.Delete(&priceTick{price: price})
		return
	}
	// ReplaceOrInsert handles both new levels and qty updates
	s.tree.ReplaceOrInsert(&priceTick{price: price, qty: qty})
}

// sweep walks the book from best price and returns (avgFill, slippage).
// Called under RLock — no allocation beyond the loop stack.
func (s *sideBook) sweep(notionalUSD float64) (avgPrice, slippage float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.tree.Len() == 0 {
		return 0, 1.0
	}

	var topPrice float64
	first := true
	remaining := notionalUSD
	totalQty := 0.0
	totalCost := 0.0

	s.tree.Ascend(func(tick *priceTick) bool {
		if first {
			topPrice = tick.price
			first = false
		}
		fillQty := math.Min(remaining/tick.price, tick.qty)
		totalQty += fillQty
		totalCost += fillQty * tick.price
		remaining -= fillQty * tick.price
		return remaining > 0
	})

	if totalQty == 0 {
		return 0, 1.0
	}
	avgPrice = totalCost / totalQty
	if s.asc {
		slippage = (avgPrice - topPrice) / topPrice
	} else {
		slippage = (topPrice - avgPrice) / topPrice
	}
	return avgPrice, slippage
}

// Book holds one venue's L2 order book. Bids and asks are independent B-Trees
// so writes on one side don't block reads on the other.
type Book struct {
	Exchange  string
	Symbol    string
	bids      *sideBook
	asks      *sideBook
	updatedAt int64 // unix nanos, atomic-ish (single writer per feed goroutine)
}

func newBook(exchange, symbol string) *Book {
	return &Book{
		Exchange: exchange,
		Symbol:   symbol,
		bids:     newSideBook(false), // desc — best bid first
		asks:     newSideBook(true),  // asc  — best ask first
	}
}

func (b *Book) applyDelta(side string, price, qty float64) {
	if side == "bid" {
		b.bids.update(price, qty)
	} else {
		b.asks.update(price, qty)
	}
	b.updatedAt = time.Now().UnixNano()
}

// MarketImpact — O(k) sweep, no allocations, no sort. This is the right path.
func (b *Book) MarketImpact(side string, notionalUSD float64) (avgPrice, slippage float64) {
	if side == "sell" {
		return b.bids.sweep(notionalUSD)
	}
	return b.asks.sweep(notionalUSD)
}

// BinanceHandler — depth@100ms stream for ETH/USDT.
// Using the diff stream (not snapshot) so we need an initial snapshot on reconnect.
// TODO: implement snapshot fetch + seq validation on reconnect.
type BinanceHandler struct {
	book *Book
	log  *zap.SugaredLogger
}

func (h *BinanceHandler) Connect(ctx context.Context) error {
	url := "wss://stream.binance.com:9443/ws/ethusdt@depth@100ms"
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("binance dial: %w", err)
	}
	defer conn.Close()
	h.log.Info("binance book connected")

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
		h.handleDepth(msg)
	}
}

type binanceDepth struct {
	Bids [][]json.RawMessage `json:"b"`
	Asks [][]json.RawMessage `json:"a"`
}

func (h *BinanceHandler) handleDepth(raw []byte) {
	var msg binanceDepth
	if json.Unmarshal(raw, &msg) != nil {
		return
	}
	for _, lvl := range msg.Bids {
		p, q := parseRawLevel(lvl)
		h.book.applyDelta("bid", p, q)
	}
	for _, lvl := range msg.Asks {
		p, q := parseRawLevel(lvl)
		h.book.applyDelta("ask", p, q)
	}
}

// BybitHandler — orderbook.50 linear stream.
type BybitHandler struct {
	book *Book
	log  *zap.SugaredLogger
}

func (h *BybitHandler) Connect(ctx context.Context) error {
	url := "wss://stream.bybit.com/v5/public/linear"
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("bybit dial: %w", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"op":   "subscribe",
		"args": []string{"orderbook.50.ETHUSDT"},
	}); err != nil {
		return fmt.Errorf("bybit subscribe: %w", err)
	}
	h.log.Info("bybit book connected")

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

type bybitMsg struct {
	Topic string `json:"topic"`
	Data  struct {
		Bids [][]string `json:"b"`
		Asks [][]string `json:"a"`
	} `json:"data"`
}

func (h *BybitHandler) handleMsg(raw []byte) {
	var msg bybitMsg
	if json.Unmarshal(raw, &msg) != nil || msg.Topic == "" {
		return
	}
	for _, lvl := range msg.Data.Bids {
		if len(lvl) == 2 {
			p, q := parseStrLevel(lvl[0], lvl[1])
			h.book.applyDelta("bid", p, q)
		}
	}
	for _, lvl := range msg.Data.Asks {
		if len(lvl) == 2 {
			p, q := parseStrLevel(lvl[0], lvl[1])
			h.book.applyDelta("ask", p, q)
		}
	}
}

// AggregatedBook fans out slippage queries to each venue.
type AggregatedBook struct {
	books []*Book
}

func newAggregatedBook(books ...*Book) *AggregatedBook {
	return &AggregatedBook{books: books}
}

// SlippageForNotional returns per-venue + combined slippage for a given sell/buy size.
// Combined is a simple average for now — TODO: proper cross-venue NBBO sweep.
func (ab *AggregatedBook) SlippageForNotional(side string, notionalUSD float64) map[string]float64 {
	result := make(map[string]float64, len(ab.books)+1)
	var sum float64
	for _, b := range ab.books {
		_, slip := b.MarketImpact(side, notionalUSD)
		result[b.Exchange] = slip
		sum += slip
	}
	if len(ab.books) > 0 {
		result["combined"] = sum / float64(len(ab.books))
	}
	return result
}

// parseRawLevel handles Binance's [[string, string], ...] format.
func parseRawLevel(raw []json.RawMessage) (float64, float64) {
	if len(raw) < 2 {
		return 0, 0
	}
	var ps, qs string
	json.Unmarshal(raw[0], &ps)
	json.Unmarshal(raw[1], &qs)
	p, _ := strconv.ParseFloat(ps, 64)
	q, _ := strconv.ParseFloat(qs, 64)
	return p, q
}

func parseStrLevel(ps, qs string) (float64, float64) {
	p, _ := strconv.ParseFloat(ps, 64)
	q, _ := strconv.ParseFloat(qs, 64)
	return p, q
}

func withReconnect(ctx context.Context, name string, log *zap.SugaredLogger, fn func(context.Context) error) {
	for {
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			log.Warnw("reconnecting", "feed", name, "err", err)
			time.Sleep(2 * time.Second)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func main() {
	raw, _ := zap.NewProduction()
	log := raw.Sugar()
	defer log.Sync()

	binanceBook := newBook("binance", "ETH/USDT")
	bybitBook   := newBook("bybit", "ETH/USDT")
	agg         := newAggregatedBook(binanceBook, bybitBook)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go withReconnect(ctx, "binance", log, (&BinanceHandler{book: binanceBook, log: log}).Connect)
	go withReconnect(ctx, "bybit",   log, (&BybitHandler{book: bybitBook, log: log}).Connect)

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				for _, n := range []float64{1e6, 5e6, 10e6} {
					s := agg.SlippageForNotional("sell", n)
					log.Infow("slippage", "notional", n, "slip", s)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Info("aggregator shutting down")
}
