package solanarpc

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

const (
	poolAddr  = "PooL11111111111111111111111111111111111111"
	traderKey = "Trader111111111111111111111111111111111111"
	mint      = "Mint1111111111111111111111111111111111111"
)

type node struct {
	mu        sync.Mutex
	sigs      []SignatureInfo
	txs       map[string]any
	sigErr    bool
	sigReqs   int
	lastUntil string
}

func (n *node) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	trimmed := strings.TrimSpace(string(body))

	if strings.HasPrefix(trimmed, "[") {
		var reqs []struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
			Params []any  `json:"params"`
		}
		json.Unmarshal(body, &reqs)

		n.mu.Lock()
		defer n.mu.Unlock()
		out := make([]map[string]any, 0, len(reqs))
		for _, req := range reqs {
			sig, _ := req.Params[0].(string)
			out = append(out, map[string]any{
				"jsonrpc": "2.0", "id": req.ID, "result": n.txs[sig],
			})
		}
		json.NewEncoder(w).Encode(out)
		return
	}

	var req struct {
		ID     int    `json:"id"`
		Method string `json:"method"`
		Params []any  `json:"params"`
	}
	json.Unmarshal(body, &req)

	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Method == "getSignaturesForAddress" {
		n.sigReqs++
		if opts, ok := req.Params[1].(map[string]any); ok {
			n.lastUntil, _ = opts["until"].(string)
		}
		if n.sigErr {
			json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0", "id": req.ID,
				"error": map[string]any{"code": -32000, "message": "unavailable"},
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": n.sigs})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": nil})
}

// swapTx builds a getTransaction result where the trader's balance moves by
// traderDelta and the pool's by the opposite amount.
func swapTx(blockTime int64, traderPre, traderPost, poolPre, poolPost float64) map[string]any {
	bal := func(idx int, owner string, amt float64) map[string]any {
		return map[string]any{
			"accountIndex": idx, "mint": mint, "owner": owner,
			"uiTokenAmount": map[string]any{"amount": "0", "decimals": 9, "uiAmount": amt},
		}
	}
	return map[string]any{
		"slot": 100, "blockTime": blockTime,
		"meta": map[string]any{
			"err":               nil,
			"preTokenBalances":  []any{bal(1, traderKey, traderPre), bal(2, poolAddr, poolPre)},
			"postTokenBalances": []any{bal(1, traderKey, traderPost), bal(2, poolAddr, poolPost)},
		},
		"transaction": map[string]any{
			"message": map[string]any{
				"accountKeys": []any{
					map[string]any{"pubkey": traderKey, "signer": true},
					map[string]any{"pubkey": poolAddr, "signer": false},
				},
			},
		},
	}
}

type fixedPrices map[string]float64

func (f fixedPrices) PriceUSD(_ domain.Chain, addr string) (float64, bool) {
	p, ok := f[addr]
	return p, ok
}

func newSource(t *testing.T, n *node, prices PriceLookup, opts Options) *Source {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(n.handle))
	t.Cleanup(srv.Close)

	s := NewSource(NewRPC(srv.URL, 100000), prices, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.SetPools([]domain.Pool{{Chain: domain.ChainSolana, Address: poolAddr, DEX: "raydium"}})
	s.SetTokens([]domain.Token{{Chain: domain.ChainSolana, Address: mint, Symbol: "ABC"}})
	return s
}

func drain(t *testing.T, s *Source) []domain.Trade {
	t.Helper()
	ch := make(chan domain.Trade, 64)
	if err := s.Poll(t.Context(), ch); err != nil {
		t.Fatalf("poll: %v", err)
	}
	close(ch)
	var out []domain.Trade
	for tr := range ch {
		out = append(out, tr)
	}
	return out
}

func TestBuyIsAttributedToTheFeePayer(t *testing.T) {
	// The trader's balance rises by 100 and the pool's falls by 100. Both
	// movements are equal in size, so picking "the largest delta" would be a
	// coin flip; the fee payer settles it.
	ts := time.Date(2026, 9, 1, 12, 30, 14, 0, time.UTC)
	n := &node{
		sigs: []SignatureInfo{{Signature: "sig1", Slot: 100}},
		txs:  map[string]any{"sig1": swapTx(ts.Unix(), 400, 500, 9000, 8900)},
	}
	s := newSource(t, n, fixedPrices{mint: 2.5}, DefaultOptions())

	got := drain(t, s)
	if len(got) != 1 {
		t.Fatalf("got %d trades, want 1", len(got))
	}
	if got[0].Side != domain.SideBuy {
		t.Fatalf("side = %s, want BUY", got[0].Side)
	}
	if got[0].TokenAmount != 100 {
		t.Fatalf("amount = %v, want 100", got[0].TokenAmount)
	}
	if got[0].USDVolume != 250 {
		t.Fatalf("usd = %v, want 250", got[0].USDVolume)
	}
	if !got[0].Timestamp.Equal(ts) {
		t.Fatalf("timestamp = %v, want %v", got[0].Timestamp, ts)
	}
	if got[0].DedupKey() != "solana|sig1|0" {
		t.Fatalf("dedup key = %q", got[0].DedupKey())
	}
}

func TestSellIsAttributedToTheFeePayer(t *testing.T) {
	n := &node{
		sigs: []SignatureInfo{{Signature: "sig1"}},
		txs:  map[string]any{"sig1": swapTx(time.Now().Unix(), 500, 400, 9000, 9100)},
	}
	s := newSource(t, n, fixedPrices{mint: 2.5}, DefaultOptions())

	got := drain(t, s)
	if len(got) != 1 || got[0].Side != domain.SideSell {
		t.Fatalf("expected one SELL, got %+v", got)
	}
}

func TestSideIsStableAcrossRuns(t *testing.T) {
	// Guards the ambiguity directly: an equal-and-opposite swap must decode
	// the same way every time, not depend on map ordering.
	for i := 0; i < 50; i++ {
		n := &node{
			sigs: []SignatureInfo{{Signature: "sig1"}},
			txs:  map[string]any{"sig1": swapTx(time.Now().Unix(), 400, 500, 9000, 8900)},
		}
		s := newSource(t, n, fixedPrices{mint: 1}, DefaultOptions())
		got := drain(t, s)
		if len(got) != 1 || got[0].Side != domain.SideBuy {
			t.Fatalf("run %d decoded %+v", i, got)
		}
	}
}

func TestFailedTransactionIsIgnored(t *testing.T) {
	tx := swapTx(time.Now().Unix(), 400, 500, 9000, 8900)
	tx["meta"].(map[string]any)["err"] = map[string]any{"InstructionError": []any{0, "Custom"}}

	n := &node{sigs: []SignatureInfo{{Signature: "sig1"}}, txs: map[string]any{"sig1": tx}}
	s := newSource(t, n, fixedPrices{mint: 1}, DefaultOptions())

	if got := drain(t, s); len(got) != 0 {
		t.Fatalf("a failed transaction settled nothing, got %+v", got)
	}
}

func TestSignatureErrorEntryIsSkippedBeforeFetching(t *testing.T) {
	n := &node{
		sigs: []SignatureInfo{{Signature: "sig1", Err: map[string]any{"x": 1}}},
		txs:  map[string]any{},
	}
	s := newSource(t, n, fixedPrices{mint: 1}, DefaultOptions())
	if got := drain(t, s); len(got) != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestCursorAdvancesAndIsSentAsUntil(t *testing.T) {
	n := &node{
		sigs: []SignatureInfo{{Signature: "newest"}, {Signature: "older"}},
		txs: map[string]any{
			"newest": swapTx(time.Now().Unix(), 400, 500, 9000, 8900),
			"older":  swapTx(time.Now().Unix(), 400, 450, 9000, 8950),
		},
	}
	s := newSource(t, n, fixedPrices{mint: 1}, DefaultOptions())
	drain(t, s)

	if s.Cursor(poolAddr) != "newest" {
		t.Fatalf("cursor = %q, want the head signature", s.Cursor(poolAddr))
	}
	drain(t, s)
	if n.lastUntil != "newest" {
		t.Fatalf("second poll sent until=%q; without it the same trades would be refetched", n.lastUntil)
	}
}

func TestTruncationMarksSourceUnhealthy(t *testing.T) {
	// Being over budget means trades of this interval went unread. Reporting
	// healthy would let the engine record an understated minute as real.
	opts := DefaultOptions()
	opts.SignaturesPerPool = 2
	opts.MaxTransactionsPerPoll = 2

	n := &node{
		sigs: []SignatureInfo{{Signature: "a"}, {Signature: "b"}},
		txs: map[string]any{
			"a": swapTx(time.Now().Unix(), 400, 500, 9000, 8900),
			"b": swapTx(time.Now().Unix(), 400, 500, 9000, 8900),
		},
	}
	s := newSource(t, n, fixedPrices{mint: 1}, opts)
	drain(t, s)

	if s.Healthy() {
		t.Fatal("a truncated poll must report unhealthy so those minutes become MISSING")
	}
}

func TestRPCErrorMarksUnhealthy(t *testing.T) {
	n := &node{sigErr: true}
	s := newSource(t, n, fixedPrices{mint: 1}, DefaultOptions())

	ch := make(chan domain.Trade, 4)
	if err := s.Poll(t.Context(), ch); err == nil {
		t.Fatal("expected an error")
	}
	if s.Healthy() {
		t.Fatal("source must be unhealthy after an RPC failure")
	}
}

func TestUnpricedTokenIsSkipped(t *testing.T) {
	n := &node{
		sigs: []SignatureInfo{{Signature: "sig1"}},
		txs:  map[string]any{"sig1": swapTx(time.Now().Unix(), 400, 500, 9000, 8900)},
	}
	s := newSource(t, n, fixedPrices{}, DefaultOptions())
	if got := drain(t, s); len(got) != 0 {
		t.Fatalf("got %+v", got)
	}
}
