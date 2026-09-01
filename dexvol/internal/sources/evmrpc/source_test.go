package evmrpc

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/evm"
)

const (
	poolAddr  = "0x1111111111111111111111111111111111111111"
	tokenAddr = "0x2222222222222222222222222222222222222222"
	wethAddr  = "0x3333333333333333333333333333333333333333"
)

// node is a scriptable JSON-RPC stub.
type node struct {
	mu        sync.Mutex
	head      uint64
	logs      []Log
	logsErr   bool
	getLogsN  int
	lastFrom  string
	lastTo    string
	blockTime int64
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

		out := make([]map[string]any, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, map[string]any{
				"jsonrpc": "2.0", "id": req.ID, "result": n.batchResult(req.Method, req.Params),
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

	switch req.Method {
	case "eth_blockNumber":
		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": req.ID, "result": hexUint(n.head),
		})
	case "eth_getLogs":
		n.getLogsN++
		if n.logsErr {
			json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0", "id": req.ID,
				"error": map[string]any{"code": -32000, "message": "range too wide"},
			})
			return
		}
		if f, ok := req.Params[0].(map[string]any); ok {
			n.lastFrom, _ = f["fromBlock"].(string)
			n.lastTo, _ = f["toBlock"].(string)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": req.ID, "result": n.logs,
		})
	default:
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": nil})
	}
}

func (n *node) batchResult(method string, params []any) any {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch method {
	case "eth_getBlockByNumber":
		return map[string]any{"timestamp": hexUint(uint64(n.blockTime))}
	case "eth_call":
		call, _ := params[0].(map[string]any)
		to, _ := call["to"].(string)
		data, _ := call["data"].(string)
		switch data {
		case selToken0:
			return wordFor(tokenAddr)
		case selToken1:
			return wordFor(wethAddr)
		case selDecimals:
			_ = to
			return "0x" + strings.Repeat("0", 62) + "12" // 18
		}
	}
	return nil
}

func wordFor(addr string) string {
	a := strings.TrimPrefix(addr, "0x")
	return "0x" + strings.Repeat("0", 64-len(a)) + a
}

// v2SwapLog builds a Uniswap V2 Swap log: amount0Out means token0 was bought.
func v2SwapLog(block uint64, logIndex int, txHash string, in1, out0 *big.Int) Log {
	var sb strings.Builder
	for _, v := range []*big.Int{big.NewInt(0), in1, out0, big.NewInt(0)} {
		h := v.Text(16)
		sb.WriteString(strings.Repeat("0", 64-len(h)) + h)
	}
	topic := evm.EventTopic("Swap(address,uint256,uint256,uint256,uint256,address)")
	return Log{
		Address:     poolAddr,
		Topics:      []string{"0x" + hex.EncodeToString(topic[:])},
		Data:        "0x" + sb.String(),
		BlockNumber: hexUint(block),
		TxHash:      txHash,
		LogIndex:    hexUint(uint64(logIndex)),
	}
}

type fixedPrices map[string]float64

func (f fixedPrices) PriceUSD(_ domain.Chain, addr string) (float64, bool) {
	p, ok := f[strings.ToLower(addr)]
	return p, ok
}

func newSource(t *testing.T, n *node, prices PriceLookup) *Source {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(n.handle))
	t.Cleanup(srv.Close)

	rpc := NewRPC("test", srv.URL, 100000)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewSource(domain.ChainBase, rpc, prices, DefaultOptions(), log)
	s.SetPools([]domain.Pool{{Chain: domain.ChainBase, Address: poolAddr, DEX: "uniswap"}})
	s.SetTokens([]domain.Token{{Chain: domain.ChainBase, Address: tokenAddr, Symbol: "ABC"}})
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

func TestFirstPollSeedsWithoutBackfilling(t *testing.T) {
	// Backfilling on the first poll would spend the request budget on minutes
	// that are already sealed and can never be admitted.
	n := &node{head: 1000, blockTime: time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC).Unix()}
	s := newSource(t, n, fixedPrices{tokenAddr: 2})

	if got := drain(t, s); len(got) != 0 {
		t.Fatalf("first poll emitted %d trades, want 0", len(got))
	}
	if s.LastBlock() != 998 { // head minus two confirmations
		t.Fatalf("lastBlock = %d, want 998", s.LastBlock())
	}
	if n.getLogsN != 0 {
		t.Fatalf("first poll should not call getLogs, called %d times", n.getLogsN)
	}
}

func TestDecodesBuyAndPricesInUSD(t *testing.T) {
	ts := time.Date(2026, 9, 1, 12, 30, 14, 0, time.UTC)
	n := &node{head: 1000, blockTime: ts.Unix()}
	s := newSource(t, n, fixedPrices{tokenAddr: 2})
	drain(t, s) // seed

	e18 := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	n.mu.Lock()
	n.head = 1010
	n.logs = []Log{v2SwapLog(1000, 3, "0xAAA",
		e18,                                     // 1 WETH in
		new(big.Int).Mul(big.NewInt(500), e18))} // 500 ABC out
	n.mu.Unlock()

	got := drain(t, s)
	if len(got) != 1 {
		t.Fatalf("got %d trades, want 1", len(got))
	}
	tr := got[0]
	if tr.Side != domain.SideBuy {
		t.Fatalf("side = %s, want BUY (tokens left the pool)", tr.Side)
	}
	if tr.TokenAmount != 500 {
		t.Fatalf("token amount = %v, want 500", tr.TokenAmount)
	}
	if tr.USDVolume != 1000 {
		t.Fatalf("usd = %v, want 1000 (500 x $2)", tr.USDVolume)
	}
	if !tr.Timestamp.Equal(ts) {
		t.Fatalf("timestamp = %v, want the block time %v", tr.Timestamp, ts)
	}
	if tr.DedupKey() != "base|0xaaa|3" {
		t.Fatalf("dedup key = %q", tr.DedupKey())
	}
}

func TestDecodesSell(t *testing.T) {
	n := &node{head: 1000, blockTime: time.Now().Unix()}
	s := newSource(t, n, fixedPrices{tokenAddr: 2})
	drain(t, s)

	e18 := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	// amount0In > 0 means the trader sold token0 into the pool.
	var sb strings.Builder
	for _, v := range []*big.Int{new(big.Int).Mul(big.NewInt(100), e18), big.NewInt(0), big.NewInt(0), e18} {
		h := v.Text(16)
		sb.WriteString(strings.Repeat("0", 64-len(h)) + h)
	}
	topic := evm.EventTopic("Swap(address,uint256,uint256,uint256,uint256,address)")

	n.mu.Lock()
	n.head = 1010
	n.logs = []Log{{
		Address: poolAddr, Topics: []string{"0x" + hex.EncodeToString(topic[:])},
		Data: "0x" + sb.String(), BlockNumber: hexUint(1000), TxHash: "0xBBB", LogIndex: "0x0",
	}}
	n.mu.Unlock()

	got := drain(t, s)
	if len(got) != 1 || got[0].Side != domain.SideSell {
		t.Fatalf("expected one SELL, got %+v", got)
	}
	if got[0].USDVolume != 200 {
		t.Fatalf("usd = %v, want 200", got[0].USDVolume)
	}
}

func TestReorgedLogIsDropped(t *testing.T) {
	n := &node{head: 1000, blockTime: time.Now().Unix()}
	s := newSource(t, n, fixedPrices{tokenAddr: 2})
	drain(t, s)

	e18 := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	l := v2SwapLog(1000, 0, "0xCCC", e18, e18)
	l.Removed = true

	n.mu.Lock()
	n.head = 1010
	n.logs = []Log{l}
	n.mu.Unlock()

	if got := drain(t, s); len(got) != 0 {
		t.Fatalf("a removed log must not become volume, got %+v", got)
	}
}

func TestPricingFallsBackToTheOtherSide(t *testing.T) {
	// A freshly listed token has no aggregator price yet, but its WETH
	// counterpart always does, and the swap is worth the same from either side.
	n := &node{head: 1000, blockTime: time.Now().Unix()}
	s := newSource(t, n, fixedPrices{wethAddr: 4000})
	drain(t, s)

	e18 := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	n.mu.Lock()
	n.head = 1010
	n.logs = []Log{v2SwapLog(1000, 0, "0xDDD",
		e18,                                     // 1 WETH in => $4000
		new(big.Int).Mul(big.NewInt(500), e18))} // 500 ABC out
	n.mu.Unlock()

	got := drain(t, s)
	if len(got) != 1 {
		t.Fatalf("got %d trades", len(got))
	}
	if got[0].USDVolume != 4000 {
		t.Fatalf("usd = %v, want 4000 from the WETH side", got[0].USDVolume)
	}
	if got[0].Price != 8 {
		t.Fatalf("implied price = %v, want 8 ($4000 / 500)", got[0].Price)
	}
}

func TestUnpricedTradeIsSkippedNotZeroed(t *testing.T) {
	n := &node{head: 1000, blockTime: time.Now().Unix()}
	s := newSource(t, n, fixedPrices{}) // no prices at all
	drain(t, s)

	e18 := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	n.mu.Lock()
	n.head = 1010
	n.logs = []Log{v2SwapLog(1000, 0, "0xEEE", e18, e18)}
	n.mu.Unlock()

	if got := drain(t, s); len(got) != 0 {
		t.Fatalf("an unpriceable trade must be skipped, got %+v", got)
	}
}

func TestRPCFailureMarksSourceUnhealthy(t *testing.T) {
	// Health is what turns a failed interval into MISSING minutes instead of
	// zeros, so it has to flip on a real failure.
	n := &node{head: 1000, blockTime: time.Now().Unix()}
	s := newSource(t, n, fixedPrices{tokenAddr: 2})
	drain(t, s)

	n.mu.Lock()
	n.head = 1010
	n.logsErr = true
	n.mu.Unlock()

	ch := make(chan domain.Trade, 8)
	if err := s.Poll(t.Context(), ch); err == nil {
		t.Fatal("expected an error")
	}
	if s.Healthy() {
		t.Fatal("a failed poll must leave the source unhealthy")
	}
}

func TestLongGapIsWalkedInChunks(t *testing.T) {
	n := &node{head: 1000, blockTime: time.Now().Unix()}
	s := newSource(t, n, fixedPrices{tokenAddr: 2})
	drain(t, s) // lastBlock = 998

	// Jump far ahead: the range must be split, not requested in one call that
	// a public endpoint would reject.
	n.mu.Lock()
	n.head = 5002
	n.mu.Unlock()

	drain(t, s)
	if n.getLogsN != DefaultOptions().MaxCatchUpChunks {
		t.Fatalf("getLogs called %d times, want %d chunks", n.getLogsN, DefaultOptions().MaxCatchUpChunks)
	}
	if n.lastTo == hexUint(5000) && n.lastFrom == hexUint(999) {
		t.Fatal("the whole gap was requested in one call")
	}
}

func TestNoPoolsIsHealthyNotAnOutage(t *testing.T) {
	n := &node{head: 1000, blockTime: time.Now().Unix()}
	s := newSource(t, n, fixedPrices{})
	s.SetPools(nil)

	if got := drain(t, s); len(got) != 0 {
		t.Fatalf("got %+v", got)
	}
	if !s.Healthy() {
		t.Fatal("having nothing to watch is a healthy state: those minutes are real zeros")
	}
}
