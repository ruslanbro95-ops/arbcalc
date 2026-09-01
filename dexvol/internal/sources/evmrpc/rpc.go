// Package evmrpc ingests trades straight from an EVM node.
//
// This is the primary trade source for ethereum, bsc, base and robinhood. The
// reason is arithmetic, not preference: eth_getLogs accepts a list of addresses
// and a list of topics, so one call per chain per interval covers every pool of
// every tracked token. Every aggregator alternative charges one request per
// pool, which exhausts a free tier before it covers a single token. See
// docs/RESEARCH.md §3.6.
package evmrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources"
)

// RPC is a minimal JSON-RPC 2.0 client with batching.
type RPC struct {
	http *sources.HTTP
	url  string
}

// NewRPC builds a client. perMinute should reflect the endpoint's limit —
// public endpoints are far stingier than a paid one, and the whole ingestion
// design depends on staying inside the free allowance.
func NewRPC(name, url string, perMinute int) *RPC {
	return &RPC{
		http: sources.NewHTTP(name, perMinute, 30*time.Second),
		url:  url,
	}
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

// Call performs a single JSON-RPC request.
func (r *RPC) Call(ctx context.Context, method string, params []any, out any) error {
	req := rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params}
	var resp rpcResponse
	if err := r.http.PostJSON(ctx, r.url, req, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("%s: %w", method, resp.Error)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resp.Result, out)
}

// BatchCall sends several requests in one HTTP round trip and returns the
// results in the order they were requested.
//
// Batching is what keeps block-timestamp and token-metadata lookups from
// dominating the request budget: a poll that touches forty blocks costs one
// call, not forty.
func (r *RPC) BatchCall(ctx context.Context, calls []Call) ([]json.RawMessage, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	reqs := make([]rpcRequest, len(calls))
	for i, c := range calls {
		reqs[i] = rpcRequest{JSONRPC: "2.0", ID: i, Method: c.Method, Params: c.Params}
	}

	var resps []rpcResponse
	if err := r.http.PostJSON(ctx, r.url, reqs, &resps); err != nil {
		return nil, err
	}

	// Nodes are permitted to return batch responses out of order, so results
	// are placed by id rather than by position.
	out := make([]json.RawMessage, len(calls))
	for _, resp := range resps {
		if resp.ID < 0 || resp.ID >= len(calls) {
			continue
		}
		if resp.Error != nil {
			// One failed sub-call must not discard the rest of the batch; the
			// caller sees a nil result for that entry.
			continue
		}
		out[resp.ID] = resp.Result
	}
	return out, nil
}

// Call is one entry in a batch.
type Call struct {
	Method string
	Params []any
}

// Log is an eth_getLogs entry.
type Log struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
	Removed     bool     `json:"removed"`
	// BlockTimestamp is the block's time, returned inline by Geth-derived
	// nodes since 2023 and by every endpoint this project ships a default
	// for except one.
	//
	// It is worth a field of its own because it removes an entire class of
	// request. Without it each batch of logs needs an eth_getBlockByNumber
	// per distinct block, and on Robinhood Chain — 0.12-second blocks, so a
	// twelve-second poll spans a hundred of them — that was hundreds of extra
	// sub-calls a minute, which is what made its own node rate limit us into
	// one usable minute out of fifty-nine.
	BlockTimestamp string `json:"blockTimestamp"`
}

// BlockNumberValue parses the hex block number.
func (l Log) BlockNumberValue() (uint64, error) { return parseHexUint(l.BlockNumber) }

// LogIndexValue parses the hex log index.
func (l Log) LogIndexValue() (int, error) {
	v, err := parseHexUint(l.LogIndex)
	return int(v), err
}

// BlockNumber returns the current head.
func (r *RPC) BlockNumber(ctx context.Context) (uint64, error) {
	var hexNum string
	if err := r.Call(ctx, "eth_blockNumber", []any{}, &hexNum); err != nil {
		return 0, err
	}
	return parseHexUint(hexNum)
}

// GetLogs fetches Swap logs for the given pools in one call.
func (r *RPC) GetLogs(ctx context.Context, from, to uint64, addresses []string, topics []string) ([]Log, error) {
	filter := map[string]any{
		"fromBlock": hexUint(from),
		"toBlock":   hexUint(to),
		// A nested array means "topic0 is any of these".
		"topics": []any{topics},
	}
	if len(addresses) > 0 {
		filter["address"] = addresses
	}

	var logs []Log
	if err := r.Call(ctx, "eth_getLogs", []any{filter}, &logs); err != nil {
		return nil, err
	}
	return logs, nil
}

// BlockTimes resolves the timestamp of each block number in one batch.
func (r *RPC) BlockTimes(ctx context.Context, blocks []uint64) (map[uint64]time.Time, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	calls := make([]Call, len(blocks))
	for i, b := range blocks {
		// false: transaction hashes only. Full bodies would be megabytes of
		// payload for a field we do not read.
		calls[i] = Call{Method: "eth_getBlockByNumber", Params: []any{hexUint(b), false}}
	}

	results, err := r.BatchCall(ctx, calls)
	if err != nil {
		return nil, err
	}

	out := make(map[uint64]time.Time, len(blocks))
	for i, raw := range results {
		if len(raw) == 0 {
			continue
		}
		var blk struct {
			Timestamp string `json:"timestamp"`
		}
		if err := json.Unmarshal(raw, &blk); err != nil {
			continue
		}
		ts, err := parseHexUint(blk.Timestamp)
		if err != nil {
			continue
		}
		out[blocks[i]] = time.Unix(int64(ts), 0).UTC()
	}
	return out, nil
}

// EthCall performs a batch of read-only contract calls, returning raw hex
// results aligned with the input.
func (r *RPC) EthCall(ctx context.Context, targets []string, data string) ([]string, error) {
	calls := make([]Call, len(targets))
	for i, addr := range targets {
		calls[i] = Call{
			Method: "eth_call",
			Params: []any{map[string]string{"to": addr, "data": data}, "latest"},
		}
	}

	results, err := r.BatchCall(ctx, calls)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(targets))
	for i, raw := range results {
		if len(raw) == 0 {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			continue
		}
		out[i] = s
	}
	return out, nil
}

func hexUint(v uint64) string { return "0x" + strconv.FormatUint(v, 16) }

func parseHexUint(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty hex value")
	}
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	// Some nodes return values wider than 64 bits for timestamps; big.Int
	// parses them without overflowing.
	v, ok := new(big.Int).SetString(s, 16)
	if !ok {
		return 0, fmt.Errorf("%q is not hex", s)
	}
	if !v.IsUint64() {
		return 0, fmt.Errorf("hex value %q does not fit in 64 bits", s)
	}
	return v.Uint64(), nil
}

// ChainID returns the endpoint's advertised chain id.
//
// Preflight uses it to catch the mistake no other check would: an RPC_* variable
// pointing at a perfectly healthy node for the wrong network. Nothing
// downstream would error — the service would just poll the wrong chain forever
// and report that the token has no trades.
func (r *RPC) ChainID(ctx context.Context) (uint64, error) {
	var hexNum string
	if err := r.Call(ctx, "eth_chainId", []any{}, &hexNum); err != nil {
		return 0, err
	}
	return parseHexUint(hexNum)
}

// BlockTimestampValue parses the inline block time, reporting false when the
// node did not send one so the caller knows to ask for it separately.
func (l Log) BlockTimestampValue() (time.Time, bool) {
	if l.BlockTimestamp == "" {
		return time.Time{}, false
	}
	secs, err := parseHexUint(l.BlockTimestamp)
	if err != nil || secs == 0 {
		return time.Time{}, false
	}
	return time.Unix(int64(secs), 0).UTC(), true
}
