// Package solanarpc ingests Solana trades from a standard JSON-RPC endpoint.
//
// Solana is the weak spot this project cannot engineer away for free, and
// docs/RESEARCH.md §3.7 says so plainly. There is no analogue of eth_getLogs
// that filters many accounts over a slot range in one call, and the parsed
// trade streams that would solve it (Helius transactionSubscribe) moved behind
// a paid plan in April 2026.
//
// What is left within free limits is: ask each pool for its new signatures,
// then fetch those transactions and read the token balance deltas. That is
// exact — the deltas are the settled truth of what moved — but it costs one
// request per pool per poll plus one per transaction, so the budget grows with
// activity rather than staying flat. The source reports itself unhealthy when
// it has to truncate, which turns the shortfall into MISSING minutes instead
// of silently understated volume.
package solanarpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources"
)

// PriceLookup supplies USD prices for a mint.
type PriceLookup interface {
	PriceUSD(chain domain.Chain, tokenAddress string) (float64, bool)
}

// Options bounds the request budget.
type Options struct {
	// SignaturesPerPool caps how many new signatures one poll pulls per pool.
	SignaturesPerPool int
	// MaxTransactionsPerPoll caps the transaction fetches across all pools, so
	// one busy token cannot consume the whole allowance and starve the rest.
	MaxTransactionsPerPoll int
	// TransactionBatchSize is how many getTransaction calls ride in one HTTP
	// request.
	TransactionBatchSize int
}

func DefaultOptions() Options {
	return Options{SignaturesPerPool: 100, MaxTransactionsPerPoll: 300, TransactionBatchSize: 25}
}

// RPC is a minimal Solana JSON-RPC client.
type RPC struct {
	http *sources.HTTP
	url  string
}

func NewRPC(url string, perMinute int) *RPC {
	return &RPC{http: sources.NewHTTP("solanarpc", perMinute, 30*time.Second), url: url}
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
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (r *RPC) call(ctx context.Context, method string, params []any, out any) error {
	var resp rpcResponse
	if err := r.http.PostJSON(ctx, r.url, rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params}, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("%s: rpc error %d: %s", method, resp.Error.Code, resp.Error.Message)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resp.Result, out)
}

// SignatureInfo is one entry from getSignaturesForAddress.
type SignatureInfo struct {
	Signature string `json:"signature"`
	Slot      uint64 `json:"slot"`
	BlockTime *int64 `json:"blockTime"`
	Err       any    `json:"err"`
}

// Signatures returns the pool's signatures newer than `until`.
func (r *RPC) Signatures(ctx context.Context, pool, until string, limit int) ([]SignatureInfo, error) {
	opts := map[string]any{"limit": limit}
	if until != "" {
		opts["until"] = until
	}
	var out []SignatureInfo
	if err := r.call(ctx, "getSignaturesForAddress", []any{pool, opts}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// TokenBalance is one pre/post token balance record.
type TokenBalance struct {
	AccountIndex  int    `json:"accountIndex"`
	Mint          string `json:"mint"`
	Owner         string `json:"owner"`
	UITokenAmount struct {
		Amount   string  `json:"amount"`
		Decimals int     `json:"decimals"`
		UIAmount float64 `json:"uiAmount"`
	} `json:"uiTokenAmount"`
}

// Transaction is the slice of getTransaction this source reads.
type Transaction struct {
	Slot      uint64 `json:"slot"`
	BlockTime *int64 `json:"blockTime"`
	Meta      *struct {
		Err               any            `json:"err"`
		PreTokenBalances  []TokenBalance `json:"preTokenBalances"`
		PostTokenBalances []TokenBalance `json:"postTokenBalances"`
	} `json:"meta"`
	Transaction struct {
		Message struct {
			// jsonParsed encoding returns objects; the first entry is the fee
			// payer, which is how a trade's initiator is identified.
			AccountKeys []struct {
				Pubkey string `json:"pubkey"`
				Signer bool   `json:"signer"`
			} `json:"accountKeys"`
		} `json:"message"`
	} `json:"transaction"`
}

// Transactions fetches several transactions in one HTTP round trip.
func (r *RPC) Transactions(ctx context.Context, sigs []string) (map[string]*Transaction, error) {
	if len(sigs) == 0 {
		return nil, nil
	}
	reqs := make([]rpcRequest, len(sigs))
	for i, sig := range sigs {
		reqs[i] = rpcRequest{
			JSONRPC: "2.0", ID: i, Method: "getTransaction",
			Params: []any{sig, map[string]any{
				"encoding": "jsonParsed",
				// Without this, versioned transactions come back as an error,
				// and versioned transactions are most of Solana DEX traffic.
				"maxSupportedTransactionVersion": 0,
				"commitment":                     "confirmed",
			}},
		}
	}

	var resps []rpcResponse
	if err := r.http.PostJSON(ctx, r.url, reqs, &resps); err != nil {
		return nil, err
	}

	out := make(map[string]*Transaction, len(sigs))
	for _, resp := range resps {
		if resp.ID < 0 || resp.ID >= len(sigs) || resp.Error != nil || len(resp.Result) == 0 {
			continue
		}
		var tx Transaction
		if err := json.Unmarshal(resp.Result, &tx); err != nil {
			continue
		}
		out[sigs[resp.ID]] = &tx
	}
	return out, nil
}

// Source ingests Solana trades for the tracked tokens.
type Source struct {
	rpc    *RPC
	prices PriceLookup
	opts   Options
	log    *slog.Logger

	mu      sync.RWMutex
	pools   map[string]domain.Pool
	tokens  map[string]domain.Token
	cursor  map[string]string // pool -> newest signature already processed
	healthy bool
}

func NewSource(rpc *RPC, prices PriceLookup, opts Options, log *slog.Logger) *Source {
	return &Source{
		rpc:    rpc,
		prices: prices,
		opts:   opts,
		log:    log.With("chain", domain.ChainSolana, "source", "solanarpc"),
		pools:  map[string]domain.Pool{},
		tokens: map[string]domain.Token{},
		cursor: map[string]string{},
	}
}

func (s *Source) Name() string        { return "solanarpc" }
func (s *Source) Chain() domain.Chain { return domain.ChainSolana }

func (s *Source) Healthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

func (s *Source) SetPools(pools []domain.Pool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]domain.Pool, len(pools))
	for _, p := range pools {
		if p.Chain != domain.ChainSolana {
			continue
		}
		next[p.Address] = p
	}
	s.pools = next
	// Drop cursors for pools that went away, so the map cannot grow forever.
	for addr := range s.cursor {
		if _, ok := next[addr]; !ok {
			delete(s.cursor, addr)
		}
	}
}

func (s *Source) SetTokens(toks []domain.Token) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = make(map[string]domain.Token, len(toks))
	for _, t := range toks {
		if t.Chain != domain.ChainSolana {
			continue
		}
		s.tokens[t.Address] = t
	}
}

// Poll pulls new signatures per pool, then resolves them in batches.
func (s *Source) Poll(ctx context.Context, out chan<- domain.Trade) error {
	s.mu.RLock()
	pools := make([]domain.Pool, 0, len(s.pools))
	for _, p := range s.pools {
		pools = append(pools, p)
	}
	cursors := make(map[string]string, len(s.cursor))
	for k, v := range s.cursor {
		cursors[k] = v
	}
	s.mu.RUnlock()

	if len(pools) == 0 {
		s.setHealthy(true)
		return nil
	}
	sort.Slice(pools, func(i, j int) bool { return pools[i].Address < pools[j].Address })

	type job struct {
		pool domain.Pool
		sig  string
	}
	var jobs []job
	newest := map[string]string{}
	truncated := false

	for _, p := range pools {
		sigs, err := s.rpc.Signatures(ctx, p.Address, cursors[p.Address], s.opts.SignaturesPerPool)
		if err != nil {
			s.setHealthy(false)
			return fmt.Errorf("signatures for %s: %w", p.Address, err)
		}
		if len(sigs) == 0 {
			continue
		}
		// The API returns newest first, so the head is the next cursor.
		newest[p.Address] = sigs[0].Signature
		if len(sigs) == s.opts.SignaturesPerPool {
			// A full page means there may be more we did not see.
			truncated = true
		}
		for _, si := range sigs {
			if si.Err != nil {
				continue // failed transaction: nothing settled
			}
			jobs = append(jobs, job{pool: p, sig: si.Signature})
		}
	}

	if len(jobs) > s.opts.MaxTransactionsPerPoll {
		jobs = jobs[:s.opts.MaxTransactionsPerPoll]
		truncated = true
	}

	for start := 0; start < len(jobs); start += s.opts.TransactionBatchSize {
		end := min(start+s.opts.TransactionBatchSize, len(jobs))
		batch := jobs[start:end]

		sigs := make([]string, len(batch))
		for i, j := range batch {
			sigs[i] = j.sig
		}
		txs, err := s.rpc.Transactions(ctx, sigs)
		if err != nil {
			s.setHealthy(false)
			return fmt.Errorf("transactions: %w", err)
		}
		for _, j := range batch {
			tx := txs[j.sig]
			if tx == nil {
				continue
			}
			trade, ok := s.decode(j.pool, j.sig, tx)
			if !ok {
				continue
			}
			select {
			case out <- trade:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	s.mu.Lock()
	for pool, sig := range newest {
		s.cursor[pool] = sig
	}
	s.mu.Unlock()

	if truncated {
		// Being over budget means some trades of this interval were never
		// read. Saying so turns those minutes into MISSING rather than into
		// quietly understated volume that would later read as a spike.
		s.log.Warn("poll truncated; some trades were not read this interval")
		s.setHealthy(false)
		return nil
	}
	s.setHealthy(true)
	return nil
}

// decode turns one transaction into a trade for the tracked token.
//
// Amounts come from settled token balance deltas rather than from parsing
// instruction data: every AMM encodes its swap differently, but all of them
// move tokens, and the balance delta is the same regardless of which program
// produced it.
func (s *Source) decode(pool domain.Pool, sig string, tx *Transaction) (domain.Trade, bool) {
	if tx.Meta == nil || tx.Meta.Err != nil {
		return domain.Trade{}, false
	}
	ts, ok := blockTime(tx)
	if !ok {
		// Without a timestamp the trade cannot be filed into a minute.
		return domain.Trade{}, false
	}

	s.mu.RLock()
	tokens := s.tokens
	s.mu.RUnlock()

	// Find which tracked token this transaction moved.
	var tracked domain.Token
	found := false
	for _, list := range [][]TokenBalance{tx.Meta.PostTokenBalances, tx.Meta.PreTokenBalances} {
		for _, bal := range list {
			if t, ok := tokens[bal.Mint]; ok {
				tracked, found = t, true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		return domain.Trade{}, false
	}

	byOwner := deltasByOwner(tx, tracked.Address)

	// The trade is measured from the trader's own account. Deriving the side
	// from "whichever account moved most" would be ambiguous in the ordinary
	// two-account swap, where the debit and the credit are equal and opposite
	// and the winner would depend on map iteration order.
	var (
		amount float64
		side   domain.Side
	)
	switch {
	case len(tx.Transaction.Message.AccountKeys) > 0 && byOwner[tx.Transaction.Message.AccountKeys[0].Pubkey] != 0:
		delta := byOwner[tx.Transaction.Message.AccountKeys[0].Pubkey]
		amount = math.Abs(delta)
		side = domain.SideBuy
		if delta < 0 {
			side = domain.SideSell // the trader's balance fell: they sold
		}
	case byOwner[pool.Address] != 0:
		// Some AMMs route through a program-owned account, leaving the fee
		// payer with no direct balance change. The pool's own movement is the
		// mirror image of the trader's.
		delta := byOwner[pool.Address]
		amount = math.Abs(delta)
		side = domain.SideSell
		if delta < 0 {
			side = domain.SideBuy // the pool paid out: the trader bought
		}
	default:
		// Neither side could be attributed. Guessing would invent a direction,
		// so the trade is dropped and shows up as a coverage gap instead.
		return domain.Trade{}, false
	}
	if amount <= 0 {
		return domain.Trade{}, false
	}

	price, ok := s.prices.PriceUSD(domain.ChainSolana, tracked.Address)
	if !ok || price <= 0 {
		return domain.Trade{}, false
	}

	return domain.Trade{
		Timestamp:    ts,
		Chain:        domain.ChainSolana,
		Token:        tracked.Symbol,
		TokenAddress: tracked.Address,
		Pool:         pool.Address,
		DEX:          pool.DEX,
		TxHash:       sig,
		// Deltas are netted across the whole transaction, so a signature maps
		// to exactly one trade per token and the index is always zero.
		LogIndex:    0,
		Side:        side,
		TokenAmount: amount,
		USDVolume:   amount * price,
		Price:       price,
		Source:      s.Name(),
	}, true
}

// deltasByOwner sums each owner's change in the mint's balance across the
// transaction. Owners are used rather than account indexes because one owner
// can hold several token accounts for the same mint.
func deltasByOwner(tx *Transaction, mint string) map[string]float64 {
	pre := map[int]TokenBalance{}
	for _, b := range tx.Meta.PreTokenBalances {
		if b.Mint == mint {
			pre[b.AccountIndex] = b
		}
	}
	post := map[int]TokenBalance{}
	for _, b := range tx.Meta.PostTokenBalances {
		if b.Mint == mint {
			post[b.AccountIndex] = b
		}
	}

	out := map[string]float64{}
	for idx, b := range post {
		owner := b.Owner
		if owner == "" {
			owner = pre[idx].Owner
		}
		out[owner] += b.UITokenAmount.UIAmount - pre[idx].UITokenAmount.UIAmount
	}
	for idx, b := range pre {
		if _, ok := post[idx]; ok {
			continue
		}
		// The account was closed during the transaction: its whole balance left.
		out[b.Owner] -= b.UITokenAmount.UIAmount
	}
	return out
}

func blockTime(tx *Transaction) (time.Time, bool) {
	if tx.BlockTime == nil {
		return time.Time{}, false
	}
	return time.Unix(*tx.BlockTime, 0).UTC(), true
}

func (s *Source) setHealthy(v bool) {
	s.mu.Lock()
	s.healthy = v
	s.mu.Unlock()
}

// Cursor exposes the newest processed signature for a pool, for diagnostics.
func (s *Source) Cursor(pool string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursor[strings.TrimSpace(pool)]
}
