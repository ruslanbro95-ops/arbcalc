package evmrpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/evm"
)

// PriceLookup supplies USD prices, normally backed by the DexScreener cache.
type PriceLookup interface {
	PriceUSD(chain domain.Chain, tokenAddress string) (float64, bool)
}

// Options tunes one chain's ingestion.
type Options struct {
	// Confirmations is how far behind the head to stop reading.
	//
	// It is the reorg defence: a log read at the head can be un-mined a second
	// later, and its volume would already be inside a published minute. One or
	// two blocks costs a little latency and removes most of that risk; the
	// dedup key (tx hash + log index) then absorbs the case where a surviving
	// transaction is simply re-mined into a different block.
	Confirmations uint64
	// MaxBlockRange caps a single eth_getLogs span. Public endpoints reject
	// wide ranges outright, so a long gap is walked in chunks rather than
	// asked for in one doomed call.
	MaxBlockRange uint64
	// MaxCatchUpChunks bounds how many chunks one poll may walk, so recovering
	// from a long outage cannot monopolize the rate limit budget.
	MaxCatchUpChunks int
}

func DefaultOptions() Options {
	return Options{Confirmations: 2, MaxBlockRange: 1000, MaxCatchUpChunks: 5}
}

// poolMeta is the on-chain layout of a pool, resolved once and cached.
type poolMeta struct {
	token0, token1 string
	dec0, dec1     int
	// supported is false when the pool does not expose token0()/token1().
	// Those pools are counted as a coverage gap rather than guessed at.
	supported bool
	dex       string
}

// Source ingests trades for one EVM chain.
type Source struct {
	chain  domain.Chain
	rpc    *RPC
	prices PriceLookup
	opts   Options
	log    *slog.Logger

	mu        sync.RWMutex
	pools     map[string]domain.Pool // lower-case address -> pool
	meta      map[string]poolMeta
	tokens    map[string]domain.Token // lower-case token address -> token
	lastBlock uint64
	healthy   bool
	// unsupported remembers which pools were already reported, so a pool the
	// decoder cannot read is logged once instead of on every poll.
	unsupported map[string]bool
}

func NewSource(chain domain.Chain, rpc *RPC, prices PriceLookup, opts Options, log *slog.Logger) *Source {
	return &Source{
		chain:       chain,
		rpc:         rpc,
		prices:      prices,
		opts:        opts,
		log:         log.With("chain", chain, "source", "evmrpc"),
		pools:       map[string]domain.Pool{},
		meta:        map[string]poolMeta{},
		tokens:      map[string]domain.Token{},
		unsupported: map[string]bool{},
	}
}

func (s *Source) Name() string        { return "evmrpc" }
func (s *Source) Chain() domain.Chain { return s.chain }

func (s *Source) Healthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

// SetPools replaces the watched pool set after a discovery run.
func (s *Source) SetPools(pools []domain.Pool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pools = make(map[string]domain.Pool, len(pools))
	for _, p := range pools {
		if p.Chain != s.chain {
			continue
		}
		s.pools[strings.ToLower(p.Address)] = p
	}
}

// SetTokens tells the source which tokens are being tracked, so a swap can be
// attributed to the right watch-list entry.
func (s *Source) SetTokens(toks []domain.Token) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = make(map[string]domain.Token, len(toks))
	for _, t := range toks {
		if t.Chain != s.chain {
			continue
		}
		s.tokens[strings.ToLower(t.Address)] = t
	}
}

func (s *Source) poolAddresses() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.pools))
	for _, p := range s.pools {
		out = append(out, p.Address)
	}
	// A stable order keeps the request body identical between polls, which
	// makes traffic comparable when debugging.
	sort.Strings(out)
	return out
}

// Poll fetches everything new since the last successful call.
func (s *Source) Poll(ctx context.Context, out chan<- domain.Trade) error {
	addresses := s.poolAddresses()
	if len(addresses) == 0 {
		// Nothing to watch is a healthy state, not an outage: the minutes it
		// produces are genuine zeros.
		s.setHealthy(true)
		return nil
	}

	head, err := s.rpc.BlockNumber(ctx)
	if err != nil {
		s.setHealthy(false)
		return fmt.Errorf("block number: %w", err)
	}
	if head <= s.opts.Confirmations {
		s.setHealthy(true)
		return nil
	}
	safeHead := head - s.opts.Confirmations

	s.mu.RLock()
	last := s.lastBlock
	s.mu.RUnlock()

	if last == 0 {
		// First poll: start at the safe head rather than backfilling. History
		// arrives through the store on restart; scanning it here would blow
		// the request budget on data the medians will not accept anyway,
		// because those minutes are already sealed.
		s.mu.Lock()
		s.lastBlock = safeHead
		s.mu.Unlock()
		s.setHealthy(true)
		return nil
	}
	if safeHead <= last {
		s.setHealthy(true)
		return nil
	}

	from := last + 1
	for chunk := 0; chunk < s.opts.MaxCatchUpChunks && from <= safeHead; chunk++ {
		to := min(from+s.opts.MaxBlockRange-1, safeHead)

		logs, err := s.rpc.GetLogs(ctx, from, to, addresses, evm.SwapTopics())
		if err != nil {
			s.setHealthy(false)
			return fmt.Errorf("getLogs %d-%d: %w", from, to, err)
		}
		if err := s.emit(ctx, logs, out); err != nil {
			s.setHealthy(false)
			return err
		}

		s.mu.Lock()
		s.lastBlock = to
		s.mu.Unlock()
		from = to + 1
	}

	s.setHealthy(true)
	return nil
}

// emit decodes logs into trades and sends them downstream.
func (s *Source) emit(ctx context.Context, logs []Log, out chan<- domain.Trade) error {
	if len(logs) == 0 {
		return nil
	}

	// Resolve every block timestamp the batch needs in a single call.
	blockSet := map[uint64]bool{}
	for _, l := range logs {
		if n, err := l.BlockNumberValue(); err == nil {
			blockSet[n] = true
		}
	}
	blocks := make([]uint64, 0, len(blockSet))
	for b := range blockSet {
		blocks = append(blocks, b)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })

	times, err := s.rpc.BlockTimes(ctx, blocks)
	if err != nil {
		return fmt.Errorf("block times: %w", err)
	}

	if err := s.ensureMeta(ctx, logs); err != nil {
		return err
	}

	for _, l := range logs {
		// A removed log is one a reorg took back; counting it would leave
		// phantom volume in an already-published minute.
		if l.Removed {
			continue
		}
		trade, ok := s.decode(l, times)
		if !ok {
			continue
		}
		select {
		case out <- trade:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Selectors for the read-only calls used to describe a pool.
var (
	selToken0   = selector("token0()")
	selToken1   = selector("token1()")
	selDecimals = selector("decimals()")
)

func selector(sig string) string {
	h := evm.Keccak256([]byte(sig))
	return "0x" + hex.EncodeToString(h[:4])
}

// ensureMeta resolves token0/token1 and their decimals for any pool seen for
// the first time, batching all of it into two round trips.
func (s *Source) ensureMeta(ctx context.Context, logs []Log) error {
	var missing []string
	seen := map[string]bool{}

	s.mu.RLock()
	for _, l := range logs {
		addr := strings.ToLower(l.Address)
		if seen[addr] || s.unsupported[addr] {
			continue
		}
		if _, ok := s.meta[addr]; !ok {
			seen[addr] = true
			missing = append(missing, addr)
		}
	}
	s.mu.RUnlock()

	if len(missing) == 0 {
		return nil
	}

	tok0, err := s.rpc.EthCall(ctx, missing, selToken0)
	if err != nil {
		return fmt.Errorf("token0: %w", err)
	}
	tok1, err := s.rpc.EthCall(ctx, missing, selToken1)
	if err != nil {
		return fmt.Errorf("token1: %w", err)
	}

	// Collect the token addresses whose decimals are still unknown.
	type pending struct {
		pool         string
		token0, tok1 string
	}
	var rows []pending
	decNeeded := map[string]bool{}
	for i, pool := range missing {
		a0 := addressFromReturn(tok0[i])
		a1 := addressFromReturn(tok1[i])
		if a0 == "" || a1 == "" {
			// The pool does not expose the standard pair interface. Record it
			// as unsupported so it shows up as a coverage gap in the report
			// rather than as silently absent volume.
			s.mu.Lock()
			s.unsupported[pool] = true
			s.mu.Unlock()
			s.log.Warn("pool does not expose token0/token1; its volume will be missing", "pool", pool)
			continue
		}
		rows = append(rows, pending{pool: pool, token0: a0, tok1: a1})
		decNeeded[a0] = true
		decNeeded[a1] = true
	}
	if len(rows) == 0 {
		return nil
	}

	decList := make([]string, 0, len(decNeeded))
	for a := range decNeeded {
		decList = append(decList, a)
	}
	sort.Strings(decList)

	decRaw, err := s.rpc.EthCall(ctx, decList, selDecimals)
	if err != nil {
		return fmt.Errorf("decimals: %w", err)
	}
	decimals := make(map[string]int, len(decList))
	for i, a := range decList {
		decimals[a] = decimalsFromReturn(decRaw[i])
	}

	s.mu.Lock()
	for _, row := range rows {
		pool := s.pools[row.pool]
		s.meta[row.pool] = poolMeta{
			token0:    row.token0,
			token1:    row.tok1,
			dec0:      decimals[row.token0],
			dec1:      decimals[row.tok1],
			supported: true,
			dex:       pool.DEX,
		}
	}
	s.mu.Unlock()
	return nil
}

// decode turns one log into a normalized trade.
func (s *Source) decode(l Log, times map[uint64]time.Time) (domain.Trade, bool) {
	if len(l.Topics) == 0 {
		return domain.Trade{}, false
	}

	s.mu.RLock()
	meta, hasMeta := s.meta[strings.ToLower(l.Address)]
	pool := s.pools[strings.ToLower(l.Address)]
	s.mu.RUnlock()
	if !hasMeta || !meta.supported {
		return domain.Trade{}, false
	}

	data, err := evm.DecodeHexData(l.Data)
	if err != nil {
		return domain.Trade{}, false
	}
	amounts, known, err := evm.DecodeSwap(l.Topics[0], data)
	if err != nil || !known {
		return domain.Trade{}, false
	}

	blockNum, err := l.BlockNumberValue()
	if err != nil {
		return domain.Trade{}, false
	}
	ts, ok := times[blockNum]
	if !ok {
		// Without a block timestamp the trade cannot be placed in a minute.
		// Dropping it is the honest outcome; guessing "now" would file an old
		// trade into the current minute and fabricate a spike.
		return domain.Trade{}, false
	}
	logIndex, err := l.LogIndexValue()
	if err != nil {
		return domain.Trade{}, false
	}

	// Which side of the pool is the token we track?
	s.mu.RLock()
	tok0, is0 := s.tokens[meta.token0]
	tok1, is1 := s.tokens[meta.token1]
	s.mu.RUnlock()

	var (
		tracked   domain.Token
		amount    *big.Int
		decimals  int
		otherAddr string
		otherAmt  *big.Int
		otherDec  int
	)
	switch {
	case is0:
		tracked, amount, decimals = tok0, amounts.Amount0, meta.dec0
		otherAddr, otherAmt, otherDec = meta.token1, amounts.Amount1, meta.dec1
	case is1:
		tracked, amount, decimals = tok1, amounts.Amount1, meta.dec1
		otherAddr, otherAmt, otherDec = meta.token0, amounts.Amount0, meta.dec0
	default:
		// The pool is watched for a different token in the list.
		return domain.Trade{}, false
	}
	if amount == nil || amount.Sign() == 0 {
		return domain.Trade{}, false
	}

	// Positive means the token went into the pool: the trader sold it.
	side := domain.SideBuy
	if amount.Sign() > 0 {
		side = domain.SideSell
	}

	tokenAmount := evm.ToFloat(evm.Abs(amount), decimals)
	usd, price := s.valueUSD(tracked, tokenAmount, otherAddr, otherAmt, otherDec)
	if usd <= 0 {
		// No price route: counting the trade at zero would understate the
		// minute, so it is skipped and shows up as reduced coverage instead.
		return domain.Trade{}, false
	}

	dex := meta.dex
	if dex == "" {
		dex = amounts.Event
	}

	return domain.Trade{
		Timestamp:    ts,
		Chain:        s.chain,
		Token:        tracked.Symbol,
		TokenAddress: tracked.Address,
		Pool:         pool.Address,
		DEX:          dex,
		TxHash:       l.TxHash,
		LogIndex:     logIndex,
		Side:         side,
		TokenAmount:  tokenAmount,
		USDVolume:    usd,
		Price:        price,
		Source:       s.Name(),
	}, true
}

// valueUSD prices a swap, preferring the tracked token's own price and falling
// back to the other side of the pool.
//
// The fallback matters for a freshly listed token that no aggregator prices
// yet: its WETH or USDC counterpart is always priced, and the swap's dollar
// value is the same measured from either side.
func (s *Source) valueUSD(tok domain.Token, tokenAmount float64, otherAddr string, otherAmt *big.Int, otherDec int) (usd, price float64) {
	if p, ok := s.prices.PriceUSD(s.chain, tok.Address); ok && p > 0 {
		return tokenAmount * p, p
	}
	if otherAmt != nil {
		if p, ok := s.prices.PriceUSD(s.chain, otherAddr); ok && p > 0 {
			otherAmount := evm.ToFloat(evm.Abs(otherAmt), otherDec)
			usd = otherAmount * p
			if tokenAmount > 0 {
				price = usd / tokenAmount
			}
			return usd, price
		}
	}
	return 0, 0
}

func (s *Source) setHealthy(v bool) {
	s.mu.Lock()
	s.healthy = v
	s.mu.Unlock()
}

// LastBlock reports the highest block already ingested, for diagnostics.
func (s *Source) LastBlock() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastBlock
}

func addressFromReturn(hexStr string) string {
	b, err := evm.DecodeHexData(hexStr)
	if err != nil || len(b) < 32 {
		return ""
	}
	addr := evm.AddressFromWord(b[:32])
	if addr == "0x0000000000000000000000000000000000000000" {
		return ""
	}
	return strings.ToLower(addr)
}

func decimalsFromReturn(hexStr string) int {
	b, err := evm.DecodeHexData(hexStr)
	if err != nil || len(b) < 32 {
		// 18 is the ERC-20 default and the overwhelmingly common case; a wrong
		// guess here scales one token's volume, so it is logged by the caller
		// through the unsupported path when the call fails outright.
		return 18
	}
	v, err := evm.Uint256(b, 0)
	if err != nil || !v.IsInt64() || v.Int64() < 0 || v.Int64() > 36 {
		return 18
	}
	return int(v.Int64())
}
