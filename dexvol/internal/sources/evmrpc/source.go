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
	"sync/atomic"
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
	// MaxAddressesPerCall caps how many pool addresses go into one
	// eth_getLogs filter.
	//
	// Reading a whole chain in one request is the arithmetic this design
	// rests on, and it still happens wherever the endpoint allows it. The
	// public ones do not: publicnode answers `-32602 "Request blocked"` the
	// moment the address array holds ten entries, on ethereum, bsc and base
	// alike. The failure is total, not partial — no logs come back at all —
	// so a token with ten pools would put its whole chain into an unbroken
	// run of MISSING minutes. Nine is that endpoint's measured limit.
	//
	// The cost is one request per nine pools instead of one per chain. At
	// 40 pools on four chains and four polls a minute that is 80 requests a
	// minute rather than 16 — still far inside the budget in §5 of
	// docs/RESEARCH.md. Raise it when RPC_* points at your own node.
	MaxAddressesPerCall int
}

func DefaultOptions() Options {
	return Options{Confirmations: 2, MaxBlockRange: 1000, MaxCatchUpChunks: 5, MaxAddressesPerCall: 9}
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
	// skipped holds discovered pools whose identifier is not a contract
	// address — Uniswap V4 pool ids, mostly. They are a known coverage gap,
	// kept so the gap can be reported instead of vanishing.
	skipped []domain.Pool
	// drops counts decoded logs that never became a trade, by reason.
	//
	// A coverage number cannot tell "the pipeline saw nothing" from "the
	// pipeline saw it and threw it away", and those need opposite responses.
	// The spec asks the coverage report to account for missing volume rather
	// than let it vanish, so every silent `return false` in decode is counted
	// here and printed by cmd/coverage.
	drops struct {
		unsupportedPool atomic.Int64
		undecodable     atomic.Int64
		noBlockTime     atomic.Int64
		otherToken      atomic.Int64
		zeroAmount      atomic.Int64
		unpriced        atomic.Int64
	}
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
//
// Identifiers that are not contract addresses are dropped here, and that is
// not tidiness. eth_getLogs rejects the entire filter when one entry of its
// address array is not 20 bytes, so a single Uniswap V4 pool id — 32 bytes,
// and routinely listed by DEX Screener — would fail every poll on the chain
// and take every token on it down as an unbroken run of MISSING minutes.
// Dropping that one pool costs its volume; keeping it costs the chain.
func (s *Source) SetPools(pools []domain.Pool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pools = make(map[string]domain.Pool, len(pools))
	s.skipped = nil
	for _, p := range pools {
		if p.Chain != s.chain {
			continue
		}
		if !PollableAddress(p.Address) {
			s.skipped = append(s.skipped, p)
			continue
		}
		s.pools[strings.ToLower(p.Address)] = p
	}
	if len(s.skipped) > 0 {
		sort.Slice(s.skipped, func(i, j int) bool {
			return s.skipped[i].Volume24hUSD > s.skipped[j].Volume24hUSD
		})
		s.log.Warn("pools dropped: identifier is not a contract address",
			"count", len(s.skipped), "largest", s.skipped[0].Address,
			"largest_volume_24h_usd", s.skipped[0].Volume24hUSD)
	}
}

// Skipped lists the discovered pools this source cannot poll, largest first.
//
// cmd/coverage reports them with their 24h volume so the gap they leave is a
// number in the coverage table rather than an unexplained shortfall.
func (s *Source) Skipped() []domain.Pool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Pool, len(s.skipped))
	copy(out, s.skipped)
	return out
}

// PollableAddress reports whether an identifier is a contract address, the
// only thing an eth_getLogs address filter accepts: 0x and 40 hex digits.
func PollableAddress(id string) bool {
	if len(id) != 42 || !strings.EqualFold(id[:2], "0x") {
		return false
	}
	_, err := hex.DecodeString(id[2:])
	return err == nil
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

// getLogs reads one block range across every watched pool, splitting the
// address filter into as many requests as the endpoint will accept.
//
// Chunks cover disjoint pools, so concatenating their results loses nothing;
// the engine buckets by block timestamp and dedup keys on tx hash plus log
// index, neither of which depends on the order logs arrive in.
func (s *Source) getLogs(ctx context.Context, from, to uint64, addresses, topics []string) ([]Log, error) {
	size := s.opts.MaxAddressesPerCall
	if size <= 0 || size > len(addresses) {
		size = len(addresses)
	}
	var out []Log
	for start := 0; start < len(addresses); start += size {
		logs, err := s.rpc.GetLogs(ctx, from, to, addresses[start:min(start+size, len(addresses))], topics)
		if err != nil {
			return nil, err
		}
		out = append(out, logs...)
	}
	return out, nil
}

// Poll fetches everything new since the last successful call.
func (s *Source) Poll(ctx context.Context, out chan<- domain.Trade) error {
	addresses := s.poolAddresses()
	if len(addresses) == 0 {
		// Having nothing to watch is only a healthy state when there is also
		// nothing being tracked. With tokens on the watch list but no pools
		// known for them, this chain is simply not covered — and reporting
		// health would seal those minutes as confirmed zeros, drag every
		// median down, and make the first minute after pools return read as a
		// spike. Discovery failing transiently is exactly how that happens.
		s.setHealthy(!s.tracksTokens())
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

		logs, err := s.getLogs(ctx, from, to, addresses, evm.SwapTopics())
		if err != nil {
			s.setHealthy(false)
			return fmt.Errorf("getLogs %d-%d: %w", from, to, err)
		}
		s.log.Debug("read blocks", "from", from, "to", to, "pools", len(addresses), "logs", len(logs))
		if err := s.emit(ctx, logs, out); err != nil {
			s.setHealthy(false)
			return err
		}

		s.mu.Lock()
		s.lastBlock = to
		s.mu.Unlock()
		from = to + 1
	}

	// Caught up only if the walk actually reached the safe head. Bounded
	// catch-up means a long outage takes several polls to drain, and during
	// them the trades being emitted carry old block timestamps that the engine
	// rejects as too late. Claiming health there would seal the current
	// minutes as real zeros while the backlog is still being read.
	s.setHealthy(from > safeHead)
	return nil
}

// tracksTokens reports whether the watch list holds anything on this chain.
func (s *Source) tracksTokens() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tokens) > 0
}

// emit decodes logs into trades and sends them downstream.
func (s *Source) emit(ctx context.Context, logs []Log, out chan<- domain.Trade) error {
	if len(logs) == 0 {
		return nil
	}

	// Block times come from the logs themselves wherever the node sends them,
	// and only the leftovers are fetched. On the endpoints this project
	// defaults to that is all of them, which turns a per-block request into
	// nothing at all — the difference between Robinhood Chain being usable
	// and rate limiting us into silence.
	times := map[uint64]time.Time{}
	var missing []uint64
	seen := map[uint64]bool{}
	for _, l := range logs {
		n, err := l.BlockNumberValue()
		if err != nil {
			continue
		}
		if _, have := times[n]; have {
			continue
		}
		if ts, ok := l.BlockTimestampValue(); ok {
			times[n] = ts
			continue
		}
		if !seen[n] {
			seen[n] = true
			missing = append(missing, n)
		}
	}

	s.log.Debug("block times", "logs", len(logs), "inline", len(times), "missing", len(missing))
	if len(missing) > 0 {
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		fetched, err := s.rpc.BlockTimes(ctx, missing)
		if err != nil {
			return fmt.Errorf("block times: %w", err)
		}
		for n, ts := range fetched {
			times[n] = ts
		}
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
		s.drops.undecodable.Add(1)
		return domain.Trade{}, false
	}

	s.mu.RLock()
	meta, hasMeta := s.meta[strings.ToLower(l.Address)]
	pool := s.pools[strings.ToLower(l.Address)]
	s.mu.RUnlock()
	if !hasMeta || !meta.supported {
		s.drops.unsupportedPool.Add(1)
		return domain.Trade{}, false
	}

	data, err := evm.DecodeHexData(l.Data)
	if err != nil {
		s.drops.undecodable.Add(1)
		return domain.Trade{}, false
	}
	amounts, known, err := evm.DecodeSwap(l.Topics[0], data)
	if err != nil || !known {
		s.drops.undecodable.Add(1)
		return domain.Trade{}, false
	}

	blockNum, err := l.BlockNumberValue()
	if err != nil {
		s.drops.undecodable.Add(1)
		return domain.Trade{}, false
	}
	ts, ok := times[blockNum]
	if !ok {
		// Without a block timestamp the trade cannot be placed in a minute.
		// Dropping it is the honest outcome; guessing "now" would file an old
		// trade into the current minute and fabricate a spike.
		s.drops.noBlockTime.Add(1)
		return domain.Trade{}, false
	}
	logIndex, err := l.LogIndexValue()
	if err != nil {
		s.drops.undecodable.Add(1)
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
		s.drops.otherToken.Add(1)
		return domain.Trade{}, false
	}
	if amount == nil || amount.Sign() == 0 {
		s.drops.zeroAmount.Add(1)
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
		s.drops.unpriced.Add(1)
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

// valueUSD prices one swap, reading the counterparty leg first.
//
// A swap is an exchange, so it can be valued from either side, and the sides
// are not equally trustworthy. The counterparty is USDT, USDC, WETH, WBNB or
// SOL — a handful of assets per chain, always deeply liquid, always priced by
// every source, and for a stablecoin the swap's own amount is the dollar
// figure with no quote needed at all. The tracked token is the opposite: it is
// often new, thin, or listed minutes ago, which is exactly when an aggregator
// has no price for it — and exactly when a volume spike matters most.
//
// Taking the token's own price first also made the whole watch list depend on
// per-token price lookups, which is what the provider's batch endpoint was
// quietly failing to deliver. Reading the counterparty needs about five prices
// per chain instead of one per token.
func (s *Source) valueUSD(tok domain.Token, tokenAmount float64, otherAddr string, otherAmt *big.Int, otherDec int) (usd, price float64) {
	if otherAmt != nil && otherAmt.Sign() != 0 {
		if p, ok := s.prices.PriceUSD(s.chain, otherAddr); ok && p > 0 {
			usd = evm.ToFloat(evm.Abs(otherAmt), otherDec) * p
			if tokenAmount > 0 {
				price = usd / tokenAmount
			}
			return usd, price
		}
	}
	// No price for the counterparty either — an exotic pair, or a pool whose
	// both sides are unlisted. The token's own quote is the last resort.
	if p, ok := s.prices.PriceUSD(s.chain, tok.Address); ok && p > 0 {
		return tokenAmount * p, p
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

// Drops is a snapshot of the decode-stage losses, by reason.
type Drops struct {
	// UnsupportedPool is a pool whose token0()/token1() could not be read.
	UnsupportedPool int64
	// Undecodable is a log whose Swap layout this decoder does not know —
	// Uniswap V4 among them.
	Undecodable int64
	// NoBlockTime is a log whose block timestamp did not resolve, so it could
	// not be placed in a minute.
	NoBlockTime int64
	// OtherToken is a swap in a watched pool that moved a token we do not
	// track. Not a loss: it is the pool's other side.
	OtherToken int64
	// ZeroAmount is a swap that moved none of the tracked token.
	ZeroAmount int64
	// Unpriced is a swap with no route to a USD value. This is the one that
	// costs real coverage.
	Unpriced int64
}

// Drops reports what decode discarded and why.
func (s *Source) Drops() Drops {
	return Drops{
		UnsupportedPool: s.drops.unsupportedPool.Load(),
		Undecodable:     s.drops.undecodable.Load(),
		NoBlockTime:     s.drops.noBlockTime.Load(),
		OtherToken:      s.drops.otherToken.Load(),
		ZeroAmount:      s.drops.zeroAmount.Load(),
		Unpriced:        s.drops.unpriced.Load(),
	}
}

// AddressCap picks the eth_getLogs address-filter size for one chain: the
// override when the operator set one, otherwise the limit the chain's endpoint
// is known to have, otherwise no cap.
//
// It lives here so cmd/monitor and cmd/coverage cannot drift apart on it —
// they did once already, on the seal deadline, and it cost a measurement that
// read $0 against a chain that was trading.
func AddressCap(chain domain.Chain, override int) int {
	if override > 0 {
		return override
	}
	if info, ok := domain.Info(chain); ok {
		return info.MaxLogAddresses
	}
	return 0
}
