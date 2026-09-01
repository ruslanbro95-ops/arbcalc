package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/detect"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/store"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

// TradeSource is one chain's ingestion path.
type TradeSource interface {
	Name() string
	Chain() domain.Chain
	SetPools(pools []domain.Pool)
	SetTokens(toks []domain.Token)
	Poll(ctx context.Context, out chan<- domain.Trade) error
	Healthy() bool
}

// Notifier delivers a rendered alert. The Telegram bot implements it.
type Notifier interface {
	Notify(ctx context.Context, m alert.Message) error
}

// tradeBuffer sizes the hand-off between ingestion and aggregation. It is
// generous because a burst of trades must never block a poll: a blocked poll
// would look like an outage and cost a whole minute of data.
const tradeBuffer = 4096

// alertBuffer sizes the hand-off between judging and delivering. Filling it
// means Telegram has been unreachable for a very long time.
const alertBuffer = 256

// Service runs the whole pipeline.
type Service struct {
	static    config.Static
	settings  *config.Store
	db        *store.Store
	engine    *volume.Engine
	discovery *Discovery
	prices    *PriceCache
	alerts    *alert.Manager
	sources   map[domain.Chain]TradeSource
	health    *healthTracker
	backfill  *Backfiller
	log       *slog.Logger

	notifier Notifier
	trades   chan domain.Trade
	outbox   chan outgoing

	mu sync.RWMutex
	// lastSealed is the most recent minute that has been closed and judged.
	lastSealed time.Time
	// rediscover is poked when the watch list changes, so a token added from
	// Telegram starts collecting immediately instead of at the next refresh.
	rediscover chan struct{}
	// poolsByToken is the latest discovery result per watch-list entry, which
	// the backfill loop needs to know which pools to pull history from.
	poolsByToken map[string][]domain.Pool
	// backfillDone records the tokens already attempted, so a chain with no
	// history provider is not retried every thirty seconds forever.
	backfillDone map[string]bool
}

func New(
	static config.Static,
	settings *config.Store,
	db *store.Store,
	engine *volume.Engine,
	discovery *Discovery,
	prices *PriceCache,
	alerts *alert.Manager,
	sources map[domain.Chain]TradeSource,
	log *slog.Logger,
) *Service {
	return &Service{
		static:       static,
		settings:     settings,
		db:           db,
		engine:       engine,
		discovery:    discovery,
		prices:       prices,
		alerts:       alerts,
		sources:      sources,
		health:       newHealthTracker(),
		log:          log.With("component", "service"),
		trades:       make(chan domain.Trade, tradeBuffer),
		outbox:       make(chan outgoing, alertBuffer),
		rediscover:   make(chan struct{}, 1),
		poolsByToken: map[string][]domain.Pool{},
		backfillDone: map[string]bool{},
	}
}

// SetBackfiller enables historical backfill. Without one the service still
// works, but every median has to warm up from live data — a full day before the
// 24h baseline can judge anything.
func (s *Service) SetBackfiller(b *Backfiller) { s.backfill = b }

// SetNotifier wires the alert channel. It is separate from New because the bot
// needs the service as its controller, and the service needs the bot as its
// notifier.
func (s *Service) SetNotifier(n Notifier) { s.notifier = n }

// Run starts every loop and blocks until ctx is cancelled.
func (s *Service) Run(ctx context.Context) error {
	// Prime pools and prices before the first poll, so the opening minutes are
	// real data rather than a warm-up gap.
	s.runDiscovery(ctx)
	if err := s.prices.Refresh(ctx, s.settings.Get().Tokens); err != nil {
		s.log.Warn("initial price refresh failed", "err", err)
	}

	var wg sync.WaitGroup
	loops := []func(context.Context){
		s.consumeTrades,
		s.pollLoop,
		s.priceLoop,
		s.sealLoop,
		s.deliverLoop,
		s.discoveryLoop,
		s.backfillLoop,
		s.maintenanceLoop,
	}
	for _, loop := range loops {
		wg.Add(1)
		go func(fn func(context.Context)) {
			defer wg.Done()
			fn(ctx)
		}(loop)
	}

	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

// consumeTrades is the single writer into the engine, which is what keeps
// dedup and aggregation free of cross-source races.
func (s *Service) consumeTrades(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case tr := <-s.trades:
			if !s.engine.Ingest(tr) {
				continue // duplicate, or a minute already sealed
			}
			if err := s.db.AppendTrade(tr); err != nil {
				s.log.Warn("could not persist raw trade", "err", err)
			}
		}
	}
}

// pollLoop asks every source for new trades on the configured interval.
func (s *Service) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(s.static.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pollAll(ctx)
		}
	}
}

// pollAll runs every chain concurrently: a slow or failing chain must not delay
// the others, since each one's health is judged per minute.
func (s *Service) pollAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, src := range s.sources {
		wg.Add(1)
		go func(src TradeSource) {
			defer wg.Done()
			err := src.Poll(ctx, s.trades)
			// Healthy() also covers the case of a source that returned no
			// error but knows it could not read everything, such as a Solana
			// poll that hit its request budget.
			ok := err == nil && src.Healthy()
			s.health.record(src.Chain(), time.Now().UTC(), ok)
			if err != nil && ctx.Err() == nil {
				s.log.Warn("poll failed", "chain", src.Chain(), "source", src.Name(), "err", err)
			}
		}(src)
	}
	wg.Wait()
}

func (s *Service) priceLoop(ctx context.Context) {
	ticker := time.NewTicker(s.static.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.prices.Refresh(ctx, s.settings.Get().Tokens); err != nil && ctx.Err() == nil {
				s.log.Warn("price refresh failed", "err", err)
			}
		}
	}
}

// sealLoop closes each calendar minute once the seal delay has passed and
// judges it.
func (s *Service) sealLoop(ctx context.Context) {
	// A one-second tick keeps the seal close to its deadline without spinning.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.sealDue(ctx, now.UTC())
		}
	}
}

// sealDue seals every minute that has fully elapsed plus the seal delay.
//
// It walks forward from the last sealed minute rather than sealing only the
// most recent one, so a pause — a laptop asleep, a long GC, a slow restart —
// closes the minutes it skipped instead of leaving holes that would silently
// shrink every median's sample set.
//
// The extra minute subtracted below is the whole point of the arithmetic:
// minute m spans [m, m+1min), so it is complete only at m+1min, and the grace
// period runs from there. Truncating `now - SealDelay` alone sealed m as early
// as m+SealDelay — a third of the way into a minute that was still filling —
// and every trade in its remaining forty seconds was then rejected as late.
// Worse than the lost volume, such a minute sealed QualityOK at $0 because the
// poll behind it had succeeded, so fake zeros entered the medians and the next
// ordinary minute read as a spike.
func (s *Service) sealDue(ctx context.Context, now time.Time) {
	deadlineMinute := now.Add(-s.static.SealDelay).Add(-time.Minute).Truncate(time.Minute)

	s.mu.RLock()
	last := s.lastSealed
	s.mu.RUnlock()

	if last.IsZero() {
		// Start with the minute now closing; earlier minutes were never
		// observed by this process.
		last = deadlineMinute.Add(-time.Minute)
	}

	rt := s.settings.Get()
	for m := last.Add(time.Minute); !m.After(deadlineMinute); m = m.Add(time.Minute) {
		s.sealMinute(ctx, rt, m)
		s.mu.Lock()
		s.lastSealed = m
		s.mu.Unlock()
	}
}

func (s *Service) sealMinute(ctx context.Context, rt config.Runtime, minute time.Time) {
	for _, tok := range rt.Tokens {
		if !tok.Enabled {
			continue
		}
		healthy := s.health.healthy(tok.Chain, minute)
		s.engine.Seal(tok.Key(), minute, healthy)

		snap := s.engine.Snapshot(tok, minute)
		if err := s.db.AppendMinute(store.MinuteRow{
			TokenKey: tok.Key(),
			Minute:   minute,
			Buy:      snap.Current.Buy,
			Sell:     snap.Current.Sell,
			Total:    snap.Current.Total,
			Trades:   snap.Current.Trades,
			Quality:  snap.Current.Quality,
		}); err != nil {
			s.log.Warn("could not persist minute", "token", tok.Key(), "err", err)
		}

		if !rt.Monitoring {
			// Collection continues while alerting is off, so turning it back
			// on does not start from an empty history.
			continue
		}
		s.judge(ctx, rt, tok, snap)
	}
}

// judge runs the detector and the alert policy for one sealed minute.
func (s *Service) judge(ctx context.Context, rt config.Runtime, tok domain.Token, snap volume.Snapshot) {
	det := detect.Detector{ThresholdPct: rt.ThresholdPct, Windows: rt.Windows}
	res := det.Evaluate(tok, snap)
	if !res.Anomalous {
		return
	}

	policy := alert.Policy{
		Cooldown:         time.Duration(rt.CooldownMinutes) * time.Minute,
		EscalationFactor: rt.EscalationFactor,
	}
	// The minute being judged, not the wall clock. Every rule the manager
	// applies is stated in market minutes — "cooldown 5 minutes", "one
	// continuing anomaly is one message" — and sealDue is built to walk a
	// backlog, judging skipped minutes back to back within the same
	// millisecond. Measured on the wall clock the cooldown would never elapse
	// during a catch-up, so a stall would silently collapse hours of separate
	// events into a single message, precisely when alerts matter most.
	decision := s.alerts.Decide(tok.Key(), res, snap.Minute, policy)
	if !decision.Send {
		s.log.Debug("alert suppressed",
			"token", tok.Key(), "reason", decision.Reason, "pct", res.Primary.Pct)
		return
	}
	if s.notifier == nil {
		return
	}

	// Hand off rather than send inline. Delivery used to happen here, inside
	// sealMinute, inside the one-second seal loop, against a client with a
	// 90-second HTTP timeout — so one slow send stalled minute sealing for as
	// long as it took, and a market-wide move alerting several tokens at once
	// stalled it for minutes. That backlog then fed straight back into the
	// alerting logic as a catch-up.
	out := outgoing{token: tok.Key(), msg: alert.Render(res, decision), decision: decision, primary: res.Primary, volume: res.Volume}
	select {
	case s.outbox <- out:
	default:
		// The queue is deep enough that filling it means Telegram has been
		// unreachable for a long time. Dropping one alert is better than
		// stalling data collection for every token.
		s.log.Error("alert dropped: delivery queue is full",
			"token", tok.Key(), "queued", len(s.outbox))
	}
}

// outgoing is one rendered alert waiting to be delivered.
type outgoing struct {
	token    string
	msg      alert.Message
	decision alert.Decision
	primary  detect.Change
	volume   float64
}

// deliverLoop is the only place that talks to the notifier, so a slow or
// unreachable Telegram costs delivery latency and nothing else.
func (s *Service) deliverLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case out := <-s.outbox:
			if err := s.notifier.Notify(ctx, out.msg); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Error("could not deliver alert", "token", out.token, "err", err)
				continue
			}
			s.log.Info("alert sent",
				"token", out.token, "reason", out.decision.Reason,
				"window", out.primary.Window, "pct", out.primary.Pct, "volume", out.volume)
		}
	}
}

// discoveryLoop refreshes the pool set on a timer and on demand.
func (s *Service) discoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(s.static.PoolRefresh)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runDiscovery(ctx)
		case <-s.rediscover:
			s.runDiscovery(ctx)
		}
	}
}

// runDiscovery refreshes pools and pushes them into every source.
//
// It runs repeatedly rather than once per token, because a token can list on a
// new DEX at any time; a pool set frozen at add-time is the most likely way to
// lose coverage without noticing.
func (s *Service) runDiscovery(ctx context.Context) {
	rt := s.settings.Get()
	if len(rt.Tokens) == 0 {
		return
	}

	res := s.discovery.Run(ctx, rt.Tokens)

	tracked := map[domain.Chain]int{}
	for _, t := range rt.Tokens {
		if t.Enabled {
			tracked[t.Chain]++
		}
	}

	for chain, src := range s.sources {
		src.SetTokens(rt.Tokens)

		// A pass that came back empty for a chain we are tracking is far more
		// likely to be both aggregators failing than every pool vanishing at
		// once. Pushing it would erase a working pool set on a transient 429,
		// so the previous one is kept and the source reports itself uncovered
		// until discovery succeeds again.
		if len(res.ByChain[chain]) == 0 && tracked[chain] > 0 {
			s.log.Warn("discovery returned no pools; keeping the previous set",
				"chain", chain, "tracked_tokens", tracked[chain])
			continue
		}
		src.SetPools(res.ByChain[chain])
	}
	s.prices.TrackQuoteAssets(res.QuoteAssets)

	s.mu.Lock()
	s.poolsByToken = res.ByToken
	s.mu.Unlock()

	for provider, pools := range res.ExclusiveTo {
		s.log.Info("pools found by only one provider",
			"provider", provider, "count", len(pools))
	}

	// Fill in symbols for tokens the owner added by address alone.
	if len(res.Symbols) > 0 {
		err := s.settings.Update(func(rt *config.Runtime) {
			for i := range rt.Tokens {
				if rt.Tokens[i].Symbol == "" {
					if sym, ok := res.Symbols[rt.Tokens[i].Key()]; ok {
						rt.Tokens[i].Symbol = sym
					}
				}
			}
		})
		if err != nil {
			s.log.Warn("could not persist resolved symbols", "err", err)
		}
	}

	total := 0
	for _, pools := range res.ByChain {
		total += len(pools)
	}
	s.log.Info("pool discovery complete", "tokens", len(rt.Tokens), "pools", total)
}

// backfillLoop fills each token's recent history, one token at a time.
//
// It runs apart from discovery so a slow fetch cannot stall pool refresh: a
// full day of history for a token with a dozen pools is a couple of dozen
// rate-limited requests, and serializing them keeps the provider's free tier
// intact while the live pipeline keeps running underneath.
func (s *Service) backfillLoop(ctx context.Context) {
	if s.backfill == nil {
		return
	}
	// A short tick: the first pass should land within seconds of startup,
	// because until it does the 24h baseline cannot judge anything.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.backfillNext(ctx)
		}
	}
}

// backfillNext fills at most one token per tick.
func (s *Service) backfillNext(ctx context.Context) {
	now := time.Now().UTC()

	for _, tok := range s.settings.Get().Tokens {
		if !tok.Enabled {
			continue
		}
		s.mu.RLock()
		done := s.backfillDone[tok.Key()]
		pools := s.poolsByToken[tok.Key()]
		s.mu.RUnlock()

		if done || len(pools) == 0 || !s.backfill.Needed(tok, now) {
			continue
		}

		rep := s.backfill.Run(ctx, tok, pools, now)
		if ctx.Err() != nil {
			return
		}

		s.mu.Lock()
		// Marked done either way: a token whose chain has no history provider,
		// or whose pool coverage is too thin to trust, must not be retried on
		// every tick for the life of the process.
		s.backfillDone[tok.Key()] = true
		s.mu.Unlock()

		if rep.Filled {
			s.log.Info("history backfilled",
				"token", tok.Key(), "minutes", rep.Minutes, "active_minutes", rep.Active,
				"pools", rep.PoolsUsed, "volume_share", rep.VolumeShare)
		} else {
			s.log.Warn("history backfill skipped; medians will warm up from live data",
				"token", tok.Key(), "reason", rep.Reason)
		}
		return // one token per tick
	}
}

// maintenanceLoop trims persisted data and stale health records.
func (s *Service) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			if n, err := s.db.PruneTrades(s.static.RawTradeRetention, now); err != nil {
				s.log.Warn("raw trade pruning failed", "err", err)
			} else if n > 0 {
				s.log.Info("pruned raw trade files", "files", n)
			}
			if err := s.db.CompactMinutes(now.Add(-25 * time.Hour)); err != nil {
				s.log.Warn("minute log compaction failed", "err", err)
			}
			s.health.prune(now.Add(-2 * time.Hour))
		}
	}
}

// --- telegram.Controller ---

// Snapshot returns the latest sealed minute for a token.
func (s *Service) Snapshot(tok domain.Token) (volume.Snapshot, bool) {
	s.mu.RLock()
	last := s.lastSealed
	s.mu.RUnlock()
	if last.IsZero() {
		return volume.Snapshot{}, false
	}
	return s.engine.Snapshot(tok, last), true
}

func (s *Service) Stats() volume.Stats { return s.engine.Stats() }

// TokensChanged asks for a discovery pass without blocking the caller. The
// buffered channel means a burst of edits collapses into one refresh.
//
// It also forgets which tokens were backfilled, so a token removed and re-added
// — or one whose earlier attempt failed because discovery had not found its
// pools yet — gets another try.
func (s *Service) TokensChanged() {
	s.mu.Lock()
	s.backfillDone = map[string]bool{}
	s.mu.Unlock()

	select {
	case s.rediscover <- struct{}{}:
	default:
	}
}
