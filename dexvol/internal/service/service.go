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
	log       *slog.Logger

	notifier Notifier
	trades   chan domain.Trade

	mu sync.RWMutex
	// lastSealed is the most recent minute that has been closed and judged.
	lastSealed time.Time
	// rediscover is poked when the watch list changes, so a token added from
	// Telegram starts collecting immediately instead of at the next refresh.
	rediscover chan struct{}
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
		static:     static,
		settings:   settings,
		db:         db,
		engine:     engine,
		discovery:  discovery,
		prices:     prices,
		alerts:     alerts,
		sources:    sources,
		health:     newHealthTracker(),
		log:        log.With("component", "service"),
		trades:     make(chan domain.Trade, tradeBuffer),
		rediscover: make(chan struct{}, 1),
	}
}

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
		s.discoveryLoop,
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
func (s *Service) sealDue(ctx context.Context, now time.Time) {
	deadlineMinute := now.Add(-s.static.SealDelay).Truncate(time.Minute)

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
	decision := s.alerts.Decide(tok.Key(), res, time.Now().UTC(), policy)
	if !decision.Send {
		s.log.Debug("alert suppressed",
			"token", tok.Key(), "reason", decision.Reason, "pct", res.Primary.Pct)
		return
	}
	if s.notifier == nil {
		return
	}

	msg := alert.Render(res)
	if err := s.notifier.Notify(ctx, msg); err != nil {
		s.log.Error("could not deliver alert", "token", tok.Key(), "err", err)
		return
	}
	s.log.Info("alert sent",
		"token", tok.Key(), "reason", decision.Reason,
		"window", res.Primary.Window, "pct", res.Primary.Pct, "volume", res.Volume)
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
	for chain, src := range s.sources {
		src.SetPools(res.ByChain[chain])
		src.SetTokens(rt.Tokens)
	}
	s.prices.TrackQuoteAssets(res.QuoteAssets)

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
func (s *Service) TokensChanged() {
	select {
	case s.rediscover <- struct{}{}:
	default:
	}
}
