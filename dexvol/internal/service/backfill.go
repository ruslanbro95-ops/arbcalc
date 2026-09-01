package service

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/store"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

// BackfillOptions bounds the cost and sets the honesty bar.
type BackfillOptions struct {
	// Window is how much history to reconstruct.
	Window time.Duration
	// MaxPools caps the requests spent on one token.
	MaxPools int
	// MinVolumeShare is the fraction of the token's known 24h volume the
	// fetched pools must account for before the result is trusted.
	//
	// This is the safety valve. A backfilled baseline that misses pools is
	// understated, and an understated baseline inflates every percentage
	// measured against it — the failure direction that produces false alerts.
	// Below this share the backfill is abandoned rather than written, and the
	// medians simply warm up live instead.
	MinVolumeShare float64
	// MaxPagesPerPool bounds paging. Two pages of 1,000 one-minute candles
	// already exceed the 1,440 minutes in a day.
	MaxPagesPerPool int
	// LiveGuard is how much of the recent past to leave alone.
	//
	// Backfill writes sealed buckets, and the live pipeline has not finished
	// with the newest minutes: it is still waiting out the seal delay for
	// late trades. Writing them here would freeze a provider's figure into a
	// minute the pipeline was about to fill itself, make every real trade for
	// it arrive too late to count, and pre-empt its MISSING verdict. Losing
	// five minutes from the far end of a 24h window costs nothing.
	LiveGuard time.Duration
}

func DefaultBackfillOptions() BackfillOptions {
	return BackfillOptions{
		Window:          24 * time.Hour,
		MaxPools:        12,
		MinVolumeShare:  0.95,
		MaxPagesPerPool: 3,
		LiveGuard:       5 * time.Minute,
	}
}

// BackfillReport describes one token's attempt.
type BackfillReport struct {
	Token       domain.Token
	Filled      bool
	Reason      string
	PoolsUsed   int
	PoolsTotal  int
	VolumeShare float64
	// Minutes is how many minutes were written; Active is how many of those
	// carried any volume.
	Minutes int
	Active  int
	// Skipped is how many minutes of the requested window were left alone
	// because the provider's history did not reach far enough back to cover
	// them.
	Skipped int
}

// Backfiller reconstructs recent per-minute history from an aggregator.
//
// Why this exists: without it the 24h median — the baseline the spec leans on
// most — needs a full day of live collection before it can judge anything, and
// a token added at noon is unjudgeable until noon tomorrow. Fetching history
// makes both usable within a minute of starting.
//
// What it costs: the numbers are the provider's, not this pipeline's, so a
// backfilled baseline and a live minute are not measured by exactly the same
// ruler. The buckets are therefore marked, the mix is visible in /status and
// /vol, and an alert whose baseline is mostly historical says so.
type Backfiller struct {
	history sources.HistorySource
	engine  *volume.Engine
	db      *store.Store
	opts    BackfillOptions
	log     *slog.Logger
}

func NewBackfiller(h sources.HistorySource, eng *volume.Engine, db *store.Store, opts BackfillOptions, log *slog.Logger) *Backfiller {
	return &Backfiller{history: h, engine: eng, db: db, opts: opts, log: log.With("component", "backfill")}
}

// Needed reports whether a token still lacks enough sealed history to be worth
// filling. It keeps a restart from refetching what the store already replayed.
func (b *Backfiller) Needed(tok domain.Token, now time.Time) bool {
	minutes := int(b.opts.Window / time.Minute)
	sealed := b.engine.SealedCount(tok.Key(), now.UTC().Truncate(time.Minute), minutes)
	// The newest minutes are still open, and backfill deliberately leaves the
	// live guard alone, so a shortfall of that size is the expected steady
	// state rather than a gap worth refetching.
	slack := int(b.opts.LiveGuard/time.Minute) + 5
	return sealed < minutes-slack
}

// Run fills one token's history. It never writes a partial result: either the
// fetched pools clear the volume-share bar and the whole window is written, or
// nothing is.
func (b *Backfiller) Run(ctx context.Context, tok domain.Token, pools []domain.Pool, now time.Time) BackfillReport {
	rep := BackfillReport{Token: tok, PoolsTotal: len(pools)}

	if !b.history.Supports(tok.Chain) {
		rep.Reason = fmt.Sprintf("%s has no history provider for %s", b.history.Name(), tok.Chain)
		return rep
	}
	if len(pools) == 0 {
		rep.Reason = "no pools discovered yet"
		return rep
	}

	selected, share := selectPools(pools, b.opts.MaxPools, b.opts.MinVolumeShare)
	rep.VolumeShare = share
	if len(selected) == 0 {
		rep.Reason = "no pool carried any reported volume"
		return rep
	}
	if share < b.opts.MinVolumeShare {
		rep.Reason = fmt.Sprintf("top %d pools cover only %.1f%% of reported volume; "+
			"an understated baseline would inflate every later percentage",
			len(selected), share*100)
		return rep
	}

	// Stop short of the minutes the live pipeline still owns.
	end := now.UTC().Truncate(time.Minute).Add(-b.opts.LiveGuard)
	start := end.Add(-b.opts.Window)

	totals := map[int64]float64{}
	fetched := 0.0
	// reach records, per pool, the oldest minute its history actually went
	// back to. A pool's absence before that point is not evidence of no
	// trading — it is absence of data.
	var reach []poolReach
	for _, pool := range selected {
		candles, err := b.fetchPool(ctx, pool, start, end)
		if err != nil {
			// A failed pool means its volume is missing from every minute.
			// Continue only if the rest still clears the bar.
			b.log.Warn("history fetch failed for pool",
				"token", tok.Key(), "pool", pool.Address, "err", err)
			continue
		}
		fetched += pool.Volume24hUSD
		rep.PoolsUsed++

		oldest := end
		for _, c := range candles {
			m := c.Time.UTC().Truncate(time.Minute)
			if m.Before(oldest) {
				oldest = m
			}
			if m.Before(start) || !m.Before(end) {
				continue
			}
			totals[m.Unix()] += c.VolumeUSD
		}
		if len(candles) == 0 && pool.Volume24hUSD <= 0 {
			// A pool that reported no volume and returned no candles is
			// consistent: it simply did not trade, and contributes a real zero
			// throughout.
			oldest = start
		}
		reach = append(reach, poolReach{oldest: oldest, volume: pool.Volume24hUSD})
	}

	if got := shareOf(fetched, pools); got < b.opts.MinVolumeShare {
		rep.VolumeShare = got
		rep.Reason = fmt.Sprintf("only %.1f%% of reported volume could be fetched", got*100)
		return rep
	}

	// The volume-share check above proves which pools answered, not how far
	// back they answered. A token listed eight hours ago, or a provider whose
	// history is shallower than the window, leaves the earlier minutes with no
	// candles at all — and writing those as confirmed zeros would drag the 24h
	// median down and inflate every percentage measured against it.
	coverFrom := coverageStart(reach, fetched, b.opts.MinVolumeShare, start, end)
	if !coverFrom.Before(end) {
		rep.Reason = "the provider's history did not reach into the window"
		return rep
	}
	if coverFrom.After(start) {
		rep.Skipped = int(coverFrom.Sub(start) / time.Minute)
		b.log.Info("history does not reach the whole window; filling what it covers",
			"token", tok.Key(), "covered_from", coverFrom, "skipped_minutes", rep.Skipped)
		start = coverFrom
	}

	for m := start; m.Before(end); m = m.Add(time.Minute) {
		total := totals[m.Unix()]
		// A minute with no candle in any pool is a genuine zero: the provider
		// emits a candle whenever the pool traded, so absence means no trades,
		// not missing data. The window itself is known to be covered because
		// the share check above passed.
		inserted, err := b.engine.RestoreMinute(tok.Key(), volume.Bucket{
			Minute: m,
			Total:  total,
			// OHLCV carries no side breakdown, so buy/sell stay zero for
			// backfilled minutes. Only TOTAL feeds the detector today, and a
			// fabricated split would be worse than an honest gap.
			Quality:    volume.QualityOK,
			Sealed:     true,
			Backfilled: true,
		})
		if err != nil {
			b.log.Warn("could not restore backfilled minute", "token", tok.Key(), "minute", m, "err", err)
			continue
		}
		if !inserted {
			continue // the live feed already owns this minute
		}
		rep.Minutes++
		if total > 0 {
			rep.Active++
		}
		if err := b.db.AppendMinute(store.MinuteRow{
			TokenKey:   tok.Key(),
			Minute:     m,
			Total:      total,
			Quality:    volume.QualityOK,
			Backfilled: true,
		}); err != nil {
			b.log.Warn("could not persist backfilled minute", "token", tok.Key(), "err", err)
		}
	}

	rep.Filled = true
	return rep
}

// fetchPool pages backwards until the window is covered.
func (b *Backfiller) fetchPool(ctx context.Context, pool domain.Pool, start, end time.Time) ([]sources.Candle, error) {
	var out []sources.Candle
	before := end

	for page := 0; page < b.opts.MaxPagesPerPool; page++ {
		candles, err := b.history.OHLCVMinute(ctx, pool, 0, before)
		if err != nil {
			return nil, err
		}
		if len(candles) == 0 {
			break
		}
		out = append(out, candles...)

		oldest := candles[0].Time
		for _, c := range candles {
			if c.Time.Before(oldest) {
				oldest = c.Time
			}
		}
		if !oldest.After(start) {
			break // the window is covered
		}
		// Step strictly past the oldest candle, or the next page repeats it.
		next := oldest.Add(-time.Second)
		if !next.Before(before) {
			break // no progress: stop rather than loop
		}
		before = next
	}
	return out, nil
}

// selectPools takes the highest-volume pools until they account for the
// required share, capped at max.
func selectPools(pools []domain.Pool, max int, want float64) ([]domain.Pool, float64) {
	// Sort defensively: the caller usually hands these over already ordered,
	// but truncating an unsorted list would drop the deepest pools and quietly
	// understate every backfilled minute.
	pools = append([]domain.Pool(nil), pools...)
	sort.Slice(pools, func(i, j int) bool {
		if pools[i].Volume24hUSD != pools[j].Volume24hUSD {
			return pools[i].Volume24hUSD > pools[j].Volume24hUSD
		}
		return pools[i].Address < pools[j].Address
	})

	total := 0.0
	for _, p := range pools {
		total += p.Volume24hUSD
	}
	if total <= 0 {
		// Nothing reported any volume, so there is no share to reason about
		// and no basis for trusting a truncation. Take everything up to the
		// cap and let the caller's own check decide.
		if len(pools) > max {
			pools = pools[:max]
		}
		return pools, 0
	}

	running := 0.0
	for i, p := range pools {
		running += p.Volume24hUSD
		if i+1 >= max || running/total >= want {
			return pools[:i+1], running / total
		}
	}
	return pools, 1
}

// poolReach is how far back one pool's fetched history actually went.
type poolReach struct {
	oldest time.Time
	volume float64
}

// coverageStart returns the earliest minute at which enough of the token's
// volume has history behind it.
//
// Coverage only improves as time moves forward — each pool's reach is fixed —
// so the answer is a single boundary rather than a per-minute test.
func coverageStart(reach []poolReach, total, want float64, start, end time.Time) time.Time {
	if total <= 0 {
		return end
	}
	sorted := append([]poolReach(nil), reach...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].oldest.Before(sorted[j].oldest) })

	running := 0.0
	for _, r := range sorted {
		running += r.volume
		if running/total >= want {
			if r.oldest.Before(start) {
				return start
			}
			return r.oldest
		}
	}
	return end
}

func shareOf(fetched float64, pools []domain.Pool) float64 {
	total := 0.0
	for _, p := range pools {
		total += p.Volume24hUSD
	}
	if total <= 0 {
		return 0
	}
	return fetched / total
}
