package service

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/store"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

type stubHistory struct {
	// perPool maps a pool address to the volume it reports in every minute.
	perPool   map[string]float64
	failPools map[string]bool
	// depth is how many minutes before origin the provider's history for a
	// pool begins; absent means unlimited. It stands in for a pool created
	// recently, or a provider whose data is shallower than the window asked
	// for. It is an absolute boundary, not a page size — paging past it must
	// return nothing rather than inventing more history.
	depth       map[string]int
	origin      time.Time
	calls       int
	unsupported bool
}

// stubPageSize mirrors the real endpoint's maximum page.
const stubPageSize = 1000

func (s *stubHistory) Name() string { return "stub-history" }

func (s *stubHistory) Supports(domain.Chain) bool { return !s.unsupported }

func (s *stubHistory) OHLCVMinute(_ context.Context, pool domain.Pool, limit int, before time.Time) ([]sources.Candle, error) {
	s.calls++
	if s.failPools[pool.Address] {
		return nil, errors.New("provider error")
	}
	vol, ok := s.perPool[pool.Address]
	if !ok {
		return nil, nil
	}
	var earliest time.Time
	if d, limited := s.depth[pool.Address]; limited {
		earliest = s.origin.UTC().Truncate(time.Minute).Add(-time.Duration(d) * time.Minute)
	}

	out := make([]sources.Candle, 0, stubPageSize)
	end := before.UTC().Truncate(time.Minute)
	for i := 1; i <= stubPageSize; i++ {
		at := end.Add(-time.Duration(i) * time.Minute)
		if !earliest.IsZero() && at.Before(earliest) {
			break // the provider simply has nothing older
		}
		out = append(out, sources.Candle{Time: at, VolumeUSD: vol, Close: 1})
	}
	return out, nil
}

func newBackfiller(t *testing.T, h sources.HistorySource) (*Backfiller, *volume.Engine, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	db, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_ = filepath.Join

	eng := volume.NewEngine()
	return NewBackfiller(h, eng, db, DefaultBackfillOptions(), discardLog()), eng, db
}

var bfToken = domain.Token{Symbol: "ABC", Chain: domain.ChainBase, Address: "0xAA", Enabled: true}

func poolsFor(vols ...float64) []domain.Pool {
	out := make([]domain.Pool, len(vols))
	for i, v := range vols {
		out[i] = domain.Pool{
			Chain: domain.ChainBase, Address: string(rune('A' + i)), Volume24hUSD: v,
		}
	}
	return out
}

func TestBackfillMakesBaselinesUsableImmediately(t *testing.T) {
	// The whole point: a service that just started must be able to judge a
	// minute against a 24h median instead of waiting a day for one.
	h := &stubHistory{perPool: map[string]float64{"A": 60, "B": 40}}
	bf, eng, _ := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	rep := bf.Run(context.Background(), bfToken, poolsFor(600, 400), now)
	if !rep.Filled {
		t.Fatalf("backfill did not run: %s", rep.Reason)
	}
	if rep.Minutes != 1440 {
		t.Fatalf("filled %d minutes, want 1440", rep.Minutes)
	}
	// The newest minutes belong to the live pipeline and must be untouched.
	guard := DefaultBackfillOptions().LiveGuard
	for i := 0; i < int(guard/time.Minute); i++ {
		m := now.Add(-time.Duration(i) * time.Minute)
		if _, ok := eng.Snapshot(bfToken, m).Baselines[volume.Window10m]; !ok {
			t.Fatalf("unexpected snapshot shape")
		}
		if eng.SealedCount(bfToken.Key(), m.Add(time.Minute), 1) != 0 {
			t.Fatalf("minute %v is inside the live guard and must not be sealed by backfill", m)
		}
	}

	snap := eng.Snapshot(bfToken, now.Add(-time.Minute))
	for _, w := range []int{volume.Window10m, volume.Window30m, volume.Window60m, volume.Window24h} {
		bl := snap.Baselines[w]
		if !bl.Usable {
			t.Errorf("window %d unusable right after backfill: %+v", w, bl)
		}
		// Both pools contribute to every minute.
		if bl.Median != 100 {
			t.Errorf("window %d median = %v, want 100", w, bl.Median)
		}
		if bl.Backfilled != bl.Samples {
			t.Errorf("window %d: %d of %d samples marked backfilled, want all",
				w, bl.Backfilled, bl.Samples)
		}
	}
}

func TestBackfillRefusesWhenPoolCoverageIsTooThin(t *testing.T) {
	// Twenty pools where the top twelve carry only ~70% of volume. Writing
	// that would understate every baseline minute, and an understated baseline
	// inflates every later percentage — the direction that fabricates alerts.
	var vols []float64
	for i := 0; i < 20; i++ {
		vols = append(vols, 100)
	}
	h := &stubHistory{perPool: map[string]float64{}}
	bf, eng, _ := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	rep := bf.Run(context.Background(), bfToken, poolsFor(vols...), now)
	if rep.Filled {
		t.Fatal("thin coverage must abandon the backfill, not write a partial one")
	}
	if rep.VolumeShare > 0.95 {
		t.Fatalf("share = %v, expected it to fall short", rep.VolumeShare)
	}
	if eng.SealedCount(bfToken.Key(), now, volume.Window24h) != 0 {
		t.Fatal("nothing should have been written")
	}
}

func TestBackfillAbandonsWhenAMajorPoolFails(t *testing.T) {
	// The dominant pool is unreachable; the remainder cannot represent the
	// token, so the whole attempt is dropped rather than written short.
	h := &stubHistory{
		perPool:   map[string]float64{"A": 90, "B": 10},
		failPools: map[string]bool{"A": true},
	}
	bf, eng, _ := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	rep := bf.Run(context.Background(), bfToken, poolsFor(900, 100), now)
	if rep.Filled {
		t.Fatalf("expected the backfill to be abandoned, got %+v", rep)
	}
	if eng.SealedCount(bfToken.Key(), now, volume.Window24h) != 0 {
		t.Fatal("no minutes should have been written")
	}
}

func TestBackfillToleratesALosingPoolThatDoesNotMatter(t *testing.T) {
	// A dust pool worth 1% of volume fails: the rest still clears the bar.
	h := &stubHistory{
		perPool:   map[string]float64{"A": 99, "B": 1},
		failPools: map[string]bool{"B": true},
	}
	bf, _, _ := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	rep := bf.Run(context.Background(), bfToken, poolsFor(9900, 100), now)
	if !rep.Filled {
		t.Fatalf("expected the backfill to proceed: %s", rep.Reason)
	}
	if rep.PoolsUsed != 1 {
		t.Fatalf("pools used = %d, want 1", rep.PoolsUsed)
	}
}

func TestBackfillNeverOverwritesLiveMinutes(t *testing.T) {
	// A minute the live feed already produced must keep its own number: it was
	// measured by this pipeline, and the provider's figure is a different
	// ruler.
	h := &stubHistory{perPool: map[string]float64{"A": 50}}
	bf, eng, _ := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	live := now.Add(-5 * time.Minute)

	eng.Ingest(domain.Trade{
		Timestamp: live.Add(10 * time.Second), Chain: bfToken.Chain,
		TokenAddress: bfToken.Address, TxHash: "0xlive", USDVolume: 777,
		Side: domain.SideBuy,
	})
	eng.Seal(bfToken.Key(), live, true)

	bf.Run(context.Background(), bfToken, poolsFor(1000), now)

	snap := eng.Snapshot(bfToken, live)
	if snap.Current.Total != 777 {
		t.Fatalf("live minute = %v, want it untouched at 777", snap.Current.Total)
	}
	if snap.Current.Backfilled {
		t.Fatal("a live minute must not be relabelled as backfilled")
	}
}

func TestBackfillSkipsChainWithoutHistoryProvider(t *testing.T) {
	// Every registry chain has a GeckoTerminal id today, but the provider can
	// still refuse a chain — an id retired upstream, or a chain added here
	// before its history exists. Then the service must say so rather than
	// write zeros.
	h := &stubHistory{unsupported: true}
	bf, _, _ := newBackfiller(t, h)

	rep := bf.Run(context.Background(),
		domain.Token{Chain: domain.ChainRobinhood, Address: "0xAA"},
		poolsFor(100), time.Now().UTC())

	if rep.Filled {
		t.Fatal("expected a skip")
	}
	if rep.Reason == "" {
		t.Fatal("a skip must explain itself")
	}
}

func TestBackfillPersistsSoARestartDoesNotRefetch(t *testing.T) {
	h := &stubHistory{perPool: map[string]float64{"A": 100}}
	bf, _, db := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	bf.Run(context.Background(), bfToken, poolsFor(1000), now)

	fresh := volume.NewEngine()
	n, err := db.Restore(fresh, []domain.Token{bfToken}, now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1440 {
		t.Fatalf("restored %d rows, want 1440", n)
	}
	bl := fresh.Snapshot(bfToken, now.Add(-time.Minute)).Baselines[volume.Window24h]
	if !bl.Usable || bl.Backfilled == 0 {
		t.Fatalf("provenance should survive the round trip: %+v", bl)
	}
}

func TestNeededGoesFalseOnceHistoryIsPresent(t *testing.T) {
	h := &stubHistory{perPool: map[string]float64{"A": 100}}
	bf, _, _ := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	if !bf.Needed(bfToken, now) {
		t.Fatal("a cold token needs history")
	}
	bf.Run(context.Background(), bfToken, poolsFor(1000), now)
	if bf.Needed(bfToken, now) {
		t.Fatal("after a fill the token must not be refetched")
	}
}

func TestSelectPoolsTakesVolumeNotCount(t *testing.T) {
	// One pool holds 96% of the volume; taking it alone clears the bar, and
	// the remaining nineteen are not worth a request each.
	pools := []domain.Pool{{Address: "small", Volume24hUSD: 4}, {Address: "big", Volume24hUSD: 96}}
	got, share := selectPools(pools, 12, 0.95)

	if len(got) != 1 || got[0].Address != "big" {
		t.Fatalf("selected %+v", got)
	}
	if share < 0.95 {
		t.Fatalf("share = %v", share)
	}
}

func TestBackfillDoesNotWriteMinutesTheLivePipelineStillOwns(t *testing.T) {
	// Backfill writes sealed buckets. The newest minutes are still filling and
	// still waiting out the seal delay for late trades, so writing them here
	// would freeze a provider's figure into a minute the pipeline was about to
	// fill itself and make every real trade for it arrive too late to count.
	h := &stubHistory{perPool: map[string]float64{"A": 100}}
	bf, eng, _ := newBackfiller(t, h)
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	bf.Run(context.Background(), bfToken, poolsFor(1000), now)

	// The minute one step back is inside the guard: a live trade must still
	// land in it.
	live := now.Add(-time.Minute)
	if !eng.Ingest(domain.Trade{
		Timestamp: live.Add(10 * time.Second), Chain: bfToken.Chain,
		TokenAddress: bfToken.Address, TxHash: "0xlive", USDVolume: 500,
		Side: domain.SideBuy,
	}) {
		t.Fatal("a recent minute was sealed by backfill, so live trades are being dropped")
	}
	if got := eng.Snapshot(bfToken, live).Current.Total; got != 500 {
		t.Fatalf("live minute total = %v, want the real trade at 500", got)
	}
}

func TestBackfillStopsWhereTheProvidersHistoryStops(t *testing.T) {
	// A token listed eight hours ago has no candles before that. Writing those
	// minutes as confirmed zeros would drag the 24h median down and inflate
	// every percentage measured against it — the direction that fabricates
	// alerts.
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	h := &stubHistory{
		perPool: map[string]float64{"A": 100},
		depth:   map[string]int{"A": 480}, // history begins eight hours ago
		origin:  now,
	}
	bf, eng, _ := newBackfiller(t, h)

	rep := bf.Run(context.Background(), bfToken, poolsFor(1000), now)
	if !rep.Filled {
		t.Fatalf("what history exists should still be filled: %s", rep.Reason)
	}
	// The window ends at the live guard, so the covered span is the eight
	// hours of history minus that guard.
	guard := int(DefaultBackfillOptions().LiveGuard / time.Minute)
	wantFilled := 480 - guard
	if rep.Minutes != wantFilled {
		t.Fatalf("filled %d minutes, want the %d the provider actually covered", rep.Minutes, wantFilled)
	}
	if rep.Minutes+rep.Skipped != 1440 {
		t.Fatalf("filled %d + skipped %d should account for the whole window", rep.Minutes, rep.Skipped)
	}

	// Every filled minute carries real volume; none is a fabricated zero.
	end := now.Add(-DefaultBackfillOptions().LiveGuard)
	total, healthy, _ := eng.Sum(bfToken.Key(), end.Add(-time.Duration(wantFilled)*time.Minute), end)
	if healthy != wantFilled || total != float64(wantFilled)*100 {
		t.Fatalf("sum = %v over %d minutes, want %d covered", total, healthy, wantFilled)
	}
}

func TestBackfillReportsWhenHistoryMissesTheWindowEntirely(t *testing.T) {
	// A pool that answers but whose history does not reach the window at all
	// must produce nothing rather than 1,440 zeros.
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	h := &stubHistory{
		perPool: map[string]float64{"A": 100},
		depth:   map[string]int{"A": 1}, // one minute of history, inside the live guard
		origin:  now,
	}
	bf, eng, _ := newBackfiller(t, h)

	rep := bf.Run(context.Background(), bfToken, poolsFor(1000), now)
	if rep.Minutes > 1 {
		t.Fatalf("filled %d minutes from one candle of history", rep.Minutes)
	}
	if eng.SealedCount(bfToken.Key(), now, volume.Window24h) > 1 {
		t.Fatal("almost nothing should have been written")
	}
}
