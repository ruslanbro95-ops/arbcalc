package detect

import (
	"math"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

func snapshot(total float64, quality volume.Quality, medians map[int]float64) volume.Snapshot {
	s := volume.Snapshot{
		Minute:    time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC),
		Current:   volume.Bucket{Total: total, Quality: quality, Sealed: true},
		Baselines: map[int]volume.Baseline{},
	}
	for w, m := range medians {
		s.Baselines[w] = volume.Baseline{WindowMinutes: w, Median: m, Samples: 100, Usable: m > 0}
	}
	return s
}

func allWindows() map[int]bool {
	return map[int]bool{10: true, 30: true, 60: true, 1440: true}
}

// The worked example from section 29 of the spec.
func TestEvaluateSpecExample(t *testing.T) {
	d := Detector{ThresholdPct: 20, Windows: allWindows()}
	snap := snapshot(150000, volume.QualityOK, map[int]float64{
		10: 100000, 30: 115000, 60: 120000, 1440: 130000,
	})
	res := d.Evaluate(domain.Token{Symbol: "ABC"}, snap)

	if !res.Anomalous {
		t.Fatal("expected an anomaly")
	}
	want := map[int]float64{10: 50, 30: 30.4348, 60: 25, 1440: 15.3846}
	for _, ch := range res.Changes {
		if math.Abs(ch.Pct-want[ch.Window]) > 0.01 {
			t.Errorf("window %d: pct = %.4f, want %.4f", ch.Window, ch.Pct, want[ch.Window])
		}
	}
	// 24h is below the 20% threshold and must not be the trigger.
	if res.Primary.Window != 10 {
		t.Fatalf("primary window = %d, want 10 (the largest exceeded change)", res.Primary.Window)
	}
	if res.Primary.Median != 100000 {
		t.Fatalf("primary median = %v, want 100000", res.Primary.Median)
	}
}

func TestNoAnomalyBelowThreshold(t *testing.T) {
	d := Detector{ThresholdPct: 20, Windows: allWindows()}
	res := d.Evaluate(domain.Token{}, snapshot(110000, volume.QualityOK, map[int]float64{
		10: 100000, 30: 100000, 60: 100000, 1440: 100000,
	}))
	if res.Anomalous {
		t.Fatalf("+10%% must not trigger a 20%% threshold")
	}
}

func TestSingleWindowIsEnough(t *testing.T) {
	d := Detector{ThresholdPct: 20, Windows: allWindows()}
	res := d.Evaluate(domain.Token{}, snapshot(150000, volume.QualityOK, map[int]float64{
		10: 100000, 30: 1000000, 60: 1000000, 1440: 1000000,
	}))
	if !res.Anomalous || res.Primary.Window != 10 {
		t.Fatalf("one exceeded window should raise the anomaly, got %+v", res.Primary)
	}
}

func TestMissingMinuteIsNeverAnomalous(t *testing.T) {
	// An outage leaves the minute at zero volume. Judging it would report a
	// -100% move, or after a partial recovery a fake spike.
	d := Detector{ThresholdPct: 20, Windows: allWindows()}
	res := d.Evaluate(domain.Token{}, snapshot(0, volume.QualityMissing, map[int]float64{
		10: 100000,
	}))
	if res.Anomalous {
		t.Fatal("a MISSING minute must never raise an alert")
	}
}

func TestUnusableBaselineIsSkipped(t *testing.T) {
	d := Detector{ThresholdPct: 20, Windows: allWindows()}
	snap := snapshot(150000, volume.QualityOK, map[int]float64{10: 0})
	res := d.Evaluate(domain.Token{}, snap)
	if res.Anomalous {
		t.Fatal("a zero median gives no meaningful percentage and must not trigger")
	}
}

func TestDisabledWindowIsIgnored(t *testing.T) {
	d := Detector{ThresholdPct: 20, Windows: map[int]bool{10: false, 30: true, 60: true, 1440: true}}
	res := d.Evaluate(domain.Token{}, snapshot(150000, volume.QualityOK, map[int]float64{
		10: 100000, 30: 1000000, 60: 1000000, 1440: 1000000,
	}))
	if res.Anomalous {
		t.Fatal("the only exceeded window was disabled")
	}
}

func TestBackfilledCountReachesTheResult(t *testing.T) {
	// The alert renderer decides whether to caveat a baseline from this field,
	// so it has to survive the detector.
	d := Detector{ThresholdPct: 20, Windows: allWindows()}
	snap := snapshot(150, volume.QualityOK, map[int]float64{10: 100})
	bl := snap.Baselines[10]
	bl.Backfilled = 90
	bl.Samples = 100
	snap.Baselines[10] = bl

	res := d.Evaluate(domain.Token{}, snap)
	if res.Primary.Backfilled != 90 {
		t.Fatalf("backfilled = %d, want 90", res.Primary.Backfilled)
	}
}

func TestMinVolumeFloorRejectsTinyMinutes(t *testing.T) {
	snap := volume.Snapshot{
		Current: volume.Bucket{Total: 222, Quality: volume.QualityOK},
		Baselines: map[int]volume.Baseline{
			volume.Window10m: {Median: 7, Samples: 10, Usable: true},
		},
	}
	d := Detector{ThresholdPct: 30, MinVolumeUSD: 1000, Windows: map[int]bool{volume.Window10m: true}}

	res := d.Evaluate(domain.Token{Symbol: "ABC", Chain: domain.ChainBase}, snap)
	if res.Anomalous {
		t.Fatalf("$222 against a $7 median reported as anomalous at %.0f%%", res.Primary.Pct)
	}

	// With the floor off, the same input is judged — the check is a floor,
	// not a rewrite of the arithmetic.
	d.MinVolumeUSD = 0
	if res := d.Evaluate(domain.Token{Symbol: "ABC"}, snap); !res.Anomalous {
		t.Fatal("with no floor the percentage must still be evaluated")
	}
}

func TestMinVolumeFloorLetsRealVolumeThrough(t *testing.T) {
	snap := volume.Snapshot{
		Current: volume.Bucket{Total: 200000, Quality: volume.QualityOK},
		Baselines: map[int]volume.Baseline{
			volume.Window10m: {Median: 5000, Samples: 10, Usable: true},
		},
	}
	d := Detector{ThresholdPct: 30, MinVolumeUSD: 1000, Windows: map[int]bool{volume.Window10m: true}}

	if res := d.Evaluate(domain.Token{Symbol: "ABC"}, snap); !res.Anomalous {
		t.Fatal("a $200K minute above a $5K median must alert")
	}
}
