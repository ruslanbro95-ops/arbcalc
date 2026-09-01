package alert

import (
	"strings"
	"testing"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/detect"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

func TestRenderMatchesSpecExample(t *testing.T) {
	res := detect.Result{
		Token:  domain.Token{Symbol: "ABC", Chain: domain.ChainBase, Address: "0xAA"},
		Volume: 150_000,
		Changes: []detect.Change{
			{Window: 10, Pct: 50, Median: 100_000, Usable: true, Exceeded: true},
			{Window: 30, Pct: 30.4348, Median: 115_000, Usable: true, Exceeded: true},
			{Window: 60, Pct: 25, Median: 120_000, Usable: true, Exceeded: true},
			{Window: 1440, Pct: 15.3846, Median: 130_000, Usable: true},
		},
		Primary:   detect.Change{Window: 10, Pct: 50, Median: 100_000},
		Anomalous: true,
	}

	want := strings.Join([]string{
		"#ABC · base",
		"$150K",
		"Volume +50%",
		"10m +50%",
		"30m +30%",
		"60m +25%",
		"24h +15%",
		"Median: $100K",
	}, "\n")

	got := Render(res)
	if got.Text != want {
		t.Fatalf("message mismatch:\n--- got ---\n%s\n--- want ---\n%s", got.Text, want)
	}
	if len(got.Links) != 2 {
		t.Fatalf("expected GMGN and OKX buttons, got %+v", got.Links)
	}
}

func TestRenderOmitsUnusableWindows(t *testing.T) {
	res := detect.Result{
		Token:  domain.Token{Symbol: "ABC", Chain: domain.ChainBase},
		Volume: 1000,
		Changes: []detect.Change{
			{Window: 10, Pct: 50, Median: 666, Usable: true, Exceeded: true},
			{Window: 1440, Usable: false},
		},
		Primary:   detect.Change{Window: 10, Pct: 50, Median: 666},
		Anomalous: true,
	}
	if strings.Contains(Render(res).Text, "24h") {
		t.Fatal("a window without enough history must be omitted, not shown as 0%")
	}
}

func TestRenderFlagsDegradedData(t *testing.T) {
	res := detect.Result{
		Token:     domain.Token{Symbol: "ABC", Chain: domain.ChainBase},
		Snap:      volume.Snapshot{HealthyMinutes: 41, TotalMinutes: 60},
		Primary:   detect.Change{Window: 10, Pct: 50, Median: 100},
		Anomalous: true,
	}
	if !strings.Contains(Render(res).Text, "41/60m") {
		t.Fatalf("degraded data must be visible in the alert:\n%s", Render(res).Text)
	}
}

func TestSanitizeSymbol(t *testing.T) {
	res := detect.Result{
		Token:   domain.Token{Symbol: "EVIL #TOKEN", Chain: domain.ChainBase},
		Primary: detect.Change{Median: 1},
	}
	line := strings.SplitN(Render(res).Text, "\n", 2)[0]
	if line != "#EVIL_TOKEN · base" {
		t.Fatalf("got %q", line)
	}
}

func TestRenderFlagsAMostlyHistoricalBaseline(t *testing.T) {
	// Right after a start, or right after a token is added, the baseline is
	// the aggregator's history rather than this pipeline's own measurements.
	// That is exactly when the number deserves a caveat.
	res := detect.Result{
		Token:     domain.Token{Symbol: "ABC", Chain: domain.ChainBase},
		Volume:    150,
		Primary:   detect.Change{Window: 10, Pct: 50, Median: 100, Samples: 10, Backfilled: 9},
		Anomalous: true,
	}
	if !strings.Contains(Render(res).Text, "baseline mostly from history") {
		t.Fatalf("expected the caveat:\n%s", Render(res).Text)
	}
}

func TestRenderStaysQuietWhenBaselineIsMostlyLive(t *testing.T) {
	res := detect.Result{
		Token:     domain.Token{Symbol: "ABC", Chain: domain.ChainBase},
		Volume:    150,
		Primary:   detect.Change{Window: 10, Pct: 50, Median: 100, Samples: 10, Backfilled: 2},
		Anomalous: true,
	}
	if strings.Contains(Render(res).Text, "from history") {
		t.Fatalf("a mostly-live baseline needs no caveat:\n%s", Render(res).Text)
	}
}
