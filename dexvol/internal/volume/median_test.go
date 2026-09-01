package volume

import (
	"math"
	"testing"
)

func closeTo(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestMedianIgnoresSpike(t *testing.T) {
	// The example from the spec: an average would land near $1008, the median
	// must stay at the normal level.
	got := Median([]float64{10, 11, 9, 12, 5000})
	if got != 11 {
		t.Fatalf("median = %v, want 11", got)
	}
}

func TestMedianEven(t *testing.T) {
	if got := Median([]float64{1, 2, 3, 4}); got != 2.5 {
		t.Fatalf("median = %v, want 2.5", got)
	}
}

func TestMedianEmpty(t *testing.T) {
	if got := Median(nil); got != 0 {
		t.Fatalf("median = %v, want 0", got)
	}
}

func TestMedianDoesNotMutateInput(t *testing.T) {
	in := []float64{5, 1, 3}
	Median(in)
	if in[0] != 5 || in[1] != 1 || in[2] != 3 {
		t.Fatalf("input mutated: %v", in)
	}
}

func TestPercentChange(t *testing.T) {
	pct, ok := PercentChange(120000, 100000)
	if !ok || !closeTo(pct, 20) {
		t.Fatalf("pct = %v ok = %v, want ~20 true", pct, ok)
	}
	if _, ok := PercentChange(100, 0); ok {
		t.Fatal("zero baseline must not yield a usable percentage")
	}
}
