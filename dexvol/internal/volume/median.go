// Package volume aggregates normalized trades into calendar-minute buckets and
// derives the median baselines the anomaly detector compares against.
package volume

import "sort"

// Median returns the median of vals without mutating the caller's slice.
//
// The project deliberately uses a median rather than an average: a single
// outsized swap must not drag the baseline up, or every subsequent minute would
// be measured against a spike instead of against normal activity.
func Median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := make([]float64, len(vals))
	copy(cp, vals)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

// PercentChange expresses current relative to baseline, as required by the spec:
//
//	(current / baseline - 1) * 100
//
// A zero baseline has no meaningful percentage, so ok is false and callers must
// not raise an alert on the result.
func PercentChange(current, baseline float64) (pct float64, ok bool) {
	if baseline <= 0 {
		return 0, false
	}
	return (current/baseline - 1) * 100, true
}
