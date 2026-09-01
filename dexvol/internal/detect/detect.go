// Package detect turns a sealed minute into an anomaly verdict.
//
// It is deliberately free of I/O and of wall-clock reads: given a snapshot and
// a threshold it always produces the same verdict, which is what makes the
// alerting logic testable.
package detect

import (
	"sort"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

// Change is how one minute compares against one baseline window.
type Change struct {
	Window   int
	Median   float64
	Pct      float64
	Samples  int
	Usable   bool
	Exceeded bool
}

// Result is the verdict for one token at one minute.
type Result struct {
	Token   domain.Token
	Snap    volume.Snapshot
	Volume  float64
	Changes []Change
	// Primary is the window that drove the alert: the triggered window with
	// the largest percentage change. Its median is the one shown to the user,
	// so the number in the message is the number the alert actually fired on.
	Primary   Change
	Anomalous bool
}

// Detector evaluates snapshots against the owner's current settings.
type Detector struct {
	// ThresholdPct is the minimum percentage above a baseline that counts.
	ThresholdPct float64
	// Windows selects which baselines participate.
	Windows map[int]bool
}

// Evaluate compares the minute against every enabled window. A single exceeded
// window is enough to raise the anomaly, as the spec requires.
func (d Detector) Evaluate(tok domain.Token, snap volume.Snapshot) Result {
	res := Result{Token: tok, Snap: snap, Volume: snap.Current.Total}

	// A minute the sources could not cover says nothing about the market, so
	// it is never judged. Alerting on it would turn every outage into a spike.
	if snap.Current.Quality != volume.QualityOK {
		return res
	}

	windows := make([]int, 0, len(snap.Baselines))
	for w := range snap.Baselines {
		windows = append(windows, w)
	}
	sort.Ints(windows)

	for _, w := range windows {
		if enabled, ok := d.Windows[w]; ok && !enabled {
			continue
		}
		bl := snap.Baselines[w]
		ch := Change{Window: w, Median: bl.Median, Samples: bl.Samples, Usable: bl.Usable}
		if bl.Usable {
			if pct, ok := volume.PercentChange(snap.Current.Total, bl.Median); ok {
				ch.Pct = pct
				ch.Exceeded = pct >= d.ThresholdPct
			}
		}
		res.Changes = append(res.Changes, ch)

		if ch.Exceeded && (!res.Anomalous || ch.Pct > res.Primary.Pct) {
			res.Primary = ch
			res.Anomalous = true
		}
	}
	return res
}
