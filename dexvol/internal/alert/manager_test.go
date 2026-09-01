package alert

import (
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/detect"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

// result builds a verdict where the named windows crossed the threshold.
// pcts maps a window to its percentage change; a window is "exceeded" when its
// percentage reaches thresholdPct.
func result(thresholdPct float64, pcts map[int]float64) detect.Result {
	res := detect.Result{}
	for _, w := range []int{volume.Window10m, volume.Window30m, volume.Window60m, volume.Window24h} {
		pct, ok := pcts[w]
		if !ok {
			continue
		}
		ch := detect.Change{
			Window: w, Pct: pct, Median: 100, Samples: 100,
			Usable: true, Exceeded: pct >= thresholdPct,
		}
		res.Changes = append(res.Changes, ch)
		if ch.Exceeded && (!res.Anomalous || ch.Pct > res.Primary.Pct) {
			res.Primary = ch
			res.Anomalous = true
		}
	}
	return res
}

// daily is the scenario from the owner's example: only the 24h baseline
// crossed, by 50%.
func daily(pct float64) detect.Result {
	return result(20, map[int]float64{volume.Window24h: pct})
}

func policy() Policy {
	return Policy{Cooldown: 5 * time.Minute, EscalationFactor: 2.0}
}

func base() time.Time { return time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC) }

func TestRepeatOfTheSameReadingIsSilent(t *testing.T) {
	// The owner's example: an alert fires on the 24h median at +50%. The next
	// minute reads the same, crossing nothing it has not already crossed, so
	// nothing is sent.
	m := NewManager()
	p := policy()

	d := m.Decide("k", daily(50), base(), p)
	if !d.Send || d.Reason != ReasonFirst {
		t.Fatalf("first alert: %+v", d)
	}
	if d := m.Decide("k", daily(50), base().Add(time.Minute), p); d.Send {
		t.Fatalf("an identical reading must stay silent, got %s", d.Reason)
	}
	// Still silent four minutes in, while the cooldown holds.
	if d := m.Decide("k", daily(52), base().Add(4*time.Minute), p); d.Send {
		t.Fatalf("a near-identical reading must stay silent, got %s", d.Reason)
	}
}

func TestNewBaselineCrossingBreaksTheCooldown(t *testing.T) {
	// The alert went out on the 24h median. A minute later the 10m baseline
	// crosses for the first time — a different fact about the market, not a
	// repeat — so it goes out immediately rather than waiting five minutes.
	m := NewManager()
	p := policy()

	m.Decide("k", daily(50), base(), p)

	d := m.Decide("k", result(20, map[int]float64{
		volume.Window24h: 50, volume.Window10m: 35,
	}), base().Add(time.Minute), p)

	if !d.Send || d.Reason != ReasonNewTrigger {
		t.Fatalf("a newly crossed baseline must send, got %+v", d)
	}
	if len(d.NewWindows) != 1 || d.NewWindows[0] != volume.Window10m {
		t.Fatalf("new windows = %v, want [10]", d.NewWindows)
	}
}

func TestAWindowOnlyCountsAsNewOnce(t *testing.T) {
	// Once 10m has been reported, it keeps crossing every minute while the
	// anomaly runs. That is the same trigger, not a new one.
	m := NewManager()
	p := policy()
	both := result(20, map[int]float64{volume.Window24h: 50, volume.Window10m: 35})

	m.Decide("k", daily(50), base(), p)
	if d := m.Decide("k", both, base().Add(time.Minute), p); !d.Send {
		t.Fatal("the first 10m crossing should send")
	}
	if d := m.Decide("k", both, base().Add(2*time.Minute), p); d.Send {
		t.Fatalf("the same pair of windows must not send again, got %s", d.Reason)
	}
}

func TestFlappingWindowDoesNotReAlert(t *testing.T) {
	// A baseline hovering at the threshold would otherwise alternate between
	// "gone" and "new" and alert every other minute.
	m := NewManager()
	p := policy()

	m.Decide("k", result(20, map[int]float64{volume.Window24h: 50, volume.Window10m: 25}), base(), p)
	// 10m drops below the threshold, then comes back.
	m.Decide("k", daily(50), base().Add(time.Minute), p)
	d := m.Decide("k", result(20, map[int]float64{volume.Window24h: 50, volume.Window10m: 25}),
		base().Add(2*time.Minute), p)

	if d.Send {
		t.Fatalf("a window that already fired this episode must not re-fire, got %s", d.Reason)
	}
}

// The spec's own worked example: four consecutive elevated minutes on the same
// baselines are one anomaly and must produce exactly one message.
func TestSustainedAnomalyIsOneMessage(t *testing.T) {
	m := NewManager()
	p := policy()
	sent := 0

	for i, pct := range []float64{50, 70, 80, 60} {
		d := m.Decide("k", daily(pct), base().Add(time.Duration(i)*time.Minute), p)
		if d.Send {
			sent++
		}
	}
	if sent != 1 {
		t.Fatalf("sent %d messages, want 1", sent)
	}
}

func TestEscalationStillBreaksCooldownWithoutANewWindow(t *testing.T) {
	// Same single baseline, but the move quadrupled. Holding that for five
	// minutes behind a +50% notice would hide the larger event.
	m := NewManager()
	p := policy()

	m.Decide("k", daily(50), base(), p)
	d := m.Decide("k", daily(200), base().Add(time.Minute), p)
	if !d.Send || d.Reason != ReasonEscalation {
		t.Fatalf("expected escalation, got %+v", d)
	}
	// The bar now sits at 200%, so 210% is not a further escalation.
	if d := m.Decide("k", daily(210), base().Add(2*time.Minute), p); d.Send {
		t.Fatalf("escalation must re-arm at the new level, got %s", d.Reason)
	}
}

func TestEscalationBarIsCalibratedToTheSpecExample(t *testing.T) {
	// At a factor of 1.5 the spec's +80% minute would clear 50% x 1.5 = 75%
	// and send a second message, contradicting the example that calls the run
	// a single anomaly. This pins the reason the default is 2.0.
	m := NewManager()
	loose := Policy{Cooldown: 5 * time.Minute, EscalationFactor: 1.5}

	m.Decide("k", daily(50), base(), loose)
	if d := m.Decide("k", daily(80), base().Add(2*time.Minute), loose); !d.Send {
		t.Fatal("sanity: at 1.5 the +80% minute does clear the bar")
	}

	m2 := NewManager()
	m2.Decide("k", daily(50), base(), policy())
	if d := m2.Decide("k", daily(80), base().Add(2*time.Minute), policy()); d.Send {
		t.Fatalf("at the 2.0 default it must not, got %s", d.Reason)
	}
}

func TestCooldownExpiryRestartsTheEpisode(t *testing.T) {
	m := NewManager()
	p := policy()

	m.Decide("k", daily(50), base(), p)
	d := m.Decide("k", daily(50), base().Add(5*time.Minute), p)
	if !d.Send || d.Reason != ReasonCooldown {
		t.Fatalf("expected a send once the cooldown expired, got %+v", d)
	}
	// The window set restarts too, so a baseline that fired before the reset
	// counts as new again after it.
	if len(d.NewWindows) != 1 || d.NewWindows[0] != volume.Window24h {
		t.Fatalf("new windows = %v, want the episode to restart with [1440]", d.NewWindows)
	}
}

func TestNoAnomalyNeverSends(t *testing.T) {
	m := NewManager()
	if d := m.Decide("k", detect.Result{}, base(), policy()); d.Send {
		t.Fatal("a non-anomalous minute must not send")
	}
}

func TestResetClearsTheEpisode(t *testing.T) {
	m := NewManager()
	p := policy()

	m.Decide("k", daily(50), base(), p)
	m.Reset("k")
	if d := m.Decide("k", daily(50), base().Add(time.Second), p); !d.Send || d.Reason != ReasonFirst {
		t.Fatalf("after a reset the token starts fresh, got %+v", d)
	}
}

func TestTokensAreIndependent(t *testing.T) {
	m := NewManager()
	p := policy()

	m.Decide("a", daily(50), base(), p)
	if d := m.Decide("b", daily(50), base(), p); !d.Send {
		t.Fatal("one token's cooldown must not silence another")
	}
}
