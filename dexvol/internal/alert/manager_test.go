package alert

import (
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/detect"
)

func anomaly(pct float64) detect.Result {
	return detect.Result{Anomalous: true, Primary: detect.Change{Window: 10, Pct: pct}}
}

func policy() Policy {
	return Policy{Cooldown: 5 * time.Minute, EscalationFactor: 2.0}
}

// The four-minute run from the spec must produce one message, not four.
func TestContinuingAnomalyIsNotSpammed(t *testing.T) {
	m := NewManager()
	t0 := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	p := policy()

	if d := m.Decide("k", anomaly(50), t0, p); !d.Send {
		t.Fatal("first minute must alert")
	}
	for i, pct := range []float64{70, 80, 60} {
		at := t0.Add(time.Duration(i+1) * time.Minute)
		if d := m.Decide("k", anomaly(pct), at, p); d.Send {
			t.Fatalf("minute %d (%v%%) must be suppressed, got %s", i+2, pct, d.Reason)
		}
	}
}

func TestAlertResumesAfterCooldown(t *testing.T) {
	m := NewManager()
	t0 := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	p := policy()

	m.Decide("k", anomaly(50), t0, p)
	d := m.Decide("k", anomaly(55), t0.Add(5*time.Minute), p)
	if !d.Send || d.Reason != ReasonCooldown {
		t.Fatalf("expected a send after the cooldown, got %+v", d)
	}
}

func TestStrongEscalationBreaksCooldown(t *testing.T) {
	// +50% then +200% inside the cooldown: the second is a different event and
	// hiding it behind the first would be the more expensive mistake.
	m := NewManager()
	t0 := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	p := policy()

	m.Decide("k", anomaly(50), t0, p)
	d := m.Decide("k", anomaly(200), t0.Add(time.Minute), p)
	if !d.Send || d.Reason != ReasonEscalation {
		t.Fatalf("expected escalation, got %+v", d)
	}
	// The bar now sits at 200%, so a mere 210% must not escalate again.
	if d := m.Decide("k", anomaly(210), t0.Add(2*time.Minute), p); d.Send {
		t.Fatalf("escalation must re-arm at the new level, got %s", d.Reason)
	}
}

func TestNoAnomalyNeverSends(t *testing.T) {
	m := NewManager()
	if d := m.Decide("k", detect.Result{}, time.Now(), policy()); d.Send {
		t.Fatal("a non-anomalous minute must not send")
	}
}

func TestResetClearsCooldown(t *testing.T) {
	m := NewManager()
	t0 := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	p := policy()

	m.Decide("k", anomaly(50), t0, p)
	m.Reset("k")
	if d := m.Decide("k", anomaly(50), t0.Add(time.Second), p); !d.Send || d.Reason != ReasonFirst {
		t.Fatalf("after a reset the token starts fresh, got %+v", d)
	}
}
