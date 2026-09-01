package alert

import (
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/detect"
)

// Reason explains a send-or-suppress decision. It is logged so a missing alert
// can always be traced to a rule rather than to a bug.
type Reason string

const (
	ReasonFirst      Reason = "first_alert"
	ReasonCooldown   Reason = "cooldown_expired"
	ReasonEscalation Reason = "escalated"
	ReasonNoAnomaly  Reason = "no_anomaly"
	ReasonSuppressed Reason = "cooldown_active"
)

// Decision is the outcome for one token at one minute.
type Decision struct {
	Send   bool
	Reason Reason
}

// Policy is the owner-configurable part of the decision.
type Policy struct {
	Cooldown time.Duration
	// EscalationFactor lets a much stronger anomaly interrupt an active
	// cooldown. A value of 1.5 means the new change must be at least 50%
	// already reported. The default is calibrated so that the spec's own
	// example run of +50/+70/+80/+60% stays a single event.
	EscalationFactor float64
}

type state struct {
	lastAlert time.Time
	lastPct   float64
}

// Manager enforces cooldown and deduplication across alerts.
//
// The problem it solves is the one the spec calls out: a single anomaly that
// persists for four minutes at +50/+70/+80/+60% is one event, not four, and
// sending it four times trains the owner to ignore the bot.
type Manager struct {
	mu     sync.Mutex
	states map[string]*state
}

func NewManager() *Manager {
	return &Manager{states: make(map[string]*state)}
}

// Decide reports whether res should be sent now, and records the send.
func (m *Manager) Decide(key string, res detect.Result, now time.Time, p Policy) Decision {
	if !res.Anomalous {
		return Decision{Reason: ReasonNoAnomaly}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.states[key]
	if st == nil {
		m.states[key] = &state{lastAlert: now, lastPct: res.Primary.Pct}
		return Decision{Send: true, Reason: ReasonFirst}
	}

	if now.Sub(st.lastAlert) >= p.Cooldown {
		st.lastAlert = now
		st.lastPct = res.Primary.Pct
		return Decision{Send: true, Reason: ReasonCooldown}
	}

	// Escalation: the anomaly grew enough that staying silent would hide a
	// materially different event behind the earlier, smaller one.
	if p.EscalationFactor > 1 && st.lastPct > 0 && res.Primary.Pct >= st.lastPct*p.EscalationFactor {
		st.lastAlert = now
		st.lastPct = res.Primary.Pct
		return Decision{Send: true, Reason: ReasonEscalation}
	}

	return Decision{Reason: ReasonSuppressed}
}

// Reset forgets a token's alert history, used when it is removed from the
// watch list so re-adding it does not inherit a stale cooldown.
func (m *Manager) Reset(key string) {
	m.mu.Lock()
	delete(m.states, key)
	m.mu.Unlock()
}
