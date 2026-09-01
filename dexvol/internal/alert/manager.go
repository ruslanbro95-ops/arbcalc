package alert

import (
	"sort"
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
	ReasonNewTrigger Reason = "new_trigger"
	ReasonEscalation Reason = "escalated"
	ReasonNoAnomaly  Reason = "no_anomaly"
	ReasonSuppressed Reason = "cooldown_active"
)

// Decision is the outcome for one token at one minute.
type Decision struct {
	Send   bool
	Reason Reason
	// NewWindows names the baselines that crossed the threshold for the first
	// time in this episode. The message quotes them, so a repeat inside the
	// cooldown always answers "why am I seeing this again".
	NewWindows []int
}

// Policy is the owner-configurable part of the decision.
type Policy struct {
	Cooldown time.Duration
	// EscalationFactor lets a much stronger reading interrupt an active
	// cooldown even when no new baseline crossed: the change must be at least
	// this many times the one already reported.
	//
	// The default is 2.0, not 1.5, and the spec is what fixes it. Its worked
	// example runs +50/+70/+80/+60% over four consecutive minutes and calls
	// that one continuing anomaly deserving one message. At 1.5 the +80%
	// minute clears the bar (80/50 = 1.6) and sends a second. At 2.0 none of
	// the three follow-ups clear it, while a genuinely different move — +200%
	// after +50% — still gets through immediately.
	EscalationFactor float64
}

// state is one token's live alert episode.
type state struct {
	lastAlert time.Time
	lastPct   float64
	// fired is the set of baseline windows already reported in this episode.
	//
	// This is what separates "the same anomaly, still running" from "something
	// new happened". A minute repeating the previous reading crosses no window
	// it has not already crossed, so it stays silent; a minute where another
	// baseline crosses for the first time is a different fact about the market
	// and goes out immediately, without waiting for the cooldown.
	fired map[int]bool
}

// Manager enforces cooldown and deduplication across alerts.
//
// The rule it implements: one continuing anomaly is one message, but a new
// trigger is never held back. Repeating the same reading every minute trains
// the owner to ignore the bot; swallowing a genuinely new signal for five
// minutes is worse.
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
	exceeded := exceededWindows(res)

	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.states[key]
	if st == nil {
		m.states[key] = &state{lastAlert: now, lastPct: res.Primary.Pct, fired: setOf(exceeded)}
		return Decision{Send: true, Reason: ReasonFirst, NewWindows: exceeded}
	}

	// The cooldown has run out: the episode restarts, and the window set with
	// it, so a baseline that crosses again later counts as new again.
	if now.Sub(st.lastAlert) >= p.Cooldown {
		st.lastAlert = now
		st.lastPct = res.Primary.Pct
		st.fired = setOf(exceeded)
		return Decision{Send: true, Reason: ReasonCooldown, NewWindows: exceeded}
	}

	// A baseline crossing for the first time in this episode is a new fact,
	// not a repeat, and goes out regardless of the cooldown.
	var fresh []int
	for _, w := range exceeded {
		if !st.fired[w] {
			fresh = append(fresh, w)
		}
	}
	if len(fresh) > 0 {
		for _, w := range fresh {
			st.fired[w] = true
		}
		st.lastAlert = now
		st.lastPct = res.Primary.Pct
		return Decision{Send: true, Reason: ReasonNewTrigger, NewWindows: fresh}
	}

	// No new baseline, but the move itself grew enough that staying silent
	// would hide a materially different event behind the earlier, smaller one.
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

// exceededWindows lists the baselines that crossed the threshold, smallest
// window first so messages read consistently.
func exceededWindows(res detect.Result) []int {
	var out []int
	for _, ch := range res.Changes {
		if ch.Exceeded {
			out = append(out, ch.Window)
		}
	}
	sort.Ints(out)
	return out
}

func setOf(windows []int) map[int]bool {
	m := make(map[int]bool, len(windows))
	for _, w := range windows {
		m[w] = true
	}
	return m
}
