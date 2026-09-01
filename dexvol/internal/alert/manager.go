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
	// EscalatedWindows names the baselines whose own reading grew past the
	// escalation factor since they were last reported.
	EscalatedWindows []int
}

// Policy is the owner-configurable part of the decision.
type Policy struct {
	Cooldown time.Duration
	// EscalationFactor lets a much stronger reading interrupt an active
	// cooldown even when no new baseline crossed: some baseline's change must
	// be at least this many times what that same baseline last reported.
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
	// reported holds, per baseline window, the percentage last announced for
	// it. A window present here has already fired in this episode.
	//
	// Keeping it per window does two jobs. It separates "the same anomaly,
	// still running" from "something new happened": a minute repeating the
	// previous reading crosses no window it has not already crossed, so it
	// stays silent, while a baseline crossing for the first time goes out at
	// once.
	//
	// And it keeps escalation honest. Measuring growth against whichever
	// window happened to be largest last time compares different baselines to
	// each other, which silences the most common shape of a sustained pump:
	// the short medians absorb the new level within minutes so their
	// percentages fade, while the 24h median — 1,440 samples deep — barely
	// moves, so its percentage keeps climbing. Compared against the earlier
	// 10m reading, a 24h anomaly growing fivefold looks like a decline and is
	// suppressed. Compared against its own last value, it is what it is.
	reported map[int]float64
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
	exceeded := exceededChanges(res)

	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.states[key]
	if st == nil {
		m.states[key] = &state{lastAlert: now, reported: pctByWindow(exceeded)}
		return Decision{Send: true, Reason: ReasonFirst, NewWindows: windowsOf(exceeded)}
	}

	// The cooldown has run out: the episode restarts, and the reported set
	// with it, so a baseline that crosses again later counts as new again.
	if now.Sub(st.lastAlert) >= p.Cooldown {
		st.lastAlert = now
		st.reported = pctByWindow(exceeded)
		return Decision{Send: true, Reason: ReasonCooldown, NewWindows: windowsOf(exceeded)}
	}

	// A baseline crossing for the first time in this episode is a new fact,
	// not a repeat, and goes out regardless of the cooldown.
	var fresh []int
	for _, ch := range exceeded {
		if _, seen := st.reported[ch.Window]; !seen {
			fresh = append(fresh, ch.Window)
		}
	}
	if len(fresh) > 0 {
		st.lastAlert = now
		st.record(exceeded)
		return Decision{Send: true, Reason: ReasonNewTrigger, NewWindows: fresh}
	}

	// No new baseline, but some baseline's own reading grew enough that
	// staying silent would hide a materially different event behind the
	// earlier, smaller one. Each window is judged against what it itself last
	// reported, never against another window's number.
	var escalated []int
	if p.EscalationFactor > 1 {
		for _, ch := range exceeded {
			prev := st.reported[ch.Window]
			if prev > 0 && ch.Pct >= prev*p.EscalationFactor {
				escalated = append(escalated, ch.Window)
			}
		}
	}
	if len(escalated) > 0 {
		st.lastAlert = now
		st.record(exceeded)
		return Decision{Send: true, Reason: ReasonEscalation, EscalatedWindows: escalated}
	}

	return Decision{Reason: ReasonSuppressed}
}

// record refreshes the reported percentage of every exceeded window.
//
// All of them, not only the ones that triggered the send: the message lists
// every crossing baseline, so the owner has now seen each of those numbers and
// the bar for "grew again" belongs at what was shown.
func (s *state) record(exceeded []detect.Change) {
	for _, ch := range exceeded {
		s.reported[ch.Window] = ch.Pct
	}
}

// Reset forgets a token's alert history, used when it is removed from the
// watch list so re-adding it does not inherit a stale cooldown.
func (m *Manager) Reset(key string) {
	m.mu.Lock()
	delete(m.states, key)
	m.mu.Unlock()
}

// exceededChanges lists the baselines that crossed the threshold, smallest
// window first so messages read consistently.
func exceededChanges(res detect.Result) []detect.Change {
	var out []detect.Change
	for _, ch := range res.Changes {
		if ch.Exceeded {
			out = append(out, ch)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Window < out[j].Window })
	return out
}

func windowsOf(changes []detect.Change) []int {
	out := make([]int, 0, len(changes))
	for _, ch := range changes {
		out = append(out, ch.Window)
	}
	return out
}

func pctByWindow(changes []detect.Change) map[int]float64 {
	m := make(map[int]float64, len(changes))
	for _, ch := range changes {
		m[ch.Window] = ch.Pct
	}
	return m
}
