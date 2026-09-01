package alert

import (
	"math"
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
	// Escalated names the baselines that took a new step up the ladder, with
	// how far each has grown since the episode's opening alert.
	Escalated []WindowGrowth
}

// WindowGrowth is one baseline's growth relative to the episode anchor.
type WindowGrowth struct {
	Window int
	// Multiple is the current percentage divided by the one this window showed
	// in the alert that first reported it.
	Multiple float64
}

// Policy is the owner-configurable part of the decision.
type Policy struct {
	Cooldown time.Duration
	// EscalationFactor is the step size of the escalation ladder: a baseline
	// must reach this multiple of where it stood at the episode's opening
	// alert before it is worth reporting again, then this multiple again, and
	// so on.
	//
	// The anchor is the opening alert and never moves during an episode, which
	// is what makes the multiple mean something a person can hold onto: "this
	// is three times the move I was first told about". Re-anchoring on each
	// message instead would make every step relative to the previous one and
	// lose the sense of total growth.
	//
	// The ladder is what keeps a fixed anchor from spamming. A bare
	// "pct >= anchor * factor" rule would fire every single minute for as long
	// as the anomaly stayed above that one line; requiring the NEXT rung means
	// each further message reports a genuinely larger move.
	//
	// Note the owner-chosen default of 1.5 deliberately parts with the spec's
	// worked example, which runs +50/+70/+80/+60% and calls it one message: at
	// 1.5 the +80% minute clears the first rung (50 x 1.5 = 75) and sends a
	// second. At 2.0 it would not. Tunable at runtime with /escalation.
	EscalationFactor float64
}

// state is one token's live alert episode.
type state struct {
	lastAlert time.Time
	// anchor holds, per baseline window, the percentage that window showed in
	// the alert which first reported it. It is the episode's reference point
	// and does not move until the cooldown expires and the episode restarts.
	//
	// A window present here has already fired, which is what separates "the
	// same anomaly, still running" from "something new happened": a minute
	// repeating the previous reading crosses no window it has not already
	// crossed, so it stays silent, while a baseline crossing for the first
	// time goes out at once.
	//
	// Anchoring per window is also what keeps escalation honest. Measuring
	// growth against whichever window happened to be largest last time
	// compares different baselines to each other, which silences the most
	// common shape of a sustained pump: the short medians absorb the new level
	// within minutes so their percentages fade, while the 24h median — 1,440
	// samples deep — barely moves, so its percentage keeps climbing. Compared
	// against the earlier 10m reading, a 24h anomaly growing fivefold looks
	// like a decline and is suppressed. Compared against its own opening
	// value, it is what it is.
	anchor map[int]float64
	// announced is the highest percentage already reported for each window.
	//
	// The rung it corresponds to is recomputed on demand rather than stored,
	// so that changing /escalation mid-episode re-reads both sides of the
	// comparison on the new ladder. Storing a rung count instead leaves a
	// number measured on the old one: after a change from 1.5 to 3.0, a window
	// sitting on rung 3 of the finer ladder needs rung 4 of the coarser one,
	// and a jump from +200% to +900% is silently swallowed.
	announced map[int]float64
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
		st = newEpisode(exceeded)
		st.lastAlert = now
		m.states[key] = st
		return Decision{Send: true, Reason: ReasonFirst, NewWindows: windowsOf(exceeded)}
	}

	// The cooldown has run out: the episode restarts, and the anchors with it,
	// so growth is measured afresh from here.
	if now.Sub(st.lastAlert) >= p.Cooldown {
		*st = *newEpisode(exceeded)
		st.lastAlert = now
		return Decision{Send: true, Reason: ReasonCooldown, NewWindows: windowsOf(exceeded)}
	}

	// A baseline crossing for the first time in this episode is a new fact,
	// not a repeat, and goes out regardless of the cooldown.
	var fresh []int
	for _, ch := range exceeded {
		if _, seen := st.anchor[ch.Window]; !seen {
			fresh = append(fresh, ch.Window)
		}
	}
	if len(fresh) > 0 {
		st.lastAlert = now
		st.record(exceeded)
		return Decision{Send: true, Reason: ReasonNewTrigger, NewWindows: fresh}
	}

	// No new baseline, but some baseline climbed to a rung of its own ladder
	// that has not been announced yet. Each window is judged against its own
	// opening value, never against another window's number.
	var escalated []WindowGrowth
	for _, ch := range exceeded {
		a := st.anchor[ch.Window]
		reached := rungsCleared(ch.Pct, a, p.EscalationFactor)
		shown := rungsCleared(st.announced[ch.Window], a, p.EscalationFactor)
		if reached > shown {
			escalated = append(escalated, WindowGrowth{Window: ch.Window, Multiple: ch.Pct / a})
		}
	}
	if len(escalated) > 0 {
		st.lastAlert = now
		st.record(exceeded)
		return Decision{Send: true, Reason: ReasonEscalation, Escalated: escalated}
	}

	return Decision{Reason: ReasonSuppressed}
}

func newEpisode(exceeded []detect.Change) *state {
	st := &state{
		anchor:    make(map[int]float64, len(exceeded)),
		announced: make(map[int]float64, len(exceeded)),
	}
	for _, ch := range exceeded {
		st.anchor[ch.Window] = ch.Pct
		st.announced[ch.Window] = ch.Pct
	}
	return st
}

// record marks how far up its ladder each shown window has been reported.
//
// Every exceeded window, not only the one that triggered the send: the message
// lists them all, so the owner has now seen each of those numbers and none of
// them should immediately re-announce the same rung. Anchors are never touched
// here — that is the point of anchoring on the opening alert.
func (s *state) record(exceeded []detect.Change) {
	for _, ch := range exceeded {
		if _, seen := s.anchor[ch.Window]; !seen {
			// A window joining mid-episode anchors where it first crossed.
			s.anchor[ch.Window] = ch.Pct
			s.announced[ch.Window] = ch.Pct
			continue
		}
		if ch.Pct > s.announced[ch.Window] {
			s.announced[ch.Window] = ch.Pct
		}
	}
}

// rungsCleared counts how many times pct has multiplied past the anchor by the
// factor: anchor*f, anchor*f^2, and so on.
//
// Computed rather than counted in a loop. The loop needed a cap to stay
// bounded, and a cap silently disables escalation for the rest of the episode:
// once both the reached and the announced rung saturate at the same number,
// "reached > announced" can never be true again no matter how far the anomaly
// runs.
func rungsCleared(pct, anchor, factor float64) int {
	if anchor <= 0 || factor <= 1 || pct <= 0 || pct < anchor*factor {
		return 0
	}

	n := int(math.Floor(math.Log(pct/anchor) / math.Log(factor)))
	if n < 0 {
		n = 0
	}
	// Logs are approximate, and a reading landing exactly on a rung is the
	// ordinary case rather than a corner one, so settle the boundary by
	// comparison. Each step is at least a factor apart, so this corrects by at
	// most one.
	for n > 0 && pct < anchor*math.Pow(factor, float64(n)) {
		n--
	}
	for pct >= anchor*math.Pow(factor, float64(n+1)) {
		n++
	}
	return n
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
