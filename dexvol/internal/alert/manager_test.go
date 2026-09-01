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

// policy mirrors the shipped defaults: a five-minute cooldown and a ladder
// step of 1.5.
func policy() Policy {
	return Policy{Cooldown: 5 * time.Minute, EscalationFactor: 1.5}
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
	// Still silent four minutes in, while the cooldown holds and the reading
	// has not reached the first rung (50 x 1.5 = 75).
	if d := m.Decide("k", daily(70), base().Add(4*time.Minute), p); d.Send {
		t.Fatalf("a reading below the first rung must stay silent, got %s", d.Reason)
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

// The spec's worked example — +50/+70/+80/+60% over four consecutive minutes —
// is described there as one anomaly deserving one message. That holds at a
// ladder step of 2.0 and not at the shipped default of 1.5, where the +80%
// minute clears the first rung at 75.
//
// The 1.5 default is a deliberate owner choice, traded for catching smaller
// intensifications sooner. This test pins both halves so the trade stays a
// recorded decision rather than drifting into a silent regression.
func TestSpecRunCostsOneMessageAtTwoAndTwoAtTheDefault(t *testing.T) {
	run := func(factor float64) int {
		m := NewManager()
		p := Policy{Cooldown: 5 * time.Minute, EscalationFactor: factor}
		sent := 0
		for i, pct := range []float64{50, 70, 80, 60} {
			if m.Decide("k", daily(pct), base().Add(time.Duration(i)*time.Minute), p).Send {
				sent++
			}
		}
		return sent
	}

	if got := run(2.0); got != 1 {
		t.Errorf("at a step of 2.0 the spec run should be one message, got %d", got)
	}
	if got := run(1.5); got != 2 {
		t.Errorf("at the 1.5 default the +80%% minute clears rung one, so two messages; got %d", got)
	}
}

func TestEscalationBreaksCooldownWithoutANewWindow(t *testing.T) {
	// Same single baseline, but the move quadrupled. Holding that for five
	// minutes behind a +50% notice would hide the larger event.
	m := NewManager()
	p := policy()

	m.Decide("k", daily(50), base(), p)
	d := m.Decide("k", daily(200), base().Add(time.Minute), p)
	if !d.Send || d.Reason != ReasonEscalation {
		t.Fatalf("expected escalation, got %+v", d)
	}
	if len(d.Escalated) != 1 || d.Escalated[0].Multiple != 4 {
		t.Fatalf("growth = %+v, want the 24h window at x4 of its opening 50%%", d.Escalated)
	}
	// 200 sits on rung 3 (50 -> 75 -> 112.5 -> 168.75). 210 is still rung 3,
	// so it is the same news reported twice.
	if d := m.Decide("k", daily(210), base().Add(2*time.Minute), p); d.Send {
		t.Fatalf("the same rung must not re-announce, got %s", d.Reason)
	}
}

func TestAnchorStaysAtTheOpeningAlert(t *testing.T) {
	// The distinguishing case between anchoring on the opening alert and
	// re-anchoring on each message.
	//
	// Opening at 50 puts the rungs at 75, 112.5, 168.75. An escalation at 80
	// takes rung one. A later reading of 115 takes rung two and must send.
	// Had the anchor moved to 80, the next bar would sit at 120 and 115 would
	// be silently swallowed.
	m := NewManager()
	p := policy()

	m.Decide("k", daily(50), base(), p)
	if d := m.Decide("k", daily(80), base().Add(time.Minute), p); !d.Send {
		t.Fatal("80 clears the first rung at 75")
	}
	d := m.Decide("k", daily(115), base().Add(2*time.Minute), p)
	if !d.Send || d.Reason != ReasonEscalation {
		t.Fatalf("115 clears rung two at 112.5 measured from the opening 50, got %+v", d)
	}
	if got := d.Escalated[0].Multiple; got < 2.29 || got > 2.31 {
		t.Fatalf("multiple = %v, want ~2.3 against the opening alert", got)
	}
}

func TestEachFurtherMessageNeedsTheNextRung(t *testing.T) {
	// A fixed anchor with a bare "pct >= anchor * factor" rule would fire every
	// minute for as long as the anomaly stayed above one line. The ladder is
	// what stops that.
	m := NewManager()
	p := policy()

	m.Decide("k", daily(50), base(), p)
	if d := m.Decide("k", daily(80), base().Add(time.Minute), p); !d.Send {
		t.Fatal("rung one")
	}
	for i, pct := range []float64{82, 90, 100, 110} {
		at := base().Add(time.Duration(2+i) * time.Minute)
		if d := m.Decide("k", daily(pct), at, p); d.Send {
			t.Fatalf("%.0f%% is still rung one and must stay silent, got %s", pct, d.Reason)
		}
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

// pump is the usual shape of a sustained rally: the short medians absorb the
// new level within minutes so their percentages fade, while the 24h median —
// 1,440 samples deep — barely moves, so its percentage keeps climbing.
func pump(short, daily float64) detect.Result {
	return result(20, map[int]float64{volume.Window10m: short, volume.Window24h: daily})
}

func TestEscalationIsMeasuredPerWindow(t *testing.T) {
	// Minute 1: 10m +200%, 24h +50%. Minute 5: 10m has faded to +80% while
	// 24h has grown fivefold to +250%.
	//
	// Comparing "the largest window now" against "the largest window then"
	// pits 24h's 250 against 10m's 200 and calls it no escalation — silencing
	// a baseline that quintupled. Each window must be judged against its own
	// last reported value.
	m := NewManager()
	p := policy()

	if d := m.Decide("k", pump(200, 50), base(), p); !d.Send {
		t.Fatal("the opening alert should send")
	}
	d := m.Decide("k", pump(80, 250), base().Add(4*time.Minute), p)

	if !d.Send || d.Reason != ReasonEscalation {
		t.Fatalf("a 24h baseline growing 50%% -> 250%% must escalate, got %+v", d)
	}
	if len(d.Escalated) != 1 || d.Escalated[0].Window != volume.Window24h {
		t.Fatalf("escalated = %+v, want just the 24h window", d.Escalated)
	}
}

func TestAFadingWindowDoesNotEscalate(t *testing.T) {
	// The mirror case: 10m shrank, so nothing about it is new. Without a
	// per-window ladder, 10m's fall could be masked by 24h's rise or vice versa.
	m := NewManager()
	p := policy()

	m.Decide("k", pump(200, 50), base(), p)
	d := m.Decide("k", pump(120, 70), base().Add(time.Minute), p)
	if d.Send {
		t.Fatalf("10m fell and 24h has not reached its rung at 75, got %s", d.Reason)
	}
}

func TestEveryShownWindowAdvancesItsRung(t *testing.T) {
	// The message lists every crossing baseline, so after a send the owner has
	// seen all of those numbers and none of them should immediately
	// re-announce the rung it already occupies.
	m := NewManager()
	p := policy()

	m.Decide("k", pump(30, 30), base(), p)
	// 10m reaches rung 2 (30 -> 45 -> 67.5) and escalates; the message also
	// shows 24h at 100, which is rung 3 for it (30 -> 45 -> 67.5 -> 101.25 is
	// rung 3 at 101.25, so 100 is rung 2).
	if d := m.Decide("k", pump(90, 100), base().Add(time.Minute), p); !d.Send {
		t.Fatal("10m tripling must escalate")
	}
	// 24h at 101 has now reached its rung 3 and is genuinely new.
	if d := m.Decide("k", pump(90, 102), base().Add(2*time.Minute), p); !d.Send {
		t.Fatalf("24h crossing 101.25 is a new rung, got %s", d.Reason)
	}
	// But repeating that same level is not.
	if d := m.Decide("k", pump(90, 103), base().Add(3*time.Minute), p); d.Send {
		t.Fatalf("the same rung must not re-announce, got %s", d.Reason)
	}
}

func TestRungsCleared(t *testing.T) {
	cases := []struct {
		pct, anchor, factor float64
		want                int
	}{
		{50, 50, 1.5, 0},    // at the anchor
		{74, 50, 1.5, 0},    // just under rung one
		{75, 50, 1.5, 1},    // exactly rung one
		{112.5, 50, 1.5, 2}, // exactly rung two
		{200, 50, 1.5, 3},
		{100, 0, 1.5, 0},  // no anchor to measure against
		{100, 50, 1.0, 0}, // a factor of one would be an infinite ladder
		{0, 50, 1.5, 0},
	}
	for _, c := range cases {
		if got := rungsCleared(c.pct, c.anchor, c.factor); got != c.want {
			t.Errorf("rungsCleared(%v, %v, %v) = %d, want %d",
				c.pct, c.anchor, c.factor, got, c.want)
		}
	}
}

func TestRaisingTheFactorMidEpisodeStillLetsABigJumpThrough(t *testing.T) {
	// The owner runs /escalation 3 partway through an episode. The window has
	// already been reported at +200%, which was rung 3 of the old 1.5 ladder.
	// Carrying that rung count over to the coarser ladder would demand rung 4
	// of it — over +1080% — and swallow a jump to +900%.
	//
	// Recomputing both sides on the current factor keeps the comparison honest:
	// +200% is rung 1 of the 3.0 ladder, +900% is rung 2, so it sends.
	m := NewManager()
	loose := Policy{Cooldown: 5 * time.Minute, EscalationFactor: 1.5}
	tight := Policy{Cooldown: 5 * time.Minute, EscalationFactor: 3.0}

	m.Decide("k", daily(40), base(), loose)
	if d := m.Decide("k", daily(200), base().Add(time.Minute), loose); !d.Send {
		t.Fatal("+200% should escalate on the 1.5 ladder")
	}

	d := m.Decide("k", daily(900), base().Add(2*time.Minute), tight)
	if !d.Send || d.Reason != ReasonEscalation {
		t.Fatalf("+900%% after a factor change must still send, got %+v", d)
	}
	// And the coarser ladder then holds: +1000% is still rung 2.
	if d := m.Decide("k", daily(1000), base().Add(3*time.Minute), tight); d.Send {
		t.Fatalf("the same rung must not re-announce, got %s", d.Reason)
	}
}

func TestLoweringTheFactorMidEpisodeIsAlsoConsistent(t *testing.T) {
	// The mirror direction: a finer ladder must not replay rungs the owner has
	// already been shown.
	m := NewManager()
	tight := Policy{Cooldown: 5 * time.Minute, EscalationFactor: 3.0}
	loose := Policy{Cooldown: 5 * time.Minute, EscalationFactor: 1.5}

	m.Decide("k", daily(40), base(), tight)
	if d := m.Decide("k", daily(400), base().Add(time.Minute), tight); !d.Send {
		t.Fatal("+400% should escalate on the 3.0 ladder")
	}
	// On the 1.5 ladder +400% is rung 5, already covered by what was shown.
	if d := m.Decide("k", daily(410), base().Add(2*time.Minute), loose); d.Send {
		t.Fatalf("a finer ladder must not re-announce a level already reported, got %s", d.Reason)
	}
}

// The owner's stated ladder, pinned to the digit: an opening alert at +50% puts
// the next thresholds at 75, 112.5 and 168.75 — each one 1.5x the threshold
// before it, all compounding from the opening value.
func TestLadderThresholdsCompoundFromTheOpeningAlert(t *testing.T) {
	const opening = 50.0
	want := []float64{75, 112.5, 168.75, 253.125}

	for i, threshold := range want {
		rung := i + 1
		// A hair under the threshold is still the previous rung.
		if got := rungsCleared(threshold-0.001, opening, 1.5); got != rung-1 {
			t.Errorf("just under %v: rung %d, want %d", threshold, got, rung-1)
		}
		// Exactly at it takes the next one.
		if got := rungsCleared(threshold, opening, 1.5); got != rung {
			t.Errorf("at %v: rung %d, want %d", threshold, got, rung)
		}
	}
}

// The same ladder driven through the manager, minute by minute, showing which
// readings send and which stay silent.
func TestLadderSendsOncePerThresholdCrossed(t *testing.T) {
	m := NewManager()
	p := policy() // cooldown 5m, factor 1.5

	type step struct {
		pct  float64
		want bool
	}
	// Held inside the cooldown throughout, so every send here is the ladder's
	// doing and not the cooldown expiring.
	steps := []step{
		{50, true},     // opening alert; thresholds become 75, 112.5, 168.75
		{60, false},    // under 75
		{74, false},    // still under 75
		{75, true},     // takes rung 1
		{90, false},    // same rung
		{110, false},   // same rung
		{112.5, true},  // takes rung 2
		{140, false},   // same rung
		{168.75, true}, // takes rung 3
		{200, false},   // same rung
	}

	for i, s := range steps {
		at := base().Add(time.Duration(i) * 10 * time.Second)
		d := m.Decide("k", daily(s.pct), at, p)
		if d.Send != s.want {
			t.Errorf("+%.2f%% -> send=%v (%s), want send=%v", s.pct, d.Send, d.Reason, s.want)
		}
	}
}

func TestLadderDoesNotSaturate(t *testing.T) {
	// The ladder used to be counted in a capped loop, and the cap disabled
	// escalation permanently: once the reached and the announced rung both sat
	// at the ceiling, no further growth could ever exceed it.
	if got := rungsCleared(1e30, 30, 1.5); got < 100 {
		t.Fatalf("rungs = %d, want an unbounded count for an astronomical move", got)
	}

	m := NewManager()
	p := policy()
	m.Decide("k", daily(30), base(), p)
	// A move so large the old loop saturated, followed by a larger one still.
	if d := m.Decide("k", daily(1e30), base().Add(time.Minute), p); !d.Send {
		t.Fatal("the first enormous move must escalate")
	}
	if d := m.Decide("k", daily(1e34), base().Add(2*time.Minute), p); !d.Send {
		t.Fatal("growth past a saturating ladder must still escalate")
	}
}
