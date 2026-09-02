package service

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/alert"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/config"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/store"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

type recordingNotifier struct {
	mu   sync.Mutex
	sent []alert.Message
}

func (r *recordingNotifier) Notify(_ context.Context, m alert.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, m)
	return nil
}

func (r *recordingNotifier) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

var testToken = domain.Token{
	Symbol: "ABC", Chain: domain.ChainBase,
	Address: "0x1111111111111111111111111111111111111111", Enabled: true,
}

func newService(t *testing.T) (*Service, *recordingNotifier, *config.Store) {
	t.Helper()
	dir := t.TempDir()

	settings := config.NewStore(filepath.Join(dir, "state.json"))
	settings.Update(func(rt *config.Runtime) {
		rt.Tokens = []domain.Token{testToken}
		// These cases exercise the percentage logic with tidy round numbers in
		// the hundreds. The production floor is there to reject exactly that
		// size, so it is off here and tested on its own.
		rt.MinVolumeUSD = 0
	})

	db, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	static := config.Static{
		PollInterval:      12 * time.Second,
		SealDelay:         20 * time.Second,
		PoolRefresh:       5 * time.Minute,
		RawTradeRetention: 48 * time.Hour,
	}
	svc := New(static, settings, db, volume.NewEngine(),
		NewDiscovery(discardLog()), NewPriceCache(&stubProvider{}, discardLog()),
		alert.NewManager(), map[domain.Chain]TradeSource{}, discardLog())

	n := &recordingNotifier{}
	svc.SetNotifier(n)
	return svc, n, settings
}

// feed writes one minute of volume and marks the chain healthy for it.
func feed(svc *Service, minute time.Time, usd float64) {
	if usd > 0 {
		svc.engine.Ingest(domain.Trade{
			Timestamp: minute.Add(10 * time.Second), Chain: testToken.Chain,
			TokenAddress: testToken.Address, TxHash: minute.String(), LogIndex: 0,
			Side: domain.SideBuy, USDVolume: usd,
		})
	}
	svc.health.record(testToken.Chain, minute.Add(30*time.Second), true)
}

func base() time.Time { return time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC) }

// seal closes exactly the given minute. Minute m ends at m+1min and the grace
// period runs from there, so any instant just past m+1min+SealDelay lands on m.
func seal(svc *Service, minute time.Time) {
	svc.sealDue(context.Background(), minute.Add(time.Minute).Add(svc.static.SealDelay).Add(time.Second))
	drainAlerts(svc)
}

// drainAlerts stands in for deliverLoop, which the tests do not run. Delivery
// is asynchronous in the real service so a slow Telegram cannot stall minute
// sealing; here it only has to happen before the assertions.
func drainAlerts(svc *Service) {
	for {
		select {
		case out := <-svc.outbox:
			svc.notifier.Notify(context.Background(), out.msg)
		default:
			return
		}
	}
}

// advance records one minute of volume and then closes it, the way the running
// service does minute by minute.
func advance(svc *Service, minute time.Time, usd float64) {
	feed(svc, minute, usd)
	seal(svc, minute)
}

// advanceMissing closes a minute nobody reported on: an outage.
func advanceMissing(svc *Service, minute time.Time) {
	seal(svc, minute)
}

func TestSpikeProducesOneAlert(t *testing.T) {
	svc, notifier, _ := newService(t)
	b := base()

	// Thirty quiet minutes at $100 establish the baseline.
	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	if notifier.count() != 0 {
		t.Fatalf("a flat series must not alert, got %d", notifier.count())
	}

	// Then one minute at $500: +400% against every window.
	advance(svc, b.Add(31*time.Minute), 500)

	if notifier.count() != 1 {
		t.Fatalf("got %d alerts, want 1", notifier.count())
	}
	text := notifier.sent[0].Text
	for _, want := range []string{"#ABC · base", "$500", "Volume +400%", "Median: $100"} {
		if !strings.Contains(text, want) {
			t.Errorf("alert missing %q:\n%s", want, text)
		}
	}
}

// The spec's worked example end to end. Its four elevated minutes read as
// +50/+70/+80/+60% against the settled baseline, which the spec calls one
// anomaly deserving one message.
//
// At the shipped escalation step of 1.5 that costs two messages, because the
// +80% minute clears the first rung at 75. That is the owner's deliberate
// trade for catching smaller intensifications sooner, and the second half of
// this test shows the spec's own behaviour is one /escalation command away.
func TestSustainedAnomalyCostsTwoAtTheDefaultAndOneAtStepTwo(t *testing.T) {
	run := func(t *testing.T, factor float64) int {
		t.Helper()
		svc, notifier, settings := newService(t)
		if err := settings.Update(func(rt *config.Runtime) { rt.EscalationFactor = factor }); err != nil {
			t.Fatal(err)
		}
		b := base()
		for i := 1; i <= 30; i++ {
			advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
		}
		for i, usd := range []float64{150, 170, 180, 160} {
			advance(svc, b.Add(time.Duration(31+i)*time.Minute), usd)
		}
		return notifier.count()
	}

	if got := run(t, 1.5); got != 2 {
		t.Errorf("at the 1.5 default the run should cost two messages, got %d", got)
	}
	if got := run(t, 2.0); got != 1 {
		t.Errorf("raising the step to 2.0 should restore the spec's single message, got %d", got)
	}
}

func TestThresholdAndEscalationAreTunableWithoutARestart(t *testing.T) {
	// Both defaults are owner-chosen and both must be changeable from the bot
	// while the service runs, which is what the settings store round trip
	// proves here.
	svc, notifier, settings := newService(t)
	b := base()

	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	// +25% sits under the 30% default and must stay silent.
	advance(svc, b.Add(31*time.Minute), 125)
	if notifier.count() != 0 {
		t.Fatalf("+25%% is below the default threshold, got %d alerts", notifier.count())
	}

	// Lower the threshold mid-run; the very next minute is judged by the new
	// value with no restart in between.
	if err := settings.Update(func(rt *config.Runtime) { rt.ThresholdPct = 20 }); err != nil {
		t.Fatal(err)
	}
	advance(svc, b.Add(32*time.Minute), 125)
	if notifier.count() != 1 {
		t.Fatalf("after lowering the threshold the same move should alert, got %d", notifier.count())
	}
}

func TestOutageDoesNotAlertAndDoesNotPoisonTheBaseline(t *testing.T) {
	svc, notifier, _ := newService(t)
	b := base()

	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}

	// Five minutes where the source was down: no trades, and no health record.
	for i := 31; i <= 35; i++ {
		advanceMissing(svc, b.Add(time.Duration(i)*time.Minute))
	}
	if notifier.count() != 0 {
		t.Fatalf("an outage must not raise an alert, got %d", notifier.count())
	}

	// A normal minute after recovery must read as normal, not as a spike
	// against a baseline dragged down by five zeros.
	advance(svc, b.Add(36*time.Minute), 105)
	if notifier.count() != 0 {
		t.Fatalf("recovery to a normal level must not alert, got %d:\n%s",
			notifier.count(), notifier.sent[0].Text)
	}
}

func TestSealWalksForwardAfterAPause(t *testing.T) {
	// A laptop asleep or a long stall must not leave holes: the skipped
	// minutes still need sealing, or every median quietly loses samples.
	svc, _, _ := newService(t)
	b := base()

	advance(svc, b.Add(time.Minute), 100)

	// Jump ten minutes ahead in one call.
	seal(svc, b.Add(11*time.Minute))

	series := svc.engine.Snapshot(testToken, b.Add(11*time.Minute))
	if !series.Current.Sealed {
		t.Fatal("the skipped minutes should have been sealed")
	}
	if series.Current.Quality != volume.QualityMissing {
		t.Fatalf("unobserved minutes must seal as MISSING, got %s", series.Current.Quality)
	}
}

func TestMonitoringOffKeepsCollecting(t *testing.T) {
	svc, notifier, settings := newService(t)
	settings.Update(func(rt *config.Runtime) { rt.Monitoring = false })
	b := base()

	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	advance(svc, b.Add(31*time.Minute), 5000)

	if notifier.count() != 0 {
		t.Fatal("alerts are off")
	}
	// History must still be there, so switching back on does not start blind.
	snap := svc.engine.Snapshot(testToken, b.Add(31*time.Minute))
	if snap.Current.Total != 5000 {
		t.Fatalf("volume = %v, want the minute to have been recorded anyway", snap.Current.Total)
	}
	if !snap.Baselines[volume.Window10m].Usable {
		t.Fatal("baselines should have kept building while alerting was off")
	}
}

func TestMinutesArePersisted(t *testing.T) {
	svc, _, _ := newService(t)
	b := base()

	advance(svc, b.Add(time.Minute), 250)

	rows, err := svc.db.LoadMinutes(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("no minutes were written")
	}
	var found bool
	for _, r := range rows {
		if r.Minute.Equal(b.Add(time.Minute)) && r.Total == 250 {
			found = true
		}
	}
	if !found {
		t.Fatalf("the sealed minute was not persisted: %+v", rows)
	}
}

func TestTokensChangedDoesNotBlock(t *testing.T) {
	svc, _, _ := newService(t)
	// The channel is buffered by one; a burst of edits must collapse rather
	// than deadlock the bot's command handler.
	for i := 0; i < 10; i++ {
		svc.TokensChanged()
	}
}

func TestNewBaselineCrossingSendsInsideTheCooldown(t *testing.T) {
	// End to end for the rule the owner asked for: a repeat of the same
	// reading is silent, but a baseline crossing for the first time is a new
	// fact and goes out without waiting for the cooldown.
	svc, notifier, _ := newService(t)
	b := base()

	// Two hours of $100 minutes, so every window has a settled $100 median.
	for i := 1; i <= 120; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}

	// A step to $150 crosses all four baselines at once: one message.
	advance(svc, b.Add(121*time.Minute), 150)
	if notifier.count() != 1 {
		t.Fatalf("got %d alerts on the step, want 1", notifier.count())
	}

	// Holding at $150 crosses nothing new, and the short medians have started
	// absorbing the new level, so the next minutes stay silent.
	for i := 122; i <= 124; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 150)
	}
	if notifier.count() != 1 {
		t.Fatalf("holding the same level must stay silent, got %d alerts", notifier.count())
	}
}

func TestRepeatedIdenticalMinuteStaysSilent(t *testing.T) {
	// The owner's example, at service level: an alert on the daily median,
	// then an identical minute, then nothing.
	svc, notifier, _ := newService(t)
	b := base()

	for i := 1; i <= 120; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	advance(svc, b.Add(121*time.Minute), 150)
	advance(svc, b.Add(122*time.Minute), 150)

	if notifier.count() != 1 {
		t.Fatalf("got %d alerts, want 1", notifier.count())
	}
}

func TestMinuteIsNotSealedBeforeItEnds(t *testing.T) {
	// Minute m spans [m, m+1min). Sealing it any earlier both loses the trades
	// still to come and — worse — seals it QualityOK at whatever partial total
	// it holds, because the poll behind it succeeded. Those fake numbers then
	// enter every median.
	//
	// This drives the real seal loop: a one-second tick calling sealDue, with a
	// trade arriving late in the minute exactly as consumeTrades would deliver
	// it.
	svc, _, _ := newService(t)
	m := base().Add(10 * time.Minute)

	var sealedAt time.Time
	for sec := -5; sec <= 120; sec++ {
		now := m.Add(time.Duration(sec) * time.Second)
		if sec >= 0 && sec%12 == 0 {
			svc.health.record(testToken.Chain, now, true)
		}
		if sec == 46 {
			ok := svc.engine.Ingest(domain.Trade{
				Timestamp: m.Add(45 * time.Second), Chain: testToken.Chain,
				TokenAddress: testToken.Address, TxHash: "late", LogIndex: 0,
				Side: domain.SideBuy, USDVolume: 999,
			})
			if !ok {
				t.Fatal("a trade 45s into the minute must still be accepted")
			}
		}
		before := svc.lastSealed
		svc.sealDue(context.Background(), now)
		if !svc.lastSealed.Equal(before) && svc.lastSealed.Equal(m) {
			sealedAt = now
		}
	}

	if sealedAt.IsZero() {
		t.Fatal("the minute was never sealed")
	}
	if sealedAt.Before(m.Add(time.Minute)) {
		t.Fatalf("minute sealed at %s, %v before it ended",
			sealedAt.Format("15:04:05"), m.Add(time.Minute).Sub(sealedAt))
	}
	if got := sealedAt.Sub(m.Add(time.Minute)); got > svc.static.SealDelay+2*time.Second {
		t.Fatalf("minute sealed %v after it ended, later than the %v grace period",
			got, svc.static.SealDelay)
	}

	snap := svc.engine.Snapshot(testToken, m)
	if snap.Current.Total != 999 || snap.Current.Trades != 1 {
		t.Fatalf("bucket = %v over %d trades, want the late trade counted",
			snap.Current.Total, snap.Current.Trades)
	}
	if svc.engine.Stats().TooLate != 0 {
		t.Fatalf("no trade should have missed the deadline, got %d", svc.engine.Stats().TooLate)
	}
}

func TestLateTradesDoNotCreateFakeZeroMinutes(t *testing.T) {
	// The downstream harm of sealing early: minutes whose trades all missed the
	// deadline sealed healthy at $0, dragged every median down, and made the
	// next ordinary minute read as a spike.
	svc, notifier, _ := newService(t)
	b := base()

	for i := 1; i <= 60; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	// Five more ordinary minutes, each with its trade arriving 50 seconds in.
	for i := 61; i <= 65; i++ {
		m := b.Add(time.Duration(i) * time.Minute)
		svc.engine.Ingest(domain.Trade{
			Timestamp: m.Add(50 * time.Second), Chain: testToken.Chain,
			TokenAddress: testToken.Address, TxHash: m.String(), LogIndex: 0,
			Side: domain.SideBuy, USDVolume: 100,
		})
		svc.health.record(testToken.Chain, m.Add(30*time.Second), true)
		seal(svc, m)

		snap := svc.engine.Snapshot(testToken, m)
		if snap.Current.Total != 100 {
			t.Fatalf("minute %d recorded %v, want the late trade counted", i, snap.Current.Total)
		}
	}

	advance(svc, b.Add(66*time.Minute), 100)
	if notifier.count() != 0 {
		t.Fatalf("a flat series must never alert, got %d:\n%s",
			notifier.count(), notifier.sent[0].Text)
	}
}

func TestBacklogDoesNotCollapseSeparateSpikes(t *testing.T) {
	// The alert cooldown is stated in market minutes, but the judge used to
	// read the wall clock. sealDue is built to walk a backlog — a sleeping
	// laptop, a slow restart, or simply a slow Telegram send stalling the seal
	// loop — and every minute in that backlog is judged within the same
	// millisecond. Measured on the wall clock the cooldown never elapses, so a
	// whole catch-up collapses into one message however far apart the spikes
	// really were.
	svc, notifier, _ := newService(t)
	b := base()

	// Warm the baselines up minute by minute, the way the running service does.
	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	// Now the service stalls: volume keeps arriving but nothing is sealed.
	// Two spikes ten market-minutes apart — double the five-minute cooldown.
	for i := 31; i <= 45; i++ {
		usd := 100.0
		if i == 31 || i == 41 {
			usd = 5000
		}
		feed(svc, b.Add(time.Duration(i)*time.Minute), usd)
	}

	// One catch-up pass seals and judges all fifteen back to back.
	seal(svc, b.Add(45*time.Minute))

	if notifier.count() != 2 {
		t.Fatalf("two spikes ten minutes apart should produce two alerts, got %d", notifier.count())
	}
}

func TestMutedTokenIsNotDeliveredButKeepsItsBookkeeping(t *testing.T) {
	// The mute button is pressed by whoever is watching the alert chat, and it
	// stops delivery only. Collection, sealing and the alert manager's episode
	// state all keep running: a mute that left a hole in the medians would
	// make the first minute after it expire look like a spike, which is the
	// exact failure this project spends most of its care avoiding.
	svc, notifier, settings := newService(t)
	b := base()

	if err := settings.Update(func(rt *config.Runtime) {
		rt.Muted = map[string]time.Time{testToken.Key(): time.Now().Add(30 * time.Minute)}
	}); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	advance(svc, b.Add(31*time.Minute), 500)

	if notifier.count() != 0 {
		t.Fatalf("got %d alerts, want silence while muted", notifier.count())
	}

	// The spike minute must still be in the series at its real value.
	snap, ok := svc.Snapshot(testToken)
	if !ok {
		t.Fatal("the muted token lost its snapshot")
	}
	if snap.Current.Total != 500 {
		t.Fatalf("muted minute recorded as %v, want 500 — a mute must not touch the data",
			snap.Current.Total)
	}
}

func TestMuteExpiryRestoresDelivery(t *testing.T) {
	svc, notifier, settings := newService(t)
	b := base()

	if err := settings.Update(func(rt *config.Runtime) {
		rt.Muted = map[string]time.Time{testToken.Key(): time.Now().Add(-time.Minute)}
	}); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}
	advance(svc, b.Add(31*time.Minute), 500)

	if notifier.count() != 1 {
		t.Fatalf("got %d alerts, want 1 once the mute has expired", notifier.count())
	}
}

func TestBackfillIgnoresPoolsTheLiveFeedCannotRead(t *testing.T) {
	// A Uniswap V4 pool id is 32 bytes. The history provider serves it, and
	// eth_getLogs cannot subscribe to it — so its volume would enter the
	// baseline and never enter a live minute. The token would then measure
	// small against its own median forever and simply stop alerting, which
	// looks identical to a quiet market.
	const v4 = "0x54f7883914619af9105355bf83ed678bcf9f63560218ac61c9963b9503d0ba32"
	pools := []domain.Pool{
		{Chain: domain.ChainBase, Address: "0x1111111111111111111111111111111111111111", Volume24hUSD: 100},
		{Chain: domain.ChainBase, Address: v4, Volume24hUSD: 900},
	}

	got := ingestablePools(domain.ChainBase, pools)
	if len(got) != 1 || got[0].Address != "0x1111111111111111111111111111111111111111" {
		t.Fatalf("kept %+v, want only the contract-addressed pool", got)
	}

	// Solana pool ids are base58 and every one of them is readable, so the
	// filter must not touch that chain.
	sol := []domain.Pool{{Chain: domain.ChainSolana, Address: "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"}}
	if len(ingestablePools(domain.ChainSolana, sol)) != 1 {
		t.Fatal("a non-EVM chain must keep all of its pools")
	}
}

func TestATinyMinuteIsNotAnEventHoweverLargeThePercentage(t *testing.T) {
	// The alert this prevents, verbatim from a live run:
	//
	//	#BASECAT · base
	//	$222
	//	Volume +3272%
	//	30m -75%   60m -70%   24h -52%
	//	Median: $7
	//
	// Every longer window was deeply negative — nothing rose. The token
	// barely trades in the pools this pipeline can see, so its 10m median was
	// seven dollars and one ordinary swap became a four-figure percentage.
	svc, notifier, settings := newService(t)
	if err := settings.Update(func(rt *config.Runtime) { rt.MinVolumeUSD = 1000 }); err != nil {
		t.Fatal(err)
	}
	b := base()

	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 7)
	}
	advance(svc, b.Add(31*time.Minute), 222)

	if notifier.count() != 0 {
		t.Fatalf("got %d alerts for a $222 minute; below the floor nothing is an event",
			notifier.count())
	}

	// The same relative move at a size worth reading still alerts. A fresh
	// service, because walking this one up from seven dollars to five
	// thousand is itself a spike several times over.
	svc2, notifier2, settings2 := newService(t)
	if err := settings2.Update(func(rt *config.Runtime) { rt.MinVolumeUSD = 1000 }); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 30; i++ {
		advance(svc2, b.Add(time.Duration(i)*time.Minute), 5000)
	}
	advance(svc2, b.Add(31*time.Minute), 200000)

	if notifier2.count() != 1 {
		t.Fatalf("got %d alerts, want 1 — the floor must not silence real volume",
			notifier2.count())
	}
	advance(svc, b.Add(61*time.Minute), 200000)

	if notifier.count() != 1 {
		t.Fatalf("got %d alerts, want 1 — the floor must not silence real volume",
			notifier.count())
	}
}
