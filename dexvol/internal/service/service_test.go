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
	settings.Update(func(rt *config.Runtime) { rt.Tokens = []domain.Token{testToken} })

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

// seal closes exactly the given minute: the seal deadline for minute m falls
// at m + SealDelay, so any instant just past that lands on m.
func seal(svc *Service, minute time.Time) {
	svc.sealDue(context.Background(), minute.Add(svc.static.SealDelay).Add(time.Second))
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

func TestSustainedAnomalyIsNotSpammed(t *testing.T) {
	svc, notifier, _ := newService(t)
	b := base()

	for i := 1; i <= 30; i++ {
		advance(svc, b.Add(time.Duration(i)*time.Minute), 100)
	}

	// Four consecutive elevated minutes, none of them double the first.
	for i, usd := range []float64{150, 170, 180, 160} {
		advance(svc, b.Add(time.Duration(31+i)*time.Minute), usd)
	}

	if notifier.count() != 1 {
		t.Fatalf("one continuing anomaly should produce one message, got %d", notifier.count())
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
