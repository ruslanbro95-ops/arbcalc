package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

func open(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func TestMinutesRoundTrip(t *testing.T) {
	s, _ := open(t)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		if err := s.AppendMinute(MinuteRow{
			TokenKey: "base:0xaa", Minute: base.Add(time.Duration(i) * time.Minute),
			Total: float64(100 * (i + 1)), Quality: volume.QualityOK,
		}); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := s.LoadMinutes(base.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].Total != 100 || rows[2].Total != 300 {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestLoadSkipsRowsBeforeCutoff(t *testing.T) {
	s, _ := open(t)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	s.AppendMinute(MinuteRow{TokenKey: "k", Minute: base.Add(-48 * time.Hour), Total: 1})
	s.AppendMinute(MinuteRow{TokenKey: "k", Minute: base, Total: 2})

	rows, _ := s.LoadMinutes(base.Add(-time.Hour))
	if len(rows) != 1 || rows[0].Total != 2 {
		t.Fatalf("got %+v", rows)
	}
}

func TestTornFinalLineIsSkipped(t *testing.T) {
	// A crash mid-write leaves a truncated last line. Losing that one minute
	// is a far smaller problem than refusing to start.
	s, dir := open(t)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	s.AppendMinute(MinuteRow{TokenKey: "k", Minute: base, Total: 5, Quality: volume.QualityOK})

	f, err := os.OpenFile(filepath.Join(dir, "minutes.jsonl"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"token":"k","minute":"2026-09-01T12:0`)
	f.Close()

	rows, err := s.LoadMinutes(base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("a torn line must not fail the load: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
}

func TestCompactDropsOldRowsAndKeepsAppending(t *testing.T) {
	s, dir := open(t)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	s.AppendMinute(MinuteRow{TokenKey: "k", Minute: base.Add(-72 * time.Hour), Total: 1})
	s.AppendMinute(MinuteRow{TokenKey: "k", Minute: base, Total: 2})

	if err := s.CompactMinutes(base.Add(-25 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.LoadMinutes(time.Time{})
	if len(rows) != 1 || rows[0].Total != 2 {
		t.Fatalf("after compaction: %+v", rows)
	}

	// The append handle must still be usable after the file was replaced.
	if err := s.AppendMinute(MinuteRow{TokenKey: "k", Minute: base.Add(time.Minute), Total: 3}); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.LoadMinutes(time.Time{})
	if len(rows) != 2 {
		t.Fatalf("expected the new row to land: %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "minutes.jsonl.tmp")); !os.IsNotExist(err) {
		t.Fatal("the temp file should have been renamed away")
	}
}

func TestRawTradesRotateByDay(t *testing.T) {
	s, dir := open(t)
	d1 := time.Date(2026, 9, 1, 23, 59, 0, 0, time.UTC)
	d2 := time.Date(2026, 9, 2, 0, 1, 0, 0, time.UTC)

	s.AppendTrade(domain.Trade{Timestamp: d1, TxHash: "0xA"})
	s.AppendTrade(domain.Trade{Timestamp: d2, TxHash: "0xB"})

	for _, day := range []string{"2026-09-01", "2026-09-02"} {
		if _, err := os.Stat(filepath.Join(dir, "trades", day+".jsonl")); err != nil {
			t.Fatalf("missing %s: %v", day, err)
		}
	}
}

func TestPruneKeepsTheOpenDayAndRecentOnes(t *testing.T) {
	s, dir := open(t)
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)

	s.AppendTrade(domain.Trade{Timestamp: time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC)})
	s.AppendTrade(domain.Trade{Timestamp: time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)})
	s.AppendTrade(domain.Trade{Timestamp: now}) // the currently open file

	removed, err := s.PruneTrades(48*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed %d files, want 1", removed)
	}
	if _, err := os.Stat(filepath.Join(dir, "trades", "2026-09-01.jsonl")); !os.IsNotExist(err) {
		t.Fatal("the old day should be gone")
	}
	for _, day := range []string{"2026-09-09", "2026-09-10"} {
		if _, err := os.Stat(filepath.Join(dir, "trades", day+".jsonl")); err != nil {
			t.Fatalf("%s should have been kept", day)
		}
	}
}

func TestRestoreRebuildsMedians(t *testing.T) {
	// The point of persistence: after a restart a 24h baseline is usable
	// immediately instead of needing a full day to warm up again.
	s, _ := open(t)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	tok := domain.Token{Chain: domain.ChainBase, Address: "0xAA", Symbol: "ABC"}

	for i := 1; i <= 30; i++ {
		s.AppendMinute(MinuteRow{
			TokenKey: tok.Key(), Minute: base.Add(time.Duration(i) * time.Minute),
			Total: 100, Quality: volume.QualityOK,
		})
	}

	eng := volume.NewEngine()
	n, err := s.Restore(eng, []domain.Token{tok}, base.Add(31*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 30 {
		t.Fatalf("restored %d rows, want 30", n)
	}

	snap := eng.Snapshot(tok, base.Add(31*time.Minute))
	bl := snap.Baselines[volume.Window10m]
	if !bl.Usable || bl.Median != 100 {
		t.Fatalf("restored baseline = %+v", bl)
	}
}

func TestRestoreKeepsOutagesMissing(t *testing.T) {
	// If a persisted outage came back as a real zero it would sink every
	// restored median.
	s, _ := open(t)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	tok := domain.Token{Chain: domain.ChainBase, Address: "0xAA"}

	for i := 1; i <= 10; i++ {
		s.AppendMinute(MinuteRow{TokenKey: tok.Key(), Minute: base.Add(time.Duration(i) * time.Minute),
			Total: 100, Quality: volume.QualityOK})
	}
	for i := 11; i <= 15; i++ {
		s.AppendMinute(MinuteRow{TokenKey: tok.Key(), Minute: base.Add(time.Duration(i) * time.Minute),
			Total: 0, Quality: volume.QualityMissing})
	}

	eng := volume.NewEngine()
	if _, err := s.Restore(eng, []domain.Token{tok}, base.Add(16*time.Minute)); err != nil {
		t.Fatal(err)
	}
	bl := eng.Snapshot(tok, base.Add(16*time.Minute)).Baselines[volume.Window10m]
	if bl.Median != 100 {
		t.Fatalf("median = %v, want 100 — the outage must stay excluded", bl.Median)
	}
	if bl.Samples != 5 {
		t.Fatalf("samples = %d, want 5", bl.Samples)
	}
}

func TestRestoreIgnoresTokensNoLongerTracked(t *testing.T) {
	s, _ := open(t)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	s.AppendMinute(MinuteRow{TokenKey: "base:0xgone", Minute: base, Total: 1, Quality: volume.QualityOK})

	eng := volume.NewEngine()
	n, err := s.Restore(eng, []domain.Token{{Chain: domain.ChainBase, Address: "0xAA"}}, base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("restored %d rows for an untracked token", n)
	}
}
