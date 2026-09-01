// Package store persists sealed minutes and raw trades.
//
// It is a pair of append-only JSONL files rather than an embedded database.
// The working set is small — one row per token per minute, so 1,440 rows a day
// for a token — and a plain file keeps the service dependency-free and its
// state inspectable with the tools already on any machine. Recovery from a
// crash is trivial: a torn final line is dropped on load.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/volume"
)

// MinuteRow is one sealed minute for one token.
type MinuteRow struct {
	TokenKey string    `json:"token"`
	Minute   time.Time `json:"minute"`
	Buy      float64   `json:"buy"`
	Sell     float64   `json:"sell"`
	Total    float64   `json:"total"`
	Trades   int       `json:"trades"`
	// Quality is persisted so a restart cannot resurrect an outage as a real
	// zero. Losing it would poison the restored medians.
	Quality volume.Quality `json:"quality"`
}

// Store owns the on-disk state.
type Store struct {
	dir string

	mu      sync.Mutex
	minutes *os.File
	trades  *os.File
	// tradesDay is the UTC date the open raw-trade file belongs to, so the
	// file rotates at midnight and retention can delete whole days.
	tradesDay string
}

// Open prepares the directory and the append handles.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "trades"), 0o700); err != nil {
		return nil, err
	}
	s := &Store{dir: dir}

	f, err := os.OpenFile(s.minutesPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	s.minutes = f
	return s, nil
}

func (s *Store) minutesPath() string { return filepath.Join(s.dir, "minutes.jsonl") }

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	if s.minutes != nil {
		errs = append(errs, s.minutes.Close())
	}
	if s.trades != nil {
		errs = append(errs, s.trades.Close())
	}
	return errors.Join(errs...)
}

// AppendMinute records one sealed minute.
func (s *Store) AppendMinute(row MinuteRow) error {
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.minutes.Write(append(b, '\n'))
	return err
}

// LoadMinutes reads back every row not older than `since`.
//
// A truncated final line — the signature of a crash mid-write — is skipped
// rather than treated as corruption, because losing one minute is a much
// smaller problem than refusing to start.
func (s *Store) LoadMinutes(since time.Time) ([]MinuteRow, error) {
	f, err := os.Open(s.minutesPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []MinuteRow
	sc := bufio.NewScanner(f)
	// Minute rows are small, but a very long line should not kill the scan.
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var row MinuteRow
		if err := json.Unmarshal(line, &row); err != nil {
			continue
		}
		if row.Minute.Before(since) {
			continue
		}
		out = append(out, row)
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return out, err
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Minute.Before(out[j].Minute) })
	return out, nil
}

// CompactMinutes rewrites the minute log keeping only rows newer than `since`.
//
// Without it the file would grow without bound; with it, startup cost stays
// proportional to the 24h window the medians actually use.
func (s *Store) CompactMinutes(since time.Time) error {
	rows, err := s.LoadMinutes(since)
	if err != nil {
		return err
	}

	tmp := s.minutesPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			continue
		}
		w.Write(append(b, '\n'))
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.minutes != nil {
		s.minutes.Close()
	}
	if err := os.Rename(tmp, s.minutesPath()); err != nil {
		return err
	}
	s.minutes, err = os.OpenFile(s.minutesPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	return err
}

// AppendTrade records one raw trade for debugging and for verifying a source's
// output against an aggregator after the fact.
func (s *Store) AppendTrade(t domain.Trade) error {
	day := t.Timestamp.UTC().Format("2006-01-02")

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trades == nil || s.tradesDay != day {
		if s.trades != nil {
			s.trades.Close()
		}
		f, err := os.OpenFile(s.tradesPath(day), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		s.trades, s.tradesDay = f, day
	}

	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = s.trades.Write(append(b, '\n'))
	return err
}

func (s *Store) tradesPath(day string) string {
	return filepath.Join(s.dir, "trades", day+".jsonl")
}

// PruneTrades deletes whole days of raw trades older than the retention window.
func (s *Store) PruneTrades(retention time.Duration, now time.Time) (int, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "trades"))
	if err != nil {
		return 0, err
	}
	cutoff := now.UTC().Add(-retention)

	s.mu.Lock()
	openDay := s.tradesDay
	s.mu.Unlock()

	removed := 0
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) != ".jsonl" {
			continue
		}
		day := name[:len(name)-len(".jsonl")]
		if day == openDay {
			continue // never delete the file currently being written
		}
		ts, err := time.Parse("2006-01-02", day)
		if err != nil {
			continue
		}
		// A day file is only safe to delete once its last minute is past the
		// cutoff, hence the end of that day rather than its start.
		if ts.AddDate(0, 0, 1).After(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, "trades", name)); err == nil {
			removed++
		}
	}
	return removed, nil
}

// Restore replays persisted minutes into the engine so the medians survive a
// restart. Without it a 24h baseline would need a full day to become usable
// again, and the service would be blind to slow anomalies for that whole time.
func (s *Store) Restore(eng *volume.Engine, tokens []domain.Token, now time.Time) (int, error) {
	since := now.UTC().Add(-25 * time.Hour)
	rows, err := s.LoadMinutes(since)
	if err != nil {
		return 0, err
	}

	wanted := make(map[string]bool, len(tokens))
	for _, t := range tokens {
		wanted[t.Key()] = true
	}

	n := 0
	for _, row := range rows {
		if !wanted[row.TokenKey] {
			continue
		}
		if err := eng.RestoreMinute(row.TokenKey, volume.Bucket{
			Minute:  row.Minute.UTC(),
			Buy:     row.Buy,
			Sell:    row.Sell,
			Total:   row.Total,
			Trades:  row.Trades,
			Quality: row.Quality,
			Sealed:  true,
		}); err != nil {
			return n, fmt.Errorf("restore %s at %s: %w", row.TokenKey, row.Minute, err)
		}
		n++
	}
	return n, nil
}
