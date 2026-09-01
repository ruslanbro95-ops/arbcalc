package volume

import (
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/dedup"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// dedupTTL spans the longest baseline window plus a margin, so a trade replayed
// by a slow source is still recognized as a duplicate.
const dedupTTL = 25 * time.Hour

// Stats are ingestion counters, surfaced through the bot as a data-quality view.
type Stats struct {
	Accepted  int64
	Duplicate int64
	// TooLate counts trades whose minute was already sealed. A nonzero value
	// means a source lags further behind than the seal delay allows.
	TooLate int64
}

// Snapshot is everything the detector needs about one token at one minute.
type Snapshot struct {
	Token     domain.Token
	Minute    time.Time
	Current   Bucket
	Baselines map[int]Baseline
	// HealthyMinutes/TotalMinutes describe the last hour of data quality.
	HealthyMinutes int
	TotalMinutes   int
}

// Engine owns one Series per tracked token and the shared dedup set.
type Engine struct {
	mu     sync.RWMutex
	series map[string]*Series
	stats  Stats
	dedup  *dedup.Set
}

func NewEngine() *Engine {
	return &Engine{
		series: make(map[string]*Series),
		dedup:  dedup.New(dedupTTL),
	}
}

// seriesFor returns the token's series, creating it on first use.
func (e *Engine) seriesFor(key string) *Series {
	e.mu.RLock()
	s := e.series[key]
	e.mu.RUnlock()
	if s != nil {
		return s
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if s = e.series[key]; s == nil {
		s = NewSeries()
		e.series[key] = s
	}
	return s
}

// Ingest normalizes one trade into its minute bucket. It returns false when the
// trade was dropped, which happens for a duplicate or for a minute that has
// already been sealed and published.
func (e *Engine) Ingest(t domain.Trade) bool {
	if e.dedup.Seen(t.DedupKey(), time.Now()) {
		e.bump(&e.stats.Duplicate)
		return false
	}
	tokenKey := string(t.Chain) + ":" + lower(t.TokenAddress)
	if !e.seriesFor(tokenKey).Add(t.Timestamp, t.Side == domain.SideBuy, t.USDVolume) {
		e.bump(&e.stats.TooLate)
		return false
	}
	e.bump(&e.stats.Accepted)
	return true
}

// Seal closes a minute for one token. healthy must reflect whether every source
// feeding that token's chain stayed available for the whole minute — passing
// true during an outage is what turns a gap into a false anomaly later.
func (e *Engine) Seal(tokenKey string, minute time.Time, healthy bool) {
	e.seriesFor(tokenKey).Seal(minute, healthy)
}

// Snapshot gathers the sealed minute and its four baselines.
func (e *Engine) Snapshot(tok domain.Token, minute time.Time) Snapshot {
	s := e.seriesFor(tok.Key())
	cur, _ := s.Get(minute)
	healthy, total := s.Health(minute.Add(time.Minute), Window60m)

	snap := Snapshot{
		Token:          tok,
		Minute:         minute,
		Current:        cur,
		Baselines:      make(map[int]Baseline, 4),
		HealthyMinutes: healthy,
		TotalMinutes:   total,
	}
	for _, w := range []int{Window10m, Window30m, Window60m, Window24h} {
		snap.Baselines[w] = s.BaselineFor(minute, w)
	}
	return snap
}

// Stats returns a copy of the ingestion counters.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// DedupSize reports how many trade keys are currently held.
func (e *Engine) DedupSize() int { return e.dedup.Len() }

func (e *Engine) bump(p *int64) {
	e.mu.Lock()
	*p++
	e.mu.Unlock()
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// RestoreMinute reinstates a sealed bucket read back from disk.
//
// It exists so medians survive a restart: rebuilding a 24h baseline from live
// data alone would leave the service unable to judge a full day of minutes.
// Restored buckets arrive already sealed and keep their recorded quality, so a
// past outage stays MISSING instead of returning as a real zero.
func (e *Engine) RestoreMinute(tokenKey string, b Bucket) error {
	return e.seriesFor(tokenKey).restore(b)
}
