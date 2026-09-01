// Package sources defines the contracts every data provider implements and the
// shared HTTP plumbing they use.
//
// The split into three narrow interfaces is deliberate. The research stage
// (docs/RESEARCH.md) found that no free provider covers all five chains for
// per-minute trades, but that different providers are individually excellent at
// discovery, at pricing, and at raw trades. Separate interfaces let each
// provider be used only for what it is actually good at.
package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// PoolDiscoverer finds every pool trading a token.
//
// This runs repeatedly rather than once: a token can list on a new DEX at any
// time, and a pool list frozen at add-time is the most likely way to silently
// lose coverage.
type PoolDiscoverer interface {
	Name() string
	DiscoverPools(ctx context.Context, tok domain.Token) ([]domain.Pool, error)
}

// PriceSource resolves a token's USD price, used to value a swap whose pool
// does not trade against a stablecoin.
type PriceSource interface {
	Name() string
	Prices(ctx context.Context, toks []domain.Token) (map[string]float64, error)
}

// ReferenceVolume reports a provider's own view of a token's traded volume.
// It is not used for alerting — only by cmd/coverage, to measure how much of
// the market our own pipeline reconstructed.
type ReferenceVolume interface {
	Name() string
	Volume(ctx context.Context, tok domain.Token) (Reference, error)
}

// Reference is a provider's aggregate view of one token.
type Reference struct {
	Source      string
	H1USD       float64
	H24USD      float64
	Pools       []domain.Pool
	RetrievedAt time.Time
}

// TradeSource streams normalized trades for the pools it was given.
//
// Implementations must report health honestly through Healthy: the engine turns
// an unhealthy interval into MISSING minutes, and a source that hides an outage
// behind a silent zero would manufacture false anomalies on recovery.
type TradeSource interface {
	Name() string
	Chain() domain.Chain
	// SetPools replaces the watched pool set. Called after every discovery run.
	SetPools(pools []domain.Pool)
	// Poll fetches trades produced since the last successful call and sends
	// them to out. It must be safe to call on a ticker.
	Poll(ctx context.Context, out chan<- domain.Trade) error
	// Healthy reports whether the last poll cycle succeeded.
	Healthy() bool
}

// HTTP is a rate-limited JSON client shared by the aggregator adapters.
//
// The limiter is the point: the free tiers this project runs on are the binding
// constraint, and exceeding one gets the whole source throttled or banned. Each
// adapter constructs its own HTTP with the published limit for its provider.
type HTTP struct {
	client  *http.Client
	limiter *limiter
	name    string
	// userAgent identifies this service to providers, which is basic courtesy
	// on a free endpoint and makes our traffic attributable if we misbehave.
	userAgent string
}

// NewHTTP builds a client capped at perMinute requests.
//
// The burst is intentionally a tenth of the budget: a full-size burst would let
// a startup stampede spend the entire per-minute allowance in one second and
// trip the provider's throttle on the very first tick.
func NewHTTP(name string, perMinute int, timeout time.Duration) *HTTP {
	burst := perMinute / 10
	if burst < 1 {
		burst = 1
	}
	return &HTTP{
		client:    &http.Client{Timeout: timeout},
		limiter:   newLimiter(perMinute, burst),
		name:      name,
		userAgent: "dexvol-monitor/1.0",
	}
}

// GetJSON performs a rate-limited GET and decodes the body into out.
func (h *HTTP) GetJSON(ctx context.Context, url string, out any) error {
	if err := h.limiter.wait(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", h.userAgent)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", h.name, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return fmt.Errorf("%s: read body: %w", h.name, err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return &RateLimitError{Source: h.name, RetryAfter: retryAfter(resp)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: http %d: %s", h.name, resp.StatusCode, truncate(string(body), 200))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("%s: decode: %w", h.name, err)
	}
	return nil
}

// PostJSON performs a rate-limited POST, used for JSON-RPC.
func (h *HTTP) PostJSON(ctx context.Context, url string, payload, out any) error {
	if err := h.limiter.wait(ctx); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, byteReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", h.userAgent)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", h.name, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("%s: read body: %w", h.name, err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return &RateLimitError{Source: h.name, RetryAfter: retryAfter(resp)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: http %d: %s", h.name, resp.StatusCode, truncate(string(raw), 200))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s: decode: %w", h.name, err)
	}
	return nil
}

// RateLimitError lets callers back off instead of hammering a throttled API.
type RateLimitError struct {
	Source     string
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("%s: rate limited, retry after %s", e.Source, e.RetryAfter)
}

func retryAfter(resp *http.Response) time.Duration {
	if v := resp.Header.Get("Retry-After"); v != "" {
		if d, err := time.ParseDuration(v + "s"); err == nil {
			return d
		}
	}
	return 30 * time.Second
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
