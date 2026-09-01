// Package service wires the sources, the volume engine, the detector and the
// bot into one running process.
package service

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

// PriceProvider is the aggregator used to resolve USD prices.
type PriceProvider interface {
	Name() string
	Prices(ctx context.Context, toks []domain.Token) (map[string]float64, error)
}

// maxPriceAge is how long a quote stays usable.
//
// A stale price is worse than no price: valuing this minute's swaps at a figure
// from twenty minutes ago silently rewrites the volume, and the error looks
// like real market movement. Past this age the cache reports "unknown", the
// trade is skipped, and the shortfall shows up as a coverage gap instead.
const maxPriceAge = 10 * time.Minute

type quote struct {
	usd       float64
	retrieved time.Time
}

// PriceCache holds USD prices for tracked tokens and for the quote assets on
// the other side of their pools.
//
// Quote assets matter as much as the tracked ones: a freshly listed token has
// no aggregator price yet, but its WETH or USDC counterpart always does, and a
// swap is worth the same measured from either side.
type PriceCache struct {
	provider PriceProvider
	log      *slog.Logger

	mu     sync.RWMutex
	quotes map[string]quote
	extra  map[string]domain.Token // quote assets to keep priced
	now    func() time.Time
}

func NewPriceCache(p PriceProvider, log *slog.Logger) *PriceCache {
	return &PriceCache{
		provider: p,
		log:      log.With("component", "prices"),
		quotes:   map[string]quote{},
		extra:    map[string]domain.Token{},
		now:      time.Now,
	}
}

func priceKey(chain domain.Chain, addr string) string {
	return string(chain) + ":" + strings.ToLower(addr)
}

// TrackQuoteAssets registers the pool counterparties that need a price.
func (c *PriceCache) TrackQuoteAssets(toks []domain.Token) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range toks {
		if t.Address == "" {
			continue
		}
		c.extra[priceKey(t.Chain, t.Address)] = t
	}
}

// Refresh re-prices the watch list plus the tracked quote assets.
func (c *PriceCache) Refresh(ctx context.Context, tracked []domain.Token) error {
	c.mu.RLock()
	want := make([]domain.Token, 0, len(tracked)+len(c.extra))
	want = append(want, tracked...)
	seen := make(map[string]bool, len(tracked))
	for _, t := range tracked {
		seen[priceKey(t.Chain, t.Address)] = true
	}
	for k, t := range c.extra {
		if !seen[k] {
			want = append(want, t)
		}
	}
	c.mu.RUnlock()

	if len(want) == 0 {
		return nil
	}

	got, err := c.provider.Prices(ctx, want)
	// A partial result is still worth keeping: the provider may have answered
	// for several chains before failing on one.
	c.mu.Lock()
	now := c.now()
	for k, v := range got {
		if v > 0 {
			c.quotes[k] = quote{usd: v, retrieved: now}
		}
	}
	c.mu.Unlock()
	return err
}

// PriceUSD returns a fresh price, or false when none is available.
func (c *PriceCache) PriceUSD(chain domain.Chain, addr string) (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	q, ok := c.quotes[priceKey(chain, addr)]
	if !ok || q.usd <= 0 {
		return 0, false
	}
	if c.now().Sub(q.retrieved) > maxPriceAge {
		return 0, false
	}
	return q.usd, true
}

// Size reports how many prices are cached, for diagnostics.
func (c *PriceCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.quotes)
}
