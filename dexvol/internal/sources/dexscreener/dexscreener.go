// Package dexscreener adapts the DEX Screener API.
//
// Per docs/RESEARCH.md this provider is used for pool discovery, pricing and
// reference volume — never for per-minute volume. Its aggregates are rolling
// windows (m5/h1/h6/h24), and a rolling five-minute figure cannot be turned
// back into the volume of a specific calendar minute without inventing data.
package dexscreener

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/sources"
)

// DefaultBaseURL is the public API root.
const DefaultBaseURL = "https://api.dexscreener.com"

// rateLimit is the documented free-tier ceiling for the pairs/tokens endpoints.
// Staying a little under it leaves room for the occasional retry.
const rateLimit = 280

// chainID resolves the DEX Screener identifier from the shared chain registry,
// so a network added there needs no edit here.
func chainID(c domain.Chain) (string, bool) {
	info, ok := domain.Info(c)
	if !ok || info.DexScreenerID == "" {
		return "", false
	}
	return info.DexScreenerID, true
}

// Client talks to DEX Screener.
type Client struct {
	http    *sources.HTTP
	baseURL string
}

func New() *Client { return NewWithBase(DefaultBaseURL) }

// NewWithBase lets the tests point the client at a stub server.
func NewWithBase(base string) *Client {
	return &Client{
		http:    sources.NewHTTP("dexscreener", rateLimit, 20*time.Second),
		baseURL: strings.TrimRight(base, "/"),
	}
}

func (c *Client) Name() string { return "dexscreener" }

// pair mirrors the fields of the API's pair object that this service uses.
type pair struct {
	ChainID     string `json:"chainId"`
	DexID       string `json:"dexId"`
	PairAddress string `json:"pairAddress"`
	BaseToken   struct {
		Address string `json:"address"`
		Symbol  string `json:"symbol"`
	} `json:"baseToken"`
	QuoteToken struct {
		Address string `json:"address"`
		Symbol  string `json:"symbol"`
	} `json:"quoteToken"`
	// PriceUsd is a string in the API, and is empty for a pool with no
	// resolvable USD route.
	PriceUsd string `json:"priceUsd"`
	// PriceNative is the base token priced in quote-token units, which is what
	// lets a pool value its quote side too.
	PriceNative string `json:"priceNative"`
	Volume      struct {
		H24 float64 `json:"h24"`
		H6  float64 `json:"h6"`
		H1  float64 `json:"h1"`
		M5  float64 `json:"m5"`
	} `json:"volume"`
	Liquidity struct {
		USD float64 `json:"usd"`
	} `json:"liquidity"`
}

type tokensResponse struct {
	Pairs []pair `json:"pairs"`
}

// DiscoverPools returns every pool DEX Screener knows for the token.
func (c *Client) DiscoverPools(ctx context.Context, tok domain.Token) ([]domain.Pool, error) {
	id, ok := chainID(tok.Chain)
	if !ok {
		return nil, fmt.Errorf("dexscreener: unsupported chain %q", tok.Chain)
	}

	endpoint := fmt.Sprintf("%s/token-pairs/v1/%s/%s", c.baseURL, id, url.PathEscape(tok.Address))
	var pairs []pair
	if err := c.http.GetJSON(ctx, endpoint, &pairs); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	out := make([]domain.Pool, 0, len(pairs))
	for _, p := range pairs {
		if p.PairAddress == "" {
			continue
		}
		out = append(out, domain.Pool{
			Chain:        tok.Chain,
			Address:      p.PairAddress,
			DEX:          p.DexID,
			BaseAddr:     p.BaseToken.Address,
			BaseSymbol:   p.BaseToken.Symbol,
			QuoteAddr:    p.QuoteToken.Address,
			QuoteSymbol:  p.QuoteToken.Symbol,
			LiquidityUSD: p.Liquidity.USD,
			Volume24hUSD: p.Volume.H24,
			DiscoveredAt: now,
			Source:       c.Name(),
		})
	}
	return out, nil
}

// Prices resolves USD prices, one request per token.
//
// It used to batch addresses into /latest/dex/tokens/{a,b,c}. That endpoint
// answers with pairs rather than with tokens, and it caps the answer at thirty
// pairs in total — so one token with thirty pools of its own consumes the whole
// response and every other address in the batch comes back with no price at
// all. Measured: asking for CAKE and 牛来 together returns thirty CAKE pairs and
// not one mention of 牛来, whose swaps were then dropped as unpriced and whose
// measured coverage read 0%. Batching here was not an optimization, it was
// silent data loss weighted towards exactly the tokens a watch list is least
// likely to notice.
func (c *Client) Prices(ctx context.Context, toks []domain.Token) (map[string]float64, error) {
	out := make(map[string]float64, len(toks))
	var firstErr error

	for _, t := range toks {
		id, ok := chainID(t.Chain)
		if !ok || t.Address == "" {
			continue
		}
		endpoint := fmt.Sprintf("%s/token-pairs/v1/%s/%s", c.baseURL, id, url.PathEscape(t.Address))
		var pairs []pair
		if err := c.http.GetJSON(ctx, endpoint, &pairs); err != nil {
			// One token failing must not cost the others their price: a
			// partial map still lets most trades be valued, and the ones that
			// cannot be are counted as a coverage gap rather than as zero.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if price, ok := priceFrom(pairs, t.Address); ok {
			out[string(t.Chain)+":"+strings.ToLower(t.Address)] = price
		}
	}
	return out, firstErr
}

// priceFrom picks the deepest pool that can value the token and reads the price
// off whichever side of it the token sits on.
//
// Reading only the base side was the other half of the batching bug. A quote
// asset like WETH is the base token of almost none of its pools, so it ended up
// priced from whatever pair happened to come back — WETH measured $0.0000108,
// which is the price of the memecoin it was paired against. A pool prices both
// of its tokens: priceUsd is the base in dollars and priceNative is the base in
// quote units, so the quote is worth priceUsd / priceNative.
func priceFrom(pairs []pair, addr string) (float64, bool) {
	want := strings.ToLower(addr)

	var best, bestLiquidity float64
	found := false
	for _, p := range pairs {
		usd, err := strconv.ParseFloat(p.PriceUsd, 64)
		if err != nil || usd <= 0 {
			continue
		}

		var price float64
		switch want {
		case strings.ToLower(p.BaseToken.Address):
			price = usd
		case strings.ToLower(p.QuoteToken.Address):
			native, err := strconv.ParseFloat(p.PriceNative, 64)
			if err != nil || native <= 0 {
				continue
			}
			price = usd / native
		default:
			continue
		}

		// The deepest pool is the most trustworthy quote.
		if !found || p.Liquidity.USD >= bestLiquidity {
			found, best, bestLiquidity = true, price, p.Liquidity.USD
		}
	}
	return best, found
}

// Volume reports DEX Screener's own aggregate for the token, summed across all
// its pools. cmd/coverage uses it as one of the reference points.
func (c *Client) Volume(ctx context.Context, tok domain.Token) (sources.Reference, error) {
	pools, err := c.DiscoverPools(ctx, tok)
	if err != nil {
		return sources.Reference{}, err
	}

	id, _ := chainID(tok.Chain)
	endpoint := fmt.Sprintf("%s/token-pairs/v1/%s/%s", c.baseURL, id, url.PathEscape(tok.Address))
	var pairs []pair
	if err := c.http.GetJSON(ctx, endpoint, &pairs); err != nil {
		return sources.Reference{}, err
	}

	ref := sources.Reference{Source: c.Name(), Pools: pools, RetrievedAt: time.Now().UTC()}
	for _, p := range pairs {
		// Only count pools where the tracked token is the base side, or a
		// token that appears on both sides of two pools would be double
		// counted.
		if !strings.EqualFold(p.BaseToken.Address, tok.Address) {
			continue
		}
		ref.H1USD += p.Volume.H1
		ref.H24USD += p.Volume.H24
	}
	return ref, nil
}
