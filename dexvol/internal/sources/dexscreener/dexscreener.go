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

// BatchSize is how many token addresses the /latest/dex/tokens endpoint accepts
// in one call. Batching is what keeps price polling at four requests a minute
// instead of four per token.
const BatchSize = 30

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
	Volume   struct {
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

// Prices resolves USD prices for a batch of tokens.
//
// Tokens are grouped by chain and then chunked, because the batch endpoint
// takes one chain's worth of addresses at a time.
func (c *Client) Prices(ctx context.Context, toks []domain.Token) (map[string]float64, error) {
	out := make(map[string]float64, len(toks))

	byChain := map[domain.Chain][]domain.Token{}
	for _, t := range toks {
		byChain[t.Chain] = append(byChain[t.Chain], t)
	}

	for chain, list := range byChain {
		if _, ok := chainID(chain); !ok {
			continue
		}
		for start := 0; start < len(list); start += BatchSize {
			end := min(start+BatchSize, len(list))
			chunk := list[start:end]

			addrs := make([]string, len(chunk))
			for i, t := range chunk {
				addrs[i] = t.Address
			}
			endpoint := fmt.Sprintf("%s/latest/dex/tokens/%s", c.baseURL, url.PathEscape(strings.Join(addrs, ",")))

			var resp tokensResponse
			if err := c.http.GetJSON(ctx, endpoint, &resp); err != nil {
				return out, err
			}
			// A token appears once per pool; the deepest pool is the most
			// trustworthy quote, so the highest-liquidity pair wins.
			best := map[string]float64{}
			for _, p := range resp.Pairs {
				price, err := strconv.ParseFloat(p.PriceUsd, 64)
				if err != nil || price <= 0 {
					continue
				}
				key := string(chain) + ":" + strings.ToLower(p.BaseToken.Address)
				if p.Liquidity.USD >= best[key] {
					best[key] = p.Liquidity.USD
					out[key] = price
				}
			}
		}
	}
	return out, nil
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
