// Package geckoterminal adapts the GeckoTerminal API.
//
// Its trades endpoint is the only free one that returns real per-trade data
// without a key, but it works one pool at a time under a 30 req/min ceiling.
// At the 15-second cadence the spec asks for, that budget covers roughly seven
// pools in total — less than a single mid-liquidity token. So this provider is
// used for discovery, pricing, and as an independent reference in
// cmd/coverage, and its trades endpoint is used only for spot verification,
// never as the ingestion path. See docs/RESEARCH.md §3.1.
package geckoterminal

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

const DefaultBaseURL = "https://api.geckoterminal.com/api/v2"

// rateLimit sits just under the documented 30 calls/min so a burst of retries
// does not cross the line.
const rateLimit = 25

// network maps our chain to GeckoTerminal's network id.
//
// Robinhood Chain is deliberately absent: its GeckoTerminal network id could
// not be confirmed while this was built, and inventing one would produce silent
// 404s that look like "no pools" instead of "unsupported".
var network = map[domain.Chain]string{
	domain.ChainEthereum: "eth",
	domain.ChainBNB:      "bsc",
	domain.ChainSolana:   "solana",
	domain.ChainBase:     "base",
}

type Client struct {
	http    *sources.HTTP
	baseURL string
}

func New() *Client { return NewWithBase(DefaultBaseURL) }

func NewWithBase(base string) *Client {
	return &Client{
		http:    sources.NewHTTP("geckoterminal", rateLimit, 20*time.Second),
		baseURL: strings.TrimRight(base, "/"),
	}
}

func (c *Client) Name() string { return "geckoterminal" }

// Supports reports whether the chain has a known GeckoTerminal network id.
func (c *Client) Supports(chain domain.Chain) bool {
	_, ok := network[chain]
	return ok
}

// The API speaks JSON:API, so every payload is data/attributes/relationships.
type poolsResponse struct {
	Data []struct {
		ID         string `json:"id"`
		Attributes struct {
			Address           string `json:"address"`
			Name              string `json:"name"`
			BaseTokenPriceUSD string `json:"base_token_price_usd"`
			ReserveInUSD      string `json:"reserve_in_usd"`
			VolumeUSD         struct {
				H1  string `json:"h1"`
				H24 string `json:"h24"`
			} `json:"volume_usd"`
		} `json:"attributes"`
		Relationships struct {
			BaseToken  relRef `json:"base_token"`
			QuoteToken relRef `json:"quote_token"`
			DEX        relRef `json:"dex"`
		} `json:"relationships"`
	} `json:"data"`
}

type relRef struct {
	Data struct {
		ID string `json:"id"`
	} `json:"data"`
}

// tokenAddressFromID strips the network prefix from an id like
// "eth_0xdac17f...", which is how JSON:API references a token here.
func tokenAddressFromID(id string) string {
	if i := strings.IndexByte(id, '_'); i >= 0 {
		return id[i+1:]
	}
	return id
}

func parseFloat(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

// DiscoverPools returns the token's pools as GeckoTerminal sees them. It is the
// second opinion that catches a pool DEX Screener missed.
func (c *Client) DiscoverPools(ctx context.Context, tok domain.Token) ([]domain.Pool, error) {
	net, ok := network[tok.Chain]
	if !ok {
		return nil, fmt.Errorf("geckoterminal: no network id for chain %q", tok.Chain)
	}

	endpoint := fmt.Sprintf("%s/networks/%s/tokens/%s/pools", c.baseURL, net, url.PathEscape(tok.Address))
	var resp poolsResponse
	if err := c.http.GetJSON(ctx, endpoint, &resp); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	out := make([]domain.Pool, 0, len(resp.Data))
	for _, d := range resp.Data {
		if d.Attributes.Address == "" {
			continue
		}
		out = append(out, domain.Pool{
			Chain:        tok.Chain,
			Address:      d.Attributes.Address,
			DEX:          d.Relationships.DEX.Data.ID,
			BaseAddr:     tokenAddressFromID(d.Relationships.BaseToken.Data.ID),
			QuoteAddr:    tokenAddressFromID(d.Relationships.QuoteToken.Data.ID),
			LiquidityUSD: parseFloat(d.Attributes.ReserveInUSD),
			Volume24hUSD: parseFloat(d.Attributes.VolumeUSD.H24),
			DiscoveredAt: now,
			Source:       c.Name(),
		})
	}
	return out, nil
}

// Volume sums GeckoTerminal's per-pool aggregates for the token.
func (c *Client) Volume(ctx context.Context, tok domain.Token) (sources.Reference, error) {
	net, ok := network[tok.Chain]
	if !ok {
		return sources.Reference{}, fmt.Errorf("geckoterminal: no network id for chain %q", tok.Chain)
	}

	endpoint := fmt.Sprintf("%s/networks/%s/tokens/%s/pools", c.baseURL, net, url.PathEscape(tok.Address))
	var resp poolsResponse
	if err := c.http.GetJSON(ctx, endpoint, &resp); err != nil {
		return sources.Reference{}, err
	}

	ref := sources.Reference{Source: c.Name(), RetrievedAt: time.Now().UTC()}
	now := time.Now().UTC()
	for _, d := range resp.Data {
		// Counting only pools where the tracked token is the base side keeps a
		// token that sits on both sides of two pools from being counted twice.
		if !strings.EqualFold(tokenAddressFromID(d.Relationships.BaseToken.Data.ID), tok.Address) {
			continue
		}
		ref.H1USD += parseFloat(d.Attributes.VolumeUSD.H1)
		ref.H24USD += parseFloat(d.Attributes.VolumeUSD.H24)
		ref.Pools = append(ref.Pools, domain.Pool{
			Chain:        tok.Chain,
			Address:      d.Attributes.Address,
			DEX:          d.Relationships.DEX.Data.ID,
			LiquidityUSD: parseFloat(d.Attributes.ReserveInUSD),
			Volume24hUSD: parseFloat(d.Attributes.VolumeUSD.H24),
			DiscoveredAt: now,
			Source:       c.Name(),
		})
	}
	return ref, nil
}

type tradesResponse struct {
	Data []struct {
		Attributes struct {
			BlockNumber     int64  `json:"block_number"`
			BlockTimestamp  string `json:"block_timestamp"`
			TxHash          string `json:"tx_hash"`
			Kind            string `json:"kind"`
			VolumeInUSD     string `json:"volume_in_usd"`
			FromTokenAmount string `json:"from_token_amount"`
			ToTokenAmount   string `json:"to_token_amount"`
			PriceToInUSD    string `json:"price_to_in_usd"`
			PriceFromInUSD  string `json:"price_from_in_usd"`
		} `json:"attributes"`
	} `json:"data"`
}

// Trades returns the pool's recent trades — up to the last 300 within 24h.
//
// This is a verification tool, not an ingestion path: see the package comment
// for why the rate limit rules it out for continuous polling.
func (c *Client) Trades(ctx context.Context, pool domain.Pool, tok domain.Token) ([]domain.Trade, error) {
	net, ok := network[pool.Chain]
	if !ok {
		return nil, fmt.Errorf("geckoterminal: no network id for chain %q", pool.Chain)
	}

	endpoint := fmt.Sprintf("%s/networks/%s/pools/%s/trades", c.baseURL, net, url.PathEscape(pool.Address))
	var resp tradesResponse
	if err := c.http.GetJSON(ctx, endpoint, &resp); err != nil {
		return nil, err
	}

	out := make([]domain.Trade, 0, len(resp.Data))
	for i, d := range resp.Data {
		ts, err := time.Parse(time.RFC3339, d.Attributes.BlockTimestamp)
		if err != nil {
			continue
		}
		side := domain.SideSell
		if strings.EqualFold(d.Attributes.Kind, "buy") {
			side = domain.SideBuy
		}
		out = append(out, domain.Trade{
			Timestamp:    ts.UTC(),
			Chain:        pool.Chain,
			Token:        tok.Symbol,
			TokenAddress: tok.Address,
			Pool:         pool.Address,
			DEX:          pool.DEX,
			TxHash:       d.Attributes.TxHash,
			// The API exposes no log index, so position within the response
			// stands in. It keeps two swaps in one transaction distinct, but it
			// will not match the RPC adapter's key — which is fine, because
			// this path never feeds the live engine.
			LogIndex:    i,
			Side:        side,
			TokenAmount: parseFloat(d.Attributes.ToTokenAmount),
			USDVolume:   parseFloat(d.Attributes.VolumeInUSD),
			Price:       parseFloat(d.Attributes.PriceToInUSD),
			Source:      c.Name(),
		})
	}
	return out, nil
}
