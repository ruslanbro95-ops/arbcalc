// Package domain describes the normalized entities every data source must
// produce. Nothing below this package knows which API a trade came from.
package domain

import (
	"fmt"
	"strings"
	"time"
)

// Chain is a supported network. Adding a network means adding a constant here
// and a config entry — no changes in the engine.
type Chain string

const (
	ChainEthereum  Chain = "ethereum"
	ChainBNB       Chain = "bsc"
	ChainSolana    Chain = "solana"
	ChainBase      Chain = "base"
	ChainRobinhood Chain = "robinhood"
	ChainArbitrum  Chain = "arbitrum"
	ChainAvalanche Chain = "avalanche"
	ChainPolygon   Chain = "polygon"
	ChainOptimism  Chain = "optimism"
)

// IsEVM reports whether the chain speaks the JSON-RPC eth_* dialect, which
// decides who ingests it: the shared EVM adapter or a chain-specific one.
func (c Chain) IsEVM() bool {
	info, ok := Info(c)
	return ok && info.EVM
}

// Supported reports whether the chain is in the registry at all.
func (c Chain) Supported() bool {
	_, ok := Info(c)
	return ok
}

// Side is the direction of a swap from the tracked token's point of view.
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// Token is a monitoring target as the user configured it.
type Token struct {
	Symbol  string `json:"symbol"`
	Chain   Chain  `json:"chain"`
	Address string `json:"address"`
	// Decimals is filled in by pool discovery; 0 means "not resolved yet".
	Decimals int  `json:"decimals"`
	Enabled  bool `json:"enabled"`
}

// Key uniquely identifies a token across chains.
func (t Token) Key() string {
	return string(t.Chain) + ":" + strings.ToLower(t.Address)
}

func (t Token) String() string {
	return fmt.Sprintf("%s@%s", t.Symbol, t.Chain)
}

// Pool is one liquidity pool trading the token. Discovery keeps this list fresh
// because a token can show up on a new DEX at any time.
type Pool struct {
	Chain    Chain  `json:"chain"`
	Address  string `json:"address"`
	DEX      string `json:"dex"`
	BaseAddr string `json:"base_addr"`
	// BaseSymbol lets discovery fill in a symbol the owner did not type when
	// adding the token.
	BaseSymbol string `json:"base_symbol"`
	// QuoteAddr and QuoteSymbol let us price a swap when the pool does not
	// trade against a stablecoin directly.
	QuoteAddr    string    `json:"quote_addr"`
	QuoteSymbol  string    `json:"quote_symbol"`
	LiquidityUSD float64   `json:"liquidity_usd"`
	Volume24hUSD float64   `json:"volume_24h_usd"`
	DiscoveredAt time.Time `json:"discovered_at"`
	// Source records which adapter found the pool, so coverage reports can
	// attribute a miss to a specific discovery provider.
	Source string `json:"source"`
}

func (p Pool) Key() string {
	return string(p.Chain) + ":" + strings.ToLower(p.Address)
}

// Trade is the single normalized shape every source converts into. The engine
// consumes nothing else.
type Trade struct {
	Timestamp    time.Time `json:"timestamp"`
	Chain        Chain     `json:"chain"`
	Token        string    `json:"token"`
	TokenAddress string    `json:"token_address"`
	Pool         string    `json:"pool"`
	DEX          string    `json:"dex"`
	TxHash       string    `json:"tx_hash"`
	// LogIndex disambiguates several swaps inside one transaction. On Solana
	// this carries the instruction index instead.
	LogIndex    int     `json:"log_index"`
	Side        Side    `json:"side"`
	TokenAmount float64 `json:"token_amount"`
	USDVolume   float64 `json:"usd_volume"`
	Price       float64 `json:"price"`
	Source      string  `json:"source"`
}

// DedupKey is stable across sources: the same on-chain swap seen through the
// RPC adapter and through an aggregator collapses to one entry.
func (t Trade) DedupKey() string {
	return fmt.Sprintf("%s|%s|%d", t.Chain, strings.ToLower(t.TxHash), t.LogIndex)
}

// Minute truncates the trade time to the calendar minute it belongs to.
func (t Trade) Minute() time.Time { return t.Timestamp.UTC().Truncate(time.Minute) }
