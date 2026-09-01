package domain

import (
	"fmt"
	"sort"
	"strings"
)

// ChainInfo is everything the rest of the service needs to know about a
// network, gathered in one place.
//
// Before this table the same knowledge was scattered across four packages: the
// DEX Screener adapter, the GeckoTerminal adapter, the alert link builder and
// the bot's command parser each kept their own map. Adding a network meant
// editing all of them and hoping none was missed — and a missed one fails
// quietly, as "this token has no pools" rather than as an error. Now adding an
// EVM network is one entry here.
type ChainInfo struct {
	Chain Chain
	// EVM decides which ingestion adapter handles the chain.
	EVM bool
	// ChainID is the EVM chain id, 0 for non-EVM networks.
	ChainID uint64
	// DexScreenerID and GeckoTerminalID are the provider-side identifiers. An
	// empty string means that provider does not cover the chain, and the
	// adapter reports it as unsupported instead of issuing a request that
	// would 404 and read like an empty result.
	DexScreenerID   string
	GeckoTerminalID string
	// GMGNSlug and OKXSlug drive the alert buttons; empty means no button.
	GMGNSlug string
	OKXSlug  string
	// RPCEnv is the environment variable that overrides DefaultRPC.
	RPCEnv     string
	DefaultRPC string
	// MaxLogAddresses is how many addresses DefaultRPC accepts in one
	// eth_getLogs filter, 0 when it takes as many as we have.
	//
	// It belongs next to DefaultRPC because it is a property of that endpoint,
	// not of the chain: every publicnode URL here answers -32602 "Request
	// blocked" at ten addresses — measured on all seven — and rejects the
	// whole request rather than part of it. Robinhood Chain's own node was
	// still answering at sixty, and capping it at nine there only multiplied
	// the request count until that node started rate limiting us.
	//
	// EVM_MAX_ADDRESSES_PER_CALL overrides this for every chain, which is what
	// you want after pointing RPC_* at your own node.
	MaxLogAddresses int
	// Aliases are what a person may type in a Telegram command.
	Aliases []string
}

// chainRegistry is the single source of truth for supported networks.
//
// GeckoTerminal ids are the ones its /networks endpoint publishes; `cmd/coverage
// -verify-networks` checks this column against the live list, because a wrong
// id here costs a whole chain's second discovery opinion without any error.
var chainRegistry = []ChainInfo{
	{
		Chain: ChainEthereum, EVM: true, ChainID: 1,
		DexScreenerID: "ethereum", GeckoTerminalID: "eth",
		GMGNSlug: "eth", OKXSlug: "ethereum",
		RPCEnv: "RPC_ETHEREUM", DefaultRPC: "https://ethereum-rpc.publicnode.com",
		MaxLogAddresses: 9,
		Aliases:         []string{"eth", "ethereum"},
	},
	{
		Chain: ChainBNB, EVM: true, ChainID: 56,
		DexScreenerID: "bsc", GeckoTerminalID: "bsc",
		GMGNSlug: "bsc", OKXSlug: "bsc",
		RPCEnv: "RPC_BSC", DefaultRPC: "https://bsc-rpc.publicnode.com",
		MaxLogAddresses: 9,
		Aliases:         []string{"bsc", "bnb", "bnbchain"},
	},
	{
		// Solana's public endpoint is the one the docs name and the one that
		// cannot do this job: measured, api.mainnet-beta.solana.com refused 2
		// of 12 signature requests in a burst and then every one of the next
		// 12, and a coverage run over an hour got 0 usable minutes out of 59.
		// solana-rpc.publicnode.com answered 60 of 60 with no key, so it is
		// the default. Both are free and neither needs registration; the
		// difference is entirely in what they will actually serve.
		Chain: ChainSolana, EVM: false,
		DexScreenerID: "solana", GeckoTerminalID: "solana",
		GMGNSlug: "sol", OKXSlug: "solana",
		RPCEnv: "RPC_SOLANA", DefaultRPC: "https://solana-rpc.publicnode.com",
		Aliases: []string{"sol", "solana"},
	},
	{
		Chain: ChainBase, EVM: true, ChainID: 8453,
		DexScreenerID: "base", GeckoTerminalID: "base",
		GMGNSlug: "base", OKXSlug: "base",
		RPCEnv: "RPC_BASE", DefaultRPC: "https://base-rpc.publicnode.com",
		MaxLogAddresses: 9,
		Aliases:         []string{"base"},
	},
	{
		// Robinhood Chain: an Arbitrum Orbit L2, mainnet since 2026-07-01.
		// GeckoTerminal's id for it is "robinhood", confirmed against the
		// live /networks list and against its pools and minute-OHLCV
		// endpoints, so the chain has both discovery opinions and history
		// backfill. GMGN does not list it, hence no button. Its own node takes
		// sixty addresses in a getLogs filter, so no cap here.
		Chain: ChainRobinhood, EVM: true, ChainID: 4663,
		DexScreenerID: "robinhood", GeckoTerminalID: "robinhood",
		GMGNSlug: "", OKXSlug: "",
		RPCEnv: "RPC_ROBINHOOD", DefaultRPC: "https://rpc.mainnet.chain.robinhood.com",
		Aliases: []string{"rh", "robinhood"},
	},
	// The networks below are the expansion the spec asks the architecture to
	// allow. They are EVM, so they need no new adapter — only this entry.
	{
		Chain: ChainArbitrum, EVM: true, ChainID: 42161,
		DexScreenerID: "arbitrum", GeckoTerminalID: "arbitrum",
		// OKX names this one arbitrum-one, unlike every other chain here
		// where its slug matches the common name. Confirmed by preflight:
		// "arbitrum" answered 404 while the other seven answered 200.
		GMGNSlug: "", OKXSlug: "arbitrum-one",
		RPCEnv: "RPC_ARBITRUM", DefaultRPC: "https://arbitrum-one-rpc.publicnode.com",
		MaxLogAddresses: 9,
		Aliases:         []string{"arb", "arbitrum"},
	},
	{
		Chain: ChainAvalanche, EVM: true, ChainID: 43114,
		DexScreenerID: "avalanche", GeckoTerminalID: "avax",
		GMGNSlug: "", OKXSlug: "avalanche",
		RPCEnv: "RPC_AVALANCHE", DefaultRPC: "https://avalanche-c-chain-rpc.publicnode.com",
		MaxLogAddresses: 9,
		Aliases:         []string{"avax", "avalanche"},
	},
	{
		Chain: ChainPolygon, EVM: true, ChainID: 137,
		DexScreenerID: "polygon", GeckoTerminalID: "polygon_pos",
		GMGNSlug: "", OKXSlug: "polygon",
		RPCEnv: "RPC_POLYGON", DefaultRPC: "https://polygon-bor-rpc.publicnode.com",
		MaxLogAddresses: 9,
		Aliases:         []string{"polygon", "matic", "pol"},
	},
	{
		Chain: ChainOptimism, EVM: true, ChainID: 10,
		DexScreenerID: "optimism", GeckoTerminalID: "optimism",
		GMGNSlug: "", OKXSlug: "optimism",
		RPCEnv: "RPC_OPTIMISM", DefaultRPC: "https://optimism-rpc.publicnode.com",
		MaxLogAddresses: 9,
		Aliases:         []string{"op", "optimism"},
	},
}

var (
	byChain = func() map[Chain]ChainInfo {
		m := make(map[Chain]ChainInfo, len(chainRegistry))
		for _, c := range chainRegistry {
			m[c.Chain] = c
		}
		return m
	}()
	byAlias = func() map[string]Chain {
		m := map[string]Chain{}
		for _, c := range chainRegistry {
			m[strings.ToLower(string(c.Chain))] = c.Chain
			for _, a := range c.Aliases {
				m[strings.ToLower(a)] = c.Chain
			}
		}
		return m
	}()
)

// Chains returns every supported network, in registry order.
func Chains() []ChainInfo {
	out := make([]ChainInfo, len(chainRegistry))
	copy(out, chainRegistry)
	return out
}

// Info looks up one network.
func Info(c Chain) (ChainInfo, bool) {
	info, ok := byChain[c]
	return info, ok
}

// ParseChain resolves whatever a person typed into a supported network.
func ParseChain(s string) (Chain, error) {
	if c, ok := byAlias[strings.ToLower(strings.TrimSpace(s))]; ok {
		return c, nil
	}
	return "", fmt.Errorf("unknown chain %q — supported: %s", s, strings.Join(ChainNames(), ", "))
}

// ChainNames lists the canonical names, for help text and error messages.
func ChainNames() []string {
	out := make([]string, 0, len(chainRegistry))
	for _, c := range chainRegistry {
		out = append(out, string(c.Chain))
	}
	sort.Strings(out)
	return out
}
