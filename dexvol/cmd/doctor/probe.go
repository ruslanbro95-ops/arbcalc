package main

import "github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"

// probeToken is a well-known, deep-liquidity token used only to prove that a
// provider answers for a chain and that our identifier for that chain is the
// one it expects.
//
// These are wrapped natives and are as stable as anything on chain gets, but a
// wrong entry here must never read as a broken provider: an empty result is
// reported as "verify manually", not as a failure. The check that actually
// matters is whether the request was accepted at all.
var probeToken = map[domain.Chain]string{
	domain.ChainEthereum:  "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  // WETH
	domain.ChainBNB:       "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  // WBNB
	domain.ChainBase:      "0x4200000000000000000000000000000000000006",  // WETH
	domain.ChainSolana:    "So11111111111111111111111111111111111111112", // wSOL
	domain.ChainArbitrum:  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  // WETH
	domain.ChainOptimism:  "0x4200000000000000000000000000000000000006",  // WETH
	domain.ChainPolygon:   "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",  // WPOL
	domain.ChainAvalanche: "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",  // WAVAX
	// Robinhood Chain has no obvious canonical probe token yet, so its
	// aggregator coverage is left to the coverage tool on a real watch-list
	// token rather than guessed at here.
}
