package dexscreener

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

const pairsBody = `[
 {"chainId":"base","dexId":"uniswap","pairAddress":"0xPOOL1",
  "baseToken":{"address":"0xTOKEN","symbol":"ABC"},
  "quoteToken":{"address":"0xWETH","symbol":"WETH"},
  "priceUsd":"1.25","volume":{"h24":1000,"h1":100,"m5":10},"liquidity":{"usd":500000}},
 {"chainId":"base","dexId":"aerodrome","pairAddress":"0xPOOL2",
  "baseToken":{"address":"0xTOKEN","symbol":"ABC"},
  "quoteToken":{"address":"0xUSDC","symbol":"USDC"},
  "priceUsd":"1.26","volume":{"h24":2000,"h1":250,"m5":20},"liquidity":{"usd":900000}},
 {"chainId":"base","dexId":"uniswap","pairAddress":"0xPOOL3",
  "baseToken":{"address":"0xOTHER","symbol":"OTH"},
  "quoteToken":{"address":"0xTOKEN","symbol":"ABC"},
  "priceUsd":"7","volume":{"h24":9999,"h1":9999,"m5":9},"liquidity":{"usd":1}}
]`

func stub(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewWithBase(srv.URL)
}

func TestDiscoverPools(t *testing.T) {
	c := stub(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/token-pairs/v1/base/0xTOKEN") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Write([]byte(pairsBody))
	})

	pools, err := c.DiscoverPools(t.Context(), domain.Token{Chain: domain.ChainBase, Address: "0xTOKEN"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pools) != 3 {
		t.Fatalf("got %d pools, want 3", len(pools))
	}
	if pools[0].DEX != "uniswap" || pools[0].LiquidityUSD != 500000 {
		t.Fatalf("unexpected pool: %+v", pools[0])
	}
}

func TestUnsupportedChainIsAnError(t *testing.T) {
	c := stub(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`[]`)) })
	if _, err := c.DiscoverPools(t.Context(), domain.Token{Chain: "sui", Address: "0x1"}); err == nil {
		t.Fatal("an unmapped chain must fail loudly rather than return no pools")
	}
}

func TestExpansionChainsAreMapped(t *testing.T) {
	// The networks the spec lists as future work are EVM, so they need no new
	// adapter — only a registry entry. This checks the entry actually reaches
	// the request path.
	for chain, want := range map[domain.Chain]string{
		domain.ChainArbitrum:  "arbitrum",
		domain.ChainAvalanche: "avalanche",
		domain.ChainPolygon:   "polygon",
		domain.ChainOptimism:  "optimism",
	} {
		var gotPath string
		c := stub(t, func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			w.Write([]byte(`[]`))
		})
		if _, err := c.DiscoverPools(t.Context(), domain.Token{Chain: chain, Address: "0xT"}); err != nil {
			t.Errorf("%s: %v", chain, err)
			continue
		}
		if !strings.Contains(gotPath, "/"+want+"/") {
			t.Errorf("%s: path %q does not carry the DEX Screener id %q", chain, gotPath, want)
		}
	}
}

func TestPricesPrefersDeepestPool(t *testing.T) {
	c := stub(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(pairsBody)) })

	got, err := c.Prices(t.Context(), []domain.Token{{Chain: domain.ChainBase, Address: "0xTOKEN"}})
	if err != nil {
		t.Fatal(err)
	}
	// $1.26 comes from the $900k pool; the $500k pool's $1.25 must lose.
	if got["base:0xtoken"] != 1.26 {
		t.Fatalf("price = %v, want 1.26 from the deepest pool", got["base:0xtoken"])
	}
}

func TestPricesAsksPerTokenSoABusyOneCannotCrowdOutTheRest(t *testing.T) {
	// The batch endpoint answers with pairs, not tokens, and caps the answer
	// at thirty pairs. Live, CAKE's thirty pools filled the response and the
	// token asked for alongside it came back with no price at all — its swaps
	// were then dropped as unpriced and its coverage read 0%. One request per
	// token is what makes that impossible.
	var paths []string
	c := stub(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if strings.Contains(r.URL.Path, "0xBUSY") {
			w.Write([]byte(pairsBody)) // a token with pools of its own
			return
		}
		w.Write([]byte(`[{"chainId":"base","dexId":"uniswap","pairAddress":"0xP",
		  "baseToken":{"address":"0xQUIET","symbol":"Q"},
		  "quoteToken":{"address":"0xUSDC","symbol":"USDC"},
		  "priceUsd":"0.5","liquidity":{"usd":10}}]`))
	})

	got, err := c.Prices(t.Context(), []domain.Token{
		{Chain: domain.ChainBase, Address: "0xBUSY"},
		{Chain: domain.ChainBase, Address: "0xQUIET"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("made %d requests for 2 tokens: %v", len(paths), paths)
	}
	if got["base:0xquiet"] != 0.5 {
		t.Fatalf("the quiet token priced at %v, want 0.5 — it must not depend on how "+
			"many pools the other token has", got["base:0xquiet"])
	}
}

func TestPricesReadsTheQuoteSideToo(t *testing.T) {
	// A quote asset is the base token of almost none of its pools. Reading
	// only the base side priced WETH at $0.0000108 — the memecoin it was
	// paired against. priceUsd is the base in dollars and priceNative is the
	// base in quote units, so the quote is priceUsd / priceNative.
	c := stub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[{"chainId":"ethereum","dexId":"uniswap","pairAddress":"0xP",
		  "baseToken":{"address":"0xMEME","symbol":"MEME"},
		  "quoteToken":{"address":"0xWETH","symbol":"WETH"},
		  "priceUsd":"0.00001","priceNative":"0.000000004","liquidity":{"usd":700000}}]`))
	})

	got, err := c.Prices(t.Context(), []domain.Token{{Chain: domain.ChainEthereum, Address: "0xWETH"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := 2500.0; got["ethereum:0xweth"] != want {
		t.Fatalf("WETH priced at %v, want %v from priceUsd / priceNative", got["ethereum:0xweth"], want)
	}
}

func TestPricesSurvivesOneTokenFailing(t *testing.T) {
	// A partial map still lets most trades be valued; the rest become a
	// visible coverage gap rather than a zero.
	c := stub(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "0xBAD") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(pairsBody))
	})

	got, err := c.Prices(t.Context(), []domain.Token{
		{Chain: domain.ChainBase, Address: "0xBAD"},
		{Chain: domain.ChainBase, Address: "0xTOKEN"},
	})
	if err == nil {
		t.Fatal("the failure must still be reported")
	}
	if got["base:0xtoken"] != 1.26 {
		t.Fatalf("the healthy token priced at %v, want 1.26", got["base:0xtoken"])
	}
}

func TestVolumeCountsOnlyBaseSidePools(t *testing.T) {
	c := stub(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(pairsBody)) })

	ref, err := c.Volume(t.Context(), domain.Token{Chain: domain.ChainBase, Address: "0xTOKEN"})
	if err != nil {
		t.Fatal(err)
	}
	// POOL3 lists the token on the quote side; counting it would double up.
	if ref.H24USD != 3000 {
		t.Fatalf("h24 = %v, want 3000", ref.H24USD)
	}
	if ref.H1USD != 350 {
		t.Fatalf("h1 = %v, want 350", ref.H1USD)
	}
}

func TestHTTPErrorSurfaces(t *testing.T) {
	c := stub(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if _, err := c.DiscoverPools(t.Context(), domain.Token{Chain: domain.ChainBase, Address: "0xT"}); err == nil {
		t.Fatal("a 500 must be reported, not silently treated as an empty pool list")
	}
}
