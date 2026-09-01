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
	if _, err := c.DiscoverPools(t.Context(), domain.Token{Chain: "polygon", Address: "0x1"}); err == nil {
		t.Fatal("an unmapped chain must fail loudly rather than return no pools")
	}
}

func TestPricesPrefersDeepestPool(t *testing.T) {
	c := stub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"pairs":` + pairsBody + `}`))
	})

	got, err := c.Prices(t.Context(), []domain.Token{{Chain: domain.ChainBase, Address: "0xTOKEN"}})
	if err != nil {
		t.Fatal(err)
	}
	// $1.26 comes from the $900k pool; the $500k pool's $1.25 must lose.
	if got["base:0xtoken"] != 1.26 {
		t.Fatalf("price = %v, want 1.26 from the deepest pool", got["base:0xtoken"])
	}
}

func TestPricesBatchesAddresses(t *testing.T) {
	var gotPath string
	c := stub(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(`{"pairs":[]}`))
	})

	toks := []domain.Token{
		{Chain: domain.ChainBase, Address: "0xA"},
		{Chain: domain.ChainBase, Address: "0xB"},
	}
	if _, err := c.Prices(t.Context(), toks); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotPath, "0xA,0xB") {
		t.Fatalf("both addresses should ride in one request, path = %q", gotPath)
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
