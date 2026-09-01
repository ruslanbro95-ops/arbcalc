package geckoterminal

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ruslanbro95-ops/arbcalc/dexvol/internal/domain"
)

const poolsBody = `{"data":[
 {"id":"base_0xPOOL1","attributes":{"address":"0xPOOL1","reserve_in_usd":"500000",
   "volume_usd":{"h1":"100","h24":"1000"}},
  "relationships":{"base_token":{"data":{"id":"base_0xTOKEN"}},
                   "quote_token":{"data":{"id":"base_0xWETH"}},
                   "dex":{"data":{"id":"uniswap_v3"}}}},
 {"id":"base_0xPOOL2","attributes":{"address":"0xPOOL2","reserve_in_usd":"10",
   "volume_usd":{"h1":"9999","h24":"9999"}},
  "relationships":{"base_token":{"data":{"id":"base_0xOTHER"}},
                   "quote_token":{"data":{"id":"base_0xTOKEN"}},
                   "dex":{"data":{"id":"aerodrome"}}}}
]}`

func stub(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewWithBase(srv.URL)
}

func TestDiscoverPoolsStripsNetworkPrefix(t *testing.T) {
	c := stub(t, poolsBody)
	pools, err := c.DiscoverPools(t.Context(), domain.Token{Chain: domain.ChainBase, Address: "0xTOKEN"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pools) != 2 {
		t.Fatalf("got %d pools, want 2", len(pools))
	}
	// The JSON:API id is "base_0xTOKEN"; the address must come out clean.
	if pools[0].BaseAddr != "0xTOKEN" || pools[0].QuoteAddr != "0xWETH" {
		t.Fatalf("unexpected addresses: %+v", pools[0])
	}
	if pools[0].DEX != "uniswap_v3" || pools[0].LiquidityUSD != 500000 {
		t.Fatalf("unexpected pool: %+v", pools[0])
	}
}

func TestVolumeCountsOnlyBaseSidePools(t *testing.T) {
	c := stub(t, poolsBody)
	ref, err := c.Volume(t.Context(), domain.Token{Chain: domain.ChainBase, Address: "0xTOKEN"})
	if err != nil {
		t.Fatal(err)
	}
	if ref.H24USD != 1000 || ref.H1USD != 100 {
		t.Fatalf("h1/h24 = %v/%v, want 100/1000", ref.H1USD, ref.H24USD)
	}
}

func TestRobinhoodIsExplicitlyUnsupported(t *testing.T) {
	// Guessing a network id would turn "unsupported" into a silent 404 that
	// reads like "this token has no pools".
	c := stub(t, poolsBody)
	if c.Supports(domain.ChainRobinhood) {
		t.Fatal("Robinhood Chain has no confirmed GeckoTerminal network id")
	}
	if _, err := c.DiscoverPools(t.Context(), domain.Token{Chain: domain.ChainRobinhood, Address: "0xT"}); err == nil {
		t.Fatal("expected an explicit error")
	}
}

const tradesBody = `{"data":[
 {"attributes":{"block_number":100,"block_timestamp":"2026-09-01T12:30:14Z",
   "tx_hash":"0xAAA","kind":"buy","volume_in_usd":"2000",
   "to_token_amount":"1600","price_to_in_usd":"1.25"}},
 {"attributes":{"block_number":101,"block_timestamp":"2026-09-01T12:30:45Z",
   "tx_hash":"0xBBB","kind":"sell","volume_in_usd":"1500",
   "to_token_amount":"1200","price_to_in_usd":"1.25"}},
 {"attributes":{"block_number":102,"block_timestamp":"not-a-timestamp",
   "tx_hash":"0xCCC","kind":"buy","volume_in_usd":"1"}}
]}`

func TestTradesNormalization(t *testing.T) {
	c := stub(t, tradesBody)
	pool := domain.Pool{Chain: domain.ChainBase, Address: "0xPOOL1", DEX: "uniswap_v3"}
	tok := domain.Token{Chain: domain.ChainBase, Address: "0xTOKEN", Symbol: "ABC"}

	trades, err := c.Trades(t.Context(), pool, tok)
	if err != nil {
		t.Fatal(err)
	}
	// The third entry has an unparseable timestamp; a trade with no usable
	// time cannot be placed in a minute bucket, so it is dropped.
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want 2", len(trades))
	}
	if trades[0].Side != domain.SideBuy || trades[1].Side != domain.SideSell {
		t.Fatalf("sides = %v/%v", trades[0].Side, trades[1].Side)
	}
	if trades[0].USDVolume != 2000 {
		t.Fatalf("usd = %v", trades[0].USDVolume)
	}
	want := time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)
	if !trades[0].Minute().Equal(want) {
		t.Fatalf("minute = %v, want %v", trades[0].Minute(), want)
	}
	// Distinct positions keep two trades in one response from colliding.
	if trades[0].DedupKey() == trades[1].DedupKey() {
		t.Fatal("dedup keys must differ")
	}
}
