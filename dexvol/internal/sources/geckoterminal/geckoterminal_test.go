package geckoterminal

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

const ohlcvBody = `{"data":{"attributes":{"ohlcv_list":[
 [1788609600, 1.20, 1.25, 1.19, 1.24, 5000],
 [1788609540, "1.18", "1.21", "1.17", "1.20", "3200.5"],
 [1788609480, 1.15, 1.19, 1.14, 1.18, 0],
 [1788609420]
]}}}`

func TestOHLCVMinuteParsesMixedNumberAndStringFields(t *testing.T) {
	// GeckoTerminal quotes some numerics and not others. A strict decode into
	// float64 would silently drop whichever form it did not expect, and a
	// dropped candle is an understated backfilled minute.
	c := stub(t, ohlcvBody)
	pool := domain.Pool{Chain: domain.ChainBase, Address: "0xPOOL1"}

	got, err := c.OHLCVMinute(t.Context(), pool, 0, time.Unix(1788609660, 0))
	if err != nil {
		t.Fatal(err)
	}
	// The fourth row is truncated and cannot be read as a candle.
	if len(got) != 3 {
		t.Fatalf("got %d candles, want 3", len(got))
	}
	if got[0].VolumeUSD != 5000 {
		t.Fatalf("numeric volume = %v, want 5000", got[0].VolumeUSD)
	}
	if got[1].VolumeUSD != 3200.5 {
		t.Fatalf("quoted volume = %v, want 3200.5", got[1].VolumeUSD)
	}
	if got[1].Close != 1.20 {
		t.Fatalf("quoted close = %v, want 1.20", got[1].Close)
	}
	// A zero-volume candle is real data — the minute traded nothing — and must
	// survive rather than be filtered out as noise.
	if got[2].VolumeUSD != 0 {
		t.Fatalf("zero-volume candle = %v", got[2].VolumeUSD)
	}
	want := time.Unix(1788609600, 0).UTC()
	if !got[0].Time.Equal(want) {
		t.Fatalf("time = %v, want %v", got[0].Time, want)
	}
}

func TestOHLCVMinuteRequestsUSDAndPagesBackwards(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Write([]byte(ohlcvBody))
	}))
	t.Cleanup(srv.Close)
	c := NewWithBase(srv.URL)

	before := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if _, err := c.OHLCVMinute(t.Context(), domain.Pool{Chain: domain.ChainBase, Address: "0xP"}, 0, before); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"aggregate=1", "currency=usd", "limit=1000",
		"before_timestamp=" + strconv.FormatInt(before.Unix(), 10)} {
		if !strings.Contains(gotURL, want) {
			t.Errorf("request %q missing %q", gotURL, want)
		}
	}
}

func TestOHLCVUnsupportedChainIsAnError(t *testing.T) {
	c := stub(t, ohlcvBody)
	_, err := c.OHLCVMinute(t.Context(), domain.Pool{Chain: domain.ChainRobinhood, Address: "0xP"}, 0, time.Now())
	if err == nil {
		t.Fatal("a chain with no network id must fail loudly, not return an empty history")
	}
}

func TestNetworksTreatsAPastTheEndErrorAsTheEnd(t *testing.T) {
	// The live endpoint answers a request past the last page with 400
	// ("expected :page in 1..3") rather than an empty list. Reporting that as
	// a failure made preflight declare the provider unreachable while holding
	// a complete network list.
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		if page > 3 {
			http.Error(w, `{"errors":[{"status":"400","title":"expected :page in 1..3; got 4"}]}`,
				http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"data":[{"id":"eth","attributes":{"name":"Ethereum"}}]}`))
	}))
	t.Cleanup(srv.Close)

	got, err := NewWithBase(srv.URL).Networks(t.Context(), 20)
	if err != nil {
		t.Fatalf("running past the last page must not be an error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d networks, want the 3 pages that answered", len(got))
	}
}

func TestNetworksReportsAFailureOnTheFirstPage(t *testing.T) {
	// With nothing collected, an error is a real one and must surface.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gateway down", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	if _, err := NewWithBase(srv.URL).Networks(t.Context(), 20); err == nil {
		t.Fatal("an unreachable provider must be reported")
	}
}

func TestNetworksStopsOnAnEmptyPage(t *testing.T) {
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		if page > 2 {
			w.Write([]byte(`{"data":[]}`))
			return
		}
		w.Write([]byte(`{"data":[{"id":"eth","attributes":{"name":"Ethereum"}}]}`))
	}))
	t.Cleanup(srv.Close)

	got, err := NewWithBase(srv.URL).Networks(t.Context(), 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d, want 2", len(got))
	}
}
