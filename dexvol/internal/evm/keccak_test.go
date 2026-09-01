package evm

import (
	"encoding/hex"
	"strings"
	"testing"
)

func hexDigest(s string) string {
	d := Keccak256([]byte(s))
	return hex.EncodeToString(d[:])
}

func TestKeccak256KnownVectors(t *testing.T) {
	cases := map[string]string{
		"":    "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
		"abc": "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45",
		// The canonical ERC-20 Transfer topic, fixed across every deployed
		// token contract. If this matches, the implementation is Ethereum's
		// Keccak and not SHA3-256.
		"Transfer(address,address,uint256)": "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
		"Approval(address,address,uint256)": "8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
	}
	for in, want := range cases {
		if got := hexDigest(in); got != want {
			t.Errorf("Keccak256(%q)\n got %s\nwant %s", in, got, want)
		}
	}
}

func TestKeccak256BlockBoundaries(t *testing.T) {
	// Inputs at rate-1, rate and rate+1 bytes exercise the padding edge where
	// both pad bits share a byte, and the multi-block absorb path.
	for _, n := range []int{0, 1, 135, 136, 137, 272, 1000} {
		in := strings.Repeat("a", n)
		d := Keccak256([]byte(in))
		if len(d) != 32 {
			t.Fatalf("length %d for input of %d bytes", len(d), n)
		}
	}
	// A 136-byte input must not hash the same as its 135-byte prefix.
	if hexDigest(strings.Repeat("a", 135)) == hexDigest(strings.Repeat("a", 136)) {
		t.Fatal("padding collision across the block boundary")
	}
}

func TestEventTopicMatchesKnownSwapEvents(t *testing.T) {
	// Uniswap V2 and V3 Swap topics, both widely published and stable.
	cases := map[string]string{
		"Swap(address,uint256,uint256,uint256,uint256,address)":     "d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"Swap(address,address,int256,int256,uint160,uint128,int24)": "c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
		"Swap(address,address,uint256,uint256,uint256,uint256)":     "b3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b",
		"Sync(uint112,uint112)":                                     "1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1",
	}
	for sig, want := range cases {
		got := EventTopic(sig)
		if hex.EncodeToString(got[:]) != want {
			t.Errorf("EventTopic(%q)\n got %s\nwant %s", sig, hex.EncodeToString(got[:]), want)
		}
	}
}
