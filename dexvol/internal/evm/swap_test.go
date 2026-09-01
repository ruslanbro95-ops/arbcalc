package evm

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
)

// pad renders a big.Int as one 32-byte ABI word, two's complement for negatives.
func padWord(v *big.Int) string {
	if v.Sign() < 0 {
		v = new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 256), v)
	}
	h := v.Text(16)
	return strings.Repeat("0", 64-len(h)) + h
}

func words(vals ...*big.Int) []byte {
	var sb strings.Builder
	for _, v := range vals {
		sb.WriteString(padWord(v))
	}
	b, err := hex.DecodeString(sb.String())
	if err != nil {
		panic(err)
	}
	return b
}

func bi(n int64) *big.Int { return big.NewInt(n) }

func TestSwapTopicsAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for _, topic := range SwapTopics() {
		if seen[topic] {
			t.Fatalf("duplicate topic %s", topic)
		}
		seen[topic] = true
	}
	if len(seen) != 4 {
		t.Fatalf("expected 4 layouts, got %d", len(seen))
	}
}

func TestDecodeUniswapV2Buy(t *testing.T) {
	// A trader puts 1000 of token1 in and takes 500 of token0 out: token0 was
	// bought, so its normalized amount must come out negative.
	topic := topicHex("Swap(address,uint256,uint256,uint256,uint256,address)")
	data := words(bi(0), bi(1000), bi(500), bi(0))

	got, ok, err := DecodeSwap(topic, data)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.Amount0.Int64() != -500 {
		t.Fatalf("amount0 = %s, want -500", got.Amount0)
	}
	if got.Amount1.Int64() != 1000 {
		t.Fatalf("amount1 = %s, want 1000", got.Amount1)
	}
	if got.Event != "uniswap_v2" {
		t.Fatalf("event = %s", got.Event)
	}
}

func TestDecodeUniswapV2Sell(t *testing.T) {
	topic := topicHex("Swap(address,uint256,uint256,uint256,uint256,address)")
	data := words(bi(500), bi(0), bi(0), bi(1000))

	got, _, err := DecodeSwap(topic, data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Amount0.Int64() != 500 {
		t.Fatalf("amount0 = %s, want +500 (sold into the pool)", got.Amount0)
	}
}

func TestDecodeSolidlyUsesSameConvention(t *testing.T) {
	// The Solidly layout differs only in which parameters are indexed, so the
	// data words are identical to V2 — but the topic is not.
	v2 := topicHex("Swap(address,uint256,uint256,uint256,uint256,address)")
	solidly := topicHex("Swap(address,address,uint256,uint256,uint256,uint256)")
	if v2 == solidly {
		t.Fatal("V2 and Solidly must not share a topic")
	}

	data := words(bi(0), bi(1000), bi(500), bi(0))
	got, ok, err := DecodeSwap(solidly, data)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.Amount0.Int64() != -500 || got.Event != "solidly" {
		t.Fatalf("got %+v", got)
	}
}

func TestDecodeUniswapV3SignedAmounts(t *testing.T) {
	topic := topicHex("Swap(address,address,int256,int256,uint160,uint128,int24)")
	// amount0 = -750 (token0 leaves the pool), amount1 = +2000.
	data := words(bi(-750), bi(2000), bi(0), bi(0), bi(0))

	got, ok, err := DecodeSwap(topic, data)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.Amount0.Int64() != -750 || got.Amount1.Int64() != 2000 {
		t.Fatalf("got %s / %s", got.Amount0, got.Amount1)
	}
}

func TestDecodePancakeV3IgnoresTrailingFeeWords(t *testing.T) {
	topic := topicHex("Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)")
	data := words(bi(-750), bi(2000), bi(0), bi(0), bi(0), bi(11), bi(22))

	got, ok, err := DecodeSwap(topic, data)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.Amount0.Int64() != -750 || got.Event != "pancake_v3" {
		t.Fatalf("got %+v", got)
	}
}

func TestUnknownTopicIsSkippedNotGuessed(t *testing.T) {
	_, ok, err := DecodeSwap("0x"+strings.Repeat("ab", 32), words(bi(1), bi(2), bi(3), bi(4)))
	if ok {
		t.Fatal("an unknown layout must not be decoded")
	}
	if err != nil {
		t.Fatalf("an unknown layout is not an error, got %v", err)
	}
}

func TestTruncatedPayloadIsAnError(t *testing.T) {
	topic := topicHex("Swap(address,uint256,uint256,uint256,uint256,address)")
	_, ok, err := DecodeSwap(topic, words(bi(1), bi(2))) // only two of four words
	if !ok {
		t.Fatal("the topic was recognized")
	}
	if err == nil {
		t.Fatal("a truncated payload must fail rather than decode as zeros")
	}
}

func TestTopicMatchIsCaseInsensitive(t *testing.T) {
	topic := strings.ToUpper(topicHex("Swap(address,uint256,uint256,uint256,uint256,address)"))
	topic = "0x" + strings.TrimPrefix(topic, "0X")
	if !IsSwapTopic(topic) {
		t.Fatal("an upper-case topic from an RPC node must still match")
	}
}
