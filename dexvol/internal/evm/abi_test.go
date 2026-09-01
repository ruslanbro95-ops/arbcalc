package evm

import (
	"math/big"
	"strings"
	"testing"
)

func word(hexBody string) string {
	return strings.Repeat("0", 64-len(hexBody)) + hexBody
}

func TestUint256(t *testing.T) {
	data, err := DecodeHexData("0x" + word("de0b6b3a7640000")) // 1e18
	if err != nil {
		t.Fatal(err)
	}
	v, err := Uint256(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "1000000000000000000" {
		t.Fatalf("got %s", v)
	}
}

func TestInt256HandlesNegativeAmounts(t *testing.T) {
	// -1000 in two's complement: a Uniswap V3 swap taking token0 out of the
	// pool. Read as unsigned this would be ~1.15e77.
	neg := strings.Repeat("f", 61) + "c18"
	data, err := DecodeHexData("0x" + neg)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Int256(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "-1000" {
		t.Fatalf("got %s, want -1000", v)
	}
}

func TestInt256PositiveStaysPositive(t *testing.T) {
	data, _ := DecodeHexData("0x" + word("3e8"))
	v, _ := Int256(data, 0)
	if v.String() != "1000" {
		t.Fatalf("got %s", v)
	}
}

func TestWordOutOfRange(t *testing.T) {
	data, _ := DecodeHexData("0x" + word("1"))
	if _, err := Uint256(data, 3); err == nil {
		t.Fatal("reading past the payload must fail rather than return zero")
	}
}

func TestToFloatSurvivesEighteenDecimals(t *testing.T) {
	// 1234.5 tokens at 18 decimals is far beyond int64.
	raw, _ := new(big.Int).SetString("1234500000000000000000", 10)
	if got := ToFloat(raw, 18); got < 1234.49 || got > 1234.51 {
		t.Fatalf("got %v, want ~1234.5", got)
	}
	// USDC-style 6 decimals.
	if got := ToFloat(big.NewInt(2_500_000), 6); got != 2.5 {
		t.Fatalf("got %v, want 2.5", got)
	}
	if got := ToFloat(big.NewInt(42), 0); got != 42 {
		t.Fatalf("got %v, want 42", got)
	}
}

func TestAddressFromTopic(t *testing.T) {
	topic := "0x000000000000000000000000AAbBCcDdEeFf00112233445566778899aAbBcCdD"
	if got := AddressFromTopic(topic); got != "0xaabbccddeeff00112233445566778899aabbccdd" {
		t.Fatalf("got %s", got)
	}
	if got := AddressFromTopic("0x00"); got != "" {
		t.Fatalf("a short topic should yield no address, got %q", got)
	}
}

func TestDecodeHexDataRejectsOddLength(t *testing.T) {
	if _, err := DecodeHexData("0xabc"); err == nil {
		t.Fatal("odd-length payload must be rejected")
	}
}
