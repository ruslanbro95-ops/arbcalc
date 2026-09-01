package evm

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// WordSize is the width of one ABI slot.
const WordSize = 32

// two256 is 2^256, used to reinterpret a word as a signed integer.
var two256 = new(big.Int).Lsh(big.NewInt(1), 256)

// DecodeHexData turns "0x…" log data into raw bytes.
func DecodeHexData(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd-length hex payload (%d chars)", len(s))
	}
	return hex.DecodeString(s)
}

// Word returns the i-th 32-byte slot of an ABI payload.
func Word(data []byte, i int) ([]byte, error) {
	start := i * WordSize
	if start+WordSize > len(data) {
		return nil, fmt.Errorf("word %d out of range: payload is %d bytes", i, len(data))
	}
	return data[start : start+WordSize], nil
}

// Uint256 reads slot i as an unsigned integer.
func Uint256(data []byte, i int) (*big.Int, error) {
	w, err := Word(data, i)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(w), nil
}

// Int256 reads slot i as a two's-complement signed integer.
//
// Uniswap V3 reports swap amounts as signed: positive means tokens flowed into
// the pool, negative means out. Reading those words as unsigned would turn a
// small sale into a number near 2^256 and produce absurd volume.
func Int256(data []byte, i int) (*big.Int, error) {
	v, err := Uint256(data, i)
	if err != nil {
		return nil, err
	}
	// The top bit set means the value is negative.
	if v.Bit(255) == 1 {
		v.Sub(v, two256)
	}
	return v, nil
}

// AddressFromTopic extracts the 20-byte address from a 32-byte indexed topic,
// which is left-padded with zeros.
func AddressFromTopic(topic string) string {
	t := strings.TrimPrefix(topic, "0x")
	if len(t) < 40 {
		return ""
	}
	return "0x" + strings.ToLower(t[len(t)-40:])
}

// AddressFromWord extracts an address from a returned ABI word.
func AddressFromWord(w []byte) string {
	if len(w) < 20 {
		return ""
	}
	return "0x" + hex.EncodeToString(w[len(w)-20:])
}

// ToFloat scales a raw on-chain integer by the token's decimals.
//
// It goes through big.Float rather than converting to int64 first: a token with
// 18 decimals overflows int64 at around 9.2 tokens, so the naive conversion
// breaks on almost every real trade.
func ToFloat(raw *big.Int, decimals int) float64 {
	if raw == nil || raw.Sign() == 0 {
		return 0
	}
	f := new(big.Float).SetInt(raw)
	if decimals > 0 {
		scale := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
		f.Quo(f, scale)
	}
	out, _ := f.Float64()
	return out
}

// Abs returns |v| without mutating v.
func Abs(v *big.Int) *big.Int { return new(big.Int).Abs(v) }
