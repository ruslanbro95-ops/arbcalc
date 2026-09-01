package evm

import (
	"encoding/hex"
	"fmt"
	"math/big"
)

// SwapEvent describes one recognized Swap log layout.
type SwapEvent struct {
	Name      string
	Signature string
	Topic0    string
	decode    func(data []byte) (a0, a1 *big.Int, err error)
}

// Amounts is a decoded swap, normalized across DEX families.
//
// Sign convention: positive means the token flowed INTO the pool (the trader
// sold it), negative means it flowed OUT (the trader bought it). Uniswap V3
// already reports amounts this way; the V2 and Solidly layouts report separate
// in/out words and are folded into the same convention here, so everything
// downstream has one rule instead of one per DEX.
type Amounts struct {
	Amount0 *big.Int
	Amount1 *big.Int
	Event   string
}

func topicHex(sig string) string {
	t := EventTopic(sig)
	return "0x" + hex.EncodeToString(t[:])
}

// decodeV2Style reads the four unsigned words shared by Uniswap V2 and the
// Solidly forks: amount0In, amount1In, amount0Out, amount1Out.
func decodeV2Style(data []byte) (*big.Int, *big.Int, error) {
	in0, err := Uint256(data, 0)
	if err != nil {
		return nil, nil, err
	}
	in1, err := Uint256(data, 1)
	if err != nil {
		return nil, nil, err
	}
	out0, err := Uint256(data, 2)
	if err != nil {
		return nil, nil, err
	}
	out1, err := Uint256(data, 3)
	if err != nil {
		return nil, nil, err
	}
	return new(big.Int).Sub(in0, out0), new(big.Int).Sub(in1, out1), nil
}

// decodeV3Style reads the two signed amounts that lead the Uniswap V3 payload.
// PancakeSwap V3 appends two protocol-fee words after the V3 fields, so the
// same reader covers it.
func decodeV3Style(data []byte) (*big.Int, *big.Int, error) {
	a0, err := Int256(data, 0)
	if err != nil {
		return nil, nil, err
	}
	a1, err := Int256(data, 1)
	if err != nil {
		return nil, nil, err
	}
	return a0, a1, nil
}

// knownSwaps covers the layouts behind the overwhelming majority of DEX volume
// on the EVM chains this service watches. A log whose topic0 is not here is
// skipped rather than guessed at — see docs/RESEARCH.md on why an unrecognized
// pool must show up as missing coverage instead of as invented volume.
var knownSwaps = func() map[string]SwapEvent {
	defs := []SwapEvent{
		{
			Name:      "uniswap_v2",
			Signature: "Swap(address,uint256,uint256,uint256,uint256,address)",
			decode:    decodeV2Style,
		},
		{
			Name:      "solidly",
			Signature: "Swap(address,address,uint256,uint256,uint256,uint256)",
			decode:    decodeV2Style,
		},
		{
			Name:      "uniswap_v3",
			Signature: "Swap(address,address,int256,int256,uint160,uint128,int24)",
			decode:    decodeV3Style,
		},
		{
			Name:      "pancake_v3",
			Signature: "Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)",
			decode:    decodeV3Style,
		},
	}
	m := make(map[string]SwapEvent, len(defs))
	for _, d := range defs {
		d.Topic0 = topicHex(d.Signature)
		m[d.Topic0] = d
	}
	return m
}()

// SwapTopics returns every topic0 to filter on, for the eth_getLogs call.
func SwapTopics() []string {
	out := make([]string, 0, len(knownSwaps))
	for topic := range knownSwaps {
		out = append(out, topic)
	}
	return out
}

// IsSwapTopic reports whether topic0 is a layout this service can decode.
func IsSwapTopic(topic0 string) bool {
	_, ok := knownSwaps[normalizeTopic(topic0)]
	return ok
}

// DecodeSwap turns a log into normalized amounts. An unrecognized topic0
// returns ok=false, which the caller records as an unsupported pool rather than
// treating as zero volume.
func DecodeSwap(topic0 string, data []byte) (Amounts, bool, error) {
	ev, ok := knownSwaps[normalizeTopic(topic0)]
	if !ok {
		return Amounts{}, false, nil
	}
	a0, a1, err := ev.decode(data)
	if err != nil {
		return Amounts{}, true, fmt.Errorf("%s: %w", ev.Name, err)
	}
	return Amounts{Amount0: a0, Amount1: a1, Event: ev.Name}, true, nil
}

func normalizeTopic(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
