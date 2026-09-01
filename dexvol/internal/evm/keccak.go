// Package evm holds the Ethereum primitives this service needs: Keccak-256 and
// enough ABI decoding to read a Swap log.
package evm

import (
	"encoding/binary"
	"math/bits"
)

// Keccak-256 is implemented here rather than pulled from x/crypto/sha3 so the
// service keeps zero external dependencies. Correctness is pinned by
// known-answer tests, including the Transfer(address,address,uint256) event
// topic, whose value is fixed for every ERC-20 contract ever deployed.
//
// Note this is original Keccak (0x01 padding), not the later NIST SHA3-256
// (0x06 padding). Ethereum uses the former; using the latter would produce
// hashes that look plausible and match nothing on chain.

const (
	rate      = 136 // 1088 bits, the Keccak-256 block size
	rounds    = 24
	padByte   = 0x01
	padFinish = 0x80
)

var roundConstants = [rounds]uint64{
	0x0000000000000001, 0x0000000000008082, 0x800000000000808a, 0x8000000080008000,
	0x000000000000808b, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
	0x000000000000008a, 0x0000000000000088, 0x0000000080008009, 0x000000008000000a,
	0x000000008000808b, 0x800000000000008b, 0x8000000000008089, 0x8000000000008003,
	0x8000000000008002, 0x8000000000000080, 0x000000000000800a, 0x800000008000000a,
	0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
}

// rotationOffsets and lanePermutation drive the rho and pi steps.
var (
	rotationOffsets = [24]int{1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44}
	lanePermutation = [24]int{10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1}
)

func permute(a *[25]uint64) {
	var bc [5]uint64
	for round := 0; round < rounds; round++ {
		// Theta
		for i := 0; i < 5; i++ {
			bc[i] = a[i] ^ a[i+5] ^ a[i+10] ^ a[i+15] ^ a[i+20]
		}
		for i := 0; i < 5; i++ {
			t := bc[(i+4)%5] ^ bits.RotateLeft64(bc[(i+1)%5], 1)
			for j := 0; j < 25; j += 5 {
				a[i+j] ^= t
			}
		}
		// Rho and Pi
		t := a[1]
		for i := 0; i < 24; i++ {
			j := lanePermutation[i]
			bc[0] = a[j]
			a[j] = bits.RotateLeft64(t, rotationOffsets[i])
			t = bc[0]
		}
		// Chi
		for j := 0; j < 25; j += 5 {
			for i := 0; i < 5; i++ {
				bc[i] = a[j+i]
			}
			for i := 0; i < 5; i++ {
				a[j+i] ^= (^bc[(i+1)%5]) & bc[(i+2)%5]
			}
		}
		// Iota
		a[0] ^= roundConstants[round]
	}
}

// Keccak256 returns the 32-byte Keccak-256 digest of data.
func Keccak256(data []byte) [32]byte {
	var state [25]uint64

	absorb := func(block []byte) {
		for i := 0; i < rate/8; i++ {
			state[i] ^= binary.LittleEndian.Uint64(block[i*8:])
		}
		permute(&state)
	}

	for len(data) >= rate {
		absorb(data[:rate])
		data = data[rate:]
	}

	// Pad the final partial block: 0x01, zeros, then 0x80 in the last byte.
	// When the remainder is exactly rate-1 bytes both pad bits land on the
	// same byte, which is why this is an OR rather than an assignment.
	var block [rate]byte
	copy(block[:], data)
	block[len(data)] = padByte
	block[rate-1] |= padFinish
	absorb(block[:])

	var out [32]byte
	for i := 0; i < 4; i++ {
		binary.LittleEndian.PutUint64(out[i*8:], state[i])
	}
	return out
}

// EventTopic returns the topic0 for an event signature such as
// "Swap(address,uint256,uint256,uint256,uint256,address)".
//
// Deriving it from the signature rather than hardcoding a hex constant means a
// typo cannot silently produce a filter that matches nothing.
func EventTopic(signature string) [32]byte {
	return Keccak256([]byte(signature))
}
