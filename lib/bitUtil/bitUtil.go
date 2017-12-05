package bitUtil

import (
	"errors"
)

type BitStream struct {
	// Bit stream
	Stream []byte
	// Number of bits in the stream
	NumBits uint64
	// Position of bit that have been read
	BitPos uint64
}

/*
func (b BitStream) String() string {
	res := fmt.Sprintln(b.Stream)
	res += fmt.Sprintf("NumBits: %d\n", b.NumBits)
	res += fmt.Sprintf("BitPos: %d\n", b.BitPos)
	return res
}
*/

// Adds a value to a bit stream. `bitsInValue` specifies the number
// of least significant bits that will be added to the bit
// stream. The bits from `value` will be added from the most
// significant bit to the least significant bit.
func (b *BitStream) AddValueToBitStream(value uint64, bitsInValue uint64) {
	var bitsAvailable uint64
	// Calculate the numbers of bits available in the last byte
	if b.NumBits&0x7 == 0 {
		bitsAvailable = 0
	} else {
		bitsAvailable = 8 - (b.NumBits & 0x7)
	}
	b.NumBits += bitsInValue
	lastByte := len(b.Stream) - 1

	if bitsInValue <= bitsAvailable {
		// Value can be stored in the last byte
		b.Stream[lastByte] += byte(value << (bitsAvailable - bitsInValue))
		return
	}

	bitLeft := bitsInValue
	if bitsAvailable > 0 {
		// Fill up the last byte
		b.Stream[lastByte] += byte(value >> (bitsInValue - bitsAvailable))
		bitLeft -= bitsAvailable
	}

	for bitLeft >= 8 {
		// Store every 8 bits as a byte
		b.Stream = append(b.Stream, byte(value>>(bitLeft-8)&0xFF))
		bitLeft -= 8
	}

	if bitLeft != 0 {
		// Store the rest of the bits in a new byte
		b.Stream = append(b.Stream, byte(value&((1<<bitLeft)-1)<<(8-bitLeft)))
	}
}

// Reads a value from a bit stream. `bitsToRead` must be 64 or less.
func (b *BitStream) ReadValueFromBitStream(bitsToRead uint64) (uint64, error) {
	if b.BitPos+bitsToRead > b.NumBits {
		var err = errors.New("Trying to read too many bits!")
		return 0, err
	}
	var res uint64
	for i := uint64(0); i < bitsToRead; i++ {
		res <<= 1
		bit := uint64((b.Stream[b.BitPos>>3] >> (7 - (b.BitPos & 0x7))) & 1)
		res += bit
		b.BitPos++
	}
	return res, nil
}

// Finds the first zero bit and returns its distance from bitPos. If
// not found within limit, returns limit.
func (b *BitStream) FindTheFirstZeroBit(limit uint64) (uint64, error) {
	for i := uint64(0); i < limit; i++ {
		bit, err := b.ReadValueFromBitStream(1)
		if err != nil {
			return 0, err
		}
		if bit == 0 {
			return i, nil
		}
	}
	return limit, nil
}

// Reads a value until the first zero bit is found or limit reached.
// The zero is included in the value as the least significant bit.
// `limit` must be 32 or less.
func (b *BitStream) ReadValueThroughFirstZero(limit uint64) (uint64, error) {
	var res uint64
	for i := uint64(0); i < limit; i++ {
		bit, err := b.ReadValueFromBitStream(1)
		if err != nil {
			return 0, err
		}
		res = (res << 1) + bit
		if bit == 0 {
			return res, nil
		}
	}
	return res, nil
}

// Counts trailing zeroes
func Ctz(x uint64) uint64 {
	if x == 0 {
		return 64
	}

	var n uint64
	if (x & 0x00000000FFFFFFFF) == 0 {
		n = n + 32
		x = x >> 32
	}
	if (x & 0x000000000000FFFF) == 0 {
		n = n + 16
		x = x >> 16
	}
	if (x & 0x00000000000000FF) == 0 {
		n = n + 8
		x = x >> 8
	}
	if (x & 0x000000000000000F) == 0 {
		n = n + 4
		x = x >> 4
	}
	if (x & 0x0000000000000003) == 0 {
		n = n + 2
		x = x >> 2
	}
	if (x & 0x0000000000000001) == 0 {
		n = n + 1
	}

	return n
}

// Counts leading zeroes
func Clz(x uint64) uint64 {
	var n uint64 = 1

	if (x >> 32) == 0 {
		n = n + 32
		x = x << 32
	}
	if (x >> (32 + 16)) == 0 {
		n = n + 16
		x = x << 16
	}
	if (x >> (32 + 16 + 8)) == 0 {
		n = n + 8
		x = x << 8
	}
	if (x >> (32 + 16 + 8 + 4)) == 0 {
		n = n + 4
		x = x << 4
	}
	if (x >> (32 + 16 + 8 + 4 + 2)) == 0 {
		n = n + 2
		x = x << 2
	}

	n = n - (x >> 63)
	return n
}
