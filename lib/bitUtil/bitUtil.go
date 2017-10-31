package bitUtil

import (
	"errors"
	"fmt"
)

type BitStream struct {
	Stream  []byte
	NumBits uint64
	BitPos  uint64
}

func (b BitStream) String() string {
	res := fmt.Sprintln(b.Stream)
	res += fmt.Sprintf("NumBits: %d\n", b.NumBits)
	res += fmt.Sprintf("BitPos: %d\n", b.BitPos)
	return res
}

func (b *BitStream) AddValueToBitStream(value uint64, bitsInValue uint64) {
	var bitsAvailable uint64
	// calculate the numbers of bits available in the last byte
	if b.NumBits&0x7 == 0 {
		bitsAvailable = 0
	} else {
		bitsAvailable = 8 - (b.NumBits & 0x7)
	}
	b.NumBits += bitsInValue
	lastByte := len(b.Stream) - 1

	if bitsInValue <= bitsAvailable {
		// value can be stored in the last byte
		b.Stream[lastByte] += byte(value << (bitsAvailable - bitsInValue))
		return
	}

	bitLeft := bitsInValue
	if bitsAvailable > 0 {
		// fill up the last byte
		b.Stream[lastByte] += byte(value >> (bitsInValue - bitsAvailable))
		bitLeft -= bitsAvailable
	}

	for bitLeft >= 8 {
		// store every 8 bits as a byte
		b.Stream = append(b.Stream, byte(value>>(bitLeft-8)&0xFF))
		bitLeft -= 8
	}

	if bitLeft != 0 {
		// store the rest of the bits in a new byte
		b.Stream = append(b.Stream, byte(value&((1<<bitLeft)-1)<<(8-bitLeft)))
	}
}

func (b *BitStream) ReadValueFromBitStream(bitsToRead uint64) (uint64, error) {
	if b.BitPos+bitsToRead > b.NumBits {
		var err = errors.New("Trying to read too many bits")
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

func (b *BitStream) FindTheFirstZeroBit(limit uint64) (uint64, error) {
	for index := uint64(0); index < limit; index++ {
		bit, err := b.ReadValueFromBitStream(1)
		if err != nil {
			return 0, err
		}
		if bit == 0 {
			return index, nil
		}
	}
	return limit, nil
}

// Ctz counts trailing zeroes
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

// Clz counts leading zeroes
func Clz(x uint64) uint64 {
	var n uint64

	n = 1

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
	return uint64(n)
}
