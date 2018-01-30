package tsdb

import (
	"testing"
)

func TestAddAndRead(t *testing.T) {
	// Normal add and read value
	b := NewBitStream(nil)
	for i := uint64(1); i < 20; i++ {
		b.AddValueToBitStream(i, i)
	}
	for i := uint64(1); i < 20; i++ {
		res, err := b.ReadValueFromBitStream(i)
		if err != nil {
			t.Fatal(err)
		}
		if res != i {
			t.Fatal("different between Add and Read!")
		}
	}

	// Read too many bits
	_, err := b.ReadValueFromBitStream(1)
	if err == nil || err.Error() != "Trying to read too many bits!" {
		t.Fatal("Wrong err message when reading too many bits")
	}
}

func TestFindTheFirstZeroBit(t *testing.T) {
	b := NewBitStream(nil)
	b.AddValueToBitStream(240, 8) // 11110000
	res, err := b.FindTheFirstZeroBit(2)
	if err != nil {
		t.Fatal(err)
	}
	if res != 2 {
		t.Fatal("Limit did not work!")
	}

	var b2 BitStream
	b2.AddValueToBitStream(240, 8)
	res, err = b2.FindTheFirstZeroBit(b2.NumBits)
	if err != nil {
		t.Fatal(err)
	}
	if res != 4 {
		t.Fatal("Wrong result!")
	}
}

func TestReadValueThroughFirstZero(t *testing.T) {
	b := NewBitStream(nil)
	b.AddValueToBitStream(240, 8)
	res, err := b.ReadValueThroughFirstZero(b.NumBits)
	if err != nil {
		t.Fatal(err)
	}
	if res != 30 { // 11110
		t.Fatal("Wrong Result!")
	}

	// test of limit
	var b2 BitStream
	b2.AddValueToBitStream(240, 8)
	res, err = b2.ReadValueThroughFirstZero(2)
	if err != nil {
		t.Fatal(err)
	}
	if res != 3 { // 11
		t.Fatal("Wrong result!")
	}
}

func TestCtz(t *testing.T) {
	res := Ctz(0)
	if res != 64 {
		t.Fatal("Wrong result when x = 0!")
	}

	var x uint64 = 1 << 63
	res = Ctz(x)
	if res != 63 {
		t.Fatal("Wrong result when x = 1<<63!")
	}
}

func TestClz(t *testing.T) {
	res := Clz(1)
	if res != 63 {
		t.Fatal("Wrong reault when x = 63!")
	}
}
