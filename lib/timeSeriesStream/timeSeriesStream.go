// This class operates on a stream of TimeSeries Data (timestamp, value)
package timeSeriesStream

import (
	"errors"
	"github.com/huangaz/tsdb/lib/bitUtil"
	"math"
)

const (
	DEFAULT_DELTA             = 60
	BITS_FOR_FIRST_TIMESTAMP  = 32
	LEADING_ZEROS_LENGTH_BITS = 5
	BLOCK_SIZE_LENGTH_BITS    = 6
	BLOCK_SIZE_ADJUSTMENT     = 1
	MAX_LEADING_ZEROS_LENGTH  = (1 << LEADING_ZEROS_LENGTH_BITS) - 1
)

type Series struct {
	Bs bitUtil.BitStream

	// use for appendTimestamp()
	prevTimeWrite      int64
	prevTimeDeltaWrite int64

	// use for readNextTimestamp()
	prevTimeRead      int64
	prevTimeDeltaRead int64

	// use for appendValue()
	prevValueWrite    float64
	prevLeadingWrite  uint64
	prevTrailingWrite uint64

	// use for readNextValue()
	prevValueRead    float64
	prevLeadingRead  uint64
	prevTrailingRead uint64

	ExtraData uint16
}

type timestampEncoding struct {
	bitsForValue          uint64
	controlValue          uint64
	controlValueBitLength uint64
}

/*
* deltaOfDelta 	tag 	value bits
* 0		-	1
* -63,64	10	7
* -255,256	110	9
* -2047,2048	1110	12
* >2048		1111	32
 */
var timestampEncodings = []timestampEncoding{
	{7, 2, 2},
	{9, 6, 3},
	{12, 14, 4},
	{32, 15, 4},
}

// Appends a (timestamp, value) pair to the current stream.
// minTimestampDelta is the minimum timestamp delta
func (s *Series) Append(timestamp int64, value float64, minTimestampDelta int64) error {
	if err := s.appendTimestamp(timestamp, minTimestampDelta); err != nil {
		return err
	}
	s.appendValue(value)
	return nil
}

// Read a (timestamp, value) pair from the current stream.
func (s *Series) Read() (timestamp int64, value float64, err error) {
	if timestamp, err = s.readNextTimestamp(); err != nil {
		return 0, 0, err
	}
	if value, err = s.readNextValue(); err != nil {
		return 0, 0, err
	}
	return
}

// Append a timestamp in the current stream.
// timestamp:0-4294967295
// Store a delta of delta for the rest of the values in one of the
// following ways
//
// '0' = delta of delta did not change
// '10' followed by a value length of 7
// '110' followed by a value length of 9
// '1110' followed by a value length of 12
// '1111' followed by a value length of 32
func (s *Series) appendTimestamp(timestamp int64, minTimestampDelta int64) error {
	delta := timestamp - s.prevTimeWrite
	// Skip the minTimestampDelta check for the first timestamp.
	if delta < minTimestampDelta && s.prevTimeWrite != 0 {
		return errors.New("Timestamp delta is smaller than minTimestampDelta!")
	}

	if len(s.Bs.Stream) == 0 {
		// Store the first timestamp
		s.Bs.AddValueToBitStream(uint64(timestamp), BITS_FOR_FIRST_TIMESTAMP)
		s.prevTimeWrite = timestamp
		s.prevTimeDeltaWrite = DEFAULT_DELTA
		return nil
	}

	deltaOfDelta := delta - s.prevTimeDeltaWrite

	if deltaOfDelta == 0 {
		s.prevTimeWrite = timestamp
		s.Bs.AddValueToBitStream(0, 1)
		return nil
	}

	if deltaOfDelta > 0 {
		// There are no zeros. Shift by one to fit in x number of bits
		deltaOfDelta--
	}

	absValue := int64(math.Abs(float64(deltaOfDelta)))

	for i := 0; i < 4; i++ {
		if absValue < (1 << uint(timestampEncodings[i].bitsForValue-1)) {
			s.Bs.AddValueToBitStream(timestampEncodings[i].controlValue,
				timestampEncodings[i].controlValueBitLength)
			// Make this value between [0, 2^timestampEncodings[i].bitsForValue - 1]
			encodedValue := deltaOfDelta + (1 << uint(timestampEncodings[i].bitsForValue-1))
			s.Bs.AddValueToBitStream(uint64(encodedValue), timestampEncodings[i].bitsForValue)
			break
		}
	}

	s.prevTimeWrite = timestamp
	s.prevTimeDeltaWrite = delta
	return nil
}

// Read a timestamp fron the current stream.
func (s *Series) readNextTimestamp() (int64, error) {
	// first timestamp
	if s.Bs.BitPos == 0 {
		s.prevTimeDeltaRead = DEFAULT_DELTA
		timestamp, err := s.Bs.ReadValueFromBitStream(BITS_FOR_FIRST_TIMESTAMP)
		if err != nil {
			return 0, err
		}
		s.prevTimeRead = int64(timestamp)
		return int64(timestamp), nil
	}

	index, err := s.Bs.FindTheFirstZeroBit(4)
	if err != nil {
		return 0, err
	}
	if index > 0 {
		// Delta of delta is non zero. Calculate the new delta.
		// 'index' will be used to find the right length for the value
		// that is read.
		index--
		decodeValue, err := s.Bs.ReadValueFromBitStream(timestampEncodings[index].bitsForValue)
		if err != nil {
			return 0, err
		}
		value := int64(decodeValue)
		// [0,255] becomes [-128,127]
		value -= (1 << (timestampEncodings[index].bitsForValue - 1))
		if value >= 0 {
			// [-128,127] becomes [-128,128] without the zero in the middle
			value++
		}
		s.prevTimeDeltaRead += value
	}
	s.prevTimeRead += s.prevTimeDeltaRead
	return s.prevTimeRead, nil
}

// Append a value in the current stream
// Values are encoded by XORing them with the previous value. If
// XORing results in a zero value (value is the same as the previous
// value), only a single zero bit is stored, otherwise 1 bit is
// stored.
//
// For non-zero XORred results, there are two choices:
//
// 1) If the block of meaningful bits falls in between the block of
//    previous meaningful bits, i.e., there are at least as many
//    leading zeros and as many trailing zeros as with the previous
//    value, use that information for the block position and just
//    store the XORred value.
//
// 2) Length of the number of leading zeros is stored in the next 5
//    bits, then length of the XORred value is stored in the next 6
//    bits and finally the XORred value is stored.
func (s *Series) appendValue(value float64) {
	xorWithPrev := math.Float64bits(value) ^ math.Float64bits(s.prevValueWrite)
	if xorWithPrev == 0 {
		s.Bs.AddValueToBitStream(0, 1)
		return
	} else {
		s.Bs.AddValueToBitStream(1, 1)
	}

	// Calculate the numbers of leading and trailing zeros
	leading := bitUtil.Clz(xorWithPrev)
	trailing := bitUtil.Ctz(xorWithPrev)

	if leading > MAX_LEADING_ZEROS_LENGTH {
		leading = MAX_LEADING_ZEROS_LENGTH
	}

	blockSize := 64 - leading - trailing
	expectedSize := LEADING_ZEROS_LENGTH_BITS + BLOCK_SIZE_LENGTH_BITS + blockSize
	prevBolckInformationSize := 64 - s.prevLeadingWrite - s.prevTrailingWrite

	if leading >= s.prevLeadingWrite && trailing >= s.prevTrailingWrite && prevBolckInformationSize < expectedSize {
		// Control bit for using previous block information.
		s.Bs.AddValueToBitStream(1, 1)
		blockValue := xorWithPrev >> s.prevTrailingWrite
		s.Bs.AddValueToBitStream(blockValue, prevBolckInformationSize)
	} else {
		// Control bit for not using previous block information.
		s.Bs.AddValueToBitStream(0, 1)
		s.Bs.AddValueToBitStream(leading, LEADING_ZEROS_LENGTH_BITS)
		// To fit in 6 bits. There will never be a zero size block
		s.Bs.AddValueToBitStream(blockSize-BLOCK_SIZE_ADJUSTMENT, BLOCK_SIZE_LENGTH_BITS)
		blockValue := xorWithPrev >> trailing
		s.Bs.AddValueToBitStream(blockValue, blockSize)
		s.prevLeadingWrite = leading
		s.prevTrailingWrite = trailing
	}
	s.prevValueWrite = value
}

// Read a value from the current stream.
func (s *Series) readNextValue() (float64, error) {
	nonZeroValue, err := s.Bs.ReadValueFromBitStream(1)
	if err != nil {
		return 0, err
	}

	if nonZeroValue == 0 {
		return s.prevValueRead, nil
	}

	usePrevBlockInformation, err := s.Bs.ReadValueFromBitStream(1)
	if err != nil {
		return 0, err
	}

	var xorValue uint64
	if usePrevBlockInformation == 1 {
		xorValue, err = s.Bs.ReadValueFromBitStream(64 - s.prevLeadingRead - s.prevTrailingRead)
		if err != nil {
			return 0, err
		}
		xorValue <<= s.prevTrailingRead
	} else {
		leading, err := s.Bs.ReadValueFromBitStream(LEADING_ZEROS_LENGTH_BITS)
		if err != nil {
			return 0, err
		}
		blockSize, err := s.Bs.ReadValueFromBitStream(BLOCK_SIZE_LENGTH_BITS)
		if err != nil {
			return 0, err
		}
		blockSize += BLOCK_SIZE_ADJUSTMENT
		s.prevTrailingRead = 64 - leading - blockSize
		xorValue, err = s.Bs.ReadValueFromBitStream(blockSize)
		if err != nil {
			return 0, err
		}
		xorValue <<= s.prevTrailingRead
		s.prevLeadingRead = leading
	}

	value := math.Float64frombits(xorValue ^ math.Float64bits(s.prevValueRead))
	s.prevValueRead = value
	return value, nil
}

// Reset Series.
func (s *Series) Reset() {
	s.Bs.Stream = s.Bs.Stream[:0]
	s.Bs.NumBits = 0
	s.Bs.BitPos = 0
	s.prevTimeDeltaRead = 0
	s.prevTimeDeltaWrite = 0
	s.prevValueRead = 0
	s.prevValueWrite = 0
	s.prevLeadingRead = 0
	s.prevLeadingWrite = 0
	s.prevTrailingRead = 0
	s.prevTrailingWrite = 0
}

// Return the stream as a byte slice.
func (s *Series) ReadData() []byte {
	return s.Bs.Stream
}
