// This class appends data points to a log file.
package dataLog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/huangaz/tsdb/lib/bitUtil"
	"github.com/huangaz/tsdb/lib/fileUtils"
	"io/ioutil"
	"math"
)

const (
	// The size of the internal buffer when logging data. Buffer size of 64K
	// equals roughly to 3 seconds of data before it's written to disk
	DATA_LOG_BUFFER_SIZE = 65535

	// This is the maximum allowed id for a time series in a shard. This is
	// used for sanity checking that the file isn't corrupt and to avoid
	// allocating too much memory
	MAX_ALLOWED_TIMESERIES_ID = 10000000

	// 3 bytes with with three unused bits. One of the is used for the control bit.
	SHORT_ID_BITS = 21
	// 4 bytes with with three unused bits. One of the is used for the control bit.
	LONG_ID_BITS = 29

	// Control bit of id.
	SHORT_ID_CONTROL_BIT = 0
	LONG_ID_CONTROL_BIT  = 1

	// 7 + 2 control bits -> 7 bits left in the byte.
	SHORT_DELTA_BITS = 7
	// -63
	SHORT_DELTA_MIN = -(1 << (SHORT_DELTA_BITS - 1)) + 1
	// 64
	SHORT_DELTA_MAX = (1 << (SHORT_DELTA_BITS - 1))

	// 14 + 3 control bits -> 7 bits left in the byte.
	MEDIUM_DELTA_BITS = 14
	// -8191
	MEDIUM_DELTA_MIN = -(1 << (MEDIUM_DELTA_BITS - 1)) + 1
	// 8192
	MEDIUM_DELTA_MAX = (1 << (MEDIUM_DELTA_BITS - 1))

	LARGE_DELTA_BITS = 32
	LARGE_DELTA_MIN  = -(1 << (LARGE_DELTA_BITS - 1)) + 1
	// LARGE_DELTA_MIN  = math.MinInt32

	// Control bits for the timestamp type
	ZERO_DELTA_CONTROL_VALUE   = 0 // 0
	SHORT_DELTA_CONTROL_VALUE  = 2 // 10
	MEDIUM_DELTA_CONTROL_VALUE = 6 // 110
	LARGE_DELTA_CONTROL_VALUE  = 7 // 111

	PREVIOUS_VALUE_VECTOR_SIZE_INCREMENT = 1000

	BLOCK_SIZE_BITS   = 6
	LEADING_ZERO_BITS = 5
	MIN_BYTE_NEEDED   = 3

	SAME_VALUE_CONTROL_BIT      = 0
	DIFFERENT_VALUE_CONTROL_BIT = 1
)

// This class appends data points to a log file.
type DataLogWriter struct {
	out_            *fileUtils.File
	lastTimestamp_  uint64
	buffer_         []byte
	bufferSize_     uint64
	previousvalues_ []float64
}

type DataLogReader struct {
}

// Initialize a DataLogWriter which will append data to the given file.
func NewDataLogWriter(out *fileUtils.File, baseTime uint64) *DataLogWriter {
	res := &DataLogWriter{
		out_:           out,
		lastTimestamp_: baseTime,
		buffer_:        make([]byte, DATA_LOG_BUFFER_SIZE),
		bufferSize_:    0,
	}
	return res
}

// Destructor
func (d *DataLogWriter) DeleteDataLogWriter() {
	if d.out_.File != nil {
		d.FlushBuffer()
		d.out_.File.Close()
	}
}

// Flushes the buffer that has been created with `append` calls to disk.
func (d *DataLogWriter) FlushBuffer() error {
	if d.bufferSize_ > 0 {
		tmpbuffer := d.buffer_[:d.bufferSize_]
		written, err := d.out_.File.Write(tmpbuffer)
		if err != nil {
			return err
		}
		if uint64(written) != d.bufferSize_ {
			errString := fmt.Sprintf("Flushing buffer failed! Wrote %d of %d to %s",
				written, d.bufferSize_, d.out_.Name)
			return errors.New(errString)
		}
		d.bufferSize_ = 0
	}
	return nil
}

// Appends a data point to the internal buffer. This operation is
// not thread safe. Caller is responsible for locking. Data will be
// written to disk when buffer is full or `flushBuffer` is called or
// destructor is called.
func (d *DataLogWriter) Append(id, unixTime uint64, value float64) error {
	b := bitUtil.NewBitStream(nil)

	if id > MAX_ALLOWED_TIMESERIES_ID {
		return errors.New("ID too large. Increase MAX_ALLOWED_TIMESERIES_ID?")
	}

	// Leave two bits unused in the current byte after adding the id to
	// allow the best case scenario of time delta = 0 and value/xor = 0.
	if id >= (1 << SHORT_ID_BITS) {
		b.AddValueToBitStream(LONG_ID_CONTROL_BIT, 1)
		b.AddValueToBitStream(id, LONG_ID_BITS)
	} else {
		b.AddValueToBitStream(SHORT_ID_CONTROL_BIT, 1)
		b.AddValueToBitStream(id, SHORT_ID_BITS)
	}

	// Optimize for zero delta case and increase used bits 8 at a time
	// to fill bytes.
	delta := int64(unixTime - d.lastTimestamp_)
	if delta == 0 {
		b.AddValueToBitStream(ZERO_DELTA_CONTROL_VALUE, 1)
	} else if delta >= SHORT_DELTA_MIN && delta <= SHORT_DELTA_MAX {
		delta -= SHORT_DELTA_MIN
		if delta >= (1 << SHORT_DELTA_BITS) {
			return errors.New("Delta is not less than 1<<SHORT_DELTA_BITS!")
		}

		b.AddValueToBitStream(SHORT_DELTA_CONTROL_VALUE, 2)
		b.AddValueToBitStream(uint64(delta), SHORT_DELTA_BITS)
	} else if delta >= MEDIUM_DELTA_MIN && delta <= MEDIUM_DELTA_MAX {
		delta -= MEDIUM_DELTA_MIN
		if delta >= (1 << MEDIUM_DELTA_BITS) {
			return errors.New("Delta is not less than 1<<MEDIUM_DELTA_BITS!")
		}

		b.AddValueToBitStream(MEDIUM_DELTA_CONTROL_VALUE, 3)
		b.AddValueToBitStream(uint64(delta), MEDIUM_DELTA_BITS)
	} else {
		delta -= LARGE_DELTA_MIN
		b.AddValueToBitStream(LARGE_DELTA_CONTROL_VALUE, 3)
		b.AddValueToBitStream(uint64(delta), LARGE_DELTA_BITS)
	}

	if id >= uint64(len(d.previousvalues_)) {
		// If the value hasn't been seen before, assume that the previous
		// value is zero.
		tmpSlice := make([]float64, id+PREVIOUS_VALUE_VECTOR_SIZE_INCREMENT-
			uint64(len(d.previousvalues_)))
		d.previousvalues_ = append(d.previousvalues_, tmpSlice...)
	}

	v := math.Float64bits(value)
	previousValue := math.Float64bits(d.previousvalues_[id])
	xorWithPrevious := v ^ previousValue
	if xorWithPrevious == 0 {
		// Same as previous value, just store a single bit.
		b.AddValueToBitStream(SAME_VALUE_CONTROL_BIT, 1)
	} else {
		b.AddValueToBitStream(DIFFERENT_VALUE_CONTROL_BIT, 1)

		leadingZeros := bitUtil.Clz(xorWithPrevious)
		trailingZeros := bitUtil.Ctz(xorWithPrevious)
		if leadingZeros > 31 {
			leadingZeros = 31
		}

		blockSize := 64 - leadingZeros - trailingZeros
		blockValue := xorWithPrevious >> trailingZeros

		b.AddValueToBitStream(leadingZeros, LEADING_ZERO_BITS)
		b.AddValueToBitStream(blockSize-1, BLOCK_SIZE_BITS)
		b.AddValueToBitStream(blockValue, blockSize)
	}

	d.previousvalues_[id] = value
	d.lastTimestamp_ = unixTime

	size := uint64(len(b.Stream))

	if size+d.bufferSize_ > DATA_LOG_BUFFER_SIZE {
		d.FlushBuffer()
	}

	// Write stream into buffer.
	dest := d.buffer_[d.bufferSize_ : d.bufferSize_+size]
	binary.Read(bytes.NewReader(b.Stream), binary.BigEndian, dest)
	d.bufferSize_ += size

	return nil
}

// Pull all the data points from the file.
// Returns the number of points read, or -1 if the file could not be read.
// The callback should return false if reading should be stopped.
func (d *DataLogReader) ReadLog(file *fileUtils.File, baseTime uint64,
	f func(uint64, uint64, float64) (out bool)) (points int, err error) {

	stream, err := ioutil.ReadAll(file.File)
	if err != nil {
		return -1, err
	}
	b := bitUtil.NewBitStream(stream)
	length := len(b.Stream)
	if length == 0 {
		return 0, nil
	}

	// Read out all the available points.
	prevTime := baseTime
	var previousValues []float64

	// Need at least three bytes for a complete value.
	for b.BitPos <= uint64(length*8-MIN_BYTE_NEEDED*8) {
		// Read the id of the time series.
		idControlBit, err := b.ReadValueFromBitStream(1)
		if err != nil {
			return -1, err
		}
		var id uint64
		if idControlBit == SHORT_ID_CONTROL_BIT {
			id, err = b.ReadValueFromBitStream(SHORT_ID_BITS)
			if err != nil {
				return points, err
			}
		} else {
			id, err = b.ReadValueFromBitStream(LONG_ID_BITS)
			if err != nil {
				return points, err
			}
		}

		if id > MAX_ALLOWED_TIMESERIES_ID {
			err = errors.New(fmt.Sprintf("Corrupt file. ID is too large %d", id))
			return points, err

		}

		// Read the time stamp delta based on the the number of bits in
		// the delta.
		timeDeltaControlValue, err := b.ReadValueThroughFirstZero(3)
		if err != nil {
			return points, err
		}

		var timeDalte int64
		var tmp uint64
		switch timeDeltaControlValue {
		case ZERO_DELTA_CONTROL_VALUE:
			break
		case SHORT_DELTA_CONTROL_VALUE:
			tmp, err = b.ReadValueFromBitStream(SHORT_DELTA_BITS)
			if err != nil {
				return points, err
			}
			timeDalte = int64(tmp) + SHORT_DELTA_MIN
		case MEDIUM_DELTA_CONTROL_VALUE:
			tmp, err = b.ReadValueFromBitStream(MEDIUM_DELTA_BITS)
			if err != nil {
				return points, err
			}
			timeDalte = int64(tmp) + MEDIUM_DELTA_MIN
		case LARGE_DELTA_CONTROL_VALUE:
			tmp, err = b.ReadValueFromBitStream(LARGE_DELTA_BITS)
			if err != nil {
				return points, err
			}
			timeDalte = int64(tmp) + LARGE_DELTA_MIN
		default:
			err = errors.New(fmt.Sprintf("Invalid time delta control value %d",
				timeDeltaControlValue))
			return points, err
		}

		unixTime := uint64(int64(prevTime) + timeDalte)
		prevTime = unixTime

		if id >= uint64(len(previousValues)) {
			tmpSlice := make([]float64, id+PREVIOUS_VALUE_VECTOR_SIZE_INCREMENT-
				uint64(len(previousValues)))
			previousValues = append(previousValues, tmpSlice...)
		}

		// Finally read the value.
		var value float64
		sameValueControlBit, err := b.ReadValueFromBitStream(1)
		if err != nil {
			return points, err
		}
		if sameValueControlBit == SAME_VALUE_CONTROL_BIT {
			value = previousValues[id]
		} else {
			leadingZeros, err := b.ReadValueFromBitStream(LEADING_ZERO_BITS)
			if err != nil {
				return points, err
			}
			blockSize, err := b.ReadValueFromBitStream(BLOCK_SIZE_BITS)
			if err != nil {
				return points, err
			}
			// ??
			blockSize += 1
			blockValue, err := b.ReadValueFromBitStream(blockSize)
			if err != nil {
				return points, err
			}

			// Shift to left by the number of trailing zeros
			blockValue <<= (64 - blockSize - leadingZeros)

			previousValue := math.Float64bits(previousValues[id])
			xorredValue := blockValue ^ previousValue
			value = math.Float64frombits(xorredValue)
		}

		previousValues[id] = value

		// Each tuple (id, unixTime, value) in the file is byte aligned.
		if b.BitPos%8 != 0 {
			b.BitPos += (8 - b.BitPos%8)
		}

		if f(id, unixTime, value) != true {
			// // Callback doesn't accept more points.
			break
		}
		points++
	}
	return points, nil
}
