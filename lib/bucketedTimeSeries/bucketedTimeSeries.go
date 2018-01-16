// This class hold a rolling window of TimeSeries data.
package bucketedTimeSeries

import (
	"errors"
	"github.com/huangaz/tsdb/lib/bucketStorage"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/timeSeriesStream"
	"github.com/huangaz/tsdb/lib/utils"
	"math"
	"sync"
)

const (
	// Values coming in faster than this are considered spam.
	MIN_TIMESERIES_DELTA        = 30
	DEFAULT_CATEGORY     uint16 = 0
)

type BucketedTimeSeries struct {
	// Current stream of data.
	stream_ *timeSeriesStream.Series

	// Number of points in the active bucket (stream_)
	count_ uint16

	// Currently active bucket
	current_ uint32

	queriedBucketsAgo_ uint8

	// Blocks of metadata for previous data.
	blocks_ []uint64

	lock_ sync.Mutex
}

func NewBucketedTimeSeries() *BucketedTimeSeries {
	res := &BucketedTimeSeries{
		stream_: timeSeriesStream.NewSeries(nil),
	}
	return res
}

// Initialize a BucketedTimeSeries with n historical buckets and
// one active bucket.
// Not thread-safe.
func (b *BucketedTimeSeries) Reset(n uint8) {
	b.queriedBucketsAgo_ = math.MaxUint8
	//lock_Init()
	b.current_ = 0
	b.blocks_ = make([]uint64, n)
	for i := 0; i < int(n); i++ {
		b.blocks_[i] = bucketStorage.INVALID_ID
	}
	b.count_ = 0
	b.stream_.Reset()
	b.stream_.ExtraData = DEFAULT_CATEGORY
}

// Open the next bucket for writes, copy out the current active data.
// `next`: the number of next bucket
// `storage`: store current active data
func (b *BucketedTimeSeries) open(next, timeSeriesId uint32,
	storage *bucketStorage.BucketStorage) (err error) {

	if b.current_ == 0 {
		// Skip directly to the new value.
		b.current_ = next
		return nil
	}

	var block uint64
	// Wipe all the blocks in between.
	for b.current_ != next {
		if b.count_ > 0 {
			// Copy out the active data.
			block, err = storage.Store(b.current_, b.stream_.Bs.Stream, b.count_,
				timeSeriesId)
			if err != nil {
				return err
			}
		} else {
			block = bucketStorage.INVALID_ID
		}
		b.blocks_[b.current_%uint32(storage.NumBuckets())] = block

		// Prepare for writes.
		b.count_ = 0
		b.stream_.Reset()
		b.current_++

		if b.queriedBucketsAgo_ < math.MaxUint8 {
			b.queriedBucketsAgo_++
		}
	}
	return nil
}

// Add a data point to the given bucket.
// If category pointer is defined, sets the category.
// `i`: the number of bucket to store data
// `dp`: data point to be stored
func (b *BucketedTimeSeries) Put(i, timeSeriesId uint32, dp dataTypes.DataPoint,
	storage *bucketStorage.BucketStorage, category *uint16) (err error) {

	b.lock_.Lock()
	defer b.lock_.Unlock()

	if i < b.current_ {
		return errors.New("Invalid bucket number!")
	}

	if i > b.current_ {
		err = b.open(i, timeSeriesId, storage)
		if err != nil {
			return err
		}
	}

	err = b.stream_.Append(dp.Timestamp, dp.Value, MIN_TIMESERIES_DELTA)
	if err != nil {
		return err
	}

	if category != nil {
		b.stream_.ExtraData = *category
	}

	b.count_++
	return nil
}

// Read out buckets between begin and end inclusive, including current one.
func (b *BucketedTimeSeries) Get(begin, end uint32,
	storage *bucketStorage.BucketStorage) (out []dataTypes.TimeSeriesBlock, err error) {

	n := storage.NumBuckets()

	b.lock_.Lock()
	defer b.lock_.Unlock()

	getCurrent := begin <= b.current_ && end >= b.current_

	if b.current_ >= 1 {
		end = utils.MinUint32(end, b.current_-1)
	} else {
		end = utils.MinUint32(end, 0)
	}

	if b.current_ >= uint32(n) {
		begin = utils.MaxUint32(begin, b.current_-uint32(n))
	} else {
		begin = utils.MaxUint32(begin, 0)
	}

	// Read data.
	var outBlock dataTypes.TimeSeriesBlock
	for i := begin; i <= end; i++ {
		outBlock.Data, outBlock.Count, err = storage.Fetch(i, b.blocks_[i%uint32(n)])
		/*
			if err != nil {
				return nil, err
			}
			out = append(out, outBlock)
		*/
		if err == nil {
			out = append(out, outBlock)
		}
	}

	if getCurrent == true {
		outBlock.Count = b.count_
		outBlock.Data = b.stream_.ReadData()
		out = append(out, outBlock)
	}

	return out, nil
}

// Sets the current bucket. Flushes data from the previous bucket to
// BucketStorage. No-op if this time series is already at currentBucket.
func (b *BucketedTimeSeries) SetCurrentBucket(currentBucket, timeSeriesId uint32,
	storage *bucketStorage.BucketStorage) (err error) {

	b.lock_.Lock()
	defer b.lock_.Unlock()

	if b.current_ < currentBucket {
		err = b.open(currentBucket, timeSeriesId, storage)
		if err != nil {
			return err
		}
	}
	return nil
}

// Sets that this time series was just queried.
func (b *BucketedTimeSeries) SetQueried() {
	b.queriedBucketsAgo_ = 0
}

func (b *BucketedTimeSeries) SetDataBlock(position uint32, numBuckets uint8, id uint64) {
	b.lock_.Lock()
	defer b.lock_.Unlock()
	// Needed for time series that receive data very rarely.
	if position >= b.current_ {
		b.current_ = position + 1
		b.count_ = 0
		b.stream_.Reset()
	}
	b.blocks_[position%uint32(numBuckets)] = id
}

// Returns true if there are data points for this time series.
func (b *BucketedTimeSeries) HasDataPoints(numBuckets uint8) bool {
	b.lock_.Lock()
	defer b.lock_.Unlock()

	if b.count_ > 0 {
		return true
	}

	for i := 0; i < int(numBuckets); i++ {
		if b.blocks_[i] != bucketStorage.INVALID_ID {
			return true
		}
	}
	return false
}

// Returns how many buckets ago this value was queried.
// Will return 255 if it has never been queried.
func (b *BucketedTimeSeries) GetQueriedBucketsAgo() uint8 {
	return b.queriedBucketsAgo_
}

// Returns the ODS category associated with this time series.
func (b *BucketedTimeSeries) GetCategory() uint16 {
	b.lock_.Lock()
	defer b.lock_.Unlock()
	return b.stream_.ExtraData
}

// Sets the ODS category for this time series.
func (b *BucketedTimeSeries) SetCategory(category uint16) {
	b.lock_.Lock()
	defer b.lock_.Unlock()
	b.stream_.ExtraData = category
}
