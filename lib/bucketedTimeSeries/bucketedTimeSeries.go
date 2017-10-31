package bucketedTimeSeries

import (
	"errors"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/timeSeriesStream"
)

const (
	// Values coming in faster than this are considered spam.
	MIN_TIMESERIES_DELTA = 30
)

type BucketedTimeSeries struct {
	// Current stream of data.
	stream_ timeSeriesStream.Series
	// Number of points in the active bucket (stream_)
	count_ uint16
	// Currently active bucket
	current_ uint32
}

func (b *BucketedTimeSeries) GetInfo() (count uint16) {
	return b.count_
}

func NewBucketedTimeSeries() *BucketedTimeSeries {
	return &BucketedTimeSeries{}
}

func (b *BucketedTimeSeries) Put(i, timeseriesId uint32, dp dataTypes.DataPoint) error {
	if i < b.current_ {
		return errors.New("invalid bucket number!")
	}

	if i != b.current_ {
		b.openNextBucket(i, timeseriesId)
	}

	if err := b.stream_.Append(dp.Timestamp, dp.Value, MIN_TIMESERIES_DELTA); err != nil {
		return err
	}

	b.count_++
	return nil
}

/*
func (b *BucketedTimeSeries) Put(dp dataTypes.DataPoint) error {
	if err := b.stream_.Append(dp.Timestamp, dp.Value, MIN_TIMESERIES_DELTA); err != nil {
		return err
	} else {
		b.count_++
		return nil
	}
}
*/

// Open the next bucket for writes
func (b *BucketedTimeSeries) openNextBucket(next, timeseriesId uint32) {

}

func (b *BucketedTimeSeries) Get() (dps []dataTypes.DataPoint, err error) {
	var dp dataTypes.DataPoint

	for i := b.count_; i > 0; i-- {
		if dp.Timestamp, dp.Value, err = b.stream_.Read(); err != nil {
			return dps, err
		} else {
			dps = append(dps, dp)
		}
	}

	return dps, nil
}
