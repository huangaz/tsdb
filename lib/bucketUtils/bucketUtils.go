package bucketUtils

import (
	"errors"
	"fmt"
)

const (
	GORILLA_SHARDS = 100
)

// Conversions from timestamp to bucket number.
func Bucket(unixTime, windowSize uint64, shardId int) (bucket uint32, err error) {
	if unixTime < uint64(shardId)*windowSize/GORILLA_SHARDS {
		err = errors.New(fmt.Sprintf("Timestamp %d falls into a negative bucket!", unixTime))
		return 0, err
	}
	bucket = uint32((unixTime - (uint64(shardId) * windowSize / GORILLA_SHARDS)) / windowSize)
	return bucket, nil
}

// Conversions from bucket number to timestamp.
func Timestamp(bucket uint32, windowSize uint64, shardId int) uint64 {
	return uint64(bucket)*windowSize + (uint64(shardId) * windowSize / GORILLA_SHARDS)
}

// Conversions from duration to number of buckets.
func Buckets(duration, windowSize uint64) uint32 {
	return uint32(duration / windowSize)
}

// Conversions from number of buckets to duration.
func Duration(buckets uint32, windowSize uint64) uint64 {
	return uint64(buckets) * windowSize
}

// Gets the timestamp of the bucket the original timestamp is in.
func FloorTimestamp(unixTime, windowSize uint64, shardId int) (uint64, error) {
	bucket, err := Bucket(unixTime, windowSize, shardId)
	if err != nil {
		return 0, err
	}
	return Timestamp(bucket, windowSize, shardId), nil
}
