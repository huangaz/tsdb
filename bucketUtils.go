package tsdb

// Conversions from timestamp to bucket number.
func Bucket(unixTime int64, windowSize uint64) uint32 {
	return uint32(unixTime/int64(windowSize) + 1)
}

// Conversions from bucket number to timestamp.
func Timestamp(bucket uint32, windowSize uint64) int64 {
	return int64(uint64(bucket-1) * windowSize)
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
func FloorTimestamp(unixTime int64, windowSize uint64) int64 {
	return Timestamp(Bucket(unixTime, windowSize), windowSize)
}
