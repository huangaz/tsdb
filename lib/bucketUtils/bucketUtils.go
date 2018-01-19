package bucketUtils

const (
	// Number of maps for gorilla to use
	GORILLA_SHARDS = 100
)

// Conversions from timestamp to bucket number.
func Bucket(unixTime int64, windowSize uint64, shardId int64) uint32 {
	if unixTime < shardId*int64(windowSize)/GORILLA_SHARDS {
		// log.Printf("Timestamp %d falls into a negative bucket!", unixTime)
		return 0
	}
	return uint32((unixTime - (shardId * int64(windowSize) / GORILLA_SHARDS)) / int64(windowSize))
}

// Conversions from bucket number to timestamp.
func Timestamp(bucket uint32, windowSize uint64, shardId int64) int64 {
	return int64(bucket)*int64(windowSize) + (shardId * int64(windowSize) / GORILLA_SHARDS)
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
func FloorTimestamp(unixTime int64, windowSize uint64, shardId int64) int64 {
	return Timestamp(Bucket(unixTime, windowSize, shardId), windowSize, shardId)
}
