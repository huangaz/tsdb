package dataTypes

/*
unixTime 	: int64
shardId 	: int64

bucketNumber 	: uint32
windowSize 	: uint64
*/

type DataPoint struct {
	Value     float64
	Timestamp int64
}

type DataBlock struct {
	Data [DATA_BLOCK_SIZE]byte
}

type TimeSeriesBlock struct {
	Count uint16
	Data  []byte
}

const (
	DATA_BLOCK_SIZE = 65536
	PAGE_SIZE       = DATA_BLOCK_SIZE
	DATA_PRE_FIX    = "block_data"
	COMPLETE_PREFIX = "complete_block"
	LOG_FILE_PREFIX = "log"
	KEY_FILE_PREFIX = "key_list"
)
