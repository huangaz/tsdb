package dataTypes

type DataPoint struct {
	Value     float64
	Timestamp uint64
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
)
