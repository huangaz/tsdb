package dataTypes

type DataPoint struct {
	Value     float64
	Timestamp uint64
}

type DataBlock struct {
	data [DATA_BLOCK_SIZE]byte
}

const (
	DATA_BLOCK_SIZE = 65535
	DATA_PRE_FIX    = "block_data"
	COMPLETE_PREFIX = "complete_block"
)
