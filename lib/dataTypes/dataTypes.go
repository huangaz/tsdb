package dataTypes

import "fmt"

/*
unixTime 	: int64
shardId 	: int64

bucketNumber 	: uint32
windowSize 	: uint64
*/

const (
	DATA_BLOCK_SIZE = 65536
	PAGE_SIZE       = DATA_BLOCK_SIZE
	DATA_PRE_FIX    = "block_data"
	COMPLETE_PREFIX = "complete_block"
	LOG_FILE_PREFIX = "log"
	KEY_FILE_PREFIX = "key_list"

	// Number of maps for gorilla to use
	GORILLA_SHARDS = 100
)

type Key struct {
	Key     string
	ShardId int64
}

type TimeValuePair struct {
	Value     float64
	Timestamp int64
}

type DataPoint struct {
	Key   *Key
	Value *TimeValuePair
}

type DataPoints struct {
	Key    *Key
	Values []*TimeValuePair
}

type DataBlock struct {
	Data [DATA_BLOCK_SIZE]byte
}

type TimeSeriesBlock struct {
	Count uint16
	Data  []byte
}

type PutRequest struct {
	Data []*DataPoint
}

type PutResponse struct {
	N int32
}

type GetRequest struct {
	Keys  []*Key
	Begin int64
	End   int64
}

type GetResponse struct {
	Data []*DataPoints
}

func (p *PutRequest) String() string {
	var res string
	res += fmt.Sprintf("--PutRequest--\n")
	for _, dp := range p.Data {
		res += fmt.Sprintf("%4s%20s%10s%3d%8s%5d%8s%15f\n", "key:", dp.Key.Key,
			"shardId:", dp.Key.ShardId, "time:", dp.Value.Timestamp, "value:", dp.Value.Value)
	}
	return res
}

func (g *GetResponse) String() string {
	var res string
	res += fmt.Sprintf("--GetResponse--\n")
	for _, dp := range g.Data {
		res += fmt.Sprintf("key: %s\n", dp.Key.Key)
		for _, v := range dp.Values {
			res += fmt.Sprintf("%10s%5d%8s%10f\n", "timestamp:", v.Timestamp, "value:", v.Value)
		}

	}
	return res
}
