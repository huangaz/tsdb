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

type Data struct {
	Key     string
	ShardId int64
	DataPoint
}

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

type PutRequest struct {
	Datas []*Data
}

type PutResponse struct {
	N int32
}

type GetRequest struct {
	Begin   int64
	End     int64
	Key     string
	ShardId int64
}

type GetResponse struct {
	Key string
	Dps []*DataPoint
}

func (p *PutRequest) String() string {
	var res string
	res += fmt.Sprintf("--PutRequest--\n")
	for _, data := range p.Datas {
		res += fmt.Sprintf("%4s%20s%10s%3d%8s%5d%8s%15f\n", "key:", data.Key,
			"shardId:", data.ShardId, "time:", data.Timestamp, "value:", data.Value)
	}
	return res
}

func (g *GetResponse) String() string {
	var res string
	res += fmt.Sprintf("--GetResponse--\n")
	res += fmt.Sprintf("key: %s\n", g.Key)
	for _, dp := range g.Dps {
		res += fmt.Sprintf("%10s%5d%8s%10f\n", "timestamp:", dp.Timestamp, "value:", dp.Value)
	}
	return res
}
