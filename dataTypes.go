package tsdb

import fmt "fmt"

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

	// These files are only used as marker files to indicate which
	// blocks have been completed. The files are empty but the file name
	// has the id of the completed block.
	COMPLETE_PREFIX = "complete_block"
	LOG_FILE_PREFIX = "log"
	KEY_FILE_PREFIX = "key_list"
)

type DataBlock struct {
	Data [DATA_BLOCK_SIZE]byte
}

type TimeSeriesBlock struct {
	Count uint16
	Data  []byte
}

/*
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

*/

func (p *PutRequest) PrintForDebug() string {
	var res string
	res += fmt.Sprintf("--PutRequest--\n")
	for _, dp := range p.Datas {
		res += fmt.Sprintf("%4s%20s%10s%3d%8s%5d%8s%15f\n", "key:", string(dp.Key.Key),
			"shardId:", dp.Key.ShardId, "time:", dp.Value.Timestamp,
			"value:", dp.Value.Value)
	}
	return res
}

func (g *GetResponse) PrintForDebug() string {
	var res string
	res += fmt.Sprintf("--GetResponse--\n")
	for _, dp := range g.Datas {
		res += fmt.Sprintf("key: %s\n", string(dp.Key.Key))
		for _, v := range dp.Values {
			res += fmt.Sprintf("%10s%5d%8s%10f\n", "timestamp:", v.Timestamp,
				"value:", v.Value)
		}

	}
	return res
}
