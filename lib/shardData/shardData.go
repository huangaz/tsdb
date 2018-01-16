package shardData

import (
	"github.com/huangaz/tsdb/lib/bucketMap"
)

// shard state
const (
	// success
	SUCCESS = iota

	// retryable error
	ERROR

	// async operation in progress, may succeed or fail
	IN_PROGRESS
)

type ShardData struct {
	data_                 []*bucketMap.BucketMap
	totalShards_          int64
	numShards_            int
	numShardsBeginAdded_  int
	addShardQueue_        chan int64
	readBlocksShardQueue_ chan int64
}

func NewShardData(totalShards, threads int) *ShardData {

}
