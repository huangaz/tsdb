package tsdb

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/golang/glog"
)

type TsdbConfig struct {
	sync.RWMutex

	// Values coming in faster than this are considered spam.
	MinTimestampDelta int64

	// Number of shards
	ShardsNum int

	// The size of the qeueue that holds the data points in memory before they
	// can be handled. This queue is only used when shards are being added.
	DataPointQueueSize int

	// Default delta of timestamp
	DefaultDelta int64
}

var (
	TSDBConf TsdbConfig
	// 13*2h
	bucketNum          uint8  = 13
	shardsNum                 = 10
	dataDirectory             = DataDirectory_Test
	keyWriterNum              = 2
	keyWriterQueueSize uint32 = 100
	logWiterNum               = 2
	logWriterQueueSize uint32 = 100
	bucketSize         uint64 = 2 * 3600

	// 15min
	allowTimestampBehind uint32 = 15 * 60

	// 1 min
	allowTimestampAhead uint32 = 60

	// 1 hour
	cleanInterval = time.Hour

	// 1o min
	finalizeInterval = 10 * time.Minute

	adjustTimestamp = false
)

const (
	SERVICE_NAME = "tsdb"
)

func init() {
	TSDBConf.MinTimestampDelta = 3
	TSDBConf.ShardsNum = 100
	TSDBConf.DataPointQueueSize = 1000
	TSDBConf.DefaultDelta = 60
}

func (t *TsdbConfig) SetConfig(conf *TsdbConfig) {
	t.Lock()
	defer t.Unlock()

	t.MinTimestampDelta = conf.MinTimestampDelta
	t.ShardsNum = conf.ShardsNum
	t.DataPointQueueSize = conf.DataPointQueueSize
	t.DefaultDelta = conf.DefaultDelta

}

type TsdbService struct {
	sync.RWMutex
	buckets map[int]*BucketMap
	ids     []int32

	ctx    context.Context
	cancel context.CancelFunc
}

func NewService() *TsdbService {
	return nil
}

func createShardPath(shardId int, dataDirectory string) error {
	return os.MkdirAll(fmt.Sprintf("%s/%d", dataDirectory, shardId), 0755)
}

func (t *TsdbService) Start() (err error) {
	// config adapter not done
	t.ids = make([]int32, 1)
	t.ids[0] = 1

	t.ctx, t.cancel = context.WithCancel(context.Background())

	keyWriters := make([]*KeyListWriter, keyWriterNum)
	for i := 0; i < len(keyWriters); i++ {
		keyWriters[i] = NewKeyListWriter(dataDirectory, keyWriterQueueSize)
	}

	bucketLogWriters := make([]*BucketLogWriter, logWiterNum)
	for i := 0; i < len(bucketLogWriters); i++ {
		bucketLogWriters[i] = NewBucketLogWriter(bucketSize, dataDirectory,
			logWriterQueueSize, allowTimestampBehind)
	}

	t.buckets = make(map[int]*BucketMap)
	for i := 0; i < shardsNum; i++ {
		k := keyWriters[rand.Intn(len(keyWriters))]
		b := bucketLogWriters[rand.Intn(len(bucketLogWriters))]

		if err := createShardPath(i, dataDirectory); err != nil {
			return err
		}

		newMap := NewBucketMap(bucketNum, bucketSize, int32(i),
			dataDirectory, k, b, UNOWNED)

		t.buckets[i] = newMap
	}

	t.reload()

	go t.cleanWorker()
	go t.finalizeBucketWorker()

	return nil

}

func (t *TsdbService) Stop() error {
	t.cancel()
	return nil
}

func (t *TsdbService) Put(req *PutRequest) (*PutResponse, error) {
	t.RLock()
	defer t.RUnlock()

	res := &PutResponse{}
	for _, dp := range req.Data {
		m := t.buckets[int(dp.Key.ShardId)]
		if m == nil {
			return res, fmt.Errorf("key not exit")
		}

		// Adjust 0, late, or early timestamps to now. Disable only for testing.
		now := time.Now().Unix()
		if adjustTimestamp {
			if dp.Value.Timestamp == 0 || dp.Value.Timestamp < now-int64(allowTimestampBehind) ||
				dp.Value.Timestamp > now+int64(allowTimestampAhead) {
				dp.Value.Timestamp = now
			}
		}

		newRows, dataPoints, err := m.Put(string(dp.Key.Key), &TimeValuePair{Value: dp.Value.Value,
			Timestamp: dp.Value.Timestamp}, 0, false)
		if err != nil {
			return res, err
		}

		if newRows == NOT_OWNED && dataPoints == NOT_OWNED {
			return res, fmt.Errorf("key not own!")
		}

		res.N++
	}

	return res, nil
}

func (t *TsdbService) Get(req *GetRequest) (res *GetResponse, err error) {

	res = &GetResponse{Data: make([]*DataPoints, len(req.Keys))}
	for i, key := range req.Keys {
		dps := &DataPoints{Key: key}
		if dps.Values, err = t.getOneKey(key, req.Begin, req.End); err != nil {
			return
		}
		res.Data[i] = dps
	}
	return
}

func (t *TsdbService) getOneKey(key *Key, begin, end int64) (res []*TimeValuePair, err error) {
	t.RLock()
	t.RUnlock()

	m := t.buckets[int(key.ShardId)]
	if m == nil {
		return nil, fmt.Errorf("key not exit")
	}

	state := m.GetState()
	switch state {
	case UNOWNED:
		return nil, fmt.Errorf("Don't own shard %d", key.ShardId)

	case PRE_OWNED, READING_KEYS, READING_KEYS_DONE,
		READING_LOGS, PROCESSING_QUEUED_DATA_POINTS:
		return nil, fmt.Errorf("Shard %d in progress", key.ShardId)
	default:
		datas, err := m.Get(string(key.Key), begin, end)
		if err != nil {
			return res, err
		}

		for _, dp := range datas {
			res = append(res, &TimeValuePair{Value: dp.Value, Timestamp: dp.Timestamp})
		}

		if state == READING_BLOCK_DATA {
			glog.V(1).Infof("Shard %d in process", key.ShardId)
		}

		if begin < m.GetReliableDataStartTime() {
			glog.V(1).Infof("missing too much data")
		}

		return res, nil

	}

}

func (t *TsdbService) reload() error {
	t.RLock()
	defer t.RUnlock()

	// config adapter not done
	ids := []int32{1, 3, 5, 7}

	newMap := make(map[int]bool, len(ids))
	for i := 0; i < len(ids); i++ {
		id := ids[i]
		newMap[int(id)] = true
	}

	var shardToBeAdded []int
	var shardToBeDropped []int

	for k, v := range t.buckets {
		state := v.GetState()
		if _, ok := newMap[k]; ok {
			if state == UNOWNED {
				shardToBeAdded = append(shardToBeAdded, k)
			} else if state == PRE_UNOWNED {
				v.CancelUnowning()
			}
		} else if state == OWNED {
			shardToBeDropped = append(shardToBeDropped, k)
		}
	}

	sort.Ints(shardToBeAdded)
	sort.Ints(shardToBeDropped)

	go t.addShard(shardToBeAdded)
	go t.dropShard(shardToBeDropped)

	return nil
}

func (t *TsdbService) addShard(shards []int) {
	t.RLock()
	t.RUnlock()

	for _, shardId := range shards {
		m := t.buckets[shardId]
		if m == nil {
			glog.Infof("%s Invalid shardId :%d", SERVICE_NAME, shardId)
			continue
		}

		if m.GetState() >= PRE_OWNED {
			continue
		}

		if err := m.SetState(PRE_OWNED); err != nil {
			glog.Infof("%s %v", SERVICE_NAME, err)
			continue
		}

		if err := m.ReadKeyList(); err != nil {
			glog.Infof("%s %v", SERVICE_NAME, err)
			continue
		}

		if err := m.ReadData(); err != nil {
			glog.Infof("%s %v", SERVICE_NAME, err)
			continue
		}
	}

	go func() {
		for _, shardId := range shards {
			m := t.buckets[shardId]
			if m == nil {
				glog.Infof("%s Invalid shardId :%d", SERVICE_NAME, shardId)
				continue
			}

			if m.GetState() != READING_BLOCK_DATA {
				continue
			}

			more, err := m.ReadBlockFiles()
			if err != nil {
				glog.Infof("%s %v", SERVICE_NAME, err)
				continue
			}

			for more {
				more, err = m.ReadBlockFiles()
				if err != nil {
					glog.Infof("%s %v", SERVICE_NAME, err)
					break
				}
			}

		}
	}()
}

func (t *TsdbService) dropShard(shards []int) error {
	t.RLock()
	defer t.RUnlock()

	for _, shardId := range shards {
		m := t.buckets[shardId]
		if m == nil {
			glog.Infof("%s Invalid shardId :%d", SERVICE_NAME, shardId)
			continue
		}

		if m.GetState() != OWNED {
			continue
		}

		if err := m.SetState(PRE_UNOWNED); err != nil {
			glog.Infof("%s %v", SERVICE_NAME, err)
			continue
		}
	}

	return nil
}

func (t *TsdbService) cleanWorker() {
	ticker := time.NewTicker(cleanInterval).C

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker:
			t.RLock()
			defer t.RUnlock()

			for _, v := range t.buckets {
				state := v.GetState()
				if state == OWNED {
					v.CompactKeyList()
					err := v.DeleteOldBlockFiles()
					if err != nil {
						glog.Infof("%s %v", SERVICE_NAME, err)
						continue
					}
				} else if state == PRE_UNOWNED {
					err := v.SetState(UNOWNED)
					if err != nil {
						glog.Infof("%s %v", SERVICE_NAME, err)
						continue
					}
				}
			}
		}
	}
}

func (t *TsdbService) finalizeBucketWorker() {
	ticker := time.NewTicker(finalizeInterval).C

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker:
			timestamp := time.Now().Unix() - int64(allowTimestampAhead+allowTimestampBehind) -
				int64(Duration(1, bucketSize))
			t.finalizeBucket(timestamp)
		}
	}
}

func (t *TsdbService) finalizeBucket(timestamp int64) {
	go func() {
		t.RLock()
		defer t.RUnlock()

		for _, bucket := range t.buckets {
			bucketToFinalize := bucket.Bucket(timestamp)
			_, err := bucket.FinalizeBuckets(bucketToFinalize)
			if err != nil {
				glog.Infof("%s %v", SERVICE_NAME, err)
				continue
			}
		}
	}()
}
