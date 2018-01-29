package service

import (
	"fmt"
	"sync"

	"github.com/golang/glog"
	"github.com/huangaz/tsdb/lib/bucketLogWriter"
	"github.com/huangaz/tsdb/lib/bucketMap"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/keyListWriter"
	"github.com/huangaz/tsdb/lib/testUtil"
)

type TsdbService struct {
	sync.RWMutex
	buckets map[int]*bucketMap.BucketMap
	ids     []int64
}

var (
	dataDirectory = testUtil.DataDirectory_Test
)

func NewService() *TsdbService {

}

func (t *TsdbService) Start() (err error) {
	// config adapter not done
	t.ids = make([]int64, 1)
	t.ids[0] = 1

	t.buckets = make(map[int]*bucketMap.BucketMap)

	k := keyListWriter.NewKeyListWriter(dataDirectory, 100)
	b := bucketLogWriter.NewBucketLogWriter(4*3600, dataDirectory, 100, 0)

	for _, shardId := range t.ids {
		// todo
		if err := testUtil.PathCreate(shardId); err != nil {
			return err
		}
		t.buckets[int(shardId)] = bucketMap.NewBucketMap(6, 4*3600, shardId, dataDirectory,
			k, b, bucketMap.UNOWNED)
	}

	// check
	go func() {
		for _, m := range t.buckets {
			if ok := m.SetState(bucketMap.PRE_OWNED); !ok {
				glog.Fatal("set state failed")
			}
			if err := m.ReadKeyList(); err != nil {
				glog.Fatal(err)
			}
			if err := m.ReadData(); err != nil {
				glog.Fatal(err)
			}
		}
		for _, m := range t.buckets {
			more, err := m.ReadBlockFiles()
			if err != nil {
				glog.Fatal(err)
			}

			for more {
				more, err = m.ReadBlockFiles()
				if err != nil {
					glog.Fatal(err)
				}
			}
		}

	}()

	return nil
}

func (t *TsdbService) Put(req *dataTypes.PutRequest) (*dataTypes.PutResponse, error) {
	res := &dataTypes.PutResponse{}
	for _, data := range req.Datas {
		m := t.buckets[int(data.ShardId)]
		if m == nil {
			return res, fmt.Errorf("key not exit")
		}

		newRows, dataPoints, err := m.Put(data.Key, dataTypes.DataPoint{Value: data.Value,
			Timestamp: data.Timestamp}, 0, false)
		if err != nil {
			return res, err
		}

		if newRows == bucketMap.NOT_OWNED && dataPoints == bucketMap.NOT_OWNED {
			return res, fmt.Errorf("key not own!")
		}

		res.N++
	}

	return res, nil
}

func (t *TsdbService) Get(req *dataTypes.GetRequest) (*dataTypes.GetResponse, error) {
	res := &dataTypes.GetResponse{}
	if req.Key == "" {
		return nil, fmt.Errorf("null key!")
	}

	m := t.buckets[int(req.ShardId)]
	if m == nil {
		return nil, fmt.Errorf("key not exit")
	}

	res.Key = req.Key
	state := m.GetState()
	switch state {
	case bucketMap.UNOWNED:
		return nil, fmt.Errorf("Don't own shard %d", req.ShardId)
	case bucketMap.PRE_OWNED, bucketMap.READING_KEYS, bucketMap.READING_KEYS_DONE, bucketMap.READING_LOGS, bucketMap.PROCESSING_QUEUED_DATA_POINTS:
		return nil, fmt.Errorf("Shard %d in progress", req.ShardId)
	default:
		datas, err := m.Get(req.Key, req.Begin, req.End)
		if err != nil {
			return res, err
		}

		for _, dp := range datas {
			res.Dps = append(res.Dps, &dataTypes.DataPoint{Value: dp.Value, Timestamp: dp.Timestamp})
		}

		if state == bucketMap.READING_BLOCK_DATA {
			return res, fmt.Errorf("Shard %d in progress", req.ShardId)
		} else if req.Begin < m.GetReliableDataStartTime() {
			return res, fmt.Errorf("missing too much data")
		}

		return res, nil

	}
}
