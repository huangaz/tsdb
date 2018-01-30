package bucketLogWriter

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/huangaz/tsdb/lib/bucketUtils"
	"github.com/huangaz/tsdb/lib/dataLog"
	"github.com/huangaz/tsdb/lib/fileUtils"
	"github.com/huangaz/tsdb/lib/testUtil"
)

var (
	dataDirectory = testUtil.DataDirectory_Test
	logPrefix     = testUtil.LogPrefix
)

func TestWriteSingleValue(t *testing.T) {

	var shardId int64 = 54
	var windowSize uint64 = 100
	var unixTime int64 = 6480

	testUtil.PathCreate(shardId)

	// path := testUtil.PathCreate(shardId)
	// testUtil.SingleFileCreate(path, unixTime)
	defer testUtil.FileDelete()

	fileUtil := fileUtils.NewFileUtils(shardId, logPrefix, dataDirectory)
	// fileUtil.ClearAll()

	writer := NewBucketLogWriter(windowSize, dataDirectory, 10, 0)
	if writer == nil {
		t.Fatal("Create new bucketLogWriter failed!")
	}

	writer.StartShard(shardId)
	writer.LogData(shardId, 37, unixTime, 38.0)
	writer.StopShard(shardId)
	// 	writer.flushQueue()

	time.Sleep(100 * time.Millisecond)
	err := readSingleValueFromLog(*fileUtil, shardId, 37, unixTime, 38.0, windowSize)
	if err != nil {
		t.Fatal(err)
	}
}

func TestThreadWrite(t *testing.T) {
	var (
		shardId    int64  = 23
		windowSize uint64 = 100
		unixTime          = bucketUtils.FloorTimestamp(5000, windowSize)
		ts1               = unixTime + 1
		ts2               = unixTime + int64(windowSize) - 1
		ts3               = unixTime + int64(windowSize)
		ts4               = unixTime + int64(5*windowSize/2)
	)

	testUtil.PathCreate(shardId)
	defer testUtil.FileDelete()

	fileUtil := fileUtils.NewFileUtils(shardId, logPrefix, dataDirectory)

	writer := NewBucketLogWriter(windowSize, dataDirectory, 10, 0)
	if writer == nil {
		t.Fatal("Create new bucketLogWriter failed!")
	}

	writer.StartShard(shardId)
	writer.LogData(shardId, 37, ts1, 1.0)
	writer.LogData(shardId, 38, ts2, 2.0)
	writer.LogData(shardId, 39, ts3, 3.0)
	writer.LogData(shardId, 40, ts4, 4.0)
	writer.StopShard(shardId)
	// writer.flushQueue()

	time.Sleep(100 * time.Millisecond)

	f, err := fileUtil.Open(int(ts1), "r")
	if err != nil {
		t.Fatal(err)
	}
	defer f.File.Close()
	var ids []uint32
	var unixTimes []int64
	var values []float64
	points, err := dataLog.ReadLog(&f, ts1, func(_id uint32, _time int64, _value float64) bool {
		ids = append(ids, _id)
		unixTimes = append(unixTimes, _time)
		values = append(values, _value)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	expectedIds := []uint32{37, 38}
	expectedTimes := []int64{ts1, ts2}
	expectedValues := []float64{1.0, 2.0}

	if points != 2 || !reflect.DeepEqual(ids, expectedIds) || !reflect.DeepEqual(unixTimes, expectedTimes) || !reflect.DeepEqual(values, expectedValues) {
		t.Fatal("wrong!")
	}

	err = readSingleValueFromLog(*fileUtil, shardId, 39, ts3, 3.0, windowSize)
	if err != nil {
		t.Fatal(err)
	}

	err = readSingleValueFromLog(*fileUtil, shardId, 40, ts4, 4.0, windowSize)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	writer.DeleteBucketLogWriter()
}

func readSingleValueFromLog(fileUtils fileUtils.FileUtils, shardId int64, expectedId uint32,
	expectedUnixTime int64, expectedValue float64, windowSize uint64) error {

	baseTime := expectedUnixTime
	f, err := fileUtils.Open(int(expectedUnixTime), "r")
	if f.File == nil {
		// The file has been opened in advanced with the bucket starting
		// time file name.
		baseTime = bucketUtils.FloorTimestamp(expectedUnixTime, windowSize)
		f, err = fileUtils.Open(int(baseTime), "r")
	}
	if err != nil {
		return err
	}
	defer f.File.Close()

	var id uint32
	var unixTime int64
	var value float64
	points, err := dataLog.ReadLog(&f, baseTime, func(_id uint32, _time int64, _value float64) bool {
		id = _id
		unixTime = _time
		value = _value
		return true
	})

	if err != nil {
		return err
	}
	if points != 1 {
		return fmt.Errorf("numbers of points wrong!")
	}
	if id != expectedId || unixTime != expectedUnixTime || value != expectedValue {
		fmt.Printf("id: %d, expectedId: %d, unixTime: %d, expectedUnixTime: %d, value: %f, expectedValue: %f\n",
			id, expectedId, unixTime, expectedUnixTime, value, expectedValue)
		return fmt.Errorf("data wrong!")
	}
	return nil
}
