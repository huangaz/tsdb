package bucketMap

import (
	"fmt"
	"testing"
	"time"

	"github.com/huangaz/tsdb/lib/bucketLogWriter"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/keyListWriter"
	"github.com/huangaz/tsdb/lib/persistentKeyList"
	"github.com/huangaz/tsdb/lib/testUtil"
	"github.com/huangaz/tsdb/lib/timeConstants"
)

const (
	NUM_OF_KEYS = 1000
)

var (
	dataDirectory = testUtil.DataDirectory_Test
	testString    = make([]string, NUM_OF_KEYS)
)

func createString() {
	for i := range testString {
		testString[i] = testUtil.RandStr(10)
	}
}

func init() {
	createString()
}

func testMap(m *BucketMap, t *testing.T) {
	var dp dataTypes.DataPoint
	dp.Value = 100.0
	dp.Timestamp = m.Timestamp(1)

	for _, s := range testString {
		_, _, err := m.Put(s, dp, 0, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	dp.Timestamp += 60
	dp.Value += 10.0
	for _, s := range testString {
		_, _, err := m.Put(s, dp, 0, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := m.FinalizeBuckets(1)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	testReads(m, t)
}

func testReads(m *BucketMap, t *testing.T) {
	for i := 0; i < NUM_OF_KEYS; i++ {
		row := m.Get(testString[i])
		if row == nil {
			t.Fatal("the result of Get() is nil!")
		}
		out, err := row.s.Get(0, 0, m.GetStorage())
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range out {
			if o.Count != 2 {
				t.Fatal("The number of block is wrong!")
			}
		}
	}

	ptrs := m.GetEverything()
	fmt.Printf("Length of ptrs is: %d\n", len(ptrs))
}

/*
func TestTimeSeries(t *testing.T) {
	var shardId int64 = 10
	testUtil.PathCreate(shardId)
	defer testUtil.FileDelete()

	k := keyListWriter.NewKeyListWriter(dataDirectory, 100)
	b := bucketLogWriter.NewBucketLogWriter(4*timeConstants.SECONDS_PER_HOUR, dataDirectory, 100, 0)
	k.StartShard(shardId)
	b.StartShard(shardId)

	m := NewBucketMap(6, 4*timeConstants.SECONDS_PER_HOUR, shardId, dataDirectory, k, b, OWNED)
	testMap(m, t)
}
*/

func TestReload(t *testing.T) {
	var shardId int64 = 10
	testUtil.PathCreate(shardId)
	defer testUtil.FileDelete()

	k := keyListWriter.NewKeyListWriter(dataDirectory, 100)
	b := bucketLogWriter.NewBucketLogWriter(4*timeConstants.SECONDS_PER_HOUR, dataDirectory, 100, 0)
	b.StartShard(shardId)
	k.StartShard(shardId)

	// Fill, then close the BucketMap.
	m := NewBucketMap(6, 4*timeConstants.SECONDS_PER_HOUR, shardId, dataDirectory, k, b, OWNED)
	testMap(m, t)

	m2 := NewBucketMap(6, 4*timeConstants.SECONDS_PER_HOUR, shardId, dataDirectory, k, b, OWNED)
	if success := m2.SetState(PRE_UNOWNED); !success {
		t.Fatal("set state failed!")
	}
	if success := m2.SetState(UNOWNED); !success {
		t.Fatal("set state failed!")
	}
	if success := m2.SetState(PRE_OWNED); !success {
		t.Fatal("set state failed!")
	}

	if err := m2.ReadKeyList(); err != nil {
		t.Fatal(err)
	}
	if err := m2.ReadData(); err != nil {
		t.Fatal(err)
	}
	for more, _ := m2.ReadBlockFiles(); more; more, _ = m2.ReadBlockFiles() {
	}

	testReads(m2, t)

	// Now wipe the key_list file and reload the data yet again.
	// We have to give KeyListWriter at least one key to make it replace the file.
	item := persistentKeyList.KeyItem{0, "a key", 0}
	err := k.Compact(shardId, func() persistentKeyList.KeyItem {
		item2 := item
		item.Key = ""
		return item2
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read it all again.
	// This time, insert a point before reading blocks. This point should not have
	// older data.
	m3 := NewBucketMap(6, 4*timeConstants.SECONDS_PER_HOUR, shardId, dataDirectory, k, b, OWNED)
	if success := m3.SetState(PRE_UNOWNED); !success {
		t.Fatal("set state failed!")
	}
	if success := m3.SetState(UNOWNED); !success {
		t.Fatal("set state failed!")
	}
	if err := m3.ReadKeyList(); err != nil {
		t.Fatal(err)
	}
	if err := m3.ReadData(); err != nil {
		t.Fatal(err)
	}
	// Add a point. This will get assigned an ID that still has block
	// data on disk.
	var dp dataTypes.DataPoint
	dp.Value = 100.0
	dp.Timestamp = m3.Timestamp(2)
	if _, _, err := m3.Put("another key", dp, 0, false); err != nil {
		t.Fatal(err)
	}
	for more, _ := m3.ReadBlockFiles(); more; more, _ = m3.ReadBlockFiles() {
	}

	everything := m3.GetEverything()
	var have int = 0
	for _, thing := range everything {
		if thing != nil {
			have++
		}
	}
	if have != 2 {
		t.Fatal("the number of keys is wrong")
	}
	out, err := m3.Get("another key").s.Get(0, uint32(m3.Timestamp(3)), m3.GetStorage())
	if len(out) != 1 {
		t.Fatal("length of output is wrong")
	}
}

func TestPut(t *testing.T) {
	var shardId int64 = 10
	testUtil.PathCreate(shardId)
	defer testUtil.FileDelete()

	k := keyListWriter.NewKeyListWriter(dataDirectory, 100)
	b := bucketLogWriter.NewBucketLogWriter(4*timeConstants.SECONDS_PER_HOUR, dataDirectory, 100, 0)
	k.StartShard(shardId)
	b.StartShard(shardId)

	// Fill, then close the BucketMap.
	m := NewBucketMap(6, 4*timeConstants.SECONDS_PER_HOUR, shardId, dataDirectory, k, b, OWNED)
	testMap(m, t)
	if success := m.SetState(PRE_UNOWNED); !success {
		t.Fatal("set state failed!")
	}
	if success := m.SetState(UNOWNED); !success {
		t.Fatal("set state failed!")
	}

	m2 := NewBucketMap(6, 4*timeConstants.SECONDS_PER_HOUR, shardId, dataDirectory, k, b, UNOWNED)
	var dp dataTypes.DataPoint
	dp.Value = 100.0
	dp.Timestamp = m2.Timestamp(1)

	// UNOWNED
	if res1, res2, err := m2.Put("test string 1", dp, 10, false); res1 != NOT_OWNED || res2 != NOT_OWNED || err != nil {
		t.Fatal("Wrong result of Put() when state is UNOWNED")
	}

	// PRE_OWNED
	if success := m2.SetState(PRE_OWNED); !success {
		t.Fatal("set state failed")
	}
	if res1, res2, err := m2.Put("test string 2", dp, 10, false); res1 != 0 || res2 != 1 || err != nil {
		t.Fatal("Wrong result of Put() when state is PRE_OWNED")
	}

	// read keys
	if err := m2.ReadKeyList(); err != nil {
		t.Fatal(err)
	}
	// queueDataPointWithKey
	if res1, res2, err := m2.Put("test string 3", dp, 10, false); res1 != 0 || res2 != 1 || err != nil {
		t.Fatal("Wrong result of Put() when state is READING_KEYS_DONE")
	}

	// read datas
	if err := m2.ReadData(); err != nil {
		t.Fatal(err)
	}
	if res1, res2, err := m2.Put("test string 4", dp, 10, false); res1 != 1 || res2 != 1 || err != nil {
		t.Fatal("Wrong result of Put() when state is READING_BLOCK_DATA")
	}

	// read block datas
	for more, _ := m2.ReadBlockFiles(); more; more, _ = m2.ReadBlockFiles() {
	}
	if m2.GetState() != OWNED {
		t.Fatal("state must be OWNED")
	}

	// putDataPointWithId
	dp.Timestamp += 60
	if res1, res2, err := m2.Put("test string 4", dp, 10, false); res1 != 0 || res2 != 1 || err != nil {
		t.Fatal("Wrong result of Put() when state is OWNED")
	}

	if m2.Get("test string 1") != nil {
		t.Fatal("Wrong result with 'test string 1'!")
	}

	if out, err := m2.Get("test string 2").s.Get(0, uint32(m2.Timestamp(3)),
		m2.GetStorage()); len(out) != 1 || err != nil {
		t.Fatal("Wrong result with 'test string 2'!")
	}

	// test GetSome()
	if items, more := m2.GetSome(len(m2.rows_)+2, 1); items != nil || more != false {
		t.Fatal("Wrong result when offset is too large!")
	}
	if items, more := m2.GetSome(len(m2.rows_)-2, 4); len(items) != 2 || more != false {
		t.Fatal("Wrong result when offset+count>=len(b.rows_)")
	}
	if items, more := m2.GetSome(len(m2.rows_)-10, 4); len(items) != 4 || more != true {
		t.Fatal("Wrong result for GetSome()!")
	}
}
