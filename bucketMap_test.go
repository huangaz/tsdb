package tsdb

import (
	"fmt"
	"testing"
	"time"
)

const (
	NUM_OF_KEYS = 1000
)

var (
	testString = make([]string, NUM_OF_KEYS)
)

func createString() {
	for i := range testString {
		testString[i] = RandStr(10)
	}
}

func init() {
	createString()
}

func testMap(m *BucketMap, t *testing.T) {
	var dp TimeValuePair
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
		row := m.GetItem(testString[i])
		if row == nil {
			t.Fatal("the result of GetItem() is nil!")
		}
		out, err := row.S.Get(0, 0, m.GetStorage())
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range out {
			if o.Count != 2 {
				t.Fatal("The number of block is wrong!")
			}
		}
	}
}

/*
func TestTimeSeries(t *testing.T) {
	var shardId int64 = 100
	PathCreate(shardId)
	// defer FileDelete()

	k := NewKeyListWriter(DataDirectory_Test, 100)
	b := NewBucketLogWriter(4*SECONDS_PER_HOUR, DataDirectory_Test, 100, 0)
	k.StartShard(shardId)
	b.StartShard(shardId)

	m := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, OWNED)
	testMap(m, t)
}
*/

func TestReload(t *testing.T) {
	var shardId int32 = 4
	var DataDirectory_Test = DataDirectory_Test
	PathCreate(shardId)
	// defer FileDelete()

	k := NewKeyListWriter(DataDirectory_Test, 100)
	b := NewBucketLogWriter(4*SECONDS_PER_HOUR, DataDirectory_Test, 100, 0)
	b.StartShard(shardId)
	k.StartShard(shardId)

	// Fill, then close the BucketMap.
	m := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, OWNED)
	testMap(m, t)

	// recover datas from disk
	m2 := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, OWNED)
	if err := m2.SetState(PRE_UNOWNED); err != nil {
		t.Fatal(err)
	}
	if err := m2.SetState(UNOWNED); err != nil {
		t.Fatal(err)
	}
	if err := m2.SetState(PRE_OWNED); err != nil {
		t.Fatal(err)
	}

	if err := m2.ReadKeyList(); err != nil {
		t.Fatal(err)
	}
	if err := m2.ReadData(); err != nil {
		t.Fatal(err)
	}
	for more, _ := m2.ReadBlockFiles(); more; more, _ = m2.ReadBlockFiles() {
	}

	if m2.GetState() != OWNED {
		t.Fatal("state isn't OWNED")
	}

	testReads(m2, t)

	/*
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
		m3 := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, OWNED)
		if err := m3.SetState(PRE_UNOWNED); err != nil {
			t.Fatal(err)
		}
		if err := m3.SetState(UNOWNED); err != nil {
			t.Fatal(err)
		}
		if err := m3.ReadKeyList(); err != nil {
			t.Fatal(err)
		}
		if err := m3.ReadData(); err != nil {
			t.Fatal(err)
		}
		// Add a point. This will get assigned an ID that still has block
		// data on disk.
		var dp DataPoint
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
		out, err := m3.GetItem("another key").S.Get(0, uint32(m3.Timestamp(3)), m3.GetStorage())
		if len(out) != 1 {
			t.Fatal("length of output is wrong")
		}
	*/
}

func TestPut(t *testing.T) {
	var shardId int32 = 17
	PathCreate(shardId)
	// defer FileDelete()

	k := NewKeyListWriter(DataDirectory_Test, 100)
	b := NewBucketLogWriter(4*SECONDS_PER_HOUR, DataDirectory_Test, 100, 0)
	k.StartShard(shardId)
	b.StartShard(shardId)

	// Fill, then close the BucketMap.
	m := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, OWNED)
	testMap(m, t)
	if err := m.SetState(PRE_UNOWNED); err != nil {
		t.Fatal(err)
	}
	if err := m.SetState(UNOWNED); err != nil {
		t.Fatal(err)
	}

	m2 := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, UNOWNED)
	var dp TimeValuePair
	dp.Value = 100.0
	dp.Timestamp = m2.Timestamp(1)

	// UNOWNED
	if res1, res2, err := m2.Put("test string 1", dp, 10, false); res1 != NOT_OWNED || res2 != NOT_OWNED || err != nil {
		t.Fatal("Wrong result of Put() when state is UNOWNED")
	}

	// PRE_OWNED
	if err := m2.SetState(PRE_OWNED); err != nil {
		t.Fatal(err)
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

	if m2.GetItem("test string 1") != nil {
		t.Fatal("Wrong result with 'test string 1'!")
	}

	if out, err := m2.GetItem("test string 2").S.Get(0, uint32(m2.Timestamp(3)),
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

func TestBucketPutAndGet(t *testing.T) {
	var shardId int32 = 10
	PathCreate(shardId)
	// defer FileDelete()

	k := NewKeyListWriter(DataDirectory_Test, 100)
	b := NewBucketLogWriter(4*SECONDS_PER_HOUR, DataDirectory_Test, 100, 0)
	k.StartShard(shardId)
	b.StartShard(shardId)

	// Fill, then close the BucketMap.
	m := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, OWNED)

	testDatas := []DataPoint{
		{Key: &Key{Key: []byte("testa"), ShardId: shardId},
			Value: &TimeValuePair{Value: 100.0, Timestamp: 60}},
		{Key: &Key{Key: []byte("testa"), ShardId: shardId},
			Value: &TimeValuePair{Value: 110.0, Timestamp: 118}},
		{Key: &Key{Key: []byte("testa"), ShardId: shardId},
			Value: &TimeValuePair{Value: 170.0, Timestamp: 183}},
		{Key: &Key{Key: []byte("testa"), ShardId: shardId},
			Value: &TimeValuePair{Value: 210.0, Timestamp: 247}},
		{Key: &Key{Key: []byte("testa"), ShardId: shardId},
			Value: &TimeValuePair{Value: 110.0, Timestamp: 300}},
	}

	for _, testData := range testDatas {
		_, _, err := m.Put(string(testData.Key.Key), *testData.Value, 0, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := m.FinalizeBuckets(1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	res, err := m.Get("testa", 60, 280)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(res)
}
