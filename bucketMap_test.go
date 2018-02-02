package tsdb

import (
	"testing"
	"time"
)

const (
	NUM_OF_KEYS = 10
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
	v := &TimeValuePair{
		Value:     100.0,
		Timestamp: m.Timestamp(1),
	}

	for _, s := range testString {
		_, _, err := m.Put(s, v, 0, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	v.Timestamp += 60
	v.Value += 10.0
	for _, s := range testString {
		_, _, err := m.Put(s, v, 0, false)
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
func TestBucketMapTimeSeries(t *testing.T) {
	var shardId int32 = 100
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

func TestBucketMapReload(t *testing.T) {
	var shardId int32 = 4
	var DataDirectory_Test = DataDirectory_Test
	PathCreate(shardId)
	defer FileDelete()

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

	// Now wipe the key_list file and reload the data yet again.
	// We have to give KeyListWriter at least one key to make it replace the file.
	item := KeyItem{0, "a key", 0}
	err := k.Compact(shardId, func() *KeyItem {
		item2 := item
		item.Key = ""
		return &item2
	})
	if err != nil {
		t.Fatal(err)
	}

	/*

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
		var v TimeValuePair
		v.Value = 100.0
		v.Timestamp = m3.Timestamp(2)
		if _, _, err := m3.Put("another key", v, 0, false); err != nil {
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

func TestBucketMapPut(t *testing.T) {
	var shardId int32 = 17
	PathCreate(shardId)
	defer FileDelete()

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
	v := &TimeValuePair{
		Value:     100.0,
		Timestamp: m2.Timestamp(1),
	}

	// UNOWNED
	if res1, res2, err := m2.Put("test string 1", v, 10, false); res1 != NOT_OWNED || res2 != NOT_OWNED || err != nil {
		t.Fatal("Wrong result of Put() when state is UNOWNED")
	}

	// PRE_OWNED
	if err := m2.SetState(PRE_OWNED); err != nil {
		t.Fatal(err)
	}
	if res1, res2, err := m2.Put("test string 2", v, 10, false); res1 != 0 || res2 != 1 || err != nil {
		t.Fatal("Wrong result of Put() when state is PRE_OWNED")
	}

	// read keys
	if err := m2.ReadKeyList(); err != nil {
		t.Fatal(err)
	}
	// queueDataPointWithKey
	if res1, res2, err := m2.Put("test string 3", v, 10, false); res1 != 0 || res2 != 1 || err != nil {
		t.Fatal("Wrong result of Put() when state is READING_KEYS_DONE")
	}

	// read datas
	if err := m2.ReadData(); err != nil {
		t.Fatal(err)
	}
	if res1, res2, err := m2.Put("test string 4", v, 10, false); res1 != 1 || res2 != 1 || err != nil {
		t.Fatal("Wrong result of Put() when state is READING_BLOCK_DATA")
	}

	// read block datas
	for more, _ := m2.ReadBlockFiles(); more; more, _ = m2.ReadBlockFiles() {
	}
	if m2.GetState() != OWNED {
		t.Fatal("state must be OWNED")
	}

	// putDataPointWithId
	v.Timestamp += 60
	if res1, res2, err := m2.Put("test string 4", v, 10, false); res1 != 0 || res2 != 1 || err != nil {
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

func TestBucketMapPutAndGet(t *testing.T) {
	var shardId int32 = 10
	PathCreate(shardId)
	defer FileDelete()

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
		_, _, err := m.Put(string(testData.Key.Key), testData.Value, 0, false)
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

	if len(res) != 4 {
		t.Fatalf("Wrong length of result, want 4, get %d", len(res))
	}

	for i, v := range res {
		if v.Timestamp != testDatas[i].Value.Timestamp || v.Value != testDatas[i].Value.Value {
			t.Fatalf("Wrong result, want (%d,%f), get (%d,%f)", testDatas[i].Value.Timestamp,
				testDatas[i].Value.Value, v.Timestamp, v.Value)
		}
	}
}

func TestBucketMapCompactKeyList(t *testing.T) {
	var shardId int32 = 121
	PathCreate(shardId)
	defer FileDelete()

	k := NewKeyListWriter(DataDirectory_Test, 100)
	b := NewBucketLogWriter(4*SECONDS_PER_HOUR, DataDirectory_Test, 100, 0)
	k.StartShard(shardId)
	b.StartShard(shardId)

	m := NewBucketMap(6, 4*SECONDS_PER_HOUR, shardId, DataDirectory_Test, k, b, OWNED)

	v := &TimeValuePair{
		Value:     100.0,
		Timestamp: m.Timestamp(1),
	}
	key1 := "Key1"

	_, _, err := m.Put(key1, v, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	v.Value += 10.0
	v.Timestamp += 60
	key2 := "Key2"

	_, _, err = m.Put(key2, v, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	var keyItems []*KeyItem
	ReadKeys(shardId, DataDirectory_Test, func(item *KeyItem) bool {
		keyItems = append(keyItems, item)
		return true
	})
	if len(keyItems) != 2 || keyItems[0].Key != "Key1" || keyItems[1].Key != "Key2" {
		t.Fatal("sth wrong with key_list")
	}

	items := m.GetEverything()
	m.Erase(0, items[0])

	m.CompactKeyList()

	var newKeyItems []*KeyItem
	ReadKeys(shardId, DataDirectory_Test, func(item *KeyItem) bool {
		newKeyItems = append(newKeyItems, item)
		return true
	})
	if len(newKeyItems) != 1 || newKeyItems[0].Key != "Key2" {
		t.Fatal("CompactKeyList wrong")
	}
}
