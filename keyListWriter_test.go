package tsdb

import (
	"testing"
	"time"
)

func TestKeyWriteAndRead(t *testing.T) {
	var shardId int64 = 321
	PathCreate(shardId)
	defer FileDelete()

	keyWriter := NewKeyListWriter(DataDirectory_Test, 10)
	keyWriter.StartShard(shardId)
	keyWriter.AddKey(shardId, 6, "hi", 43)
	keyWriter.StopShard(shardId)
	keyWriter.AddKey(shardId, 7, "bye", 44)
	keyWriter.flushQueue()

	time.Sleep(100 * time.Millisecond)

	var id int32
	var key string
	var category uint16
	keys, err := ReadKeys(shardId, DataDirectory_Test,
		func(item KeyItem) bool {
			id = item.Id
			key = item.Key
			category = item.Category
			return true
		})
	if err != nil {
		t.Fatal(err)
	}

	if keys != 1 || id != 6 || key != "hi" || category != 43 {
		t.Fatal("Wrong data!")
	}
}

func TestKeyCompactAndRead(t *testing.T) {
	var shardId int64 = 321
	PathCreate(shardId)
	defer FileDelete()

	keyWriter := NewKeyListWriter(DataDirectory_Test, 10)
	keyWriter.StartShard(shardId)
	keyWriter.flushQueue()
	time.Sleep(100 * time.Millisecond)

	item := KeyItem{7, "hello", 72}
	err := keyWriter.Compact(shardId, func() KeyItem {
		item2 := item
		item.Key = ""
		return item2
	})
	if err != nil {
		t.Fatal(err)
	}

	var id int32
	var key string
	var category uint16

	keys, err := ReadKeys(shardId, DataDirectory_Test,
		func(item KeyItem) bool {
			id = item.Id
			key = item.Key
			category = item.Category
			return true
		})
	if err != nil {
		t.Fatal(err)
	}

	if keys != 1 || id != 7 || key != "hello" || category != 72 {
		t.Fatal("Wrong data!")
	}

}
