package keyListWriter

import (
	"github.com/huangaz/tsdb/lib/persistentKeyList"
	"github.com/huangaz/tsdb/lib/testUtil"
	"testing"
	"time"
)

var (
	dataDirectory = testUtil.DataDirectory_Test
)

func TestWriteAndRead(t *testing.T) {
	var shardId int64 = 321
	testUtil.PathCreate(shardId)
	defer testUtil.FileDelete()

	keyWriter := NewKeyListWriter(dataDirectory, 10)
	keyWriter.StartShard(shardId)
	keyWriter.AddKey(shardId, 6, "hi", 43)
	keyWriter.StopShard(shardId)
	keyWriter.AddKey(shardId, 7, "bye", 44)
	keyWriter.flushQueue()

	time.Sleep(100 * time.Millisecond)

	var id int32
	var key string
	var category uint16
	keys, err := persistentKeyList.ReadKeys(shardId, dataDirectory,
		func(item persistentKeyList.KeyItem) bool {
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

func TestCompactAndRead(t *testing.T) {
	var shardId int64 = 321
	testUtil.PathCreate(shardId)
	defer testUtil.FileDelete()

	keyWriter := NewKeyListWriter(dataDirectory, 10)
	keyWriter.StartShard(shardId)
	keyWriter.flushQueue()
	time.Sleep(100 * time.Millisecond)

	item := persistentKeyList.KeyItem{7, "hello", 72}
	err := keyWriter.Compact(shardId, func() persistentKeyList.KeyItem {
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

	keys, err := persistentKeyList.ReadKeys(shardId, dataDirectory,
		func(item persistentKeyList.KeyItem) bool {
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
