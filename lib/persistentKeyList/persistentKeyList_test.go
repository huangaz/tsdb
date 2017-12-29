package persistentKeyList

import (
	"github.com/huangaz/tsdb/lib/testUtil"
	"testing"
)

var (
	dataDirectory = testUtil.DataDirectory_Test
)

func TestWriteAndRead(t *testing.T) {
	var shardId int64 = 7
	testUtil.PathCreate(shardId)
	defer testUtil.FileDelete()

	keys := NewPersistentKeyList(shardId, dataDirectory)

	called := false
	_, err := keys.readKeys(shardId, dataDirectory, func(KeyItem) bool {
		called = true
		return true
	})

	if err != nil {
		t.Fatal(err)
	}

	if called == true {
		t.Fatal("wrong")
	}

	data1 := KeyItem{5, "hi", 1}
	data2 := KeyItem{4, "test", 2}
	data3 := KeyItem{7, "bye", 3}

	keys.appendKey(data1)
	keys.appendKey(data2)
	keys.appendKey(data3)
	err = keys.flush(true)
	if err != nil {
		t.Fatal(err)
	}

	var datas []KeyItem

	keys.readKeys(shardId, dataDirectory, func(item KeyItem) bool {
		datas = append(datas, item)
		return true
	})

	if datas[0].id != 5 || datas[0].key != "hi" || datas[0].category != 1 {
		t.Fatal("Wrong data!")
	}

	if datas[1].id != 4 || datas[1].key != "test" || datas[1].category != 2 {
		t.Fatal("Wrong data!")
	}

	if datas[2].id != 7 || datas[2].key != "bye" || datas[2].category != 3 {
		t.Fatal("Wrong data!")
	}

	// Rewrite two keys.
	err = keys.compact(func() []KeyItem {
		var items []KeyItem
		for i := 0; i < 2; i++ {
			item := KeyItem{1, "test2", 15}
			items = append(items, item)
		}
		return items
	})
	if err != nil {
		t.Fatal(err)
	}
	keys.appendKey(KeyItem{8, "test3", 122})
	keys.flush(true)

	datas = datas[:0]
	keys.readKeys(shardId, dataDirectory, func(item KeyItem) bool {
		datas = append(datas, item)
		return true
	})

	if datas[0].id != 1 || datas[0].key != "test2" || datas[0].category != 15 {
		t.Fatal("wrong data!")
	}

	if datas[2].id != 8 || datas[2].key != "test3" || datas[2].category != 122 {
		t.Fatal("wrong data!")
	}

}
