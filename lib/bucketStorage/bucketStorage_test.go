package bucketStorage

import (
	"github.com/huangaz/tsdb/lib/testUtil"
	"testing"
)

func TestNewBucketData(t *testing.T) {
	data := NewBucketData()
	if data == nil {
		t.Error("Initial BucketData failed!")
	}
}

func TestNewBucketStorage(t *testing.T) {
	res := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)
	if res == nil {
		t.Error("Initial BueketStorage failed!")
	}
}

func TestEnable(t *testing.T) {
	b := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)
	b.enable()
	d := b.data_[0]

	if d.disabled != false || d.activePages != 0 || d.lastPageBytesUsed != 0 {
		t.Error("enable() failed!")
	}
}

func TestCreateId_ParseId(t *testing.T) {
	b := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)
	var pageIndex uint32 = 123
	var pageOffset uint32 = 456
	var dataLength uint16 = 789
	var itemCount uint16 = 100
	id := b.createId(pageIndex, pageOffset, dataLength, itemCount)
	pageIndex_res, pageOffset_res, dataLength_res, itemCount_res := b.parseId(id)
	if pageIndex != pageIndex_res || pageOffset != pageOffset_res || dataLength != dataLength_res || itemCount != itemCount_res {
		t.Error("createId() or paseId() failed!")
	}
}

func TestStoreAndFetch(t *testing.T) {
	b := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)
	testString := "test"
	testData := []byte(testString)

	// normal store and fetch
	id, err := b.Store(11, testData, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id == INVALID_ID {
		t.Fatal("Invalid id!")
	}

	resData, count, err := b.fetch(11, id)
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatal("wrong count!")
	}
	if testUtil.IsEqualByteSlice(testData, resData) != true {
		t.Fatal("Different between store and fetch!")
	}

	// Dedup data
	id1, err := b.Store(11, testData, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id == INVALID_ID {
		t.Fatal("Invalid id!")
	}
	if id != id1 {
		t.Fatal("Different id between dedup data!")
	}

	id2, err := b.Store(11, testData, 101, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id == INVALID_ID {
		t.Fatal("Invalid id!")
	}
	if id == id2 {
		t.Error("Same id between different data!")
	}
}

func TestTooMuchData(t *testing.T) {
	b := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)

	// count too large
	testString := "test"
	testData := []byte(testString)
	_, err := b.Store(11, testData, 32768, 0)
	if err.Error() != "Attempted to insert too much data. Length : 4 Count : 32768" {
		t.Fatal("Wrong err message when count is too large!")
	}

	// Length too large
	testData = make([]byte, 32768)
	for i := 0; i < 32768; i++ {
		testData[i] = 1
	}
	_, err = b.Store(11, testData, 100, 0)
	if err.Error() != "Attempted to insert too much data. Length : 32768 Count : 100" {
		t.Fatal("Wrong err message when Length is too large!")
	}

}

func TestCleanAndDisable(t *testing.T) {
	b := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)
	b.clearAndDisable()
	testString := "test"
	testData := []byte(testString)
	_, err := b.Store(11, testData, 100, 0)
	if err.Error() != "Data is disabled!" {
		t.Fatal("Wrong err message when data is disabled!")
	}
}

func TestStoreToExpiredBucket(t *testing.T) {
	b := NewBueketStorage(5, 1, &testUtil.DataDirectory_Test)
	testString := "test"
	testData := []byte(testString)

	for i := 1; i < 10; i++ {
		id, err := b.Store(uint32(i), testData, 100, 0)
		if err != nil {
			t.Fatal(err)
		}
		if id == INVALID_ID {
			t.Fatal("Invalid id!")
		}
	}

	// bucket 1 to 4 has been expired
	for i := 1; i < 5; i++ {
		id, err := b.Store(uint32(i), testData, 100, 0)
		if id != INVALID_ID || err.Error() != "Trying to write data to an expired bucket" {
			t.Fatal("Wrong err message when bucket is expired!")
		}
	}
}

func TestFinalizedAndLoad(t *testing.T) {
	b := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)
	testString := "test"
	testData := []byte(testString)
	testUtil.FileCreate(1)
	defer testUtil.FileDelete()

	_, err := b.Store(1, testData, 100, 35)
	if err != nil {
		t.Fatal(err)
	}

	err = b.finalizeBucket(1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = b.Store(1, testData, 100, 35)
	if err == nil || err.Error() != "Trying to write data to a finalized bucket" {
		t.Fatal("Wrong err message when write data to a finalized bucket!")
	}

	b2 := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)

	_, storageIds, err := b2.loadPosition(1)
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range storageIds {
		resData, count, err := b2.fetch(1, id)
		if err != nil {
			t.Fatal(err)
		}
		if count != 100 {
			t.Fatal("wrong count!")
		}
		if testUtil.IsEqualByteSlice(testData, resData) != true {
			t.Fatal("Different between write and load!")
		}
	}
}

func TestDeleteBucketOlderThan(t *testing.T) {
	b := NewBueketStorage(1, 1, &testUtil.DataDirectory_Test)
	for i := 1; i < 10; i++ {
		testUtil.FileCreate(i)
	}
	err := b.deleteBucketOlderThan(5)
	if err != nil {
		t.Fatal(err)
	}
}
