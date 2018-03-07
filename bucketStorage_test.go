package tsdb

import (
	"log"
	"testing"
)

func TestBucketStorageNewBucketData(t *testing.T) {
	data := NewBucketData()
	if data == nil {
		t.Error("Initial BucketData failed!")
	}
}

func TestBucketStorageNewBucketStorage(t *testing.T) {
	res := NewBueketStorage(1, 1, DataDirectory_Test)
	if res == nil {
		t.Error("Initial BueketStorage failed!")
	}
}

func TestBucketStorageEnable(t *testing.T) {
	b := NewBueketStorage(1, 1, DataDirectory_Test)
	b.Enable()
	d := b.data_[0]

	if d.disabled != false || d.activePages != 0 || d.lastPageBytesUsed != 0 {
		t.Error("enable() failed!")
	}
}

func TestBucketStorageCreateId_ParseId(t *testing.T) {
	b := NewBueketStorage(1, 1, DataDirectory_Test)
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

func TestBucketStorageStoreAndFetch(t *testing.T) {
	b := NewBueketStorage(5, 1, DataDirectory_Test)
	testString1 := "test"
	testData1 := []byte(testString1)
	testString2 := "text"
	testData2 := []byte(testString2)

	// normal store and fetch
	id1, err := b.Store(10, testData1, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id1 == INVALID_ID {
		t.Fatal("Invalid id!")
	}

	id2, err := b.Store(10, testData2, 200, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id2 == INVALID_ID {
		t.Fatal("Invalid id!")
	}

	resData1, count1, err := b.Fetch(10, id1)
	if err != nil {
		t.Fatal(err)
	}
	if count1 != 100 {
		t.Fatal("Wrong count!")
	}
	if IsEqualByteSlice(testData1, resData1) != true {
		t.Fatal("Different between store and fetch!")
	}

	resData2, count2, err := b.Fetch(10, id2)
	if err != nil {
		t.Fatal(err)
	}
	if count2 != 200 {
		t.Fatal("Wrong count!")
	}
	if IsEqualByteSlice(testData2, resData2) != true {
		t.Fatal("Different between store and fetch!")
	}

	// Dedup data
	id3, err := b.Store(10, testData1, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id3 == INVALID_ID {
		t.Fatal("Invalid id!")
	}
	/*
		if id3 != id1 {
			t.Fatal("Different id between dedup data!")
		}
	*/
}

func TestBucketStorageTooMuchData(t *testing.T) {
	b := NewBueketStorage(1, 1, DataDirectory_Test)

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

func TestBucketStorageCleanAndDisable(t *testing.T) {
	b := NewBueketStorage(1, 1, DataDirectory_Test)
	b.ClearAndDisable()
	testString := "test"
	testData := []byte(testString)
	_, err := b.Store(11, testData, 100, 0)
	if err.Error() != "Data is disabled!" {
		t.Fatal("Wrong err message when data is disabled!")
	}
}

func TestBucketStorageStoreToExpiredBucket(t *testing.T) {
	b := NewBueketStorage(5, 1, DataDirectory_Test)
	testString := "test"
	testData := []byte(testString)

	for i := 1; i < 10; i++ {
		id, err := b.Store(uint32(i), testData, 100+uint16(i), 0)
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
		if id != INVALID_ID || err.Error() != "Trying to write data to an expired bucket!" {
			t.Fatal("Wrong err message when bucket is expired!")
		}
	}
}

func TestBucketStorageFinalizedAndLoad(t *testing.T) {
	b := NewBueketStorage(13, 1, DataDirectory_Test)
	var testPosition uint32 = 18
	testString := "test"
	testData := []byte(testString)
	PathCreate(1)
	// defer FileDelete()

	_, err := b.Store(testPosition, testData, 100, 35)
	if err != nil {
		t.Fatal(err)
	}

	err = b.FinalizeBucket(testPosition)
	if err != nil {
		t.Fatal(err)
	}

	_, err = b.Store(testPosition, testData, 100, 35)
	if err == nil || err.Error() != "Trying to write data to a finalized bucket!" {
		t.Fatal("Wrong err message when write data to a finalized bucket!")
	}

	var testPosition2 uint32 = 19
	testString2 := "test2"
	testData2 := []byte(testString2)

	_, err = b.Store(testPosition2, testData2, 110, 36)
	if err != nil {
		t.Fatal(err)
	}

	err = b.FinalizeBucket(testPosition2)
	if err != nil {
		t.Fatal(err)
	}

	b2 := NewBueketStorage(13, 1, DataDirectory_Test)

	_, storageIds, err := b2.LoadPosition(testPosition)
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range storageIds {
		resData, count, err := b2.Fetch(testPosition, id)
		if err != nil {
			t.Fatal(err)
		}
		if count != 100 {
			t.Fatal("wrong count!")
		}
		if IsEqualByteSlice(testData, resData) != true {
			t.Fatal("Different between write and load!")
		}
	}

	_, storageIds, err = b2.LoadPosition(testPosition2)
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range storageIds {
		resData, count, err := b2.Fetch(testPosition2, id)
		if err != nil {
			t.Fatal(err)
		}
		if count != 110 {
			t.Fatal("wrong count!")
		}
		if IsEqualByteSlice(testData2, resData) != true {
			t.Fatal("Different between write and load!")
		}
	}
}

func TestBucketStorageDeleteBucketOlderThan(t *testing.T) {
	b := NewBueketStorage(1, 1, DataDirectory_Test)
	for i := 1; i < 10; i++ {
		FileCreate(i)
	}
	err := b.DeleteBucketOlderThan(5)
	if err != nil {
		t.Fatal(err)
	}
	FileDelete()
}

func benchmarkBucketStorageStore(b *testing.B) {
	s := NewBueketStorage(5, 1, DataDirectory_Test)
	testString := "test1"
	testData := []byte(testString)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := s.Store(10, testData, 100, 0)
		if err != nil {
			log.Println(err)
		}
	}
}

func benchmarkBucketStorageCreateAndParseId(b *testing.B) {
	s := NewBueketStorage(1, 1, DataDirectory_Test)
	var pageIndex uint32 = 123
	var pageOffset uint32 = 456
	var dataLength uint16 = 789
	var itemCount uint16 = 100
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := s.createId(pageIndex, pageOffset, dataLength, itemCount)
		s.parseId(id)
	}
}

func benchmarkBucketStorageFetch(b *testing.B) {
	s := NewBueketStorage(5, 1, DataDirectory_Test)
	testString := "test1"
	testData := []byte(testString)
	id, _ := s.Store(10, testData, 100, 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := s.Fetch(10, id)
		if err != nil {
			log.Println(err)
		}
	}
}

func BenchmarkBucketStorageFinalize(b *testing.B) {
	PathCreate(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewBueketStorage(13, 1, DataDirectory_Test)
		testString := "test"
		testData := []byte(testString)
		s.Store(18, testData, 100, 35)

		err := s.FinalizeBucket(18)
		if err != nil {
			log.Println(err)
		}
	}
}
