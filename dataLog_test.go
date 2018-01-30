package tsdb

import (
	"reflect"
	"testing"
)

var (
	prefix = DataPrefix
	ids    []uint32
	times  []int64
	values []float64
)

func TestDataLogAppendAndRead(t *testing.T) {
	dataDirectory := DataDirectory_Test

	FileDelete()
	FileCreate(1)
	files := NewFileUtils(1, prefix, dataDirectory)
	testFile, err := files.Open(1, "w")
	if err != nil {
		t.Fatal(err)
	}

	writer := NewDataLogWriter(&testFile, 0)
	// medium delta
	err = writer.Append(0, 101, 2)
	if err != nil {
		t.Fatal(err)
	}
	// zero delta
	err = writer.Append(0, 101, 2)
	if err != nil {
		t.Fatal(err)
	}
	// short delta
	err = writer.Append(3, 104, 5)
	if err != nil {
		t.Fatal(err)
	}
	// long id and large delta
	err = writer.Append(3000000, 9000, 0)
	if err != nil {
		t.Fatal(err)
	}

	// flush and close file
	writer.DeleteDataLogWriter()

	testFile, err = files.Open(1, "r")
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close(testFile)

	_, err = ReadLog(&testFile, 0, readLogHandle)
	if err != nil {
		t.Fatal(err)
	}

	expectedIds := []uint32{0, 0, 3, 3000000}
	expectedTimes := []int64{101, 101, 104, 9000}
	expectedValues := []float64{2.0, 2.0, 5.0, 0.0}

	if reflect.DeepEqual(expectedIds, ids) != true {
		t.Fatal("error!")
	}
	if reflect.DeepEqual(expectedTimes, times) != true {
		t.Fatal("error!")
	}
	if reflect.DeepEqual(expectedValues, values) != true {
		t.Fatal("error!")
	}
}

func readLogHandle(id uint32, time int64, value float64) (out bool) {
	ids = append(ids, id)
	times = append(times, time)
	values = append(values, value)
	return true
}
