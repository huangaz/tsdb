package dataLog

import (
	"github.com/huangaz/tsdb/lib/fileUtils"
	"github.com/huangaz/tsdb/lib/testUtil"
	"reflect"
	"testing"
)

var (
	prefix        = testUtil.DataPrefix
	dataDirectory = testUtil.DataDirectory_Test
	ids           []uint64
	times         []uint64
	values        []float64
)

func TestAppendAndRead(t *testing.T) {
	testUtil.FileDelete()
	testUtil.FileCreate(1)
	files := fileUtils.NewFileUtils(1, &prefix, &dataDirectory)
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

	var read DataLogReader
	_, err = read.ReadLog(&testFile, 0, f)
	if err != nil {
		t.Fatal(err)
	}

	expectedIds := []uint64{0, 0, 3, 3000000}
	expectedTimes := []uint64{101, 101, 104, 9000}
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

func f(id, time uint64, value float64) (out bool) {
	ids = append(ids, id)
	times = append(times, time)
	values = append(values, value)
	return true
}
