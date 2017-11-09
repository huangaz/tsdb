package dataBlockReader

import (
	"encoding/binary"
	"fmt"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"os"
	"strconv"
	"testing"
)

var (
	dataDirectory_Test  = "/tmp/path_test"
	shardDirectory_Test = dataDirectory_Test + "/1"
	d                   = NewDataBlockReader(1, &dataDirectory_Test)
)

func create(numOfFile int) {
	err := os.MkdirAll(shardDirectory_Test, 0777)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 1; i <= numOfFile; i++ {
		dataFile_Test := shardDirectory_Test + "/" + dataTypes.DATA_PRE_FIX + "." + strconv.Itoa(i)
		completeFile_Test := shardDirectory_Test + "/" + dataTypes.COMPLETE_PREFIX + "." + strconv.Itoa(i)
		os.Create(dataFile_Test)
		os.Create(completeFile_Test)
	}
}

func delete() {
	err := os.RemoveAll(dataDirectory_Test)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func TestFindCompletedBlockFiles(t *testing.T) {
	create(10)
	defer delete()
	get, err := d.FindCompletedBlockFiles()
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if len(get) != len(want) {
		t.Error("len(get) != len(want)")
	} else {
		for i := 0; i < len(want); i++ {
			if get[i] != want[i] {
				t.Errorf("want %d, get %d\n", want[i], get[i])
			}
		}
	}
}

func TestReadBlocks(t *testing.T) {
	filePath := shardDirectory_Test + "/" + dataTypes.DATA_PRE_FIX + ".1"
	create(1)
	// defer delete()

	// test for empty file
	_, _, _, err := d.ReadBlocks(1)
	want := "Empty data file" + filePath
	if err == nil || err.Error() != want {
		t.Fatalf("wrong error message for empty file!\n want: %v\n get : %v", want, err.Error())
	}

	//test for too short file
	f, err := d.dataFiles_.Open(1, "w", 0)
	if err != nil {
		t.Fatal(err)
	}

	byteSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(byteSlice, uint32(1))

	f.File.Write(byteSlice)
	_, _, _, err = d.ReadBlocks(1)
	want = "Not enough data" + filePath
	if err == nil || err.Error() != want {
		t.Fatalf("wrong error message for short file!\n want: %v\n get : %v", want, err.Error())
	}

	// test for corrupt data file

	binary.BigEndian.PutUint32(byteSlice, uint32(1))
	f.File.Write(byteSlice)
	_, _, _, err = d.ReadBlocks(1)
	want = "Corrupt data file: expected 65555 bytes, got 8 bytes." + filePath
	if err == nil || err.Error() != want {
		t.Fatalf("wrong error message for corrupt file!\n want: %v\n get : %v", want, err.Error())
	}

	// test for corret file

	binary.BigEndian.PutUint32(byteSlice, uint32(100))
	f.File.Write(byteSlice)
	byteSlice_64 := make([]byte, 8)
	binary.BigEndian.PutUint64(byteSlice_64, uint64(200))
	f.File.Write(byteSlice_64)
	os.Truncate(filePath, 65555)

	pointers, timeSeriesIds, storageIds, err := d.ReadBlocks(1)
	if err != nil {
		t.Fatal(err)
	}
	if timeSeriesIds[0] != 100 {
		t.Errorf("timeSeriesIds[0] want 100, get %d\n", timeSeriesIds[0])
	}
	if storageIds[0] != 200 {
		t.Errorf("storageIds[0] want 200, get %d\n", storageIds[0])
	}
	if pointers == nil {
		t.Error("pointers is nil!")
	}
}
