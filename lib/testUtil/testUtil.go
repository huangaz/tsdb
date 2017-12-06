package testUtil

import (
	"fmt"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"os"
	"strconv"
)

var (
	DataPrefix          = dataTypes.DATA_PRE_FIX
	CompletePrefix      = dataTypes.COMPLETE_PREFIX
	DataDirectory_Test  = "/tmp/path_test"
	ShardDirectory_Test = DataDirectory_Test + "/1"
)

func FileCreate(numOfFile int) {
	err := os.MkdirAll(ShardDirectory_Test, 0777)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 1; i <= numOfFile; i++ {
		dataFile_Test := ShardDirectory_Test + "/" + DataPrefix + "." + strconv.Itoa(i)
		completeFile_Test := ShardDirectory_Test + "/" + CompletePrefix + "." + strconv.Itoa(i)
		os.Create(dataFile_Test)
		os.Create(completeFile_Test)
	}
}

func FileDelete() {
	err := os.RemoveAll(DataDirectory_Test)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func IsEqualByteSlice(s1 []byte, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}
