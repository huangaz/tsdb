package fileUtils

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

var (
	prefix_Test         = "abc"
	dataDirectory_Test  = "/tmp/path_test"
	shardDirectory_Test = dataDirectory_Test + "/1"
	f                   = NewFileUtils(1, &prefix_Test, &dataDirectory_Test)
)

func create(numOfFile int) {
	err := os.MkdirAll(shardDirectory_Test, 0777)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 1; i <= numOfFile; i++ {
		fileName_Test := shardDirectory_Test + "/" + prefix_Test + "." + strconv.Itoa(i)
		os.Create(fileName_Test)
	}
}

func delete() {
	err := os.RemoveAll(dataDirectory_Test)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func TestLs(t *testing.T) {
	create(10)
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if len(get) != len(want) {
		t.Error(`len(get) != len(want)`)
	} else {
		for i := 0; i < len(want); i++ {
			if get[i] != want[i] {
				t.Errorf("want %d, get %d\n", want[i], get[i])
			}
		}
	}
	delete()
	get, err = f.Ls()
	if err == nil {
		t.Error(`err == nil`)
	}
}

func TestMode_Atoi(t *testing.T) {
	var tests = []struct {
		input string
		want  int
	}{
		{"w", os.O_WRONLY},
		{"r", os.O_RDONLY},
		{"a", os.O_APPEND},
	}
	for _, test := range tests {
		res, _ := f.Mode_Atoi(test.input)
		if res != test.want {
			t.Errorf("Mode_Atoi(%q) = %v\n", test.input, test.want)
		}
	}
	_, err := f.Mode_Atoi("test")
	if err == nil || err.Error() != "invalid mode!" {
		t.Error("err == nil")
	}
}

func TestClearTo(t *testing.T) {
	create(10)
	defer delete()
	err := f.ClearTo(6)
	if err != nil {
		t.Fatal(err)
	}
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	want := []int{6, 7, 8, 9, 10}
	if len(get) != len(want) {
		t.Error(`len(get) != len(want)`)
	} else {
		for i := 0; i < len(want); i++ {
			if get[i] != want[i] {
				t.Errorf("want %d, get %d\n", want[i], get[i])
			}
		}
	}
}

func TestClearAll(t *testing.T) {
	create(10)
	defer delete()
	err := f.ClearAll()
	if err != nil {
		t.Fatal(err)
	}
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	if len(get) != 0 {
		t.Error(`ClearAll() failed!`)
	}
}

func TestRemove(t *testing.T) {
	create(1)
	defer delete()
	err := f.remove(1)
	if err != nil {
		t.Fatal(err)
	}
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	if len(get) != 0 {
		t.Error(`remove() failed!`)
	}
}

func TestRename(t *testing.T) {
	create(2)
	defer delete()
	err := f.Rename(1, 11)
	if err != nil {
		t.Fatal(err)
	}
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	want := []int{2, 11}
	if len(get) != len(want) {
		t.Error(`len(get) != len(want)`)
	} else {
		for i := 0; i < len(want); i++ {
			if get[i] != want[i] {
				t.Errorf("want %d, get %d\n", want[i], get[i])
			}
		}
	}
}

func TestCreateDirectories(t *testing.T) {
	err := f.CreateDirectories()
	if err != nil {
		t.Fatal(err)
	}
	defer delete()
	_, err = os.Stat(shardDirectory_Test)
	if err != nil {
		if os.IsNotExist(err) {
			t.Error(`dataDiretory not exit!`)
		} else {
			t.Fatal(err)
		}
	}
}

func TestOpen(t *testing.T) {
	create(1)
	defer delete()
	res, err := f.Open(1, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close(res)
	if res.File.Name() != f.FilePath(1) {
		t.Errorf("Wrong FilePath! Want %s, get %s\n", f.FilePath(1), res.File.Name())
	}
}
