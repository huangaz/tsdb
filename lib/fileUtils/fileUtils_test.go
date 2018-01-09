package fileUtils

import (
	"github.com/huangaz/tsdb/lib/testUtil"
	"os"
	"testing"
)

var (
	prefix        = testUtil.DataPrefix
	dataDirectory = testUtil.DataDirectory_Test
	f             = NewFileUtils(1, prefix, dataDirectory)
)

func TestLs(t *testing.T) {
	testUtil.FileCreate(10)
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if len(get) != len(want) {
		t.Fatal(`len(get) != len(want)`)
	} else {
		for i := 0; i < len(want); i++ {
			if get[i] != want[i] {
				t.Fatalf("want %d, get %d\n", want[i], get[i])
			}
		}
	}
	testUtil.FileDelete()
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
			t.Fatalf("Mode_Atoi(%q) = %v\n", test.input, test.want)
		}
	}
	_, err := f.Mode_Atoi("test")
	if err == nil || err.Error() != "invalid mode!" {
		t.Fatal("Wrong err message!")
	}
}

func TestClearTo(t *testing.T) {
	testUtil.FileCreate(10)
	defer testUtil.FileDelete()
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
				t.Fatalf("want %d, get %d\n", want[i], get[i])
			}
		}
	}
}

func TestClearAll(t *testing.T) {
	testUtil.FileCreate(10)
	defer testUtil.FileDelete()
	err := f.ClearAll()
	if err != nil {
		t.Fatal(err)
	}
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	if len(get) != 0 {
		t.Fatal(`ClearAll() failed!`)
	}
}

func TestRemove(t *testing.T) {
	testUtil.FileCreate(1)
	defer testUtil.FileDelete()
	err := f.Remove(1)
	if err != nil {
		t.Fatal(err)
	}
	get, err := f.Ls()
	if err != nil {
		t.Fatal(err)
	}
	if len(get) != 0 {
		t.Fatal(`Remove() failed!`)
	}
}

func TestRename(t *testing.T) {
	testUtil.FileCreate(2)
	defer testUtil.FileDelete()
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
		t.Fatal(`len(get) != len(want)`)
	} else {
		for i := 0; i < len(want); i++ {
			if get[i] != want[i] {
				t.Fatalf("want %d, get %d\n", want[i], get[i])
			}
		}
	}
}

func TestCreateDirectories(t *testing.T) {
	err := f.CreateDirectories()
	if err != nil {
		t.Fatal(err)
	}
	defer testUtil.FileDelete()
	_, err = os.Stat(dataDirectory + "/1")
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatal(`dataDircetory not exit!`)
		} else {
			t.Fatal(err)
		}
	}
}

func TestOpen(t *testing.T) {
	testUtil.FileCreate(1)
	defer testUtil.FileDelete()
	res, err := f.Open(1, "r")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close(res)
	if res.File.Name() != f.FilePath(1) {
		t.Fatalf("Wrong FilePath! Want %s, get %s\n", f.FilePath(1), res.File.Name())
	}
}
