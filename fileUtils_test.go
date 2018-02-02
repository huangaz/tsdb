package tsdb

import (
	"os"
	"testing"
)

var (
	testFileHandle = NewFileUtils(1, DataPrefix, DataDirectory_Test)
)

func TestLs(t *testing.T) {
	FileCreate(10)
	get, err := testFileHandle.Ls()
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
	FileDelete()
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
		res, _ := testFileHandle.mode_Atoi(test.input)
		if res != test.want {
			t.Fatalf("mode_Atoi(%q) = %v\n", test.input, test.want)
		}
	}
	_, err := testFileHandle.mode_Atoi("test")
	if err == nil || err.Error() != "invalid mode!" {
		t.Fatal("Wrong err message!")
	}
}

func TestClearTo(t *testing.T) {
	FileCreate(10)
	defer FileDelete()
	err := testFileHandle.ClearTo(6)
	if err != nil {
		t.Fatal(err)
	}
	get, err := testFileHandle.Ls()
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
	FileCreate(10)
	defer FileDelete()
	err := testFileHandle.ClearAll()
	if err != nil {
		t.Fatal(err)
	}
	get, err := testFileHandle.Ls()
	if err != nil {
		t.Fatal(err)
	}
	if len(get) != 0 {
		t.Fatal(`ClearAll() failed!`)
	}
}

func TestRemove(t *testing.T) {
	FileCreate(1)
	defer FileDelete()
	err := testFileHandle.Remove(1)
	if err != nil {
		t.Fatal(err)
	}
	get, err := testFileHandle.Ls()
	if err != nil {
		t.Fatal(err)
	}
	if len(get) != 0 {
		t.Fatal(`Remove() failed!`)
	}
}

func TestRename(t *testing.T) {
	FileCreate(2)
	defer FileDelete()
	err := testFileHandle.Rename(1, 11)
	if err != nil {
		t.Fatal(err)
	}
	get, err := testFileHandle.Ls()
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
	err := testFileHandle.CreateDirectories()
	if err != nil {
		t.Fatal(err)
	}
	defer FileDelete()
	_, err = os.Stat(DataDirectory_Test + "/1")
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatal(`dataDircetory not exit!`)
		} else {
			t.Fatal(err)
		}
	}
}

func TestOpen(t *testing.T) {
	FileCreate(1)
	defer FileDelete()
	res, err := testFileHandle.Open(1, "r")
	if err != nil {
		t.Fatal(err)
	}
	defer testFileHandle.Close(&res)
	if res.File.Name() != testFileHandle.filePath(1) {
		t.Fatalf("Wrong filePath! Want %s, get %s\n", testFileHandle.filePath(1), res.File.Name())
	}
}
