// Tools to manage groups of files of the form
// <path to data>/shardID/prefix.xxxx
package fileUtils

import (
	"errors"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type FileUtils struct {
	directory_ string
	prefix_    string
}

type File struct {
	File *os.File
	Name string
}

// Return a new FileUtils with the given shardId, prefix and dataDirectory
func NewFileUtils(shardId int, prefix, dataDirectory *string) *FileUtils {
	res := new(FileUtils)
	res.directory_ = *dataDirectory
	res.prefix_ = *prefix
	res.directory_ = res.directory_ + "/" + strconv.Itoa(shardId)
	return res
}

// Get the file with the given id
func (f *FileUtils) Open(id int, mode string) (res File, err error) {
	path := f.FilePath(id)

	flag, err := f.Mode_Atoi(mode)
	if err != nil {
		return res, err
	}

	res.File, err = os.OpenFile(path, flag, 0777)
	if err != nil {
		return res, err
	}

	res.Name = path
	return res, nil
}

// Convert from string to int for "mode" in os.OpenFile()
func (f *FileUtils) Mode_Atoi(mode string) (flag int, err error) {
	switch mode {
	case "w":
		flag = os.O_WRONLY
	case "r":
		flag = os.O_RDONLY
	case "a":
		flag = os.O_APPEND
	case "wc":
		flag = os.O_WRONLY | os.O_CREATE
	default:
		err = errors.New("invalid mode!")
	}
	return flag, err
}

// Return the file path as a string with the given id.
func (f *FileUtils) FilePath(id int) string {
	return f.directory_ + "/" + f.prefix_ + "." + strconv.Itoa(id)
}

// Remove all files with id less than targetId
func (f *FileUtils) ClearTo(targetId int) error {
	ids, err := f.Ls()
	if err != nil {
		return err
	}

	for _, id := range ids {
		if id >= targetId {
			return nil
		}
		if err := f.Remove(id); err != nil {
			return err
		}
	}
	return nil
}

// Clear all files.
func (f *FileUtils) ClearAll() error {
	if err := f.ClearTo(math.MaxInt64); err != nil {
		return err
	}
	return nil
}

// Remove a file with the given id.
func (f *FileUtils) Remove(id int) error {
	path := f.FilePath(id)
	err := os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}

// Get the sorted list of valid ids for this prefix
func (f *FileUtils) Ls() (ids []int, err error) {
	fileInfos, err := ioutil.ReadDir(f.directory_)
	if err != nil {
		return ids, err
	}

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		} else {
			fileName := strings.Split(fileInfo.Name(), ".")
			if fileName[0] != f.prefix_ {
				continue
			}
			id, err := strconv.Atoi(fileName[len(fileName)-1])
			if err != nil {
				continue
			}
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids, nil
}

// Rename a file.
func (f *FileUtils) Rename(from, to int) error {
	originalPath := f.FilePath(from)
	newPath := f.FilePath(to)
	err := os.Rename(originalPath, newPath)
	if err != nil {
		return err
	}
	return nil
}

// Creates directories. Must be called before other file operations are used
func (f *FileUtils) CreateDirectories() error {
	err := os.MkdirAll(f.directory_, 0755)
	if err != nil {
		return err
	}
	return nil
}

// Close file.
func (f *FileUtils) Close(fileToClose File) error {
	err := fileToClose.File.Close()
	if err != nil {
		return err
	}
	return nil
}
