// Returns allocated blocks for every page in the given position.
// Fills in timeSeriesIds and storageIds with the metadata associated with
// the blocks.
package dataBlockReader

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
	"io/ioutil"
)

type DataBlockReader struct {
	dataFiles_      *fileUtils.FileUtils
	completedFiles_ *fileUtils.FileUtils
}

func NewDataBlockReader(shardId int, dataDiretory *string) *DataBlockReader {
	res := new(DataBlockReader)
	dataPrefix := dataTypes.DATA_PRE_FIX
	res.dataFiles_ = fileUtils.NewFileUtils(shardId, &dataPrefix, dataDiretory)
	completePrefix := dataTypes.COMPLETE_PREFIX
	res.completedFiles_ = fileUtils.NewFileUtils(shardId, &completePrefix, dataDiretory)
	return res
}

// Returns allocated blocks for every page in the given position.
// Fills in timeSeriesIds and storageIds with the metadata associated with
// the blocks.
func (d *DataBlockReader) ReadBlocks(position uint) (pointers [](*dataTypes.DataBlock), timeSeriesIds []uint32, storageIds []uint64, err error) {
	f, err := d.dataFiles_.Open(int(position), "r", 0)
	if err != nil {
		return pointers, nil, nil, err
	}
	defer f.File.Close()

	buffer, err := ioutil.ReadAll(f.File)
	if err != nil {
		return pointers, nil, nil, err
	}
	len := len(buffer)
	if len == 0 {
		err = errors.New("Empty data file" + f.Name)
		return pointers, nil, nil, err
	} else if len < 8 {
		// the length of 2 uint32
		err = errors.New("Not enough data" + f.Name)
		return pointers, nil, nil, err
	}

	count := binary.BigEndian.Uint32(buffer[:4])
	buffer = buffer[4:]
	activePages := binary.BigEndian.Uint32(buffer[:4])
	buffer = buffer[4:]

	expectedLength := int(4 + 4 + count*4 + count*8 + activePages*dataTypes.PAGE_SIZE)
	if len != expectedLength {
		errString := fmt.Sprintf("Corrupt data file: expected %d bytes, got %d bytes.", expectedLength, len)
		err = errors.New(errString + f.Name)
		return pointers, nil, nil, err
	}

	err = binary.Read(bytes.NewReader(buffer[:count*4+1]), binary.BigEndian, timeSeriesIds)
	if err != nil {
		return nil, nil, nil, err
	}
	buffer = buffer[count*4+1:]
	err = binary.Read(bytes.NewReader(buffer[:count*4+1]), binary.BigEndian, storageIds)
	if err != nil {
		return nil, nil, nil, err
	}
	buffer = buffer[count*4+1:]

	newDataBlock := new(dataTypes.DataBlock)
	for i := uint32(0); i < activePages; i++ {
		err = binary.Read(bytes.NewReader(buffer[:dataTypes.PAGE_SIZE+1]), binary.BigEndian, newDataBlock.Data)
		if err != nil {
			return nil, nil, nil, err
		}
		buffer = buffer[dataTypes.PAGE_SIZE+1:]
		pointers = append(pointers, newDataBlock)
	}

	return pointers, timeSeriesIds, storageIds, nil
}

// Returns the file ids for the completed blocks
func (d *DataBlockReader) FindCompletedBlockFiles() ([]int, error) {
	files, err := d.completedFiles_.Ls()
	return files, err
}
