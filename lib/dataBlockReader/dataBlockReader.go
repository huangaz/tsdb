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
func (d *DataBlockReader) ReadBlocks(position uint) (pointers [](*dataTypes.DataBlock),
	timeSeriesIds []uint32, storageIds []uint64, err error) {

	f, err := d.dataFiles_.Open(int(position), "r")
	if err != nil {
		return pointers, nil, nil, err
	}
	defer f.File.Close()

	buffer, err := ioutil.ReadAll(f.File)
	if err != nil {
		return pointers, nil, nil, err
	}
	length := len(buffer)
	if length == 0 {
		err = errors.New("Empty data file: " + f.Name)
		return pointers, nil, nil, err
	} else if length < 8 {
		// the length of 2 uint32
		err = errors.New("Not enough data: " + f.Name)
		return pointers, nil, nil, err
	}

	count := binary.BigEndian.Uint32(buffer[:4])
	buffer = buffer[4:]
	activePages := binary.BigEndian.Uint32(buffer[:4])
	buffer = buffer[4:]

	lengthOfTimeSeriesIds := 4 * count
	lengthOfStorageIds := 8 * count

	// uint32 + uint32 + count*uint32 + count * uint64 + activePages * PAGE_SIZE
	expectedLength := int(4 + 4 + lengthOfTimeSeriesIds + lengthOfStorageIds +
		activePages*dataTypes.PAGE_SIZE)
	if length != expectedLength {
		errString := fmt.Sprintf("Corrupt data file: expected %d bytes, got %d bytes. ",
			expectedLength, length)
		err = errors.New(errString + f.Name)
		return pointers, nil, nil, err
	}

	timeSeriesIds = make([]uint32, count)
	err = binary.Read(bytes.NewReader(buffer[:lengthOfTimeSeriesIds]), binary.BigEndian,
		timeSeriesIds)
	if err != nil {
		return nil, nil, nil, err
	}
	buffer = buffer[lengthOfTimeSeriesIds:]

	storageIds = make([]uint64, count)
	err = binary.Read(bytes.NewReader(buffer[:lengthOfStorageIds]), binary.BigEndian, storageIds)
	if err != nil {
		return nil, nil, nil, err
	}
	buffer = buffer[lengthOfStorageIds:]

	DataBlockBuffer := [dataTypes.PAGE_SIZE]byte{}
	for i := uint32(0); i < activePages; i++ {
		err = binary.Read(bytes.NewReader(buffer[:dataTypes.PAGE_SIZE]), binary.BigEndian,
			&DataBlockBuffer)
		if err != nil {
			return nil, nil, nil, err
		}
		buffer = buffer[dataTypes.PAGE_SIZE:]
		newDataBlock := &dataTypes.DataBlock{Data: DataBlockBuffer}
		pointers = append(pointers, newDataBlock)
	}

	return pointers, timeSeriesIds, storageIds, nil
}

// Returns the file ids for the completed blocks
func (d *DataBlockReader) FindCompletedBlockFiles() ([]int, error) {
	files, err := d.completedFiles_.Ls()
	return files, err
}
