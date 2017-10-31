package dataBlockReader

import (
	"github.com/huangaz/tsdb/lib/fileUtils"
)

type DataBlockReader struct {
	dataFiles_      fileUtils.FileUtils
	completedFiles_ fileUtils.FileUtils
}

func NewDataBlockReader(shardId int, dataDiretory string) *DataBlockReader {
	return &DataBlockReader{}
}
