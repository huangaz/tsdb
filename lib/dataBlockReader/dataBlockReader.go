// Returns allocated blocks for every page in the given position.
// Fills in timeSeriesIds and storageIds with the metadata associated with
// the blocks.
package dataBlockReader

import (
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
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

/*
func (d *DataBlockReader) ReadBlocks(position uint32, timeSeriesIds []uint32, storageIds []uint64) [](*dataTypes.DataBlock) {

}
*/

// Returns the file ids for the completed blocks
func (d *DataBlockReader) findCompletedBlockFiles() ([]int, error) {
	files, err := d.completedFiles_.Ls()
	return files, err
}
