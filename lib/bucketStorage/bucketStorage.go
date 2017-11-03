package bucketStorage

import (
	"github.com/huangaz/tsdb/lib/dataBlockReader"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
)

type (
	BucketStorageId uint64
	FetchStatus     int
)

const (
	SUCCESS FetchStatus = iota
	FAILURE
)

type BucketStorage struct {
	numbuckets_      uint8
	newestPosition   int
	data_            *([]BucketData)
	dataBlockReader_ *dataBlockReader.DataBlockReader
	dataFiles_       *fileUtils.FileUtils
	completeFiles_   *fileUtils.FileUtils
}

type BucketData struct {
	pages               []*dataBlock
	activePages         uint32
	lastPageBytesUsed   uint32
	position            uint32
	disable             bool
	finalized           bool
	timeSeriesIds       []uint32
	storageIds          []BucketStorageId
	storageIdsLookupMap map[uint64]uint64
}

type DataBlock struct {
	data [dataTypes.DATA_BLOCK_SIZE]byte
}

func NewBucketData() *BucketData {
	return &BucketData{
		activePages:       0,
		lastPageBytesUsed: 0,
		position:          0,
		disable:           false,
		finalized:         false,
	}
}

func NewBueketStorage(numBuckets uint8, shardId int, dataDiretory string) *BucketStorage {
	res := &BucketStorage{
		numbuckets_:      numBuckets,
		newestPosition:   0,
		data_:            new([]BucketData),
		dataBlockReader_: dataBlockReader.NewDataBlockReader(shardId, dataDiretory),
		dataFiles_:       fileUtils.NewFileUtils(shardId, DATA_PREFIX, dataDiretory),
		completeFiles_:   fileUtils.NewFileUtils(shardId, COMPLETE_PREFIX, dataDiretory),
	}
	res.enable()
	return res
}

func (b *BucketStorage) enable() {
	for i := uint8(0); i < b.numbuckets_; i++ {
		(*b.data_)[i].disable = false
		(*b.data_)[i].activePages = 0
		(*b.data_)[i].lastPageBytesUsed = 0
	}
}

/*
func (b *BucketStorage) Store(position uint32, data *string, dataLength, itemCount, timeSeriesId uint16) {

}

func (b *BucketStorage) fetch(position uint32, id BucketStorageId, data *string, itemCount uint16) FetchStatus {
}

func (b *BucketStorage) loadPosition(position uint32, timeSeriesIds []uint32, storageIds []uint64) bool {

}

func (b *BucketStorage) clearAndDisable() {
}

func (b *BucketStorage) numBuckets() uint8 {
}

func (b *BucketStorage) parseId(id BucketStorageId, pageIndex, pageOffset uint32, dataLength, itemCount uint16) {
}

func (b *BucketStorage) finalizeBucket(position uint32) {
}

func (b *BucketStorage) deleteBucketsOlderThan(position uint32) {
}

func (b *BucketStorage) startMonitoring() {
}

func (b *BucketStorage) createId(pageIndex, pageOffset uint32, dataLength, itemCount uint16) BucketStorageId {
}

func (b *BucketStorage) write(position, activePages uint32, pages [](*DataBlock), timeSeriesIds []uint32, storageIds []BucketStorageId) {
}

func (b *BucketStorage) sanityCheck(bucket uint8, position uint32) bool {
}

*/
