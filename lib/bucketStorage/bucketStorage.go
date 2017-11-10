package bucketStorage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/huangaz/tsdb/lib/dataBlockReader"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
)

const (
	INVALID_ID        uint64 = 0
	PAGE_SIZE         uint32 = dataTypes.DATA_BLOCK_SIZE
	DATA_PREFIX              = dataTypes.DATA_PRE_FIX
	COMPLETE_PREFIX          = dataTypes.COMPLETE_PREFIX
	MAX_ITEM_COUNT           = 32767
	MAX_DATA_LENGTH          = 32767
	MAX_PAGE_COUNT           = 262144
	LARGE_FILE_BUFFER        = 1024 * 1024
)

type BucketStorage struct {
	numbuckets_      uint8
	newestPosition   uint32
	data_            *([]BucketData)
	dataBlockReader_ *dataBlockReader.DataBlockReader
	dataFiles_       *fileUtils.FileUtils
	completeFiles_   *fileUtils.FileUtils
}

type BucketData struct {
	pages               []*dataTypes.DataBlock
	activePages         uint32
	lastPageBytesUsed   uint32
	position            uint32
	disabled            bool
	finalized           bool
	timeSeriesIds       []uint32
	storageIds          []uint64
	storageIdsLookupMap map[uint64]([]uint64)
}

func NewBucketData() *BucketData {
	return &BucketData{
		activePages:       0,
		lastPageBytesUsed: 0,
		position:          0,
		disabled:          false,
		finalized:         false,
	}
}

func NewBueketStorage(numBuckets uint8, shardId int, dataDiretory *string) *BucketStorage {
	data_prefix := DATA_PREFIX
	complete_prefix := COMPLETE_PREFIX
	res := &BucketStorage{
		numbuckets_:      numBuckets,
		newestPosition:   0,
		data_:            new([]BucketData),
		dataBlockReader_: dataBlockReader.NewDataBlockReader(shardId, dataDiretory),
		dataFiles_:       fileUtils.NewFileUtils(shardId, &data_prefix, dataDiretory),
		completeFiles_:   fileUtils.NewFileUtils(shardId, &complete_prefix, dataDiretory),
	}
	res.enable()
	return res
}

// Enables a previously disabled storage.
func (b *BucketStorage) enable() {
	for i := uint8(0); i < b.numbuckets_; i++ {
		(*b.data_)[i].disabled = false
		(*b.data_)[i].activePages = 0
		(*b.data_)[i].lastPageBytesUsed = 0
	}
}

// Stores data.
//
// `position` is the bucket position from the beginning before modulo.
// `data` is the data to be stored.
// `dataLength` is the length of the data in bytes
// `itemCount` is the item count that will be returned when fetching data
//
// Returns an id that can be used to fetch data later, or kInvalidId if data
// could not be stored. This can happen if data is tried to be stored for
// a position that is too old, i.e., more than numBuckets behind the current
// position.

func (b *BucketStorage) Store(position uint32, data *([]byte), dataLength, itemCount uint16, timeSeriesId uint32) (res uint64, err error) {
	if dataLength > MAX_DATA_LENGTH || itemCount > MAX_ITEM_COUNT {
		errString := fmt.Sprintf("Attempted to insert too much data. Length : %d Count : %d", dataLength, itemCount)
		err = errors.New(errString)
		return INVALID_ID, err
	}

	var pageIndex, pageOffset uint32
	bucket := uint8(position % uint32(b.numbuckets_))

	if (*b.data_)[bucket].disabled == true {
		err = errors.New("Data is disabled!")
		return INVALID_ID, err
	}

	if position > b.newestPosition {
		if (*b.data_)[bucket].activePages < uint32(len((*b.data_)[bucket].pages)) {
			(*b.data_)[bucket].pages = (*b.data_)[bucket].pages[:(*b.data_)[bucket].activePages]
		}

		(*b.data_)[bucket].activePages = 0
		(*b.data_)[bucket].lastPageBytesUsed = 0
		(*b.data_)[bucket].position = position
		(*b.data_)[bucket].storageIds = (*b.data_)[bucket].storageIds[:0]
		(*b.data_)[bucket].timeSeriesIds = (*b.data_)[bucket].timeSeriesIds[:0]
		(*b.data_)[bucket].finalized = false
		(*b.data_)[bucket].storageIdsLookupMap = make(map[uint64]([]uint64))
		b.newestPosition = position
	}

	if (*b.data_)[bucket].position != position {
		err = errors.New("Trying to write data to an expired bucket")
		return INVALID_ID, err
	}

	if (*b.data_)[bucket].finalized == true {
		err = errors.New("Trying to write data to a finalized bucket")
		return INVALID_ID, err
	}

	var id uint64 = INVALID_ID
	// hash
	var hash uint64 = 1
	s := (*b.data_)[bucket].storageIdsLookupMap[hash]
	for _, idInSlice := range s {
		index, offset, length, count := b.parseId(idInSlice)
		tmpdata := (*b.data_)[bucket].pages[index].Data[offset : offset+uint32(dataLength)]
		if length == dataLength && count == itemCount && bytes.Compare(*data, tmpdata) == 0 {
			id = idInSlice
			break
		}
	}

	if id == INVALID_ID {
		if (*b.data_)[bucket].activePages == 0 || (*b.data_)[bucket].lastPageBytesUsed+uint32(dataLength) > PAGE_SIZE {
			if (*b.data_)[bucket].activePages == uint32(len((*b.data_)[bucket].pages)) {
				// All allocated pages used, need to allocate more pages.
				if uint32(len((*b.data_)[bucket].pages)) == MAX_PAGE_COUNT {
					err = errors.New("All pages are already in use.")
					return INVALID_ID, err
				}
				newDataBlock := new(dataTypes.DataBlock)
				(*b.data_)[bucket].pages = append((*b.data_)[bucket].pages, newDataBlock)
			}

			// Use the next page.
			(*b.data_)[bucket].activePages++
			(*b.data_)[bucket].lastPageBytesUsed = 0
		}
		pageIndex = (*b.data_)[bucket].activePages - 1
		pageOffset = (*b.data_)[bucket].lastPageBytesUsed
		(*b.data_)[bucket].lastPageBytesUsed += uint32(dataLength)

		tmpdata := (*b.data_)[bucket].pages[pageIndex].Data[pageOffset : pageOffset+uint32(dataLength)]
		err = binary.Read(bytes.NewReader(tmpdata), binary.BigEndian, data)
		if err != nil {
			return INVALID_ID, err
		}

		id = b.createId(pageIndex, pageOffset, dataLength, itemCount)
		(*b.data_)[bucket].storageIdsLookupMap[hash] = append((*b.data_)[bucket].storageIdsLookupMap[hash], id)
	}

	(*b.data_)[bucket].timeSeriesIds = append((*b.data_)[bucket].timeSeriesIds, timeSeriesId)
	(*b.data_)[bucket].storageIds = append((*b.data_)[bucket].storageIds, id)
	return id, nil
}

func (b *BucketStorage) parseId(id uint64) (pageIndex, pageOffset uint32, dataLength, itemCount uint16) {
	pageIndex = uint32(id >> 46)
	pageOffset = uint32((id >> 30)) & (PAGE_SIZE - 1)
	dataLength = uint16((id >> 15) & MAX_DATA_LENGTH)
	itemCount = uint16(id & MAX_ITEM_COUNT)
	return
}

func (b *BucketStorage) createId(pageIndex, pageOffset uint32, dataLength, itemCount uint16) (id uint64) {
	id += uint64(pageIndex << 46)

	id += uint64(pageOffset << 30)

	id += uint64(dataLength << 15)

	id += uint64(itemCount)

	return id
}

// Fetches data.
// fills `data` and `itemCount`,
func (b *BucketStorage) fetch(position uint32, id uint64, data *([]byte)) (itemCount uint16, err error) {
	if id == INVALID_ID {
		err = errors.New("Invalid ID!")
		return itemCount, err
	}

	pageIndex, pageOffset, dataLength, itemCount := b.parseId(id)
	bucket := uint8(position % uint32(b.numbuckets_))

	if pageOffset+uint32(dataLength) > PAGE_SIZE {
		err = errors.New("Corrupt ID!")
		return itemCount, err
	}

	if (*b.data_)[bucket].disabled == true {
		err = errors.New("Data is disabled!")
		return itemCount, err
	}

	if (*b.data_)[bucket].position != position && (*b.data_)[bucket].position != 0 {
		err = errors.New("Tried to fetch data for an expired bucket.")
		return itemCount, err
	}

	if pageIndex < uint32(len((*b.data_)[bucket].pages)) && (*b.data_)[bucket].pages[pageIndex] != nil {
		tmpdata := (*b.data_)[bucket].pages[pageIndex].Data[pageOffset : pageOffset+uint32(dataLength)]
		err = binary.Read(bytes.NewReader(tmpdata), binary.BigEndian, data)
		if err != nil {
			return itemCount, err
		}
		return itemCount, nil
	}

	err = errors.New("error!")
	return itemCount, err
}

// Read all blocks for a given position into memory.
//
// Returns true if the position was successfully read from disk and
// false if it wasn't, due to disk failure or the position being
// expired or disabled. Fills in timeSeriesIds and storageIds with
// the metadata associated with the blocks.
func (b *BucketStorage) loadPosition(position uint32) (timeSeriesIds []uint32, storageIds []uint64, err error) {
	bucket := uint8(position % uint32(b.numbuckets_))

	if err = b.sanityCheck(bucket, position); err != nil {
		return nil, nil, err
	}

	// Ignore buckets that have been completely read from disk or are being
	// actively filled by store().
	if (*b.data_)[bucket].activePages != 0 {
		err = errors.New("Ignore buckets!")
		return nil, nil, err
	}

	blocks, timeSeriesIds, storageIds, err := b.dataBlockReader_.ReadBlocks(uint(position))
	if err != nil {
		return nil, nil, err
	}

	blocksSize := len(blocks)
	if blocksSize == 0 {
		err = errors.New("Block file read failures!")
		return nil, nil, err
	}

	// Grab the lock and do the same sanity checks again.

	(*b.data_)[bucket].pages = make([]*dataTypes.DataBlock, blocksSize)
	(*b.data_)[bucket].activePages = uint32(blocksSize)

	for i := 0; i < blocksSize; i++ {
		(*b.data_)[bucket].pages[i] = blocks[i]
	}

	return timeSeriesIds, storageIds, nil
}

// Verify that the given position is active and not disabled.
// Caller must hold the write lock because this can open a new bucket.
func (b *BucketStorage) sanityCheck(bucket uint8, position uint32) (err error) {
	if (*b.data_)[bucket].disabled == true {
		err = errors.New("Tried to fetch bucket for disabled shard.")
		return err
	}

	if (*b.data_)[bucket].position != position {
		if (*b.data_)[bucket].position == 0 {
			// First time this bucket is used for anything. Mark the
			// position.
			(*b.data_)[bucket].position = position
		} else {
			err = errors.New("Tried to fetch expired bucket")
			return err
		}
	}
	return nil
}

// This clears and disables the buckets for reads and writes.
func (b *BucketStorage) clearAndDisable() {
	for i := uint8(0); i < b.numbuckets_; i++ {
		(*b.data_)[i].disabled = true

		// c++ swap
		(*b.data_)[i].pages = (*b.data_)[i].pages[:0]
		(*b.data_)[i].activePages = 0
		(*b.data_)[i].lastPageBytesUsed = 0
	}
}

func (b *BucketStorage) NumBuckets() uint8 {
	return b.numbuckets_
}

// Finalizes a bucket at the given position. After calling this no
// more data can be stored in this bucket.
func (b *BucketStorage) finalizeBucket(position uint32) (err error) {
	bucket := uint8(position % uint32(b.numbuckets_))

	if (*b.data_)[bucket].disabled == true {
		err = errors.New("Trying to finalize a disabled bucket")
		return err
	}

	if (*b.data_)[bucket].position != position {
		err = errors.New("Trying to finalize an expired bucket")
		return err
	}

	if (*b.data_)[bucket].finalized == true {
		errString := fmt.Sprintf("This bucket has already been finalized %d", position)
		err = errors.New(errString)
		return err
	}

	pages := (*b.data_)[bucket].pages
	timeSeriesIds := (*b.data_)[bucket].timeSeriesIds
	storageIds := (*b.data_)[bucket].storageIds
	activePages := (*b.data_)[bucket].activePages
	(*b.data_)[bucket].timeSeriesIds = (*b.data_)[bucket].timeSeriesIds[:0]
	(*b.data_)[bucket].storageIds = (*b.data_)[bucket].storageIds[:0]
	(*b.data_)[bucket].storageIdsLookupMap = make(map[uint64]([]uint64))
	(*b.data_)[bucket].finalized = true

	if activePages > 0 && len(timeSeriesIds) > 0 {
		b.write(position, activePages, pages, timeSeriesIds, storageIds)
	}
	return nil
}

func (b *BucketStorage) write(position, activePages uint32, pages []*dataTypes.DataBlock, timeSeriesIds []uint32, storageIds []uint64) (err error) {
	if len(timeSeriesIds) != len(storageIds) {
		err = errors.New("length of timeSeriesIds and storageIds are not equal!")
		return err
	}

	// Delete files older than 24h
	err = b.dataFiles_.Remove(int(position - uint32(b.numbuckets_)))
	if err != nil {
		return err
	}
	err = b.completeFiles_.Remove(int(position - uint32(b.numbuckets_)))
	if err != nil {
		return err
	}

	dataFile, err := b.dataFiles_.Open(int(position), "w", LARGE_FILE_BUFFER)
	if err != nil {
		return err
	}
	defer b.dataFiles_.Close(dataFile)

	count := len(timeSeriesIds)
	// count + activePages + timeSeriesIds + storageIds + blocks
	dataLen := 4 + 4 + 4*count + 8*count + int(activePages)*dataTypes.DATA_BLOCK_SIZE
	buffer := make([]byte, dataLen)

	index := 0
	tmpSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(tmpSlice, uint32(count))
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
	index += 4

	binary.BigEndian.PutUint32(tmpSlice, activePages)
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
	index += 4

	tmpSlice = make([]byte, 4*count)
	for i := 0; i < count; i++ {
		tSlice := make([]byte, 4)
		binary.BigEndian.PutUint32(tSlice, timeSeriesIds[i])
		tmpSlice = append(tmpSlice, tSlice...)
	}
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4*count])
	index += 4 * count

	tmpSlice = make([]byte, 8*count)
	for i := 0; i < count; i++ {
		tSlice := make([]byte, 8)
		binary.BigEndian.PutUint64(tSlice, storageIds[i])
		tmpSlice = append(tmpSlice, tSlice...)
	}
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+8*count])
	index += 8 * count

	for i := uint32(0); i < activePages; i++ {
		tmpSlice = (*pages[i]).Data[:]
		binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+dataTypes.DATA_BLOCK_SIZE])
		index += dataTypes.DATA_BLOCK_SIZE
	}

	_, err = dataFile.File.Write(buffer)
	if err != nil {
		return err
	}

	completeFile, err := b.completeFiles_.Open(int(position), "w", 0)
	if err != nil {
		return err
	}
	defer b.completeFiles_.Close(completeFile)

	return nil
}

func (b *BucketStorage) deleteBucketOlderThan(position uint32) (err error) {
	err = b.completeFiles_.ClearTo(int(position))
	if err != nil {
		return err
	}
	err = b.dataFiles_.ClearTo(int(position))
	if err != nil {
		return err
	}
	return nil
}
