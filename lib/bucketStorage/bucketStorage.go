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
	INVALID_ID  uint64 = 0
	PAGE_SIZE   uint32 = dataTypes.DATA_BLOCK_SIZE
	DATA_PREFIX        = dataTypes.DATA_PRE_FIX

	// These files are only used as marker files to indicate which
	// blocks have been completed. The files are empty but the file name
	// has the id of the completed block.
	COMPLETE_PREFIX   = dataTypes.COMPLETE_PREFIX
	MAX_ITEM_COUNT    = 32767
	MAX_DATA_LENGTH   = 32767
	MAX_PAGE_COUNT    = 262144
	LARGE_FILE_BUFFER = 1024 * 1024
)

type BucketStorage struct {
	numbuckets_      uint8
	newestPosition   uint32
	data_            []BucketData
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
	res := &BucketData{
		activePages:       0,
		lastPageBytesUsed: 0,
		position:          0,
		disabled:          false,
		finalized:         false,
	}
	return res
}

func NewBueketStorage(numBuckets uint8, shardId int, dataDiretory *string) *BucketStorage {
	data_prefix := DATA_PREFIX
	complete_prefix := COMPLETE_PREFIX
	res := &BucketStorage{
		numbuckets_:      numBuckets,
		newestPosition:   0,
		dataBlockReader_: dataBlockReader.NewDataBlockReader(shardId, dataDiretory),
		dataFiles_:       fileUtils.NewFileUtils(shardId, &data_prefix, dataDiretory),
		completeFiles_:   fileUtils.NewFileUtils(shardId, &complete_prefix, dataDiretory),
	}
	newdata := []BucketData{}
	for i := uint8(0); i < numBuckets; i++ {
		newBucketData := NewBucketData()
		newdata = append(newdata, *newBucketData)
	}
	res.data_ = newdata

	res.enable()
	return res
}

// Enables a previously disabled storage.
func (b *BucketStorage) enable() {
	for i := uint8(0); i < b.numbuckets_; i++ {
		ptr := b.data_[:]
		ptr[i].disabled = false
		ptr[i].activePages = 0
		ptr[i].lastPageBytesUsed = 0
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

func (b *BucketStorage) Store(position uint32, data []byte, itemCount uint16, timeSeriesId uint32) (id uint64, err error) {
	dataLength := uint16(len(data))
	// insert too much data
	if dataLength > MAX_DATA_LENGTH || itemCount > MAX_ITEM_COUNT {
		errString := fmt.Sprintf("Attempted to insert too much data. Length : %d Count : %d", dataLength, itemCount)
		err = errors.New(errString)
		return INVALID_ID, err
	}

	var pageIndex, pageOffset uint32
	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	// data is disabled
	if ptr[bucket].disabled == true {
		err = errors.New("Data is disabled!")
		return INVALID_ID, err
	}

	// Check if this is the first time this position is seen. If it
	// is, buckets are rotated and an old bucket is now the active
	// one.
	if position > b.newestPosition {
		// Only delete memory if the pages were not fully used the previous time around.
		if ptr[bucket].activePages < uint32(len(ptr[bucket].pages)) {
			ptr[bucket].pages = ptr[bucket].pages[:ptr[bucket].activePages]
		}

		ptr[bucket].activePages = 0
		ptr[bucket].lastPageBytesUsed = 0
		ptr[bucket].position = position
		ptr[bucket].storageIds = ptr[bucket].storageIds[:0]
		ptr[bucket].timeSeriesIds = ptr[bucket].timeSeriesIds[:0]
		ptr[bucket].finalized = false
		ptr[bucket].storageIdsLookupMap = make(map[uint64]([]uint64))
		b.newestPosition = position
	}

	if ptr[bucket].position != position {
		err = errors.New("Trying to write data to an expired bucket")
		return INVALID_ID, err
	}

	if ptr[bucket].finalized == true {
		err = errors.New("Trying to write data to a finalized bucket")
		return INVALID_ID, err
	}

	id = INVALID_ID
	// hash not done yet
	var hash uint64 = 1
	s := ptr[bucket].storageIdsLookupMap[hash]
	for _, idInSlice := range s {
		index, offset, length, count := b.parseId(idInSlice)
		tmpdata := ptr[bucket].pages[index].Data[offset : offset+uint32(dataLength)]
		// already have this data
		if length == dataLength && count == itemCount && bytes.Compare(data, tmpdata) == 0 {
			// timeseries block dedup size
			id = idInSlice
			break
		}
	}

	if id == INVALID_ID {
		if ptr[bucket].activePages == 0 || ptr[bucket].lastPageBytesUsed+uint32(dataLength) > PAGE_SIZE {
			// All allocated pages used, need to allocate more pages.
			if ptr[bucket].activePages == uint32(len(ptr[bucket].pages)) {
				if uint32(len(ptr[bucket].pages)) == MAX_PAGE_COUNT {
					err = errors.New("All pages are already in use.")
					return INVALID_ID, err
				}
				newDataBlock := new(dataTypes.DataBlock)
				ptr[bucket].pages = append(ptr[bucket].pages, newDataBlock)
			}

			// Use the next page.
			ptr[bucket].activePages++
			ptr[bucket].lastPageBytesUsed = 0
		}

		pageIndex = ptr[bucket].activePages - 1
		pageOffset = ptr[bucket].lastPageBytesUsed
		ptr[bucket].lastPageBytesUsed += uint32(dataLength)

		dst := ptr[bucket].pages[pageIndex].Data[pageOffset : pageOffset+uint32(dataLength)]
		err = binary.Read(bytes.NewReader(data), binary.BigEndian, dst)
		// tmpdata := ptr[bucket].pages[pageIndex].Data[pageOffset : pageOffset+uint32(dataLength)]
		// err = binary.Read(bytes.NewReader(tmpdata), binary.BigEndian, data)
		if err != nil {
			return INVALID_ID, err
		}

		id = b.createId(pageIndex, pageOffset, dataLength, itemCount)
		ptr[bucket].storageIdsLookupMap[hash] = append(ptr[bucket].storageIdsLookupMap[hash], id)
	}

	ptr[bucket].timeSeriesIds = append(ptr[bucket].timeSeriesIds, timeSeriesId)
	ptr[bucket].storageIds = append(ptr[bucket].storageIds, id)
	return id, nil
}

// Parse Id to get pageIndex, pageOffset, dataLength and itemCount
func (b *BucketStorage) parseId(id uint64) (pageIndex, pageOffset uint32, dataLength, itemCount uint16) {
	pageIndex = uint32(id >> 46)

	pageOffset = uint32((id >> 30)) & (PAGE_SIZE - 1)

	dataLength = uint16((id >> 15) & MAX_DATA_LENGTH)

	itemCount = uint16(id & MAX_ITEM_COUNT)

	return
}

// Use pageIndex, pageOffset, dataLength, itemCount to create an Id
// Store all the values in 64 bits
func (b *BucketStorage) createId(pageIndex, pageOffset uint32, dataLength, itemCount uint16) (id uint64) {
	// Using the first 18 bits.
	id += uint64(pageIndex) << 46

	// The next 16 bits.
	id += uint64(pageOffset) << 30

	// The next 15 bits.
	id += uint64(dataLength) << 15

	// The last 15 bits.
	id += uint64(itemCount)

	return
}

// Fetches data.
// fill `data` and return `itemCount`,
func (b *BucketStorage) Fetch(position uint32, id uint64) (data []byte, itemCount uint16, err error) {
	if id == INVALID_ID {
		err = errors.New("Invalid ID!")
		return nil, itemCount, err
	}

	pageIndex, pageOffset, dataLength, itemCount := b.parseId(id)
	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	if pageOffset+uint32(dataLength) > PAGE_SIZE {
		err = errors.New("Corrupt ID!")
		return nil, itemCount, err
	}

	if ptr[bucket].disabled == true {
		err = errors.New("Data is disabled!")
		return nil, itemCount, err
	}

	if ptr[bucket].position != position && ptr[bucket].position != 0 {
		err = errors.New("Tried to fetch data for an expired bucket.")
		return nil, itemCount, err
	}

	if pageIndex < uint32(len(ptr[bucket].pages)) && ptr[bucket].pages[pageIndex] != nil {
		tmpdata := ptr[bucket].pages[pageIndex].Data[pageOffset : pageOffset+uint32(dataLength)]
		data = make([]byte, dataLength)
		err = binary.Read(bytes.NewReader(tmpdata), binary.BigEndian, data)
		if err != nil {
			return nil, itemCount, err
		}
		return data, itemCount, nil
	}

	err = errors.New("error!")
	return nil, itemCount, err
}

// Read all blocks for a given position into memory.
//
// Returns true if the position was successfully read from disk and
// false if it wasn't, due to disk failure or the position being
// expired or disabled. Fills in timeSeriesIds and storageIds with
// the metadata associated with the blocks.
func (b *BucketStorage) loadPosition(position uint32) (timeSeriesIds []uint32, storageIds []uint64, err error) {
	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	if err = b.sanityCheck(bucket, position); err != nil {
		return nil, nil, err
	}

	// Ignore buckets that have been completely read from disk or are being
	// actively filled by store().
	if ptr[bucket].activePages != 0 {
		err = errors.New("Bucket have been completely read or are being filled!")
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

	ptr[bucket].pages = make([]*dataTypes.DataBlock, blocksSize)
	ptr[bucket].activePages = uint32(blocksSize)

	for i := 0; i < blocksSize; i++ {
		ptr[bucket].pages[i] = blocks[i]
	}

	return timeSeriesIds, storageIds, nil
}

// Verify that the given position is active and not disabled.
// Caller must hold the write lock because this can open a new bucket.
func (b *BucketStorage) sanityCheck(bucket uint8, position uint32) (err error) {
	ptr := b.data_[:]
	if ptr[bucket].disabled == true {
		err = errors.New("Tried to fetch bucket for disabled shard.")
		return err
	}

	if ptr[bucket].position != position {
		if ptr[bucket].position == 0 {
			// First time this bucket is used for anything. Mark the
			// position.
			ptr[bucket].position = position
		} else {
			err = errors.New("Tried to fetch expired bucket.")
			return err
		}
	}
	return nil
}

// This clears and disables the buckets for reads and writes.
func (b *BucketStorage) clearAndDisable() {
	for i := uint8(0); i < b.numbuckets_; i++ {
		ptr := b.data_[:]
		ptr[i].disabled = true

		// c++ swap
		ptr[i].pages = ptr[i].pages[:0]
		ptr[i].activePages = 0
		ptr[i].lastPageBytesUsed = 0
	}
}

func (b *BucketStorage) NumBuckets() uint8 {
	return b.numbuckets_
}

// Finalizes a bucket at the given position. After calling this no
// more data can be stored in this bucket.
func (b *BucketStorage) finalizeBucket(position uint32) (err error) {
	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	if ptr[bucket].disabled == true {
		err = errors.New("Trying to finalize a disabled bucket")
		return err
	}

	if ptr[bucket].position != position {
		err = errors.New("Trying to finalize an expired bucket")
		return err
	}

	if ptr[bucket].finalized == true {
		errString := fmt.Sprintf("This bucket has already been finalized: %d", position)
		err = errors.New(errString)
		return err
	}

	pages := ptr[bucket].pages
	timeSeriesIds := ptr[bucket].timeSeriesIds
	storageIds := ptr[bucket].storageIds
	activePages := ptr[bucket].activePages

	ptr[bucket].timeSeriesIds = ptr[bucket].timeSeriesIds[:0]
	ptr[bucket].storageIds = ptr[bucket].storageIds[:0]
	ptr[bucket].storageIdsLookupMap = make(map[uint64]([]uint64))
	ptr[bucket].finalized = true

	if activePages > 0 && len(timeSeriesIds) > 0 {
		err := b.write(position, activePages, pages, timeSeriesIds, storageIds)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BucketStorage) write(position, activePages uint32, pages []*dataTypes.DataBlock, timeSeriesIds []uint32, storageIds []uint64) (err error) {
	if len(timeSeriesIds) != len(storageIds) {
		err = errors.New("length of timeSeriesIds and storageIds are not equal!")
		return err
	}

	// Delete files older than 24h
	b.dataFiles_.Remove(int(position - uint32(b.numbuckets_)))
	b.completeFiles_.Remove(int(position - uint32(b.numbuckets_)))

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

	// write "count" in buffer
	tmpSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(tmpSlice, uint32(count))
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
	index += 4

	// write "activePages" in buffer
	binary.BigEndian.PutUint32(tmpSlice, activePages)
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
	index += 4

	// write "timeSeriesIds[]" in buffer
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint32(tmpSlice, timeSeriesIds[i])
		binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
		index += 4
	}

	// write "storageIds[]" in buffer
	tmpSlice = make([]byte, 8)
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint64(tmpSlice, storageIds[i])
		binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+8])
		index += 8
	}

	// write "pages[]->Data" in buffer
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
