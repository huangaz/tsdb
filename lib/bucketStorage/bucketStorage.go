// This class stores data for each bucket in 64K blocks. The reason
// for storing data in a single place (or single place for each shard)
// is to avoid the memory overhead and fragmentation that comes from
// allocating millions of ~500 byte blocks.
package bucketStorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/huangaz/tsdb/lib/dataBlockReader"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
)

const (
	// Zero can be used as the invalid ID because no valid ID will ever be zero
	INVALID_ID uint64 = 0

	// Store data in 64K chunks.
	PAGE_SIZE uint32 = dataTypes.DATA_BLOCK_SIZE

	// To fit in 15 bits.
	MAX_ITEM_COUNT  = 32767
	MAX_DATA_LENGTH = 32767

	// To fit in 18 bits.
	MAX_PAGE_COUNT = 262144

	LARGE_FILE_BUFFER = 1024 * 1024
)

var (
	dataPrefix = dataTypes.DATA_PRE_FIX

	// These files are only used as marker files to indicate which
	// blocks have been completed. The files are empty but the file name
	// has the id of the completed block.
	completePrefix = dataTypes.COMPLETE_PREFIX
)

type BucketStorage struct {
	numbuckets_      uint8
	newestPosition_  uint32
	data_            []*BucketData
	dataBlockReader_ *dataBlockReader.DataBlockReader
	dataFiles_       *fileUtils.FileUtils
	completeFiles_   *fileUtils.FileUtils
}

type BucketData struct {
	pages             []*dataTypes.DataBlock
	activePages       uint32
	lastPageBytesUsed uint32
	position          uint32
	disabled          bool
	finalized         bool

	// Two separate vectors for metadata to save memory.
	timeSeriesIds []uint32
	storageIds    []uint64
	// storageIdsLookupMap map[uint64]([]uint64)

	// To control that reads will always work, i.e., allocated pages
	// won't be deleted.
	fetchLock sync.RWMutex

	// To control modifying pages vector and the other data in this struct.
	pagesMutex sync.Mutex
}

// Return a new BucketData.
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

// Return a new BucketStorage with the given bucket number, shardId and dataDirectory.
func NewBueketStorage(numBuckets uint8, shardId int64, dataDirectory string) *BucketStorage {
	res := &BucketStorage{
		numbuckets_:      numBuckets,
		newestPosition_:  0,
		dataBlockReader_: dataBlockReader.NewDataBlockReader(shardId, dataDirectory),
		dataFiles_:       fileUtils.NewFileUtils(shardId, dataPrefix, dataDirectory),
		completeFiles_:   fileUtils.NewFileUtils(shardId, completePrefix, dataDirectory),
	}

	res.data_ = make([]*BucketData, numBuckets)
	for i := 0; i < len(res.data_); i++ {
		res.data_[i] = NewBucketData()
	}

	res.Enable()
	return res
}

// Enables a previously disabled storage.
func (b *BucketStorage) Enable() {
	ptr := b.data_[:]
	for i := uint8(0); i < b.numbuckets_; i++ {
		ptr[i].pagesMutex.Lock()

		ptr[i].disabled = false
		ptr[i].activePages = 0
		ptr[i].lastPageBytesUsed = 0

		ptr[i].pagesMutex.Unlock()
	}
}

// Stores data with given position, itemCount and timeSeriesId.
//
// `position` is the bucket position from the beginning before modulo.
// `data` is the data to be stored.
// `itemCount` is the numbers of data points in data[].
//
// Returns an id that can be used to fetch data later, or kInvalidId if data
// could not be stored. This can happen if data is tried to be stored for a
// position that is too old, i.e., more than numBuckets behind the current position.

func (b *BucketStorage) Store(position uint32, data []byte, itemCount uint16,
	timeSeriesId uint32) (storageId uint64, err error) {

	dataLength := uint16(len(data))
	// insert too much data
	if dataLength > MAX_DATA_LENGTH || itemCount > MAX_ITEM_COUNT {
		return INVALID_ID, fmt.Errorf("Attempted to insert too much data. Length : %d Count : %d",
			dataLength, itemCount)
	}

	var pageIndex, pageOffset uint32
	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	ptr[bucket].pagesMutex.Lock()
	defer ptr[bucket].pagesMutex.Unlock()

	// data is disabled
	if ptr[bucket].disabled == true {
		return INVALID_ID, fmt.Errorf("Data is disabled!")
	}

	// Check if this is the first time this position is seen. If it is,
	// buckets are rotated and an old bucket is now the active one.
	if position > b.newestPosition_ {
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
		// ptr[bucket].storageIdsLookupMap = make(map[uint64]([]uint64))
		b.newestPosition_ = position
	}

	if ptr[bucket].position != position {
		return INVALID_ID, fmt.Errorf("Trying to write data to an expired bucket!")
	}

	if ptr[bucket].finalized == true {
		return INVALID_ID, fmt.Errorf("Trying to write data to a finalized bucket!")
	}

	storageId = INVALID_ID

	/*
		// hash not done yet
		var hash uint64 = 1
		s := ptr[bucket].storageIdsLookupMap[hash]
		for _, idInMap := range s {
			index, offset, length, count := b.parseId(idInMap)
			tmpdata := ptr[bucket].pages[index].Data[offset : offset+uint32(dataLength)]
			// already have this data
			if length == dataLength && count == itemCount && bytes.Compare(data, tmpdata) == 0 {
				// timeseries block dedup size
				storageId = idInMap
				break
			}
		}
	*/

	// New data.
	if storageId == INVALID_ID {
		if ptr[bucket].activePages == 0 || ptr[bucket].lastPageBytesUsed+uint32(dataLength) >
			PAGE_SIZE {
			// All allocated pages used, need to allocate more pages.
			if ptr[bucket].activePages == uint32(len(ptr[bucket].pages)) {
				if uint32(len(ptr[bucket].pages)) == MAX_PAGE_COUNT {
					return INVALID_ID, fmt.Errorf("All pages are already in use!")
				}
				// Create a new DataBlock.
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

		dst := ptr[bucket].pages[pageIndex].Data[pageOffset:ptr[bucket].lastPageBytesUsed]
		err = binary.Read(bytes.NewReader(data), binary.BigEndian, dst)
		// tmpdata := ptr[bucket].pages[pageIndex].Data[pageOffset : pageOffset+uint32(dataLength)]
		// err = binary.Read(bytes.NewReader(tmpdata), binary.BigEndian, data)
		if err != nil {
			return INVALID_ID, err
		}

		storageId = b.createId(pageIndex, pageOffset, dataLength, itemCount)
		// ptr[bucket].storageIdsLookupMap[hash] = append(ptr[bucket].storageIdsLookupMap[hash], storageId)
	}

	ptr[bucket].timeSeriesIds = append(ptr[bucket].timeSeriesIds, timeSeriesId)
	ptr[bucket].storageIds = append(ptr[bucket].storageIds, storageId)
	return storageId, nil
}

// Parse storageId to get pageIndex, pageOffset, dataLength and itemCount.
func (b *BucketStorage) parseId(storageId uint64) (pageIndex, pageOffset uint32, dataLength, itemCount uint16) {
	pageIndex = uint32(storageId >> 46)

	pageOffset = uint32((storageId >> 30)) & (PAGE_SIZE - 1)

	dataLength = uint16((storageId >> 15) & MAX_DATA_LENGTH)

	itemCount = uint16(storageId & MAX_ITEM_COUNT)

	return
}

// Use pageIndex, pageOffset, dataLength, itemCount to create a storageId
// Store all the values in 64 bits
func (b *BucketStorage) createId(pageIndex, pageOffset uint32, dataLength, itemCount uint16) (storageId uint64) {
	// Using the first 18 bits.
	storageId += uint64(pageIndex) << 46

	// The next 16 bits.
	storageId += uint64(pageOffset) << 30

	// The next 15 bits.
	storageId += uint64(dataLength) << 15

	// The last 15 bits.
	storageId += uint64(itemCount)

	return
}

// Fetches data with given position and storageId.
// Return `data[]` and `itemCount`
func (b *BucketStorage) Fetch(position uint32, storageId uint64) (data []byte, itemCount uint16, err error) {
	if storageId == INVALID_ID {
		return nil, 0, fmt.Errorf("Invalid StorageId!")
	}

	pageIndex, pageOffset, dataLength, itemCount := b.parseId(storageId)
	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	if pageOffset+uint32(dataLength) > PAGE_SIZE {
		return nil, 0, fmt.Errorf("Corrupt StorageId!")
	}

	ptr[bucket].fetchLock.RLock()
	defer ptr[bucket].fetchLock.RUnlock()

	if ptr[bucket].disabled == true {
		return nil, 0, fmt.Errorf("Data is disabled!")
	}

	if ptr[bucket].position != position && ptr[bucket].position != 0 {
		return nil, 0, fmt.Errorf("Tried to fetch data for an expired bucket!")
	}

	if pageIndex < uint32(len(ptr[bucket].pages)) && ptr[bucket].pages[pageIndex] != nil {
		tmpdata := ptr[bucket].pages[pageIndex].Data[pageOffset : pageOffset+uint32(dataLength)]
		data = make([]byte, dataLength)
		err = binary.Read(bytes.NewReader(tmpdata), binary.BigEndian, data)
		if err != nil {
			return nil, 0, err
		}
		return data, itemCount, nil
	}

	return nil, 0, fmt.Errorf("Error!")
}

// Read all blocks for a given position into memory from disk.
// Return timeSeriesIds and storageIds with the metadata associated with the blocks.
func (b *BucketStorage) LoadPosition(position uint32) (timeSeriesIds []uint32,
	storageIds []uint64, err error) {

	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	ptr[bucket].fetchLock.Lock()

	if err = b.sanityCheck(bucket, position); err != nil {
		ptr[bucket].fetchLock.Unlock()
		return nil, nil, err
	}

	// Ignore buckets that have been completely read from disk or are
	// being actively filled by store().
	if ptr[bucket].activePages != 0 {
		ptr[bucket].fetchLock.Unlock()
		return nil, nil, fmt.Errorf("Bucket have been completely read or are being filled!")
	}
	ptr[bucket].fetchLock.Unlock()

	blocks, timeSeriesIds, storageIds, err := b.dataBlockReader_.ReadBlocks(position)
	if err != nil {
		return nil, nil, err
	}

	blocksSize := len(blocks)
	if blocksSize == 0 {
		return nil, nil, fmt.Errorf("Block file read failures!")
	}

	ptr[bucket].fetchLock.Lock()
	defer ptr[bucket].fetchLock.Unlock()

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
		return fmt.Errorf("Tried to fetch bucket for disabled shard.")
	}

	if ptr[bucket].position != position {
		if ptr[bucket].position == 0 {
			// First time this bucket is used for anything. Mark the
			// position.
			ptr[bucket].position = position
		} else {
			return fmt.Errorf("Tried to fetch expired bucket.")
		}
	}
	return nil
}

// Clear and disable the buckets for reads and writes.
func (b *BucketStorage) ClearAndDisable() {
	ptr := b.data_[:]
	for i := uint8(0); i < b.numbuckets_; i++ {
		ptr[i].pagesMutex.Lock()

		ptr[i].disabled = true

		// c++ swap
		ptr[i].pages = ptr[i].pages[:0]
		ptr[i].activePages = 0
		ptr[i].lastPageBytesUsed = 0
		// ptr[i].storageIdsLookupMap = make(map[uint64]([]uint64))
		ptr[i].finalized = false

		ptr[i].pagesMutex.Unlock()
	}
}

// Return the number of buckets.
func (b *BucketStorage) NumBuckets() uint8 {
	return b.numbuckets_
}

// Finalizes a bucket at the given position. After calling this no
// more data can be stored in this bucket.
func (b *BucketStorage) FinalizeBucket(position uint32) (err error) {
	bucket := uint8(position % uint32(b.numbuckets_))
	ptr := b.data_[:]

	ptr[bucket].pagesMutex.Lock()

	if ptr[bucket].disabled == true {
		ptr[bucket].pagesMutex.Unlock()
		return fmt.Errorf("Trying to finalize a disabled bucket")
	}

	if ptr[bucket].position != position {
		ptr[bucket].pagesMutex.Unlock()
		return fmt.Errorf("Trying to finalize an expired bucket")
	}

	if ptr[bucket].finalized == true {
		ptr[bucket].pagesMutex.Unlock()
		errString := fmt.Sprintf("This bucket has already been finalized: %d", position)
		return fmt.Errorf(errString)
	}

	pages := ptr[bucket].pages
	timeSeriesIds := ptr[bucket].timeSeriesIds
	storageIds := ptr[bucket].storageIds
	activePages := ptr[bucket].activePages

	ptr[bucket].timeSeriesIds = ptr[bucket].timeSeriesIds[:0]
	ptr[bucket].storageIds = ptr[bucket].storageIds[:0]
	// ptr[bucket].storageIdsLookupMap = make(map[uint64]([]uint64))
	ptr[bucket].finalized = true

	ptr[bucket].pagesMutex.Unlock()

	if activePages > 0 && len(timeSeriesIds) > 0 {
		err := b.write(position, activePages, pages, timeSeriesIds, storageIds)
		if err != nil {
			return err
		}
	}
	return nil
}

// Write data blocks into file on disk.
func (b *BucketStorage) write(position, activePages uint32, pages []*dataTypes.DataBlock,
	timeSeriesIds []uint32, storageIds []uint64) (err error) {

	if len(timeSeriesIds) != len(storageIds) {
		return fmt.Errorf("Length of timeSeriesIds and storageIds don't match!")
	}

	// Delete files older than 24h
	b.dataFiles_.Remove(int(position - uint32(b.numbuckets_)))
	b.completeFiles_.Remove(int(position - uint32(b.numbuckets_)))

	dataFile, err := b.dataFiles_.Open(int(position), "wc")
	if err != nil {
		return err
	}
	defer b.dataFiles_.Close(dataFile)

	count := len(timeSeriesIds)
	// count + activePages + timeSeriesIds + storageIds + blocks
	dataLen := 4 + 4 + 4*count + 8*count + int(activePages)*dataTypes.DATA_BLOCK_SIZE
	buffer := make([]byte, dataLen)
	index := 0

	// Write "count" in buffer
	tmpSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(tmpSlice, uint32(count))
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
	index += 4

	// Write "activePages" in buffer
	binary.BigEndian.PutUint32(tmpSlice, activePages)
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
	index += 4

	// Write "timeSeriesIds[]" in buffer
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint32(tmpSlice, timeSeriesIds[i])
		binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+4])
		index += 4
	}

	// Write "storageIds[]" in buffer
	tmpSlice = make([]byte, 8)
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint64(tmpSlice, storageIds[i])
		binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, buffer[index:index+8])
		index += 8
	}

	// Write "pages[]->Data" in buffer
	for i := uint32(0); i < activePages; i++ {
		tmpSlice = (*pages[i]).Data[:]
		binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian,
			buffer[index:index+dataTypes.DATA_BLOCK_SIZE])
		index += dataTypes.DATA_BLOCK_SIZE
	}

	// Write buffer into file on disk.
	_, err = dataFile.File.Write(buffer)
	if err != nil {
		return err
	}

	completeFile, err := b.completeFiles_.Open(int(position), "wc")
	if err != nil {
		return err
	}
	defer b.completeFiles_.Close(completeFile)

	return nil
}

// Delete buckets older than the given position.
func (b *BucketStorage) DeleteBucketOlderThan(position uint32) (err error) {
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
