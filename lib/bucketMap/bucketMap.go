// This class handles all the time series in a shard. It loads shard
// information when the shard is added. It also keeps a track of the
// state of the shard.
package bucketMap

import (
	"fmt"
	"github.com/huangaz/tsdb/lib/bucketLogWriter"
	"github.com/huangaz/tsdb/lib/bucketStorage"
	"github.com/huangaz/tsdb/lib/bucketUtils"
	"github.com/huangaz/tsdb/lib/bucketedTimeSeries"
	"github.com/huangaz/tsdb/lib/dataBlockReader"
	"github.com/huangaz/tsdb/lib/dataLog"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
	"github.com/huangaz/tsdb/lib/keyListWriter"
	"github.com/huangaz/tsdb/lib/persistentKeyList"
	"github.com/huangaz/tsdb/lib/timer"
	"github.com/huangaz/tsdb/lib/utils/priorityQueue"
	"log"
	"sync"
	"time"
)

const (
	// The order here matters. It's only possible to go to a bigger
	// state and from OWNED to PRE_UNOWNED.

	// About to be unowned. No resources have been released yet. Can
	// be owned again by just calling `cancelUnowning` function.
	PRE_UNOWNED = iota

	// Unowned. No resources allocated. To own the shard, it must be
	// moved to PRE_OWNED state.
	UNOWNED

	// Pre-owned. Resources allocated and reading keys and logs can
	// start. If no keys/data need to be read, it can be moved to
	// OWNED state after this state directly.
	PRE_OWNED

	// Currently reading keys.
	READING_KEYS

	// Reading keys is done and logs can be read next.
	READING_KEYS_DONE

	// Currenly reading logs.
	READING_LOGS

	// Processing queued data points.
	PROCESSING_QUEUED_DATA_POINTS

	// Reading block files.
	READING_BLOCK_DATA

	// Everything is read and owned.
	OWNED
)

const (
	NOT_OWNED = -1

	MAX_ALLOWED_KEY_LENGTH    = 400
	MAX_ALLOWED_TIMESERIES_ID = 10000000
	// When performing initial insertion, add this much buffer to the vector
	// on each resize.
	ROWS_AT_A_TIME = 10000

	// The size of the qeueue that holds the data points in memory before they
	// can be handled. This queue is only used when shards are being added.
	DATA_POINT_QUEUE_SIZE = 1000

	// Count gaps longer than this as holes in the log files.
	MISSING_LOGS_THRESHOLD_SECS = 600
)

type BucketMap struct {
	n_                     uint8
	windowSize_            uint64
	reliableDataStartTime_ int64

	rwLock_ sync.RWMutex
	map_    map[string]uint32

	// Always equal to rows_.size()
	tableSize_ uint32
	rows_      [](*Item)

	// priority_queue
	freeList_ *priorityQueue.PriorityQueue

	storage_       *bucketStorage.BucketStorage
	state_         int
	shardId_       int64
	dataDirectory_ string

	keyWriter_ *keyListWriter.KeyListWriter
	logWriter_ *bucketLogWriter.BucketLogWriter
	addTimer_  timer.Timer

	mutex_ sync.Mutex

	// stateChangeMutex_ sync.Mutex

	QueueDataPoint

	dataPointQueue_      chan QueueDataPoint
	lastFinalizedBucket_ uint32

	// unreadBlockFilesMutex_ sync.Mutex

	// set
	unreadBlockFiles_ []int

	// Rows from key_list files. Used during shard loading and cleared after.
	rowsFromDisk_ [](*Item)

	// Circular vector for the deviations.
	// deviations_ []([]uint32)
}

type QueueDataPoint struct {
	timeSeriesId uint32

	unixTime int64

	// Empty string will indicate that timeSeriesId is used.
	key      string
	value    float64
	category uint16
}

type Item struct {
	key string
	s   *bucketedTimeSeries.BucketedTimeSeries
}

func NewBucketMap(buckets uint8, windowSize uint64, shardId int64, dataDirectory string,
	keyWriter *keyListWriter.KeyListWriter, logWriter *bucketLogWriter.BucketLogWriter,
	state int) *BucketMap {

	res := &BucketMap{
		n_:                     buckets,
		windowSize_:            windowSize,
		reliableDataStartTime_: 0,
		map_:                 make(map[string]uint32),
		tableSize_:           0,
		storage_:             bucketStorage.NewBueketStorage(buckets, shardId, dataDirectory),
		state_:               state,
		shardId_:             shardId,
		dataDirectory_:       dataDirectory,
		keyWriter_:           keyWriter,
		logWriter_:           logWriter,
		lastFinalizedBucket_: 0,

		freeList_:       priorityQueue.NewPriorityQueue(),
		dataPointQueue_: make(chan QueueDataPoint, DATA_POINT_QUEUE_SIZE),
	}
	return res
}

func NewItem(key string) *Item {
	res := &Item{
		key: key,
		s:   bucketedTimeSeries.NewBucketedTimeSeries(),
	}
	return res
}

// Insert the given data point, creating a new row if necessary.
// Returns the number of new rows created (0 or 1) and the number of
// data points successfully inserted (0 or 1).
// Returns {kNotOwned,kNotOwned} if this map is currenly not owned.
func (b *BucketMap) Put(key string, value dataTypes.DataPoint, category uint16,
	skipStateCheck bool) (newRows, dataPoints int, err error) {

	state := b.getState()
	existingItem, id := b.getInternal(key)

	// State check can only skipped when processing data points from the queue.
	// Data points that come in externally during processing will still be queued.
	if skipStateCheck {
		if state != PROCESSING_QUEUED_DATA_POINTS {
			return 0, 0, fmt.Errorf("State check can only skipped when processing data points!")
		}
	} else {
		switch state {
		case UNOWNED:
			return NOT_OWNED, NOT_OWNED, nil
		case PRE_OWNED:
			fallthrough
		case READING_KEYS:
			b.queueDataPointWithKey(key, value, category)
			// Assume the data point will be added and no new keys will be
			// added. This might not be the case but these return values
			// are only used for counters.
			return 0, 1, nil
		case READING_KEYS_DONE:
			fallthrough
		case READING_LOGS:
			fallthrough
		case PROCESSING_QUEUED_DATA_POINTS:
			if existingItem != nil {
				b.queueDataPointWithId(id, value, category)
			} else {
				b.queueDataPointWithKey(key, value, category)
			}
			return 0, 1, nil
		case READING_BLOCK_DATA:
			fallthrough
		case OWNED:
			fallthrough
		case PRE_UNOWNED:
			// Continue normal processing. PRE_UNOWNED is still completely
			// considered to be owned.
			break
		default:
			return 0, 0, fmt.Errorf("Invalid state!")
		}
	}

	if existingItem != nil {
		// Directly put the data point to the timeSeries instead of queueing it.
		if added := b.putDataPointWithId(existingItem.s, id, value, category); added {
			return 0, 1, nil
		} else {
			return 0, 0, nil
		}
	}

	bucketNum := b.bucket(value.Timestamp)

	// Prepare a row now to minimize critical section.
	newRow := NewItem(key)
	newRow.s.Reset(b.n_)
	newRow.s.Put(bucketNum, 0, value, b.storage_, &category)

	var index int = 0

	// Lock the map
	b.rwLock_.Lock()
	defer b.rwLock_.Unlock()

	// Nothing was inserted, just update the existing one.
	if timeSeriesId, ok := b.map_[key]; ok {
		if added := b.putDataPointWithId(b.rows_[timeSeriesId].s, timeSeriesId, value,
			category); added {
			return 0, 1, nil
		} else {
			return 0, 0, nil
		}
	}

	// Find a row in the vector.
	if b.freeList_.Len() > 0 {
		// pop from the priority queue
		index = b.freeList_.Pop()
		b.rows_[index] = newRow
	} else {
		// add a new row
		b.tableSize_++
		b.rows_ = append(b.rows_, newRow)
		index = len(b.rows_) - 1
	}
	// Add a new item into the map.
	b.map_[key] = uint32(index)

	// Write the new key out to disk.
	b.keyWriter_.AddKey(b.shardId_, int32(index), key, category)
	b.logWriter_.LogData(b.shardId_, int32(index), value.Timestamp, value.Value)

	return 1, 1, nil
}

func stateString(state int) string {
	var res string
	switch state {
	case PRE_UNOWNED:
		res = "PRE_UNOWNED"
	case UNOWNED:
		res = "UNOWNED"
	case PRE_OWNED:
		res = "PRE_OWNED"
	case READING_KEYS:
		res = "READING_KEYS"
	case READING_KEYS_DONE:
		res = "READING_KEYS_DONE"
	case READING_LOGS:
		res = "READING_LOGS"
	case PROCESSING_QUEUED_DATA_POINTS:
		res = "PROCESSING_QUEUED_DATA_POINTS"
	case READING_BLOCK_DATA:
		res = "READING_BLOCK_DATA"
	case OWNED:
		res = "OWNED"
	default:
		res = ""
	}
	return res
}

// Returns a ptr to the item if found.
// Return `id` if item is found.
func (b *BucketMap) getInternal(key string) (*Item, uint32) {
	b.rwLock_.RLock()
	defer b.rwLock_.RUnlock()

	// Either the state is UNOWNED or keys are being read. In both
	// cases do not try to find the key.
	if b.state_ >= UNOWNED && b.state_ <= READING_KEYS {
		return nil, 0
	}

	if id, ok := b.map_[key]; ok {
		return b.rows_[id], id
	}

	return nil, 0
}

func (b *BucketMap) queueDataPointWithKey(key string, value dataTypes.DataPoint, category uint16) {
	if key == "" {
		log.Println("Not queueing with empty key")
		return
	}

	var dp QueueDataPoint
	dp.key = key
	dp.unixTime = value.Timestamp
	dp.value = value.Value
	dp.category = category

	b.queueDataPoint(dp)
}

func (b *BucketMap) queueDataPointWithId(id uint32, value dataTypes.DataPoint, category uint16) {
	var dp QueueDataPoint

	// Leave key string empty to indicate that timeSeriesId is used.
	dp.timeSeriesId = id
	dp.unixTime = value.Timestamp
	dp.value = value.Value
	dp.category = category

	b.queueDataPoint(dp)
}

func (b *BucketMap) putDataPointWithId(timeSeries *bucketedTimeSeries.BucketedTimeSeries,
	timeSeriesId uint32, value dataTypes.DataPoint, category uint16) bool {

	bucketNum := b.bucket(value.Timestamp)

	err := timeSeries.Put(bucketNum, timeSeriesId, value, b.storage_, &category)
	if err != nil {
		log.Println(err)
		return false
	}

	b.logWriter_.LogData(b.shardId_, int32(timeSeriesId), value.Timestamp, value.Value)
	return true
}

func (b *BucketMap) queueDataPoint(dp QueueDataPoint) {
	b.dataPointQueue_ <- dp
	b.reliableDataStartTime_ = time.Now().Unix()
}

// Get a ptr of a timeSeries.
func (b *BucketMap) Get(key string) *Item {
	item, _ := b.getInternal(key)
	return item
}

// Get all the TimeSeries.
func (b *BucketMap) getEverything() []*Item {
	b.rwLock_.RLock()
	defer b.rwLock_.RUnlock()
	res := make([]*Item, len(b.rows_))
	copy(res, b.rows_)
	return res
}

// Get some of the TimeSeries. Returns true if there is more data left.
func (b *BucketMap) getSome(offset, count int) ([]*Item, bool) {
	b.rwLock_.RLock()
	defer b.rwLock_.RUnlock()

	if offset >= len(b.rows_) {
		return nil, false
	} else if offset+count >= len(b.rows_) {
		res := make([]*Item, len(b.rows_)-offset)
		copy(res, b.rows_[offset:])
		return res, false
	} else {
		res := make([]*Item, count)
		copy(res, b.rows_[offset:offset+count])
		return res, true
	}
}

func (b *BucketMap) erase(index uint32, item *Item) {
	b.rwLock_.Lock()
	defer b.rwLock_.Unlock()

	if item == nil || b.rows_[index] != item {
		return
	}

	id, ok := b.map_[item.key]
	if ok && id == index {
		delete(b.map_, item.key)
	}

	b.rows_[index] = nil
	b.freeList_.Push(int(index))
}

func (b *BucketMap) bucket(unixTime int64) uint32 {
	return bucketUtils.Bucket(unixTime, b.windowSize_, b.shardId_)
}

func (b *BucketMap) timestamp(bucket uint32) int64 {
	return bucketUtils.Timestamp(bucket, b.windowSize_, b.shardId_)
}

func (b *BucketMap) duration(buckets uint32) uint64 {
	return bucketUtils.Duration(buckets, b.windowSize_)
}

func (b *BucketMap) buckets(duration uint64) uint32 {
	return bucketUtils.Buckets(duration, b.windowSize_)
}

func (b *BucketMap) getStorage() *bucketStorage.BucketStorage {
	return b.storage_
}

func (b *BucketMap) compactKeyList() {
	items := b.getEverything()

	b.keyWriter_.Compact(b.shardId_, func() persistentKeyList.KeyItem {
		for i, item := range items {
			if item != nil {
				keyItem := persistentKeyList.KeyItem{int32(i), item.key, item.s.GetCategory()}
				return keyItem
			}
		}
		return persistentKeyList.KeyItem{0, "", 0}
	})
}

func (b *BucketMap) deleteOldBlockFiles() error {
	// Start far enough back that we can't possibly interfere with anything.
	err := b.storage_.DeleteBucketOlderThan(b.bucket(time.Now().Unix()) - uint32(b.n_) - 1)
	if err != nil {
		return err
	}
	return nil
}

// Reads the key list. This function should be called after moving to PRE_OWNED state.
func (b *BucketMap) readKeyList() error {
	// Timer := timer.NewTimer(true)

	if success := b.setState(READING_KEYS); !success {
		return fmt.Errorf("Setting state failed!")
	}

	persistentKeyList.ReadKeys(b.shardId_, b.dataDirectory_, func(item persistentKeyList.KeyItem) bool {

		if len(item.Key) >= MAX_ALLOWED_KEY_LENGTH {
			log.Printf("Key is too long. Key file is corrupt for shard %d", b.shardId_)

			// Don't continue reading from this file anymore.
			return false
		}

		if item.Id > MAX_ALLOWED_TIMESERIES_ID {
			log.Printf("Id is too large. Key file is corrupt for shard %d", b.shardId_)

			// Don't continue reading from this file anymore.
			return false
		}

		if item.Id >= int32(len(b.rows_)) {
			newRows := make([]*Item, int(item.Id)+ROWS_AT_A_TIME-len(b.rows_))
			b.rows_ = append(b.rows_, newRows...)
		}

		newItem := NewItem(item.Key)
		newItem.s.Reset(b.n_)
		newItem.s.SetCategory(item.Category)
		b.rows_[item.Id] = newItem

		return true
	})

	b.tableSize_ = uint32(len(b.rows_))

	// Put all the rows in either the map or the free list.
	for i, it := range b.rows_ {
		if it != nil {
			if _, ok := b.map_[it.key]; ok {
				// Ignore keys that already exist.
				b.rows_[i] = nil
				b.freeList_.Push(i)
			} else {
				b.map_[it.key] = uint32(i)
			}
		} else {
			b.freeList_.Push(i)
		}
	}

	b.rowsFromDisk_ = b.getEverything()
	if success := b.setState(READING_KEYS_DONE); !success {
		return fmt.Errorf("Setting state failed!")
	}

	return nil
}

// Sets the state. Returns true if state was set, false if the state
// transition is not allowed or already in that state.
func (b *BucketMap) setState(state int) bool {
	if state < PRE_UNOWNED || state > OWNED {
		return false
	}

	b.rwLock_.Lock()

	if !isAllowedStateTransition(b.state_, state) {
		log.Printf("Illegal transition of state from %s to %s", stateString(b.state_),
			stateString(b.state_))
		return false
	}

	switch state {
	case PRE_OWNED:
		b.addTimer_.Start()
		b.keyWriter_.StartShard(b.shardId_)
		b.logWriter_.StartShard(b.shardId_)
		// b.dataPointQueue_ = make(chan QueueDataPoint, DATA_POINT_QUEUE_SIZE)
	case UNOWNED:
		b.map_ = make(map[string]uint32)
		b.freeList_ = priorityQueue.NewPriorityQueue()
		b.rows_ = b.rows_[:0]
		b.tableSize_ = 0

		// These operations do block, but only to enqueue flags, not drain the
		// queues to disk.
		b.keyWriter_.StopShard(b.shardId_)
		b.logWriter_.StopShard(b.shardId_)
	case OWNED:
		b.rowsFromDisk_ = b.rowsFromDisk_[:0]

		// Calling this won't hurt even if the timer isn't running.
		b.addTimer_.Stop()
	}

	// oldState := b.state_
	b.state_ = state
	b.rwLock_.Unlock()

	// Enable/disable storage outside the lock because it might take a
	// while and the the storage object has its own locking.
	if state == PRE_OWNED {
		b.storage_.Enable()
	} else if state == UNOWNED {
		b.storage_.ClearAndDisable()
	}

	/*
		log.Printf("Change state of shard %d from %s to %s. ", b.shardId_, stateString(oldState),
			stateString(state))
	*/
	return true
}

func isAllowedStateTransition(from, to int) bool {
	return to > from || (from == OWNED && to == PRE_UNOWNED)
}

// Raads the data. The function should be called after calling
// readKeyList.
func (b *BucketMap) readData() (err error) {
	if success := b.setState(READING_LOGS); !success {
		return fmt.Errorf("Setting state failed!")
	}

	reader := dataBlockReader.NewDataBlockReader(b.shardId_, b.dataDirectory_)

	// find unread block files.
	b.mutex_.Lock()

	b.unreadBlockFiles_, err = reader.FindCompletedBlockFiles()
	if err != nil {
		b.mutex_.Unlock()
		return err
	}
	if l := len(b.unreadBlockFiles_); l > 0 {
		b.checkForMissingBlockFiles()
		b.lastFinalizedBucket_ = uint32(b.unreadBlockFiles_[l-1])
	}

	b.mutex_.Unlock()

	b.readLogFiles(b.lastFinalizedBucket_)
	if b.getState() != READING_LOGS {
		return fmt.Errorf("Must be state: %s", stateString(READING_LOGS))
	}

	if success := b.setState(PROCESSING_QUEUED_DATA_POINTS); !success {
		return fmt.Errorf("Setting state failed!")
	}

	// Skip state check when processing queued data points.
	b.processQueueDataPoints(true)

	// There's a tiny chance that incoming data points will think that
	// the state is PROCESSING_QUEUED_DATA_POINTS and they will be
	// queued after the second call to processQueuedDataPoints.
	if success := b.setState(READING_BLOCK_DATA); !success {
		return fmt.Errorf("Setting state failed!")
	}

	// Process queued data points again, just to be sure that the queue
	// is empty because it is possible that something was inserted into
	// the queue after it was emptied and before the state was set to
	// READING_BLOCK_DATA.
	b.processQueueDataPoints(false)

	return nil
}

func (b *BucketMap) checkForMissingBlockFiles() {
	// Just look for holes in the progression of files.
	// Gaps between log and block files will be checked elsewhere.
	var missingFiles int = 0
	for i := 0; i < len(b.unreadBlockFiles_)-1; i++ {
		if b.unreadBlockFiles_[i]+1 != b.unreadBlockFiles_[i+1] {
			missingFiles++
		}
	}

	if missingFiles > 0 {
		now := b.bucket(time.Now().Unix())
		var errString string
		errString = fmt.Sprintf("%d completed block files are missing. Got blocks:\n", missingFiles)
		errString += fmt.Sprintln(b.unreadBlockFiles_)
		errString += fmt.Sprintf("Expected blocks in range [ %d, %d ] for shard %d",
			now-uint32(b.n_), now-1, b.shardId_)
		log.Println(errString)
		b.reliableDataStartTime_ = time.Now().Unix()
	}
}

// Load all the datapoints out of the logfiles for this shard that
// are newer than what is covered by the lastBlock.
func (b *BucketMap) readLogFiles(lastBlock uint32) error {
	files := fileUtils.NewFileUtils(b.shardId_, dataTypes.LOG_FILE_PREFIX, b.dataDirectory_)
	var unknownKeys uint32 = 0
	lastTimestamp := b.timestamp(lastBlock + 1)

	ids, err := files.Ls()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if int64(id) < b.timestamp(lastBlock+1) {
			// log.Printf("Skipping log file %d because it's already covered by a block", id)
			continue
		}

		file, err := files.Open(id, "rc")
		if err != nil || file.File == nil {
			log.Println("Could not open logfile for reading!")
			continue
		}
		defer file.File.Close()

		bucketNum := b.bucket(int64(id))

		dataLog.ReadLog(&file, int64(id), func(index uint32, unixTime int64, value float64) bool {

			if unixTime < b.timestamp(bucketNum) || unixTime > b.timestamp(bucketNum+1) {
				log.Printf("Unix time is out of the expected range: %d [%d,%d]",
					unixTime, b.timestamp(bucketNum), b.timestamp(bucketNum+1))

				// It's better to stop reading this log file here because
				// none of the data can be trusted after this.
				return false
			}

			b.rwLock_.RLock()
			defer b.rwLock_.RUnlock()

			if index < uint32(len(b.rows_)) && b.rows_[index] != nil {
				var dp dataTypes.DataPoint
				dp.Timestamp = unixTime
				dp.Value = value
				b.rows_[index].s.Put(b.bucket(unixTime), index, dp, b.storage_, nil)
			} else {
				unknownKeys++
			}

			gap := unixTime - lastTimestamp
			if gap > MISSING_LOGS_THRESHOLD_SECS && lastTimestamp > b.timestamp(1) {
				// log.Printf("%d seconds of missing logs from %d to %d for shard %d",
				// gap, lastTimestamp, unixTime, b.shardId_)
				b.reliableDataStartTime_ = unixTime
			}

			if unixTime > lastTimestamp {
				lastTimestamp = unixTime
			}

			return true
		})
	}

	now := time.Now().Unix()
	gap := now - lastTimestamp
	if gap > MISSING_LOGS_THRESHOLD_SECS && lastTimestamp > b.timestamp(1) {
		// log.Printf("%d seconds of missing logs from %d to now(%d) for shard %d",
		// 	gap, lastTimestamp, now, b.shardId_)
		b.reliableDataStartTime_ = now
	}
	return nil
}

func (b *BucketMap) getState() int {
	b.rwLock_.RLock()
	defer b.rwLock_.RUnlock()
	return b.state_
}

func (b *BucketMap) processQueueDataPoints(skipStateCheck bool) {
	var dps []QueueDataPoint

	if len(b.dataPointQueue_) == 0 {
		return
	}

	for dp := range b.dataPointQueue_ {
		dps = append(dps, dp)
		if len(b.dataPointQueue_) == 0 {
			break
		}
	}

	for _, dp := range dps {
		var value dataTypes.DataPoint
		value.Timestamp = dp.unixTime
		value.Value = dp.value

		if dp.key == "" {
			// Time series id is known. It's possbible to take a few
			// shortcuts to make adding the data point faster.

			b.rwLock_.RLock()

			if int(dp.timeSeriesId) >= len(b.rows_) {
				log.Println("invalid timeSeriesId!")
				b.rwLock_.RUnlock()
				continue
			}
			item := b.rows_[dp.timeSeriesId]
			state := b.state_

			b.rwLock_.RUnlock()

			if !skipStateCheck && state != OWNED && state != PRE_UNOWNED {
				continue
			}
			b.putDataPointWithId(item.s, dp.timeSeriesId, value, dp.category)
		} else {
			// Run these through the normal workflow.
			b.Put(dp.key, value, dp.category, skipStateCheck)
		}
	}
}

// Reads compressed block files for the newest unread time window.
// This function should be called repeatedly after calling readData.
// Returns true if there might be more files to read, in which case the caller
// should call again later.
func (b *BucketMap) readBlockFiles() (bool, error) {

	b.mutex_.Lock()

	l := len(b.unreadBlockFiles_)
	if l == 0 {
		if success := b.setState(OWNED); !success {
			b.mutex_.Unlock()
			return false, fmt.Errorf("Setting state failed!")
		}
		b.mutex_.Unlock()
		return false, nil
	}

	position := uint32(b.unreadBlockFiles_[l-1])
	// delete one number from the set
	b.unreadBlockFiles_ = b.unreadBlockFiles_[:l-1]

	b.mutex_.Unlock()

	timeSeriesIds, storageIds, err := b.storage_.LoadPosition(position)
	if err != nil {
		return false, fmt.Errorf("Failed to read blockfiles for shard %d: %d",
			b.shardId_, position)
	}
	for i, id := range timeSeriesIds {
		b.rwLock_.RLock()
		if id < uint32(len(b.rowsFromDisk_)) && b.rowsFromDisk_[id] != nil {
			b.rows_[id].s.SetDataBlock(position, b.n_, storageIds[i])
		}
		b.rwLock_.RUnlock()
	}

	return true, nil
}

// Cancels unowning. This should only be called if current state is
// PRE_UNOWNED. Returns true if unowning was successful. State will
// be OWNED after a successful call.
func (b *BucketMap) cancelUnowning() bool {
	b.rwLock_.Lock()
	defer b.rwLock_.Unlock()

	if b.state_ != PRE_UNOWNED {
		return false
	}

	b.state_ = OWNED
	return true
}

// Finalizes all the buckets which haven't been finalized up to the
// given position. Returns the number of buckets that were finalized.
// If the shard is not owned, will return immediately with 0.
func (b *BucketMap) finalizeBuckets(lastBucketToFinalize uint32) (int, error) {

	if b.getState() != OWNED {
		return 0, nil
	}

	var bucketToFinalize uint32
	if b.lastFinalizedBucket_ == 0 {
		bucketToFinalize = lastBucketToFinalize
	} else {
		bucketToFinalize = b.lastFinalizedBucket_ + 1
	}

	if bucketToFinalize <= b.lastFinalizedBucket_ || bucketToFinalize > lastBucketToFinalize {
		return 0, nil
	}

	// There might be more than one bucket to finalize if the server was
	// restarted or shards moved.
	bucketsToFinalize := lastBucketToFinalize - bucketToFinalize + 1
	items := b.getEverything()
	for bucket := bucketToFinalize; bucket <= lastBucketToFinalize; bucket++ {
		for i, item := range items {
			if item != nil {
				// `i` is the id of the time series
				if err := item.s.SetCurrentBucket(bucket+1, uint32(i),
					b.getStorage()); err != nil {
					return int(bucket - bucketToFinalize), err
				}
			}
		}
		if err := b.getStorage().FinalizeBucket(bucket); err != nil {
			return int(bucket - bucketToFinalize), err
		}
	}

	b.lastFinalizedBucket_ = lastBucketToFinalize
	return int(bucketsToFinalize), nil
}

// Returns whether this BucketMap is behind more than 1 bucket.
func (b *BucketMap) isBehind(bucketToFinalize uint32) bool {
	return b.getState() == OWNED && b.lastFinalizedBucket_ != 0 &&
		bucketToFinalize > b.lastFinalizedBucket_+1
}

func (b *BucketMap) getLastFinalizedBucket() uint32 {
	return b.lastFinalizedBucket_
}

// Returns the earliest timestamp (inclusive) from which is
// unaware of any missing data.  Initialized to 0 and returns 0
// if a shard has no missing data
func (b *BucketMap) getReliableDataStartTime() int64 {
	return b.reliableDataStartTime_
}
