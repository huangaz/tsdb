package tsdb

import (
	"log"
	"time"
)

type BucketLogWriter struct {
	// numShards_              uint32
	windowSize_             uint64
	dataDirectory_          string
	waitTimeBeforeClosing_  uint32
	keepLogFilesAroundTime_ uint64

	// shardId to *ShardWriter
	shardWriters_ map[int32](*ShardWriter)

	// MPMCQueue
	logDataQueue_ chan *LogDataInfo

	// thread controller
	// stopThread_    chan struct{}
	// threadIsRuning bool
	done chan struct{}
}

type LogDataInfo struct {
	Index    int32
	ShardId  int32
	UnixTime int64
	Value    float64
}

type ShardWriter struct {
	// bucketNumber to DataLogWriter
	logWriters        map[int](*DataLogWriter)
	fileUtils         *FileUtils
	nextClearTimeSecs int64
}

const (
	LOG_START_SHARD = -1
	LOG_STOP_SHARD  = -2
	NO_OP_INDEX     = -3

	LOG_FILE_OPEN_RETRY = 5

	SLEEP_BETWEEN_FAILURES = 100 * time.Microsecond // 100 us
)

func NewShardWriter() *ShardWriter {
	res := &ShardWriter{
		logWriters: make(map[int](*DataLogWriter)),
	}
	return res
}

func NewBucketLogWriter(windowSize uint64, dataDirectory string, queueSize,
	allowTimestampBehind uint32) *BucketLogWriter {

	res := &BucketLogWriter{
		windowSize_:             windowSize,
		dataDirectory_:          dataDirectory,
		waitTimeBeforeClosing_:  allowTimestampBehind * 2,
		keepLogFilesAroundTime_: Duration(2, windowSize),
		logDataQueue_:           make(chan *LogDataInfo, queueSize),
		shardWriters_:           make(map[int32](*ShardWriter)),
	}

	if windowSize <= uint64(allowTimestampBehind) {
		log.Printf("Window size %d must be larger than allowTimestampBehind %d",
			windowSize, allowTimestampBehind)
		return nil
	}

	res.startWriterThread()
	return res
}

func (b *BucketLogWriter) DeleteBucketLogWriter() {
	b.stopWriterThread()
	close(b.logDataQueue_)
	for _, sw := range b.shardWriters_ {
		sw.DeleteShradWriter()
	}
}

func (s *ShardWriter) DeleteShradWriter() {
	for _, lw := range s.logWriters {
		lw.DeleteDataLogWriter()
	}
}

func (b *BucketLogWriter) startWriterThread() {
	if b.done != nil {
		select {
		case <-b.done:
			// thread is stopped
		default:
			// thread is running
			return
		}
	}

	b.done = make(chan struct{}, 0)
	ch := b.logDataQueue_

	go func() {
		for {
			select {
			case logInfo := <-ch:
				b.writeOneLogEntry(logInfo)
			case <-b.done:
				return
			}
		}
	}()
}

func (b *BucketLogWriter) stopWriterThread() {
	select {
	case <-b.done:
		// thread is already stopped
		return
	default:
	}

	b.done <- struct{}{}
	close(b.done)
}

/*
func (b *BucketLogWriter) bucket(unixTime, shardId int64) uint32 {
	return Bucket(unixTime, b.windowSize_, shardId)
}

func (b *BucketLogWriter) timestamp(bucket uint32, shardId int64) int64 {
	return Timestamp(bucket, b.windowSize_, shardId)
}
*/

func (b *BucketLogWriter) bucket(unixTime int64) uint32 {
	return Bucket(unixTime, b.windowSize_)
}

func (b *BucketLogWriter) timestamp(bucket uint32) int64 {
	return Timestamp(bucket, b.windowSize_)
}

func (b *BucketLogWriter) duration(buckets uint32) uint64 {
	return Duration(buckets, b.windowSize_)
}

func (b *BucketLogWriter) floorTimestamp(unixTime int64) int64 {
	return b.timestamp(b.bucket(unixTime))
}

/*
func (b *BucketLogWriter) flushQueue() {
	b.stopWriterThread()
	for b.writeOneLogEntry() {
		// do noting
	}
	b.startWriterThread()
}
*/

// This will push the given data entry to a queue for logging.
func (b *BucketLogWriter) LogData(shardId, index int32, unixTime int64,
	value float64) {

	info := &LogDataInfo{
		ShardId:  shardId,
		Index:    index,
		UnixTime: unixTime,
		Value:    value,
	}

	b.logDataQueue_ <- info
}

// Writes a single entry from the queue. Does not need to be called
// if `writerThread` was defined in the constructor.
func (b *BucketLogWriter) writeOneLogEntry(info *LogDataInfo) {
	switch info.Index {
	case LOG_START_SHARD:
		writer := NewShardWriter()

		// Select the next clear time to be the start of a bucket between
		// windowSize_ and windowSize_ * 2 to spread out the clear operations.
		writer.nextClearTimeSecs = b.floorTimestamp(time.Now().Unix() +
			int64(b.duration(2)))

		writer.fileUtils = NewFileUtils(info.ShardId, LOG_FILE_PREFIX,
			b.dataDirectory_)
		b.shardWriters_[info.ShardId] = writer

	case LOG_STOP_SHARD:

		// close file
		sw, ok := b.shardWriters_[info.ShardId]
		if ok {
			sw.DeleteShradWriter()
		}

		delete(b.shardWriters_, info.ShardId)

	case NO_OP_INDEX:
		return

	default:
		sw, ok := b.shardWriters_[info.ShardId]
		if !ok {
			log.Printf("Trying to write to a shard thst is not enables for writing %d",
				info.ShardId)
			return
		}

		bucketNum := int(b.bucket(info.UnixTime))
		logWriter := sw.logWriters[bucketNum]

		// Create a new DataLogWriter and a new file.
		if logWriter == nil {
			for i := 0; i < LOG_FILE_OPEN_RETRY; i++ {
				f, err := sw.fileUtils.Open(int(info.UnixTime), "wc")
				if err == nil && f.File != nil {
					logWriter = NewDataLogWriter(&f, info.UnixTime)
					break
				}
				if i == LOG_FILE_OPEN_RETRY-1 {
					log.Printf("Failed too many times to open log file %s", f.Name)
				}
				time.Sleep(SLEEP_BETWEEN_FAILURES)
			}
		}

		// Append the new logWriter
		sw.logWriters[bucketNum] = logWriter

		// Open files for the next bucket in the last 1/10 of the time window.
		openNextFileTime := b.timestamp(uint32(bucketNum)) +
			int64(float64(b.windowSize_)*0.9)
		_, ok = sw.logWriters[bucketNum+1]
		if time.Now().Unix() > openNextFileTime && !ok {
			// Create a new DataLogWriter and a new file for next bucket.
			baseTime := b.timestamp(uint32(bucketNum + 1))
			for i := 0; i < LOG_FILE_OPEN_RETRY; i++ {
				f, err := sw.fileUtils.Open(int(baseTime), "wc")
				if err == nil && f.File != nil {
					// Append the new logWriter.
					sw.logWriters[bucketNum+1] = NewDataLogWriter(&f,
						baseTime)
					break
				}
				if i == LOG_FILE_OPEN_RETRY-1 {
					log.Printf("Failed too many times to open next log file %s",
						f.Name)
				}
				time.Sleep(SLEEP_BETWEEN_FAILURES)
			}
		}

		// Only clear at most one previous bucket because the operation
		// is really slow and queue might fill up if multiple buckets
		// are cleared.
		now := time.Now().Unix()
		_, ok = sw.logWriters[bucketNum-1]

		if now-b.floorTimestamp(now) > int64(b.waitTimeBeforeClosing_) && ok {
			delete(sw.logWriters, bucketNum-1)
		}

		if now > sw.nextClearTimeSecs {
			sw.fileUtils.ClearTo(int(now - int64(b.keepLogFilesAroundTime_)))
			sw.nextClearTimeSecs += int64(b.duration(1))
		}

		if logWriter != nil {
			logWriter.Append(uint32(info.Index), info.UnixTime, info.Value)
			// flushBuffer() when debugging
			logWriter.FlushBuffer()
		}
	}
}

// Starts writing points for this shard.
func (b *BucketLogWriter) StartShard(shardId int32) {
	info := &LogDataInfo{
		ShardId: shardId,
		Index:   LOG_START_SHARD,
	}
	b.logDataQueue_ <- info
}

// Stops writing points for this shard and closes all the open files.
func (b *BucketLogWriter) StopShard(shardId int32) {
	info := &LogDataInfo{
		ShardId: shardId,
		Index:   LOG_STOP_SHARD,
	}
	b.logDataQueue_ <- info
}
