package bucketLogWriter

import (
	"github.com/huangaz/tsdb/lib/bucketUtils"
	"github.com/huangaz/tsdb/lib/dataLog"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
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
	shardWriters_ map[int64](*ShardWriter)

	// MPMCQueue
	logDataQueue_ chan LogDataInfo

	// thread controller
	stopThread_    chan struct{}
	threadIsRuning bool
}

type LogDataInfo struct {
	index    int32
	shardId  int64
	unixTime int64
	value    float64
}

type ShardWriter struct {
	// bucketNumber to DataLogWriter
	logWriters        map[int](*dataLog.DataLogWriter)
	fileUtils         *fileUtils.FileUtils
	nextClearTimeSecs int64
}

const (
	START_SHARD_INDEX = -1
	STOP_SHARD_INDEX  = -2
	NO_OP_INDEX       = -3

	FILE_OPEN_RETRY = 5

	SLEEP_BETWEEN_FAILURES = 100 * time.Microsecond // 100 us
)

var (
	LogFilePrefix = dataTypes.LOG_FILE_PREFIX
)

func NewShardWriter() *ShardWriter {
	res := &ShardWriter{
		logWriters: make(map[int](*dataLog.DataLogWriter)),
	}
	return res
}

func NewBucketLogWriter(windowSize uint64, dataDirectory *string, queueSize,
	allowTimestampBehind uint32) *BucketLogWriter {

	res := &BucketLogWriter{
		windowSize_:             windowSize,
		dataDirectory_:          *dataDirectory,
		waitTimeBeforeClosing_:  allowTimestampBehind * 2,
		keepLogFilesAroundTime_: bucketUtils.Duration(2, windowSize),
		logDataQueue_:           make(chan LogDataInfo, queueSize),
		threadIsRuning:          false,
		shardWriters_:           make(map[int64](*ShardWriter)),
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
	if b.threadIsRuning {
		return
	}

	b.stopThread_ = make(chan struct{})
	b.threadIsRuning = true
	go func() {
		for {
			select {
			case <-b.stopThread_:
				return
			default:
				if !b.writeOneLogEntry() {
					// return
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()
}

func (b *BucketLogWriter) stopWriterThread() {
	if !b.threadIsRuning {
		return
	}
	close(b.stopThread_)
	b.threadIsRuning = false
}

func (b *BucketLogWriter) bucket(unixTime, shardId int64) uint32 {
	return bucketUtils.Bucket(unixTime, b.windowSize_, shardId)
}

func (b *BucketLogWriter) timestamp(bucket uint32, shardId int64) int64 {
	return bucketUtils.Timestamp(bucket, b.windowSize_, shardId)
}

func (b *BucketLogWriter) duration(buckets uint32) uint64 {
	return bucketUtils.Duration(buckets, b.windowSize_)
}

func (b *BucketLogWriter) flushQueue() {
	b.stopWriterThread()
	for b.writeOneLogEntry() {
		// do noting
	}
	b.startWriterThread()
}

// This will push the given data entry to a queue for logging.
func (b *BucketLogWriter) LogData(shardId int64, index int32, unixTime int64,
	value float64) {

	var info LogDataInfo
	info.shardId = shardId
	info.index = index
	info.unixTime = unixTime
	info.value = value

	b.logDataQueue_ <- info
}

// Writes a single entry from the queue. Does not need to be called
// if `writerThread` was defined in the constructor.
func (b *BucketLogWriter) writeOneLogEntry() bool {
	var data []LogDataInfo
	var info LogDataInfo

	if !b.threadIsRuning || len(b.logDataQueue_) == 0 {
		return false
	}

	// dequeue
	for info = range b.logDataQueue_ {
		data = append(data, info)
		if len(b.logDataQueue_) == 0 {
			break
		}
	}

	for _, info = range data {
		if info.index == START_SHARD_INDEX {
			writer := NewShardWriter()

			// Select the next clear time to be the start of a bucket between
			// windowSize_ and windowSize_ * 2 to spread out the clear operations.
			writer.nextClearTimeSecs = bucketUtils.FloorTimestamp(time.Now().Unix()+
				int64(b.duration(2)), b.windowSize_, info.shardId)

			writer.fileUtils = fileUtils.NewFileUtils(info.shardId, LogFilePrefix,
				b.dataDirectory_)
			b.shardWriters_[info.shardId] = writer

		} else if info.index == STOP_SHARD_INDEX {

			// close file
			sw, ok := b.shardWriters_[info.shardId]
			if ok {
				sw.DeleteShradWriter()
			}

			delete(b.shardWriters_, info.shardId)

		} else if info.index != NO_OP_INDEX {
			sw, ok := b.shardWriters_[info.shardId]
			if !ok {
				log.Printf("Trying to write to a shard thst is not enables for writing %d",
					info.shardId)
				continue
			}

			bucketNum := int(b.bucket(info.unixTime, info.shardId))
			logWriter := sw.logWriters[bucketNum]

			// Create a new DataLogWriter and a new file.
			if logWriter == nil {
				for i := 0; i < FILE_OPEN_RETRY; i++ {
					f, err := sw.fileUtils.Open(int(info.unixTime), "wc")
					if err == nil && f.File != nil {
						logWriter = dataLog.NewDataLogWriter(&f, info.unixTime)
						break
					}
					if i == FILE_OPEN_RETRY-1 {
						log.Printf("Failed too many times to open log file %s", f.Name)
					}
					time.Sleep(SLEEP_BETWEEN_FAILURES)
				}
			}

			// Append the new logWriter
			sw.logWriters[bucketNum] = logWriter

			// Open files for the next bucket in the last 1/10 of the time window.
			openNextFileTime := b.timestamp(uint32(bucketNum), info.shardId) +
				int64(float64(b.windowSize_)*0.9)
			_, ok = sw.logWriters[bucketNum+1]
			if time.Now().Unix() > openNextFileTime && !ok {
				// Create a new DataLogWriter and a new file for next bucket.
				baseTime := b.timestamp(uint32(bucketNum+1), info.shardId)
				for i := 0; i < FILE_OPEN_RETRY; i++ {
					f, err := sw.fileUtils.Open(int(baseTime), "wc")
					if err == nil && f.File != nil {
						// Append the new logWriter.
						sw.logWriters[bucketNum+1] = dataLog.NewDataLogWriter(&f,
							baseTime)
						break
					}
					if i == FILE_OPEN_RETRY-1 {
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
			var onePreviousLogWriterCleared bool = false

			if !onePreviousLogWriterCleared && now-bucketUtils.FloorTimestamp(now, b.windowSize_,
				info.shardId) > int64(b.waitTimeBeforeClosing_) && !ok {
				delete(sw.logWriters, bucketNum-1)
				onePreviousLogWriterCleared = true
			}

			if now > sw.nextClearTimeSecs {
				sw.fileUtils.ClearTo(int(now - int64(b.keepLogFilesAroundTime_)))
				sw.nextClearTimeSecs += int64(b.duration(1))
			}

			if logWriter != nil {
				logWriter.Append(uint32(info.index), info.unixTime, info.value)
				// logWriter.DeleteDataLogWriter()
				logWriter.FlushBuffer()
			}
		}
	}

	// Don't flush any of the logWriters. DataLog class will handle the
	// flushing when there's enough data.
	return !(len(data) == 0)
}

// Starts writing points for this shard.
func (b *BucketLogWriter) StartShard(shardId int64) {
	var info LogDataInfo
	info.shardId = shardId
	info.index = START_SHARD_INDEX
	b.logDataQueue_ <- info
}

// Stops writing points for this shard and closes all the open files.
func (b *BucketLogWriter) StopShard(shardId int64) {
	var info LogDataInfo
	info.shardId = shardId
	b.logDataQueue_ <- info
}
