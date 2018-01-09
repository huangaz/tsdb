package keyListWriter

import (
	"fmt"
	"github.com/huangaz/tsdb/lib/persistentKeyList"
	"log"
	"sync"
)

// keyType
const (
	START_SHARD = iota
	STOP_SHARD
	WRITE_KEY
)

type KeyListWriter struct {
	keyInfoQueue_  chan KeyInfo
	dataDirectory_ string
	lock_          sync.Mutex
	keyWriters_    map[int64]*persistentKeyList.PersistentKeyList

	// thread controller
	stopThread_    chan struct{}
	threadIsRuning bool
}

type KeyInfo struct {
	shardId  int64
	key      string
	keyId    int32
	category uint16
	keyType  int
}

func NewKeyListWriter(dataDirectory string, queueSize uint32) *KeyListWriter {
	res := &KeyListWriter{
		keyInfoQueue_:  make(chan KeyInfo, queueSize),
		dataDirectory_: dataDirectory,
		keyWriters_:    make(map[int64]*persistentKeyList.PersistentKeyList),
		threadIsRuning: false,
	}

	res.startWriterThread()
	return res
}

func (k *KeyListWriter) DeleteKeyListWriter() {
	k.stopWriterThread()
	close(k.keyInfoQueue_)
}

// Copy a new key onto the queue for writing.
func (k *KeyListWriter) AddKey(shardId int64, id int32, key string, category uint16) {
	var info KeyInfo
	info.shardId = shardId
	info.key = key
	info.keyId = id
	info.keyType = WRITE_KEY
	info.category = category

	k.keyInfoQueue_ <- info
}

// Pass a compaction call down to the appropriate PersistentKeyList.
func (k *KeyListWriter) Compact(shardId int64, generator func() persistentKeyList.KeyItem) error {
	writer := k.get(shardId)
	if writer == nil {
		return fmt.Errorf("Trying to compact non-enabled shard %d", shardId)
	}
	err := writer.Compact(generator)
	if err != nil {
		return err
	}
	return nil
}

func (k *KeyListWriter) StartShard(shardId int64) {
	var info KeyInfo
	info.shardId = shardId
	info.keyType = START_SHARD
	k.keyInfoQueue_ <- info
}

func (k *KeyListWriter) StopShard(shardId int64) {
	var info KeyInfo
	info.shardId = shardId
	info.keyType = STOP_SHARD
	k.keyInfoQueue_ <- info
}

func (k *KeyListWriter) get(shardId int64) *persistentKeyList.PersistentKeyList {
	k.lock_.Lock()
	defer k.lock_.Unlock()

	res, ok := k.keyWriters_[shardId]
	if !ok {
		return nil
	}
	return res
}

func (k *KeyListWriter) enable(shardId int64) {
	k.lock_.Lock()
	defer k.lock_.Unlock()
	k.keyWriters_[shardId] = persistentKeyList.NewPersistentKeyList(shardId, k.dataDirectory_)
}

func (k *KeyListWriter) disable(shardId int64) {
	k.lock_.Lock()
	defer k.lock_.Unlock()
	delete(k.keyWriters_, shardId)
}

func (k *KeyListWriter) flushQueue() {
	// Stop thread to flush keys
	k.stopWriterThread()
	k.startWriterThread()
}

func (k *KeyListWriter) startWriterThread() {
	if k.threadIsRuning {
		return
	}

	k.stopThread_ = make(chan struct{})
	k.threadIsRuning = true
	go func() {
		for {
			select {
			case <-k.stopThread_:
				return
			default:
				k.writeOneKey()
			}
		}
	}()
}

func (k *KeyListWriter) stopWriterThread() {
	if !k.threadIsRuning {
		return
	}
	close(k.stopThread_)
	k.threadIsRuning = false
}

// Write a single entry from the queue.
func (k *KeyListWriter) writeOneKey() {
	var keys []KeyInfo
	var info KeyInfo

	if !k.threadIsRuning || len(k.keyInfoQueue_) == 0 {
		return
	}

	// dequeue
	for info = range k.keyInfoQueue_ {
		keys = append(keys, info)
		if len(k.keyInfoQueue_) == 0 {
			break
		}
	}

	for _, key := range keys {
		switch key.keyType {
		case START_SHARD:
			k.enable(key.shardId)
		case STOP_SHARD:
			k.disable(key.shardId)
		case WRITE_KEY:
			writer := k.get(key.shardId)
			if writer == nil {
				log.Printf("Trying to write key to non-enabled shard %d", key.shardId)
				continue
			}
			item := persistentKeyList.KeyItem{key.keyId, key.key, key.category}
			if !writer.AppendKey(item) {
				log.Printf("Failed to write key '%s' to log for shard %d", key.key,
					key.shardId)

				// Try to put it back in the queue for later.
				k.keyInfoQueue_ <- key
			}
		default:
			log.Printf("Invalid keyType: %d", key.keyType)
		}
	}

	return
}
