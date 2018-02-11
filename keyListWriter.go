package tsdb

import (
	"fmt"
	"log"
	"sync"
)

// keyType
const (
	KEYLIST_START_SHARD = iota
	KEYLIST_STOP_SHARD
	KEYLIST_WRITE_KEY
)

type KeyListWriter struct {
	sync.RWMutex
	keyInfoQueue_  chan *KeyInfo
	dataDirectory_ string
	keyWriters_    map[int32]*PersistentKeyList

	// thread controller
	//stopThread_    chan struct{}
	//threadIsRuning bool
	done chan struct{}
}

type KeyInfo struct {
	ShardId  int32
	Key      string
	KeyId    int32
	Category uint16
	KeyType  int
}

func NewKeyListWriter(dataDirectory string, queueSize uint32) *KeyListWriter {
	res := &KeyListWriter{
		keyInfoQueue_:  make(chan *KeyInfo, queueSize),
		dataDirectory_: dataDirectory,
		keyWriters_:    make(map[int32]*PersistentKeyList),
	}

	res.startWriterThread()
	return res
}

func (k *KeyListWriter) DeleteKeyListWriter() {
	k.stopWriterThread()
	close(k.keyInfoQueue_)
}

// Copy a new key onto the queue for writing.
func (k *KeyListWriter) AddKey(shardId, keyId int32, key string, category uint16) {
	info := &KeyInfo{
		ShardId:  shardId,
		Key:      key,
		KeyId:    keyId,
		KeyType:  KEYLIST_WRITE_KEY,
		Category: category,
	}
	k.keyInfoQueue_ <- info
}

// Pass a compaction call down to the appropriate PersistentKeyList.
func (k *KeyListWriter) Compact(shardId int32, generator func() *KeyItem) error {
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

func (k *KeyListWriter) StartShard(shardId int32) {
	info := &KeyInfo{
		ShardId: shardId,
		KeyType: KEYLIST_START_SHARD,
	}
	k.keyInfoQueue_ <- info
}

func (k *KeyListWriter) StopShard(shardId int32) {
	info := &KeyInfo{
		ShardId: shardId,
		KeyType: KEYLIST_STOP_SHARD,
	}
	k.keyInfoQueue_ <- info
}

func (k *KeyListWriter) get(shardId int32) *PersistentKeyList {
	k.RLock()
	defer k.RUnlock()

	res, ok := k.keyWriters_[shardId]
	if !ok {
		return nil
	}
	return res
}

func (k *KeyListWriter) enable(shardId int32) {
	k.Lock()
	defer k.Unlock()
	k.keyWriters_[shardId] = NewPersistentKeyList(shardId, k.dataDirectory_)
}

func (k *KeyListWriter) disable(shardId int32) {
	k.Lock()
	defer k.Unlock()
	if p, ok := k.keyWriters_[shardId]; ok {
		p.DeletePersistentKeyList()
	}
	delete(k.keyWriters_, shardId)
}

/*
func (k *KeyListWriter) flushQueue() {
	// Stop thread to flush keys
	k.stopWriterThread()
	k.startWriterThread()
}
*/

func (k *KeyListWriter) startWriterThread() {
	if k.done != nil {
		select {
		case <-k.done:
			// thread is stopped
		default:
			// thread is running
			return
		}
	}

	k.done = make(chan struct{}, 0)
	ch := k.keyInfoQueue_

	go func() {
		for {
			select {
			case key := <-ch:
				k.writeOneKey(key)
			case <-k.done:
				return
			}
		}
	}()
}

func (k *KeyListWriter) stopWriterThread() {
	select {
	case <-k.done:
		// thread is already stopped
		return
	default:
	}

	k.done <- struct{}{}
	close(k.done)
}

// Write a single entry from the queue.
func (k *KeyListWriter) writeOneKey(key *KeyInfo) {
	switch key.KeyType {
	case KEYLIST_START_SHARD:
		k.enable(key.ShardId)
	case KEYLIST_STOP_SHARD:
		k.disable(key.ShardId)
	case KEYLIST_WRITE_KEY:
		writer := k.get(key.ShardId)
		if writer == nil {
			log.Printf("Trying to write key to non-enabled shard %d", key.ShardId)
			return
		}
		item := &KeyItem{key.KeyId, key.Key, key.Category}
		if !writer.AppendKey(item) {
			log.Printf("Failed to write key '%s' to log for shard %d", key.Key,
				key.ShardId)
			return

		}
	default:
		log.Printf("Invalid keyType: %d", key.KeyType)
	}
}
