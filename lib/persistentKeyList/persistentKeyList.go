// Keeps an on-disk list of (key_id, key_string) pairs.
// This class is thread-safe, but no attempt is made at managing the concurrency
// of the file itself.
package persistentKeyList

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/fileUtils"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"time"
)

type PersistentKeyList struct {
	activeList_ fileUtils.File
	files_      fileUtils.FileUtils
	lock_       sync.Mutex
	shard_      int64
	// buffer_     string
	buffer_                []byte
	nextHardFlushTimeSecs_ int64
}

type KeyItem struct {
	Id       int32
	Key      string
	Category uint16
}

const (
	HARD_FLUSH_INTERVAL_SECS = 120
	FILE_OPEN_RETRY          = 3
	TEMP_FILE_ID             = 0

	// Flush after 4k of keys.
	SMALL_BUFFER_SIZE = 1 << 12

	// Marker bytes to determine if there are categories or not.
	FILE_MARKER                 = '0'
	FILE_WITH_CATEGORIES_MARKER = '1'
)

var (
	KeyFilePrefix = dataTypes.KEY_FILE_PREFIX
)

func NewPersistentKeyList(shardId int64, dataDirectory string) *PersistentKeyList {
	res := &PersistentKeyList{
		activeList_: fileUtils.File{File: nil, Name: ""},
		files_:      *fileUtils.NewFileUtils(shardId, KeyFilePrefix, dataDirectory),
		shard_:      shardId,
	}

	// Randomly select the next flush time within the interval to spread
	// the fflush calls between shards.
	res.nextHardFlushTimeSecs_ = time.Now().Unix() + int64(rand.Intn(HARD_FLUSH_INTERVAL_SECS))
	res.openNext()

	return res
}

func (p *PersistentKeyList) DeletePersistentKeyList() {
	if p.activeList_.File != nil {
		p.flush(true)
		p.activeList_.File.Close()
	}
}

// Prepare a new file for writes. Returns the id of the previous one.
func (p *PersistentKeyList) openNext() (int64, error) {
	p.lock_.Lock()
	defer p.lock_.Unlock()

	if p.activeList_.File != nil {
		p.activeList_.File.Close()
	}

	ids, err := p.files_.Ls()
	if err != nil {
		return 0, err
	}

	var activeId int
	if len(ids) == 0 {
		activeId = 1
	} else {
		activeId = ids[len(ids)-1] + 1
	}

	for i := 0; i < FILE_OPEN_RETRY; i++ {
		p.activeList_, err = p.files_.Open(activeId, "wc")
		if err == nil && p.activeList_.File != nil {
			break
		}
		if i == FILE_OPEN_RETRY-1 {
			err = fmt.Errorf("Couldn't open key_list %d for write (shard %d)\n",
				activeId, p.shard_)
			return 0, err
		}
	}

	tmpSlice := []byte{FILE_WITH_CATEGORIES_MARKER}
	written, err := p.activeList_.File.Write(tmpSlice)
	if err != nil {
		return 0, err
	}
	if written != 1 {
		err = fmt.Errorf("Could not write to the key list file %s\n", p.activeList_.Name)
		return 0, err
	}

	return int64(activeId - 1), nil
}

// Writes the internal buffer to disk. If `hardFlush` is set to true
// forces new keys out to disk instead of leaving them in the OS
// buffer. This is called internally once a minute so that too much
// data isn't lost if we go OOM or segfault or get kill -9'd by
// tupperware.
func (p *PersistentKeyList) flush(hardFlush bool) error {
	if p.activeList_.File == nil {
		_, err := p.openNext()
		if err != nil {
			return err
		}
	}

	if len(p.buffer_) > 0 {
		written, err := p.activeList_.File.Write(p.buffer_)
		if err != nil {
			return err
		}
		if written != len(p.buffer_) {
			return fmt.Errorf("Failed to flush key list file %s\n", p.activeList_.Name)
		}
		p.buffer_ = p.buffer_[:0]
	}

	if hardFlush {
		// fflush
	}
	return nil
}

// Call f on each key in the list.
// The callback should return false if reading should be stopped.
func ReadKeys(shardId int64, dataDirectory string, f func(KeyItem) bool) (int, error) {

	files := fileUtils.NewFileUtils(shardId, KeyFilePrefix, dataDirectory)

	// Read all the keys from all the relevant files.
	ids, err := files.Ls()
	if err != nil {
		return 0, err
	}

	var keys int = 0
	for _, fileId := range ids {
		if fileId == TEMP_FILE_ID {
			continue
		}

		file, err := files.Open(fileId, "rc")
		if err != nil {
			log.Printf("Open file failed: %s\n", err.Error())
			continue
		}
		defer file.File.Close()

		buffer, err := ioutil.ReadAll(file.File)
		if err != nil {
			log.Printf("Read file failed: %s\n", err.Error())
			continue
		}

		length := len(buffer)
		if length <= 1 {
			continue
		}

		var keysFound int = 0

		if buffer[0] == FILE_MARKER || buffer[0] == FILE_WITH_CATEGORIES_MARKER {
			keysFound = readKeysFromBuffer(buffer[1:], length-1,
				buffer[0] == FILE_WITH_CATEGORIES_MARKER, f)
		}

		if keysFound == 0 {
			log.Printf("%s contains no valid data.\n", file.Name)
		}
		keys += keysFound
	}

	return keys, nil
}

func readKeysFromBuffer(buffer []byte, len int, categoryPresent bool,
	f func(KeyItem) bool) int {

	var keys int = 0
	// sizeof(id) + sizeof(keyLength)
	minRecordLength := 4 + 4
	if categoryPresent {
		// sizeof(uint16)
		minRecordLength += 2
	}

	// Read the records one-by-one until fewer than 5 or 7 bytes remain.
	// A minimum record is an uint32 (+uint16) and a zero-length string.
	endIndex := len - minRecordLength
	var defaultCategory uint16 = 0
	for index := 0; index <= endIndex; {
		item := KeyItem{Category: defaultCategory}

		// read "id" from buffer
		item.Id = int32(binary.BigEndian.Uint32(buffer[index : index+4]))
		index += 4

		// read "category" from buffer
		if categoryPresent {
			item.Category = binary.BigEndian.Uint16(buffer[index : index+2])
			index += 2
		}

		// read "keyLength" from buffer
		keyLength := int(binary.BigEndian.Uint32(buffer[index : index+4]))
		index += 4

		// read "key" from buffer
		keySlice := make([]byte, keyLength)
		err := binary.Read(bytes.NewReader(buffer[index:index+keyLength]),
			binary.BigEndian, keySlice)
		if err != nil {
			break
		}
		item.Key = string(keySlice)
		index += keyLength

		if !f(item) {
			break
		}
		keys++
	}

	return keys
}

// Must not be called until after a call to ReadKeys().
// Returns false on failure.
func (p *PersistentKeyList) AppendKey(item KeyItem) bool {
	p.lock_.Lock()
	defer p.lock_.Unlock()

	if p.activeList_.File == nil {
		return false
	}

	p.writeKey(item)
	return true
}

// Writes new key to internal buffer. Flushes to disk when buffer is
// big enough or enough time has passed since the last flush time.
func (p *PersistentKeyList) writeKey(item KeyItem) {
	p.appendBuffer(&p.buffer_, item)
	flushHard := time.Now().Unix() > p.nextHardFlushTimeSecs_
	if flushHard {
		p.nextHardFlushTimeSecs_ = time.Now().Unix() + HARD_FLUSH_INTERVAL_SECS
	}

	// only for test
	p.flush(true)

	/*
		if len(p.buffer_) >= SMALL_BUFFER_SIZE || flushHard {
			p.flush(flushHard)
		}
	*/
}

// Appends id, key, category to the given buffer. The buffer can be
// later written to disk. Does not clear the buffer before
// appending.
func (p *PersistentKeyList) appendBuffer(buffer *[]byte, item KeyItem) {
	// sizeof(id) + len(string) + sizeof(category) + sizeof(keyLength)
	keyLength := len(item.Key)
	dataLen := 4 + keyLength + 2 + 4
	tmpBuffer := make([]byte, dataLen)
	index := 0

	tmpSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(tmpSlice, uint32(item.Id))
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, tmpBuffer[index:index+4])
	index += 4

	tmpSlice = make([]byte, 2)
	binary.BigEndian.PutUint16(tmpSlice, item.Category)
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, tmpBuffer[index:index+2])
	index += 2

	tmpSlice = make([]byte, 4)
	binary.BigEndian.PutUint32(tmpSlice, uint32(keyLength))
	binary.Read(bytes.NewReader(tmpSlice), binary.BigEndian, tmpBuffer[index:index+4])
	index += 4

	copy(tmpBuffer[index:], []byte(item.Key))

	*buffer = append(*buffer, tmpBuffer...)
}

// Rewrite and compress the file to contain only the generated
// entries. Continues generating until receiving a nullptr key.
// This function should only be called by a single thread at a time,
// but concurrent calls to appendKey() are safe.
func (p *PersistentKeyList) Compact(generator func() KeyItem) error {
	// Directly appends to a new file.
	prev, err := p.openNext()
	if err != nil {
		return err
	}

	// Create a temporary compressed file.
	tmpFile, err := p.files_.Open(TEMP_FILE_ID, "wc")
	if err != nil {
		return err
	}

	var buffer []byte
	for item := generator(); item.Key != ""; item = generator() {
		p.appendBuffer(&buffer, item)
	}

	if len(buffer) == 0 {
		return nil
	}

	tmpSlice := []byte{FILE_WITH_CATEGORIES_MARKER}
	written, err := tmpFile.File.Write(tmpSlice)
	if err != nil {
		return err
	}
	if written != 1 {
		err = fmt.Errorf("Could not write to the key list file %s\n", tmpFile.Name)
		return err
	}

	written, err = tmpFile.File.Write(buffer)
	if err != nil {
		return err
	}
	if written != len(buffer) {
		return fmt.Errorf("Failed to flush key list file %s\n", tmpFile.Name)
	}

	// Swap the new data in for the old.
	tmpFile.File.Close()
	err = p.files_.Rename(TEMP_FILE_ID, int(prev))
	if err != nil {
		return err
	}

	// Clean up remaining files.
	err = p.files_.ClearTo(int(prev))
	if err != nil {
		return err
	}
	return nil
}
