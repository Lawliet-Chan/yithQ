package queue

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"yithQ/message"
	"yithQ/yith/conf"
)

type DiskQueue interface {
	FillToDisk(msg []*message.Message) error
	PopFromDisk(popOffset int64) ([]*message.Message, error)
}

type diskQueue struct {
	sync.Mutex
	db             *bolt.DB
	fileNamePrefix string
	writingFile    *DiskFile
	dataFiles      []*DiskFile
	lastOffset     int64
}

func NewDiskQueue(topicPartitionInfo string, conf *conf.DiskQueueConf) (DiskQueue, error) {
	db, err := bolt.Open("./"+topicPartitionInfo, 0600, nil)
	if err != nil {
		return nil, err
	}
	writingFile, err := newDiskFile(topicPartitionInfo, 0)
	if err != nil {
		return nil, err
	}
	return &diskQueue{
		db:             db,
		fileNamePrefix: topicPartitionInfo,
		writingFile:    writingFile,
		dataFiles:      make([]*DiskFile, 0),
	}, nil
}

func (dq *diskQueue) FillToDisk(msgs []*message.Message) error {
	byt, err := json.Marshal(msgs)
	if err != nil {
		return err
	}
	if dq.writingFile.outOfLimit(byt) {
		dq.Lock()
		dq.dataFiles = append(dq.dataFiles, dq.writingFile)
		dq.Unlock()
		newSeq := dq.writingFile.seq + 1
		dq.writingFile, err = newDiskFile(dq.fileNamePrefix, newSeq)
		if err != nil {
			return err
		}
	}
	return dq.writingFile.write(byt)
}

func (dq *diskQueue) PopFromDisk(popOffset int64) ([]*message.Message, error) {

}

const QueueMetaBucket = "queue_meta"

func (dq *diskQueue) putIndexToDB() error {

}

func (dq *diskQueue) getIndexFromDB() {

}

func (dq *diskQueue) getLastOffsetFromDB() int64 {
	var lastOffset int64 = 0
	dq.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(QueueMetaBucket)).Cursor()
		k, _ := c.Last()
		intOffset, err := strconv.Atoi(string(k))
		if err != nil {
			return err
		}
		lastOffset = int64(intOffset)
		return nil
	})
	return lastOffset
}

const DiskFileSizeLimit = 1024 * 1024 * 1024

type DiskFile struct {
	startOffset int64
	endOffset   int64
	indexFile   *os.File
	dataFile    *os.File
	size        int64
	seq         int
}

func newDiskFile(name string, seq int) (*DiskFile, error) {
	dataf, err := os.Open(name + "_" + strconv.Itoa(seq) + ".data")
	if err != nil {
		return nil, err
	}
	indexf, err := os.Open(name + "_" + strconv.Itoa(seq) + ".index")
	if err != nil {
		return nil, err
	}
	var startOffset, endOffset int64
	fi, _ := indexf.Stat()
	if fi.Size() > 39 {
		dataRef, err := syscall.Mmap(int(indexf.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}
		startOffset, _ = decodeIndex(dataRef[:39])
		endOffset, _ = decodeIndex(dataRef[len(dataRef)-39:])
	}
	return &DiskFile{
		startOffset: startOffset,
		endOffset:   endOffset,
		size:        0,
		indexFile:   indexf,
		dataFile:    dataf,
		seq:         seq,
	}, nil
}

func (df *DiskFile) outOfLimit(data []byte) bool {
	return int64(len(data))+df.size > DiskFileSizeLimit
}

//TODO: will use mmap() to store data into file next version.
func (df *DiskFile) write(msgOffset int64, data []byte) error {

	si, err := df.dataFile.Write(data)
	if err != nil {
		return err
	}
	df.size += int64(si)
	_, err = df.indexFile.Write(encodeIndex(msgOffset, df.size))
	return err
}

func (df *DiskFile) read(offset, length int64) ([]byte, error) {
	data := make([]byte, length)
	_, err := df.dataFile.ReadAt(data, offset)
	return data, err
}

func encodeIndex(msgOffset, dataOffset int64) []byte {
	unitIndexBytes := make([]byte, 39)
	copy(unitIndexBytes, []byte(strconv.FormatInt(msgOffset, 10)+","+strconv.FormatInt(dataOffset, 10)))
	return unitIndexBytes
}

func decodeIndex(indexBytes []byte) (msgOffset int64, dataPosition int64) {
	indexStr := string(indexBytes)
	offsets := strings.Split(strings.TrimSpace(indexStr), ",")
	msgOffsetStr := offsets[0]
	dataOffsetStr := offsets[1]
	msgOffset, _ = strconv.ParseInt(msgOffsetStr, 10, 64)
	dataPosition, _ = strconv.ParseInt(dataOffsetStr, 10, 64)
	return
}
