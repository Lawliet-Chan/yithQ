package queue

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"sync"
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
	size int64
	file *os.File
	seq  int
}

func newDiskFile(name string, seq int) (*DiskFile, error) {
	f, err := os.Open(name + "_" + strconv.Itoa(seq))
	if err != nil {
		return nil, err
	}
	return &DiskFile{
		size: 0,
		file: f,
		seq:  seq,
	}, nil
}

func (df *DiskFile) outOfLimit(data []byte) bool {
	return int64(len(data))+df.size > DiskFileSizeLimit
}

//TODO: will use mmap() to store data into file next version.
func (df *DiskFile) write(data []byte) error {
	si, err := df.file.Write(data)
	if err != nil {
		return err
	}
	df.size += int64(si)
	return nil
}

func (df *DiskFile) read(offset, length int64) ([]byte, error) {
	data := make([]byte, length)
	_, err := df.file.ReadAt(data, offset)
	return data, err
}
