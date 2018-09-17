package queue

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

//const QueueMetaBucket = "queue_meta"

func (dq *diskQueue) getLastOffsetFromDB() int64 {
	/*var lastOffset int64 = 0
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
	return lastOffset*/
}

const DiskFileSizeLimit = 1024 * 1024 * 1024
const EachIndexLen = 39

type DiskFile struct {
	startOffset int64
	endOffset   int64
	indexFile   *os.File
	dataFile    *os.File
	size        int64
	//Diskfile的编号，diskfile命名规则：topicPartition+seq
	seq int
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
	if fi.Size() >= EachIndexLen {
		dataRef, err := syscall.Mmap(int(indexf.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}
		startOffset, _ = decodeIndex(dataRef[:EachIndexLen])
		endOffset, _ = decodeIndex(dataRef[len(dataRef)-EachIndexLen:])
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
	if df.size == 0 {
		atomic.StoreInt64(&df.startOffset, msgOffset)
	}
	df.size += int64(si)
	_, err = df.indexFile.Write(encodeIndex(msgOffset, df.size))
	if err != nil {
		return err
	}
	atomic.StoreInt64(&df.endOffset, msgOffset)
	return nil
}

func (df *DiskFile) read(msgOffset int64) ([]byte, error) {
	startIndexPosition := (msgOffset-df.startOffset)*EachIndexLen + 1
	endIndexPosition := (msgOffset-df.startOffset+256)*EachIndexLen + 1
	startIndex := make([]byte, EachIndexLen)
	endIndex := make([]byte, EachIndexLen)
	_, err := df.indexFile.ReadAt(startIndex, startIndexPosition)
	if err != nil {
		return nil, err
	}
	_, err = df.indexFile.ReadAt(endIndex, endIndexPosition)
	if err != nil {
		return nil, err
	}

	_, startOffset := decodeIndex(startIndex)
	_, endOffset := decodeIndex(endIndex)

	return syscall.Mmap(int(df.dataFile.Fd()), startOffset, int(endOffset-startOffset), syscall.PROT_READ, syscall.MAP_SHARED)
}

func (df *DiskFile) getStartOffset() int64 {
	return atomic.LoadInt64(&df.startOffset)
}

func (df *DiskFile) getEndOffset() int64 {
	return atomic.LoadInt64(&df.endOffset)
}

func encodeIndex(msgOffset, dataOffset int64) []byte {
	unitIndexBytes := make([]byte, EachIndexLen)
	copy(unitIndexBytes, []byte(strconv.FormatInt(msgOffset, 10)+","+strconv.FormatInt(dataOffset, 10)))
	return unitIndexBytes
}

func decodeIndex(indexBytes []byte) (msgOffset int64, dataPosition int64) {
	indexStr := string(indexBytes)
	offsets := strings.Split(strings.TrimSpace(indexStr), ",")
	msgOffsetStr := offsets[0]
	dataPositionStr := offsets[1]
	msgOffset, _ = strconv.ParseInt(msgOffsetStr, 10, 64)
	dataPosition, _ = strconv.ParseInt(dataPositionStr, 10, 64)
	return
}
