package queue

import (
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"yithQ/message"
)

type DiskQueue interface {
	FillToDisk(msg []*message.Message) error
	PopFromDisk(popOffset int64) ([]*message.Message, error)
}

type diskQueue struct {
	sync.Mutex
	//db             *bolt.DB
	fileNamePrefix string
	writingFile    *DiskFile
	readingFile    *DiskFile
	storeFiles     atomic.Value //type is  []*DiskFile
	lastOffset     int64
}

func NewDiskQueue(topicPartitionInfo string) (DiskQueue, error) {
	/*db, err := bolt.Open("./"+topicPartitionInfo, 0600, nil)
	if err != nil {
		return nil, err
	}*/

	writingFile, err := newDiskFile(topicPartitionInfo, 0)
	if err != nil {
		return nil, err
	}
	return &diskQueue{
		//db:             db,
		fileNamePrefix: topicPartitionInfo,
		writingFile:    writingFile,
		storeFiles:     atomic.Value{make([]*DiskFile, 0)},
	}, nil
}

func (dq *diskQueue) FillToDisk(msgs []*message.Message) error {
	if dq.writingFile == nil {
		storeFiles := dq.storeFiles.Load().([]*DiskFile)
		dq.writingFile = storeFiles[len(storeFiles)-1]
	}

	byt, err := json.Marshal(msgs)
	if err != nil {
		return err
	}
	if dq.writingFile.outOfLimit(byt) {
		dq.Lock()
		dq.storeFiles = append(dq.storeFiles, dq.writingFile)
		dq.Unlock()
		newSeq := dq.writingFile.seq + 1
		dq.writingFile, err = newDiskFile(dq.fileNamePrefix, newSeq)
		if err != nil {
			return err
		}
	}
	return dq.writingFile.write(byt)
}

func (dq *diskQueue) PopFromDisk(msgOffset int64) ([]*message.Message, error) {
	if dq.readingFile == nil {
		dq.readingFile = findReadingFileByOffset(dq.storeFiles.Load().([]*DiskFile), msgOffset)
	}
	data, err := dq.readingFile.read(msgOffset, 1)
	if err != nil {
		if err == io.EOF && msgOffset <= atomic.LoadInt64(&dq.lastOffset) {
			dq.readingFile = nil
			return dq.PopFromDisk(msgOffset)
		}
		return nil, err
	}
	var msgs []*message.Message
	err = json.Unmarshal(data, &msgs)
	return msgs, err
}

func findReadingFileByOffset(files []*DiskFile, msgOffset int64) *DiskFile {
	midStoreFile := files[len(files)/2]
	if midStoreFile.startOffset <= msgOffset && midStoreFile.endOffset >= msgOffset {
		return midStoreFile
	} else if midStoreFile.startOffset > msgOffset {
		return findReadingFileByOffset(files[:len(files)/2], msgOffset)
	}
	return findReadingFileByOffset(files[len(files)/2:], msgOffset)
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
//write batch
//batchStartOffset=lastOffset+1
func (df *DiskFile) write(batchStartOffset int64, msgs []*message.Message) error {

	dataFileSize := atomic.LoadInt64(&df.size)

	dataRef, err := syscall.Mmap(int(df.dataFile.Fd()), dataFileSize+1, int(DiskFileSizeLimit-dataFileSize), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	var cursor int64 = 0
	for i, msg := range msgs {
		byt, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		copy(dataRef[cursor:], byt)
		cursor += int64(len(byt))

		_, err = df.indexFile.Write(encodeIndex(batchStartOffset+int64(i), dataFileSize+cursor))
		if err != nil {
			return err
		}
	}

	err = syscall.Munmap(dataRef)
	if err != nil {
		return err
	}

	if dataFileSize == 0 {
		atomic.StoreInt64(&df.startOffset, batchStartOffset)
	}
	atomic.StoreInt64(&df.size, dataFileSize+cursor)

	atomic.StoreInt64(&df.endOffset, batchStartOffset+int64(len(msgs))-1)
	return nil
}

func (df *DiskFile) read(msgOffset int64, batchCount int) ([]byte, error) {
	startIndexPosition := (msgOffset-df.getStartOffset())*EachIndexLen + 1
	endIndexPosition := (msgOffset-df.getStartOffset()+int64(batchCount))*EachIndexLen + 1
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
