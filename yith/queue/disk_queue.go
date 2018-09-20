package queue

import (
	"bytes"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"yithQ/message"
)

type DiskQueue interface {
	FillToDisk(msg []*message.Message) error
	PopFromDisk(popOffset int64) ([]byte, error)
}

type diskQueue struct {
	fileNamePrefix string
	writingFile    *DiskFile
	readingFile    *DiskFile
	storeFiles     atomic.Value //type is  []*DiskFile
	lastOffset     int64
}

func NewDiskQueue(topicPartitionInfo string) (DiskQueue, error) {
	fis, err := ioutil.ReadDir("./")
	if err != nil {
		return nil, err
	}
	seqArr := make([]int, 0)
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		if strings.Contains(fi.Name(), topicPartitionInfo) && strings.Contains(fi.Name(), ".data") {
			fileNameArr := strings.Split(strings.TrimSuffix(fi.Name(), ".data"), "_")
			seq, err := strconv.Atoi(fileNameArr[len(fileNameArr)-1])
			if err != nil {
				return nil, err
			}
			seqArr = append(seqArr, seq)
		}
	}
	sort.Ints(seqArr)
	storeFiles := make([]*DiskFile, 0)
	for _, seqNum := range seqArr {
		diskFile, err := newDiskFile(topicPartitionInfo, seqNum, true)
		if err != nil {
			return nil, err
		}
		storeFiles = append(storeFiles, diskFile)
	}
	lastOffset := storeFiles[len(storeFiles)-1].endOffset
	writingFile, err := newDiskFile(topicPartitionInfo, seqArr[len(seqArr)-1]+1, false)
	if err != nil {
		return nil, err
	}
	storeFiles = append(storeFiles, writingFile)
	return &diskQueue{
		fileNamePrefix: topicPartitionInfo,
		writingFile:    writingFile,
		storeFiles:     atomic.Value{storeFiles},
		lastOffset:     lastOffset,
	}, nil
}

func (dq *diskQueue) FillToDisk(msgs []*message.Message) error {
	if dq.writingFile == nil {
		storeFiles := dq.storeFiles.Load().([]*DiskFile)
		dq.writingFile = storeFiles[len(storeFiles)-1]
	}

	overflowIndex, err := dq.writingFile.write(dq.getLastOffset()+1, msgs)
	if err != nil {
		return err
	}
	if overflowIndex >= 0 {
		newSeq := dq.writingFile.seq + 1
		dq.writingFile, err = newDiskFile(dq.fileNamePrefix, newSeq, false)
		if err != nil {
			return err
		}
		storeFiles := dq.storeFiles.Load().([]*DiskFile)
		dq.storeFiles.Store(append(storeFiles, dq.writingFile))
		return dq.FillToDisk(msgs[overflowIndex:])
	}

	dq.UpLastOffset(int64(len(msgs)))

	return nil
}

func (dq *diskQueue) PopFromDisk(msgOffset int64) ([]byte, error) {
	if dq.readingFile == nil {
		dq.readingFile = findReadingFileByOffset(dq.storeFiles.Load().([]*DiskFile), msgOffset)
	}
	if dq.readingFile.getStartOffset() <= msgOffset && dq.readingFile.getEndOffset() >= msgOffset {
		dq.readingFile = findReadingFileByOffset(dq.storeFiles.Load().([]*DiskFile), msgOffset)
	}

	data, err := dq.readingFile.read(msgOffset, 20)
	if err != nil {
		if err == io.EOF && msgOffset <= dq.getLastOffset() {
			dq.readingFile = nil
			return dq.PopFromDisk(msgOffset)
		}
		return nil, err
	}
	return data, nil
}

func (dq *diskQueue) getLastOffset() int64 {
	return atomic.LoadInt64(&dq.lastOffset)
}

func (dq *diskQueue) UpLastOffset(delta int64) int64 {
	return atomic.AddInt64(&dq.lastOffset, delta)
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

var ErrMsgTooLarge error = errors.New("message too large")

type DiskFile struct {
	startOffset int64
	endOffset   int64
	indexFile   *os.File
	dataFile    *os.File
	size        int64
	//Diskfile的编号，diskfile命名规则：topicPartition+seq
	seq    int
	isFull bool
}

func newDiskFile(name string, seq int, isFull bool) (*DiskFile, error) {
	dataf, err := os.OpenFile(name+"_"+strconv.Itoa(seq)+".data", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	indexf, err := os.OpenFile(name+"_"+strconv.Itoa(seq)+".index", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	dataFi, err := dataf.Stat()
	if err != nil {
		return nil, err
	}
	if dataFi.Size() < DiskFileSizeLimit {
		_, err = dataf.WriteAt([]byte(" "), DiskFileSizeLimit-1)
		if err != nil {
			return nil, err
		}
	}

	dataFileSize, err := dataFileSize(dataf)
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
		size:        dataFileSize,
		indexFile:   indexf,
		dataFile:    dataf,
		seq:         seq,
		isFull:      isFull,
	}, nil
}

/*
func recoverDiskFile(dataFileName string) (*DiskFile, error) {
	dataf,err:=os.Open(dataFileName)
	if err != nil {
		return nil, err
	}
	indexf,err:=os.Open(strings.TrimSuffix(dataFileName,".data")+".index")
	if err != nil {
		return nil,err
	}

}
*/
//TODO: will use mmap() to store data into file next version.
//write batch
//batchStartOffset=lastOffset+1
func (df *DiskFile) write(batchStartOffset int64, msgs []*message.Message) (int, error) {

	dataFileSize := atomic.LoadInt64(&df.size)

	dataRef, err := syscall.Mmap(int(df.dataFile.Fd()), dataFileSize, int(DiskFileSizeLimit-dataFileSize), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return -1, err
	}

	var cursor int64 = 0
	for i, msg := range msgs {
		byt, err := json.Marshal(msg)
		if err != nil {
			return -1, err
		}

		if len(byt) > DiskFileSizeLimit {
			return -1, ErrMsgTooLarge
		}

		if len(dataRef[cursor:]) < len(byt) {
			df.isFull = true
			return i, syscall.Munmap(dataRef)
		}

		copy(dataRef[cursor:], byt)
		cursor += int64(len(byt))

		_, err = df.indexFile.Write(encodeIndex(batchStartOffset+int64(i), dataFileSize+cursor))
		if err != nil {
			return -1, err
		}
	}

	err = syscall.Munmap(dataRef)
	if err != nil {
		return -1, err
	}

	if dataFileSize == 0 {
		atomic.StoreInt64(&df.startOffset, batchStartOffset)
	}
	atomic.StoreInt64(&df.size, dataFileSize+cursor)

	atomic.StoreInt64(&df.endOffset, batchStartOffset+int64(len(msgs))-1)
	return -1, nil
}

func (df *DiskFile) read(msgOffset int64, batchCount int) ([]byte, error) {
	startIndexPosition := (msgOffset - df.getStartOffset()) * EachIndexLen

	var endIndexPosition int64
	if msgOffset+int64(batchCount)-1 < df.getEndOffset() {
		endIndexPosition = (msgOffset - df.getStartOffset() + int64(batchCount)) * EachIndexLen
	}
	endIndexPosition = (df.getEndOffset() - df.getStartOffset()) * EachIndexLen
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

func dataFileSize(f *os.File) (int64, error) {
	data, err := syscall.Mmap(int(f.Fd()), 0, DiskFileSizeLimit, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return 0, err
	}
	realData := bytes.TrimRight(data, " ")
	return int64(len(realData)), nil
}
