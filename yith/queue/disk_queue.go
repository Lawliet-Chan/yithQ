package queue

import (
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
	"unsafe"
	"yithQ/message"
	"yithQ/meta"
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
	lastFileSeq    int
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
	var lastOffset int64
	if len(storeFiles) == 0 {
		lastOffset = 0
	} else {
		lastOffset = storeFiles[len(storeFiles)-1].endOffset
	}
	var lastSeq int
	if len(seqArr) == 0 {
		lastSeq = 0
	} else {
		lastSeq = seqArr[len(seqArr)-1]
	}
	/*writingFile, err := newDiskFile(topicPartitionInfo, lastSeq+1, false)
	if err != nil {
		return nil, err
	}
	storeFiles = append(storeFiles, writingFile)*/
	dq := &diskQueue{
		fileNamePrefix: topicPartitionInfo,
		//writingFile:    writingFile,
		storeFiles:  atomic.Value{},
		lastOffset:  lastOffset,
		lastFileSeq: lastSeq,
	}
	dq.storeFiles.Store(storeFiles)
	return dq, nil
}

func (dq *diskQueue) FillToDisk(msgs []*message.Message) error {
	if len(dq.storeFiles.Load().([]*DiskFile)) == 0 {
		writingFile, err := newDiskFile(dq.fileNamePrefix, dq.lastFileSeq+1, false)
		if err != nil {
			return err
		}
		dq.lastFileSeq++
		dq.writingFile = writingFile
		dfs := dq.storeFiles.Load().([]*DiskFile)
		dfs = append(dfs, writingFile)
		dq.storeFiles.Store(dfs)
	}
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
	if len(dq.storeFiles.Load().([]*DiskFile)) == 0 || dq.getLastOffset() == 0 {
		return nil, ErrNoneMsg
	}
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
	//fmt.Printf("files length is %d, msg offset is %d   ", len(files), msgOffset)
	midStoreFile := files[len(files)/2]
	//fmt.Printf("startOffset is %d, endOffset is %d", midStoreFile.getStartOffset(), midStoreFile.getEndOffset())
	if midStoreFile.getStartOffset() <= msgOffset && midStoreFile.getEndOffset() >= msgOffset {
		return midStoreFile
	} else if midStoreFile.getStartOffset() > msgOffset {
		return findReadingFileByOffset(files[:len(files)/2], msgOffset)
	}
	return findReadingFileByOffset(files[len(files)/2:], msgOffset)
}

const DiskFileSizeLimit = 1024 * 1024 * 1024
const EachIndexLen = 39

var ErrMsgTooLarge error = errors.New("message too large")
var ErrNoneMsg error = errors.New("none message")

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
	dataf, err := os.OpenFile(name+"_"+strconv.Itoa(seq)+".data", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	indexf, err := os.OpenFile(name+"_"+strconv.Itoa(seq)+".index", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
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

//TODO: will use mmap() to store data into file next version.
//write batch
//batchStartOffset=lastOffset+1
func (df *DiskFile) write(batchStartOffset int64, msgs []*message.Message) (int, error) {

	dataFileSize := atomic.LoadInt64(&df.size)

	dataRef, err := syscall.Mmap(int(df.dataFile.Fd()), dataFileSize, int(DiskFileSizeLimit-dataFileSize), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return -1, err
	}

	err = madvise(dataRef, syscall.MADV_RANDOM)
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

		byt = []byte(string(byt) + ",")
		//var buf bytes.Buffer
		//buf.Write(byt)
		//buf.Write([]byte(`,`))
		//fmt.Println("bytes is ", string(buf.Bytes()))
		copy(dataRef[cursor:], byt)

		_, err = df.indexFile.Write(encodeIndex(batchStartOffset+int64(i), dataFileSize+cursor))
		if err != nil {
			return -1, err
		}
		cursor += int64(len(byt))

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
	var startOffset, endOffset int64
	var err error

	startPositionInIndexFile := (msgOffset - df.getStartOffset()) * EachIndexLen

	startOffset, err = df.getDatafilePosition(startPositionInIndexFile)
	if err != nil {
		return nil, err
	}

	var endPositionInIndexFile int64
	if msgOffset+int64(batchCount)-1 < df.getEndOffset() {
		endPositionInIndexFile = (msgOffset - df.getStartOffset() + int64(batchCount)) * EachIndexLen
		endOffset, err = df.getDatafilePosition(endPositionInIndexFile)
		if err != nil {
			return nil, err
		}
	} else {
		endOffset = atomic.LoadInt64(&df.size)
	}

	dataRef, err := syscall.Mmap(int(df.dataFile.Fd()), startOffset, int(endOffset-startOffset-1), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	err = madvise(dataRef, syscall.MADV_RANDOM)
	if err != nil {
		return nil, err
	}

	return dataRef, nil

}

func (df *DiskFile) getDatafilePosition(positionInIndexFile int64) (offset int64, err error) {
	index := make([]byte, EachIndexLen)
	_, err = df.indexFile.ReadAt(index, positionInIndexFile)
	if err != nil {
		return
	}

	_, offset = decodeIndex(index)
	return
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
	dataPosition, _ = strconv.ParseInt(strings.Trim(dataPositionStr, "\x00"), 10, 64)

	return
}

func dataFileSize(f *os.File) (int64, error) {
	data, err := syscall.Mmap(int(f.Fd()), 0, DiskFileSizeLimit, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return 0, err
	}
	var i int
	for i = 0; i < len(data); i++ {
		if data[i] == 0 {
			break
		}
	}
	//realData := bytes.TrimRight(data, " ")
	return int64(len(data[:i])), nil
}

func PickupTopicInfoFromDisk() ([]meta.TopicMetadata, error) {
	fis, err := ioutil.ReadDir("./")
	if err != nil {
		return nil, err
	}

	topicInfoMap := make(map[string]int64)
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		if strings.Contains(fi.Name(), ".data") {
			file, err := os.Open(fi.Name())
			if err != nil {
				return nil, err
			}
			fileNameArr := strings.Split(strings.TrimSuffix(fi.Name(), ".data"), "_")
			var topicPartition string
			for _, finame := range fileNameArr[:len(fileNameArr)-1] {
				topicPartition += finame
			}
			dataFileSize, err := dataFileSize(file)
			if err != nil {
				return nil, err
			}
			topicInfoMap[topicPartition] += dataFileSize
		}
	}

	topicInfos := make([]meta.TopicMetadata, 0)

	for topicPartition, size := range topicInfoMap {
		tp := strings.Split(topicPartition, "-")
		partitionID, err := strconv.Atoi(tp[len(tp)-1])
		if err != nil {
			return nil, err
		}
		var topic string
		for _, t := range tp[:len(tp)-1] {
			topic += t
		}
		topicInfos = append(topicInfos, meta.TopicMetadata{
			Topic:       topic,
			PartitionID: partitionID,
			Size:        size,
		})
	}
	return topicInfos, nil
}

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
