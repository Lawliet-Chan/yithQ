package yith

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type DiskEngine struct {
	mmapSize        int
	maxMmapFileSize int64
	eFiles          *sync.Map //map[string]*TopicFiles
}

func NewDiskEngine(mmapSize int, maxMmapFileSize int64) *DiskEngine {
	err := os.MkdirAll("yith", 0777)
	if err != nil {
		panic(err)
	}
	return &DiskEngine{
		mmapSize:        mmapSize,
		maxMmapFileSize: maxMmapFileSize,
		eFiles:          &sync.Map{},
	}
}

func (de *DiskEngine) Store(topic string, data []byte) (*Meta, error) {
	topicFiles, loaded := de.eFiles.Load(topic)
	if loaded {
		return topicFiles.(*TopicFiles).Write(data, de.maxMmapFileSize, de.mmapSize)
	}
	newTopicFiles, err := NewTopicFiles(topic, de.mmapSize)
	if err != nil {
		return nil, err
	}
	de.eFiles.Store(topic, newTopicFiles)
	return newTopicFiles.Write(data, de.maxMmapFileSize, de.mmapSize)
}

func (de *DiskEngine) Read(topic string, offset int) ([]byte, error) {
	topicFiles, loaded := de.eFiles.Load(topic)
	if !loaded {
		return nil, &ErrTopicNotFound{topic: topic}
	}
	return topicFiles.(*TopicFiles).Read(meta, de.mmapSize)
}

type TopicFiles struct {
	topic string
	//maxFileSize int64
	metaFile  *os.File
	dataFiles []*mmapFile
}

func NewTopicFiles(topic string, mmapSize int) (*TopicFiles, error) {
	metaFile, err := os.Open("./yith/" + topic + ".meta")
	if err != nil {
		return nil, err
	}

	fileNames, err := filepath.Glob("./yith/" + topic + "-*")
	if err != nil {
		return nil, err
	}
	dataFiles := make([]*mmapFile, 0)
	for _, fileName := range fileNames {
		arr := strings.Split(fileName, "-")
		fileNumStr := arr[len(arr)-1]
		fileNum, err := strconv.Atoi(fileNumStr)
		if err != nil {
			return nil, err
		}
		mmapFile, err := NewMmapFile(topic, fileNum, mmapSize)
		if err != nil {
			return nil, err
		}
		dataFiles = append(dataFiles, mmapFile)
	}

	return &TopicFiles{
		topic: topic,
		//maxFileSize: maxFileSize,
		metaFile:  metaFile,
		dataFiles: dataFiles,
	}, nil
}

func (tf *TopicFiles) Write(data []byte, maxFileSize int64, mmapSize int) (*Meta, error) {
	writeFile := tf.dataFiles[len(tf.dataFiles)-1]
	if int64(len(data))+writeFile.Size() > maxFileSize {
		newWriteFile, err := NewMmapFile(tf.topic, writeFile.fileNum+1, mmapSize)
		if err != nil {
			return nil, err
		}
		tf.dataFiles = append(tf.dataFiles, newWriteFile)
		return newWriteFile.WriteFile(data, mmapSize)
	}
	return writeFile.WriteFile(data, mmapSize)
}

func (tf *TopicFiles) Read(offset int64, mmapSize int) ([]byte, error) {
	//return tf.dataFiles[meta.dataFileNum].ReadFile(meta.start, meta.offset, mmapSize)
}

func (tf *TopicFiles) Size() int64 {
	var size int64 = 0
	for _, df := range tf.dataFiles {
		size += df.Size()
	}
	return size
}

type mmapFile struct {
	fileNum int
	//	mmapSize     int
	file         *os.File
	fileSize     int64
	writeDataref []byte
	readDataref  []byte
}

func NewMmapFile(topic string, fileNum, mmapSize int) (*mmapFile, error) {
	file, err := os.Open("./yith/" + topic + "-" + strconv.Itoa(fileNum))
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	writeByt, err := mmap(file, fi.Size()+1, mmapSize)
	if err != nil {
		return nil, err
	}

	var readByt []byte

	if fi.Size() != 0 {
		readByt, err = mmap(file, 0, int(fi.Size()))
		if err != nil {
			return nil, err
		}
	} else {
		readByt = writeByt
	}

	return &mmapFile{
		//	mmapSize:     mmapSize,
		file:         file,
		fileSize:     fi.Size(),
		readDataref:  readByt,
		writeDataref: writeByt,
	}, nil
}

func (mf *mmapFile) ReadFile(start, offset int64, mmapSize int) ([]byte, error) {

}

func (mf *mmapFile) WriteFile(data []byte, mmapSize int) (*Meta, error) {

	start := len(mf.writeDataref)

	copiedLen := copy(mf.writeDataref[start:], data)
	if len(mf.writeDataref[start:]) < len(data) {

		err := mf.munmap(mf.writeDataref, len(data[copiedLen:]))
		if err != nil {
			return nil, err
		}
		copy(mf.writeDataref, data[copiedLen:])
		err = mf.munmap(mf.writeDataref, mmapSize)
		if err != nil {
			return nil, err
		}

	} else if len(mf.writeDataref[start:]) == len(data) {
		err := mf.munmap(mf.writeDataref, mmapSize)
		if err != nil {
			return nil, err
		}
	}

	offset := len(mf.writeDataref)

	return &Meta{
		dataFileNum: mf.fileNum,
		start:       int64(start) + mf.fileSize - 1,
		offset:      int64(offset) + mf.fileSize - 1,
	}, nil
}

func (mf *mmapFile) Size() int64 {
	return mf.fileSize
}

func (mf *mmapFile) Num() int {
	return mf.fileNum
}

func (mf *mmapFile) munmap(data []byte, mmapSize int) error {
	err := syscall.Munmap(data)
	if err != nil {
		return err
	}
	fi, _ := mf.file.Stat()
	mf.fileSize = fi.Size()
	byt, err := mmap(mf.file, fi.Size(), mmapSize)
	if err != nil {
		return err
	}
	mf.writeDataref = byt
	return nil
}

func mmap(file *os.File, offset int64, size int) ([]byte, error) {
	return syscall.Mmap(int(file.Fd()), offset, size, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
}

type Meta struct {
	dataFileNum int
	start       int64
	offset      int64
}

func (m *Meta) encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint(m.dataFileNum))
	buf.Write([]byte(","))
	binary.Write(&buf, binary.BigEndian, uint64(m.start))
	buf.Write([]byte(","))
	binary.Write(&buf, binary.BigEndian, uint64(m.offset))
	return buf.Bytes()
}

func decode(data []byte) *Meta {
	metaInfo := bytes.Split(data, []byte(","))
	return &Meta{
		dataFileNum: int(binary.BigEndian.Uint16(metaInfo[0])),
		start:       int64(binary.BigEndian.Uint64(metaInfo[1])),
		offset:      int64(binary.BigEndian.Uint64(metaInfo[2])),
	}

}

type ErrTopicNotFound struct {
	topic string
}

func (err *ErrTopicNotFound) Error() string {
	return fmt.Sprintf("Topic(%s) not found", err.topic)
}
