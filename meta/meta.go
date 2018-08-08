package meta

import (
	"sync/atomic"
	"sync"
)

type Metadata struct {
	topicNodeMap *sync.Map	// map[*TopicMetadata]*Node
	version uint32
}

func NewMetadata() *Metadata {
	return &Metadata{
		topicNodeMap:&sync.Map{},
		version:0,
	}
}

func (m *Metadata) Set(node,topic string,partition int,isRplica bool)  {

}

func (m *Metadata) RemoveNode(node string) {

}

func (m *Metadata) RemoveTopic(topic string) {

}

func (m *Metadata) RemoveTopicPartition(topic string,partition int) {

}

func (m *Metadata) FindNode(topic string,isReplica bool) string{

}

func (m *Metadata) FindNodeWithPartition(topic string,partition int,isReplica bool) string{

}

func (m *Metadata) Version() uint32{
	return atomic.LoadUint32(&m.version)
}

func (m *Metadata) UpdateVersion() {
	atomic.AddUint32(&m.version,1)
}

type TopicMetadata struct {
	Topic  string
	Partition int
	IsReplica bool
}
