package meta

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

type Metadata struct {
	TopicNodeMap *sync.Map // map[TopicMetadata]NodeIP
	Version      uint32
}

type JsonMetadata struct {
	TopicNodeMap map[TopicMetadata]string `json:"topic_node_map"`
	Version      uint32                   `json:"version"`
}

func NewMetadata() *Metadata {
	return &Metadata{
		TopicNodeMap: &sync.Map{},
		Version:      0,
	}
}

func (m *Metadata) Unmarshal(data []byte) error {
	var jmd *JsonMetadata
	err := json.Unmarshal(data, jmd)
	if err != nil {
		return err
	}
	m.Version = jmd.Version
	for k, v := range jmd.TopicNodeMap {
		m.TopicNodeMap.Store(k, v)
	}
	return nil
}

func (m *Metadata) Marshal() ([]byte, error) {

}

func (m *Metadata) SetTopic(node string, metadata TopicMetadata) {
	m.TopicNodeMap.Store(metadata, node)
}

func (m *Metadata) RemoveNode(node string) {

}

func (m *Metadata) RemoveTopic(node string, metadata TopicMetadata) {
	m.TopicNodeMap.Delete(metadata)
}

func (m *Metadata) FindNode(topic string) string {

}

func (m *Metadata) FindReplicaNodes(topic string) []string {
	nodes := make([]string, 0)
	m.TopicNodeMap.Range(func(tmi, node interface{}) bool {
		tm := tmi.(TopicMetadata)
		if tm.Topic == topic && tm.IsReplica {
			nodes = append(nodes, node.(string))
		}

		return true
	})
	return nodes
}

func (m *Metadata) FindNodeWithPartition(topic string, partition int, isReplica bool) string {

}

func (m *Metadata) FindPatitionID(topic, nodeIP string, isReplica bool) (parititionID int) {
	m.TopicNodeMap.Range(func(tm, node interface{}) bool {
		if tm.(TopicMetadata).Topic == topic && node.(string) == nodeIP && isReplica == tm.(TopicMetadata).IsReplica {
			parititionID = tm.(TopicMetadata).PartitionID
			return false
		}
		return true
	})
	return
}

func (m *Metadata) Range(f func(key, value interface{}) bool) {
	m.TopicNodeMap.Range(f)
}

func (m *Metadata) GetVersion() uint32 {
	return atomic.LoadUint32(&m.Version)
}

func (m *Metadata) UpgradeVersion() {
	atomic.AddUint32(&m.Version, 1)
}

type TopicMetadata struct {
	Topic          string
	PartitionID    int
	IsReplica      bool
	ReplicaFactory int
}
