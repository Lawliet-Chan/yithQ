package meta

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
)

type Metadata struct {
	TopicNodeMap *sync.Map // map[TopicMetadata]NodeIP
	Version      uint32
}

type GobMetadata struct {
	TopicNodeMap map[TopicMetadata]string `gob:"topic_node_map"`
	Version      uint32                   `gob:"version"`
}

func NewMetadata() *Metadata {
	return &Metadata{
		TopicNodeMap: &sync.Map{},
		Version:      0,
	}
}

func (m *Metadata) Unmarshal(data []byte) error {
	var gmd GobMetadata

	err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&gmd)
	if err != nil {
		return err
	}
	m.Version = gmd.Version
	for k, v := range gmd.TopicNodeMap {
		m.TopicNodeMap.Store(k, v)
	}
	return nil
}

func (m *Metadata) Marshal(tnm map[TopicMetadata]string, version uint32) ([]byte, error) {
	var data bytes.Buffer
	err := gob.NewEncoder(&data).Encode(GobMetadata{
		TopicNodeMap: tnm,
		Version:      version,
	})
	return data.Bytes(), err
}

func (m *Metadata) SetTopic(node string, metadata TopicMetadata) {
	m.TopicNodeMap.Store(metadata, node)
}

func (m *Metadata) RemoveNode(node string) {

}

func (m *Metadata) RemoveTopic(node string, metadata TopicMetadata) {
	m.TopicNodeMap.Delete(metadata)
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

func (m *Metadata) FindTopicAllPartitions(topic string) map[string][]TopicMetadata {
	nodeTopics := make(map[string][]TopicMetadata)
	m.TopicNodeMap.Range(func(tmi, node interface{}) bool {
		tm := tmi.(TopicMetadata)
		if tm.Topic == topic && !tm.IsReplica {
			nodeTopics[node.(string)] = append(nodeTopics[node.(string)], tm)
		}
		return true
	})
	return nodeTopics
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

func (m *Metadata) FindNodeWithTopicPartition(topic string, partitionID int, isReplica bool) (node string) {
	m.TopicNodeMap.Range(func(tmi, nodei interface{}) bool {
		tm := tmi.(TopicMetadata)
		if tm.Topic == topic && tm.PartitionID == partitionID && tm.IsReplica == isReplica {
			node = nodei.(string)
			return false
		}
		return true
	})
	return
}

func (m *Metadata) GetVersion() uint32 {
	return atomic.LoadUint32(&m.Version)
}

func (m *Metadata) UpgradeVersion() {
	atomic.AddUint32(&m.Version, 1)
}

type TopicMetadata struct {
	Topic          string `json:"topic"`
	PartitionID    int    `json:"partition_id"`
	Size           int64  `json:"size"`
	IsReplica      bool   `json:"is_replica"`
	ReplicaFactory int    `json:"replica_factory"`
}
