package meta

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
)

type Metadata struct {
	sync.Mutex
	TopicNodeMap *sync.Map // map[TopicMetadata]NodeIP
	Nodes        *sync.Map
	Version      uint32
}

type GobMetadata struct {
	TopicNodeMap map[TopicMetadata]string `gob:"topic_node_map"`
	Version      uint32                   `gob:"version"`
	Nodes        map[string]bool          `gob:"nodes"`
}

func NewMetadata() *Metadata {
	return &Metadata{
		TopicNodeMap: &sync.Map{},
		Nodes:        &sync.Map{},
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
	for node, _ := range gmd.Nodes {
		m.Nodes.Store(node, true)
	}
	return nil
}

func (m *Metadata) Marshal(tnm map[TopicMetadata]string, nodes []string, version uint32) ([]byte, error) {
	var data bytes.Buffer
	nodeMap := make(map[string]bool)
	for _, node := range nodes {
		nodeMap[node] = true
	}
	err := gob.NewEncoder(&data).Encode(GobMetadata{
		TopicNodeMap: tnm,
		Nodes:        nodeMap,
		Version:      version,
	})
	return data.Bytes(), err
}

func (m *Metadata) SetTopic(node string, metadata TopicMetadata) {
	m.TopicNodeMap.Store(metadata, node)
	m.Nodes.LoadOrStore(node, true)
}

func (m *Metadata) RemoveNode(node string) {

}

func (m *Metadata) RemoveTopic(node string, metadata TopicMetadata) {
	m.TopicNodeMap.Delete(metadata)
}

func (m *Metadata) GetAllNodes() []string {
	nodes := make([]string, 0)
	m.Nodes.Range(func(node, _ interface{}) bool {
		nodes = append(nodes, node.(string))
		return true
	})
	return nodes
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

//map[string]TopicMetadata   key is node
func (m *Metadata) FindTopicAllPartitions(topic string) map[string]TopicMetadata {
	nodeTopic := make(map[string]TopicMetadata)
	m.TopicNodeMap.Range(func(tmi, node interface{}) bool {
		tm := tmi.(TopicMetadata)
		if tm.Topic == topic && !tm.IsReplica {
			nodeTopic[node.(string)] = tm
		}
		return true
	})
	return nodeTopic
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

func (m *Metadata) FindNodeWithTopicPartitionID(topic string, partitionID int, isReplica bool) (node string) {
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

func (m *Metadata) SetMetadata(md *Metadata) {
	m.Lock()
	defer m.Unlock()
	m = md
}

type TopicMetadata struct {
	Topic          string `json:"topic"`
	PartitionID    int    `json:"partition_id"`
	Size           int64  `json:"size"`
	IsReplica      bool   `json:"is_replica"`
	ReplicaFactory int    `json:"replica_factory"`
}
