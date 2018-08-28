package yith

import (
	"strconv"
	"sync"
	"yithQ/message"
)

type Node struct {
	IP             string
	topicPartition *sync.Map //map[string]*Partition , key is topic+partitionID
	//partitionReplica *sync.Map
}

func NewNode(ip string) *Node {
	return &Node{
		IP:             ip,
		topicPartition: &sync.Map{},
		//partitionReplica:&sync.Map{},
	}
}

func (n *Node) AddTopicPartition(topic string, partitionID int, isReplica bool) {
	n.topicPartition.Store(topic+"-"+strconv.Itoa(partitionID), NewPartition(partitionID, topic, isReplica))
}

func (n *Node) Produce(topic string, partitionID int, msgs []*message.Message) error {
	partition, _ := n.topicPartition.Load(topic + "-" + strconv.Itoa(partitionID))
	return partition.(*Partition).Produce(msgs)
}

func (n *Node) Consume(topic string, popOffset int64) ([]*message.Message, error) {
	partition, _ := n.topicPartition.Load(topic)
	return partition.(*Partition).Consume(popOffset)
}

func (n *Node) DeleteTopicPartition(topic string, partitionID int) {
	n.topicPartition.Delete(topic + "-" + strconv.Itoa(partitionID))
}

func (n *Node) ExistTopicPartition(topic string, partitionID int) bool {
	_, exist := n.topicPartition.Load(topic + "-" + strconv.Itoa(partitionID))
	return exist
}
