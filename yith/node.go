package yith

import (
	"sync"
	"yithQ/message"
)

type Node struct {
	IP             string
	topicPartition *sync.Map //map[string]*Partition
	//partitionReplica *sync.Map
}

func NewNode(ip string) *Node {
	return &Node{
		IP:             ip,
		topicPartition: &sync.Map{},
		//partitionReplica:&sync.Map{},
	}
}

func (n *Node) AddTopicPartition(tp map[string]*Partition) {
	for topic, p := range tp {
		n.topicPartition.Store(topic, p)
	}
}

func (n *Node) Produce(topic string, msgs []*message.Message) error {
	partition, loaded := n.topicPartition.Load(topic)
	if loaded {
		return partition.(*Partition).Produce(msgs)
	}
	partition = NewPartition()
	err := partition.(*Partition).Produce(msgs)
	n.topicPartition.Store(topic, partition)
	return err
}

func (n *Node) Consume(topic string, popOffset int64) ([]*message.Message, error) {
	partition, _ := n.topicPartition.Load(topic)
	return partition.(*Partition).Consume(popOffset)
}

func (n *Node) DeleteTopicPartition(topic string) {
	n.topicPartition.Delete(topic)
}
