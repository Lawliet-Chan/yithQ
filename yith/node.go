package yith

import (
	"sync"
	"yithQ/message"
)

type Node struct {
	IP             string
	topicPartition *sync.Map //map[TopicPartitionInfo]*Partition , key is topic+partitionID
	//partitionReplica *sync.Map
}

type TopicPartitionInfo struct {
	Topic       string
	PartitionID int
}

func NewNode(ip string) *Node {
	return &Node{
		IP:             ip,
		topicPartition: &sync.Map{},
		//partitionReplica:&sync.Map{},
	}
}

func (n *Node) AddTopicPartition(topic string, partitionID int, isReplica bool) {
	n.topicPartition.Store(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	}, NewPartition(partitionID, topic, isReplica))
}

func (n *Node) Produce(topic string, partitionID int, msgs []*message.Message) error {
	partition, _ := n.topicPartition.Load(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	})
	return partition.(*Partition).Produce(msgs)
}

func (n *Node) Consume(topic string, popOffset int64) ([]*message.Message, error) {
	partition, _ := n.topicPartition.Load(topic)
	return partition.(*Partition).Consume(popOffset)
}

func (n *Node) DeleteTopicPartition(topic string, partitionID int) {
	n.topicPartition.Delete(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	})
}

func (n *Node) ExistTopic(topic string) bool {

}

func (n *Node) ExistTopicPartition(topic string, partitionID int) bool {
	_, exist := n.topicPartition.Load(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	})
	return exist
}
