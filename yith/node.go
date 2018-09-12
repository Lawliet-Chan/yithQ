package yith

import (
	"net/http"
	"sync"
	"yithQ/message"
)

type Node struct {
	IP                string
	topicPartition    *sync.Map //map[TopicPartitionInfo]*Partition
	partitionID2Topic *sync.Map //map[int]string
}

type TopicPartitionInfo struct {
	Topic       string
	PartitionID int
}

func NewNode(ip string) *Node {
	return &Node{
		IP:                ip,
		topicPartition:    &sync.Map{},
		partitionID2Topic: &sync.Map{},
	}
}

func (n *Node) AddTopicPartition(topic string, partitionID int, isReplica bool) {
	n.topicPartition.Store(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	}, NewPartition(partitionID, topic, isReplica))
	n.partitionID2Topic.Store(partitionID, topic)
}

func (n *Node) ProduceTopic(topic string, msgs []*message.Message) (err error) {
	n.partitionID2Topic.Range(func(id, topicI interface{}) bool {
		if topicI.(string) == topic {
			err = n.ProduceTopicPartition(topic, id.(int), msgs)
			return false
		}
		return true
	})
	return
}

func (n *Node) ProduceTopicPartition(topic string, partitionID int, msgs []*message.Message) error {
	partition, _ := n.topicPartition.Load(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	})
	return partition.(*Partition).Produce(msgs)
}

func (n *Node) Consume(topic string, popOffset int64, writer http.ResponseWriter) error {
	partition, _ := n.topicPartition.Load(topic)
	return partition.(*Partition).Consume(popOffset, writer)
}

func (n *Node) DeleteTopicPartition(topic string, partitionID int) {
	n.topicPartition.Delete(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	})
	n.partitionID2Topic.Delete(partitionID)
}

func (n *Node) ExistTopic(topic string) bool {
	exist := false
	n.partitionID2Topic.Range(func(id, topicI interface{}) bool {
		if topicI.(string) == topic {
			exist = true
			return false
		}
		return true
	})
	return exist
}

func (n *Node) ExistTopicPartition(topic string, partitionID int) bool {
	_, exist := n.topicPartition.Load(TopicPartitionInfo{
		Topic:       topic,
		PartitionID: partitionID,
	})
	return exist
}
