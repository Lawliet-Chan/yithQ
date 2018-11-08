package zero

import (
	"testing"
	"yithQ/meta"
)

func TestWeightQueue_AddNode(t *testing.T) {
	wq := NewWeightQueue()
	wq.AddNode("http://localhost:7777")
	for node, wgh := range wq.nodeWeight {
		t.Logf("ADD node is %s ,weight is %d \n", node, wgh)
	}
	for topicmeta, _ := range wq.topicNode {
		t.Logf("ADD topic is %s, partitionID is %d \n", topicmeta.Topic, topicmeta.PartitionID)
	}
}

func TestWeightQueue_PopNodes(t *testing.T) {
	wq := NewWeightQueue()
	wq.Put("http://localhost:7777", meta.TopicMetadata{
		Topic:       "test_queue_pop1",
		PartitionID: 1,
		IsReplica:   false,
	})
	wq.Put("http://localhost:7777", meta.TopicMetadata{
		Topic:       "test_queue_pop2",
		PartitionID: 1,
		IsReplica:   false,
	})
	wq.Put("http://localhost:8888", meta.TopicMetadata{
		Topic:       "test_queue_pop",
		PartitionID: 1,
		IsReplica:   false,
	})

	nodes := wq.PopNodes(1)
	for _, node := range nodes {
		t.Logf("Pop_Nodes is %s \n", node)
	}
}

func TestWeightQueue_PopNodesWithout(t *testing.T) {
	wq := NewWeightQueue()
	wq.Put("http://localhost:7777", meta.TopicMetadata{
		Topic:       "test_queue_pop_without1",
		PartitionID: 1,
		IsReplica:   false,
	})
	wq.Put("http://localhost:7777", meta.TopicMetadata{
		Topic:       "test_queue_pop_without2",
		PartitionID: 1,
		IsReplica:   false,
	})
	wq.Put("http://localhost:8888", meta.TopicMetadata{
		Topic:       "test_queue_pop_without",
		PartitionID: 1,
		IsReplica:   false,
	})
	nodes := wq.PopNodesWithout(2, "http://localhost:7777")
	for _, node := range nodes {
		t.Logf("Pop_Nodes_Without is %s \n", node)
	}
}

func TestWeightQueue_Put(t *testing.T) {
	wq := NewWeightQueue()
	wq.Put("http://localhost:7777", meta.TopicMetadata{
		Topic:       "test_queue_put",
		PartitionID: 1,
		IsReplica:   false,
	})
	for node, wgh := range wq.nodeWeight {
		t.Logf("PUT node is %s ,weight is %d \n", node, wgh)
	}
	for topicmeta, _ := range wq.topicNode {
		t.Logf("PUT topicmeta topic is %s,partitionID is %d \n", topicmeta.Topic, topicmeta.PartitionID)
	}
}

func TestWeightQueue_TopicNode(t *testing.T) {
	wq := NewWeightQueue()
	wq.Put("http://localhost:7777", meta.TopicMetadata{
		Topic:       "test_queue_topic_node",
		PartitionID: 1,
		IsReplica:   false,
	})
	wq.Put("http://localhost:8888", meta.TopicMetadata{
		Topic:       "test_queue_topic_node",
		PartitionID: 1,
		IsReplica:   false,
	})
	tns := wq.TopicNode()
	for topicmeta, node := range tns {
		t.Logf("TOPIC_NODE node is %s,topic is %s,paritionID is %d \n", node, topicmeta.Topic, topicmeta.PartitionID)
	}
}

func TestWeightQueue_GetNode(t *testing.T) {
	wq := NewWeightQueue()
	tm := meta.TopicMetadata{
		Topic:       "test_queue_get_node",
		PartitionID: 1,
		IsReplica:   false,
	}
	wq.Put("http://localhost:7777", tm)
	t.Logf("GET_NODE node is %s \n", wq.GetNode(tm))
}

func TestWeightQueue_DeleteNode(t *testing.T) {
	wq := NewWeightQueue()
	wq.AddNode("http://localhost:7777")
	wq.DeleteNode("http://localhost:7777")
	for node, wgh := range wq.nodeWeight {
		t.Logf("DELETE_NODE node is %s ,weight is %d \n", node, wgh)
	}
	for topicmeta, _ := range wq.topicNode {
		t.Logf("DELEET_NODE topic is %s , partitionID is %d \n", topicmeta.Topic, topicmeta.PartitionID)
	}
}

func TestWeightQueue_DeleteTopicPartition(t *testing.T) {
	wq := NewWeightQueue()
	tm := meta.TopicMetadata{
		Topic:       "test_queue_delete_topic_partition",
		PartitionID: 1,
		IsReplica:   false,
	}
	wq.Put("http://localhost:7777", tm)
	wq.DeleteTopicPartition(tm)
	for node, wgh := range wq.nodeWeight {
		t.Logf("DELETE_topic_partition node is %s ,weight is %d \n", node, wgh)
	}
	for topicmeta, _ := range wq.topicNode {
		t.Logf("DELEET_topic_partition topic is %s ,partitionID is %d \n", topicmeta.Topic, topicmeta.PartitionID)
	}
}

func TestWeightQueue_AllNodes(t *testing.T) {
	wq := NewWeightQueue()
	wq.AddNode("http://localhost:7777")
	wq.AddNode("http://localhost:9999")
	nodes := wq.AllNodes()
	for _, node := range nodes {
		t.Logf("ALL_NODES is %s", node)
	}
}
