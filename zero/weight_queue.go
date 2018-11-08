package zero

import (
	"sort"
	"sync"
	"yithQ/meta"
)

type WeightQueue struct {
	sync.RWMutex
	nodeWeight map[string]int
	topicNode  map[meta.TopicMetadata]string
}

func NewWeightQueue() *WeightQueue {
	return &WeightQueue{
		nodeWeight: make(map[string]int),
		topicNode:  make(map[meta.TopicMetadata]string),
	}
}

func (wq *WeightQueue) Put(node string, topic meta.TopicMetadata) {
	wq.Lock()
	defer wq.Unlock()
	wq.topicNode[topic] = node
	wq.nodeWeight[node]++
}

func (wq *WeightQueue) AddNode(node string) {
	wq.Lock()
	defer wq.Unlock()
	wq.nodeWeight[node] = 0
}

func (wq *WeightQueue) GetNode(topic meta.TopicMetadata) string {
	wq.RLock()
	wq.RUnlock()
	return wq.topicNode[topic]
}

//升序pop，从weight最小的node开始pop
func (wq *WeightQueue) PopNodes(amount int) []string {
	return wq.PopNodesWithout(amount, "")
}

func (wq *WeightQueue) PopNodesWithout(amount int, withoutNode string) []string {
	wq.RLock()
	defer wq.RUnlock()
	nodes := make([]string, 0)
	if amount == 0 || len(wq.nodeWeight) == 0 {
		return nodes
	}
	if amount >= len(wq.nodeWeight) {
		for node, _ := range wq.nodeWeight {
			if node != withoutNode {
				nodes = append(nodes, node)
			}
		}
		return nodes
	}

	sortWeightNode := make(map[int][]string)
	weights := make([]int, 0)
	for node, weight := range wq.nodeWeight {
		if node != withoutNode {
			weights = append(weights, weight)
			sortWeightNode[weight] = append(sortWeightNode[weight], node)
		}
	}
	sort.Ints(weights)
	for _, wgh := range weights {
		nodes = append(nodes, sortWeightNode[wgh]...)
	}

	return nodes[:amount]
}

func (wq *WeightQueue) DeleteNode(nodeName string) {
	wq.Lock()
	defer wq.Unlock()
	for tmd, node := range wq.topicNode {
		if node == nodeName {
			delete(wq.topicNode, tmd)
		}
	}
	delete(wq.nodeWeight, nodeName)
}

func (wq *WeightQueue) DeleteTopicPartition(tm meta.TopicMetadata) {
	wq.Lock()
	defer wq.Unlock()
	if node, ok := wq.topicNode[tm]; ok {
		delete(wq.topicNode, tm)
		wq.nodeWeight[node]--
	}
}

func (wq *WeightQueue) TopicNode() map[meta.TopicMetadata]string {
	wq.RLock()
	defer wq.RUnlock()
	return wq.topicNode
}

func (wq *WeightQueue) AllNodes() []string {
	wq.RLock()
	defer wq.RUnlock()
	nodes := make([]string, 0)
	for node, _ := range wq.nodeWeight {
		nodes = append(nodes, node)
	}
	return nodes
}

/*for i, nw := range wq.nodeWeights {
	if nw.Node == nodeName {
		if i == wq.nodeWeights.Len()-1 {
			wq.nodeWeights = wq.nodeWeights[:i]
		} else {
			wq.nodeWeights = append(wq.nodeWeights[:i], wq.nodeWeights[i+1:]...)
		}
	}
}*/

/*
func (wq *WeightQueue) DescPopNodes(count int) []string {

}
*/
