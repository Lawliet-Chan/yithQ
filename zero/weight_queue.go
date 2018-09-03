package zero

import (
	"sync"
	"yithQ/meta"
)

type WeightQueue struct {
	sync.RWMutex
	nodeWeights map[string]*NodeWeight
	//nodeWeight map[string]int
	topicNode map[meta.TopicMetadata]string
}

type NodeWeight struct {
	Node   string
	Weight int
}

func NewWeightQueue() *WeightQueue {
	return &WeightQueue{
		//sortedWeights: make([]int, 0),
		nodeWeights: make(map[string]*NodeWeight),
		topicNode:   make(map[meta.TopicMetadata]string),
	}
}

func (wq *WeightQueue) Put(node string, topic meta.TopicMetadata) {
	wq.Lock()
	defer wq.Unlock()
	wq.topicNode[topic] = node
	if nw, ok := wq.nodeWeights[node]; ok {
		nw.Weight++
	} else {
		wq.nodeWeights[node] = &NodeWeight{
			Node:   node,
			Weight: 0,
		}
	}
}

func (wq *WeightQueue) GetNode(topic meta.TopicMetadata) string {
	wq.RLock()
	wq.RUnlock()
	return wq.topicNode[topic]
}

//升序pop，从weight最小的node开始pop
func (wq *WeightQueue) PopNodes(count int) []string {
	wq.RLock()
	defer wq.RUnlock()
	//nodes:=make([]string,count)
	//for node, nw := range wq.nodeWeights {

	//}
}

func (wq *WeightQueue) DescPopNodes(count int) []string {

}
