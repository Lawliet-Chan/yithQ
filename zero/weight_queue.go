package zero

import (
	"sort"
	"sync"
	"yithQ/meta"
)

type WeightQueue struct {
	sync.RWMutex
	nodeWeights NodeWeights
	topicNode   map[meta.TopicMetadata]string
}

type NodeWeights []*NodeWeight

type NodeWeight struct {
	Node   string
	Weight int
}

func (nws NodeWeights) Swap(i, j int) {
	nws[i], nws[j] = nws[j], nws[i]
}

func (nws NodeWeights) Len() int {
	return len(nws)
}

func (nws NodeWeights) Less(i, j int) bool {
	return nws[i].Weight < nws[j].Weight
}

func NewWeightQueue() *WeightQueue {
	return &WeightQueue{
		nodeWeights: make([]*NodeWeight, 0),
		topicNode:   make(map[meta.TopicMetadata]string),
	}
}

func (wq *WeightQueue) Put(node string, topic meta.TopicMetadata) {
	wq.Lock()
	defer wq.Unlock()
	wq.topicNode[topic] = node
	exists := false
	for _, nw := range wq.nodeWeights {
		if nw.Node == node {
			nw.Weight++
			exists = true
		}
	}
	if !exists {
		wq.nodeWeights = append(wq.nodeWeights, &NodeWeight{
			Node:   node,
			Weight: 0,
		})
	}
	sort.Sort(wq.nodeWeights)
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
	nws := wq.nodeWeights[:count]
	nodes := make([]string, count)
	for i, nw := range nws {
		nodes[i] = nw.Node
	}
	return nodes
}

/*
func (wq *WeightQueue) DescPopNodes(count int) []string {

}
*/
