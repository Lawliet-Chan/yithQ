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

func (nws NodeWeights) AddNode(node string) {
	exist := false
	for _, nw := range nws {
		if nw.Node == node {
			exist = true
			break
		}
	}
	if !exist {
		nws = append(nws, &NodeWeight{
			Node:   node,
			Weight: 0,
		})
	}

}

func (nws NodeWeights) DeleteNode(nodeName string) {
	for i, nw := range nws {
		if nw.Node == nodeName {
			if i == nws.Len()-1 {
				nws = nws[:i]
			} else {
				nws = append(nws[:i], nws[i+1:]...)
			}
		}
	}
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
	//exists := false
	for _, nw := range wq.nodeWeights {
		if nw.Node == node {
			nw.Weight++
			//exists = true
		}
	}
	/*if !exists {
		wq.nodeWeights = append(wq.nodeWeights, &NodeWeight{
			Node:   node,
			Weight: 0,
		})
	}*/
	sort.Sort(wq.nodeWeights)
}

func (wq *WeightQueue) AddNode(node string) {
	wq.Lock()
	defer wq.Unlock()
	wq.nodeWeights.AddNode(node)
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

func (wq *WeightQueue) PopNodesWithout(count int, withoutNode string) []string {
	wq.RLock()
	defer wq.RUnlock()
	nws := wq.nodeWeights[:count+1]
	nodes := make([]string, 0)
	for _, nw := range nws {
		if nw.Node == withoutNode {
			continue
		}
		nodes = append(nodes, nw.Node)
	}
	return nodes[:count]
}

func (wq *WeightQueue) DeleteNode(nodeName string) {
	wq.Lock()
	defer wq.Unlock()
	for tmd, node := range wq.topicNode {
		if node == nodeName {
			delete(wq.topicNode, tmd)
		}
	}
	wq.nodeWeights.DeleteNode(nodeName)

}

func (wq *WeightQueue) TopicNode() map[meta.TopicMetadata]string {
	wq.RLock()
	wq.RUnlock()
	return wq.topicNode
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
