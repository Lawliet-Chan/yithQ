package zero

import (
	"github.com/CrocdileChan/yapool"
	"sync"
	"yithQ/meta"
)

type Zero struct {
	sync.Mutex
	yithNodes []string
	//metadata    *meta.Metadata
	weightQueue *WeightQueue
	center      *yapool.Center
	cfg         *Config
}

func NewZero(cfg *Config) *Zero {
	return &Zero{
		yithNodes: make([]string, 0),
		//metadata:    meta.NewMetadata(),
		weightQueue: NewWeightQueue(),
		center:      yapool.NewCenter(cfg.ListenPort),
		cfg:         cfg,
	}
}

func (z *Zero) Run() {
	go z.ListenYith()

}

func (z *Zero) ListenYith() {
	go z.center.ReceiveWithFunc(func(remoteAddr string, msg *yapool.Msg) {
		switch msg.Level {
		case meta.TopicAddChange:
			z.AddTopic(remoteAddr, msg.Msg.(meta.TopicMetadata))
		case meta.TopicDeleteChange:
			z.DeleteTopic(remoteAddr, msg.Msg.(meta.TopicMetadata))
		case meta.NodeChange:
			z.yithNodes = append(z.yithNodes, remoteAddr)
		}
	},
		z.cfg.HeartbeatTimeout,
		nil,
		z.yithNodeExpire)

}

func (z *Zero) NortifyAllYith() {

}

func (z *Zero) AddTopic(yithNode string, topic meta.TopicMetadata) {
	nodes := z.weightQueue.PopNodesWithout(topic.ReplicaFactory,yithNode)
	for i, node := range nodes {
		z.weightQueue.Put(node, meta.TopicMetadata{
			Topic:          topic.Topic,
			PartitionID:    topic.PartitionID*100 + i,
			IsReplica:      true,
			ReplicaFactory: topic.ReplicaFactory,
		})
	}
	//z.metadata.SetTopic(yithNode, topic)
}

func (z *Zero) DeleteTopic(yithNode string, topic meta.TopicMetadata) {
	//z.metadata.RemoveTopic(yithNode, topic)
}

func (z *Zero) yithNodeExpire(yithAddr string) {
	z.weightQueue.DeleteNode(yithAddr)
}
