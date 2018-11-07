package yith

import (
	"net/http"
	"strconv"
	"yithQ/message"
	"yithQ/yith/queue"
)

type Partition struct {
	id        int
	topicName string
	q         *queue.Queue

	//TODO: will use watermark to Increase performance
	watermark uint64

	isRepplica bool
}

func NewPartition(id int, topicName string, isReplica bool /* queueCfg *conf.QueueConf*/) (*Partition, error) {
	//memoryQ := queue.NewMemoryQueue(queueCfg.MemoryQueueConf)
	diskQ, err := queue.NewDiskQueue(topicName + "-" + strconv.Itoa(id))
	if err != nil {
		return nil, err
	}
	queue := queue.NewQueue(nil, diskQ)
	return &Partition{
		id:         id,
		topicName:  topicName,
		q:          queue,
		isRepplica: isReplica,
	}, nil
}

func (p *Partition) Produce(msgs []*message.Message) error {
	return p.q.Fill(msgs)
}

func (p *Partition) Consume(popOffset int64, count int, writer http.ResponseWriter) error {
	return p.q.Pop(popOffset, count, writer)
}
