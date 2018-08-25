package yith

import (
	"yithQ/message"
	"yithQ/yith/queue"
)

type Partition struct {
	id        int
	topicName string
	q         *queue.Queue

	watermark uint64

	isRepplica bool
}

func NewPartition(id int, topicName string, isReplica bool) *Partition {
	return &Partition{
		id:        id,
		topicName: topicName,
		//q:q,
		isRepplica: isReplica,
	}
}

func (p *Partition) Produce(msgs []*message.Message) error {
	return p.q.Fill(msgs)
}

func (p *Partition) Consume(popOffset int64) ([]*message.Message, error) {
	return p.q.Pop(popOffset)
}
