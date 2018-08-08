package yith

import (
	"yithQ/message"
	"yithQ/yith/queue"
)

type Partition struct {
	id        int
	topicName string
	q         *queue.Queue

	isRepplica bool
}

func NewPartition(id int, topicName string, isRepplica bool) *Partition {
	return &Partition{
		id:        id,
		topicName: topicName,
		//q:q,
		isRepplica: isRepplica,
	}
}

func (p *Partition) Produce(msg *message.Message) error {
	return p.q.Fill(msg)
}

func (p *Partition) Consume(popOffset int64) ([]*message.Message, int64, error) {
	return p.q.Pop(popOffset)
}