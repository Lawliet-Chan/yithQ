package queue

import (
	"yithQ/message"
	"yithQ/yith/conf"
)

type MemoryQueue interface {
	FillToMemory(msg []*message.Message) error
	PopFromMemory(popOffset int64) ([]*message.Message, int64, error)
}

type memoryQueue struct {
}

func NewMemoryQueue(conf *conf.MemoryQueueConf) (MemoryQueue,error) {
	return &memoryQueue{},nil
}

func (mq *memoryQueue) FillToMemory(msgs []*message.Message) error {

}

func (mq *memoryQueue) PopFromMemory(popOffset int64) ([]*message.Message, int64, error) {

}
