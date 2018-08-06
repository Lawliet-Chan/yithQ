package queue

import "yithQ/yith/message"

type MemoryQueue interface {
	FillToMemory(msg *message.Message) error
	PopFromMemory(popOffset int64) (*message.Message, error)
}

type memoryQueue struct {
}

func NewMemoryQueue() MemoryQueue {
	return &memoryQueue{}
}

func (mq *memoryQueue) FillToMemory(msg *message.Message) error {

}

func (mq *memoryQueue) PopFromMemory(popOffset int64) (*message.Message, error) {

}
