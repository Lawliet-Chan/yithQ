package queue

import (
	"github.com/smartystreets/go-disruptor"
	"io"
	"yithQ/message"
	"yithQ/yith/conf"
)

const (
	RingBufferCapacity = 1024
	RingBufferMask     = RingBufferCapacity - 1
)

type MemoryQueue interface {
	SetWriter(writer io.Writer)
	FillToMemory(msg []*message.Message) error
	PopFromMemory() ([]*message.Message, error)
}

type memoryQueue struct {
	disruptor     disruptor.Disruptor
	msgRingBuffer []*message.Message
	writer        io.Writer
}

func NewMemoryQueue(cfg *conf.MemoryQueueConf) (MemoryQueue, error) {
	mq := &memoryQueue{
		msgRingBuffer: make([]*message.Message, RingBufferCapacity),
	}
	mq.disruptor = disruptor.Configure(cfg.RingBufferCapacity).WithConsumerGroup(mq).Build()
	return mq, nil
}

func (mq *memoryQueue) SetWriter(writer io.Writer) {
	mq.writer = writer
}

func (mq *memoryQueue) FillToMemory(msgs []*message.Message) error {

}

func (mq *memoryQueue) PopFromMemory() ([]*message.Message, error) {

}

func (mq *memoryQueue) Consume(lower, upper int64) {

}
