package queue

import (
	"github.com/smartystreets/go-disruptor"
	"yithQ/message"
	"yithQ/yith/conf"

	"encoding/json"
	"net/http"
)

const (
	RingBufferCapacity = 1024
	RingBufferMask     = RingBufferCapacity - 1
	Iterations         = 1000000 * 100
	//ReserveMany        = 16
)

type MemoryQueue interface {
	FillToMemory(msg []*message.Message) error
	PopFromMemory(writer http.ResponseWriter) error
}

type memoryQueue struct {
	disruptor     disruptor.Disruptor
	msgRingBuffer []*message.Message
	writer        http.ResponseWriter
}

func NewMemoryQueue(cfg *conf.MemoryQueueConf) (MemoryQueue, error) {
	mq := &memoryQueue{
		msgRingBuffer: make([]*message.Message, RingBufferCapacity),
	}
	mq.disruptor = disruptor.Configure(cfg.RingBufferCapacity).WithConsumerGroup(mq).Build()
	return mq, nil
}

func (mq *memoryQueue) FillToMemory(msgs []*message.Message) error {
	writer := mq.disruptor.Writer()
	reserveMany := int64(len(msgs))
	seq := disruptor.InitialSequenceValue
	for seq <= Iterations {
		seq = writer.Reserve(reserveMany)
		for i := seq - reserveMany + 1; i <= seq; i++ {
			mq.msgRingBuffer[i&RingBufferMask] = msgs[i]
		}
		writer.Commit(seq-reserveMany+1, seq)
	}

	return nil
}

func (mq *memoryQueue) PopFromMemory(writer http.ResponseWriter) error {
	mq.writer = writer
	mq.disruptor.Start()
	return nil
}

func (mq *memoryQueue) Consume(lower, upper int64) {
	msgs := make([]*message.Message, 0)
	for seq := lower; seq <= upper; seq++ {
		msg := mq.msgRingBuffer[lower&RingBufferMask]
		msgs = append(msgs, msg)
	}
	data, err := json.Marshal(msgs)
	if err != nil {
		mq.writer.WriteHeader(http.StatusInternalServerError)
		mq.writer.Write([]byte(err.Error()))
	}
	mq.writer.Write(data)
}
