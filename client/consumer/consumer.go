package consumer

import (
	"sync/atomic"
	"yithQ/message"
)

type Consumer struct {
	brokersAddress []string
	zeroAddress    string
	offset         int64
}

func NewConsumer(brokersAddress []string, zeroAddress string) *Consumer {
	return &Consumer{
		brokersAddress: brokersAddress,
		zeroAddress:    zeroAddress,
	}
}

func (c *Consumer) Consume(topic string) error {

}

func (c *Consumer) ConsumeWithOffset(topic string, offset int64) ([]*message.Messages, int, error) {

}

func (c *Consumer) ConsumePartition(topic string, partitionID int) ([]*message.Messages, error) {

}

func (c *Consumer) ConsumePartitionWithOffset(topic string, partitionID int, offset int64) ([]*message.Messages, error) {

}

func (c *Consumer) Offset() int64 {
	return atomic.LoadInt64(&c.offset)
}
