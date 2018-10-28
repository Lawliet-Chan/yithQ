package consumer

import (
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"yithQ/message"
	"yithQ/meta"
)

type Consumer struct {
	brokersAddress []string
	zeroAddress    string
	offset         int64
	metadata       *meta.Metadata
}

func NewConsumer(brokersAddress []string, zeroAddress string) *Consumer {
	return &Consumer{
		brokersAddress: brokersAddress,
		zeroAddress:    zeroAddress,
		offset:         1,
	}
}

func (c *Consumer) Consume(topic string) ([]*message.Message, error) {

}

func (c *Consumer) ConsumeWithOffset(topic string, offset int64) ([]*message.Messages, int, error) {

}

/*
func (c *Consumer) ConsumePartition(topic string, partitionID int) ([]*message.Messages, error) {

}

func (c *Consumer) ConsumePartitionWithOffset(topic string, partitionID int, offset int64) ([]*message.Messages, error) {

}
*/
func (c *Consumer) Offset() int64 {
	return atomic.LoadInt64(&c.offset)
}

func (c *Consumer) obtainMetaFromZero() (*meta.Metadata, error) {
	resp, err := http.Get(c.zeroAddress + "/" + meta.FetchMetadata.String())
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	metadata := meta.NewMetadata()
	err = metadata.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (c *Consumer) consumeFromBroker(node, topic string, metaVersion uint32) {

}
