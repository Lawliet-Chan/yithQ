package consumer

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"yithQ/message"
	"yithQ/meta"
)

type Consumer struct {
	rw            sync.RWMutex
	zeroAddress   string
	topicOffset   map[string]int64 // key is topic_partitionID, ep:  yith_100
	metadata      *meta.Metadata
	consumeAmount int
}

func NewConsumer(zeroAddress string) *Consumer {
	return NewConsumerWithAmount(zeroAddress, 256)
}

func NewConsumerWithAmount(zeroAddress string, consumeAmount int) *Consumer {
	return &Consumer{
		zeroAddress: zeroAddress,
		//offset is the last consumed index
		topicOffset:   make(map[string]int64),
		metadata:      meta.NewMetadata(),
		consumeAmount: consumeAmount,
	}
}

func (c *Consumer) Consume(topic string, fn func(msg *message.Message) error) <-chan error {
	errChan := make(chan error)
	nodeTopics := c.metadata.FindTopicAllPartitions(topic)
	for node, topicmeta := range nodeTopics {
		go func(node string, topicmeta meta.TopicMetadata) {
			partitionID := topicmeta.PartitionID
			offset := c.Offset(topic, partitionID)
			msgs, err := c.consumeFromBroker(node, topic, partitionID, offset+1)
			if err != nil {
				errChan <- err
				return
			}
			for _, msg := range msgs {
				if err := fn(msg); err != nil {
					errChan <- err
				}
			}
			c.addOffset(topic, partitionID, int64(len(msgs))-1)
		}(node, topicmeta)
	}
	return errChan
}

func (c *Consumer) ConsumePartition(topic string, partitionID int) ([]*message.Message, error) {
	return c.ConsumePartitionWithOffset(topic, partitionID, c.Offset(topic, partitionID)+1)
}

//PARAM offset is index of starting to consume
func (c *Consumer) ConsumePartitionWithOffset(topic string, partitionID int, offset int64) ([]*message.Message, error) {
	if c.Offset(topic, partitionID)+1 != offset {
		c.setOffset(topic, partitionID, offset-1)
	}
	node := c.metadata.FindNodeWithTopicPartitionID(topic, partitionID, false)
	msgs, err := c.consumeFromBroker(node, topic, partitionID, offset)
	if err != nil {
		return nil, err
	}
	c.addOffset(topic, partitionID, int64(len(msgs))-1)
	return msgs, nil
}

func (c *Consumer) Offset(topic string, partitionID int) int64 {
	c.rw.RLock()
	defer c.rw.RUnlock()
	offset, _ := c.topicOffset[topic+"_"+strconv.Itoa(partitionID)]
	return offset
}

func (c *Consumer) setOffset(topic string, partitionID int, offset int64) {
	c.rw.Lock()
	defer c.rw.Unlock()
	c.topicOffset[topic+"_"+strconv.Itoa(partitionID)] = offset
}

func (c *Consumer) addOffset(topic string, partitionID int, deltaOffset int64) {
	c.rw.Lock()
	defer c.rw.Unlock()
	c.topicOffset[topic+"_"+strconv.Itoa(partitionID)] += deltaOffset
}

func (c *Consumer) consumeFromBroker(node, topic string, partitionID int, offset int64) ([]*message.Message, error) {
	resp, err := http.PostForm(node, url.Values{
		"topic":       []string{topic},
		"partitionID": []string{strconv.Itoa(partitionID)},
		"offset":      []string{strconv.FormatInt(offset, 10)},
		"version":     []string{strconv.FormatUint(uint64(c.metadata.GetVersion()), 10)},
		"amount":      []string{strconv.Itoa(c.consumeAmount)},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusMovedPermanently {
		metadata, err := c.obtainMetaFromZero()
		if err != nil {
			return nil, err
		}
		c.metadata.SetMetadata(metadata)
		return c.consumeFromBroker(node, topic, partitionID, offset)
	}
	byt, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var msgs []*message.Message
	err = json.Unmarshal([]byte("["+string(byt)+"]"), &msgs)
	return msgs, err
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
