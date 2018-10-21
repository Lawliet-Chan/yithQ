package producer

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"
	"yithQ/message"
	"yithQ/meta"
)

type Producer struct {
	brokersAddress []string
	zeroAddress    string
	metadata       *meta.Metadata

	timeSendLimit     time.Duration
	quantitySendLimit int

	currentSendQueue  []*message.Message
	prepareSendQueueA []*message.Message
	prepareSendQueueB []*message.Message
}

func NewProducer(brokersAddress []string, zeroAddress string) *Producer {
	return NewProducerWithSendLimit(brokersAddress, zeroAddress, 2*time.Millisecond, 1024)
}

func NewProducerWithSendLimit(brokersAddress []string, zeroAddress string, timeSendLimit time.Duration, quantitySendLimit int) *Producer {
	p := &Producer{
		brokersAddress:    brokersAddress,
		zeroAddress:       zeroAddress,
		timeSendLimit:     timeSendLimit,
		quantitySendLimit: quantitySendLimit,
		prepareSendQueueA: make([]*message.Message, 0),
		prepareSendQueueB: make([]*message.Message, 0),
	}
	p.currentSendQueue = p.prepareSendQueueA
	return p
}

func (p *Producer) Publish(topic string, msg []byte) error {

}

func (p *Producer) PublishParition(topic string, partitionID int, msg []byte) error {

}

func (p *Producer) MultiPublish(topic string, msgs [][]byte) error {

}

func (p *Producer) MultiPublishPartition(topic string, partitionID int, msgs [][]byte) error {

}

func (p *Producer) makeMessages(topic string, partitionID int, msgsByt [][]byte) *message.Messages {
	msgs := make([]*message.Message, 0)
	for _, msgByt := range msgsByt {
		msgs = append(msgs, &message.Message{
			Body:    msgByt,
			IsRetry: false,
			//SeqNum:
		})
	}
	return &message.Messages{
		Topic:       topic,
		Msgs:        msgs,
		PartitionID: partitionID,
		MetaVersion: p.metadata.GetVersion(),
	}
}

func (p *Producer) prepareForSend(topic string, msgByt []byte) error {
	p.currentSendQueue = append(p.currentSendQueue, &message.Message{
		Body:    msgByt,
		IsRetry: false,
		//SeqNum:
	})
	if len(p.currentSendQueue) >= p.quantitySendLimit {

	}
}

func (p *Producer) sendToBrokers(topic string, msgs [][]byte) error {
	nodeTopics := p.metadata.FindTopicAllPartitions(topic)
	for node, topicmetas := range nodeTopics {
		for _, topicmeta := range topicmetas {

		}
	}
}

func (p *Producer) sendToBroker(node string, msgsByt []byte) error {
	resp, err := http.Post(node, "application/json", bytes.NewBuffer(msgsByt))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusMovedPermanently {
		metadata, err := p.obtainMetaFromZero()
		if err != nil {
			return err
		}
		p.metadata = metadata
		return p.sendToBroker(node, msgsByt)
	}
	return nil
}

func (p *Producer) sendToBrokersWithPartition(topic string, partitionID int, msgs [][]byte) error {
	node := p.metadata.FindNodeWithTopicPartition(topic, partitionID, false)
}

func (p *Producer) obtainMetaFromZero() (*meta.Metadata, error) {
	resp, err := http.Get(p.zeroAddress + "/" + meta.FetchMetadata.String())
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
