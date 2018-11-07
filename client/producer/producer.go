package producer

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"yithQ/message"
	"yithQ/meta"
)

type Producer struct {
	zeroAddress string
	metadata    *meta.Metadata

	//	timeSendLimit  time.Duration
	//	countSendLimit int32

	//	timeCounter  *time.Timer
	//	countCounter int32

	//	nortifySend chan struct{}

	//	errorSend chan error

	//	sendingQueueMap *sync.Map //map[string][]*message.Message
}

func NewProducer(zeroAddress string) *Producer {
	return &Producer{
		zeroAddress: zeroAddress,
		metadata:    meta.NewMetadata(),
	}
}

func (p *Producer) Publish(topic string, msg []byte) <-chan error {
	errChan := make(chan error)
	p.send(topic, [][]byte{msg}, errChan)
	return errChan
}

func (p *Producer) MultiPublish(topic string, msgs [][]byte) <-chan error {
	errChan := make(chan error)
	p.send(topic, msgs, errChan)
	return errChan
}

func (p *Producer) PublishPartition(topic string, partitionID int, msg []byte) error {
	return p.sendPartition(topic, partitionID, [][]byte{msg})
}

func (p *Producer) MultiPublishPartition(topic string, partitionID int, msgs [][]byte) error {
	return p.sendPartition(topic, partitionID, msgs)
}

func (p *Producer) send(topic string, msgsByt [][]byte, errChan chan<- error) {
	nodeTopicMetas := p.metadata.FindTopicAllPartitions(topic)
	for node, tms := range nodeTopicMetas {
		for _, topicmeta := range tms {
			go func(topicmeta meta.TopicMetadata) {
				byt, err := p.makeMessagesByte(topic, msgsByt, topicmeta.PartitionID)
				if err != nil {
					errChan <- err
				}
				err = p.sendToBroker(node, byt)
				if err != nil {
					errChan <- err
				}
			}(topicmeta)
		}

	}
	if len(nodeTopicMetas) == 0 {
		nodes := p.metadata.GetAllNodes()

	}
}

func (p *Producer) sendPartition(topic string, partitionID int, msgsByt [][]byte) error {
	byt, err := p.makeMessagesByte(topic, msgsByt, partitionID)
	if err != nil {
		return err
	}
	node := p.metadata.FindNodeWithTopicPartitionID(topic, partitionID, false)
	if node == "" {
		node = p.metadata.GetAllNodes()[0]
	}
	return p.sendToBroker(node, byt)
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
		p.metadata.SetMetadata(metadata)
		return p.sendToBroker(node, msgsByt)
	}
	return nil
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

func (p *Producer) makeMessagesByte(topic string, msgsByt [][]byte, partitionID int) ([]byte, error) {
	msgs := make([]*message.Message, 0)
	for _, msgByt := range msgsByt {
		msgs = append(msgs, &message.Message{
			Body:    msgByt,
			IsRetry: false,
			//SeqNum:
		})
	}
	messages := &message.Messages{
		Topic:       topic,
		Msgs:        msgs,
		PartitionID: partitionID,
		MetaVersion: p.metadata.GetVersion(),
	}
	return json.Marshal(messages)
}
