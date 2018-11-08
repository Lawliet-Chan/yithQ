package producer

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"yithQ/message"
	"yithQ/meta"
)

type Producer struct {
	zeroAddress string
	metadata    *meta.Metadata
	//the amount that each topic can have
	partitionFactory float64
}

func NewProducer(zeroAddress string) *Producer {
	return NewProducerWithPartitionFactory(zeroAddress, 0.75)
}

func NewProducerWithPartitionFactory(zeroAddress string, partitionFactory float64) *Producer {
	return &Producer{
		zeroAddress:      zeroAddress,
		metadata:         meta.NewMetadata(),
		partitionFactory: partitionFactory,
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
	nodeTopicMeta := p.metadata.FindTopicAllPartitions(topic)
	if len(nodeTopicMeta) == 0 {
		nodes := p.metadata.GetAllNodes()
		for i, node := range nodes {
			nodeTopicMeta[node] = meta.TopicMetadata{
				Topic:       topic,
				PartitionID: i + 1,
				IsReplica:   false,
			}
		}
	}
	length := len(msgsByt) / len(nodeTopicMeta)
	i := 0
	j := length
	for node, tm := range nodeTopicMeta {
		go func(node string, topicmeta meta.TopicMetadata, msgsByt [][]byte, errChan chan<- error) {
			byt, err := p.makeMessagesByte(topic, msgsByt, topicmeta.PartitionID)
			if err != nil {
				errChan <- err
			}
			err = p.sendToBroker(node, byt)
			if err != nil {
				errChan <- err
			}
		}(node, tm, msgsByt[i:j], errChan)
		i = j
		j += length
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
