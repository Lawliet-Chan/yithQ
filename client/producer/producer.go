package producer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"yithQ/message"
	"yithQ/meta"
	"yithQ/status"
)

type Producer struct {
	zeroAddress string
	metadata    *meta.Metadata
	//the amount that each topic can have
	partitionFactory float64

	producerPort string
}

func NewProducer(zeroAddress string) (*Producer, error) {
	return NewProducerWithPfAndPort(zeroAddress, ":9970", 0.75)
}

func NewProducerWithPfAndPort(zeroAddress string, producerPort string, partitionFactory float64) (*Producer, error) {
	p := &Producer{
		zeroAddress:      zeroAddress,
		partitionFactory: partitionFactory,
		producerPort:     producerPort,
	}
	metadata, err := p.obtainMetaFromZero()
	if err != nil {
		return nil, err
	}
	p.metadata = metadata
	return p, nil
}

func (p *Producer) Publish(topic string, msg []byte) error {
	errChan := make(chan error)
	p.send(topic, [][]byte{msg}, errChan)
	return <-errChan
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
	fmt.Println("node topicmeta are ", nodeTopicMeta)
	length := len(msgsByt) / len(nodeTopicMeta)
	i := 0
	j := length
	var wg sync.WaitGroup
	wg.Add(len(nodeTopicMeta))
	for node, tm := range nodeTopicMeta {
		go func(node string, topicmeta meta.TopicMetadata, msgsByt [][]byte, wg sync.WaitGroup, errChan chan<- error) {
			err := p.sendToBroker(node, p.makeMessages(topic, msgsByt, topicmeta.PartitionID))
			if err != nil {
				errChan <- err
			}
			wg.Done()
		}(node, tm, msgsByt[i:j], wg, errChan)
		i = j
		j += length
	}
	wg.Wait()
}

func (p *Producer) sendPartition(topic string, partitionID int, msgsByt [][]byte) error {
	node := p.metadata.FindNodeWithTopicPartitionID(topic, partitionID, false)
	if node == "" {
		node = p.metadata.GetAllNodes()[0]
	}
	return p.sendToBroker(node, p.makeMessages(topic, msgsByt, partitionID))
}

func (p *Producer) sendToBroker(node string, msgs *message.Messages) error {
	node = node[:strings.LastIndex(node, ":")] + p.producerPort + "/produce"
	fmt.Printf("send to %s msg %v \n", node, msgs)
	if !strings.HasPrefix(node, "http") {
		node = "http://" + node
	}
	byt, err := json.Marshal(msgs)
	if err != nil {
		return err
	}
	metaChanged, err := p.httpSendToBroker(node, byt)
	if err != nil {
		return err
	}
	if metaChanged {
		fmt.Println(status.MetaChanged)
		metadata, err := p.obtainMetaFromZero()
		if err != nil {
			return err
		}
		p.metadata.SetMetadata(metadata)
		msgs.MetaVersion = metadata.GetVersion()
		return p.sendToBroker(node, msgs)
	}
	return nil
}

//bool is that if Meta changed
func (p *Producer) httpSendToBroker(node string, msgsByt []byte) (bool, error) {
	resp, err := http.Post(node, "application/json", bytes.NewBuffer(msgsByt))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	return status.MetaChanged == string(data), nil
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
	fmt.Println("metadata nodes from zero are ",metadata.Nodes)
	fmt.Println("metadata topicmeta from zero is ",metadata.TopicNodeMap)
	fmt.Println("metadata version from zero is ",metadata.Version)
	return metadata, nil
}

func (p *Producer) makeMessages(topic string, msgsByt [][]byte, partitionID int) *message.Messages {
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

/*
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
}*/
