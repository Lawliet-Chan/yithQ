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
	brokersAddress []string
	zeroAddress    string
	metadata       *meta.Metadata

	timeSendLimit  time.Duration
	countSendLimit int32

	timeCounter  *time.Timer
	countCounter int32

	nortifySend chan struct{}

	errorSend chan error

	sendingQueueMap *sync.Map //map[string][]*message.Message
}

func NewProducer(brokersAddress []string, zeroAddress string) *Producer {
	return NewProducerWithSendLimit(brokersAddress, zeroAddress, 2*time.Millisecond, 1024)
}

func NewProducerWithSendLimit(brokersAddress []string, zeroAddress string, timeSendLimit time.Duration, countSendLimit int32) *Producer {
	p := &Producer{
		brokersAddress:  brokersAddress,
		zeroAddress:     zeroAddress,
		timeSendLimit:   timeSendLimit,
		countSendLimit:  countSendLimit,
		nortifySend:     make(chan struct{}),
		sendingQueueMap: &sync.Map{},
		countCounter:    0,
		errorSend:       make(chan error, countSendLimit),
		metadata:        meta.NewMetadata(),
	}
	go func() {
		for {
			select {
			case <-p.nortifySend:
				p.send()
			case <-p.timeCounter.C:
				p.send()
			}
			p.resetCountCounter()
			p.resetTimeCounter()
		}
	}()
	return p
}

func (p *Producer) Publish(topic string, msg []byte) {
	p.prepareForSend(topic, msg)
}

func (p *Producer) MultiPublish(topic string, msgs [][]byte) {
	p.prepareForSend(topic, msgs...)
}

func (p *Producer) prepareForSend(topic string, msgByts ...[]byte) {
	msgq, ok := p.sendingQueueMap.Load(topic)
	if !ok {
		p.sendingQueueMap.Store(topic, make([]*message.Message, 0))
	}
	msgs := make([]*message.Message, 0)
	for _, msgByt := range msgByts {
		msgs = append(msgs, &message.Message{
			Body:    msgByt,
			IsRetry: false,
			//SeqNum:
		})
	}
	p.sendingQueueMap.Store(topic, append(msgq.([]*message.Message), msgs...))
	go func() {
		p.CountingMsg()
		p.resetTimeCounter()
	}()
}

func (p *Producer) send() {
	p.sendingQueueMap.Range(func(topicI, msgsI interface{}) bool {
		go p.sendToBrokers(topicI.(string), msgsI.([]*message.Message))
		return true
	})
}

func (p *Producer) sendToBrokers(topic string, msgs []*message.Message) {
	nodeTopics := p.metadata.FindTopicAllPartitions(topic)
	for node, topicmetas := range nodeTopics {
		for _, topicmeta := range topicmetas {
			byt, err := p.makeMessagesByte(topic, msgs, topicmeta.PartitionID)
			if err != nil {
				p.errorSend <- err
				return
			}
			err = p.sendToBroker(node, byt)
			if err != nil {
				p.errorSend <- err
			}
		}
	}

	if len(nodeTopics) == 0 {
		nodes := p.metadata.GetAllNodes()
		for i, node := range nodes {
			byt, err := p.makeMessagesByte(topic, msgs, i)
			if err != nil {
				p.errorSend <- err
				return
			}
			err = p.sendToBroker(node, byt)
			if err != nil {
				p.errorSend <- err
			}
		}
	}
}

/*
func (p *Producer) sendToBrokersWithPartition(topic string, partitionID int, msgs []*message.Message) error {
	node := p.metadata.FindNodeWithTopicPartition(topic, partitionID, false)
	byt,err:=p.makeMessagesByte(topic,msgs,partitionID)
	if err != nil {
		p.errorSend<-err
		return
	}

}*/

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

func (p *Producer) CountingMsg() {
	if p.getCountCounter() >= p.countSendLimit {
		p.nortifySend <- struct{}{}
		p.resetCountCounter()
	} else {
		p.addCountCounter()
	}
}

func (p *Producer) resetCountCounter() {
	atomic.StoreInt32(&p.countCounter, 0)
}

func (p *Producer) getCountCounter() int32 {
	return atomic.LoadInt32(&p.countCounter)
}

func (p *Producer) addCountCounter() {
	atomic.AddInt32(&p.countCounter, 1)
}

func (p *Producer) resetTimeCounter() {
	p.timeCounter.Reset(p.timeSendLimit)
}

func (p *Producer) GetSendErrors() <-chan error {
	return p.errorSend
}

func (p *Producer) makeMessagesByte(topic string, msgs []*message.Message, partitionID int) ([]byte, error) {
	messages := &message.Messages{
		Topic:       topic,
		Msgs:        msgs,
		PartitionID: partitionID,
		MetaVersion: p.metadata.GetVersion(),
	}
	return json.Marshal(messages)
}
