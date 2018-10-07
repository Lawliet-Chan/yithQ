package queue

import (
	"net/http"
	"testing"
	"time"
	"yithQ/message"
	"yithQ/yith/conf"
)

func TestFillToMemory(t *testing.T) {
	cfg := &conf.MemoryQueueConf{RingBufferCapacity: 1024 * 64}
	mq := NewMemoryQueue(cfg)
	msg1 := &message.Message{
		ID:        123456,
		Body:      []byte("abcde"),
		Timestamp: time.Now().UnixNano(),
	}
	msg2 := &message.Message{
		ID:        123457,
		Body:      []byte("fghijk"),
		Timestamp: time.Now().UnixNano(),
	}
	msgs := make([]*message.Message, 0)
	msgs = append(msgs, msg1, msg2)
	err := mq.FillToMemory(msgs)
	t.Log("fill finish")
	if err != nil {
		t.Fatal(err)
	}
}

func TestPopFromMemory(t *testing.T) {
	cfg := &conf.MemoryQueueConf{RingBufferCapacity: 1024 * 64}
	mq := NewMemoryQueue(cfg)
	msg1 := &message.Message{
		ID:        123456,
		Body:      []byte("abcde"),
		Timestamp: time.Now().UnixNano(),
	}
	msg2 := &message.Message{
		ID:        123457,
		Body:      []byte("fghijk"),
		Timestamp: time.Now().UnixNano(),
	}
	msgs := make([]*message.Message, 0)
	msgs = append(msgs, msg1, msg2)
	err := mq.FillToMemory(msgs)
	if err != nil {
		t.Fatal(err)
	}
	http.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		mq.PopFromMemory(writer)
	})
}
