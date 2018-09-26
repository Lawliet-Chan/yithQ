package queue

import (
	"encoding/json"
	"testing"
	"time"
	"yithQ/message"
)

func TestFillToDisk(t *testing.T) {
	diskQ, err := NewDiskQueue("topic-partition")
	if err != nil {
		t.Fatalf("new disk queue error : %v", err)
	}
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
	err = diskQ.FillToDisk([]*message.Message{msg1, msg2})
	if err != nil {
		t.Fatalf("fill to disk error : %v", err)
	}
	t.Log("fill to disk successful!")
}

func TestPopFromDisk(t *testing.T) {
	diskQ, err := NewDiskQueue("topic-partition")
	if err != nil {
		t.Fatalf("new disk queue error : %v", err)
	}
	data, err := diskQ.PopFromDisk(1)
	if err != nil {
		t.Fatalf("pop from disk error %v", err)
	}
	var msgs []*message.Message
	err = json.Unmarshal([]byte("["+string(data)+"]"), &msgs)
	if err != nil {
		t.Fatalf("json unmarshal %s error %v", string(data), err)
	}
	for _, msg := range msgs {
		t.Logf("msg (%d) is %s", msg.ID, string(msg.Body))
	}
}
