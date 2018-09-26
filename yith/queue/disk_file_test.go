package queue

import (
	"encoding/json"
	"testing"
	"time"
	"yithQ/message"
)

func TestWrite(t *testing.T) {
	df, err := newDiskFile("topic-partition", 1, false)
	if err != nil {
		t.Fatalf("new disk file error : %v", err)
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
	msgs := make([]*message.Message, 0)
	msgs = append(msgs, msg1, msg2)
	_, err = df.write(1, []*message.Message{msg1, msg2})
	if err != nil {
		t.Fatalf("disk file write %v error %v", msgs, err)
	}
	readByt := make([]byte, 500)
	df.dataFile.Read(readByt)
	t.Logf("write msgs(%s) to disk successful! ", string(readByt))
}

func TestRead(t *testing.T) {
	df, err := newDiskFile("topic-partition", 1, false)
	if err != nil {
		t.Fatalf("new disk file error : %v", err)
	}
	df.startOffset = 1
	t.Logf("endOffset is %d", df.getEndOffset())
	byt, err := df.read(1, 2)
	if err != nil {
		t.Fatalf("read msgs from disk file error : %v", err)
	}
	t.Logf("length is %d  ,  read bytes is %s", len(byt), string(byt))
	var msgs []*message.Message

	err = json.Unmarshal([]byte("["+string(byt)+"]"), &msgs)
	if err != nil {
		t.Fatalf("json unmarshal msgs from disk file error : %v", err)
	}
	//t.Logf("msg body is %s", string(msg.Body))
	for _, msg := range msgs {
		t.Logf("read msgs from disk file is %v , body is %s", msg, string(msg.Body))
	}
}
