package queue

import (
	"net/http"
	"yithQ/message"
)

type Queue struct {
	mq MemoryQueue
	dq DiskQueue
}

func NewQueue(mq MemoryQueue, dq DiskQueue) *Queue {
	return &Queue{
		mq: mq,
		dq: dq,
	}
}

func (q *Queue) Fill(msgs []*message.Message) error {
	/*err := q.mq.FillToMemory(msgs)
	if err != nil {
		return err
	}*/
	return q.dq.FillToDisk(msgs)
}

func (q *Queue) Pop(popOffset int64, writer http.ResponseWriter) error {
	/*if popOffset == -1 {
		q.mq.PopFromMemory(writer)
		return nil
	}*/
	msgsData, err := q.dq.PopFromDisk(popOffset)
	if err != nil {
		return err
	}
	writer.Write(msgsData)
	return nil
}
