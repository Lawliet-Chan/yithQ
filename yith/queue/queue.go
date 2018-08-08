package queue

import "yithQ/message"

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

func (q *Queue) Fill(msg *message.Message) error {
	err := q.mq.FillToMemory(msg)
	if err != nil {
		return err
	}
	return q.dq.FillToDisk(msg)
}

func (q *Queue) Pop(popOffset int64) ([]*message.Message, int64, error) {

}
