package queue

import "yithQ/yith/message"

type DiskQueue interface {
	FillToDisk(msg *message.Message) error
	PopFromDisk(popOffset int64) (*message.Message, error)
}

type diskQueue struct {
}

func NewDiskQueue() DiskQueue {
	return &diskQueue{}
}

func (dq *diskQueue) FillToDisk(msg *message.Message) error {

}

func (dq *diskQueue) PopFromDisk(popOffset int64) (*message.Message, error) {

}
