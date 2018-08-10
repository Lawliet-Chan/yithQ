package queue

import (
	"yithQ/message"
	"yithQ/yith/conf"
)

type DiskQueue interface {
	FillToDisk(msg []*message.Message) error
	PopFromDisk(popOffset int64) ([]*message.Message, error)
}

type diskQueue struct {
}

func NewDiskQueue(conf *conf.DiskQueueConf) (DiskQueue, error) {
	return &diskQueue{}, nil
}

func (dq *diskQueue) FillToDisk(msgs []*message.Message) error {

}

func (dq *diskQueue) PopFromDisk(popOffset int64) ([]*message.Message, error) {

}
