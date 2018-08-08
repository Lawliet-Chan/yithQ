package queue

import (
	"yithQ/message"
	"yithQ/yith/conf"
)

type DiskQueue interface {
	FillToDisk(msg *message.Message) error
	PopFromDisk(popOffset int64) ([]*message.Message, int64, error)
}

type diskQueue struct {
}

func NewDiskQueue(conf *conf.DiskQueueConf) DiskQueue {
	return &diskQueue{}
}

func (dq *diskQueue) FillToDisk(msg *message.Message) error {

}

func (dq *diskQueue) PopFromDisk(popOffset int64) ([]*message.Message, int64, error) {

}