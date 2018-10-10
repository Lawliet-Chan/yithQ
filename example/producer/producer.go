package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"yithQ/message"
)

var msgStr = `放置策略： 
　放置策略决定一个进程块驻留在实存中的什么地方。在一个纯粹的分段系统中，比如最佳适配，以及首次适配等都是可供选择的，但对于一个纯粹的分页系统或者段页式系统来说，如何放置其实没有关系，这是因为地址转换硬件和内存访问硬件可以以相同的效率为任何页框组合执行它们的功能。`

func main() {
	producerUrl := "http://localhost:9970/produce"
	msgs := make([]*message.Message, 0)
	for i := 0; i <= 50; i++ {
		msg := &message.Message{
			Body: []byte(msgStr),
		}
		msgs = append(msgs, msg)
	}
	messages := &message.Messages{
		Topic:       "yith",
		PartitionID: 001,
		Msgs:        msgs,
		MetaVersion: 0,
	}
	byt, err := json.Marshal(messages)
	if err != nil {
		panic("json marshal error : " + err.Error())
	}
	_, err = http.Post(producerUrl, "application/json", bytes.NewBuffer(byt))
	if err != nil {
		panic("producer http post error : " + err.Error())
	}
}
