package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"yithQ/message"
)

func main() {
	consumerUrl := "http://localhost:9971/consume?topic=yith&partitionID=001&offset=1"
	resp, err := http.Post(consumerUrl, "application/json", nil)
	if err != nil {
		panic("post consumerUrl error : " + err.Error())
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic("read consumer http response error : " + err.Error())
	}
	var msgs []*message.Message
	err = json.Unmarshal([]byte("["+string(data)+"]"), &msgs)
	if err != nil {
		panic("json unmarshal msgs by consume from yith error : " + err.Error())
	}
	for _, msg := range msgs {
		fmt.Println("msg body is : " + string(msg.Body))
	}
}
