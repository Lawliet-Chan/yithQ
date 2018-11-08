package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
	"yithQ/message"
)

func main() {
	consumerUrl := "http://localhost:9971/consume"
	//req, _ := http.NewRequest(http.MethodGet, consumerUrl, nil)
	//q:=req.URL.Query()
	//q.Set("topic","yith")
	//q.Set("partitionID", "001")
	//q.Set("offset","1")
	//req.URL.RawQuery=q.Encode()
	//fmt.Println("request url is ",req.URL.Query())
	//cli := http.Client{}
	//resp, err := cli.Do(req)

	start := time.Now()
	resp, err := http.PostForm(consumerUrl, url.Values{
		"topic":       []string{"yith"},
		"partitionID": []string{"001"},
		"offset":      []string{"1"},
		"version":     []string{"0"},
		"amount":      []string{"4096"},
	})
	//http.Get(consumerUrl)
	if err != nil {
		panic("post consumerUrl error : " + err.Error())
	}
	//fmt.Println("post bench is ",time.Since(start).Seconds())
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic("read consumer http response error : " + err.Error())
	}
	//fmt.Println("data is ", string(data))
	var msgs []*message.Message
	err = json.Unmarshal([]byte("["+string(data)+"]"), &msgs)
	if err != nil {
		panic("json unmarshal msgs by consume from yith error : " + err.Error())
	}
	fmt.Println("consume bench is ", time.Since(start).Seconds())
	for _, msg := range msgs {
		fmt.Println("msg body is : " + string(msg.Body))
	}
}
