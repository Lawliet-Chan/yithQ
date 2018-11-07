package main

import "yithQ/client/producer"

func main() {
	p := producer.NewProducer("localhost:9900")
	p.Publish("azathoth", []byte("ok"))
}
