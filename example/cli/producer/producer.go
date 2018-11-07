package main

import (
	"strconv"
	"yithQ/client/producer"
)

func main() {
	p := producer.NewProducer("localhost:9900")
	for i := 0; i < 1<<15; i++ {
		p.Publish("azathoth", []byte("the great race of Yith can be through space and time :"+strconv.Itoa(i)))
	}

}
