package main

import (
	"strconv"
	"yithQ/client/producer"
)

func main() {
	p, err := producer.NewProducer("http://localhost:9900")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1; i++ {
		err := p.Publish("azathoth", []byte("the great race of Yith can be through space and time :"+strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}

	}

}
