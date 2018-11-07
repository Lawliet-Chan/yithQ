package main

import (
	"fmt"
	"yithQ/client/consumer"
	"yithQ/message"
)

func main() {
	c := consumer.NewConsumer("localhost:9900")
	errChan := c.Consume("azathoth", func(msg *message.Message) error {
		fmt.Printf("msg is %s", string(msg.Body))
		return nil
	})
	err := <-errChan
	if err != nil {
		panic(err)
	}

}
