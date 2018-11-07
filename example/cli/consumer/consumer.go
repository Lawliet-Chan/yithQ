package main

import "yithQ/client/consumer"

func main() {
	c := consumer.NewConsumer("localhost:9900")
	msgChan, errChan := c.Consume("azathoth")

}
