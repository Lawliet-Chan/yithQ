package consumer

type Consumer struct {
	brokersAddress []string
	zeroAddress    string
}

func NewConsumer(brokersAddress []string, zeroAddress string) *Consumer {
	return &Consumer{
		brokersAddress: brokersAddress,
		zeroAddress:    zeroAddress,
	}
}

func (c *Consumer) Consume(topic string) error {

}

func (c *Consumer) ConsumeWithOffset(topic string, offset int64) {

}
