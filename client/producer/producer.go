package producer

type Producer struct {
	brokersAddress []string
	zeroAddress    string
}

func NewProducer(brokersAddress []string, zeroAddress string) *Producer {
	return &Producer{
		brokersAddress: brokersAddress,
		zeroAddress:    zeroAddress,
	}
}

func (p *Producer) Publish(topic string, msg interface{}) error {

}

func (p *Producer) MultiPublish(topic string, msgs []interface{}) error {

}

func (p *Producer) PublishParition(topic string, partitionID int, msg interface{}) error {

}

func (p *Producer) MultiPublishPartition(topic string, partitionID int, msgs []interface{}) error {

}
