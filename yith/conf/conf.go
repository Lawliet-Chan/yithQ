package conf

type Config struct {
	DiskQueueConf
	MemoryQueueConf

	ReplicaTcpPort string
	ProducerPort   string
	ConsumerPort   string
}

type DiskQueueConf struct {
}

type MemoryQueueConf struct {
}
