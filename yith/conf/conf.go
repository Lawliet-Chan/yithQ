package conf

type Config struct {
	UseMemoryQueue bool
	UseDiskQueue   bool
	*DiskQueueConf
	*MemoryQueueConf

	ReplicaTcpPort string
	ProducerPort   string
	ConsumerPort   string

	ReplicaFactory int

	ZeroAddress       string
	WatchPort         string
	HeartbeatInterval string
}

type DiskQueueConf struct {
}

type MemoryQueueConf struct {
}
