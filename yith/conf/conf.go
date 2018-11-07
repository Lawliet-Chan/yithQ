package conf

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	QueueConf *QueueConf `yaml:"queue_conf"`

	ReplicaTcpPort string `yaml:"replica_tcp_port"`
	ProducerPort   string `yaml:"producer_port"`
	ConsumerPort   string `yaml:"consumer_port"`

	ReplicaFactory int `yaml:"replica_factory"`

	ZeroAddress       string `yaml:"zero_address"`
	WatchPort         string `yaml:"watch_port"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`

	LoggerLevel string `yaml:"logger_level"`
}

type QueueConf struct {
	MemoryQueueConf *MemoryQueueConf `yaml:"memory_queue_conf"`
}

type MemoryQueueConf struct {
	RingBufferCapacity int64 `yaml:"ring_buffer_capacity"`
}

func InitConfig() *Config {
	data, err := ioutil.ReadFile("./yith.yml")
	if err != nil {
		panic("read config file error : " + err.Error())
	}
	cfg := &Config{}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		panic("unmarshal config bytes error :" + err.Error())
	}
	return cfg
}
