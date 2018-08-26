package conf

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

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
