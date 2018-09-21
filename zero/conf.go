package zero

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	ListenPort       string
	YithWatchPort    string
	ForFetchMetaPort string

	HeartbeatTimeout string
}

func InitConfig() *Config {
	data, err := ioutil.ReadFile("./yith_zero.yml")
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
