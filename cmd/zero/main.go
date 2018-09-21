package main

import (
	"os"
	"yithQ/util/logger"
	"yithQ/zero"
)

func main() {
	cfg := zero.InitConfig()
	logger.NewLogger(os.Stdout, cfg.LoggerLevel)
	zero.NewZero(cfg).Run()
}
