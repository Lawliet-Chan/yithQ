package main

import (
	"os"
	"runtime"
	"yithQ/util/logger"
	"yithQ/yith"
	"yithQ/yith/conf"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := conf.InitConfig()
	logger.NewLogger(os.Stdout, cfg.LoggerLevel)

	yith.NewServe(cfg).Run()
}
