package main

import (
	"runtime"
	"yithQ/yith"
	"yithQ/yith/conf"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := conf.InitConfig()
	yith.NewServe(cfg).Run()
}
