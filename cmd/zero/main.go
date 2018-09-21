package main

import "yithQ/zero"

func main() {
	cfg := zero.InitConfig()
	zero.NewZero(cfg).Run()
}
