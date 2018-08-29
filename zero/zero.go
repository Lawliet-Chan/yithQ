package zero

import (
	"github.com/CrocdileChan/yapool"
	"yithQ/meta"
)

type Zero struct {
	metadata *meta.Metadata
	center   *yapool.Center
	cfg      *Config
}

func NewZero(cfg *Config) *Zero {
	return &Zero{
		center: yapool.NewCenter(cfg.ListenPort),
		cfg:    cfg,
	}
}

func (z *Zero) Run() {

}

func (z *Zero) ListenYith(yithAddrChan chan<- string) {
	z.center.Receive(z.cfg.HeartbeatTimeout, yithAddrChan)
}

func (z *Zero) NortifyAllYith() {

}

func (z *Zero) listenHeartbeat() {

}

func (z *Zero) listenChangeSignal() {

}
