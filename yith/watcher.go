package yith

import (
	"bytes"
	"encoding/json"
	"github.com/CrocdileChan/yapool"
	"io"
	"io/ioutil"
	"net/http"
	. "yithQ/util/logger"
)

type Watcher struct {
	zero              string
	heartbeatInterval string
	watchPort         string
	agent             *yapool.Agent
}

type Signal struct {
	Typ SignalType `json:"typ"`
}

type SignalType int

func NewWatcher(zero, heartbeatInterval, watchPort string) *Watcher {

	return &Watcher{
		zero:              zero,
		heartbeatInterval: heartbeatInterval,
		watchPort:         watchPort,
		agent:             yapool.NewAgent([]string{zero}),
	}
}

func (w *Watcher) SendHeartbeatToZero() {
	w.agent.Heartbeat(w.heartbeatInterval)
}

func (w *Watcher) WatchZero(signalChan chan<- *Signal) {

	http.HandleFunc("/", func(wr http.ResponseWriter, r *http.Request) {
		byt, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			Lg.Error("read event from zero error : %v", err)
			wr.WriteHeader(http.StatusBadRequest)
			io.Copy(wr, bytes.NewBufferString(err.Error()))
			return
		}
		var signal *Signal
		err = json.Unmarshal(byt, signal)
		if err != nil {
			Lg.Error("decode event from zero error : %v", err)
			wr.WriteHeader(http.StatusBadRequest)
			io.Copy(wr, bytes.NewBufferString(err.Error()))
			return
		}

		signalChan <- signal

	})

	http.ListenAndServe(w.watchPort, nil)
}

func (w *Watcher) PushChangeToZero() {

}
