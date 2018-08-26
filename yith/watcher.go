package yith

import (
	"bytes"
	"encoding/json"
	"github.com/CrocdileChan/yapool"
	"io"
	"io/ioutil"
	"net/http"
	"yithQ/meta"
	. "yithQ/util/logger"
)

type Watcher struct {
	zero              string
	heartbeatInterval string
	watchPort         string
	agent             *yapool.Agent
}

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

func (w *Watcher) WatchZero(metadataChan chan<- *meta.Metadata) {

	http.HandleFunc("/", func(wr http.ResponseWriter, r *http.Request) {
		byt, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			Lg.Error("read signal from zero error : %v", err)
			wr.WriteHeader(http.StatusBadRequest)
			io.Copy(wr, bytes.NewBufferString(err.Error()))
			return
		}
		var metadata *meta.Metadata
		err = json.Unmarshal(byt, metadata)
		if err != nil {
			Lg.Error("decode signal from zero error : %v", err)
			wr.WriteHeader(http.StatusBadRequest)
			io.Copy(wr, bytes.NewBufferString(err.Error()))
			return
		}
		metadataChan <- metadata

	})

	http.ListenAndServe(w.watchPort, nil)
}

func (w *Watcher) PushChangeToZero(signal Signal, change interface{}) {
	w.agent.SendMsgToCenter(&yapool.Msg{
		Level: signal,
		Msg:   change,
	})
}
