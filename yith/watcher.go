package yith

import (
	"bytes"
	"encoding/json"
	"github.com/CrocdileChan/yapool"
	"io"
	"io/ioutil"
	"net/http"
	"time"
	"yithQ/meta"
	. "yithQ/util/logger"
)

type Watcher struct {
	zero              string
	heartbeatInterval time.Duration
	watchPort         string
	agent             *yapool.Agent
}

func NewWatcher(zero, heartbeatInterval, watchPort string) (*Watcher, error) {

	heartbeatDuration, err := time.ParseDuration(heartbeatInterval)
	if err != nil {
		return nil, err
	}

	return &Watcher{
		zero:              zero,
		heartbeatInterval: heartbeatDuration,
		watchPort:         watchPort,
		agent:             yapool.NewAgent([]string{zero}),
	}, nil
}

func (w *Watcher) SendHeartbeatToZero() error {
	req, err := http.NewRequest("GET", w.zero+"/"+meta.Heartbeat.String(), nil)
	if err != nil {
		return err
	}
	cli := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1, //MaxIdleConns=len(zero_addresses)
			MaxIdleConnsPerHost: 1,
			DisableKeepAlives:   false,
		},
	}

	ticker := time.NewTicker(w.heartbeatInterval)
	for {
		select {
		case <-ticker.C:
			resp, err := cli.Do(req)
			if err != nil {
				Lg.Errorf("send heartbeat to zero(%s) error : %v ", req.RemoteAddr, err)
				continue
			}
			resp.Body.Close()
		}
	}

	return nil
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
		metadata := meta.NewMetadata()
		err = metadata.Unmarshal(byt)
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

func (w *Watcher) PushChangeToZero(signal meta.Signal, change interface{}) error {
	var byt []byte
	var err error
	if change != nil {
		byt, err = json.Marshal(change)
		if err != nil {
			return err
		}
	}

	resp, err := http.Post(w.zero+"/"+signal.String(), "application/json", bytes.NewReader(byt))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (w *Watcher) FetchMetadata() (*meta.Metadata, error) {
	resp, err := http.Get(w.zero + "/" + meta.FetchMetadata.String())
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	metadata := meta.NewMetadata()
	err = metadata.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (w *Watcher) Pickup(tm []meta.TopicMetadata) ([]meta.TopicMetadata, error) {
	data, err := json.Marshal(tm)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(w.zero+"/"+meta.PickupStr, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var results []meta.TopicMetadata
	err = json.Unmarshal(data, &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}
