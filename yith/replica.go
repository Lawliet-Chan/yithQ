package yith

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"yithQ/message"
	"yithQ/meta"
	. "yithQ/util/logger"
)

func (s *Serve) replicateToOtherNodes(topic string, msgs []byte, replicaErrCh chan error, wg sync.WaitGroup) {
	replicaNodes := s.metadata.Load().(*meta.Metadata).FindReplicaNodes(topic)
	replicaErrCh = make(chan error, len(replicaNodes))
	wg.Add(len(replicaNodes))
	for _, node := range replicaNodes {
		go func(node string) {
			resp, err := http.Post(node+"/replica", "application/json", bytes.NewBuffer(msgs))
			if err != nil {
				replicaErrCh <- err
				return
			}
			defer resp.Body.Close()
			wg.Done()
		}(node)
	}
}

func (s *Serve) receiveReplicaFromOtherNodes(w http.ResponseWriter, req *http.Request) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		Lg.Errorf("receive messages from yith_broker(%s) error : %v", req.RemoteAddr, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var msgs *message.Messages
	err = json.Unmarshal(data, msgs)
	if err != nil {
		Lg.Errorf("json unmarshal data(%s) error : %v", string(data), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if !s.node.ExistTopic(msgs.Topic) {
		//从zero拉取最新metadata
		metadata, err := s.watcher.FetchMetadata()
		if err != nil {
			Lg.Errorf("fetch metadata from zero error : %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.updateMetadata(metadata)
		//更新本地metadata
		partitionID := s.metadata.Load().(*meta.Metadata).FindPatitionID(msgs.Topic, s.node.IP, true)
		err = s.node.AddTopicPartition(msgs.Topic, partitionID, true, s.cfg.QueueConf)
		if err != nil {
			Lg.Errorf("yith_broker(%s) replicate msgs to topic(%s) [ADD new topic partition] error : %v", req.RemoteAddr, msgs.Topic, err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	}

	err = s.node.ProduceTopic(msgs.Topic, msgs.Msgs)
	if err != nil {
		Lg.Errorf("yith_broker(%s) replicate msgs to topic(%s) error : %v", req.RemoteAddr, msgs.Topic, err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
}
