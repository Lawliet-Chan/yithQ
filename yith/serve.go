package yith

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"yithQ/message"
	"yithQ/meta"
	. "yithQ/util/logger"
	"yithQ/yith/conf"
)

type Server struct {
	cfg      *conf.Config
	metadata *meta.Metadata
	node     *Node
	watcher  *Watcher
}

func (s *Server) ReceiveMsgFromProducers(w http.ResponseWriter, req *http.Request) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		Lg.Errorf("receive messages from producer(%s) error : %v", req.RemoteAddr, err)
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
	if !s.checkeMetadataVersion(msgs.MetaVersion) {
		//返回客户端，metadata已经改变
		w.WriteHeader(http.StatusMovedPermanently)
		return
	}

	if !s.node.ExistTopicPartition(msgs.Topic, msgs.PartitionID) {
		s.node.AddTopicPartition(msgs.Topic, msgs.PartitionID, false)
		//通知zero

	}

	var replicaErrCh chan error
	var wg sync.WaitGroup
	if s.cfg.ReplicaFactory != 0 {
		s.replicateToOtherNodes(msgs.Topic, data, replicaErrCh, wg)
	}
	err = s.node.Produce(msgs.Topic, msgs.PartitionID, msgs.Msgs)
	if err != nil {
		Lg.Errorf("producer(%s) produce msgs to topic(%s) error : %v", req.RemoteAddr, msgs.Topic, err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	wg.Wait()

	if len(replicaErrCh) > s.cfg.ReplicaFactory/2 {
		//失败replicate
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errors.New("more than half relication nodes sync msgs failed").Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) SendMsgToConsumers(w http.ResponseWriter, req *http.Request) {
	topic := req.URL.Query()["topic"][0]
	offsetStr := req.URL.Query()["offset"][0]
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(string(err.Error())))
		return
	}
	msgs, err := s.node.Consume(topic, offset)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(string(err.Error())))
		return
	}

	data, err := json.Marshal(msgs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(string(err.Error())))
		return
	}

	w.Write(data)

}

func (s *Server) connToZero() {

}

func (s *Server) checkeMetadataVersion(metaVersion uint32) bool {
	return s.metadata.Version() == metaVersion
}

func (s *Server) fetchMetadataFromZero() {

}

func (s *Server) pushMetadataToZero() {

}
