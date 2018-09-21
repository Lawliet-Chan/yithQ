package yith

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"yithQ/message"
	"yithQ/meta"
	. "yithQ/util/logger"
	"yithQ/yith/conf"
)

type Serve struct {
	cfg      *conf.Config
	metadata *atomic.Value //*meta.Metadata
	node     *Node
	watcher  *Watcher
}

func NewServe(cfg *conf.Config) *Serve {
	ip, err := getLocalhostIP()
	if err != nil {
		panic(err)
	}
	s := &Serve{
		cfg:      cfg,
		metadata: &atomic.Value{},
		node:     NewNode(ip),
		watcher:  NewWatcher(cfg.ZeroAddress, cfg.HeartbeatInterval, cfg.WatchPort),
	}

	s.metadata.Store(meta.NewMetadata())

	return s
}

func (s *Serve) Run() {
	Lg.Info("yith node start run ...")
	metadata, err := s.watcher.FetchMetadata()
	if err != nil {
		Lg.Fatalf("fetch metadata from zero(%s) error : %v", s.cfg.ZeroAddress, err)
	}
	s.updateMetadata(metadata)
	go func() {
		http.HandleFunc("/", s.ReceiveMsgFromProducers)
		http.HandleFunc("/replica", s.receiveReplicaFromOtherNodes)
		http.ListenAndServe(s.cfg.ProducerPort, nil)
	}()

	go func() {
		http.HandleFunc("/", s.SendMsgToConsumers)
		http.ListenAndServe(s.cfg.ConsumerPort, nil)
	}()

	s.watcher.PushChangeToZero(meta.NodeChange, nil)
	go func() {
		Lg.Infof("send heartbeat to zero(%s)", s.cfg.ZeroAddress)
		s.watcher.SendHeartbeatToZero()
	}()

	go func() {
		metadataChan := make(chan *meta.Metadata, 0)
		go s.watcher.WatchZero(metadataChan)
		for {
			select {
			case metadata := <-metadataChan:
				if !s.checkeMetadataVersion(metadata.Version) {
					s.metadata.Store(metadata)
				}
			}
		}
	}()
	select {}

}

func (s *Serve) ReceiveMsgFromProducers(w http.ResponseWriter, req *http.Request) {
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
		err = s.node.AddTopicPartition(msgs.Topic, msgs.PartitionID, false, s.cfg.QueueConf)
		if err != nil {
			Lg.Errorf("producer(%s) produce msgs to topic(%s) [CREATE new topic partition] error : %v", req.RemoteAddr, msgs.Topic, err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		//通知zero
		s.watcher.PushChangeToZero(meta.TopicAddChange, meta.TopicMetadata{
			Topic:          msgs.Topic,
			PartitionID:    msgs.PartitionID,
			IsReplica:      false,
			ReplicaFactory: s.cfg.ReplicaFactory,
		})
	}

	var replicaErrCh chan error
	var wg sync.WaitGroup
	if s.cfg.ReplicaFactory != 0 {
		go s.replicateToOtherNodes(msgs.Topic, data, replicaErrCh, wg)
	}
	err = s.node.ProduceTopicPartition(msgs.Topic, msgs.PartitionID, msgs.Msgs)
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

func (s *Serve) SendMsgToConsumers(w http.ResponseWriter, req *http.Request) {
	topic := req.URL.Query()["topic"][0]
	offsetStr := req.URL.Query()["offset"][0]
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(string(err.Error())))
		return
	}
	err = s.node.Consume(topic, offset, w)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(string(err.Error())))
		return
	}
	/*
		data, err := json.Marshal(msgs)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(string(err.Error())))
			return
		}

		w.Write(data)
	*/
}

func (s *Serve) checkeMetadataVersion(metaVersion uint32) bool {
	return s.metadata.Load().(*meta.Metadata).Version == metaVersion
}

func (s *Serve) updateMetadata(metadata *meta.Metadata) {
	s.metadata.Store(metadata)
}

func getLocalhostIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", nil
}
