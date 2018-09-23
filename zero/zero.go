package zero

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"yithQ/meta"
	"yithQ/util/logger"
	"yithQ/util/router"
)

type Zero struct {
	//sync.Mutex
	//yithNodes []string
	//metadata    *meta.Metadata
	weightQueue *WeightQueue
	//center          *yapool.Center
	cfg             *Config
	metadataVersion uint32
}

func NewZero(cfg *Config) *Zero {
	return &Zero{
		//yithNodes: make([]string, 0),
		//metadata:    meta.NewMetadata(),
		weightQueue: NewWeightQueue(),
		//center:          yapool.NewCenter(cfg.ListenPort),
		cfg:             cfg,
		metadataVersion: 0,
	}
}

func (z *Zero) Run() {
	logger.Lg.Info("zero start running ...")
	go z.ListenYith()
	/*go func() {
		http.HandleFunc("/fetch_meta", z.ForFetchMetadata)
		http.ListenAndServe(z.cfg.ListenPort, nil)
	}()*/
	select {}
}

func (z *Zero) ListenYith() {
	logger.Lg.Infof("zero listen yith nodes by port %s ", z.cfg.ListenPort)
	logger.Lg.Infof("nortify yith nodes by port %s", z.cfg.YithWatchPort)

	r := router.NewRouter()
	r.HandleFunc(http.MethodGet, "/"+meta.HeartbeatStr, z.ReceiveHeartbeat)
	r.HandleFunc(http.MethodPost, "/"+meta.TopicReplicaAddChangeStr, z.AddTopicReplica)
	r.HandleFunc(http.MethodPost, "/"+meta.FetchMetadataStr, z.ForFetchMetadata)
	r.HandleFunc(http.MethodPost, "/"+meta.TopicPartitionDeleteChangeStr, z.DeleteTopicPartition)
	http.ListenAndServe(z.cfg.ListenPort, r)

	/*z.center.ReceiveWithFunc(func(remoteAddr string, msg *yapool.Msg) {
		switch msg.Level {
		case meta.TopicReplicaAddChange:
			z.addTopicReplica(remoteAddr, msg.Msg.(meta.TopicMetadata))
			z.NortifyAllYiths()
		case meta.TopicDeleteChange:
			z.deleteTopic(remoteAddr, msg.Msg.(meta.TopicMetadata))
			z.NortifyAllYiths()
			//	case meta.NodeChange:
			//		z.AddNode(remoteAddr)
			//		z.NortifyAllYiths()
			//z.yithNodes = append(z.yithNodes, remoteAddr)
		}
	},
		z.cfg.HeartbeatTimeout,
		nil,
		z.yithNodeExpire)*/

}

func (z *Zero) NortifyAllYiths() error {
	topicNodeMap := z.weightQueue.TopicNode()
	newVersion := atomic.AddUint32(&z.metadataVersion, 1)
	byt, err := meta.NewMetadata().Marshal(topicNodeMap, newVersion)
	if err != nil {
		return err
	}
	for _, nodeIp := range topicNodeMap {
		go func(nodeIp string) {
			node := strings.Split(nodeIp, ":")[0] + z.cfg.YithWatchPort
			resp, err := http.Post(node, "application/json", bytes.NewBuffer(byt))
			if err != nil {
				return
			}
			resp.Body.Close()
		}(nodeIp)

	}
	return nil
}

func (z *Zero) AddTopicReplica(w http.ResponseWriter, req *http.Request) {
	byt, err := ioutil.ReadAll(req.Body)
	if err != nil {
		logger.Lg.Errorf("yith(%s) add topic replica [read http body] error : %v", req.RemoteAddr, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var topic meta.TopicMetadata
	err = json.Unmarshal(byt, &topic)
	if err != nil {
		logger.Lg.Errorf("yith(%s) add topic(%s) replica [json decode]  error : %v", req.RemoteAddr, topic.Topic, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	z.addTopicReplica(req.RemoteAddr, topic)
}

func (z *Zero) addTopicReplica(yithNode string, topic meta.TopicMetadata) {
	nodes := z.weightQueue.PopNodesWithout(topic.ReplicaFactory, yithNode)
	for i, node := range nodes {
		z.weightQueue.Put(node, meta.TopicMetadata{
			Topic:          topic.Topic,
			PartitionID:    topic.PartitionID*100 + i,
			IsReplica:      true,
			ReplicaFactory: topic.ReplicaFactory,
		})
	}
}

/*
func (z *Zero) AddNode(yithNode string) {
	z.weightQueue.AddNode(yithNode)
}
*/

func (z *Zero) DeleteTopicPartition(w http.ResponseWriter, req *http.Request) {
	byt, err := ioutil.ReadAll(req.Body)
	if err != nil {
		logger.Lg.Errorf("yith(%s) delete topic  [read http body] error : %v", req.RemoteAddr, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var topic meta.TopicMetadata
	err = json.Unmarshal(byt, &topic)
	if err != nil {
		logger.Lg.Errorf("yith(%s) delete topic(%s)  [json decode]  error : %v", req.RemoteAddr, topic.Topic, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	z.deleteTopicPartition(req.RemoteAddr, topic)
}

func (z *Zero) deleteTopicPartition(yithNode string, topic meta.TopicMetadata) {
	//z.metadata.RemoveTopic(yithNode, topic)

}

func (z *Zero) yithNodeExpire(yithAddr string) {
	logger.Lg.Warnf("yith_node(%s) expired!", yithAddr)
	z.weightQueue.DeleteNode(yithAddr)
}

func (z *Zero) ForFetchMetadata(w http.ResponseWriter, req *http.Request) {
	topicNodeMap := z.weightQueue.TopicNode()
	version := atomic.LoadUint32(&z.metadataVersion)
	byt, err := meta.NewMetadata().Marshal(topicNodeMap, version)
	if err != nil {
		logger.Lg.Errorf("yith(%s) fetch metadata  error :%v", req.RemoteAddr, err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(byt)
}

func (z *Zero) ReceiveHeartbeat(w http.ResponseWriter, req *http.Request) {
	if !z.weightQueue.AddNode(req.RemoteAddr) {
		z.NortifyAllYiths()
	}
}
