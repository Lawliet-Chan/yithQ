package yith

import (
	"bytes"
	"net/http"
	"sync"
)

var NoneErr error

func (s *Server) replicateToOtherNodes(topic string, msgs []byte, replicaErrCh chan error, wg sync.WaitGroup) {
	replicaNodes := s.metadata.FindReplicaNodes(topic)
	replicaErrCh = make(chan error, len(replicaNodes))
	wg.Add(len(replicaNodes))
	for _, node := range replicaNodes {
		go func(node string) {
			resp, err := http.Post(node, "application/json", bytes.NewBuffer(msgs))
			if err != nil {
				replicaErrCh <- err
				return
			}
			defer resp.Body.Close()
			replicaErrCh <- NoneErr
			wg.Done()
		}(node)
	}
}

func (s *Server) receiveReplicaFromOtherNodes() {

}
