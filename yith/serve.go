package yith

import (
	"yithQ/meta"
	"net/http"
)

type Server struct {
	metadata *meta.Metadata
	node *Node
}

func (s *Server) ReceiveMsgFromProducers(w http.ResponseWriter, req *http.Request) {

}

func (s *Server) SendMsgToConsumers(w http.ResponseWriter, req *http.Request)  {

}

