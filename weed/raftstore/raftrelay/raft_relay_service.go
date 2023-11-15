package raftrelay

import "net/http"

type Service interface {
	AdminService
	PeerService
}

type PeerService interface {
	RaftHandler() http.Handler
}

func (rr *RaftRelay) RaftHandler() http.Handler { return rr.raftNode.Transport.Handler() }
