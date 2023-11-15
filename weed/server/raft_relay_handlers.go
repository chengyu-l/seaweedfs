package weed_server

import (
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
)

func (s *RaftRelay) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

func (s *RaftRelay) redirectToLeader(w http.ResponseWriter, req *http.Request) {
	if leader, e := s.topo.Leader(); e == nil {
		//http.StatusMovedPermanently does not cause http POST following redirection
		//  "http://"+leader+req.URL.Path
		redirectUrl := fmt.Sprintf("http://%s%s", leader, req.URL.Path)
		glog.V(0).Infoln("Redirecting to", http.StatusMovedPermanently, redirectUrl)
		http.Redirect(w, req, redirectUrl, http.StatusMovedPermanently)
	} else {
		glog.V(0).Infoln("Error: Leader Unknown")
		http.Error(w, "Leader unknown", http.StatusInternalServerError)
	}
}

func (s *RaftRelay) StatusHandler(w http.ResponseWriter, r *http.Request) {
	ret := operation.ClusterStatusResult{
		IsLeader: s.topo.IsLeader(),
		Peers:    s.Peers(),
	}
	if leader, e := s.topo.Leader(); e == nil {
		ret.Leader = leader.String()
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (s *RaftRelay) HealthzHandler(w http.ResponseWriter, r *http.Request) {
	_, err := s.topo.Leader()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (s *RaftRelay) StatsRaftHandler(w http.ResponseWriter, r *http.Request) {
	writeJsonQuiet(w, r, http.StatusNotFound, nil)
}
