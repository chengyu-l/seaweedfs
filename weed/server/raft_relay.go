package weed_server

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftrelay"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/toraft"

	"github.com/gorilla/mux"
)

var NotLeaderError = errors.New("raft.Server: Not current leader")

type RaftRelay struct {
	Relay     toraft.Relay
	clusterId string
	router    *mux.Router
	topo      *topology.Topology
}

func NewRaftRelay(r *mux.Router, clusterId string, topo *topology.Topology, cfg *raftrelay.LaunchConfig) *RaftRelay {
	s := &RaftRelay{
		clusterId: clusterId,
		router:    r,
		topo:      topo,
	}
	var err error
	s.Relay, err = toraft.NewRelay(s.clusterId, cfg)
	if err != nil {
		glog.Fatalln(err)
	}

	go s.roleChangeHook()

	r.HandleFunc("/cluster/status", s.StatusHandler).Methods("GET")
	r.HandleFunc("/cluster/healthz", s.HealthzHandler).Methods("GET", "HEAD")

	return s
}

func (s *RaftRelay) roleChangeHook() {
	for {
		<-s.Relay.RoleChaneNotify()
		if s.Relay.IsLeader() {
			if !s.getMaxVolumeId() {
				s.getMaxVolumeId()
			}
			if !s.getMaxFid() {
				s.getMaxFid()
			}
		} else {
			freeStorageSpace.Reset()
			totalStorageSpace.Reset()
			glog.V(0).Infoln("role changed to other")
		}
	}
}

func (s *RaftRelay) getMaxVolumeId() bool {
	maxVolumeIdStr, maxVolumeErr := s.Relay.GetMaxVolumeId()
	if maxVolumeErr == nil {
		maxVolumeId64, maxVolumeErr64 := strconv.ParseUint(maxVolumeIdStr, 10, 32)
		if maxVolumeErr64 == nil {
			maxVolumeId := uint32(maxVolumeId64)
			s.topo.UpAdjustMaxVolumeId(needle.VolumeId(maxVolumeId))
			glog.V(0).Infoln("read maxVolumeId from etcd success,maxVolumeId:", maxVolumeId)
		} else {
			glog.Errorln("parse maxVolumeId64 from etcd error,", maxVolumeIdStr)
			return false
		}
	} else {
		glog.Errorln("read maxVolumeId from etcd error,", maxVolumeIdStr)
		return false
	}
	return true
}

func (s *RaftRelay) getMaxFid() bool {
	maxFidStr, maxFidStrErr := s.Relay.GetMaxFid()
	if maxFidStrErr == nil {
		maxFid, maxFidErr := strconv.ParseUint(maxFidStr, 10, 64)
		if maxFidErr == nil {
			s.topo.Sequence.Initialize(maxFid, maxFid)
			glog.V(0).Infoln("read maxFid from etcd success,maxFid:", maxFid)
		} else {
			glog.Errorln("parse maxFid from etcd err ", maxFidStr)
			return false
		}
	} else {
		glog.Errorln("read maxFid from etcd err ", maxFidStr)
		return false
	}
	return true
}

func (s *RaftRelay) Peers() (members []string) {
	peers := s.Relay.Peers()

	for _, addr := range peers.PeerAddrs() {
		members = append(members, strings.TrimPrefix(addr, "http://"))
	}
	return
}

// Join joins an existing cluster.
func (s *RaftRelay) Join(peers []string) error {
	return errors.New("Could not connect to any cluster peers")
}

// a workaround because http POST following redirection misses request body
func postFollowingOneRedirect(target string, contentType string, b bytes.Buffer) error {
	backupReader := bytes.NewReader(b.Bytes())
	resp, err := http.Post(target, contentType, &b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	reply, _ := ioutil.ReadAll(resp.Body)
	statusCode := resp.StatusCode

	if statusCode == http.StatusMovedPermanently {
		var urlStr string
		if urlStr = resp.Header.Get("Location"); urlStr == "" {
			return fmt.Errorf("%d response missing Location header", resp.StatusCode)
		}

		glog.V(0).Infoln("Post redirected to ", urlStr)
		resp2, err2 := http.Post(urlStr, contentType, backupReader)
		if err2 != nil {
			return err2
		}
		defer resp2.Body.Close()
		reply, _ = ioutil.ReadAll(resp2.Body)
		statusCode = resp2.StatusCode
	}

	glog.V(0).Infoln("Post returned status: ", statusCode, string(reply))
	if statusCode != http.StatusOK {
		return errors.New(string(reply))
	}

	return nil
}
