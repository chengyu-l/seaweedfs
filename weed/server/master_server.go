package weed_server

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	SequencerType        = "master.sequencer.type"
	SequencerSnowflakeId = "master.sequencer.sequencer_snowflake_id"
)

type MasterOption struct {
	Master            pb.ServerAddress
	MetaFolder        string
	VolumeSizeLimitMB uint32
	VolumePreallocate bool
	// PulseSeconds            int
	DefaultReplicaPlacement string
	GarbageThreshold        float64
	WhiteList               []string
	DisableHttp             bool
	MetricsAddress          string
	MetricsIntervalSec      int
	IsFollower              bool
}

type MasterServer struct {
	master_pb.UnimplementedSeaweedServer
	option *MasterOption
	guard  *security.Guard

	preallocateSize int64

	Topo *topology.Topology
	vg   *topology.VolumeGrowth
	vgCh chan *topology.VolumeGrowRequest

	boundedLeaderChan chan int

	// notifying clients
	clientChansLock sync.RWMutex
	clientChans     map[string]chan *master_pb.KeepConnectedResponse

	grpcDialOption grpc.DialOption

	MasterClient *wdclient.MasterClient

	adminLocks *AdminLocks

	Cluster *cluster.Cluster
}

func NewMasterServer(r *mux.Router, option *MasterOption, peers map[string]pb.ServerAddress) *MasterServer {

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	v.SetDefault("master.replication.treat_replication_as_minimums", false)
	replicationAsMin := v.GetBool("master.replication.treat_replication_as_minimums")

	v.SetDefault("master.volume_growth.copy_1", 7)
	v.SetDefault("master.volume_growth.copy_2", 6)
	v.SetDefault("master.volume_growth.copy_3", 3)
	v.SetDefault("master.volume_growth.copy_other", 1)
	v.SetDefault("master.volume_growth.threshold", 0.9)

	var preallocateSize int64
	if option.VolumePreallocate {
		preallocateSize = int64(option.VolumeSizeLimitMB) * (1 << 20)
	}

	grpcDialOption := security.LoadClientTLS(v, "grpc.master")
	ms := &MasterServer{
		option:          option,
		preallocateSize: preallocateSize,
		vgCh:            make(chan *topology.VolumeGrowRequest, 1<<6),
		clientChans:     make(map[string]chan *master_pb.KeepConnectedResponse),
		grpcDialOption:  grpcDialOption,
		MasterClient:    wdclient.NewMasterClient(grpcDialOption, "", cluster.MasterType, option.Master, "", "", peers),
		adminLocks:      NewAdminLocks(),
		Cluster:         cluster.NewCluster(),
	}
	ms.boundedLeaderChan = make(chan int, 16)

	seq := ms.createSequencer(option)
	if nil == seq {
		glog.Fatalf("create sequencer failed.")
	}
	ms.Topo = topology.NewTopology("topo", seq, uint64(ms.option.VolumeSizeLimitMB)*1024*1024, 5, replicationAsMin)
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", ms.option.VolumeSizeLimitMB, "MB")

	ms.guard = security.NewGuard(ms.option.WhiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	handleStaticResources2(r)
	r.HandleFunc("/", ms.proxyToLeader(ms.uiStatusHandler))
	r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	if !ms.option.DisableHttp {
		r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(ms.dirAssignHandler)))
		r.HandleFunc("/dir/lookup", ms.guard.WhiteList(ms.dirLookupHandler))
		r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(ms.dirStatusHandler)))
		r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(ms.collectionDeleteHandler)))
		r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeGrowHandler)))
		r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeStatusHandler)))
		r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeVacuumHandler)))
		r.HandleFunc("/submit", ms.guard.WhiteList(ms.submitFromMasterServerHandler))
		/*
			r.HandleFunc("/stats/health", ms.guard.WhiteList(statsHealthHandler))
			r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
			r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
		*/
		r.HandleFunc("/{fileId}", ms.redirectHandler)
	}

	ms.Topo.StartRefreshWritableVolumes(
		ms.grpcDialOption,
		ms.option.GarbageThreshold,
		v.GetFloat64("master.volume_growth.threshold"),
		ms.preallocateSize,
	)

	ms.ProcessGrowRequest()

	return ms
}

func (ms *MasterServer) SetRaftServer(raftRelay *RaftRelay) {
	var raftServerName string

	ms.Topo.RaftServerAccessLock.Lock()
	ms.Topo.RaftRelay = raftRelay.Relay
	raftServerName = fmt.Sprintf("[%s]", ms.Topo.RaftRelay.Name())
	ms.Topo.RaftServerAccessLock.Unlock()

	if ms.Topo.IsLeader() {
		glog.V(0).Infof("%s I am the leader!", raftServerName)
	} else {
		var raftServerLeader string
		ms.Topo.RaftServerAccessLock.RLock()
		if ms.Topo.RaftRelay != nil {
			raftServerLeader = ms.Topo.RaftRelay.LeaderClientAddr()
		}
		ms.Topo.RaftServerAccessLock.RUnlock()
		glog.V(0).Infof("%s %s - is the leader.", raftServerName, raftServerLeader)
	}
}

func (ms *MasterServer) proxyToLeader(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
			return
		}

		// get the current raft leader
		leaderAddr, _ := ms.Topo.MaybeLeader()
		raftServerLeader := string(leaderAddr)
		if raftServerLeader == "" {
			f(w, r)
			return
		}

		ms.boundedLeaderChan <- 1
		defer func() { <-ms.boundedLeaderChan }()
		targetUrl, err := url.Parse("http://" + raftServerLeader)
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError,
				fmt.Errorf("Leader URL http://%s Parse Error: %v", raftServerLeader, err))
			return
		}

		// proxy to leader
		glog.V(4).Infoln("proxying to leader", raftServerLeader, "RemoteAddr", r.RemoteAddr, "url", r.URL.String())
		proxy := httputil.NewSingleHostReverseProxy(targetUrl)
		director := proxy.Director
		proxy.Director = func(req *http.Request) {
			actualHost, err := security.GetActualRemoteHost(req)
			if err == nil {
				req.Header.Set("HTTP_X_FORWARDED_FOR", actualHost)
			}
			director(req)
		}
		proxy.Transport = util.Transport
		proxy.ServeHTTP(w, r)
	}
}

func (ms *MasterServer) createSequencer(option *MasterOption) sequence.Sequencer {
	var seq sequence.Sequencer
	v := util.GetViper()
	seqType := strings.ToLower(v.GetString(SequencerType))
	glog.V(1).Infof("[%s] : [%s]", SequencerType, seqType)
	switch strings.ToLower(seqType) {
	case "snowflake":
		var err error
		snowflakeId := v.GetInt(SequencerSnowflakeId)
		seq, err = sequence.NewSnowflakeSequencer(string(option.Master), snowflakeId)
		if err != nil {
			glog.Error(err)
			seq = nil
		}
	default:
		seq = sequence.NewMemorySequencer()
	}
	return seq
}

func (ms *MasterServer) Shutdown() {
	if ms.Topo != nil && ms.Topo.RaftRelay != nil {
		ms.Topo.RaftRelay.Stop()
	}
}
