package command

import (
	"fmt"
	raftflags "github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/flags"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftrelay"
	"github.com/soheilhy/cmux"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/gorilla/mux"
	"google.golang.org/grpc/reflection"

	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	m MasterOptions
)

type MasterOptions struct {
	port              *int
	portGrpc          *int
	ip                *string
	ipBind            *string
	metaFolder        *string
	peers             *string
	volumeSizeLimitMB *uint
	volumePreallocate *bool
	// pulseSeconds       *int
	defaultReplication *string
	garbageThreshold   *float64
	whiteList          *string
	disableHttp        *bool
	metricsAddress     *string
	metricsIntervalSec *int
	raftResumeState    *bool
	metricsHttpPort    *int
	heartbeatInterval  *time.Duration
	electionTimeout    *time.Duration
	raftHashicorp      *bool
	raftBootstrap      *bool

	// nebulas
	clusterId      *string
	name           *string
	LPUrls         *string
	LogOutputs     *string
	LogLevel       *string
	InitialCluster *string
	ClusterState   *string
}

func init() {
	cmdMaster.Run = runMaster // break init cycle
	m.port = cmdMaster.Flag.Int("port", 9333, "http listen port")
	m.portGrpc = cmdMaster.Flag.Int("port.grpc", 0, "grpc listen port")
	m.ip = cmdMaster.Flag.String("ip", util.DetectedHostAddress(), "master <ip>|<server> address, also used as identifier")
	m.ipBind = cmdMaster.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	m.metaFolder = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	m.peers = cmdMaster.Flag.String("peers", "", "all master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095")
	m.volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	m.volumePreallocate = cmdMaster.Flag.Bool("volumePreallocate", false, "Preallocate disk space for volumes.")
	// m.pulseSeconds = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	m.defaultReplication = cmdMaster.Flag.String("defaultReplication", "", "Default replication type if not specified.")
	m.garbageThreshold = cmdMaster.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	m.whiteList = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	m.disableHttp = cmdMaster.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	m.metricsAddress = cmdMaster.Flag.String("metrics.address", "", "Prometheus gateway address <host>:<port>")
	m.metricsIntervalSec = cmdMaster.Flag.Int("metrics.intervalSeconds", 15, "Prometheus push interval in seconds")
	m.metricsHttpPort = cmdMaster.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	m.raftResumeState = cmdMaster.Flag.Bool("resumeState", false, "resume previous state on start master server")
	m.heartbeatInterval = cmdMaster.Flag.Duration("heartbeatInterval", 300*time.Millisecond, "heartbeat interval of master servers, and will be randomly multiplied by [1, 1.25)")
	m.electionTimeout = cmdMaster.Flag.Duration("electionTimeout", 10*time.Second, "election timeout of master servers")
	m.raftHashicorp = cmdMaster.Flag.Bool("raftHashicorp", false, "use hashicorp raft")
	m.raftBootstrap = cmdMaster.Flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")

	m.clusterId = cmdMaster.Flag.String("cluster", "", "cluster id")
	m.LogOutputs = cmdMaster.Flag.String("raftLog-outputs", "", "Specify 'stdout' or 'stderr' to skip journald logging even when running under systemd, or list of comma separated output targets.")
	m.LogLevel = cmdMaster.Flag.String("raftLog-level", "", "Configures log level for raft. Only supports debug, info, warn, error, panic, or fatal. Default 'info'.")
	m.name = cmdMaster.Flag.String("name", "", "Human-readable name for this member.")
	m.LPUrls = cmdMaster.Flag.String("listen-peer-urls", "", "List of URLs to listen on for peer traffic.")
	m.InitialCluster = cmdMaster.Flag.String("initial-cluster", "", "Initial cluster configuration for bootstrapping.")
	m.ClusterState = cmdMaster.Flag.String("initial-cluster-state", "", "Initial cluster state ('new' or 'existing').")
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service and sequence number of file ids

	The configuration file "security.toml" is read from ".", "$HOME/.seaweedfs/", "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order.

	The example security.toml configuration file can be generated by "weed scaffold -config=security"

  `,
}

var (
	masterCpuProfile = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")
	masterMemProfile = cmdMaster.Flag.String("memprofile", "", "memory profile output file")
)

func runMaster(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)
	util.LoadConfiguration("master", false)

	grace.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	parent, _ := util.FullPath(*m.metaFolder).DirAndName()
	if util.FileExists(string(parent)) && !util.FileExists(*m.metaFolder) {
		os.MkdirAll(*m.metaFolder, 0755)
	}
	if err := util.TestFolderWritable(util.ResolvePath(*m.metaFolder)); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *m.metaFolder, err)
	}

	masterWhiteList := util.StringSplit(*m.whiteList, ",")
	if *m.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	go stats_collect.StartMetricsServer(*m.ipBind, *m.metricsHttpPort)
	startMaster(m, masterWhiteList)

	return true
}

func startMaster(masterOption MasterOptions, masterWhiteList []string) {

	backend.LoadConfiguration(util.GetViper())

	if *masterOption.ipBind == "" {
		*masterOption.ipBind = *masterOption.ip
	}

	_, peers := checkPeers(*masterOption.ip, *masterOption.port, *masterOption.portGrpc, *masterOption.peers)

	masterPeers := make(map[string]pb.ServerAddress)
	for _, peer := range peers {
		masterPeers[string(peer)] = peer
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, masterOption.toMasterOption(masterWhiteList), masterPeers)

	raftOpt, _ := masterOption.GetRaftLaunchConfig()
	raftRelay := weed_server.NewRaftRelay(r, *masterOption.clusterId, ms.Topo, raftOpt)
	ms.SetRaftServer(raftRelay)

	// starting http & grpc server
	srv := startGrpServer(masterOption, ms, r, raftRelay)

	go ms.MasterClient.KeepConnectedToMaster()

	grace.OnInterrupt(ms.Shutdown)

	srv.Serve()
}

func startGrpServer(masterOption MasterOptions, ms *weed_server.MasterServer, r *mux.Router, raftRelay *weed_server.RaftRelay) cmux.CMux {
	listeningAddress := util.JoinHostPort(*masterOption.ipBind, *masterOption.port)
	glog.V(0).Infof("Start Seaweed Master %s at %s", util.Version(), listeningAddress)

	masterListener, _, e := util.NewIpAndLocalListeners(*masterOption.ipBind, *masterOption.port, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	cmux_ := cmux.New(masterListener)
	grpcL := cmux_.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := cmux_.Match(cmux.HTTP1Fast()) // serve http

	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.master"))
	master_pb.RegisterSeaweedServer(grpcS, ms)
	raftRelay.Relay.RegisterServers(grpcS)
	reflection.Register(grpcS)
	go grpcS.Serve(grpcL)
	grace.OnInterrupt(grpcS.Stop)

	httpS := &http.Server{Handler: r}
	go httpS.Serve(httpL)

	glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", util.Version(), *masterOption.ipBind, *masterOption.port)
	return cmux_
}

func checkPeers(masterIp string, masterPort int, masterGrpcPort int, peers string) (masterAddress pb.ServerAddress, cleanedPeers []pb.ServerAddress) {
	glog.V(0).Infof("current: %s:%d peers:%s", masterIp, masterPort, peers)
	masterAddress = pb.NewServerAddress(masterIp, masterPort, masterGrpcPort)
	cleanedPeers = pb.ServerAddresses(peers).ToAddresses()

	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer.ToHttpAddress() == masterAddress.ToHttpAddress() {
			hasSelf = true
			break
		}
	}

	if !hasSelf {
		cleanedPeers = append(cleanedPeers, masterAddress)
	}
	if len(cleanedPeers)%2 == 0 {
		glog.Fatalf("Only odd number of masters are supported: %+v", cleanedPeers)
	}
	return
}

func isTheFirstOne(self pb.ServerAddress, peers []pb.ServerAddress) bool {
	slices.SortFunc(peers, func(a, b pb.ServerAddress) bool {
		return strings.Compare(string(a), string(b)) < 0
	})
	if len(peers) <= 0 {
		return true
	}
	return self == peers[0]
}

func (m *MasterOptions) toMasterOption(whiteList []string) *weed_server.MasterOption {
	masterAddress := pb.NewServerAddress(*m.ip, *m.port, *m.portGrpc)
	return &weed_server.MasterOption{
		Master:            masterAddress,
		MetaFolder:        *m.metaFolder,
		VolumeSizeLimitMB: uint32(*m.volumeSizeLimitMB),
		VolumePreallocate: *m.volumePreallocate,
		// PulseSeconds:            *m.pulseSeconds,
		DefaultReplicaPlacement: *m.defaultReplication,
		GarbageThreshold:        *m.garbageThreshold,
		WhiteList:               whiteList,
		DisableHttp:             *m.disableHttp,
		MetricsAddress:          *m.metricsAddress,
		MetricsIntervalSec:      *m.metricsIntervalSec,
	}
}

func (m *MasterOptions) GetRaftLaunchConfig() (*raftrelay.LaunchConfig, error) {
	cfg := raftrelay.NewDefaultConfig()
	cfg.Name = *m.name
	cfg.Dir = util.ResolvePath(*m.metaFolder)
	cfg.InitialCluster = *m.InitialCluster
	cfg.ClusterState = *m.ClusterState
	// cfg.ElectionMs =
	// cfg.InitialElectionTickAdvance =
	// cfg.BackendBatchInterval =
	// cfg.BackendBatchLimit =
	// cfg.QuotaBackendBytes =
	// cfg.MaxTxnOps =
	// cfg.MaxRequestBytes =
	// cfg.InitialCluster =
	// cfg.PreVote =
	// cfg.ForceNewCluster =

	cfg.LogLevel = *m.LogLevel

	uniqueUrl := raftflags.NewUniqueURLsWithExceptions("", "")
	uniqueUrl.Set(*m.LPUrls)
	cfg.LPUrls = uniqueUrl.UniqueURLSlice()

	lCUrls := "http://" + m.GetClientAddress()
	uniqueUrl = raftflags.NewUniqueURLsWithExceptions("", "")
	uniqueUrl.Set(lCUrls)
	cfg.LCUrls = uniqueUrl.UniqueURLSlice()

	uniqueStr := raftflags.NewUniqueStringsValue("")
	uniqueStr.Set(*m.LogOutputs)
	cfg.LogOutputs = uniqueStr.StringSlice()
	return cfg, cfg.Validate()
}

func (m *MasterOptions) GetClientAddress() string {
	return fmt.Sprintf("%v:%d", *m.ip, *m.port)
}
