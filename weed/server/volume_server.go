package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

type VolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	inFlightUploadDataSize        int64
	inFlightDownloadDataSize      int64
	concurrentUploadLimit         int64
	concurrentDownloadLimit       int64
	inFlightUploadDataLimitCond   *sync.Cond
	inFlightDownloadDataLimitCond *sync.Cond
	inflightUploadDataTimeout     time.Duration
	hasSlowRead                   bool
	readBufferSizeMB              int

	SeedMasterNodes []pb.ServerAddress
	currentMaster   pb.ServerAddress
	pulseSeconds    int
	dataCenter      string
	rack            string
	store           *storage.Store
	guard           *security.Guard
	grpcDialOption  grpc.DialOption

	needleMapKind           storage.NeedleMapKind
	ldbTimout               int64
	ReadMode                string
	compactionBytePerSecond int64
	metricsAddress          string
	metricsIntervalSec      int
	fileSizeLimitBytes      int64
	isHeartbeating          bool
	stopChan                chan bool
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, grpcPort int, publicUrl string,
	folders []string, maxCounts []int32, minFreeSpaces []util.MinFreeSpace, diskTypes []types.DiskType,
	idxFolder string,
	needleMapKind storage.NeedleMapKind,
	masterNodes []pb.ServerAddress, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	readMode string,
	compactionMBPerSecond int,
	fileSizeLimitMB int,
	concurrentUploadLimit int64,
	concurrentDownloadLimit int64,
	inflightUploadDataTimeout time.Duration,
	hasSlowRead bool,
	readBufferSizeMB int,
	ldbTimeout int64,
) *VolumeServer {

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
	enableUiAccess := v.GetBool("access.ui")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	vs := &VolumeServer{
		pulseSeconds:                  pulseSeconds,
		dataCenter:                    dataCenter,
		rack:                          rack,
		needleMapKind:                 needleMapKind,
		ReadMode:                      readMode,
		grpcDialOption:                security.LoadClientTLS(util.GetViper(), "grpc.volume"),
		compactionBytePerSecond:       int64(compactionMBPerSecond) * 1024 * 1024,
		fileSizeLimitBytes:            int64(fileSizeLimitMB) * 1024 * 1024,
		isHeartbeating:                true,
		stopChan:                      make(chan bool),
		inFlightUploadDataLimitCond:   sync.NewCond(new(sync.Mutex)),
		inFlightDownloadDataLimitCond: sync.NewCond(new(sync.Mutex)),
		concurrentUploadLimit:         concurrentUploadLimit,
		concurrentDownloadLimit:       concurrentDownloadLimit,
		inflightUploadDataTimeout:     inflightUploadDataTimeout,
		hasSlowRead:                   hasSlowRead,
		readBufferSizeMB:              readBufferSizeMB,
		ldbTimout:                     ldbTimeout,
	}
	vs.SeedMasterNodes = masterNodes

	vs.checkWithMaster()

	vs.store = storage.NewStore(vs.grpcDialOption, ip, port, grpcPort, publicUrl, folders, maxCounts, minFreeSpaces, idxFolder, vs.needleMapKind, diskTypes, ldbTimeout)
	vs.guard = security.NewGuard(whiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	handleStaticResources(adminMux)
	adminMux.HandleFunc("/status", vs.statusHandler)
	adminMux.HandleFunc("/healthz", vs.healthzHandler)
	// 为了兼容老版本，下个版本会删除
	adminMux.HandleFunc("/admin/assign_volume", vs.assignVolumeHandler)
	if signingKey == "" || enableUiAccess {
		// only expose the volume server details for safe environments
		adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
		/*
			adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
			adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
			adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
		*/
	}
	adminMux.HandleFunc("/", vs.privateStoreHandler)
	if publicMux != adminMux {
		// separated admin and public port
		handleStaticResources(publicMux)
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}

	go vs.heartbeat()
	go stats.LoopPushingMetric("volumeServer", util.JoinHostPort(ip, port), vs.metricsAddress, vs.metricsIntervalSec)

	return vs
}

func (vs *VolumeServer) SetStopping() {
	glog.V(0).Infoln("Stopping volume server...")
	vs.store.SetStopping()
}

func (vs *VolumeServer) LoadNewVolumes() {
	glog.V(0).Infoln(" Loading new volume ids ...")
	vs.store.LoadNewVolumes()
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}

// 该函数是为了兼容 nebulas 老版本
func (vs *VolumeServer) assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	preallocate := int64(0)
	if r.FormValue("preallocate") != "" {
		preallocate, err = strconv.ParseInt(r.FormValue("preallocate"), 10, 64)
		if err != nil {
			glog.V(0).Infof("ignoring invalid int64 value for preallocate = %v\n", r.FormValue("preallocate"))
		}
	}

	volId, err := needle.NewVolumeId(r.FormValue("volume"))
	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
		return
	}

	rp := r.FormValue("replication")
	if rp == "010" {
		rp = "001" // 两副本
	} else if rp == "020" {
		rp = "002" // 三副本
	}

	err = vs.store.AddVolume(
		volId,
		r.FormValue("collection"),
		vs.needleMapKind,
		rp,
		r.FormValue("ttl"),
		preallocate,
		0,
		types.ToDiskType("hdd"),
		vs.ldbTimout,
	)

	if err == nil {
		writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
	}
	glog.V(2).Infof("assign volume = %s, collection = %s , replication = %s, error = %v\n",
		r.FormValue("volume"), r.FormValue("collection"), r.FormValue("replication"), err)

}
