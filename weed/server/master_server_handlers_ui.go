package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/toraft"
	"net/http"
	"time"

	ui "github.com/seaweedfs/seaweedfs/weed/server/master_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	infos["Max Volume Id"] = ms.Topo.GetMaxVolumeId()

	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	if ms.Topo.RaftRelay != nil {
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        toraft.Relay
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			util.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.RaftRelay,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}

		ui.StatusTpl.Execute(w, args)
	}
}
