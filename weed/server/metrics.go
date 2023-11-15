package weed_server

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	freeStorageSpace = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "free_space",
		Help:      "The free space from volume server.",
	},
		[]string{"volume_server"},
	)

	totalStorageSpace = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "total_space",
		Help:      "The total space from volume server.",
	},
		[]string{"volume_server"},
	)
)

func init() {
	prometheus.MustRegister(freeStorageSpace)
	prometheus.MustRegister(totalStorageSpace)
}
