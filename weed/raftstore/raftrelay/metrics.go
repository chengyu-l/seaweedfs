// Copyright 2015 The nebulasfs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftrelay

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/runtime"
	goruntime "runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	isLearner = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "is_learner",
		Help:      "Whether or not this member is a learner. 1 if is, 0 otherwise.",
	})
	learnerPromoteFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "learner_promote_failures",
		Help:      "The total number of failed learner promotions (likely learner not ready) while this member is leader.",
	},
		[]string{"Reason"},
	)
	learnerPromoteSucceed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "learner_promote_successes",
		Help:      "The total number of successful learner promotions while this member is leader.",
	})
	slowApplies = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "slow_apply_total",
		Help:      "The total number of slow apply requests (likely overloaded from slow disk).",
	})
	applySnapshotInProgress = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "snapshot_apply_in_progress_total",
		Help:      "1 if the server is applying the incoming snapshot. 0 if none.",
	})
	proposalsApplied = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "proposals_applied_total",
		Help:      "The total number of consensus proposals applied.",
	})
	proposalsPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "proposals_pending",
		Help:      "The current number of pending proposals to commit.",
	})
	proposalsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "proposals_failed_total",
		Help:      "The total number of failed proposals seen.",
	})
	slowReadIndex = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "slow_read_indexes_total",
		Help:      "The total number of pending read indexes not in sync with leader's or timed out read index requests.",
	})
	readIndexFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "read_indexes_failed_total",
		Help:      "The total number of failed read indexes seen.",
	})
	quotaBackendBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "quota_backend_bytes",
		Help:      "Current backend storage quota size in bytes.",
	})
	currentGoVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "go_version",
		Help:      "Which Go version server is running with. 1 for 'server_go_version' label with current version.",
	},
		[]string{"server_go_version"})
	serverID = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "server",
		Name:      "id",
		Help:      "Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID.",
	},
		[]string{"server_id"})
)

func init() {
	prometheus.MustRegister(slowApplies)
	prometheus.MustRegister(applySnapshotInProgress)
	prometheus.MustRegister(proposalsApplied)
	prometheus.MustRegister(proposalsPending)
	prometheus.MustRegister(proposalsFailed)
	prometheus.MustRegister(slowReadIndex)
	prometheus.MustRegister(readIndexFailed)
	prometheus.MustRegister(quotaBackendBytes)
	prometheus.MustRegister(currentGoVersion)
	prometheus.MustRegister(serverID)
	prometheus.MustRegister(isLearner)
	prometheus.MustRegister(learnerPromoteSucceed)
	prometheus.MustRegister(learnerPromoteFailed)

	currentGoVersion.With(prometheus.Labels{
		"server_go_version": goruntime.Version(),
	}).Set(1)
}

func monitorFileDescriptor(lg *zap.Logger, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		used, err := runtime.FDUsage()
		if err != nil {
			if lg != nil {
				lg.Warn("failed to get file descriptor usage", zap.Error(err))
			} else {
				glog.Errorf("cannot monitor file descriptor usage (%v)", err)
			}
			return
		}
		limit, err := runtime.FDLimit()
		if err != nil {
			if lg != nil {
				lg.Warn("failed to get file descriptor limit", zap.Error(err))
			} else {
				glog.Errorf("cannot monitor file descriptor usage (%v)", err)
			}
			return
		}
		if used >= limit/5*4 {
			if lg != nil {
				lg.Warn("80%% of file descriptors are used", zap.Uint64("used", used), zap.Uint64("limit", limit))
			} else {
				glog.Warningf("80%% of the file descriptor limit is used [used = %d, limit = %d]", used, limit)
			}
		}
		select {
		case <-ticker.C:
		case <-done:
			return
		}
	}
}
