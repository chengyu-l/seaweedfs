// Copyright 2015 The etcd Authors
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

package kv

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	rangeCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "nebulasfs",
			Subsystem: "kv",
			Name:      "range_total",
			Help:      "Total number of ranges seen by this member.",
		})

	putCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "nebulasfs",
			Subsystem: "kv",
			Name:      "put_total",
			Help:      "Total number of puts seen by this member.",
		})

	deleteCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "nebulasfs",
			Subsystem: "kv",
			Name:      "delete_total",
			Help:      "Total number of deletes seen by this member.",
		})

	txnCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "nebulasfs",
			Subsystem: "kv",
			Name:      "txn_total",
			Help:      "Total number of txns seen by this member.",
		})

	keysGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "nebulasfs_debugging",
			Subsystem: "kv",
			Name:      "keys_total",
			Help:      "Total number of keys.",
		})

	dbTotalSize = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "kv",
		Name:      "db_total_size_in_bytes",
		Help:      "Total size of the underlying database physically allocated in bytes.",
	},
		func() float64 {
			reportDbTotalSizeInBytesMu.RLock()
			defer reportDbTotalSizeInBytesMu.RUnlock()
			return reportDbTotalSizeInBytes()
		},
	)
	reportDbTotalSizeInBytesMu sync.RWMutex
	reportDbTotalSizeInBytes   = func() float64 { return 0 }

	reportDbTotalSizeInBytesDebugMu sync.RWMutex
	reportDbTotalSizeInBytesDebug   = func() float64 { return 0 }

	dbTotalSizeInUse = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "kv",
		Name:      "db_total_size_in_use_in_bytes",
		Help:      "Total size of the underlying database logically in use in bytes.",
	},
		func() float64 {
			reportDbTotalSizeInUseInBytesMu.RLock()
			defer reportDbTotalSizeInUseInBytesMu.RUnlock()
			return reportDbTotalSizeInUseInBytes()
		},
	)
	reportDbTotalSizeInUseInBytesMu sync.RWMutex
	reportDbTotalSizeInUseInBytes   = func() float64 { return 0 }

	dbOpenReadTxN = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nebulasfs",
		Subsystem: "kv",
		Name:      "db_open_read_transactions",
		Help:      "The number of currently open read transactions",
	},

		func() float64 {
			reportDbOpenReadTxNMu.RLock()
			defer reportDbOpenReadTxNMu.RUnlock()
			return reportDbOpenReadTxN()
		},
	)
	// overridden by mvcc initialization
	reportDbOpenReadTxNMu sync.RWMutex
	reportDbOpenReadTxN   = func() float64 { return 0 }
)

func init() {
	prometheus.MustRegister(rangeCounter)
	prometheus.MustRegister(putCounter)
	prometheus.MustRegister(deleteCounter)
	prometheus.MustRegister(txnCounter)
	prometheus.MustRegister(dbTotalSize)
	prometheus.MustRegister(dbTotalSizeInUse)
	prometheus.MustRegister(dbOpenReadTxN)
}
