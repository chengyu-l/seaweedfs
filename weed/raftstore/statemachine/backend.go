// Copyright 2017 The etcd Authors
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

package statemachine

import (
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/backend"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/snap"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const DefaultQuotaBytes = int64(2 * 1024 * 1024 * 1024) // 2GB

type BoltDBBackendConfig struct {
	Logger            *zap.Logger
	StorePath         string
	SnapPath          string
	QuotaBackendBytes int64
	// BackendBatchInterval is the maximum time before commit the backend transaction.
	BackendBatchInterval time.Duration
	// BackendBatchLimit is the maximum operations before commit the backend transaction.
	BackendBatchLimit int
	// BackendFreelistType is the type of the backend boltdb freelist.
	BackendFreelistType bolt.FreelistType
}

func (b *BoltDBBackendConfig) Dump() string {
	return fmt.Sprintf("")
}

func newBackend(cfg BoltDBBackendConfig) backend.Backend {
	bcfg := backend.DefaultBackendConfig()
	bcfg.Path = cfg.StorePath
	if cfg.BackendBatchLimit != 0 {
		bcfg.BatchLimit = cfg.BackendBatchLimit
		cfg.Logger.Info("setting backend batch limit", zap.Int("batch limit", cfg.BackendBatchLimit))
	}
	if cfg.BackendBatchInterval != 0 {
		bcfg.BatchInterval = cfg.BackendBatchInterval
		cfg.Logger.Info("setting backend batch interval", zap.Duration("batch interval", cfg.BackendBatchInterval))
	}
	bcfg.BackendFreelistType = cfg.BackendFreelistType
	bcfg.Logger = cfg.Logger
	if cfg.QuotaBackendBytes > 0 && cfg.QuotaBackendBytes != DefaultQuotaBytes {
		// permit 10% excess over quota for disarm
		bcfg.MmapSize = uint64(cfg.QuotaBackendBytes + cfg.QuotaBackendBytes/10)
	}
	return backend.New(bcfg)
}

// openSnapshotBackend renames a snapshot db to the current etcd db and opens it.
func openSnapshotBackend(cfg BoltDBBackendConfig, ss raftnode.SnapshotMgr, snapshot raftpb.Snapshot) (backend.Backend, error) {
	snapPath, err := ss.(*snap.Snapshotter).DBFilePath(snapshot.Metadata.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to find database snapshot file (%v)", err)
	}
	if err := os.Rename(snapPath, cfg.StorePath); err != nil {
		return nil, fmt.Errorf("failed to rename database snapshot file (%v)", err)
	}
	return openBackend(cfg), nil
}

// openBackend returns a backend using the current etcd db.
func openBackend(cfg BoltDBBackendConfig) backend.Backend {
	fn := cfg.StorePath

	now, beOpened := time.Now(), make(chan backend.Backend)
	go func() {
		beOpened <- newBackend(cfg)
	}()

	select {
	case be := <-beOpened:
		cfg.Logger.Info("opened backend db", zap.String("path", fn), zap.Duration("took", time.Since(now)))
		return be

	case <-time.After(10 * time.Second):
		cfg.Logger.Info(
			"db file is flocked by another process, or taking too long",
			zap.String("path", fn),
			zap.Duration("took", time.Since(now)),
		)
	}

	return <-beOpened
}