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
	"errors"
	"sync"

	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/cindex"
	backend "github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/backend"

	"go.uber.org/zap"
)

var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	ErrClosed = errors.New("kv: closed")
)

type store struct {
	ReadView
	WriteView

	// mu read locks for txns and write locks for non-txn store changes.
	mu sync.RWMutex

	ci cindex.ConsistentIndexer

	b backend.Backend

	// revMuLock protects currentRev and compactMainRev.
	// Locked at end of write txn and released after write txn unlock lock.
	// Locked before locking read txn and released after locking.
	revMu sync.RWMutex
	// currentRev is the revision of the last completed transaction.
	currentRev int64

	stopc chan struct{}

	lg *zap.Logger
}

func NewKV(lg *zap.Logger, b backend.Backend, ci cindex.ConsistentIndexer) KV {
	return NewStore(lg, b, ci)
}

func NewStore(lg *zap.Logger, b backend.Backend, ci cindex.ConsistentIndexer) *store {
	s := &store{
		b:  b,
		ci: ci,

		currentRev: 1,

		stopc: make(chan struct{}),

		lg: lg,
	}
	s.ReadView = &readView{s}
	s.WriteView = &writeView{s}

	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(keyBucketName)
	tx.UnsafeCreateBucket(metaBucketName)
	tx.Unlock()
	s.b.ForceCommit()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.restore(); err != nil {
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.b.BatchTx()
	tx.Lock()
	s.saveIndex(tx)
	tx.Unlock()
	s.b.ForceCommit()
}

func (s *store) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.stopc)

	s.b = b
	s.currentRev = 1
	s.stopc = make(chan struct{})
	s.ci.SetBatchTx(b.BatchTx())
	s.ci.SetConsistentIndex(0)

	return s.restore()
}

func (s *store) restore() error {
	b := s.b
	reportDbTotalSizeInBytesMu.Lock()
	reportDbTotalSizeInBytes = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesMu.Unlock()
	reportDbTotalSizeInUseInBytesMu.Lock()
	reportDbTotalSizeInUseInBytes = func() float64 { return float64(b.SizeInUse()) }
	reportDbTotalSizeInUseInBytesMu.Unlock()
	reportDbOpenReadTxNMu.Lock()
	reportDbOpenReadTxN = func() float64 { return float64(b.OpenReadTxN()) }
	reportDbOpenReadTxNMu.Unlock()

	return nil
}

type revKeyValue struct {
	key  []byte
	kv   pb.KeyValue
	kstr string
}

func (s *store) Close() error {
	close(s.stopc)
	return nil
}

func (s *store) saveIndex(tx backend.BatchTx) {
	if s.ci != nil {
		s.ci.UnsafeSave(tx)
	}
	return
}

func (s *store) ConsistentIndex() uint64 {
	if s.ci != nil {
		return s.ci.ConsistentIndex()
	}
	return 0
}
