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

package kv

import (
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	backend "github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/backend"
)

type storeTxnRead struct {
	s  *store
	tx backend.ReadTx
}

func (s *store) Read() TxnRead {
	s.mu.RLock()
	s.revMu.RLock()
	// backend holds b.readTx.RLock() only when creating the concurrentReadTx. After
	// ConcurrentReadTx is created, it will not block write transaction.
	tx := s.b.ConcurrentReadTx()
	tx.RLock() // RLock is no-op. concurrentReadTx does not need to be locked after it is created.
	s.revMu.RUnlock()
	return newMetricsTxnRead(&storeTxnRead{s, tx})
}

func (tr *storeTxnRead) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tr.rangeKeys(key, end, ro)
}

func (tr *storeTxnRead) End() {
	tr.tx.RUnlock() // RUnlock signals the end of concurrentReadTx.
	tr.s.mu.RUnlock()
}

type storeTxnWrite struct {
	storeTxnRead
	tx      backend.BatchTx
	changes []pb.KeyValue
}

func (s *store) Write() TxnWrite {
	s.mu.RLock()
	tx := s.b.BatchTx()
	tx.Lock()
	tw := &storeTxnWrite{
		storeTxnRead: storeTxnRead{s, tx},
		tx:           tx,
		changes:      make([]pb.KeyValue, 0, 4),
	}
	return newMetricsTxnWrite(tw)
}

func (tw *storeTxnWrite) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tw.rangeKeys(key, end, ro)
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) int64 {
	return tw.deleteRange(key, end)
}

func (tw *storeTxnWrite) Put(key, value []byte) {
	tw.put(key, value)
}

func (tw *storeTxnWrite) End() {
	// only update index if the txn modifies the mvcc state.
	if len(tw.changes) != 0 {
		tw.s.saveIndex(tw.tx)
		// hold revMu lock to prevent new read txns from opening until writeback.
		tw.s.revMu.Lock()
	}
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock()
	}
	tw.s.mu.RUnlock()
}

func (tr *storeTxnRead) rangeKeys(key, end []byte, ro RangeOptions) (*RangeResult, error) {
	limit := int(ro.Limit)
	kvs := make([]pb.KeyValue, 0, limit)
	ks, vs := tr.tx.UnsafeRange(keyBucketName, key, end, ro.Limit)
	if len(vs) != 0 {
		for i, _ := range vs {
			kvs = append(kvs, pb.KeyValue{Key: ks[i], Value: vs[i]})
		}
	}
	return &RangeResult{KVs: kvs, Count: len(kvs)}, nil
}

func (tw *storeTxnWrite) put(key, value []byte) {
	kv := pb.KeyValue{
		Key:   key,
		Value: value,
	}
	tw.tx.UnsafeSeqPut(keyBucketName, key, value)
	tw.changes = append(tw.changes, kv)
}

func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	// range keys
	res, err := tw.rangeKeys(key, end, RangeOptions{})
	if err != nil {
		return 0
	}
	// delete keys
	for _, kv := range res.KVs {
		tw.delete(kv.Key)
	}
	return int64(res.Count)
}

func (tw *storeTxnWrite) delete(key []byte) {
	kv := pb.KeyValue{
		Key: key,
	}
	tw.tx.UnsafeDelete(keyBucketName, key)
	tw.changes = append(tw.changes, kv)
}

func (tw *storeTxnWrite) Changes() []pb.KeyValue { return tw.changes }
