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

package backend

import (
	"math"
	"sync"

	bolt "go.etcd.io/bbolt"
)

type ReadTx interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()

	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
}

type readTx struct {
	// mu protects accesses to the txReadBuffer
	mu  sync.RWMutex
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	txMu    sync.RWMutex
	tx      *bolt.Tx
	buckets map[string]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	txWg *sync.WaitGroup
}

func (rt *readTx) Lock()    { rt.mu.Lock() }
func (rt *readTx) Unlock()  { rt.mu.Unlock() }
func (rt *readTx) RLock()   { rt.mu.RLock() }
func (rt *readTx) RUnlock() { rt.mu.RUnlock() }

func (rt *readTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	greaterThan := false
	if len(endKey) == 1 && endKey[0] == 0 {
		// '\0' means fetch keys that >= key.
		greaterThan = true
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	// Get a special key from cache
	if endKey == nil {
		limit = 1
		val, deleted := rt.buf.Get(bucketName, key)
		if deleted {
			return nil, nil
		}
		if val != nil {
			return [][]byte{key}, [][]byte{val}
		}
	}
	deletedKeys := rt.buf.DeletedKeys(bucketName)

	// If fetch a range or the speical key does not exist in buf, then fetch from db
	// find/cache bucket
	bn := string(bucketName)
	rt.txMu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txMu.RUnlock()
	if !ok {
		rt.txMu.Lock()
		bucket = rt.tx.Bucket(bucketName)
		rt.buckets[bn] = bucket
		rt.txMu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return nil, nil
	}
	rt.txMu.Lock()
	c := bucket.Cursor()
	rt.txMu.Unlock()
	kvs := unsafeRangeMap(c, key, endKey, limit, deletedKeys, greaterThan)
	// merge result with cache(pending write)
	return rt.buf.MergeRange(bucketName, key, endKey, kvs, greaterThan)
}

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[string]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

// TODO: create a base type for readTx and concurrentReadTx to avoid duplicated function implementation?
type concurrentReadTx struct {
	buf     txReadBuffer
	txMu    *sync.RWMutex
	tx      *bolt.Tx
	buckets map[string]*bolt.Bucket
	txWg    *sync.WaitGroup
}

func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}

// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}

// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }

func (rt *concurrentReadTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	greaterThan := false
	if len(endKey) == 1 && endKey[0] == 0 {
		// '\0' means fetch keys that >= key.
		greaterThan = true
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	// Get a special key from cache
	if endKey == nil {
		limit = 1
		val, deleted := rt.buf.Get(bucketName, key)
		if deleted {
			return nil, nil
		}
		if val != nil {
			return [][]byte{key}, [][]byte{val}
		}
	}
	deletedKeys := rt.buf.DeletedKeys(bucketName)

	// If fetch a range or the speical key does not exist in buf, then fetch from db
	// find/cache bucket
	bn := string(bucketName)
	rt.txMu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txMu.RUnlock()
	if !ok {
		rt.txMu.Lock()
		bucket = rt.tx.Bucket(bucketName)
		rt.buckets[bn] = bucket
		rt.txMu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return nil, nil
	}
	rt.txMu.Lock()
	c := bucket.Cursor()
	rt.txMu.Unlock()
	kvs := unsafeRangeMap(c, key, endKey, limit, deletedKeys, greaterThan)
	// merge result with cache(pending write)
	return rt.buf.MergeRange(bucketName, key, endKey, kvs, greaterThan)
}
