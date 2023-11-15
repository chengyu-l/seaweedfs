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
	"bytes"
	"sort"
)

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	buckets map[string]*bucketBuffer
}

func (txb *txBuffer) reset() {
	for bucket, _ := range txb.buckets {
		delete(txb.buckets, bucket)
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
type txWriteBuffer struct {
	txBuffer
}

func (txw *txWriteBuffer) put(bucket, k, v []byte) {
	b, ok := txw.buckets[string(bucket)]
	if !ok {
		b = newBucketBuffer()
		txw.buckets[string(bucket)] = b
	}
	b.add(k, v)
}

func (txw *txWriteBuffer) del(bucket, k []byte) {
	b, ok := txw.buckets[string(bucket)]
	if !ok {
		b = newBucketBuffer()
		txw.buckets[string(bucket)] = b
	}
	b.del(k)
}

func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	for bucket, wb := range txw.buckets {
		rb, ok := txr.buckets[bucket]
		if !ok {
			delete(txw.buckets, bucket)
			txr.buckets[bucket] = wb
			continue
		}
		rb.merge(wb)
	}
	txw.reset()
}

// txReadBuffer accesses buffered updates.
type txReadBuffer struct{ txBuffer }

func (txr *txReadBuffer) Get(bucketName, key []byte) (val []byte, deleted bool) {
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.Get(key)
	}
	return nil, false
}

func (txr *txReadBuffer) DeletedKeys(bucketName []byte) map[string]bool {
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.DeletedKeys()
	}
	return nil
}

func (txr *txReadBuffer) MergeRange(bucketName, key, endKey []byte, kvs map[string][]byte, greaterThan bool) (ks [][]byte, vs [][]byte) {
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.MergeRange(key, endKey, kvs, greaterThan)
	}
	return mapToSortSlices(kvs)
}

// unsafeCopy returns a copy of txReadBuffer, caller should acquire backend.readTx.RLock()
func (txr *txReadBuffer) unsafeCopy() txReadBuffer {
	txrCopy := txReadBuffer{
		txBuffer: txBuffer{
			buckets: make(map[string]*bucketBuffer, len(txr.txBuffer.buckets)),
		},
	}
	for bucketName, bucket := range txr.txBuffer.buckets {
		txrCopy.txBuffer.buckets[bucketName] = bucket.Copy()
	}
	return txrCopy
}

// bucketBuffer buffers key-value pairs that are pending commit.
type bucketBuffer struct {
	added   map[string][]byte // new added Key-Value
	deleted map[string]bool   // deleted Key
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{added: make(map[string][]byte), deleted: make(map[string]bool)}
}

func (bb *bucketBuffer) DeletedKeys() map[string]bool {
	return bb.deleted
}

func (bb *bucketBuffer) Get(key []byte) (val []byte, deleted bool) {
	if val, ok := bb.added[string(key)]; ok {
		return val, false
	}
	if _, ok := bb.deleted[string(key)]; ok {
		return nil, true
	}
	return nil, false
}

// MergeRange merge the range result with cache
func (bb *bucketBuffer) MergeRange(key, endKey []byte, kvs map[string][]byte, greaterThan bool) (ks [][]byte, vs [][]byte) {
	if kvs == nil {
		kvs = make(map[string][]byte)
	}
	startKey := string(key)
	lastKey := string(endKey)
	for k, val := range bb.added {
		if k >= startKey {
			if greaterThan {
				kvs[k] = val
			} else if k < lastKey {
				kvs[k] = val
			}
		}
	}
	return mapToSortSlices(kvs)
}

func mapToSortSlices(kvs map[string][]byte) (ks [][]byte, vs [][]byte) {
	for key := range kvs {
		ks = append(ks, []byte(key))
	}
	sort.Slice(ks, func(i, j int) bool { return bytes.Compare(ks[i], ks[j]) <= 0 })
	for _, k := range ks {
		vs = append(vs, kvs[string(k)])
	}
	return
}

func (bb *bucketBuffer) add(k, v []byte) {
	bb.added[string(k)] = v
}

func (bb *bucketBuffer) del(k []byte) {
	key := string(k)
	if _, exist := bb.added[key]; exist {
		delete(bb.added, key)
		return
	}
	bb.deleted[key] = true
}

// merge merges data from bb into bbsrc.
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	// Update exist key or add new key
	for key, val := range bbsrc.added {
		bb.added[key] = val
	}
	// Delete key
	for key, _ := range bbsrc.deleted {
		if _, exist := bb.added[key]; exist {
			delete(bb.added, key)
		} else {
			bb.deleted[key] = true
		}
	}
}

func (bb *bucketBuffer) Copy() *bucketBuffer {
	bbCopy := bucketBuffer{
		added:   make(map[string][]byte, len(bb.added)),
		deleted: make(map[string]bool, len(bb.deleted)),
	}
	for key, val := range bb.added {
		bbCopy.added[key] = val
	}
	for key := range bb.deleted {
		bbCopy.deleted[key] = true
	}
	return &bbCopy
}
