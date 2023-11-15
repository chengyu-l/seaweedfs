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

package api

import (
	"context"
	raftrelay "github.com/seaweedfs/seaweedfs/weed/raftstore/raftrelay"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"

	"google.golang.org/grpc"
)

type (
	PutResponse    pb.PutResponse
	GetResponse    pb.RangeResponse
	DeleteResponse pb.DeleteRangeResponse
	TxnResponse    pb.TxnResponse
)

type KV interface {
	// Put puts a key-value pair into etcd.
	// Note that key,value can be plain bytes array and string is
	// an immutable representation of that bytes array.
	// To get a string of bytes, do string([]byte{0x10, 0x20}).
	Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error)

	// Get retrieves keys.
	// By default, Get will return the value for "key", if any.
	// When passed WithRange(end), Get will return the keys in the range [key, end).
	// When passed WithFromKey(), Get returns keys greater than or equal to key.
	// When passed WithLimit(limit), the number of returned keys is bounded by limit.
	// When passed WithSort(), the keys will be sorted.
	Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error)

	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error)

	// Do applies a single Op on KV without a transaction.
	// Do is useful when creating arbitrary operations to be issued at a
	// later time; the user can range over the operations, calling Do to
	// execute them. Get/Put/Delete, on the other hand, are best suited
	// for when the operation should be issued at the time of declaration.
	Do(ctx context.Context, op Op) (OpResponse, error)

	// Txn creates a transaction.
	Txn(ctx context.Context) Txn
}

type OpResponse struct {
	put *PutResponse
	get *GetResponse
	del *DeleteResponse
	txn *TxnResponse
}

func (op OpResponse) Put() *PutResponse    { return op.put }
func (op OpResponse) Get() *GetResponse    { return op.get }
func (op OpResponse) Del() *DeleteResponse { return op.del }
func (op OpResponse) Txn() *TxnResponse    { return op.txn }

func (resp *PutResponse) OpResponse() OpResponse {
	return OpResponse{put: resp}
}
func (resp *GetResponse) OpResponse() OpResponse {
	return OpResponse{get: resp}
}
func (resp *DeleteResponse) OpResponse() OpResponse {
	return OpResponse{del: resp}
}
func (resp *TxnResponse) OpResponse() OpResponse {
	return OpResponse{txn: resp}
}

type kv struct {
	rr *raftrelay.RaftRelay
}

func NewKV(rr *raftrelay.RaftRelay) KV {
	return &kv{rr: rr}
}

func (kv *kv) Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error) {
	r, err := kv.Do(ctx, OpPut(key, val, opts...))
	return r.put, err
}

func (kv *kv) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	r, err := kv.Do(ctx, OpGet(key, opts...))
	return r.get, err
}

func (kv *kv) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	r, err := kv.Do(ctx, OpDelete(key, opts...))
	return r.del, err
}

func (kv *kv) Txn(ctx context.Context) Txn {
	return &txn{
		remote: kv,
		ctx:    ctx,
	}
}

func (kv *kv) txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	return kv.rr.Txn(ctx, r)
}

func (kv *kv) Do(ctx context.Context, op Op) (OpResponse, error) {
	var err error
	switch op.t {
	case tRange:
		var resp *pb.RangeResponse
		resp, err = kv.rr.Range(ctx, op.toRangeRequest())
		if err == nil {
			return OpResponse{get: (*GetResponse)(resp)}, nil
		}
	case tPut:
		var resp *pb.PutResponse
		r := &pb.PutRequest{Key: op.key, Value: op.val}
		resp, err = kv.rr.Put(ctx, r)
		if err == nil {
			return OpResponse{put: (*PutResponse)(resp)}, nil
		}
	case tDeleteRange:
		var resp *pb.DeleteRangeResponse
		r := &pb.DeleteRangeRequest{Key: op.key, RangeEnd: op.end}
		resp, err = kv.rr.DeleteRange(ctx, r)
		if err == nil {
			return OpResponse{del: (*DeleteResponse)(resp)}, nil
		}
	case tTxn:
		var resp *pb.TxnResponse
		resp, err = kv.rr.Txn(ctx, op.toTxnRequest())
		if err == nil {
			return OpResponse{txn: (*TxnResponse)(resp)}, nil
		}
	default:
		panic("Unknown op")
	}
	return OpResponse{}, err
}

type RPCKV struct {
	remote   pb.KVClient
	callOpts []grpc.CallOption
}

func (kv *RPCKV) txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	return kv.remote.Txn(ctx, r)
}

func NewRPCKV(cc *grpc.ClientConn, callOpts []grpc.CallOption) KV {
	return &RPCKV{
		remote:   pb.NewKVClient(cc),
		callOpts: callOpts,
	}
}

func (kv *RPCKV) Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error) {
	r, err := kv.Do(ctx, OpPut(key, val, opts...))
	return r.put, err
}

func (kv *RPCKV) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	r, err := kv.Do(ctx, OpGet(key, opts...))
	return r.get, err
}

func (kv *RPCKV) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	r, err := kv.Do(ctx, OpDelete(key, opts...))
	return r.del, err
}

func (kv *RPCKV) Txn(ctx context.Context) Txn {
	return &txn{
		remote: kv,
		ctx:    ctx,
	}
}

func (kv *RPCKV) Do(ctx context.Context, op Op) (OpResponse, error) {
	var err error
	switch op.t {
	case tRange:
		var resp *pb.RangeResponse
		resp, err = kv.remote.Range(ctx, op.toRangeRequest(), kv.callOpts...)
		if err == nil {
			return OpResponse{get: (*GetResponse)(resp)}, nil
		}
	case tPut:
		var resp *pb.PutResponse
		r := &pb.PutRequest{Key: op.key, Value: op.val}
		resp, err = kv.remote.Put(ctx, r, kv.callOpts...)
		if err == nil {
			return OpResponse{put: (*PutResponse)(resp)}, nil
		}
	case tDeleteRange:
		var resp *pb.DeleteRangeResponse
		r := &pb.DeleteRangeRequest{Key: op.key, RangeEnd: op.end}
		resp, err = kv.remote.DeleteRange(ctx, r, kv.callOpts...)
		if err == nil {
			return OpResponse{del: (*DeleteResponse)(resp)}, nil
		}
	case tTxn:
		var resp *pb.TxnResponse
		resp, err = kv.remote.Txn(ctx, op.toTxnRequest(), kv.callOpts...)
		if err == nil {
			return OpResponse{txn: (*TxnResponse)(resp)}, nil
		}
	default:
		panic("Unknown op")
	}
	return OpResponse{}, err
}

type header struct {
	clusterID uint64
	memberID  uint64
	rsg       raftrelay.RaftStatusGetter
}

func newHeader(rr *raftrelay.RaftRelay) *header {
	return &header{
		clusterID: uint64(rr.ClusterID()),
		memberID:  uint64(rr.LocalID()),
		rsg:       rr,
	}
}

func (h *header) fillHeader(rh *pb.ResponseHeader) {
	if rh == nil {
		panic("the header must not be nil")
	}
	rh.ClusterId = h.clusterID
	rh.MemberId = h.memberID
	rh.RaftTerm = h.rsg.Term()
}

type kvServer struct {
	h  *header
	kv raftrelay.RaftKV
}

func NewKVServer(rr *raftrelay.RaftRelay) pb.KVServer {
	return &kvServer{
		h:  newHeader(rr),
		kv: rr,
	}
}

func (s *kvServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp, err := s.kv.Range(ctx, r)
	if err != nil {
		return nil, err
	}
	s.h.fillHeader(resp.Header)
	return resp, nil
}

func (s *kvServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	resp, err := s.kv.Put(ctx, r)
	if err != nil {
		return nil, err
	}
	s.h.fillHeader(resp.Header)
	return resp, nil
}

func (s *kvServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := s.kv.DeleteRange(ctx, r)
	if err != nil {
		return nil, err
	}
	s.h.fillHeader(resp.Header)
	return resp, nil
}

func (s *kvServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	resp, err := s.kv.Txn(ctx, r)
	if err != nil {
		return nil, err
	}
	s.h.fillHeader(resp.Header)
	return resp, nil
}
