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

package raftrelay

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// In the health case, there might be a small gap (10s of entries) between
	// the applied index and committed index.
	// However, if the committed entries are very heavy to apply, the gap might grow.
	// We should stop accepting new proposals if the gap growing to a certain point.
	maxGapBetweenApplyAndCommitIndex = 5000
)

type RaftKV interface {
	Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error)
	Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error)
	DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error)
}

func (rr *RaftRelay) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	var resp *pb.RangeResponse
	var err error
	defer func(start time.Time) {
		warnOfExpensiveReadOnlyRangeRequest(rr.getLogger(), start, r, resp, err)
	}(time.Now())

	if !r.Serializable {
		err = rr.linearizableReadNotify(ctx)
		if err != nil {
			return nil, err
		}
	}
	// virtual raft request
	var req pb.InternalRaftRequest
	req.ClientRequest = &pb.Request{CmdType: pb.CmdType_Range, Range: r}
	ar := rr.stateMachine.Apply(&req, raftnode.MarkIndex)
	if ar != nil {
		if ar.Resp != nil {
			resp = ar.Resp.(*pb.RangeResponse)
		}
		err = ar.Err
	}
	return resp, err
}

func (rr *RaftRelay) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	resp, err := rr.raftRequest(ctx, pb.InternalRaftRequest{ClientRequest: &pb.Request{CmdType: pb.CmdType_Put, Put: r}})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.PutResponse), nil
}

func (rr *RaftRelay) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := rr.raftRequest(ctx, pb.InternalRaftRequest{ClientRequest: &pb.Request{CmdType: pb.CmdType_Delete, DeleteRange: r}})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.DeleteRangeResponse), nil
}

func (rr *RaftRelay) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if isTxnReadonly(r) {
		if !isTxnSerializable(r) {
			err := rr.linearizableReadNotify(ctx)
			if err != nil {
				return nil, err
			}
		}
		var resp *pb.TxnResponse
		var err error

		defer func(start time.Time) {
			warnOfExpensiveReadOnlyTxnRequest(rr.getLogger(), start, r, resp, err)
		}(time.Now())

		// virtual raft request
		var req pb.InternalRaftRequest
		req.ClientRequest = &pb.Request{CmdType: pb.CmdType_Txn, Txn: r}
		ar := rr.stateMachine.Apply(&req, raftnode.MarkIndex)
		if ar != nil {
			if ar.Resp != nil {
				resp = ar.Resp.(*pb.TxnResponse)
			}
			err = ar.Err
		}
		return resp, err
	}

	resp, err := rr.raftRequest(ctx, pb.InternalRaftRequest{ClientRequest: &pb.Request{CmdType: pb.CmdType_Txn, Txn: r}})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.TxnResponse), nil
}

func isTxnSerializable(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	return true
}

func isTxnReadonly(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	return true
}

func (rr *RaftRelay) raftRequest(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	for {
		resp, err := rr.raftRequestOnce(ctx, r)
		return resp, err
	}
}

func (rr *RaftRelay) raftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	result, err := rr.processInternalRaftRequestOnce(ctx, r)
	if err != nil {
		return nil, err
	}
	if result.Err != nil {
		return nil, result.Err
	}
	return result.Resp, nil
}

func (rr *RaftRelay) processInternalRaftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (*raftnode.ApplyResult, error) {
	ai := rr.getAppliedIndex()
	ci := rr.getCommittedIndex()
	if ci > ai+maxGapBetweenApplyAndCommitIndex {
		return nil, ErrTooManyRequests
	}

	if r.ID == 0 {
		r.ID = rr.reqIDGen.Next()
	}

	data, err := proto.Marshal(&r)
	if err != nil {
		return nil, err
	}

	if len(data) > int(rr.Cfg.MaxRequestBytes) {
		return nil, ErrRequestTooLarge
	}

	id := r.ID
	ch := rr.wait.Register(id)

	cctx, cancel := context.WithTimeout(ctx, rr.Cfg.ReqTimeout())
	defer cancel()

	start := time.Now()
	err = rr.raftNode.Propose(cctx, data)
	if err != nil {
		proposalsFailed.Inc()
		rr.wait.Trigger(id, nil) // GC wait
		return nil, err
	}
	proposalsPending.Inc()
	defer proposalsPending.Dec()

	select {
	case x := <-ch:
		return x.(*raftnode.ApplyResult), nil
	case <-cctx.Done():
		proposalsFailed.Inc()
		rr.wait.Trigger(id, nil) // GC wait
		return nil, rr.parseProposeCtxErr(cctx.Err(), start)
	case <-rr.done:
		return nil, ErrStopped
	}
}

func (rr *RaftRelay) linearizableReadLoop() {
	var rs raft.ReadState

	for {
		ctxToSend := make([]byte, 8)
		id1 := rr.reqIDGen.Next()
		binary.BigEndian.PutUint64(ctxToSend, id1)
		leaderChangedNotifier := rr.LeaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			continue
		case <-rr.readwaitc:
		case <-rr.stopping:
			return
		}

		nextnr := newNotifier()

		rr.readMu.Lock()
		nr := rr.readNotifier
		rr.readNotifier = nextnr
		rr.readMu.Unlock()

		lg := rr.getLogger()
		cctx, cancel := context.WithTimeout(context.Background(), rr.Cfg.ReqTimeout())
		if err := rr.raftNode.ReadIndex(cctx, ctxToSend); err != nil {
			cancel()
			if err == raft.ErrStopped {
				return
			}
			if lg != nil {
				lg.Warn("failed to get read index from Raft", zap.Error(err))
			} else {
				glog.Errorf("failed to get read index from raft: %v", err)
			}
			readIndexFailed.Inc()
			nr.notify(err)
			continue
		}
		cancel()

		var (
			timeout bool
			done    bool
		)
		for !timeout && !done {
			select {
			case rs = <-rr.raftNode.ReadStateNotify():
				done = bytes.Equal(rs.RequestCtx, ctxToSend)
				if !done {
					// a previous request might time out. now we should ignore the response of it and
					// continue waiting for the response of the current requests.
					id2 := uint64(0)
					if len(rs.RequestCtx) == 8 {
						id2 = binary.BigEndian.Uint64(rs.RequestCtx)
					}
					if lg != nil {
						lg.Warn(
							"ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader",
							zap.Uint64("sent-request-id", id1),
							zap.Uint64("received-request-id", id2),
						)
					} else {
						glog.Warningf("ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader (request ID want %d, got %d)", id1, id2)
					}
					slowReadIndex.Inc()
				}
			case <-leaderChangedNotifier:
				timeout = true
				readIndexFailed.Inc()
				// return a retryable error.
				nr.notify(ErrLeaderChanged)
			case <-time.After(rr.Cfg.ReqTimeout()):
				if lg != nil {
					lg.Warn("timed out waiting for read index response (local node might have slow network)", zap.Duration("timeout", rr.Cfg.ReqTimeout()))
				} else {
					glog.Warningf("timed out waiting for read index response (local node might have slow network)")
				}
				nr.notify(ErrTimeout)
				timeout = true
				slowReadIndex.Inc()
			case <-rr.stopping:
				return
			}
		}
		if !done {
			continue
		}

		if ai := rr.getAppliedIndex(); ai < rs.Index {
			select {
			case <-rr.applyWait.Wait(rs.Index):
			case <-rr.stopping:
				return
			}
		}
		// unblock all l-reads requested at indices before rs.Index
		nr.notify(nil)
	}
}

func (rr *RaftRelay) linearizableReadNotify(ctx context.Context) error {
	rr.readMu.RLock()
	nc := rr.readNotifier
	rr.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	case rr.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-rr.done:
		return ErrStopped
	}
}
