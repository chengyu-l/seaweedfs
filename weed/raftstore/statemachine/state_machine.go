// Copyright 2016 The etcd Authors
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
	"bytes"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	kvpb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/cindex"
	kv "github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/backend"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/snap"
	"sort"

	"go.uber.org/zap"
)

const (
	emptyKey = ""
)

// Implement raftnode.StateMachine interface
type StateMachine struct {
	*snap.Snapshotter
	applierBackend
	cfg *BoltDBBackendConfig

	consistIndex cindex.ConsistentIndexer
	be           backend.Backend
	kv           kv.KV
}

func NewBoltDBStateMachine(cfg *BoltDBBackendConfig) raftnode.StateMachine {
	sm := &StateMachine{
		applierBackend: applierBackend{
			Logger: cfg.Logger,
		},
		cfg: cfg}
	sm.be = openBackend(*cfg)
	sm.consistIndex = cindex.NewConsistentIndex(sm.be.BatchTx())
	sm.kv = kv.NewKV(cfg.Logger, sm.be, sm.consistIndex)
	sm.Snapshotter = snap.New(cfg.Logger, cfg.SnapPath)
	sm.applierBackend.rr = sm
	return sm
}

func (s *StateMachine) SnapshotMgr() raftnode.SnapshotMgr {
	return s.Snapshotter
}

func (s *StateMachine) DBSize() int64 {
	if s.be == nil {
		return 0
	}
	return s.be.Size()
}

func (s *StateMachine) DBSizeInUse() int64 {
	if s.be == nil {
		return 0
	}
	return s.be.SizeInUse()
}

func (s *StateMachine) Defrag() error {
	if s.be == nil {
		return fmt.Errorf("DB has not been initialzied")
	}
	return s.be.Defrag()
}

func (s *StateMachine) KV() kv.KV {
	return s.kv
}

func (s *StateMachine) RegisterClusterMembershipHook(fn func() *membership.RaftCluster) {
	s.applierBackend.Cluster = fn
}

func (s *StateMachine) RegisterRaftTermHook(fn func() uint64) {
	s.applierBackend.RaftTerm = fn
}

func (s *StateMachine) ConsistentIndex() uint64 {
	return s.consistIndex.ConsistentIndex()
}

func (s *StateMachine) CreateSnapshotMessage(m raftpb.Message) *raftnode.SnapMessage {
	// fetch a snapshot from backend storage
	stateMachineSnapshot := s.snapshot()
	// get a snapshot of state machine as readCloser
	rc := raftnode.NewSnapshotReaderCloser(s.cfg.Logger, stateMachineSnapshot)
	return raftnode.NewSnapMessage(m, rc, stateMachineSnapshot.Size())
}

func (s *StateMachine) SetConsistentIndex(index uint64) {
	s.consistIndex.SetConsistentIndex(index)
}

func (s *StateMachine) RecoverFromSnapshot(snapshot *raftpb.Snapshot) (err error) {
	if snapshot == nil {
		snapshot, err = s.Snapshotter.Load()
		if err != nil && err != snap.ErrNoSnapshot {
			return err
		}
	}
	if snapshot == nil ||
		snapshot.Metadata.Index <= s.ConsistentIndex() {
		return nil
	}
	if s.be != nil {
		go func(b backend.Backend) {
			if err := b.Close(); err != nil {
				s.cfg.Logger.Panic("failed to close old backend", zap.Error(err))
			}
		}(s.be)
	}
	s.be, err = openSnapshotBackend(*s.cfg, s.Snapshotter, *snapshot)
	if err != nil {
		return err
	}
	s.kv.Restore(s.be)
	return nil
}

// Apply to backend storage. It is the caller's responsibility to ensure the index is larger than current value.
func (s *StateMachine) Apply(cmd *pb.InternalRaftRequest, index uint64) *raftnode.ApplyResult {
	if index != raftnode.MarkIndex {
		s.consistIndex.SetConsistentIndex(index)
	}
	return s.applierBackend.Apply(cmd)
}

func (s *StateMachine) Close() {
	if s.kv != nil {
		defer s.kv.Close()
	}
	if s.be != nil {
		s.be.Close()
	}
}

func (s *StateMachine) Commit() {
	if s.kv != nil {
		s.kv.Commit()
	}
}

func (s *StateMachine) snapshot() raftnode.Snapshot {
	// Persist in-memory write batch
	s.Commit()
	if s.be == nil {
		return nil
	}
	return s.be.Snapshot()
}

type applierBackend struct {
	rr       *StateMachine
	Logger   *zap.Logger
	Cluster  func() *membership.RaftCluster
	RaftTerm func() uint64
}

func (a *applierBackend) Apply(r *pb.InternalRaftRequest) *raftnode.ApplyResult {
	ar := &raftnode.ApplyResult{}
	switch {
	case r.ClientRequest != nil:
		{
			cr := r.ClientRequest
			switch cr.CmdType {
			case pb.CmdType_Range:
				ar.Resp, ar.Err = a.Range(nil, cr.Range)
			case pb.CmdType_Put:
				ar.Resp, ar.Err = a.Put(nil, cr.Put)
			case pb.CmdType_Delete:
				ar.Resp, ar.Err = a.DeleteRange(nil, cr.DeleteRange)
			case pb.CmdType_Txn:
				ar.Resp, ar.Err = a.Txn(cr.Txn)
			default:
				panic("not implemented")
			}
		}
	case r.AdminRequest != nil:
		{
			admin := r.AdminRequest
			switch admin.CmdType {
			case pb.AdminCmdType_MemberAdd:
			case pb.AdminCmdType_MemberRemove:
			case pb.AdminCmdType_MemberPromote:
			case pb.AdminCmdType_MoveLeader:
			case pb.AdminCmdType_MemberList:
			case pb.AdminCmdType_MemberRaftAttrUpdate:
			case pb.AdminCmdType_MemberNoRaftAttrUpdate:
				ar.Resp, ar.Err = a.MemberNoRaftAttrUpdate(admin.MemberNoRaftAttrUpdate)
			case pb.AdminCmdType_InvalidAdmin:
			default:
				panic("not implemented")
			}
		}
	default:
		panic("not implememted")
	}
	return ar
}

func (a *applierBackend) Put(txn kv.TxnWrite, p *pb.PutRequest) (resp *pb.PutResponse, err error) {
	resp = &pb.PutResponse{}
	resp.Header = &pb.ResponseHeader{}
	if txn == nil {
		txn = a.rr.KV().Write()
		defer txn.End()
	}
	txn.Put(p.Key, p.Value)
	return resp, nil
}

func (a *applierBackend) DeleteRange(txn kv.TxnWrite, dr *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp := &pb.DeleteRangeResponse{}
	resp.Header = &pb.ResponseHeader{}

	if txn == nil {
		txn = a.rr.KV().Write()
		defer txn.End()
	}

	resp.Deleted = txn.DeleteRange(dr.Key, dr.RangeEnd)
	return resp, nil
}

func (a *applierBackend) Range(txn kv.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp := &pb.RangeResponse{}
	resp.Header = &pb.ResponseHeader{}

	if txn == nil {
		txn = a.rr.KV().Read()
		defer txn.End()
	}

	limit := r.Limit
	if r.SortOrder != pb.RangeRequest_NONE {
		// fetch everything; sort and truncate afterwards
		limit = 0
	}
	if limit > 0 {
		// fetch one extra for 'more' flag
		limit = limit + 1
	}

	ro := kv.RangeOptions{
		Limit: limit,
		Count: r.CountOnly,
	}

	rr, err := txn.Range(r.Key, r.RangeEnd, ro)
	if err != nil {
		return nil, err
	}

	sortOrder := r.SortOrder
	if r.SortTarget != pb.RangeRequest_KEY && sortOrder == pb.RangeRequest_NONE {
		// Since current kv.Range implementation returns results
		// sorted by keys in lexiographically ascending order,
		// sort ASCEND by default only when target is not 'KEY'
		sortOrder = pb.RangeRequest_ASCEND
	}
	if sortOrder != pb.RangeRequest_NONE {
		var sorter sort.Interface
		switch {
		case r.SortTarget == pb.RangeRequest_KEY:
			sorter = &kvSortByKey{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_VALUE:
			sorter = &kvSortByValue{&kvSort{rr.KVs}}
		}
		switch {
		case sortOrder == pb.RangeRequest_ASCEND:
			sort.Sort(sorter)
		case sortOrder == pb.RangeRequest_DESCEND:
			sort.Sort(sort.Reverse(sorter))
		}
	}

	if r.Limit > 0 && len(rr.KVs) > int(r.Limit) {
		rr.KVs = rr.KVs[:r.Limit]
		resp.More = true
	}

	resp.Count = int64(rr.Count)
	resp.Kvs = make([]*kvpb.KeyValue, len(rr.KVs))
	for i := range rr.KVs {
		if r.KeysOnly {
			rr.KVs[i].Value = nil
		}
		resp.Kvs[i] = &rr.KVs[i]
	}
	return resp, nil
}

func (a *applierBackend) Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error) {
	isWrite := !isTxnReadonly(rt)
	txn := kv.NewReadOnlyTxnWrite(a.rr.KV().Read())

	txnPath := compareToPath(txn, rt)
	txnResp, _ := newTxnResp(rt, txnPath)

	// When executing mutable txn ops, etcd must hold the txn lock so
	// readers do not see any intermediate results. Since writes are
	// serialized on the raft loop, the revision in the read view will
	// be the revision of the write txn.
	if isWrite {
		txn.End()
		txn = a.rr.KV().Write()
	}
	a.applyTxn(txn, rt, txnPath, txnResp)
	txn.End()
	return txnResp, nil
}

func (a *applierBackend) MemberNoRaftAttrUpdate(r *pb.MemberNoRaftAttrUpdateRequest) (*pb.MemberNoRaftAttrUpdateResponse, error) {
	a.Cluster().UpdateAttributes(
		types.ID(r.MemberId),
		membership.Attributes{
			Name:       r.MemberAttributes.Name,
			ClientURLs: r.MemberAttributes.ClientUrls,
		},
	)
	return &pb.MemberNoRaftAttrUpdateResponse{}, nil
}

// newTxnResp allocates a txn response for a txn request given a path.
func newTxnResp(rt *pb.TxnRequest, txnPath []bool) (txnResp *pb.TxnResponse, txnCount int) {
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	resps := make([]*pb.ResponseOp, len(reqs))
	txnResp = &pb.TxnResponse{
		Responses: resps,
		Succeeded: txnPath[0],
		Header:    &pb.ResponseHeader{},
	}
	for i, req := range reqs {
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{}}
		case *pb.RequestOp_RequestPut:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{}}
		case *pb.RequestOp_RequestDeleteRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{}}
		case *pb.RequestOp_RequestTxn:
			resp, txns := newTxnResp(tv.RequestTxn, txnPath[1:])
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: resp}}
			txnPath = txnPath[1+txns:]
			txnCount += txns + 1
		default:
		}
	}
	return txnResp, txnCount
}

func compareToPath(rv kv.ReadView, rt *pb.TxnRequest) []bool {
	txnPath := make([]bool, 1)
	ops := rt.Success
	if txnPath[0] = applyCompares(rv, rt.Compare); !txnPath[0] {
		ops = rt.Failure
	}
	for _, op := range ops {
		tv, ok := op.Request.(*pb.RequestOp_RequestTxn)
		if !ok || tv.RequestTxn == nil {
			continue
		}
		txnPath = append(txnPath, compareToPath(rv, tv.RequestTxn)...)
	}
	return txnPath
}

func applyCompares(rv kv.ReadView, cmps []*pb.Compare) bool {
	for _, c := range cmps {
		if !applyCompare(rv, c) {
			return false
		}
	}
	return true
}

// applyCompare applies the compare request.
// If the comparison succeeds, it returns true. Otherwise, returns false.
func applyCompare(rv kv.ReadView, c *pb.Compare) bool {
	// TODO: possible optimizations
	// * chunk reads for large ranges to conserve memory
	// * rewrite rules for common patterns:
	//	ex. "[a, b) createrev > 0" => "limit 1 /\ kvs > 0"
	// * caching
	rr, err := rv.Range(c.Key, c.RangeEnd, kv.RangeOptions{})
	if err != nil {
		return false
	}
	if len(rr.KVs) == 0 {
		if c.Target == pb.Compare_VALUE {
			// Always fail if comparing a value on a key/keys that doesn't exist;
			// nil == empty string in grpc; no way to represent missing value
			return false
		}
		return compareKV(c, kvpb.KeyValue{})
	}
	for _, kv := range rr.KVs {
		if !compareKV(c, kv) {
			return false
		}
	}
	return true
}

func compareKV(c *pb.Compare, ckv kvpb.KeyValue) bool {
	var result int
	switch c.Target {
	case pb.Compare_VALUE:
		v := []byte{}
		if tv, _ := c.TargetUnion.(*pb.Compare_Value); tv != nil {
			v = tv.Value
		}
		result = bytes.Compare(ckv.Value, v)
	case pb.Compare_KEY:
		// Key must not be empty if exist, then result must be +1
		result = bytes.Compare(ckv.Key, []byte(emptyKey))
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return result == 0
	case pb.Compare_NOT_EQUAL:
		return result != 0
	case pb.Compare_GREATER:
		return result > 0
	case pb.Compare_LESS:
		return result < 0
	case pb.Compare_EXIST:
		return result > 0
	case pb.Compare_NOT_EXIST:
		return result == 0
	}
	return true
}

func (a *applierBackend) applyTxn(txn kv.TxnWrite, rt *pb.TxnRequest, txnPath []bool, tresp *pb.TxnResponse) (txns int) {
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}

	lg := a.Logger
	for i, req := range reqs {
		respi := tresp.Responses[i].Response
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			resp, err := a.Range(txn, tv.RequestRange)
			if err != nil {
				if lg != nil {
					lg.Panic("unexpected error during txn", zap.Error(err))
				} else {
					glog.Fatalf("unexpected error during txn: %v", err)
				}
			}
			respi.(*pb.ResponseOp_ResponseRange).ResponseRange = resp
		case *pb.RequestOp_RequestPut:
			resp, err := a.Put(txn, tv.RequestPut)
			if err != nil {
				if lg != nil {
					lg.Panic("unexpected error during txn", zap.Error(err))
				} else {
					glog.Fatalf("unexpected error during txn: %v", err)
				}
			}
			respi.(*pb.ResponseOp_ResponsePut).ResponsePut = resp
		case *pb.RequestOp_RequestDeleteRange:
			resp, err := a.DeleteRange(txn, tv.RequestDeleteRange)
			if err != nil {
				if lg != nil {
					lg.Panic("unexpected error during txn", zap.Error(err))
				} else {
					glog.Fatalf("unexpected error during txn: %v", err)
				}
			}
			respi.(*pb.ResponseOp_ResponseDeleteRange).ResponseDeleteRange = resp
		case *pb.RequestOp_RequestTxn:
			resp := respi.(*pb.ResponseOp_ResponseTxn).ResponseTxn
			applyTxns := a.applyTxn(txn, tv.RequestTxn, txnPath[1:], resp)
			txns += applyTxns + 1
			txnPath = txnPath[applyTxns+1:]
		default:
			// empty union
		}
	}
	return txns
}

type kvSort struct{ kvs []kvpb.KeyValue }

func (s *kvSort) Swap(i, j int) {
	t := s.kvs[i]
	s.kvs[i] = s.kvs[j]
	s.kvs[j] = t
}
func (s *kvSort) Len() int { return len(s.kvs) }

type kvSortByKey struct{ *kvSort }

func (s *kvSortByKey) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Key, s.kvs[j].Key) < 0
}

type kvSortByValue struct{ *kvSort }

func (s *kvSortByValue) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Value, s.kvs[j].Value) < 0
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

//// mkGteRange determines if the range end is a >= range. This works around grpc
//// sending empty byte strings as nil; >= is encoded in the range end as '\0'.
//// If it is a GTE range, then []byte{} is returned to indicate the empty byte
//// string (vs nil being no byte string).
//func mkGteRange(rangeEnd []byte) []byte {
//	if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
//		return []byte{}
//	}
//	return rangeEnd
//}

//func noSideEffect(r *pb.InternalRaftRequest) bool {
//	return r.Range != nil
//}
//
//func removeNeedlessRangeReqs(txn *pb.TxnRequest) {
//	f := func(ops []*pb.RequestOp) []*pb.RequestOp {
//		j := 0
//		for i := 0; i < len(ops); i++ {
//			if _, ok := ops[i].Request.(*pb.RequestOp_RequestRange); ok {
//				continue
//			}
//			ops[j] = ops[i]
//			j++
//		}
//
//		return ops[:j]
//	}
//
//	txn.Success = f(txn.Success)
//	txn.Failure = f(txn.Failure)
//}

//	func newHeader(rr *StateMachine) *pb.ResponseHeader {
//		return &pb.ResponseHeader{
//			ClusterId: uint64(rr.ClusterID()),
//			MemberId:  uint64(rr.MemberId()),
//			RaftTerm:  rr.RaftTerm(),
//		}
//	}
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
