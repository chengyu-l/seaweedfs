package raftrelay

import (
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/pbutil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

func (r *RaftRelay) applyAll(ep *Progress, apply *raftnode.Apply) {
	r.applySnapshot(ep, apply)
	r.applyEntries(ep, apply)
	r.applyWait.Trigger(ep.appliedi)

	// wait for the raft routine to finish the disk writes before triggering a
	// snapshot. or applied index might be greater than the last index in raft
	// storage, since the raft routine might be slower than apply routine.
	<-apply.Notifyc

	r.handleSnapshot(ep)
}

func (r *RaftRelay) applySnapshot(ep *Progress, apply *raftnode.Apply) {
	if raft.IsEmptySnap(apply.Snapshot) {
		return
	}

	lg := r.getLogger()
	lg.Info(
		"applying snapshot",
		zap.Uint64("current-snapshot-index", ep.snapi),
		zap.Uint64("current-applied-index", ep.appliedi),
		zap.Uint64("incoming-leader-snapshot-index", apply.Snapshot.Metadata.Index),
		zap.Uint64("incoming-leader-snapshot-term", apply.Snapshot.Metadata.Term),
	)
	defer func() {
		lg.Info(
			"applied snapshot",
			zap.Uint64("current-snapshot-index", ep.snapi),
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("incoming-leader-snapshot-index", apply.Snapshot.Metadata.Index),
			zap.Uint64("incoming-leader-snapshot-term", apply.Snapshot.Metadata.Term),
		)
	}()

	if apply.Snapshot.Metadata.Index <= ep.appliedi {
		lg.Panic(
			"unexpected leader snapshot from outdated index",
			zap.Uint64("current-snapshot-index", ep.snapi),
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("incoming-leader-snapshot-index", apply.Snapshot.Metadata.Index),
			zap.Uint64("incoming-leader-snapshot-term", apply.Snapshot.Metadata.Term),
		)
	}

	// wait for raftNode to persist snapshot onto the disk
	<-apply.Notifyc

	// recover/reopen state machine from snapshot
	if err := r.stateMachine.RecoverFromSnapshot(&apply.Snapshot); err != nil {
		lg.Panic("failed to open snapshot backend", zap.Error(err))
	}

	// Reset storage for retreive cluster membership
	lg.Info("restoring cluster configuration")
	r.clusterMembership.SetStorage(r.stateMachine)
	r.clusterMembership.Recover()
	lg.Info("restored cluster configuration")

	// Update underlying Transport
	lg.Info("removing old peers from network")
	r.raftNode.Transport.RemoveAllPeers()
	lg.Info("removed old peers from network")

	lg.Info("adding peers from new cluster configuration")
	for _, m := range r.clusterMembership.Members() {
		if m.ID == r.LocalID() {
			continue
		}
		r.raftNode.Transport.AddPeer(m.ID, m.PeerURLs)
	}
	lg.Info("added peers from new cluster configuration")

	ep.appliedt = apply.Snapshot.Metadata.Term
	ep.appliedi = apply.Snapshot.Metadata.Index
	ep.snapi = ep.appliedi
	ep.confState = apply.Snapshot.Metadata.ConfState
}

func (r *RaftRelay) applyEntries(ep *Progress, apply *raftnode.Apply) {
	if len(apply.Entries) == 0 {
		return
	}
	firsti := apply.Entries[0].Index
	if firsti > ep.appliedi+1 {
		lg := r.getLogger()
		lg.Panic(
			"unexpected committed entry index",
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("first-committed-entry-index", firsti),
		)
	}
	var ents []raftpb.Entry
	if ep.appliedi+1-firsti < uint64(len(apply.Entries)) {
		ents = apply.Entries[ep.appliedi+1-firsti:]
	}
	if len(ents) == 0 {
		return
	}
	var shouldstop bool
	if ep.appliedt, ep.appliedi, shouldstop = r.apply(ents, &ep.confState); shouldstop {
		go r.stopWithDelay(10*100*time.Millisecond, fmt.Errorf("the member has been permanently removed from the cluster"))
	}
}

func (r *RaftRelay) apply(es []raftpb.Entry, confState *raftpb.ConfState) (appliedt uint64, appliedi uint64, shouldStop bool) {
	for i := range es {
		e := es[i]
		switch e.Type {
		case raftpb.EntryNormal:
			r.applyEntryNormal(&e)
			r.setAppliedIndex(e.Index)
			r.setTerm(e.Term)

		case raftpb.EntryConfChange:
			removedSelf := r.applyConfChange(&e, confState)
			r.setAppliedIndex(e.Index)
			r.setTerm(e.Term)
			shouldStop = shouldStop || removedSelf

		default:
			lg := r.getLogger()
			lg.Panic(
				"unknown entry type; must be either EntryNormal or EntryConfChange",
				zap.String("type", e.Type.String()),
			)
		}
		appliedi, appliedt = e.Index, e.Term
	}
	return appliedt, appliedi, shouldStop
}

// applyEntryNormal apples an EntryNormal type raftpb request to the RaftRelay
func (s *RaftRelay) applyEntryNormal(e *raftpb.Entry) {
	index := s.stateMachine.ConsistentIndex()
	shouldApply := false
	if e.Index > index {
		shouldApply = true
		// set the consistent index of current executing entry
		s.stateMachine.SetConsistentIndex(e.Index)
	}

	// raft state machine may generate noop entry when leader confirmation.
	// skip it in advance to avoid some potential bug in the future
	if len(e.Data) == 0 {
		return
	}

	var raftReq pb.InternalRaftRequest
	if err := proto.Unmarshal(e.Data, &raftReq); err != nil {
		s.lg.Panic("failed to unmarshal internal raft request", zap.Error(err))
	}
	if raftReq.AdminRequest != nil {
		// re-apply as current we does not persist admin requests
		shouldApply = true
	}

	// Apply State machine
	if !shouldApply {
		// Avoid re-apply
		return
	}

	ar := &raftnode.ApplyResult{}
	defer func(start time.Time) {
		if ar == nil {
			return
		}
		warnOfExpensiveRequest(s.getLogger(), start, &pb.InternalRaftStringer{Request: &raftReq}, ar.Resp, ar.Err)
		if ar.Err != nil {
			warnOfFailedRequest(s.getLogger(), start, &pb.InternalRaftStringer{Request: &raftReq}, ar.Resp, ar.Err)
		}
	}(time.Now())

	id := raftReq.ID
	ar = s.stateMachine.Apply(&raftReq, e.Index)
	if ar == nil {
		return
	}
	if ar.Err != ErrNoSpace {
		if s.wait.IsRegistered(id) {
			s.wait.Trigger(id, ar)
			return
		}
	}
	// TODO exceeded backend storage space, alarm to other node
}

type confChangeResponse struct {
	membs []*membership.Member
	err   error
}

// applyConfChange applies a ConfChange to the server. It is only
// invoked with a ConfChange that has already passed through Raft
func (rr *RaftRelay) applyConfChange(e *raftpb.Entry, confState *raftpb.ConfState) bool {
	if e.Index > rr.stateMachine.ConsistentIndex() {
		rr.stateMachine.SetConsistentIndex(e.Index)
	}
	var (
		cc  raftpb.ConfChange
		err error
	)
	pbutil.MustUnmarshal(&cc, e.Data)
	defer func() {
		rr.wait.Trigger(cc.ID, &confChangeResponse{rr.clusterMembership.Members(), err})
	}()
	if err = rr.clusterMembership.ValidateConfigurationChange(cc); err != nil {
		cc.NodeID = raft.None
		rr.raftNode.ApplyConfChange(cc)
		return false
	}

	*confState = *rr.raftNode.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		confChangeContext := new(membership.ConfigChangeContext)
		if err = json.Unmarshal(cc.Context, confChangeContext); err != nil {
			rr.lg.Panic("failed to unmarshal member", zap.Error(err))
		}
		if cc.NodeID != uint64(confChangeContext.Member.ID) {
			rr.lg.Panic(
				"got different member ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.NodeID).String()),
				zap.String("member-id-from-message", confChangeContext.Member.ID.String()),
			)
		}
		if confChangeContext.IsPromote {
			rr.clusterMembership.PromoteMember(confChangeContext.Member.ID)
		} else {
			rr.clusterMembership.AddMember(&confChangeContext.Member)

			if confChangeContext.Member.ID != rr.LocalID() {
				rr.raftNode.Transport.AddPeer(confChangeContext.Member.ID, confChangeContext.PeerURLs)
			}
		}

		// update the isLearner metric when this server id is equal to the id in raft member confChange
		if confChangeContext.Member.ID == rr.LocalID() {
			if cc.Type == raftpb.ConfChangeAddLearnerNode {
				isLearner.Set(1)
			} else {
				isLearner.Set(0)
			}
		}

	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		rr.clusterMembership.RemoveMember(id)
		if id == rr.LocalID() {
			return true
		}
		rr.raftNode.Transport.RemovePeer(id)

	case raftpb.ConfChangeUpdateNode:
		m := new(membership.Member)
		if err = json.Unmarshal(cc.Context, m); err != nil {
			rr.lg.Panic("failed to unmarshal member", zap.Error(err))
		}
		if cc.NodeID != uint64(m.ID) {
			rr.lg.Panic(
				"got different member ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.NodeID).String()),
				zap.String("member-id-from-message", m.ID.String()),
			)
		}
		rr.clusterMembership.UpdateRaftAttributes(m.ID, m.RaftAttributes)
		if m.ID != rr.LocalID() {
			rr.raftNode.Transport.UpdatePeer(m.ID, m.PeerURLs)
		}
	}
	return false
}
