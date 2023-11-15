package raftrelay

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	"time"

	"go.uber.org/zap"
)

type AdminService interface {
	// AddMember attempts to add a member into the cluster. It will return
	// ErrIDRemoved if member ID is removed from the cluster, or return
	// ErrIDExists if member ID exists in the cluster.
	AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error)
	// RemoveMember attempts to remove a member from the cluster. It will
	// return ErrIDRemoved if member ID is removed from the cluster, or return
	// ErrIDNotFound if member ID is not in the cluster.
	RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error)
	// UpdateMember attempts to update an existing member in the cluster. It will
	// return ErrIDNotFound if the member ID does not exist.
	UpdateMember(ctx context.Context, updateMemb membership.Member) ([]*membership.Member, error)
	// PromoteMember attempts to promote a non-voting node to a voting node. It will
	// return ErrIDNotFound if the member ID does not exist.
	// return ErrLearnerNotReady if the member are not ready.
	// return ErrMemberNotLearner if the member is not a learner.
	PromoteMember(ctx context.Context, id uint64) ([]*membership.Member, error)
	Cluster() *membership.RaftCluster
}

func (rr *RaftRelay) Cluster() *membership.RaftCluster { return rr.clusterMembership }

func (rr *RaftRelay) AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	// TODO: move Member to protobuf type
	b, err := json.Marshal(memb)
	if err != nil {
		return nil, err
	}

	if err := rr.mayAddMember(memb); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}

	if memb.IsLearner {
		cc.Type = raftpb.ConfChangeAddLearnerNode
	}
	return rr.configure(ctx, cc)
}

func (rr *RaftRelay) mayAddMember(memb membership.Member) error {
	// protect quorum when adding voting member
	if !memb.IsLearner && !rr.clusterMembership.IsReadyToAddVotingMember() {
		rr.lg.Warn(
			"rejecting member add request; not enough healthy members",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	if !isConnectedFullySince(rr.raftNode.Transport, time.Now().Add(-HealthInterval), rr.LocalID(), rr.clusterMembership.VotingMembers()) {
		rr.lg.Warn(
			"rejecting member add request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

func (rr *RaftRelay) RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := rr.mayRemoveMember(types.ID(id)); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return rr.configure(ctx, cc)
}

// PromoteMember promotes a learner node to a voting node.
func (rr *RaftRelay) PromoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	// only raft leader has information on whether the to-be-promoted learner node is ready. If promoteMember call
	// fails with ErrNotLeader, forward the request to leader node via HTTP. If promoteMember call fails with error
	// other than ErrNotLeader, return the error.
	resp, err := rr.promoteMember(ctx, id)
	if err == nil {
		learnerPromoteSucceed.Inc()
		return resp, nil
	}
	if err != ErrNotLeader {
		learnerPromoteFailed.WithLabelValues(err.Error()).Inc()
		return resp, err
	}

	cctx, cancel := context.WithTimeout(ctx, rr.Cfg.ReqTimeout())
	defer cancel()
	// forward to leader
	for cctx.Err() == nil {
		leader, err := rr.waitLeader(cctx)
		if err != nil {
			return nil, err
		}
		for _, url := range leader.PeerURLs {
			resp, err := promoteMemberHTTP(cctx, url, id, rr.peerRt)
			if err == nil {
				return resp, nil
			}
			// If member promotion failed, return early. Otherwise keep retry.
			if err == ErrLearnerNotReady || err == membership.ErrIDNotFound || err == membership.ErrMemberNotLearner {
				return nil, err
			}
		}
	}

	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (rr *RaftRelay) waitLeader(ctx context.Context) (*membership.Member, error) {
	leader := rr.clusterMembership.Member(rr.Leader())
	for leader == nil {
		// wait an election
		dur := time.Duration(rr.Cfg.ElectionTicks) * time.Duration(rr.Cfg.TickMs) * time.Millisecond
		select {
		case <-time.After(dur):
			leader = rr.clusterMembership.Member(rr.Leader())
		case <-rr.stopping:
			return nil, ErrStopped
		case <-ctx.Done():
			return nil, ErrNoLeader
		}
	}
	if leader == nil || len(leader.PeerURLs) == 0 {
		return nil, ErrNoLeader
	}
	return leader, nil
}

// promoteMember checks whether the to-be-promoted learner node is ready before sending the promote
// request to raft.
// The function returns ErrNotLeader if the local node is not raft leader (therefore does not have
// enough information to determine if the learner node is ready), returns ErrLearnerNotReady if the
// local node is leader (therefore has enough information) but decided the learner node is not ready
// to be promoted.
func (rr *RaftRelay) promoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	// check if we can promote this learner.
	if err := rr.mayPromoteMember(types.ID(id)); err != nil {
		return nil, err
	}

	// build the context for the promote confChange. mark IsLearner to false and IsPromote to true.
	promoteChangeContext := membership.ConfigChangeContext{
		Member: membership.Member{
			ID: types.ID(id),
		},
		IsPromote: true,
	}

	b, err := json.Marshal(promoteChangeContext)
	if err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  id,
		Context: b,
	}

	return rr.configure(ctx, cc)
}

func (rr *RaftRelay) mayPromoteMember(id types.ID) error {
	err := rr.isLearnerReady(uint64(id))
	if err != nil {
		return err
	}

	if !rr.clusterMembership.IsReadyToPromoteMember(uint64(id)) {
		rr.lg.Warn(
			"rejecting member promote request; not enough healthy members",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	return nil
}

// check whether the learner catches up with leader or not.
// Note: it will return nil if member is not found in cluster or if member is not learner.
// These two conditions will be checked before apply phase later.
func (rr *RaftRelay) isLearnerReady(id uint64) error {
	rs := rr.raftStatus()

	// leader's raftStatus.Progress is not nil
	if rs.Progress == nil {
		return ErrNotLeader
	}

	var learnerMatch uint64
	isFound := false
	leaderID := rs.ID
	for memberID, progress := range rs.Progress {
		if id == memberID {
			// check its status
			learnerMatch = progress.Match
			isFound = true
			break
		}
	}

	if isFound {
		leaderMatch := rs.Progress[leaderID].Match
		// the learner's Match not caught up with leader yet
		if float64(learnerMatch) < float64(leaderMatch)*readyPercent {
			return ErrLearnerNotReady
		}
	}

	return nil
}

func (rr *RaftRelay) mayRemoveMember(id types.ID) error {

	isLearner := rr.clusterMembership.IsMemberExist(id) && rr.clusterMembership.Member(id).IsLearner
	// no need to check quorum when removing non-voting member
	if isLearner {
		return nil
	}

	if !rr.clusterMembership.IsReadyToRemoveVotingMember(uint64(id)) {
		rr.lg.Warn(
			"rejecting member remove request; not enough healthy members",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := rr.raftNode.Transport.ActiveSince(id); id != rr.LocalID() && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	m := rr.clusterMembership.VotingMembers()
	active := numConnectedSince(rr.raftNode.Transport, time.Now().Add(-HealthInterval), rr.LocalID(), m)
	if (active - 1) < 1+((len(m)-1)/2) {
		rr.lg.Warn(
			"rejecting member remove request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("requested-member-remove", id.String()),
			zap.Int("active-peers", active),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

func (rr *RaftRelay) UpdateMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	b, merr := json.Marshal(memb)
	if merr != nil {
		return nil, merr
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return rr.configure(ctx, cc)
}

// configure sends a configuration change through consensus and
// then waits for it to be applied to the server. It
// will block until the change is performed or there is an error.
func (rr *RaftRelay) configure(ctx context.Context, cc raftpb.ConfChange) ([]*membership.Member, error) {
	cc.ID = rr.reqIDGen.Next()
	ch := rr.wait.Register(cc.ID)

	start := time.Now()
	if err := rr.raftNode.ProposeConfChange(ctx, cc); err != nil {
		rr.wait.Trigger(cc.ID, nil)
		return nil, err
	}

	select {
	case x := <-ch:
		if x == nil {
			rr.lg.Panic("failed to configure")
		}
		resp := x.(*confChangeResponse)
		rr.lg.Info(
			"applied a configuration change through raft",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("raft-conf-change", cc.Type.String()),
			zap.String("raft-conf-change-node-id", types.ID(cc.NodeID).String()),
		)
		return resp.membs, resp.err

	case <-ctx.Done():
		rr.wait.Trigger(cc.ID, nil) // GC wait
		return nil, rr.parseProposeCtxErr(ctx.Err(), start)

	case <-rr.stopping:
		return nil, ErrStopped
	}
}

func (rr *RaftRelay) parseProposeCtxErr(err error, start time.Time) error {
	switch err {
	case context.Canceled:
		return ErrCanceled

	case context.DeadlineExceeded:
		rr.leadTimeMu.RLock()
		curLeadElected := rr.leadElectedTime
		rr.leadTimeMu.RUnlock()
		prevLeadLost := curLeadElected.Add(-2 * time.Duration(rr.Cfg.ElectionTicks) * time.Duration(rr.Cfg.TickMs) * time.Millisecond)
		if start.After(prevLeadLost) && start.Before(curLeadElected) {
			return ErrTimeoutDueToLeaderFail
		}
		lead := types.ID(rr.getLead())
		switch lead {
		case types.ID(raft.None):
			// TODO: return error to specify it happens because the cluster does not have leader now
		case rr.LocalID():
			if !isConnectedToQuorumSince(rr.raftNode.Transport, start, rr.LocalID(), rr.clusterMembership.Members()) {
				return ErrTimeoutDueToConnectionLost
			}
		default:
			if !isConnectedSince(rr.raftNode.Transport, start, lead) {
				return ErrTimeoutDueToConnectionLost
			}
		}
		return ErrTimeout

	default:
		return err
	}
}
