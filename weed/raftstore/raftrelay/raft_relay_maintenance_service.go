package raftrelay

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"time"

	"go.uber.org/zap"
)

type MaintenanceService interface {
	DBSize() int64
	DBSizeInUse() int64
	Defrag() error
	MoveLeader(ctx context.Context, lead, transferee uint64) error
}

func (rr *RaftRelay) DBSize() int64 {
	if rr.stateMachine != nil {
		return rr.stateMachine.DBSize()
	}
	return 0
}

func (rr *RaftRelay) DBSizeInUse() int64 {
	if rr.stateMachine != nil {
		return rr.stateMachine.DBSizeInUse()
	}
	return 0
}

func (rr *RaftRelay) Defrag() error {
	if rr.stateMachine != nil {
		return rr.stateMachine.Defrag()
	}
	return fmt.Errorf("state machine has not been initialized")
}

// MoveLeader transfers the leader to the given transferee.
func (rr *RaftRelay) MoveLeader(ctx context.Context, lead, transferee uint64) error {
	if !rr.clusterMembership.IsMemberExist(types.ID(transferee)) || rr.clusterMembership.Member(types.ID(transferee)).IsLearner {
		return ErrBadLeaderTransferee
	}

	now := time.Now()
	interval := time.Duration(rr.Cfg.TickMs) * time.Millisecond

	rr.lg.Info(
		"leadership transfer starting",
		zap.String("local-member-id", rr.LocalID().String()),
		zap.String("current-leader-member-id", types.ID(lead).String()),
		zap.String("transferee-member-id", types.ID(transferee).String()),
	)

	rr.raftNode.TransferLeadership(ctx, lead, transferee)
	for rr.Lead() != transferee {
		select {
		case <-ctx.Done(): // time out
			return ErrTimeoutLeaderTransfer
		case <-time.After(interval):
		}
	}

	// TODO: drain all requests, or drop all messages to the old leader
	rr.lg.Info(
		"leadership transfer finished",
		zap.String("local-member-id", rr.LocalID().String()),
		zap.String("old-leader-member-id", types.ID(lead).String()),
		zap.String("new-leader-member-id", types.ID(transferee).String()),
		zap.Duration("took", time.Since(now)),
	)
	return nil
}
