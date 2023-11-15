package raftnode

import (
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"math"

	"github.com/gogo/protobuf/proto"
)

const (
	// MarkIndex mark the current index that should not be persisted
	MarkIndex = math.MaxUint64
)

type ApplyResult struct {
	Resp proto.Message
	Err  error
}

type StateMachineMaintenance interface {
	DBSize() int64
	DBSizeInUse() int64
	Defrag() error
}

type StateMachineBackendConfig interface {
	// Dump config into key-value pairs
	Dump() string
}

type StateMachineHook interface {
	RegisterClusterMembershipHook(func() *membership.RaftCluster)
	RegisterRaftTermHook(func() uint64)
}

type StateMachine interface {
	StateMachineMaintenance
	StateMachineHook
	// Store membersip in state machine
	membership.Storage

	// Consistent index (Applied Index)
	ConsistentIndex() uint64
	SetConsistentIndex(index uint64)

	// Apply cmd to stable storage
	Apply(cmd *pb.InternalRaftRequest, index uint64) *ApplyResult
	// Persist the meta data (eg. consistent index)
	Commit()
	// Close the state machine
	Close()

	// Recover apply state from snapshot
	RecoverFromSnapshot(snapshot *raftpb.Snapshot) error
	// Fetch a snapshot manager, used to send to peer
	SnapshotMgr() SnapshotMgr
	CreateSnapshotMessage(m raftpb.Message) *SnapMessage
}
