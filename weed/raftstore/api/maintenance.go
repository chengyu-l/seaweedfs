package api

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	raftrelay "github.com/seaweedfs/seaweedfs/weed/raftstore/raftrelay"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"

	"google.golang.org/grpc"
)

type (
	StatusResponse     pb.StatusResponse
	DefragmentResponse pb.DefragmentResponse
	MoveLeaderResponse pb.MoveLeaderResponse
)

type Maintenance interface {
	// Status gets the status of the endpoint.
	Status(ctx context.Context, endpoint string) (*StatusResponse, error)

	// Defragment releases wasted space from internal fragmentation on a given etcd member.
	// Defragment is only needed when deleting a large number of keys and want to reclaim
	// the resources.
	// Defragment is an expensive operation. User should avoid defragmenting multiple members
	// at the same time.
	// To defragment multiple members in the cluster, user need to call defragment multiple
	// times with different endpoints.
	Defragment(ctx context.Context, endpoint string) (*DefragmentResponse, error)

	// MoveLeader requests current leader to transfer its leadership to the transferee.
	// Request must be made to the leader.
	MoveLeader(ctx context.Context, transfereeID uint64) (*MoveLeaderResponse, error)
}

type maintenance struct {
	dial     func(endpoint string) (pb.MaintenanceClient, func(), error)
	remote   pb.MaintenanceClient
	callOpts []grpc.CallOption
}

func NewMaintenance(c *Client) Maintenance {
	return &maintenance{
		dial: func(endpoint string) (pb.MaintenanceClient, func(), error) {
			conn, err := c.Dial(endpoint)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to dial endpoint %s with maintenance client: %v", endpoint, err)
			}
			cancel := func() { conn.Close() }
			return pb.NewMaintenanceClient(conn), cancel, nil
		},
		remote:   pb.NewMaintenanceClient(c.conn),
		callOpts: nil,
	}
}

func (m *maintenance) Status(ctx context.Context, endpoint string) (*StatusResponse, error) {
	remote, cancel, err := m.dial(endpoint)
	if err != nil {
		return nil, err
	}
	defer cancel()
	resp, err := remote.Status(ctx, &pb.StatusRequest{}, m.callOpts...)
	if err != nil {
		return nil, err
	}
	return (*StatusResponse)(resp), nil
}

func (m *maintenance) Defragment(ctx context.Context, endpoint string) (*DefragmentResponse, error) {
	remote, cancel, err := m.dial(endpoint)
	if err != nil {
		return nil, err
	}
	defer cancel()
	resp, err := remote.Defragment(ctx, &pb.DefragmentRequest{}, m.callOpts...)
	if err != nil {
		return nil, err
	}
	return (*DefragmentResponse)(resp), nil
}

func (m *maintenance) MoveLeader(ctx context.Context, transfereeID uint64) (*MoveLeaderResponse, error) {
	resp, err := m.remote.MoveLeader(ctx, &pb.MoveLeaderRequest{TargetID: transfereeID}, m.callOpts...)
	return (*MoveLeaderResponse)(resp), err
}

type maintenanceServer struct {
	h   *header
	rsg raftrelay.RaftStatusGetter
	ms  raftrelay.MaintenanceService
}

func NewMaintenanceServer(rr *raftrelay.RaftRelay) pb.MaintenanceServer {
	return &maintenanceServer{
		h:   newHeader(rr),
		rsg: rr,
		ms:  rr,
	}
}

func (ms *maintenanceServer) Status(ctx context.Context, ar *pb.StatusRequest) (*pb.StatusResponse, error) {
	header := &pb.ResponseHeader{}
	ms.h.fillHeader(header)
	resp := &pb.StatusResponse{
		Header:           header,
		Leader:           uint64(ms.rsg.Leader()),
		IsLearner:        ms.rsg.IsLearner(),
		RaftIndex:        ms.rsg.CommittedIndex(),
		RaftAppliedIndex: ms.rsg.AppliedIndex(),
		RaftTerm:         ms.rsg.Term(),
		DbSize:           ms.ms.DBSize(),
		DbSizeInUse:      ms.ms.DBSizeInUse(),
	}
	if resp.Leader == raft.None {
		resp.Errors = append(resp.Errors, raftrelay.ErrNoLeader.Error())
	}
	return resp, nil
}

func (ms *maintenanceServer) MoveLeader(ctx context.Context, req *pb.MoveLeaderRequest) (*pb.MoveLeaderResponse, error) {
	if !ms.rsg.IsLeader() {
		return nil, raftrelay.ErrNotLeader
	}

	if err := ms.ms.MoveLeader(ctx, uint64(ms.rsg.Leader()), req.TargetID); err != nil {
		return nil, err
	}
	return &pb.MoveLeaderResponse{}, nil
}

func (ms *maintenanceServer) Defragment(ctx context.Context, req *pb.DefragmentRequest) (*pb.DefragmentResponse, error) {
	err := ms.ms.Defrag()
	if err != nil {
		return nil, err
	}
	return &pb.DefragmentResponse{}, nil
}
