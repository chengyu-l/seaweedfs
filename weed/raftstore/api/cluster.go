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

package api

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	raftrelay "github.com/seaweedfs/seaweedfs/weed/raftstore/raftrelay"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"time"

	"google.golang.org/grpc"
)

type (
	Member                       pb.Member
	MemberListResponse           pb.MemberListResponse
	MemberAddResponse            pb.MemberAddResponse
	MemberRemoveResponse         pb.MemberRemoveResponse
	MemberRaftAttrUpdateResponse pb.MemberRaftAttrUpdateResponse
	MemberPromoteResponse        pb.MemberPromoteResponse
)

type Cluster interface {
	// MemberList lists the current cluster membership.
	MemberList(ctx context.Context) (*MemberListResponse, error)

	// MemberAdd adds a new member into the cluster.
	MemberAdd(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error)

	// MemberAddAsLearner adds a new learner member into the cluster.
	MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error)

	// MemberRemove removes an existing member from the cluster.
	MemberRemove(ctx context.Context, id uint64) (*MemberRemoveResponse, error)

	// MemberUpdate updates the peer addresses of the member.
	MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*MemberRaftAttrUpdateResponse, error)

	// MemberPromote promotes a member from raft learner (non-voting) to raft voting member.
	MemberPromote(ctx context.Context, id uint64) (*MemberPromoteResponse, error)
}

type cluster struct {
	remote   pb.ClusterClient
	callOpts []grpc.CallOption
}

func NewCluster(cc *grpc.ClientConn, callOpts []grpc.CallOption) Cluster {
	return &cluster{
		remote:   pb.NewClusterClient(cc),
		callOpts: callOpts,
	}
}

func (c *cluster) MemberAdd(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error) {
	return c.memberAdd(ctx, peerAddrs, false)
}

func (c *cluster) MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error) {
	return c.memberAdd(ctx, peerAddrs, true)
}

func (c *cluster) memberAdd(ctx context.Context, peerAddrs []string, isLearner bool) (*MemberAddResponse, error) {
	// fail-fast before panic in rafthttp
	if _, err := types.NewURLs(peerAddrs); err != nil {
		return nil, err
	}

	r := &pb.MemberAddRequest{
		PeerURLs:  peerAddrs,
		IsLearner: isLearner,
	}
	resp, err := c.remote.MemberAdd(ctx, r, c.callOpts...)
	if err != nil {
		return nil, err
	}
	return (*MemberAddResponse)(resp), nil
}

func (c *cluster) MemberRemove(ctx context.Context, id uint64) (*MemberRemoveResponse, error) {
	r := &pb.MemberRemoveRequest{ID: id}
	resp, err := c.remote.MemberRemove(ctx, r, c.callOpts...)
	if err != nil {
		return nil, err
	}
	return (*MemberRemoveResponse)(resp), nil
}

func (c *cluster) MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*MemberRaftAttrUpdateResponse, error) {
	// fail-fast before panic in rafthttp
	if _, err := types.NewURLs(peerAddrs); err != nil {
		return nil, err
	}

	// it is safe to retry on update.
	r := &pb.MemberRaftAttrUpdateRequest{ID: id, PeerURLs: peerAddrs}
	resp, err := c.remote.MemberUpdate(ctx, r, c.callOpts...)
	if err == nil {
		return (*MemberRaftAttrUpdateResponse)(resp), nil
	}
	return nil, err
}

func (c *cluster) MemberList(ctx context.Context) (*MemberListResponse, error) {
	// it is safe to retry on list.
	resp, err := c.remote.MemberList(ctx, &pb.MemberListRequest{}, c.callOpts...)
	if err == nil {
		return (*MemberListResponse)(resp), nil
	}
	return nil, err
}

func (c *cluster) MemberPromote(ctx context.Context, id uint64) (*MemberPromoteResponse, error) {
	r := &pb.MemberPromoteRequest{ID: id}
	resp, err := c.remote.MemberPromote(ctx, r, c.callOpts...)
	if err != nil {
		return nil, err
	}
	return (*MemberPromoteResponse)(resp), nil
}

type clusterServer struct {
	server  raftrelay.AdminService
	cluster raftrelay.RaftStatusGetter
}

func NewClusterServer(rr *raftrelay.RaftRelay) pb.ClusterServer {
	return &clusterServer{
		cluster: rr,
		server:  rr,
	}
}

func (cs *clusterServer) MemberAdd(ctx context.Context, r *pb.MemberAddRequest) (*pb.MemberAddResponse, error) {
	urls, err := types.NewURLs(r.PeerURLs)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	var m *membership.Member
	if r.IsLearner {
		m = membership.NewMemberAsLearner("", urls, "", &now)
	} else {
		m = membership.NewMember("", urls, "", &now)
	}
	membs, merr := cs.server.AddMember(ctx, *m)
	if merr != nil {
		return nil, merr
	}

	return &pb.MemberAddResponse{
		Header: cs.header(),
		Member: &pb.Member{
			ID:        uint64(m.ID),
			PeerURLs:  m.PeerURLs,
			IsLearner: m.IsLearner,
		},
		Members: membersToProtoMembers(membs),
	}, nil
}

func (cs *clusterServer) MemberRemove(ctx context.Context, r *pb.MemberRemoveRequest) (*pb.MemberRemoveResponse, error) {
	membs, err := cs.server.RemoveMember(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	return &pb.MemberRemoveResponse{Header: cs.header(), Members: membersToProtoMembers(membs)}, nil
}

func (cs *clusterServer) MemberUpdate(ctx context.Context, r *pb.MemberRaftAttrUpdateRequest) (*pb.MemberRaftAttrUpdateResponse, error) {
	m := membership.Member{
		ID:             types.ID(r.ID),
		RaftAttributes: membership.RaftAttributes{PeerURLs: r.PeerURLs},
	}
	membs, err := cs.server.UpdateMember(ctx, m)
	if err != nil {
		return nil, err
	}
	return &pb.MemberRaftAttrUpdateResponse{Header: cs.header(), Members: membersToProtoMembers(membs)}, nil
}

func (cs *clusterServer) MemberList(ctx context.Context, r *pb.MemberListRequest) (*pb.MemberListResponse, error) {
	membs := membersToProtoMembers(cs.server.Cluster().Members())
	return &pb.MemberListResponse{Header: cs.header(), Members: membs}, nil
}

func (cs *clusterServer) MemberPromote(ctx context.Context, r *pb.MemberPromoteRequest) (*pb.MemberPromoteResponse, error) {
	membs, err := cs.server.PromoteMember(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	return &pb.MemberPromoteResponse{Header: cs.header(), Members: membersToProtoMembers(membs)}, nil
}

func (cs *clusterServer) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: uint64(cs.cluster.ClusterID()),
		MemberId:  uint64(cs.cluster.LocalID()),
		RaftTerm:  cs.cluster.Term(),
	}
}

func membersToProtoMembers(membs []*membership.Member) []*pb.Member {
	protoMembs := make([]*pb.Member, len(membs))
	for i := range membs {
		protoMembs[i] = &pb.Member{
			Name:       membs[i].Name,
			ID:         uint64(membs[i].ID),
			PeerURLs:   membs[i].PeerURLs,
			ClientURLs: membs[i].ClientURLs,
			IsLearner:  membs[i].IsLearner,
		}
	}
	return protoMembs
}
