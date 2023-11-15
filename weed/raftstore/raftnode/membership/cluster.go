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

package membership

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/netutil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const maxLearners = 1

// MembershipChanger record the ConfChange in memory which proposed through raft after current node started.
// Persist to underlying stable Storage when we crate snapshot.
type MembershipChanger struct {
	members map[types.ID]*Member
	removed map[types.ID]bool
}

// RaftCluster is a list of Members that belong to the same raft cluster
type RaftCluster struct {
	lg *zap.Logger

	localID types.ID
	cid     types.ID
	token   string

	storage Storage

	sync.Mutex // guards the fields below
	members    map[types.ID]*Member
	// removed contains the ids of removed members in the cluster.
	// removed id cannot be reused.
	removed map[types.ID]bool

	changer MembershipChanger
}

// ConfigChangeContext represents a context for confChange.
type ConfigChangeContext struct {
	Member
	// IsPromote indicates if the config change is for promoting a learner member.
	// This flag is needed because both adding a new member and promoting a learner member
	// uses the same config change type 'ConfChangeAddNode'.
	IsPromote bool `json:"isPromote"`
}

// NewClusterFromURLsMap creates a new raft cluster using provided urls map. Currently, it does not support creating
// cluster with raft learner member.
func NewClusterFromURLsMap(lg *zap.Logger, token string, urlsmap types.URLsMap) (*RaftCluster, error) {
	c := NewCluster(lg, token)
	for name, urls := range urlsmap {
		m := NewMember(name, urls, token, nil)
		if _, ok := c.members[m.ID]; ok {
			return nil, fmt.Errorf("member exists with identical ID %v", m)
		}
		if uint64(m.ID) == raft.None {
			return nil, fmt.Errorf("cannot use %x as member id", raft.None)
		}
		c.members[m.ID] = m
	}
	c.genID()
	return c, nil
}

func NewClusterFromMembers(lg *zap.Logger, token string, id types.ID, membs []*Member) *RaftCluster {
	c := NewCluster(lg, token)
	c.cid = id
	for _, m := range membs {
		c.members[m.ID] = m
	}
	return c
}

func NewCluster(lg *zap.Logger, token string) *RaftCluster {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &RaftCluster{
		lg:      lg,
		token:   token,
		members: make(map[types.ID]*Member),
		removed: make(map[types.ID]bool),
	}
}

func (c *RaftCluster) ID() types.ID { return c.cid }

func (c *RaftCluster) Members() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		ms = append(ms, m.Clone())
	}
	sort.Sort(ms)
	return []*Member(ms)
}

func (c *RaftCluster) Member(id types.ID) *Member {
	c.Lock()
	defer c.Unlock()
	return c.members[id].Clone()
}

func (c *RaftCluster) VotingMembers() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		if !m.IsLearner {
			ms = append(ms, m.Clone())
		}
	}
	sort.Sort(ms)
	return []*Member(ms)
}

// MemberByName returns a Member with the given name if exists.
// If more than one member has the given name, it will panic.
func (c *RaftCluster) MemberByName(name string) *Member {
	c.Lock()
	defer c.Unlock()
	var memb *Member
	for _, m := range c.members {
		if m.Name == name {
			if memb != nil {
				c.lg.Panic("two member with same name found", zap.String("name", name))
			}
			memb = m
		}
	}
	return memb.Clone()
}

func (c *RaftCluster) MemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.members {
		ids = append(ids, m.ID)
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (c *RaftCluster) IsIDRemoved(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	return c.removed[id]
}

// PeerURLs returns a list of all peer addresses.
// The returned list is sorted in ascending lexicographical order.
func (c *RaftCluster) PeerURLs() []string {
	c.Lock()
	defer c.Unlock()
	urls := make([]string, 0)
	for _, p := range c.members {
		urls = append(urls, p.PeerURLs...)
	}
	sort.Strings(urls)
	return urls
}

// ClientURLs returns a list of all client addresses.
// The returned list is sorted in ascending lexicographical order.
func (c *RaftCluster) ClientURLs() []string {
	c.Lock()
	defer c.Unlock()
	urls := make([]string, 0)
	for _, p := range c.members {
		urls = append(urls, p.ClientURLs...)
	}
	sort.Strings(urls)
	return urls
}

func (c *RaftCluster) String() string {
	c.Lock()
	defer c.Unlock()
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "{ClusterID:%s ", c.cid)
	var ms []string
	for _, m := range c.members {
		ms = append(ms, fmt.Sprintf("%+v", m))
	}
	fmt.Fprintf(b, "Members:[%s] ", strings.Join(ms, " "))
	var ids []string
	for id := range c.removed {
		ids = append(ids, id.String())
	}
	fmt.Fprintf(b, "RemovedMemberIDs:[%s]}", strings.Join(ids, " "))
	return b.String()
}

func (c *RaftCluster) genID() {
	mIDs := c.MemberIDs()
	b := make([]byte, 8*len(mIDs))
	for i, id := range mIDs {
		binary.BigEndian.PutUint64(b[8*i:], uint64(id))
	}
	hash := sha1.Sum(b)
	c.cid = types.ID(binary.BigEndian.Uint64(hash[:8]))
}

func (c *RaftCluster) SetID(localID, cid types.ID) {
	c.localID = localID
	c.cid = cid
}

func (c *RaftCluster) LocalMemberID() types.ID {
	return c.localID
}

func (c *RaftCluster) SetStorage(st Storage) {
	if err := st.Initialize(); err != nil {
		c.lg.Panic("Set storage for raft cluster failed")
	}
	c.storage = st
}

// Recover cluster membership from storage
func (c *RaftCluster) Recover() {
	c.Lock()
	defer c.Unlock()

	c.members, c.removed = membersFromStorage(c.lg, c.storage)
	// Update to changer
	c.changer.members = c.members
	c.changer.removed = c.removed
	for _, m := range c.members {
		c.lg.Info(
			"recovered/added member from storage",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("recovered-remote-peer-id", m.ID.String()),
			zap.Strings("recovered-remote-peer-urls", m.PeerURLs),
		)
	}
}

// ValidateConfigurationChange takes a proposed ConfChange and
// ensures that it is still valid.
func (c *RaftCluster) ValidateConfigurationChange(cc raftpb.ConfChange) error {
	members, removed := c.changer.members, c.changer.removed
	id := types.ID(cc.NodeID)
	if removed[id] {
		return ErrIDRemoved
	}
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		confChangeContext := new(ConfigChangeContext)
		if err := json.Unmarshal(cc.Context, confChangeContext); err != nil {
			c.lg.Panic("failed to unmarshal confChangeContext", zap.Error(err))
		}

		if confChangeContext.IsPromote { // promoting a learner member to voting member
			if members[id] == nil {
				return ErrIDNotFound
			}
			if !members[id].IsLearner {
				return ErrMemberNotLearner
			}
		} else { // adding a new member
			if members[id] != nil {
				return ErrIDExists
			}

			urls := make(map[string]bool)
			for _, m := range members {
				for _, u := range m.PeerURLs {
					urls[u] = true
				}
			}
			for _, u := range confChangeContext.Member.PeerURLs {
				if urls[u] {
					return ErrPeerURLexists
				}
			}

			if confChangeContext.Member.IsLearner { // the new member is a learner
				numLearners := 0
				for _, m := range members {
					if m.IsLearner {
						numLearners++
					}
				}
				if numLearners+1 > maxLearners {
					return ErrTooManyLearners
				}
			}
		}
	case raftpb.ConfChangeRemoveNode:
		if members[id] == nil {
			return ErrIDNotFound
		}

	case raftpb.ConfChangeUpdateNode:
		if members[id] == nil {
			return ErrIDNotFound
		}
		urls := make(map[string]bool)
		for _, m := range members {
			if m.ID == id {
				continue
			}
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}
		m := new(Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			c.lg.Panic("failed to unmarshal member", zap.Error(err))
		}
		for _, u := range m.PeerURLs {
			if urls[u] {
				return ErrPeerURLexists
			}
		}

	default:
		c.lg.Panic("unknown ConfChange type", zap.String("type", cc.Type.String()))
	}
	return nil
}

// AddMember adds a new Member into the cluster, and saves the given member's
// raftAttributes into the store. The given member should have empty attributes.
// A Member with a matching id must not exist.
func (c *RaftCluster) AddMember(m *Member) {
	c.Lock()
	defer c.Unlock()

	saveMemberToChanger(&c.changer, m)

	c.members[m.ID] = m

	c.lg.Info(
		"added member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("added-peer-id", m.ID.String()),
		zap.Strings("added-peer-peer-urls", m.PeerURLs),
	)
}

// RemoveMember removes a member from the store.
// The given id MUST exist, or the function panics.
func (c *RaftCluster) RemoveMember(id types.ID) {
	c.Lock()
	defer c.Unlock()
	saveDeleteMemberToChanger(&c.changer, id)

	m, ok := c.members[id]
	delete(c.members, id)
	c.removed[id] = true

	if ok {
		c.lg.Info(
			"removed member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("removed-remote-peer-id", id.String()),
			zap.Strings("removed-remote-peer-urls", m.PeerURLs),
		)
	} else {
		c.lg.Warn(
			"skipped removing already removed member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("removed-remote-peer-id", id.String()),
		)
	}
}

func (c *RaftCluster) UpdateAttributes(id types.ID, attr Attributes) {
	c.Lock()
	defer c.Unlock()

	if m, ok := c.members[id]; ok {
		m.Attributes = attr
		saveMemberToChanger(&c.changer, m)
		return
	}

	_, ok := c.removed[id]
	if !ok {
		c.lg.Panic(
			"failed to update; member unknown",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("unknown-remote-peer-id", id.String()),
		)
	}

	c.lg.Warn(
		"skipped attributes update of removed member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("updated-peer-id", id.String()),
	)
}

// PromoteMember marks the member's IsLearner RaftAttributes to false.
func (c *RaftCluster) PromoteMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	c.members[id].RaftAttributes.IsLearner = false
	saveMemberToChanger(&c.changer, c.members[id])

	c.lg.Info(
		"promote member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
	)
}

func (c *RaftCluster) UpdateRaftAttributes(id types.ID, raftAttr RaftAttributes) {
	c.Lock()
	defer c.Unlock()

	c.members[id].RaftAttributes = raftAttr
	saveMemberToChanger(&c.changer, c.members[id])

	c.lg.Info(
		"updated member",
		zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("updated-remote-peer-id", id.String()),
		zap.Strings("updated-remote-peer-urls", raftAttr.PeerURLs),
	)
}

func (c *RaftCluster) IsReadyToAddVotingMember() bool {
	nmembers := 1
	nstarted := 0

	for _, member := range c.VotingMembers() {
		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	if nstarted == 1 && nmembers == 2 {
		// a case of adding a new node to 1-member cluster for restoring cluster data
		// https://github.com/etcd-io/etcd/blob/master/Documentation/v2/admin_guide.md#restoring-the-cluster
		c.lg.Debug("number of started member is 1; can accept add member request")
		return true
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		c.lg.Warn(
			"rejecting member add; started member will be less than quorum",
			zap.Int("number-of-started-member", nstarted),
			zap.Int("quorum", nquorum),
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
		return false
	}

	return true
}

func (c *RaftCluster) IsReadyToRemoveVotingMember(id uint64) bool {
	nmembers := 0
	nstarted := 0

	for _, member := range c.VotingMembers() {
		if uint64(member.ID) == id {
			continue
		}

		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		c.lg.Warn(
			"rejecting member remove; started member will be less than quorum",
			zap.Int("number-of-started-member", nstarted),
			zap.Int("quorum", nquorum),
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
		return false
	}

	return true
}

func (c *RaftCluster) IsReadyToPromoteMember(id uint64) bool {
	nmembers := 1 // We count the learner to be promoted for the future quorum
	nstarted := 1 // and we also count it as started.

	for _, member := range c.VotingMembers() {
		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		c.lg.Warn(
			"rejecting member promote; started member will be less than quorum",
			zap.Int("number-of-started-member", nstarted),
			zap.Int("quorum", nquorum),
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
		return false
	}

	return true
}

// Persist persist memory changer into underlying stable Storage
func (c *RaftCluster) Persist() {
	if c.storage == nil {
		c.lg.Panic(
			"failed to persist membership to underlying stable storage, storage is not exist",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
	}
	for _, m := range c.changer.members {
		mustSaveMemberToStorage(c.lg, c.storage, m)
	}
	for id, _ := range c.changer.removed {
		mustDeleteMemberFromStorage(c.lg, c.storage, id)
	}
}

func membersFromStorage(lg *zap.Logger, st Storage) (map[types.ID]*Member, map[types.ID]bool) {
	if st == nil {
		lg.Panic("the storage not exist for membership")
	}
	var err error
	members := make(map[types.ID]*Member)
	removed := make(map[types.ID]bool)
	//_, values, err := st.List(ListOptions{
	//	Prefix: MembersKeyPrefix,
	//	Count:  -1,
	//})
	//if err != nil {
	//	lg.Panic("failed to get members from storage", zap.String("path", MembersKeyPrefix), zap.Error(err))
	//}
	//for _, value := range values {
	//	var m *Member
	//	if m, err = json.Unmarshal(value); err != nil {
	//		lg.Panic("failed to unmarshal member from storage", zap.Error(err))
	//	}
	//	members[m.ID] = m
	//}

	//keys, _, err = st.List(ListOptions{
	//	Prefix: RemovedMembersKeyPrefix,
	//	Count:  -1,
	//})
	//if err != nil {
	//	lg.Panic("failed to get removed members from storage", zap.String("path", storeRemovedMembersPrefix), zap.Error(err))
	//}
	//for _, key := range keys {
	//	var memberId types.ID
	//	if memberId, err = types.IDFromString(path.Base(key)); err != nil {
	//		lg.Panic("failed to fetch member ID from storage", zap.String("key", key), zap.Error(err))
	//	}
	//	removed[memberId] = true
	//}
	members, err = st.Members()
	if err != nil {
		lg.Panic("failed to get members from storage", zap.Error(err))
	}
	removed, err = st.RemovedMembers()
	if err != nil {
		lg.Panic("failed to get removed members from storage", zap.Error(err))
	}
	return members, removed
}

// ValidateClusterAndAssignIDs validates the local cluster by matching the PeerURLs
// with the existing cluster. If the validation succeeds, it assigns the IDs
// from the existing cluster to the local cluster.
// If the validation fails, an error will be returned.
func ValidateClusterAndAssignIDs(lg *zap.Logger, local *RaftCluster, existing *RaftCluster) error {
	ems := existing.Members()
	lms := local.Members()
	if len(ems) != len(lms) {
		return fmt.Errorf("member count is unequal")
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	for i := range ems {
		var err error
		ok := false
		for j := range lms {
			if ok, err = netutil.URLStringsEqual(ctx, lg, ems[i].PeerURLs, lms[j].PeerURLs); ok {
				lms[j].ID = ems[i].ID
				break
			}
		}
		if !ok {
			return fmt.Errorf("PeerURLs: no match found for existing member (%v, %v), last resolver error (%v)", ems[i].ID, ems[i].PeerURLs, err)
		}
	}
	local.members = make(map[types.ID]*Member)
	for _, m := range lms {
		local.members[m.ID] = m
	}
	return nil
}

// IsLocalMemberLearner returns if the local member is raft learner
func (c *RaftCluster) IsLocalMemberLearner() bool {
	c.Lock()
	defer c.Unlock()
	localMember, ok := c.members[c.localID]
	if !ok {
		c.lg.Panic(
			"failed to find local ID in cluster members",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
	}
	return localMember.IsLearner
}

// IsMemberExist returns if the member with the given id exists in cluster.
func (c *RaftCluster) IsMemberExist(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	_, ok := c.members[id]
	return ok
}

// VotingMemberIDs returns the ID of voting members in cluster.
func (c *RaftCluster) VotingMemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.members {
		if !m.IsLearner {
			ids = append(ids, m.ID)
		}
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}
