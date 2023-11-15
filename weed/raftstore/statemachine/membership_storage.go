package statemachine

import (
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/backend"
	"math"
	"path"
)

const (
	StorePrefix = "membership"
)

var (
	MemberStoreKeyPrefix        = path.Join(StorePrefix, "member")
	RemovedMemberStoreKeyPrefix = path.Join(StorePrefix, "removed_member")
)

func MemberStoreKey(id types.ID) []byte {
	return []byte(path.Join(MemberStoreKeyPrefix, id.String()))
}

func RemovedMemberStoreKey(id types.ID) []byte {
	return []byte(path.Join(RemovedMemberStoreKeyPrefix, id.String()))
}

var (
	memberStoreBucketName       = []byte("members")
	removeMemberStoreBucketName = []byte("members_removed")
	clusterBucketName           = []byte("cluster")
)

func (s *StateMachine) Initialize() error {
	if s.be == nil {
		return fmt.Errorf("can not found backend store")
	}
	mustCreateBackendBuckets(s.be)
	return nil
}

func (s *StateMachine) SaveMembership(m *membership.Member) error {
	if s.be == nil {
		return fmt.Errorf("can not found backend store")
	}
	mkey := MemberStoreKey(m.ID)
	mvalue, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal raftAttributes should never fail: %v", err)
	}
	tx := s.be.BatchTx()
	tx.Lock()
	tx.UnsafePut(memberStoreBucketName, mkey, mvalue)
	tx.Unlock()
	return nil
}

func (s *StateMachine) DeleteMembership(id types.ID) error {
	if s.be == nil {
		return fmt.Errorf("can not found backend store")
	}
	mkey := MemberStoreKey(id)
	rmkey := RemovedMemberStoreKey(id)
	tx := s.be.BatchTx()
	tx.Lock()
	tx.UnsafeDelete(memberStoreBucketName, mkey)
	tx.UnsafePut(removeMemberStoreBucketName, rmkey, []byte("removed"))
	tx.Unlock()
	return nil
}

func (s *StateMachine) Members() (map[types.ID]*membership.Member, error) {
	if s.be == nil {
		return nil, fmt.Errorf("can not found backend store")
	}
	members := make(map[types.ID]*membership.Member)
	startKey := MemberStoreKey(types.ID(0))
	endKey := MemberStoreKey(types.ID(math.MaxUint64))

	tx := s.be.BatchTx()
	tx.Lock()
	membersKeys, membersVals := tx.UnsafeRange(memberStoreBucketName, startKey, endKey, 0)
	tx.Unlock()

	for index, _ := range membersKeys {
		var m membership.Member
		if err := json.Unmarshal(membersVals[index], &m); err != nil {
			return nil, fmt.Errorf("failed to unmarshal member from backend, memberID: %s, err: %v", string(membersKeys[index]), err)
		}
		id, err := types.IDFromString(path.Base(string(membersKeys[index])))
		if err != nil {
			return nil, fmt.Errorf("failed to ParseUint, memberID: %s, err: %v", string(membersKeys[index]), err)
		}
		members[id] = &m
	}
	return members, nil
}

func (s *StateMachine) RemovedMembers() (map[types.ID]bool, error) {
	if s.be == nil {
		return nil, fmt.Errorf("can not found backend store")
	}
	removed := make(map[types.ID]bool)
	startKey := RemovedMemberStoreKey(types.ID(0))
	endKey := RemovedMemberStoreKey(types.ID(math.MaxUint64))

	tx := s.be.BatchTx()
	tx.Lock()
	removedKeys, _ := tx.UnsafeRange(removeMemberStoreBucketName, startKey, endKey, 0)
	tx.Unlock()

	for index, _ := range removedKeys {
		//var m membership.Member
		//if err := json.Unmarshal(removedVals[index], &m); err != nil {
		//	return nil, fmt.Errorf("failed to unmarshal removed member from backend, memberID: %s, err: %v", string(removedKeys[index]), err)
		//}
		id, err := types.IDFromString(path.Base(string(removedKeys[index])))
		if err != nil {
			return nil, fmt.Errorf("failed to ParseUint, memberID: %s, err: %v", string(removedKeys[index]), err)
		}
		removed[id] = true
	}
	return removed, nil
}

func mustCreateBackendBuckets(be backend.Backend) {
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafeCreateBucket(memberStoreBucketName)
	tx.UnsafeCreateBucket(removeMemberStoreBucketName)
	tx.UnsafeCreateBucket(clusterBucketName)
}
