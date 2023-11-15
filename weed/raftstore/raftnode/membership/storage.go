package membership

import (
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"

	"go.uber.org/zap"
)

const defaultClusterMembershipSize = 5

func saveMemberToChanger(changer *MembershipChanger, m *Member) {
	if changer.members == nil {
		changer.members = make(map[types.ID]*Member, defaultClusterMembershipSize)
	}
	changer.members[m.ID] = m
}

func saveDeleteMemberToChanger(changer *MembershipChanger, id types.ID) {
	if changer.members != nil {
		if _, ok := changer.members[id]; ok {
			delete(changer.members, id)
		}
	}
	if changer.removed == nil {
		changer.removed = make(map[types.ID]bool, defaultClusterMembershipSize)
	}
	changer.removed[id] = true
}

func mustSaveMemberToStorage(lg *zap.Logger, st Storage, m *Member) {
	if err := st.SaveMembership(m); err != nil {
		lg.Panic(
			"failed to save member information to storage",
			zap.String("member-id", m.ID.String()),
			zap.String("err", err.Error()),
		)
	}
	lg.Info(
		"save member information to storage",
		zap.String("member-id", m.ID.String()),
	)
}

func mustDeleteMemberFromStorage(lg *zap.Logger, st Storage, id types.ID) {
	if err := st.DeleteMembership(id); err != nil {
		lg.Panic(
			"failed to delete member information from storage",
			zap.String("member-id", id.String()),
			zap.String("err", err.Error()),
		)
	}
	lg.Info(
		"save deleted member information to storage",
		zap.String("member-id", id.String()),
	)
}

// Storage used to persist membership
type Storage interface {
	Initialize() error
	Members() (map[types.ID]*Member, error)
	RemovedMembers() (map[types.ID]bool, error)
	SaveMembership(m *Member) error
	DeleteMembership(types.ID) error
}
