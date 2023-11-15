package raftnode

import (
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/wal"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/wal/walpb"
	"io"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap raftpb.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
}

type storage struct {
	*wal.WAL
	SnapshotMgr
}

func NewStorage(w *wal.WAL, s SnapshotMgr) Storage {
	return &storage{w, s}
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *storage) SaveSnap(snap raftpb.Snapshot) error {
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	err := st.WAL.SaveSnapshot(walsnap)
	if err != nil {
		return err
	}
	err = st.SnapshotMgr.SaveSnap(snap)
	if err != nil {
		return err
	}
	return st.WAL.ReleaseLockTo(snap.Metadata.Index)
}

func readWAL(lg *zap.Logger, waldir string, snap walpb.Snapshot) (w *wal.WAL, memberID, clusterID types.ID,
	hardState raftpb.HardState, entries []raftpb.Entry) {
	var (
		err       error
		wmetadata []byte
	)

	repaired := false
	for {
		if w, err = wal.Open(lg, waldir, snap); err != nil {
			lg.Fatal("failed to open WAL", zap.Error(err))
		}
		if wmetadata, hardState, entries, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				lg.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
			}
			if !wal.Repair(lg, waldir) {
				lg.Fatal("failed to repair WAL", zap.Error(err))
			} else {
				lg.Info("repaired WAL", zap.Error(err))
				repaired = true
			}
			continue
		}
		break
	}
	var metadata pb.Metadata
	if err := proto.Unmarshal(wmetadata, &metadata); err != nil {
		lg.Fatal("failed to unmarshal metadata", zap.Error(err))
	}
	memberID = types.ID(metadata.NodeID)
	clusterID = types.ID(metadata.ClusterID)
	return w, memberID, clusterID, hardState, entries
}
