package raftrelay

import (
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

func (r *RaftRelay) handleSnapshot(ep *Progress) {
	r.triggerSnapshot(ep)
	select {
	case m := <-r.raftNode.MsgSnapNotify():
		snapshot := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index:     ep.appliedi,
				Term:      ep.appliedt,
				ConfState: ep.confState,
			},
		}
		m.Snapshot = snapshot
		// filling the snapshot data from backend stata machine
		msg := r.stateMachine.CreateSnapshotMessage(m)
		r.sendMergedSnap(msg)
	default:
	}
}

func (s *RaftRelay) triggerSnapshot(ep *Progress) {
	if ep.appliedi-ep.snapi <= s.Cfg.SnapshotCount {
		return
	}

	lg := s.getLogger()
	lg.Info(
		"triggering snapshot",
		zap.String("local-member-id", s.LocalID().String()),
		zap.Uint64("local-member-applied-index", ep.appliedi),
		zap.Uint64("local-member-snapshot-index", ep.snapi),
		zap.Uint64("local-member-snapshot-count", s.Cfg.SnapshotCount),
	)

	s.snapshot(ep.appliedi, ep.confState)
	ep.snapi = ep.appliedi
}

// TODO: non-blocking snapshot
func (r *RaftRelay) snapshot(snapi uint64, confState raftpb.ConfState) {
	// persist membership to stable storage
	r.clusterMembership.Persist()
	// commit stateMachine to write metadata (for example: consistent index) to disk.
	r.stateMachine.Commit()

	r.goAttach(func() {
		lg := r.getLogger()

		snap, err := r.raftNode.RaftStorage.CreateSnapshot(snapi, &confState, nil)
		if err != nil {
			// the snapshot was done asynchronously with the progress of raft.
			// raft might have already got a newer snapshot.
			if err == raft.ErrSnapOutOfDate {
				return
			}
			lg.Panic("failed to create snapshot", zap.Error(err))
		}
		// SaveSnap saves the snapshot and releases the locked wal files
		// to the snapshot index.
		if err = r.raftNode.Storage.SaveSnap(snap); err != nil {
			lg.Panic("failed to save snapshot", zap.Error(err))
		}
		lg.Info(
			"saved snapshot",
			zap.Uint64("snapshot-index", snap.Metadata.Index),
		)

		// When sending a snapshot, etcd will pause compaction.
		// After receives a snapshot, the slow follower needs to get all the entries right after
		// the snapshot sent to catch up. If we do not pause compaction, the log entries right after
		// the snapshot sent might already be compacted. It happens when the snapshot takes long time
		// to send and save. Pausing compaction avoids triggering a snapshot sending cycle.
		if atomic.LoadInt64(&r.inflightSnapshots) != 0 {
			lg.Info("skip compaction since there is an inflight snapshot")
			return
		}

		// keep some in memory log entries for slow followers.
		compacti := uint64(1)
		if snapi > r.Cfg.SnapshotCatchUpEntries {
			compacti = snapi - r.Cfg.SnapshotCatchUpEntries
		}

		err = r.raftNode.RaftStorage.Compact(compacti)
		if err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == raft.ErrCompacted {
				return
			}
			lg.Panic("failed to compact", zap.Error(err))
		}
		lg.Info(
			"compacted Raft logs",
			zap.Uint64("compact-index", compacti),
		)
	})
}

func (r *RaftRelay) sendMergedSnap(snapMsg *raftnode.SnapMessage) {
	atomic.AddInt64(&r.inflightSnapshots, 1)

	lg := r.getLogger()
	fields := []zap.Field{
		zap.String("from", r.LocalID().String()),
		zap.String("to", types.ID(snapMsg.To).String()),
		zap.Int64("bytes", snapMsg.TotalSize),
		zap.String("size", humanize.Bytes(uint64(snapMsg.TotalSize))),
		zap.Uint64("term", snapMsg.Message.Snapshot.Metadata.Term),
		zap.Uint64("index", snapMsg.Message.Snapshot.Metadata.Index),
	}

	now := time.Now()
	r.raftNode.Transport.SendSnapshot(*snapMsg)
	lg.Info("sending snapshot", fields...)

	r.goAttach(func() {
		select {
		case ok := <-snapMsg.CloseNotify():
			// delay releasing inflight snapshot for another 30 seconds to
			// block log compaction.
			// If the follower still fails to catch up, it is probably just too slow
			// to catch up. We cannot avoid the snapshot cycle anyway.
			if ok {
				select {
				case <-time.After(releaseDelayAfterSnapshot):
				case <-r.stopping:
				}
			}
			atomic.AddInt64(&r.inflightSnapshots, -1)

			lg.Info("sent snapshot", append(fields, zap.Duration("took", time.Since(now)))...)

		case <-r.stopping:
			lg.Warn("canceled sending snapshot; server stopping", fields...)
			return
		}
	})
}
