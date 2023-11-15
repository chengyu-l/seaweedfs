package raftnode

import (
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/logutil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/wal"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/wal/walpb"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/pkg/contention"
	"go.uber.org/zap"
)

const (
	maxSizePerMsg      = 1 * 1024 * 1024
	maxInflightMsgs    = 4096 / 8
	maxInflightMsgSnap = 16
)

type RaftNode struct {
	lg *zap.Logger

	RaftNodeConfig
	tickMu sync.RWMutex
	ticker *time.Ticker
	td     *contention.TimeoutDetector

	readStateC chan raft.ReadState // Send out readState
	msgSnapC   chan raftpb.Message // Send/Receive snapshot
	applyC     chan Apply          // Send apply

	stopC chan struct{}
	doneC chan struct{}
}

type RaftNodeConfig struct {
	Logger *zap.Logger
	raft.Node
	Heartbeat   time.Duration
	Storage     Storage             // store Raft State && Log Entries
	RaftStorage *raft.MemoryStorage // retrieve log entries/state from storage

	Transport   Transporter
	IsIDRemoved func(id uint64) bool // Check if msg receiver is removed from cluster
}

func NewRaftNode(cfg RaftNodeConfig) *RaftNode {
	r := &RaftNode{
		lg:             cfg.Logger,
		ticker:         time.NewTicker(cfg.Heartbeat),
		td:             contention.NewTimeoutDetector(2 * cfg.Heartbeat),
		RaftNodeConfig: cfg,
		readStateC:     make(chan raft.ReadState, 1),
		msgSnapC:       make(chan raftpb.Message, maxInflightMsgSnap),
		applyC:         make(chan Apply),
		stopC:          make(chan struct{}),
		doneC:          make(chan struct{}),
	}
	if r.Heartbeat == 0 {
		r.ticker = &time.Ticker{}
	}
	return r
}

func (r *RaftNode) tick() {
	r.tickMu.Lock()
	defer r.tickMu.Unlock()
	r.Node.Tick()
}

type RaftReadyHandler struct {
	GetLead              func() (lead uint64)
	UpdateLead           func(lead uint64)
	UpdateLeadership     func(newLeader bool)
	UpdateCommittedIndex func(uint64)
}

// Apply contains entries, snapshot to be applied. Once
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type Apply struct {
	Entries  []raftpb.Entry
	Snapshot raftpb.Snapshot
	// notifyc synchronizes raft relay applies with the raft node
	Notifyc chan struct{}
}

func (r *RaftNode) ReadStateNotify() chan raft.ReadState {
	return r.readStateC
}

func (r *RaftNode) MsgSnapNotify() chan raftpb.Message {
	return r.msgSnapC
}

// Start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *RaftNode) Start(rh *RaftReadyHandler) {
	internalTimeout := time.Second

	go func() {
		defer r.onStop()
		islead := false

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Node.Ready():
				// Update Sort state
				if rd.SoftState != nil {
					newLeader := rd.SoftState.Lead != raft.None && rh.GetLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}
					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}
					rh.UpdateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					rh.UpdateLeadership(newLeader)
					r.td.Reset()
				}

				// Notify ReadIndex
				if len(rd.ReadStates) != 0 {
					select {
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						r.lg.Warn("timed out sending read state",
							zap.Duration("timeout", internalTimeout))
					case <-r.stopC:
						return
					}
				}

				// Send out apply
				notifyc := make(chan struct{}, 1)
				ap := Apply{
					Entries:  rd.CommittedEntries,
					Snapshot: rd.Snapshot,
					Notifyc:  notifyc,
				}

				updateCommittedIndex(&ap, rh)

				select {
				case r.applyC <- ap:
				case <-r.stopC:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.Transport.Send(r.processMessages(rd.Messages))
				}

				// gofail: var raftBeforeSave struct{}
				if err := r.Storage.Save(rd.HardState, rd.Entries); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					if err := r.Storage.SaveSnap(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
					}
					// server now claim the snapshot has been persisted onto the disk.
					// then the server can apply the snapshot.
					notifyc <- struct{}{}

					// gofail: var raftAfterSaveSnap struct{}
					r.RaftStorage.ApplySnapshot(rd.Snapshot)
					r.lg.Info("applied incoming Raft snapshot",
						zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					// gofail: var raftAfterApplySnap struct{}
				}

				r.RaftStorage.Append(rd.Entries)

				if !islead {
					// finish processing incoming messages before we signal raftdone chan
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before apply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := false
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopC:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					r.Transport.Send(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}

				r.Node.Advance()
			case <-r.stopC:
				return
			}
		}
	}()
}

func updateCommittedIndex(ap *Apply, rh *RaftReadyHandler) {
	var ci uint64
	if len(ap.Entries) != 0 {
		ci = ap.Entries[len(ap.Entries)-1].Index
	}
	if ap.Snapshot.Metadata.Index > ci {
		ci = ap.Snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.UpdateCommittedIndex(ci)
	}
}

func (r *RaftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.IsIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", ms[i].To)),
					zap.Duration("heartbeat-interval", r.Heartbeat),
					zap.Duration("expected-duration", 2*r.Heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (r *RaftNode) Apply() chan Apply {
	return r.applyC
}

func (r *RaftNode) Stop() {
	r.stopC <- struct{}{}
	<-r.doneC
}

func (r *RaftNode) onStop() {
	r.Node.Stop()
	r.ticker.Stop()
	r.Transport.Stop()
	if err := r.Storage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.doneC)
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *RaftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func initializeWAL(lg *zap.Logger, walDir string, memberID, clusterID uint64) *wal.WAL {
	metadata, err := proto.Marshal(
		&pb.Metadata{
			NodeID:    memberID,
			ClusterID: clusterID,
		},
	)
	if err != nil {
		lg.Panic("failed to marshal metadata", zap.Error(err))
	}
	w, err := wal.Create(lg, walDir, metadata)
	if err != nil {
		lg.Panic("failed to create WAL", zap.Error(err))
	}
	return w
}

func initializePeers(lg *zap.Logger, cl *membership.RaftCluster, ids []types.ID) []raft.Peer {
	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		var ctx []byte
		var err error
		ctx, err = json.Marshal((*cl).Member(id))
		if err != nil {
			lg.Panic("failed to marshal member", zap.Error(err))
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	return peers
}

func initializeRaftLogger(cfg RaftNodeStartConfig) raft.Logger {
	var (
		logger raft.Logger
		err    error
	)
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		if cfg.LoggerConfig != nil {
			logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				glog.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}
	return logger
}

func StartRaftNode(cfg RaftNodeStartConfig, cl *membership.RaftCluster, ids []types.ID) (types.ID, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	member := cl.MemberByName(cfg.Name)
	wal := initializeWAL(cfg.Logger, cfg.WALDir(), uint64(member.ID), uint64(cl.ID()))
	peers := initializePeers(cfg.Logger, cl, ids)
	raftStorage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              uint64(member.ID),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         raftStorage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	c.Logger = initializeRaftLogger(cfg)

	var raftNode raft.Node
	if len(peers) == 0 {
		raftNode = raft.RestartNode(c)
	} else {
		raftNode = raft.StartNode(c, peers)
	}
	return member.ID, raftNode, raftStorage, wal
}

// RestartRaftNode start raft.Node according snapshot information
func RestartRaftNode(cfg RaftNodeStartConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	wal, memberID, clusterID, hardState, entries := readWAL(cfg.Logger, cfg.WALDir(), walsnap)

	cfg.Logger.Info(
		"restarting local member",
		zap.String("cluster-id", clusterID.String()),
		zap.String("local-member-id", memberID.String()),
		zap.Uint64("commit-index", hardState.Commit),
	)
	cl := membership.NewCluster(cfg.Logger, "")
	cl.SetID(memberID, clusterID)

	// Reconstruct raft.Storage
	raftStorage := raft.NewMemoryStorage()
	if snapshot != nil {
		raftStorage.ApplySnapshot(*snapshot)
	}
	raftStorage.SetHardState(hardState)
	raftStorage.Append(entries)

	c := &raft.Config{
		ID:              uint64(memberID),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         raftStorage,
		Applied:         cfg.AppliedIndex,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	c.Logger = initializeRaftLogger(cfg)

	raftNode := raft.RestartNode(c)
	return memberID, cl, raftNode, raftStorage, wal
}

//func RestartAsStandaloneNode(cfg RaftNodeStartConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
//	var walsnap walpb.Snapshot
//	if snapshot != nil {
//		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
//	}
//	wal, memberID, clusterID, hardState, entries := readWAL(cfg.Logger, cfg.WALDir(), walsnap)
//
//	// discard the previously uncommitted entries
//	for i, ent := range entries {
//		if ent.Index > hardState.Commit {
//			cfg.Logger.Info(
//				"discarding uncommitted WAL entries",
//				zap.Uint64("entry-index", ent.Index),
//				zap.Uint64("commit-index-from-wal", st.Commit),
//				zap.Int("number-of-discarded-entries", len(ents)-i),
//			)
//			ents = ents[:i]
//			break
//		}
//	}
//
//	// force append the configuration change entries
//	toAppEnts := createConfigChangeEnts(
//		cfg.Logger,
//		getIDs(cfg.Logger, snapshot, ents),
//		uint64(memberID),
//		hardState.Term,
//		hardState.Commit,
//	)
//	ents = append(ents, toAppEnts...)
//
//	// force commit newly appended entries
//	err := wal.Save(raftpb.HardState{}, toAppEnts)
//	if err != nil {
//		cfg.Logger.Fatal("failed to save hard state and entries", zap.Error(err))
//	}
//	if len(ents) != 0 {
//		hardState.Commit = ents[len(ents)-1].Index
//	}
//
//	cfg.Logger.Info(
//		"forcing restart member",
//		zap.String("cluster-id", clusterID.String()),
//		zap.String("local-member-id", memberID.String()),
//		zap.Uint64("commit-index", hardState.Commit),
//	)
//
//	cl := membership.NewCluster(cfg.Logger, "")
//	cl.SetID(id, cid)
//	raftStorage := raft.NewMemoryStorage()
//	if snapshot != nil {
//		raftStorage.ApplySnapshot(*snapshot)
//	}
//	raftStorage.SetHardState(hardState)
//	raftStorage.Append(ents)
//
//	c := &raft.Config{
//		ID:              uint64(id),
//		ElectionTick:    cfg.ElectionTicks,
//		HeartbeatTick:   1,
//		Storage:         s,
//		MaxSizePerMsg:   maxSizePerMsg,
//		MaxInflightMsgs: maxInflightMsgs,
//		CheckQuorum:     true,
//		PreVote:         cfg.PreVote,
//	}
//	c.Logger = initializeRaftLogger(cfg)
//
//	raftNode := raft.RestartNode(c)
//	return memberID, clusterID, raftNode, raftStorage, wal
//}
//
//// getIDs returns an ordered set of IDs included in the given snapshot and
//// the entries. The given snapshot/entries can contain two kinds of
//// ID-related entry:
//// - ConfChangeAddNode, in which case the contained ID will be added into the set.
//// - ConfChangeRemoveNode, in which case the contained ID will be removed from the set.
//func getIDs(lg *zap.Logger, snap *raftpb.Snapshot, ents []raftpb.Entry) []uint64 {
//	ids := make(map[uint64]bool)
//	if snap != nil {
//		for _, id := range snap.Metadata.ConfState.Voters {
//			ids[id] = true
//		}
//	}
//	for _, e := range ents {
//		if e.Type != raftpb.EntryConfChange {
//			continue
//		}
//		var cc raftpb.ConfChange
//		pbutil.MustUnmarshal(&cc, e.Data)
//		switch cc.Type {
//		case raftpb.ConfChangeAddNode:
//			ids[cc.NodeID] = true
//		case raftpb.ConfChangeRemoveNode:
//			delete(ids, cc.NodeID)
//		case raftpb.ConfChangeUpdateNode:
//			// do nothing
//		default:
//			lg.Panic("unknown ConfChange Type", zap.String("type", cc.Type.String()))
//		}
//	}
//	sids := make(types.Uint64Slice, 0, len(ids))
//	for id := range ids {
//		sids = append(sids, id)
//	}
//	sort.Sort(sids)
//	return []uint64(sids)
//}
//
//// createConfigChangeEnts creates a series of Raft entries (i.e.
//// EntryConfChange) to remove the set of given IDs from the cluster. The ID
//// `self` is _not_ removed, even if present in the set.
//// If `self` is not inside the given ids, it creates a Raft entry to add a
//// default member with the given `self`.
//func createConfigChangeEnts(lg *zap.Logger, ids []uint64, self uint64, term, index uint64) []raftpb.Entry {
//	found := false
//	for _, id := range ids {
//		if id == self {
//			found = true
//		}
//	}
//
//	var ents []raftpb.Entry
//	next := index + 1
//
//	// NB: always add self first, then remove other nodes. Raft will panic if the
//	// set of voters ever becomes empty.
//	if !found {
//		m := membership.Member{
//			ID:             types.ID(self),
//			RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
//		}
//		ctx, err := json.Marshal(m)
//		if err != nil {
//			lg.Panic("failed to marshal member", zap.Error(err))
//		}
//		cc := &raftpb.ConfChange{
//			Type:    raftpb.ConfChangeAddNode,
//			NodeID:  self,
//			Context: ctx,
//		}
//		e := raftpb.Entry{
//			Type:  raftpb.EntryConfChange,
//			Data:  proto.Marshal(cc),
//			Term:  term,
//			Index: next,
//		}
//		ents = append(ents, e)
//		next++
//	}
//
//	for _, id := range ids {
//		if id == self {
//			continue
//		}
//		cc := &raftpb.ConfChange{
//			Type:   raftpb.ConfChangeRemoveNode,
//			NodeID: id,
//		}
//		e := raftpb.Entry{
//			Type:  raftpb.EntryConfChange,
//			Data:  proto.Marshal(cc),
//			Term:  term,
//			Index: next,
//		}
//		ents = append(ents, e)
//		next++
//	}
//
//	return ents
//}
