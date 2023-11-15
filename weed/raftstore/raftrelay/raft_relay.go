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

package raftrelay

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/fileutil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/idutil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/schedule"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/wait"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/rafthttp"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/wal"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	DefaultSnapshotCount          = 10000
	DefaultSnapshotCatchUpEntries = 5000
	maxInFlightMsgSnap            = 16
	releaseDelayAfterSnapshot     = 30 * time.Second
	HealthInterval                = 5 * time.Second

	purgeFileInterval          = 30 * time.Second
	readyPercent               = 0.9
	recommendedMaxRequestBytes = 10 * 1024 * 1024
)

type RaftRelay struct {
	inflightSnapshots int64
	appliedIndex      uint64
	committedIndex    uint64
	term              uint64
	lead              uint64

	lg  *zap.Logger
	Cfg RaftRelayConfig

	raftNode          *raftnode.RaftNode
	readyC            chan struct{}
	errorC            chan error
	clusterMembership *membership.RaftCluster

	wait   wait.Wait
	readMu sync.RWMutex
	// read routine notifies etcd server that it waits for reading by sending an empty struct to
	// readwaitC
	readwaitc chan struct{}
	// readNotifier is used to notify the read routine that it can process the request
	// when there is no error
	readNotifier *notifier

	// stop signals the run goroutine should shutdown.
	stop chan struct{}
	// stopping is closed by run goroutine on shutdown.
	stopping chan struct{}
	// done is closed when all goroutines from start() complete.
	done chan struct{}
	// leaderChanged is used to notify the linearizable read loop to drop the old read requests.
	leaderChanged   chan struct{}
	leaderChangedMu sync.RWMutex

	sched        schedule.Scheduler
	stateMachine raftnode.StateMachine

	applyWait wait.WaitTime
	reqIDGen  *idutil.Generator

	// Request to peer to initialize
	peerRt http.RoundTripper

	// wgMu blocks concurrent waitgroup mutation while server stopping
	wgMu sync.RWMutex
	// wg is used to wait for the goroutines that depends on the server state
	// to exit when stopping the server.
	wg sync.WaitGroup

	// ctx is used for etcd-initiated requests that may need to be canceled
	// on server shutdown.
	ctx    context.Context
	cancel context.CancelFunc

	leadTimeMu      sync.RWMutex
	leadElectedTime time.Time
}

func NewRaftRelay(cfg RaftRelayConfig) (rs *RaftRelay, err error) {

	if err := fileutil.TouchDirAll(cfg.DataDir); err != nil {
		return nil, fmt.Errorf("cannot access data directory: %v", err)
	}
	if err := fileutil.TouchDirAll(cfg.SnapDir()); err != nil {
		return nil, fmt.Errorf("cannot access snapshot directory: %v", err)
	}

	stateMachine := cfg.BuildStateMachine()
	existWAL := wal.Exist(cfg.WALDir())

	prt, err := rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.peerDialTimeout())
	if err != nil {
		return nil, err
	}

	var (
		wal         *wal.WAL
		raftNode    raft.Node
		raftStorage *raft.MemoryStorage
		memberID    types.ID
		cluster     *membership.RaftCluster

		remotes  []*membership.Member
		snapshot *raftpb.Snapshot
	)
	switch {
	// Join existed cluster
	case !existWAL && !cfg.NewCluster:
		if err := cfg.VerifyJoinExisting(); err != nil {
			return nil, err
		}
		cluster, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		existingCluster, gerr := GetClusterFromRemotePeers(cfg.Logger, getRemotePeerURLs(cluster, cfg.Name), prt)
		if gerr != nil {
			return nil, fmt.Errorf("cannot fetch cluster info from peer urls: %v", gerr)
		}
		if err = membership.ValidateClusterAndAssignIDs(cfg.Logger, cluster, existingCluster); err != nil {
			return nil, fmt.Errorf("error validating peerURLs %s: %v", existingCluster, err)
		}
		remotes = existingCluster.Members()
		cluster.SetStorage(stateMachine)
		// Update current cluster id to remote cluster id
		cluster.SetID(types.ID(0), existingCluster.ID())
		memberID, raftNode, raftStorage, wal = raftnode.StartRaftNode(cfg.BuildRaftNodeStartConfig(), cluster, nil)
		cluster.SetID(memberID, existingCluster.ID())

	// Start a node with a new cluster
	case !existWAL && cfg.NewCluster:
		if err = cfg.VerifyBootstrap(); err != nil {
			return nil, err
		}
		cluster, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		m := cluster.MemberByName(cfg.Name)
		if isMemberBootstrapped(cfg.Logger, cluster, cfg.Name, prt, cfg.bootstrapTimeout()) {
			return nil, fmt.Errorf("member %s has already been bootstrapped", m.ID)
		}
		cluster.SetStorage(stateMachine)
		memberID, raftNode, raftStorage, wal = raftnode.StartRaftNode(cfg.BuildRaftNodeStartConfig(), cluster, cluster.MemberIDs())
		cluster.SetID(memberID, cluster.ID())

	case existWAL:
		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {
			return nil, fmt.Errorf("cannot write to member directory: %v", err)
		}

		if err = fileutil.IsDirWriteable(cfg.WALDir()); err != nil {
			return nil, fmt.Errorf("cannot write to WAL directory: %v", err)
		}
		// Recover state machine from snapshot if needed
		ssm := stateMachine.SnapshotMgr()
		var err error
		if snapshot, err = ssm.Load(); err != nil {
			cfg.Logger.Panic("failed to load snapshot", zap.Error(err))
		}
		if err := stateMachine.RecoverFromSnapshot(snapshot); err != nil {
			cfg.Logger.Panic("failed to recover backend from snapshot", zap.Error(err))
		}
		raftnodeStartCfg := cfg.BuildRaftNodeStartConfig()
		memberID, cluster, raftNode, raftStorage, wal = raftnode.RestartRaftNode(raftnodeStartCfg, snapshot)

		// Recover membership from membership storage.
		// as the ConfChange entry will not sync the consistent index, any prev
		// ConfChanges will be apply again and any prev normal entries will be skipped.
		cluster.SetStorage(stateMachine)
		cluster.Recover()

	default:
		return nil, fmt.Errorf("unsupported bootstrap config")
	}

	raftRelay := &RaftRelay{
		lg:  cfg.Logger,
		Cfg: cfg,
		raftNode: raftnode.NewRaftNode(
			raftnode.RaftNodeConfig{
				Logger:      cfg.Logger,
				Node:        raftNode,
				Heartbeat:   time.Duration(cfg.TickMs) * time.Millisecond,
				Storage:     raftnode.NewStorage(wal, stateMachine.SnapshotMgr()),
				RaftStorage: raftStorage,
				IsIDRemoved: func(id uint64) bool { return cluster.IsIDRemoved(types.ID(id)) },
			}),
		stateMachine: stateMachine,
		peerRt:       prt,

		readyC:            make(chan struct{}, 1),
		errorC:            make(chan error, 1),
		clusterMembership: cluster,
		reqIDGen:          idutil.NewGenerator(uint16(memberID), time.Now()),
	}
	// The consistentIndex in state machine must larger than the one in snapshot
	if raftRelay.stateMachine != nil {
		kvIndex := raftRelay.stateMachine.ConsistentIndex()
		cfg.Logger.Info("state machine consistentIndex", zap.Uint64("Index", kvIndex))
		if snapshot != nil && snapshot.Metadata.Index > kvIndex {
			if kvIndex != 0 {
				return nil, fmt.Errorf("database file (index %d) does not match with snapshot (index %d)", kvIndex, snapshot.Metadata.Index)
			}
			cfg.Logger.Warn(
				"consistent index was never saved",
				zap.Uint64("snapshot-index", snapshot.Metadata.Index),
			)
		}
	}
	raftRelay.stateMachine.RegisterClusterMembershipHook(raftRelay.Cluster)

	tr := &rafthttp.Transport{
		Logger:      cfg.Logger,
		TLSInfo:     cfg.PeerTLSInfo,
		DialTimeout: cfg.peerDialTimeout(),
		ID:          memberID,
		URLs:        cfg.PeerURLs,
		ClusterID:   cluster.ID(),
		Raft:        raftRelay,
		SnapshotMgr: stateMachine.SnapshotMgr(),
		ErrorC:      raftRelay.errorC,
	}
	if err = tr.Start(); err != nil {
		return nil, err
	}
	// add all remotes into transport
	for _, m := range remotes {
		if m.ID != memberID {
			tr.AddRemote(m.ID, m.PeerURLs)
		}
	}
	for _, m := range cluster.Members() {
		if m.ID != memberID {
			tr.AddPeer(m.ID, m.PeerURLs)
		}
	}
	raftRelay.raftNode.Transport = tr

	return raftRelay, nil
}

func (rr *RaftRelay) getLogger() *zap.Logger {
	return rr.lg
}

// Start performs any initialization of the Server necessary for it to
// begin serving requests. It must be called before Do or Process.
// Start must be non-blocking; any long-running server functionality
// should be implemented in goroutines.
func (r *RaftRelay) Start() {
	r.start()
	r.goAttach(func() { r.publish(r.Cfg.ReqTimeout()) })
	r.goAttach(r.purgeFile)
	r.goAttach(r.linearizableReadLoop)
}

// goAttach creates a goroutine on a given function and tracks it using
// the etcdserver waitgroup.
func (r *RaftRelay) goAttach(f func()) {
	r.wgMu.RLock() // this blocks with ongoing close(r.stopping)
	defer r.wgMu.RUnlock()
	select {
	case <-r.stopping:
		lg := r.getLogger()
		lg.Warn("server has stopped; skipping goAttach")
		return
	default:
	}

	// now safe to add since waitgroup wait has not started yet
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f()
	}()
}

// start prepares and starts server in a new goroutine. It is no longer safe to
// modify a server's fields after it has been sent to Start.
// This function is just used for testing.
func (rr *RaftRelay) start() {
	if rr.Cfg.SnapshotCount == 0 {
		rr.lg.Info(
			"updating snapshot-count to default",
			zap.Uint64("given-snapshot-count", rr.Cfg.SnapshotCount),
			zap.Uint64("updated-snapshot-count", DefaultSnapshotCount),
		)
		rr.Cfg.SnapshotCount = DefaultSnapshotCount
	}
	if rr.Cfg.SnapshotCatchUpEntries == 0 {
		rr.lg.Info(
			"updating snapshot catch-up entries to default",
			zap.Uint64("given-snapshot-catchup-entries", rr.Cfg.SnapshotCatchUpEntries),
			zap.Uint64("updated-snapshot-catchup-entries", DefaultSnapshotCatchUpEntries),
		)
		rr.Cfg.SnapshotCatchUpEntries = DefaultSnapshotCatchUpEntries
	}

	rr.wait = wait.New()
	rr.applyWait = wait.NewTimeList()
	rr.done = make(chan struct{})
	rr.stop = make(chan struct{})
	rr.stopping = make(chan struct{})
	rr.ctx, rr.cancel = context.WithCancel(context.Background())
	rr.readwaitc = make(chan struct{}, 1)
	rr.readNotifier = newNotifier()
	rr.leaderChanged = make(chan struct{})

	// TODO: if this is an empty log, writes all peer infos
	// into the first entry
	go rr.run()
}

func (r *RaftRelay) purgeFile() {
	lg := r.getLogger()
	var dberrc, serrc, werrc <-chan error
	var dbdonec, sdonec, wdonec <-chan struct{}
	if r.Cfg.MaxSnapFiles > 0 {
		dbdonec, dberrc = fileutil.PurgeFileWithDoneNotify(lg, r.Cfg.SnapDir(), "snap.db", r.Cfg.MaxSnapFiles, purgeFileInterval, r.stopping)
		sdonec, serrc = fileutil.PurgeFileWithDoneNotify(lg, r.Cfg.SnapDir(), "snap", r.Cfg.MaxSnapFiles, purgeFileInterval, r.stopping)
	}
	if r.Cfg.MaxWALFiles > 0 {
		wdonec, werrc = fileutil.PurgeFileWithDoneNotify(lg, r.Cfg.WALDir(), "wal", r.Cfg.MaxWALFiles, purgeFileInterval, r.stopping)
	}

	select {
	case e := <-dberrc:
		lg.Fatal("failed to purge snap db file", zap.Error(e))
	case e := <-serrc:
		lg.Fatal("failed to purge snap file", zap.Error(e))
	case e := <-werrc:
		lg.Fatal("failed to purge wal file", zap.Error(e))
	case <-r.stopping:
		if dbdonec != nil {
			<-dbdonec
		}
		if sdonec != nil {
			<-sdonec
		}
		if wdonec != nil {
			<-wdonec
		}
		return
	}
}

type Progress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedt  uint64
	appliedi  uint64
}

func (r *RaftRelay) run() {
	lg := r.getLogger()

	// asynchronously accept apply packets, dispatch progress in-order
	r.sched = schedule.NewFIFOScheduler()

	// initialize current progress
	sn, err := r.raftNode.RaftStorage.Snapshot()
	if err != nil {
		lg.Panic("failed to get snapshot from Raft storage", zap.Error(err))
	}
	ep := Progress{
		confState: sn.Metadata.ConfState,
		snapi:     sn.Metadata.Index,
		appliedt:  sn.Metadata.Term,
		appliedi:  sn.Metadata.Index,
	}

	// start raftnode to handle ready events
	rh := &raftnode.RaftReadyHandler{
		GetLead:    func() (lead uint64) { return r.getLead() },
		UpdateLead: func(lead uint64) { r.setLead(lead) },
		UpdateLeadership: func(newLeader bool) {
			if newLeader {
				r.leaderChangedMu.Lock()
				c := r.leaderChanged
				r.leaderChanged = make(chan struct{})
				close(c)
				r.leaderChangedMu.Unlock()
			}
		},
		UpdateCommittedIndex: func(committedIndex uint64) {
			if committedIndex > r.getCommittedIndex() {
				r.setCommittedIndex(committedIndex)
			}
		},
	}
	r.raftNode.Start(rh)

	defer r.cleanupServer()

	for {
		select {
		// Handle committed entries in background goroutine
		case ap := <-r.raftNode.Apply():
			f := func(context.Context) { r.applyAll(&ep, &ap) }
			r.sched.Schedule(f)
		// Handle error
		case err := <-r.errorC:
			lg.Warn("server error", zap.Error(err))
			lg.Warn("data-dir used by this member must be removed")
			return
		case <-r.stop:
			return
		}
	}
}

func (r *RaftRelay) cleanupServer() {
	// 1. Wait all interanl goroutines drain
	r.wgMu.Lock()
	close(r.stopping)
	r.wgMu.Unlock()

	// 2. Stop scheduled tasks
	r.sched.Stop()

	// must stop raft after scheduler-- can leak pipelines
	// by adding a peer after raft stops the transport
	r.raftNode.Stop()

	// wait for gouroutines before closing raft so wal stays open
	r.wg.Wait()

	// 3. Stop all context based operations
	r.cancel()

	// 4. Stop state machine
	r.stateMachine.Close()

	close(r.done)
}

// TransferLeadership transfers the leader to the chosen transferee.
func (rr *RaftRelay) TransferLeadership() error {
	if !rr.IsLeader() {
		rr.lg.Info(
			"skipped leadership transfer; local server is not leader",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("current-leader-member-id", types.ID(rr.Lead()).String()),
		)
		return nil
	}

	if !rr.hasMultipleVotingMembers() {
		rr.lg.Info(
			"skipped leadership transfer for single voting member cluster",
			zap.String("local-member-id", rr.LocalID().String()),
			zap.String("current-leader-member-id", types.ID(rr.Lead()).String()),
		)
		return nil
	}

	transferee, ok := longestConnected(rr.raftNode.Transport, rr.clusterMembership.VotingMemberIDs())
	if !ok {
		return ErrUnhealthy
	}

	tm := rr.Cfg.ReqTimeout()
	ctx, cancel := context.WithTimeout(rr.ctx, tm)
	err := rr.MoveLeader(ctx, rr.Lead(), uint64(transferee))
	cancel()
	return err
}

func (rr *RaftRelay) hasMultipleVotingMembers() bool {
	return rr.clusterMembership != nil && len(rr.clusterMembership.VotingMemberIDs()) > 1
}

// HardStop stops the server without coordination with other members in the cluster.
func (rr *RaftRelay) HardStop() {
	select {
	case rr.stop <- struct{}{}:
	case <-rr.done:
		return
	}
	<-rr.done
}

// Stop stops the server gracefully, and shuts down the running goroutine.
// Stop should be called after a Start(s), otherwise it will block forever.
// When stopping leader, Stop transfers its leadership to one of its peers
// before stopping the server.
// Stop terminates the Server and performs any necessary finalization.
// Do and Process cannot be called after Stop has been invoked.
func (rr *RaftRelay) Stop() {
	if err := rr.TransferLeadership(); err != nil {
		rr.lg.Warn("leadership transfer failed", zap.String("local-member-id", rr.LocalID().String()), zap.Error(err))
	}
	rr.HardStop()
}

// ReadyNotify returns a channel that will be closed when the server
// is ready to serve client requests
func (rr *RaftRelay) ReadyNotify() <-chan struct{} { return rr.readyC }

func (rr *RaftRelay) stopWithDelay(d time.Duration, err error) {
	select {
	case <-time.After(d):
	case <-rr.done:
	}
	select {
	case rr.errorC <- err:
	default:
	}
}

// StopNotify returns a channel that receives a empty struct
// when the server is stopped.
func (rr *RaftRelay) StopNotify() <-chan struct{} { return rr.done }

func (rr *RaftRelay) setCommittedIndex(v uint64) {
	atomic.StoreUint64(&rr.committedIndex, v)
}

func (rr *RaftRelay) getCommittedIndex() uint64 {
	return atomic.LoadUint64(&rr.committedIndex)
}

func (rr *RaftRelay) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&rr.appliedIndex, v)
}

func (rr *RaftRelay) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&rr.appliedIndex)
}

func (rr *RaftRelay) setTerm(v uint64) {
	atomic.StoreUint64(&rr.term, v)
}

func (rr *RaftRelay) getTerm() uint64 {
	return atomic.LoadUint64(&rr.term)
}

func (rr *RaftRelay) setLead(v uint64) {
	atomic.StoreUint64(&rr.lead, v)
}

func (rr *RaftRelay) getLead() uint64 {
	return atomic.LoadUint64(&rr.lead)
}

func (rr *RaftRelay) LeaderChangedNotify() <-chan struct{} {
	rr.leaderChangedMu.RLock()
	defer rr.leaderChangedMu.RUnlock()
	return rr.leaderChanged
}

// Process takes a raft message and applies it to the server's raft state
// machine, respecting any timeout of the given context.
func (rr *RaftRelay) Process(ctx context.Context, m raftpb.Message) error {
	if rr.clusterMembership.IsIDRemoved(types.ID(m.From)) {
		if lg := rr.getLogger(); lg != nil {
			lg.Warn(
				"rejected Raft message from removed member",
				zap.String("local-member-id", rr.LocalID().String()),
				zap.String("removed-member-id", types.ID(m.From).String()),
			)
		} else {
			glog.Warningf("reject message from removed member %s", types.ID(m.From).String())
		}
		return NewHTTPError(http.StatusForbidden, "cannot process message from removed member")
	}
	return rr.raftNode.Step(ctx, m)
}

func (rr *RaftRelay) IsIDRemoved(id uint64) bool {
	return rr.clusterMembership.IsIDRemoved(types.ID(id))
}

func (rr *RaftRelay) ReportUnreachable(id uint64) { rr.raftNode.ReportUnreachable(id) }

// ReportSnapshot reports snapshot sent status to the raft state machine,
// and clears the used snapshot from the snapshot store.
func (rr *RaftRelay) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rr.raftNode.ReportSnapshot(id, status)
}

func (rr *RaftRelay) LeaderClientAddrs() []string {
	leaderId := rr.Leader()
	leaderMember := rr.clusterMembership.Member(leaderId)
	if leaderMember == nil {
		if lg := rr.getLogger(); lg != nil {
			lg.Warn(
				"get leader address failed",
				zap.String("local-member-id", rr.LocalID().String()),
				zap.String("leader-id", leaderId.String()),
			)
		} else {
			glog.Warning("get leader address failed")
		}
		return nil
	}
	return leaderMember.Attributes.ClientURLs
}

func (rr *RaftRelay) Peers() []*membership.Member {
	return rr.clusterMembership.Members()
}

func (rr *RaftRelay) Name() string {
	return rr.Cfg.Name
}

// RaftStatusGetter represents etcd server and Raft progress.
type RaftStatusGetter interface {
	ClusterID() types.ID
	LocalID() types.ID
	Leader() types.ID
	CommittedIndex() uint64
	AppliedIndex() uint64
	Term() uint64
	IsLearner() bool
	IsLeader() bool
}

func (rr *RaftRelay) ClusterID() types.ID { return rr.clusterMembership.ID() }

func (rr *RaftRelay) LocalID() types.ID { return rr.clusterMembership.LocalMemberID() }

func (rr *RaftRelay) Leader() types.ID { return types.ID(rr.getLead()) }

func (rr *RaftRelay) Lead() uint64 { return rr.getLead() }

func (rr *RaftRelay) CommittedIndex() uint64 { return rr.getCommittedIndex() }

func (rr *RaftRelay) AppliedIndex() uint64 { return rr.getAppliedIndex() }

func (rr *RaftRelay) Term() uint64 { return rr.getTerm() }

func (rr *RaftRelay) IsLearner() bool { return rr.clusterMembership.IsLocalMemberLearner() }

func (rr *RaftRelay) IsLeader() bool { return rr.LocalID() == rr.Leader() }

// raftStatus returns the raft status of this etcd node.
func (rr *RaftRelay) raftStatus() raft.Status {
	return rr.raftNode.Node.Status()
}

// publish registers server information into the cluster. The information
// is the JSON representation of this server's member struct, updated with the
// static clientURLs of the server.
// The function keeps attempting to register until it succeeds,
// or its server is stopped.
func (r *RaftRelay) publish(timeout time.Duration) {
	attr := membership.Attributes{Name: r.Cfg.Name, ClientURLs: r.Cfg.ClientURLs.StringSlice()}
	req := &pb.MemberNoRaftAttrUpdateRequest{
		MemberId: uint64(r.LocalID()),
		MemberAttributes: &pb.Attributes{
			Name:       attr.Name,
			ClientUrls: attr.ClientURLs,
		},
	}
	lg := r.getLogger()
	for {
		select {
		case <-r.stopping:
			lg.Warn(
				"stopped publish because server is stopping",
				zap.String("local-member-id", r.LocalID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", attr)),
				zap.Duration("publish-timeout", timeout),
			)
			return

		default:
		}

		ctx, cancel := context.WithTimeout(r.ctx, timeout)
		_, err := r.raftRequest(ctx,
			pb.InternalRaftRequest{
				AdminRequest: &pb.AdminRequest{
					CmdType: pb.AdminCmdType_MemberNoRaftAttrUpdate, MemberNoRaftAttrUpdate: req},
			})
		cancel()
		switch err {
		case nil:
			close(r.readyC)
			lg.Info(
				"published local member to cluster through raft",
				zap.String("local-member-id", r.LocalID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", attr)),
				zap.String("cluster-id", r.clusterMembership.ID().String()),
				zap.Duration("publish-timeout", timeout),
			)
			return

		default:
			lg.Warn(
				"failed to publish local member to cluster through raft",
				zap.String("local-member-id", r.LocalID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", attr)),
				zap.Duration("publish-timeout", timeout),
				zap.Error(err),
			)
		}
	}
}
