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
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/netutil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/transport"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine"
	"path/filepath"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type StateMachineType string

const (
	BoltDBBackend  StateMachineType = "BoltDB"
	RocksDBBackend StateMachineType = "RocksDB"
)

// RaftRelayConfig holds the configuration of etcd as taken from the command line or discovery.
type RaftRelayConfig struct {
	Name       string
	PeerURLs   types.URLs
	ClientURLs types.URLs
	DataDir    string
	// DedicatedWALDir config will make the etcd to write the WAL to the WALDir
	// rather than the dataDir/member/wal.
	DedicatedWALDir string

	SnapshotCount uint64
	// SnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	// WARNING: only change this for tests. Always use "DefaultSnapshotCatchUpEntries"
	SnapshotCatchUpEntries uint64
	MaxSnapFiles           uint
	MaxWALFiles            uint

	// Cluster information
	InitialPeerURLsMap types.URLsMap
	PeerTLSInfo        transport.TLSInfo
	NewCluster         bool

	InitialClusterToken string

	TickMs        uint
	ElectionTicks int

	// InitialElectionTickAdvance is true, then local member fast-forwards
	// election ticks to speed up "initial" leader election trigger. This
	// benefits the case of larger election ticks. For instance, cross
	// datacenter deployment may require longer election timeout of 10-second.
	// If true, local node does not need wait up to 10-second. Instead,
	// forwards its election ticks to 8-second, and have only 2-second left
	// before leader election.
	//
	// Major assumptions are that:
	//  - cluster has no active leader thus advancing ticks enables faster
	//    leader election, or
	//  - cluster already has an established leader, and rejoining follower
	//    is likely to receive heartbeats from the leader after tick advance
	//    and before election timeout.
	//
	// However, when network from leader to rejoining follower is congested,
	// and the follower does not receive leader heartbeat within left election
	// ticks, disruptive election has to happen thus affecting cluster
	// availabilities.
	//
	// Disabling this would slow down initial bootstrap process for cross
	// datacenter deployments. Make your own tradeoffs by configuring
	// --initial-election-tick-advance at the cost of slow initial bootstrap.
	//
	// If single-node, it advances ticks regardless.
	//
	// See https://github.com/etcd-io/etcd/issues/9333 for more detail.
	InitialElectionTickAdvance bool

	BootstrapTimeout time.Duration

	MaxTxnOps uint
	// MaxRequestBytes is the maximum request size to send over raft.
	MaxRequestBytes uint

	// InitialCorruptCheck is true to check data corruption on boot
	// before serving any peer/client traffic.
	InitialCorruptCheck bool
	CorruptCheckTime    time.Duration

	// PreVote is true to enable Raft Pre-Vote.
	PreVote bool

	// Logger logs server-side operations.
	// If not nil, it disables "capnslog" and uses the given logger.
	Logger *zap.Logger

	// LoggerConfig is server logger configuration for Raft logger.
	// Must be either: "LoggerConfig != nil" or "LoggerCore != nil && LoggerWriteSyncer != nil".
	LoggerConfig *zap.Config
	// LoggerCore is "zapcore.Core" for raft logger.
	// Must be either: "LoggerConfig != nil" or "LoggerCore != nil && LoggerWriteSyncer != nil".
	LoggerCore        zapcore.Core
	LoggerWriteSyncer zapcore.WriteSyncer

	stateMachineBuilder *stateMachineBuilder
	StoreType           StateMachineType
	QuotaBackendBytes   int64
	// BackendBatchInterval is the maximum time before commit the backend transaction.
	BackendBatchInterval time.Duration
	// BackendBatchLimit is the maximum operations before commit the backend transaction.
	BackendBatchLimit int
	// BackendFreelistType is the type of the backend boltdb freelist.
	BackendFreelistType bolt.FreelistType
}

type stateMachineBuilder struct {
	cfg *RaftRelayConfig
}

func (s *stateMachineBuilder) build() raftnode.StateMachine {
	switch s.cfg.StoreType {
	case RocksDBBackend:
		// TODO
	case BoltDBBackend:
		return statemachine.NewBoltDBStateMachine(s.cfg.BuildBackendConfig().(*statemachine.BoltDBBackendConfig))
	default:
		return statemachine.NewBoltDBStateMachine(s.cfg.BuildBackendConfig().(*statemachine.BoltDBBackendConfig))
	}
	return nil
}

func (c *RaftRelayConfig) BuildRaftNodeStartConfig() raftnode.RaftNodeStartConfig {
	return raftnode.RaftNodeStartConfig{
		Name:              c.Name,
		WAL:               c.WALDir(),
		ElectionTicks:     c.ElectionTicks,
		PreVote:           c.PreVote,
		Logger:            c.Logger,
		LoggerConfig:      c.LoggerConfig,
		LoggerCore:        c.LoggerCore,
		LoggerWriteSyncer: c.LoggerWriteSyncer,
	}
}

func (c *RaftRelayConfig) BuildBackendConfig() raftnode.StateMachineBackendConfig {
	switch c.StoreType {
	case RocksDBBackend:
		// TODO
	case BoltDBBackend:
		return &statemachine.BoltDBBackendConfig{
			Logger:               c.Logger,
			StorePath:            c.BackendPath(),
			SnapPath:             c.SnapDir(),
			QuotaBackendBytes:    c.QuotaBackendBytes,
			BackendBatchInterval: c.BackendBatchInterval,
			BackendBatchLimit:    c.BackendBatchLimit,
			BackendFreelistType:  c.BackendFreelistType,
		}
	default:
		return &statemachine.BoltDBBackendConfig{
			Logger:               c.Logger,
			StorePath:            c.BackendPath(),
			SnapPath:             c.SnapDir(),
			QuotaBackendBytes:    c.QuotaBackendBytes,
			BackendBatchInterval: c.BackendBatchInterval,
			BackendBatchLimit:    c.BackendBatchLimit,
			BackendFreelistType:  c.BackendFreelistType,
		}
	}
	return nil
}

func (c *RaftRelayConfig) BuildStateMachine() raftnode.StateMachine {
	return c.stateMachineBuilder.build()
}

func (c *RaftRelayConfig) Initialize() error {
	c.stateMachineBuilder = &stateMachineBuilder{c}
	return nil
}

// VerifyBootstrap sanity-checks the initial config for bootstrap case
// and returns an error for things that should never happen.
func (c *RaftRelayConfig) VerifyBootstrap() error {
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if err := c.advertiseMatchesCluster(); err != nil {
		return err
	}
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}
	if c.InitialPeerURLsMap.String() == "" {
		return fmt.Errorf("initial cluster unset")
	}
	return nil
}

// VerifyJoinExisting sanity-checks the initial config for join existing cluster
// case and returns an error for things that should never happen.
func (c *RaftRelayConfig) VerifyJoinExisting() error {
	// The member has announced its peer urls to the cluster before starting; no need to
	// set the configuration again.
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}
	return nil
}

// hasLocalMember checks that the cluster at least contains the local server.
func (c *RaftRelayConfig) hasLocalMember() error {
	if urls := c.InitialPeerURLsMap[c.Name]; urls == nil {
		return fmt.Errorf("couldn't find local name %q in the initial cluster configuration", c.Name)
	}
	return nil
}

// advertiseMatchesCluster confirms peer URLs match those in the cluster peer list.
func (c *RaftRelayConfig) advertiseMatchesCluster() error {
	urls, apurls := c.InitialPeerURLsMap[c.Name], c.PeerURLs.StringSlice()
	urls.Sort()
	sort.Strings(apurls)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	ok, err := netutil.URLStringsEqual(ctx, c.Logger, apurls, urls.StringSlice())
	if ok {
		return nil
	}

	initMap, apMap := make(map[string]struct{}), make(map[string]struct{})
	for _, url := range c.PeerURLs {
		apMap[url.String()] = struct{}{}
	}
	for _, url := range c.InitialPeerURLsMap[c.Name] {
		initMap[url.String()] = struct{}{}
	}

	missing := []string{}
	for url := range initMap {
		if _, ok := apMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		for i := range missing {
			missing[i] = c.Name + "=" + missing[i]
		}
		mstr := strings.Join(missing, ",")
		apStr := strings.Join(apurls, ",")
		return fmt.Errorf("--initial-cluster has %s but missing from --initial-advertise-peer-urls=%s (%v)", mstr, apStr, err)
	}

	for url := range apMap {
		if _, ok := initMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		mstr := strings.Join(missing, ",")
		umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
		return fmt.Errorf("--initial-advertise-peer-urls has %s but missing from --initial-cluster=%s", mstr, umap.String())
	}

	// resolved URLs from "--initial-advertise-peer-urls" and "--initial-cluster" did not match or failed
	apStr := strings.Join(apurls, ",")
	umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
	return fmt.Errorf("failed to resolve %s to match --initial-cluster=%s (%v)", apStr, umap.String(), err)
}

// wal path: /data/member/wal/
// snap path: /data/member/snap/ store snapshot meta and snapshot data
// backend path: /data/member/snap/db/ store snapshot data
func (c *RaftRelayConfig) MemberDir() string { return filepath.Join(c.DataDir, "member") }

// WAL Dir used for store Log Entries && Raft State
func (c *RaftRelayConfig) WALDir() string {
	if c.DedicatedWALDir != "" {
		return c.DedicatedWALDir
	}
	return filepath.Join(c.MemberDir(), "wal")
}

func (c *RaftRelayConfig) SnapDir() string { return filepath.Join(c.MemberDir(), "snap") }

func (c *RaftRelayConfig) BackendPath() string { return filepath.Join(c.SnapDir(), "db") }

// ReqTimeout returns timeout for request to finish.
func (c *RaftRelayConfig) ReqTimeout() time.Duration {
	// 5s for queue waiting, computation and disk IO delay
	// + 2 * election timeout for possible leader election
	return 5*time.Second + 2*time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

func (c *RaftRelayConfig) electionTimeout() time.Duration {
	return time.Duration(c.ElectionTicks*int(c.TickMs)) * time.Millisecond
}

func (c *RaftRelayConfig) peerDialTimeout() time.Duration {
	// 1s for queue wait and election timeout
	return time.Second + time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

func checkDuplicateURL(urlsmap types.URLsMap) bool {
	um := make(map[string]bool)
	for _, urls := range urlsmap {
		for _, url := range urls {
			u := url.String()
			if um[u] {
				return true
			}
			um[u] = true
		}
	}
	return false
}

func (c *RaftRelayConfig) bootstrapTimeout() time.Duration {
	if c.BootstrapTimeout != 0 {
		return c.BootstrapTimeout
	}
	return time.Second
}
