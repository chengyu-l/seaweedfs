package raftrelay

import (
	"fmt"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var DefaultLogLevel = "info"

// ConvertToZapLevel converts log level string to zapcore.Level.
func ConvertToZapLevel(lvl string) zapcore.Level {
	switch lvl {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "dpanic":
		return zap.DPanicLevel
	case "panic":
		return zap.PanicLevel
	case "fatal":
		return zap.FatalLevel
	default:
		panic(fmt.Sprintf("unknown level %q", lvl))
	}
}

// DefaultZapLoggerConfig defines default zap logger configuration.
var DefaultZapLoggerConfig = zap.Config{
	Level: zap.NewAtomicLevelAt(ConvertToZapLevel(DefaultLogLevel)),

	Development: false,
	Sampling: &zap.SamplingConfig{
		Initial:    100,
		Thereafter: 100,
	},

	Encoding: "json",

	// copied from "zap.NewProductionEncoderConfig" with some updates
	EncoderConfig: zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	},

	// Use "/dev/null" to discard all
	OutputPaths:      []string{"stderr"},
	ErrorOutputPaths: []string{"stderr"},
}

// MergeOutputPaths merges logging output paths, resolving conflicts.
func MergeOutputPaths(cfg zap.Config) zap.Config {
	outputs := make(map[string]struct{})
	for _, v := range cfg.OutputPaths {
		outputs[v] = struct{}{}
	}
	outputSlice := make([]string, 0)
	if _, ok := outputs["/dev/null"]; ok {
		// "/dev/null" to discard all
		outputSlice = []string{"/dev/null"}
	} else {
		for k := range outputs {
			outputSlice = append(outputSlice, k)
		}
	}
	cfg.OutputPaths = outputSlice
	sort.Strings(cfg.OutputPaths)

	errOutputs := make(map[string]struct{})
	for _, v := range cfg.ErrorOutputPaths {
		errOutputs[v] = struct{}{}
	}
	errOutputSlice := make([]string, 0)
	if _, ok := errOutputs["/dev/null"]; ok {
		// "/dev/null" to discard all
		errOutputSlice = []string{"/dev/null"}
	} else {
		for k := range errOutputs {
			errOutputSlice = append(errOutputSlice, k)
		}
	}
	cfg.ErrorOutputPaths = errOutputSlice
	sort.Strings(cfg.ErrorOutputPaths)

	return cfg
}

const (
	ClusterStateFlagNew      = "new"
	ClusterStateFlagExisting = "existing"

	DefaultName            = "default"
	DefaultMaxSnapshots    = 5
	DefaultMaxWALs         = 5
	DefaultMaxTxnOps       = uint(128)
	DefaultMaxRequestBytes = 1.5 * 1024 * 1024

	DefaultListenPeerURLs   = "http://localhost:2380"
	DefaultListenClientURLs = "http://localhost:2379"

	DefaultLogOutput = "default"
	StdErrLogOutput  = "stderr"
	StdOutLogOutput  = "stdout"

	// maxElectionMs specifies the maximum value of election timeout.
	// More details are listed in ../Documentation/tuning.md#time-parameters.
	maxElectionMs = 50000
	// backend freelist map type
	freelistMapType = "map"
)

func NewDefaultConfig() *LaunchConfig {
	lpurl, _ := url.Parse(DefaultListenPeerURLs)
	lcurl, _ := url.Parse(DefaultListenClientURLs)
	cfg := &LaunchConfig{
		MaxSnapFiles: DefaultMaxSnapshots,
		MaxWalFiles:  DefaultMaxWALs,

		Name: DefaultName,

		SnapshotCount:          DefaultSnapshotCount,
		SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries,

		MaxTxnOps:       DefaultMaxTxnOps,
		MaxRequestBytes: DefaultMaxRequestBytes,

		TickMs:                     100,
		ElectionMs:                 1000,
		InitialElectionTickAdvance: true,

		LPUrls: []url.URL{*lpurl},
		LCUrls: []url.URL{*lcurl},

		ClusterState:        ClusterStateFlagNew,
		InitialClusterToken: "nebulasfs-cluster",

		PreVote: true,

		loggerMu:   new(sync.RWMutex),
		logger:     nil,
		LogOutputs: []string{DefaultLogOutput},
		LogLevel:   DefaultLogLevel,
	}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	return cfg
}

// LaunchConfig holds the arguments for configuring a raft-relay.
type LaunchConfig struct {
	Name   string `json:"name"`
	Dir    string `json:"data-dir"`
	WalDir string `json:"wal-dir"`

	SnapshotCount uint64 `json:"snapshot-count"`

	// SnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	// WARNING: only change this for tests.
	// Always use "DefaultSnapshotCatchUpEntries"
	SnapshotCatchUpEntries uint64

	MaxSnapFiles uint `json:"max-snapshots"`
	MaxWalFiles  uint `json:"max-wals"`

	// TickMs is the number of milliseconds between heartbeat ticks.
	// TODO: decouple tickMs and heartbeat tick (current heartbeat tick = 1).
	// make ticks a cluster wide configuration.
	TickMs     uint `json:"heartbeat-interval"`
	ElectionMs uint `json:"election-timeout"`

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
	InitialElectionTickAdvance bool `json:"initial-election-tick-advance"`

	// BackendBatchInterval is the maximum time before commit the backend transaction.
	BackendBatchInterval time.Duration `json:"backend-batch-interval"`
	// BackendBatchLimit is the maximum operations before commit the backend transaction.
	BackendBatchLimit int   `json:"backend-batch-limit"`
	QuotaBackendBytes int64 `json:"quota-backend-bytes"`
	MaxTxnOps         uint  `json:"max-txn-ops"`
	MaxRequestBytes   uint  `json:"max-request-bytes"`

	// LPUrls are the urls for listen peer
	LPUrls []url.URL
	// LPUrls are the urls for listen client
	LCUrls []url.URL

	ClusterState        string `json:"initial-cluster-state"`
	InitialCluster      string `json:"initial-cluster"`
	InitialClusterToken string `json:"initial-cluster-token"`

	// PreVote is true to enable Raft Pre-Vote.
	// If enabled, Raft runs an additional election phase
	// to check whether it would get enough votes to win
	// an election, thus minimizing disruptions.
	PreVote bool `json:"pre-vote"`
	// ForceNewCluster starts a new cluster even if previously started; unsafe.
	ForceNewCluster bool `json:"force-new-cluster"`

	// LogLevel configures log level. Only supports debug, info, warn, error, panic, or fatal. Default 'info'.
	LogLevel string `json:"log-level"`
	// LogOutputs is either:
	//  - "default" as os.Stderr,
	//  - "stderr" as os.Stderr,
	//  - "stdout" as os.Stdout,
	//  - file path to append server logs to.
	// It can be multiple when "Logger" is zap.
	LogOutputs []string `json:"log-outputs"`

	// zapLoggerBuilder is used to build the zap logger.
	zapLoggerBuilder func(*LaunchConfig) error

	// logger logs server-side operations. The default is nil,
	// and "setupLogging" must be called before starting server.
	// Do not set logger directly.
	loggerMu *sync.RWMutex
	logger   *zap.Logger

	// loggerConfig is server logger configuration for Raft logger.
	// Must be either: "loggerConfig != nil" or "loggerCore != nil && loggerWriteSyncer != nil".
	loggerConfig *zap.Config
	// loggerCore is "zapcore.Core" for raft logger.
	// Must be either: "loggerConfig != nil" or "loggerCore != nil && loggerWriteSyncer != nil".
	loggerCore        zapcore.Core
	loggerWriteSyncer zapcore.WriteSyncer
}

// GetLogger returns the logger.
func (cfg *LaunchConfig) GetLogger() *zap.Logger {
	cfg.loggerMu.RLock()
	l := cfg.logger
	cfg.loggerMu.RUnlock()
	return l
}

func (cfg *LaunchConfig) getLCURLs() (ss []string) {
	ss = make([]string, len(cfg.LCUrls))
	for i := range cfg.LCUrls {
		ss[i] = cfg.LCUrls[i].String()
	}
	return ss
}

func (cfg *LaunchConfig) getLPURLs() (ss []string) {
	ss = make([]string, len(cfg.LPUrls))
	for i := range cfg.LPUrls {
		ss[i] = cfg.LPUrls[i].String()
	}
	return ss
}

func (cfg *LaunchConfig) ReqTimeout() time.Duration {
	// 5s for queue waiting, computation and disk IO delay
	// + 2 * election timeout for possible leader election
	return 5*time.Second + 2*time.Duration(cfg.ElectionMs)*time.Millisecond
}

func (cfg *LaunchConfig) InitialClusterFromName(name string) (ret string) {
	if len(cfg.LPUrls) == 0 {
		return ""
	}
	n := name
	if name == "" {
		n = DefaultName
	}
	for i := range cfg.LPUrls {
		ret = ret + "," + n + "=" + cfg.LPUrls[i].String()
	}
	return ret[1:]
}

func (cfg *LaunchConfig) IsNewCluster() bool {
	return cfg.ClusterState == ClusterStateFlagNew
}

// setupLogging initializes etcd logging.
// Must be called after flag parsing or finishing configuring embed.Config.
func (cfg *LaunchConfig) setupLogging() error {
	if len(cfg.LogOutputs) == 0 {
		cfg.LogOutputs = []string{DefaultLogOutput}
	}
	if len(cfg.LogOutputs) > 1 {
		for _, v := range cfg.LogOutputs {
			if v == DefaultLogOutput {
				return fmt.Errorf("multi logoutput for %q is not supported yet", DefaultLogOutput)
			}
		}
	}
	outputPaths, errOutputPaths := make([]string, 0), make([]string, 0)
	for _, v := range cfg.LogOutputs {
		switch v {
		case DefaultLogOutput:
			outputPaths = append(outputPaths, StdErrLogOutput)
			errOutputPaths = append(errOutputPaths, StdErrLogOutput)

		case StdErrLogOutput:
			outputPaths = append(outputPaths, StdErrLogOutput)
			errOutputPaths = append(errOutputPaths, StdErrLogOutput)

		case StdOutLogOutput:
			outputPaths = append(outputPaths, StdOutLogOutput)
			errOutputPaths = append(errOutputPaths, StdOutLogOutput)

		default:
			outputPaths = append(outputPaths, v)
			errOutputPaths = append(errOutputPaths, v)
		}
	}
	copied := DefaultZapLoggerConfig
	copied.OutputPaths = outputPaths
	copied.ErrorOutputPaths = errOutputPaths
	copied = MergeOutputPaths(copied)
	copied.Level = zap.NewAtomicLevelAt(ConvertToZapLevel(cfg.LogLevel))
	if cfg.zapLoggerBuilder == nil {
		cfg.zapLoggerBuilder = func(c *LaunchConfig) error {
			var err error
			c.logger, err = copied.Build()
			if err != nil {
				return err
			}
			c.loggerMu.Lock()
			defer c.loggerMu.Unlock()
			c.loggerConfig = &copied
			c.loggerCore = nil
			c.loggerWriteSyncer = nil
			return nil
		}
	}
	err := cfg.zapLoggerBuilder(cfg)
	if err != nil {
		return err
	}
	return nil
}

func (cfg *LaunchConfig) PeerURLsMap(which string) (urlsmap types.URLsMap, err error) {
	return types.NewURLsMap(cfg.InitialCluster)
}

func (cfg *LaunchConfig) Validate() error {
	if err := cfg.setupLogging(); err != nil {
		return err
	}
	if err := checkBindURLs(cfg.LPUrls); err != nil {
		return err
	}
	if cfg.TickMs <= 0 {
		return fmt.Errorf("--heartbeat-interval must be >0 (set to %dms)", cfg.TickMs)
	}
	if cfg.ElectionMs <= 0 {
		return fmt.Errorf("--election-timeout must be >0 (set to %dms)", cfg.ElectionMs)
	}
	if 5*cfg.TickMs > cfg.ElectionMs {
		return fmt.Errorf("--election-timeout[%vms] should be at least as 5 times as --heartbeat-interval[%vms]", cfg.ElectionMs, cfg.TickMs)
	}
	if cfg.ElectionMs > maxElectionMs {
		return fmt.Errorf("--election-timeout[%vms] is too long, and should be set less than %vms", cfg.ElectionMs, maxElectionMs)
	}
	return nil
}

func (cfg *LaunchConfig) ElectionTicks() int { return int(cfg.ElectionMs / cfg.TickMs) }
