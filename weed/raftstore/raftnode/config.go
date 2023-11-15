package raftnode

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type RaftNodeStartConfig struct {
	Name string
	WAL  string

	// Consistent Index persist in state machine
	AppliedIndex uint64

	ElectionTicks int
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
}

func (r *RaftNodeStartConfig) WALDir() string {
	return r.WAL
}
