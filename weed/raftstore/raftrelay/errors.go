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
	"errors"
	"fmt"
)

var (
	ErrUnknownMethod              = errors.New("raftrealy: unknown method")
	ErrStopped                    = errors.New("raftrealy: server stopped")
	ErrCanceled                   = errors.New("raftrealy: request cancelled")
	ErrTimeout                    = errors.New("raftrealy: request timed out")
	ErrTimeoutDueToLeaderFail     = errors.New("raftrealy: request timed out, possibly due to previous leader failure")
	ErrTimeoutDueToConnectionLost = errors.New("raftrealy: request timed out, possibly due to connection lost")
	ErrTimeoutLeaderTransfer      = errors.New("raftrealy: request timed out, leader transfer took too long")
	ErrLeaderChanged              = errors.New("raftrealy: leader changed")
	ErrNotEnoughStartedMembers    = errors.New("raftrealy: re-configuration failed due to not enough started members")
	ErrLearnerNotReady            = errors.New("raftrealy: can only promote a learner member which is in sync with leader")
	ErrNoLeader                   = errors.New("raftrealy: no leader")
	ErrNotLeader                  = errors.New("raftrealy: not leader")
	ErrRequestTooLarge            = errors.New("raftrealy: request is too large")
	ErrNoSpace                    = errors.New("raftrealy: no space")
	ErrTooManyRequests            = errors.New("raftrealy: too many requests")
	ErrUnhealthy                  = errors.New("raftrealy: unhealthy cluster")
	ErrKeyNotFound                = errors.New("raftrealy: key not found")
	ErrCorrupt                    = errors.New("raftrealy: corrupt cluster")
	ErrBadLeaderTransferee        = errors.New("raftrealy: bad leader transferee")
)

type DiscoveryError struct {
	Op  string
	Err error
}

func (e DiscoveryError) Error() string {
	return fmt.Sprintf("failed to %s discovery cluster (%v)", e.Op, e.Err)
}
