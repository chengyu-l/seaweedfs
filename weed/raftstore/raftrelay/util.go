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
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

const warnApplyDuration = 100 * time.Millisecond

type HTTPError struct {
	Message string `json:"message"`
	// Code is the HTTP status code
	Code int `json:"-"`
}

func (e HTTPError) Error() string {
	return e.Message
}

func (e HTTPError) WriteTo(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Code)
	b, err := json.Marshal(e)
	if err != nil {
		glog.Fatalf("marshal HTTPError should never fail (%v)", err)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return nil
}

func NewHTTPError(code int, m string) *HTTPError {
	return &HTTPError{
		Message: m,
		Code:    code,
	}
}

func allowMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if method == r.Method {
		return true
	}
	w.Header().Set("Allow", method)
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	return false
}

// WriteError logs and writes the given Error to the ResponseWriter
// If Error is an etcdErr, it is rendered to the ResponseWriter
// Otherwise, it is assumed to be a StatusInternalServerError
func WriteError(lg *zap.Logger, w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	switch err {
	case ErrTimeoutDueToLeaderFail, ErrTimeoutDueToConnectionLost, ErrNotEnoughStartedMembers,
		ErrUnhealthy:
		lg.Warn(
			"v2 response error",
			zap.String("remote-addr", r.RemoteAddr),
			zap.String("internal-server-error", err.Error()),
		)

	default:
		lg.Warn(
			"unexpected v2 response error",
			zap.String("remote-addr", r.RemoteAddr),
			zap.String("internal-server-error", err.Error()),
		)
	}

	herr := NewHTTPError(http.StatusInternalServerError, "Internal Server Error")
	if et := herr.WriteTo(w); et != nil {
		lg.Debug(
			"failed to write HTTP error",
			zap.String("remote-addr", r.RemoteAddr),
			zap.String("internal-server-error", err.Error()),
			zap.Error(et),
		)
	}
}

// isConnectedToQuorumSince checks whether the local member is connected to the
// quorum of the cluster since the given time.
func isConnectedToQuorumSince(transport raftnode.Transporter, since time.Time, self types.ID, members []*membership.Member) bool {
	return numConnectedSince(transport, since, self, members) >= (len(members)/2)+1
}

// isConnectedSince checks whether the local member is connected to the
// remote member since the given time.
func isConnectedSince(transport raftnode.Transporter, since time.Time, remote types.ID) bool {
	t := transport.ActiveSince(remote)
	return !t.IsZero() && t.Before(since)
}

// isConnectedFullySince checks whether the local member is connected to all
// members in the cluster since the given time.
func isConnectedFullySince(transport raftnode.Transporter, since time.Time, self types.ID, members []*membership.Member) bool {
	return numConnectedSince(transport, since, self, members) == len(members)
}

// numConnectedSince counts how many members are connected to the local member
// since the given time.
func numConnectedSince(transport raftnode.Transporter, since time.Time, self types.ID, members []*membership.Member) int {
	connectedNum := 0
	for _, m := range members {
		if m.ID == self || isConnectedSince(transport, since, m.ID) {
			connectedNum++
		}
	}
	return connectedNum
}

// longestConnected chooses the member with longest active-since-time.
// It returns false, if nothing is active.
func longestConnected(tp raftnode.Transporter, membs []types.ID) (types.ID, bool) {
	var longest types.ID
	var oldest time.Time
	for _, id := range membs {
		tm := tp.ActiveSince(id)
		if tm.IsZero() { // inactive
			continue
		}

		if oldest.IsZero() { // first longest candidate
			oldest = tm
			longest = id
		}

		if tm.Before(oldest) {
			oldest = tm
			longest = id
		}
	}
	if uint64(longest) == 0 {
		return longest, false
	}
	return longest, true
}

type notifier struct {
	c   chan struct{}
	err error
}

func newNotifier() *notifier {
	return &notifier{
		c: make(chan struct{}),
	}
}

func (nc *notifier) notify(err error) {
	nc.err = err
	close(nc.c)
}

func warnOfExpensiveRequest(lg *zap.Logger, now time.Time, reqStringer fmt.Stringer, respMsg proto.Message, err error) {
	var resp string
	if !isNil(respMsg) {
		resp = fmt.Sprintf("size:%d", proto.Size(respMsg))
	}
	warnOfExpensiveGenericRequest(lg, now, reqStringer, "", resp, err)
}

func warnOfFailedRequest(lg *zap.Logger, now time.Time, reqStringer fmt.Stringer, respMsg proto.Message, err error) {
	var resp string
	if !isNil(respMsg) {
		resp = fmt.Sprintf("size:%d", proto.Size(respMsg))
	}
	d := time.Since(now)
	lg.Warn(
		"failed to apply request",
		zap.Duration("took", d),
		zap.String("request", reqStringer.String()),
		zap.String("response", resp),
		zap.Error(err),
	)
}

func warnOfExpensiveReadOnlyTxnRequest(lg *zap.Logger, now time.Time, r *pb.TxnRequest, txnResponse *pb.TxnResponse, err error) {
	reqStringer := pb.NewLoggableTxnRequest(r)
	var resp string
	if !isNil(txnResponse) {
		var resps []string
		for _, r := range txnResponse.Responses {
			switch op := r.Response.(type) {
			case *pb.ResponseOp_ResponseRange:
				resps = append(resps, fmt.Sprintf("range_response_count:%d", len(op.ResponseRange.Kvs)))
			default:
				// only range responses should be in a read only txn request
			}
		}
		resp = fmt.Sprintf("responses:<%s> size:%d", strings.Join(resps, " "), proto.Size(txnResponse))
	}
	warnOfExpensiveGenericRequest(lg, now, reqStringer, "read-only range ", resp, err)
}

func warnOfExpensiveReadOnlyRangeRequest(lg *zap.Logger, now time.Time, reqStringer fmt.Stringer, rangeResponse *pb.RangeResponse, err error) {
	var resp string
	if !isNil(rangeResponse) {
		resp = fmt.Sprintf("range_response_count:%d size:%d", len(rangeResponse.Kvs), proto.Size(rangeResponse))
	}
	warnOfExpensiveGenericRequest(lg, now, reqStringer, "read-only range ", resp, err)
}

func warnOfExpensiveGenericRequest(lg *zap.Logger, now time.Time, reqStringer fmt.Stringer, prefix string, resp string, err error) {
	d := time.Since(now)
	if d > warnApplyDuration {
		lg.Warn(
			"apply request took too long",
			zap.Duration("took", d),
			zap.Duration("expected-duration", warnApplyDuration),
			zap.String("prefix", prefix),
			zap.String("request", reqStringer.String()),
			zap.String("response", resp),
			zap.Error(err),
		)
		slowApplies.Inc()
	}
}

func isNil(msg proto.Message) bool {
	return msg == nil || reflect.ValueOf(msg).IsNil()
}
