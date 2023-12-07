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

package rafthttp

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/httputil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/transport"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	streamTypeMessage streamType = "message"
	streamTypeMsgApp  streamType = "msgapp"

	streamBufSize = 4096
)

type streamType string

func (t streamType) endpoint() string {
	switch t {
	case streamTypeMsgApp:
		return path.Join(RaftStreamPrefix, "msgapp")
	case streamTypeMessage:
		return path.Join(RaftStreamPrefix, "message")
	default:
		glog.Fatalf("unhandled stream type %v", t)
		return ""
	}
}

func (t streamType) String() string {
	switch t {
	case streamTypeMsgApp:
		return "stream MsgApp"
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

var (
	// linkHeartbeatMessage is a special message used as heartbeat message in
	// link layer. It never conflicts with messages from raft because raft
	// doesn't send out messages without From and To fields.
	linkHeartbeatMessage = raftpb.Message{Type: raftpb.MsgHeartbeat}
)

func isLinkHeartbeatMessage(m *raftpb.Message) bool {
	return m.Type == raftpb.MsgHeartbeat && m.From == 0 && m.To == 0
}

type outgoingConn struct {
	t streamType
	io.Writer
	http.Flusher
	io.Closer

	localID types.ID
	peerID  types.ID
}

// streamWriter writes messages to the attached outgoingConn.
type streamWriter struct {
	lg *zap.Logger

	localID types.ID
	peerID  types.ID

	status *peerStatus
	r      Raft

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer
	working bool

	msgc  chan raftpb.Message
	connc chan *outgoingConn
	stopc chan struct{}
	done  chan struct{}
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
func startStreamWriter(lg *zap.Logger, local, id types.ID, status *peerStatus, r Raft) *streamWriter {
	w := &streamWriter{
		lg: lg,

		localID: local,
		peerID:  id,

		status: status,
		r:      r,
		msgc:   make(chan raftpb.Message, streamBufSize),
		connc:  make(chan *outgoingConn),
		stopc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var (
		msgc       chan raftpb.Message
		heartbeatc <-chan time.Time
		t          streamType
		enc        encoder
		flusher    http.Flusher
		batched    int
	)
	tickc := time.NewTicker(ConnReadTimeout / 3)
	defer tickc.Stop()
	unflushed := 0

	cw.lg.Info(
		"started stream writer with remote peer",
		zap.String("local-member-id", cw.localID.String()),
		zap.String("remote-peer-id", cw.peerID.String()),
	)

	for {
		select {
		case <-heartbeatc:
			err := enc.encode(&linkHeartbeatMessage)
			unflushed += linkHeartbeatMessage.Size()
			if err == nil {
				flusher.Flush()
				batched = 0
				sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
				unflushed = 0
				continue
			}

			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			sentFailures.WithLabelValues(cw.peerID.String()).Inc()
			cw.close()
			cw.lg.Warn(
				"lost TCP streaming connection with remote peer",
				zap.String("stream-writer-type", t.String()),
				zap.String("local-member-id", cw.localID.String()),
				zap.String("remote-peer-id", cw.peerID.String()),
			)
			heartbeatc, msgc = nil, nil

		case m := <-msgc:
			err := enc.encode(&m)
			if err == nil {
				unflushed += m.Size()

				if len(msgc) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
					unflushed = 0
					batched = 0
				} else {
					batched++
				}

				continue
			}

			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error())
			cw.close()
			cw.lg.Warn(
				"lost TCP streaming connection with remote peer",
				zap.String("stream-writer-type", t.String()),
				zap.String("local-member-id", cw.localID.String()),
				zap.String("remote-peer-id", cw.peerID.String()),
			)
			heartbeatc, msgc = nil, nil
			cw.r.ReportUnreachable(m.To)
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()

		case conn := <-cw.connc:
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			t = conn.t
			switch conn.t {
			case streamTypeMsgApp:
				enc = newMsgAppEncoder(conn.Writer)
			case streamTypeMessage:
				enc = &messageEncoder{w: conn.Writer}
			default:
				glog.Fatalf("unhandled stream type %s", conn.t)
			}
			cw.lg.Info(
				"set message encoder",
				zap.String("from", conn.localID.String()),
				zap.String("to", conn.peerID.String()),
				zap.String("stream-type", t.String()),
			)
			flusher = conn.Flusher
			unflushed = 0
			cw.status.activate()
			cw.closer = conn.Closer
			cw.working = true
			cw.mu.Unlock()

			if closed {
				cw.lg.Warn(
					"closed TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			cw.lg.Warn(
				"established TCP streaming connection with remote peer",
				zap.String("stream-writer-type", t.String()),
				zap.String("local-member-id", cw.localID.String()),
				zap.String("remote-peer-id", cw.peerID.String()),
			)
			heartbeatc, msgc = tickc.C, cw.msgc

		case <-cw.stopc:
			if cw.close() {
				cw.lg.Warn(
					"closed TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			glog.Infof("stopped streaming with peer %s (writer)", cw.peerID)
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writec() (chan<- raftpb.Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		if cw.lg != nil {
			cw.lg.Warn(
				"failed to close connection with remote peer",
				zap.String("remote-peer-id", cw.peerID.String()),
				zap.Error(err),
			)
		} else {
			glog.Errorf("peer %s (writer) connection close error: %v", cw.peerID, err)
		}
	}
	if len(cw.msgc) > 0 {
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan raftpb.Message, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn:
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
type streamReader struct {
	lg *zap.Logger

	peerID types.ID
	typ    streamType

	tr     *Transport
	picker *urlPicker
	status *peerStatus
	recvc  chan<- raftpb.Message
	propc  chan<- raftpb.Message

	rl *rate.Limiter // alters the frequency of dial retrial attempts

	errorc chan<- error

	mu     sync.Mutex
	paused bool
	closer io.Closer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	if cr.errorc == nil {
		cr.errorc = cr.tr.ErrorC
	}
	if cr.ctx == nil {
		cr.ctx, cr.cancel = context.WithCancel(context.Background())
	}
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.typ

	if cr.lg != nil {
		cr.lg.Info(
			"started stream reader with remote peer",
			zap.String("stream-reader-type", t.String()),
			zap.String("local-member-id", cr.tr.ID.String()),
			zap.String("remote-peer-id", cr.peerID.String()),
		)
	} else {
		glog.Infof("started streaming with peer %s (%s reader)", cr.peerID, t)
	}

	for {
		rc, err := cr.dial(t)
		if err != nil {
			cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
		} else {
			cr.status.activate()
			if cr.lg != nil {
				cr.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			} else {
				glog.Infof("established a TCP streaming connection with peer %s (%s reader)", cr.peerID, cr.typ)
			}
			err = cr.decodeLoop(rc, t)
			if cr.lg != nil {
				cr.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			} else {
				glog.Warningf("lost the TCP streaming connection with peer %s (%s reader)", cr.peerID, cr.typ)
			}
			switch {
			// all data is read out
			case err == io.EOF:
			// connection is closed by the remote
			case transport.IsClosedConnError(err):
			default:
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
			}
		}
		// Wait for a while before new dial attempt
		err = cr.rl.Wait(cr.ctx)
		if cr.ctx.Err() != nil {
			if cr.lg != nil {
				cr.lg.Info(
					"stopped stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			} else {
				glog.Infof("stopped streaming with peer %s (%s reader)", cr.peerID, t)
			}
			close(cr.done)
			return
		}
		if err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"rate limit on stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			} else {
				glog.Errorf("streaming with peer %s (%s reader) rate limiter error: %v", cr.peerID, t, err)
			}
		}
	}
}

func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	var dec decoder
	cr.mu.Lock()
	switch t {
	case streamTypeMsgApp:
		dec = newMsgAppDecoder(rc, cr.tr.ID, cr.peerID)
	case streamTypeMessage:
		dec = &messageDecoder{r: rc}
	default:
		cr.lg.Panic("unknown stream type", zap.String("type", t.String()))
	}
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()

	// gofail: labelRaftDropHeartbeat:
	for {
		m, err := dec.decode()
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		// gofail-go: var raftDropHeartbeat struct{}
		// continue labelRaftDropHeartbeat
		receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(m.Size()))

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused {
			continue
		}

		if isLinkHeartbeatMessage(&m) {
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		recvc := cr.recvc
		if m.Type == raftpb.MsgProp {
			recvc = cr.propc
		}

		select {
		case recvc <- m:
		default:
			if cr.status.isActive() {
				cr.lg.Warn(
					"dropped internal Raft message since receiving buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", types.ID(m.To).String()),
					zap.Bool("remote-peer-active", cr.status.isActive()),
				)
			} else {
				cr.lg.Warn(
					"dropped Raft message since receiving buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", types.ID(m.To).String()),
					zap.Bool("remote-peer-active", cr.status.isActive()),
				)
			}
			recvFailures.WithLabelValues(types.ID(m.From).String()).Inc()
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) {
	u := cr.picker.pick()
	uu := u
	uu.Path = path.Join(t.endpoint(), cr.tr.ID.String())

	cr.lg.Debug(
		"dial stream reader",
		zap.String("from", cr.tr.ID.String()),
		zap.String("to", cr.peerID.String()),
		zap.String("address", uu.String()),
	)
	req, err := http.NewRequest("GET", uu.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}
	req.Header.Set("X-Server-From", cr.tr.ID.String())
	req.Header.Set("X-Nebulasfs-Cluster-ID", cr.tr.ClusterID.String())
	req.Header.Set("X-Raft-To", cr.peerID.String())

	setPeerURLsHeader(req, cr.tr.URLs)

	req = req.WithContext(cr.ctx)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	resp, err := cr.tr.streamRt.RoundTrip(req)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusGone:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved

	case http.StatusOK:
		return resp.Body, nil

	case http.StatusNotFound:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.ID)

	case http.StatusPreconditionFailed:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errClusterIDMismatch.Error():
			cr.lg.Warn(
				"request sent was ignored by remote peer due to cluster ID mismatch",
				zap.String("remote-peer-id", cr.peerID.String()),
				zap.String("remote-peer-cluster-id", resp.Header.Get("X-Nebulasfs-Cluster-ID")),
				zap.String("local-member-id", cr.tr.ID.String()),
				zap.String("local-member-cluster-id", cr.tr.ClusterID.String()),
				zap.Error(errClusterIDMismatch),
			)
			return nil, errClusterIDMismatch

		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}

	default:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			cr.lg.Warn(
				"failed to close remote peer connection",
				zap.String("local-member-id", cr.tr.ID.String()),
				zap.String("remote-peer-id", cr.peerID.String()),
				zap.Error(err),
			)
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}