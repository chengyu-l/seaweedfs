package raftnode

import (
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/ioutil"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raft/raftpb"
	"io"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

// Message is a struct that contains a raft Message and a ReadCloser. The type
// of raft message MUST be MsgSnap, which contains the raft meta-data and an
// additional data []byte field that contains the snapshot of the actual state
// machine.
// Message contains the ReadCloser field for handling large snapshot. This avoid
// copying the entire snapshot into a byte array, which consumes a lot of memory.
//
// User of Message should close the Message after sending it.
type SnapMessage struct {
	raftpb.Message
	ReadCloser io.ReadCloser
	TotalSize  int64
	closeC     chan bool
}

func NewSnapMessage(rs raftpb.Message, rc io.ReadCloser, rcSize int64) *SnapMessage {
	return &SnapMessage{
		Message:    rs,
		ReadCloser: ioutil.NewExactReadCloser(rc, rcSize),
		TotalSize:  int64(rs.Size()) + rcSize,
		closeC:     make(chan bool, 1),
	}
}

// CloseNotify returns a channel that receives a single value
// when the message sent is finished. true indicates the sent
// is successful.
func (m SnapMessage) CloseNotify() <-chan bool {
	return m.closeC
}

func (m SnapMessage) CloseWithError(err error) {
	if cerr := m.ReadCloser.Close(); cerr != nil {
		err = cerr
	}
	if err == nil {
		m.closeC <- true
	} else {
		m.closeC <- false
	}
}

type Snapshot interface {
	// Gets the size of snapshot
	Size() int64
	// Write snapshot to io io.Writer
	WriteTo(w io.Writer) (n int64, err error)
	// Close the snapshot
	Close() error
}

type SnapshotMgr interface {
	SaveDBFrom(reader io.Reader, id uint64) (int64, error)
	SaveSnap(raftpb.Snapshot) error
	Load() (*raftpb.Snapshot, error)
	//// CreateSnapshotMessage merge large data
	//CreateSnapshotMessage(m raftpb.Message, stateMachineSnapshot Snapshot, ss raftpb.Snapshot) SnapMessage
}

func NewSnapshotReaderCloser(lg *zap.Logger, snapshot Snapshot) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		n, err := snapshot.WriteTo(pw)
		if err == nil {
			lg.Info(
				"sent database snapshot to writer",
				zap.Int64("bytes", n),
				zap.String("size", humanize.Bytes(uint64(n))),
			)
		} else {
			lg.Warn(
				"failed to send database snapshot to writer",
				zap.String("size", humanize.Bytes(uint64(n))),
				zap.Error(err),
			)
		}
		pw.CloseWithError(err)
		err = snapshot.Close()
		if err != nil {
			lg.Panic("failed to close database snapshot", zap.Error(err))
		}
	}()
	return pr
}
