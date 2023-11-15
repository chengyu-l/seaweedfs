package kv

import (
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	backend "github.com/seaweedfs/seaweedfs/weed/raftstore/statemachine/kv/backend"
)

type RangeOptions struct {
	Limit int64
	Count bool
}

type RangeResult struct {
	KVs   []pb.KeyValue
	Count int
}

type ReadView interface {
	// Range gets the keys in the range at rangeRev.
	// The returned rev is the current revision of the KV when the operation is executed.
	// If rangeRev <=0, range gets the keys at currentRev.
	// If `end` is nil, the request returns the key.
	// If `end` is not nil and not empty, it gets the keys in range [key, range_end).
	// If `end` is not nil and empty, it gets the keys greater than or equal to key.
	// Limit limits the number of keys returned.
	// If the required rev is compacted, ErrCompacted will be returned.
	Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error)
}

// TxnRead represents a read-only transaction with operations that will not
// block other read transactions.
type TxnRead interface {
	ReadView
	// End marks the transaction is complete and ready to commit.
	End()
}

type WriteView interface {
	// Delete deletes the given key from the store.
	DeleteRange(key, end []byte) (n int64)

	// Put puts the given key, value into the store.
	Put(key, value []byte)
}

// TxnWrite represents a transaction that can modify the store.
type TxnWrite interface {
	TxnRead
	WriteView
	// Changes gets the changes made since opening the write txn.
	Changes() []pb.KeyValue
}

type KV interface {
	ReadView
	WriteView

	// Read creates a read transaction.
	Read() TxnRead

	// Write creates a write transaction.
	Write() TxnWrite

	// Commit commits outstanding txns into the underlying backend.
	Commit()

	// Restore restores the KV store from a backend.
	Restore(b backend.Backend) error
	Close() error

	// ConsistentIndex
	ConsistentIndex() uint64
}

// txnReadWrite coerces a read txn to a write, panicking on any write operation.
type txnReadWrite struct{ TxnRead }

func (trw *txnReadWrite) DeleteRange(key, end []byte) int64 { panic("unexpected DeleteRange") }
func (trw *txnReadWrite) Put(key, value []byte)             { panic("unexpected Put") }
func (trw *txnReadWrite) Changes() []pb.KeyValue            { return nil }

func NewReadOnlyTxnWrite(txnRead TxnRead) TxnWrite { return &txnReadWrite{txnRead} }
