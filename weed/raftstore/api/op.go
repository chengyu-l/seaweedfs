// Copyright 2016 The etcd Authors
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

package api

import (
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
)

type opType int

const (
	// A default Op has opType 0, which is invalid.
	tRange opType = iota + 1
	tPut
	tDeleteRange
	tTxn
)

var noPrefixEnd = []byte{0}

// Op represents an Operation that kv can execute.
type Op struct {
	t   opType
	key []byte
	end []byte

	// for range
	limit        int64
	sort         *SortOption
	serializable bool
	keysOnly     bool
	countOnly    bool

	// for put
	val []byte

	// txn
	cmps    []Cmp
	thenOps []Op
	elseOps []Op
}

// accessors / mutators

// IsTxn returns true if the "Op" type is transaction.
func (op Op) IsTxn() bool {
	return op.t == tTxn
}

// Txn returns the comparison(if) operations, "then" operations, and "else" operations.
func (op Op) Txn() ([]Cmp, []Op, []Op) {
	return op.cmps, op.thenOps, op.elseOps
}

// KeyBytes returns the byte slice holding the Op's key.
func (op Op) KeyBytes() []byte { return op.key }

// WithKeyBytes sets the byte slice for the Op's key.
func (op *Op) WithKeyBytes(key []byte) { op.key = key }

// RangeBytes returns the byte slice holding with the Op's range end, if any.
func (op Op) RangeBytes() []byte { return op.end }

// IsPut returns true iff the operation is a Put.
func (op Op) IsPut() bool { return op.t == tPut }

// IsGet returns true iff the operation is a Get.
func (op Op) IsGet() bool { return op.t == tRange }

// IsDelete returns true iff the operation is a Delete.
func (op Op) IsDelete() bool { return op.t == tDeleteRange }

// IsSerializable returns true if the serializable field is true.
func (op Op) IsSerializable() bool { return op.serializable }

// IsKeysOnly returns whether keysOnly is set.
func (op Op) IsKeysOnly() bool { return op.keysOnly }

// IsCountOnly returns whether countOnly is set.
func (op Op) IsCountOnly() bool { return op.countOnly }

// WithRangeBytes sets the byte slice for the Op's range end.
func (op *Op) WithRangeBytes(end []byte) { op.end = end }

// ValueBytes returns the byte slice holding the Op's value, if any.
func (op Op) ValueBytes() []byte { return op.val }

// WithValueBytes sets the byte slice for the Op's value.
func (op *Op) WithValueBytes(v []byte) { op.val = v }

func (op Op) toRangeRequest() *pb.RangeRequest {
	if op.t != tRange {
		panic("op.t != tRange")
	}
	r := &pb.RangeRequest{
		Key:          op.key,
		RangeEnd:     op.end,
		Limit:        op.limit,
		Serializable: op.serializable,
		KeysOnly:     op.keysOnly,
		CountOnly:    op.countOnly,
	}
	if op.sort != nil {
		r.SortOrder = pb.RangeRequest_SortOrder(op.sort.Order)
		r.SortTarget = pb.RangeRequest_SortTarget(op.sort.Target)
	}
	return r
}

func (op Op) toTxnRequest() *pb.TxnRequest {
	thenOps := make([]*pb.RequestOp, len(op.thenOps))
	for i, tOp := range op.thenOps {
		thenOps[i] = tOp.toRequestOp()
	}
	elseOps := make([]*pb.RequestOp, len(op.elseOps))
	for i, eOp := range op.elseOps {
		elseOps[i] = eOp.toRequestOp()
	}
	cmps := make([]*pb.Compare, len(op.cmps))
	for i := range op.cmps {
		cmps[i] = (*pb.Compare)(&op.cmps[i])
	}
	return &pb.TxnRequest{Compare: cmps, Success: thenOps, Failure: elseOps}
}

func (op Op) toRequestOp() *pb.RequestOp {
	switch op.t {
	case tRange:
		return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: op.toRangeRequest()}}
	case tPut:
		r := &pb.PutRequest{Key: op.key, Value: op.val}
		return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: r}}
	case tDeleteRange:
		r := &pb.DeleteRangeRequest{Key: op.key, RangeEnd: op.end}
		return &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: r}}
	case tTxn:
		return &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: op.toTxnRequest()}}
	default:
		panic("Unknown Op")
	}
}

func (op Op) isWrite() bool {
	if op.t == tTxn {
		for _, tOp := range op.thenOps {
			if tOp.isWrite() {
				return true
			}
		}
		for _, tOp := range op.elseOps {
			if tOp.isWrite() {
				return true
			}
		}
		return false
	}
	return op.t != tRange
}

// OpGet returns "get" operation based on given key and operation options.
func OpGet(key string, opts ...OpOption) Op {
	//	// WithPrefix and WithFromKey are not supported together
	//	if isWithPrefix(opts) && isWithFromKey(opts) {
	//		panic("`WithPrefix` and `WithFromKey` cannot be set at the same time, choose one")
	//	}
	ret := Op{t: tRange, key: []byte(key)}
	ret.applyOpts(opts)
	return ret
}

// OpDelete returns "delete" operation based on given key and operation options.
func OpDelete(key string, opts ...OpOption) Op {
	//	// WithPrefix and WithFromKey are not supported together
	//	if isWithPrefix(opts) && isWithFromKey(opts) {
	//		panic("`WithPrefix` and `WithFromKey` cannot be set at the same time, choose one")
	//	}
	ret := Op{t: tDeleteRange, key: []byte(key)}
	ret.applyOpts(opts)
	switch {
	case ret.limit != 0:
		panic("unexpected limit in delete")
	case ret.sort != nil:
		panic("unexpected sort in delete")
	case ret.serializable:
		panic("unexpected serializable in delete")
	case ret.countOnly:
		panic("unexpected countOnly in delete")
	}
	return ret
}

// OpPut returns "put" operation based on given key-value and operation options.
func OpPut(key, val string, opts ...OpOption) Op {
	ret := Op{t: tPut, key: []byte(key), val: []byte(val)}
	ret.applyOpts(opts)
	switch {
	case ret.end != nil:
		panic("unexpected range in put")
	case ret.limit != 0:
		panic("unexpected limit in put")
	case ret.sort != nil:
		panic("unexpected sort in put")
	case ret.serializable:
		panic("unexpected serializable in put")
	case ret.countOnly:
		panic("unexpected countOnly in put")
	}
	return ret
}

// OpTxn returns "txn" operation based on given transaction conditions.
func OpTxn(cmps []Cmp, thenOps []Op, elseOps []Op) Op {
	return Op{t: tTxn, cmps: cmps, thenOps: thenOps, elseOps: elseOps}
}

func (op *Op) applyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// OpOption configures Operations like Get, Put, Delete.
type OpOption func(*Op)

// WithLimit limits the number of results to return from 'Get' request.
// If WithLimit is given a 0 limit, it is treated as no limit.
func WithLimit(n int64) OpOption { return func(op *Op) { op.limit = n } }

// WithSort specifies the ordering in 'Get' request. It requires
// 'WithRange' and/or 'WithPrefix' to be specified too.
// 'target' specifies the target to sort by: key, version, revisions, value.
// 'order' can be either 'SortNone', 'SortAscend', 'SortDescend'.
func WithSort(target SortTarget, order SortOrder) OpOption {
	return func(op *Op) {
		if target == SortByKey && order == SortAscend {
			// If order != SortNone, server fetches the entire key-space,
			// and then applies the sort and limit, if provided.
			// Since by default the server returns results sorted by keys
			// in lexicographically ascending order, the client should ignore
			// SortOrder if the target is SortByKey.
			order = SortNone
		}
		op.sort = &SortOption{target, order}
	}
}

// GetPrefixRangeEnd gets the range end of the prefix.
// 'Get(foo, WithPrefix())' is equal to 'Get(foo, WithRange(GetPrefixRangeEnd(foo))'.
func GetPrefixRangeEnd(prefix string) string {
	return string(getPrefix([]byte(prefix)))
}

func getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	return noPrefixEnd
}

// WithPrefix enables 'Get', 'Delete' requests to operate
// on the keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
func WithPrefix() OpOption {
	return func(op *Op) {
		if len(op.key) == 0 {
			op.key, op.end = []byte{0}, []byte{0}
			return
		}
		op.end = getPrefix(op.key)
	}
}

// WithRange specifies the range of 'Get', 'Delete'.
// For example, 'Get' requests with 'WithRange(end)' returns
// the keys in the range [key, end).
// endKey must be lexicographically greater than start key.
func WithRange(endKey string) OpOption {
	return func(op *Op) { op.end = []byte(endKey) }
}

// WithFromKey specifies the range of 'Get', 'Delete'
// to be equal or greater than the key in the argument.
func WithFromKey() OpOption {
	return func(op *Op) {
		if len(op.key) == 0 {
			op.key = []byte{0}
		}
		op.end = []byte("\x00")
	}
}

// WithSerializable makes 'Get' request serializable. By default,
// it's linearizable. Serializable requests are better for lower latency
// requirement.
func WithSerializable() OpOption {
	return func(op *Op) { op.serializable = true }
}

// WithKeysOnly makes the 'Get' request return only the keys and the corresponding
// values will be omitted.
func WithKeysOnly() OpOption {
	return func(op *Op) { op.keysOnly = true }
}

// WithCountOnly makes the 'Get' request return only the count of keys.
func WithCountOnly() OpOption {
	return func(op *Op) { op.countOnly = true }
}

// WithFirstKey gets the lexically first key in the request range.
func WithFirstKey() []OpOption { return withTop(SortByKey, SortAscend) }

// WithLastKey gets the lexically last key in the request range.
func WithLastKey() []OpOption { return withTop(SortByKey, SortDescend) }

// withTop gets the first key over the get's prefix given a sort order
func withTop(target SortTarget, order SortOrder) []OpOption {
	return []OpOption{WithPrefix(), WithSort(target, order), WithLimit(1)}
}

//// isWithPrefix returns true if WithPrefix is being called in the op
//func isWithPrefix(opts []OpOption) bool { return isOpFuncCalled("WithPrefix", opts) }
//
//// isWithFromKey returns true if WithFromKey is being called in the op
//func isWithFromKey(opts []OpOption) bool { return isOpFuncCalled("WithFromKey", opts) }
