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

//type JudgeTarget int
//type JudgeResult int
//
//const (
//	JudgeExist JudgeTarget = 1
//)
//
//type Judge pb.Judge
//
//func Judge(judge Judge, result string) Judge {
//	var r pb.Judge_Result
//
//	switch result {
//	case "e":
//		r = pb.Key_EXIST
//	case "!e":
//		r = pb.Key_NOT_EXIST
//	default:
//		panic("Unknown result op")
//	}
//
//	judge.Result = r
//	return judge
//}
//
//func Key(key string) Judge {
//	return Judge{Key: []byte(key), Target: pb.Judge_EXIST}
//}
//
//// KeyBytes returns the byte slice holding with the comparison key.
//func (j *Judge) KeyBytes() []byte { return j.Key }
//
//// WithKeyBytes sets the byte slice for the comparison key.
//func (j *Judge) WithKeyBytes(key []byte) { j.Key = key }
//
//// WithRange sets the comparison to scan the range [key, end).
//func (j Judge) WithRange(end string) Judge {
//	j.RangeEnd = []byte(end)
//	return j
//}
//
//// WithPrefix sets the comparison to scan all keys prefixed by the key.
//func (j Judge) WithPrefix() Judge {
//	j.RangeEnd = getPrefix(j.Key)
//	return j
//}

type CompareTarget int
type CompareResult int

const (
	CompareValue CompareTarget = 1
)

type Cmp pb.Compare

func Judgement(cmp Cmp, result string) Cmp {
	var r pb.Compare_CompareResult

	switch result {
	case "e":
		r = pb.Compare_EXIST
	case "!e":
		r = pb.Compare_NOT_EXIST
	default:
		panic("Unknown result op")
	}

	cmp.Result = r
	return cmp
}

func Compare(cmp Cmp, result string, v interface{}) Cmp {
	var r pb.Compare_CompareResult

	switch result {
	case "=":
		r = pb.Compare_EQUAL
	case "!=":
		r = pb.Compare_NOT_EQUAL
	case ">":
		r = pb.Compare_GREATER
	case "<":
		r = pb.Compare_LESS
	case "e":
		r = pb.Compare_EXIST
	case "!e":
		r = pb.Compare_NOT_EXIST
	default:
		panic("Unknown result op")
	}

	cmp.Result = r
	switch cmp.Target {
	case pb.Compare_VALUE:
		val, ok := v.(string)
		if !ok {
			panic("bad compare value")
		}
		cmp.TargetUnion = &pb.Compare_Value{Value: []byte(val)}
	default:
		panic("Unknown compare type")
	}
	return cmp
}

func Key(key string) Cmp {
	return Cmp{Key: []byte(key), Target: pb.Compare_KEY}
}

func Value(key string) Cmp {
	return Cmp{Key: []byte(key), Target: pb.Compare_VALUE}
}

// KeyBytes returns the byte slice holding with the comparison key.
func (cmp *Cmp) KeyBytes() []byte { return cmp.Key }

// WithKeyBytes sets the byte slice for the comparison key.
func (cmp *Cmp) WithKeyBytes(key []byte) { cmp.Key = key }

// ValueBytes returns the byte slice holding the comparison value, if any.
func (cmp *Cmp) ValueBytes() []byte {
	if tu, ok := cmp.TargetUnion.(*pb.Compare_Value); ok {
		return tu.Value
	}
	return nil
}

// WithValueBytes sets the byte slice for the comparison's value.
func (cmp *Cmp) WithValueBytes(v []byte) { cmp.TargetUnion.(*pb.Compare_Value).Value = v }

// WithRange sets the comparison to scan the range [key, end).
func (cmp Cmp) WithRange(end string) Cmp {
	cmp.RangeEnd = []byte(end)
	return cmp
}

// WithPrefix sets the comparison to scan all keys prefixed by the key.
func (cmp Cmp) WithPrefix() Cmp {
	cmp.RangeEnd = getPrefix(cmp.Key)
	return cmp
}

// mustInt64 panics if val isn't an int or int64. It returns an int64 otherwise.
func mustInt64(val interface{}) int64 {
	if v, ok := val.(int64); ok {
		return v
	}
	if v, ok := val.(int); ok {
		return int64(v)
	}
	panic("bad value")
}
