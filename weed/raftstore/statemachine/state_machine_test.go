package statemachine

import (
	"bytes"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"os"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	key1 = []byte("key1")
	key2 = []byte("key2")
	key3 = []byte("key3")
	key4 = []byte("key4")
	key5 = []byte("key5")
	key6 = []byte("key6")
	val1 = []byte("val1")
	val2 = []byte("val2")
	val3 = []byte("val3")
	val4 = []byte("val4")
	val5 = []byte("val5")
	val6 = []byte("val6")
)

func TestApplyConcurrentReadAndWrite(t *testing.T) {
	lg, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("create logger error %v", err)
	}
	snap_path := "./test"
	store_path := snap_path + "/db"
	if err := os.Mkdir(snap_path, os.ModePerm); err != nil {
		t.Fatalf("create temp db error %v", err)
	}
	defer func() {
		if err := os.RemoveAll(snap_path); err != nil {
			t.Fatalf("remove file error %v", err)
		}
	}()
	cfg := &BoltDBBackendConfig{
		Logger:               lg,
		StorePath:            store_path,
		SnapPath:             snap_path,
		BackendFreelistType:  bbolt.FreelistMapType,
		BackendBatchInterval: 5 * time.Second,
		BackendBatchLimit:    100000,
	}
	s := NewBoltDBStateMachine(cfg)
	sm := s.(*StateMachine)
	add_key1 := &pb.PutRequest{
		Key:   key1,
		Value: val1,
	}
	if _, err := sm.Put(nil, add_key1); err != nil {
		t.Fatalf("put error %v", err)
	}
	// key1-val1
	get_key1 := &pb.RangeRequest{
		Key: key1,
	}
	// fetch from cache
	if resp, err := sm.Range(nil, get_key1); err != nil {
		t.Fatalf("range error %v", err)
	} else {
		if len(resp.Kvs) != 1 {
			t.Fatalf("range response length error, got %d, expected 1", len(resp.Kvs))
		}
		if bytes.Compare(resp.Kvs[0].Key, key1) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[0].Key, key1)
		}
		if bytes.Compare(resp.Kvs[0].Value, val1) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[0].Value, val1)
		}
	}
	add_key2 := &pb.PutRequest{
		Key:   key2,
		Value: val2,
	}
	if _, err := sm.Put(nil, add_key2); err != nil {
		t.Fatalf("put error %v", err)
	}
	// key1-val1, key2-val2
	get_key2 := &pb.RangeRequest{
		Key: key2,
	}
	// fetch from cache
	if resp, err := sm.Range(nil, get_key2); err != nil {
		t.Fatalf("range error %v", err)
	} else {
		if len(resp.Kvs) != 1 {
			t.Fatalf("range response length error, got %d, expected 1", len(resp.Kvs))
		}
		if bytes.Compare(resp.Kvs[0].Key, key2) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[0].Key, key2)
		}
		if bytes.Compare(resp.Kvs[0].Value, val2) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[0].Value, val2)
		}
	}
	// clean cache
	sm.Commit()
	range_key1_key3 := &pb.RangeRequest{
		Key:       key1,
		RangeEnd:  key3,
		SortOrder: pb.RangeRequest_ASCEND,
	}
	// fetch from db
	if resp, err := sm.Range(nil, range_key1_key3); err != nil {
		t.Fatalf("range error %v", err)
	} else {
		if len(resp.Kvs) != 2 {
			t.Fatalf("range response length error, got %d, expected 2", len(resp.Kvs))
		}
		if bytes.Compare(resp.Kvs[0].Key, key1) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[0].Key, key1)
		}
		if bytes.Compare(resp.Kvs[0].Value, val1) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[0].Value, val1)
		}
		if bytes.Compare(resp.Kvs[1].Key, key2) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[1].Key, key2)
		}
		if bytes.Compare(resp.Kvs[1].Value, val2) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[1].Value, val2)
		}
	}
	update_key2 := &pb.PutRequest{
		Key:   key2,
		Value: val4,
	}
	if _, err := sm.Put(nil, update_key2); err != nil {
		t.Fatalf("put error %v", err)
	}
	// key1-val1(db), key2-val2(db), key2-val4(cache)
	if resp, err := sm.Range(nil, get_key2); err != nil {
		t.Fatalf("range error %v", err)
	} else {
		if len(resp.Kvs) != 1 {
			t.Fatalf("range response length error, got %d, expected 1", len(resp.Kvs))
		}
		if bytes.Compare(resp.Kvs[0].Key, key2) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[0].Key, key2)
		}
		if bytes.Compare(resp.Kvs[0].Value, val4) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[0].Value, val4)
		}
	}
	add_key3 := &pb.PutRequest{
		Key:   key3,
		Value: val3,
	}
	if _, err := sm.Put(nil, add_key3); err != nil {
		t.Fatalf("put error %v", err)
	}
	// key1-val1(db), key2-val2(db), key2-val4(cache), key3-val3(db)
	if resp, err := sm.Range(nil, range_key1_key3); err != nil {
		t.Fatalf("range error %v", err)
	} else {
		if len(resp.Kvs) != 2 {
			t.Fatalf("range response length error, got %d, expected 2", len(resp.Kvs))
		}
		if bytes.Compare(resp.Kvs[0].Key, key1) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[0].Key, key1)
		}
		if bytes.Compare(resp.Kvs[0].Value, val1) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[0].Value, val1)
		}
		if bytes.Compare(resp.Kvs[1].Key, key2) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[1].Key, key2)
		}
		if bytes.Compare(resp.Kvs[1].Value, val4) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[1].Value, val4)
		}
	}
	delete_key1 := &pb.DeleteRangeRequest{
		Key: key1,
	}
	if _, err := sm.DeleteRange(nil, delete_key1); err != nil {
		t.Fatalf("put error %v", err)
	}
	// key1-val1(db), key1 deleted in cache, key2-val4(db), key3-val3(db)
	if resp, err := sm.Range(nil, range_key1_key3); err != nil {
		t.Fatalf("range error %v", err)
	} else {
		if len(resp.Kvs) != 1 {
			t.Fatalf("range response length error, got %d, expected 2", len(resp.Kvs))
		}
		if bytes.Compare(resp.Kvs[0].Key, key2) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[0].Key, key2)
		}
		if bytes.Compare(resp.Kvs[0].Value, val4) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[0].Value, val4)
		}
	}
	sm.Commit()
	// key2-val4(db), key3-val3(db)
	if resp, err := sm.Range(nil, range_key1_key3); err != nil {
		t.Fatalf("range error %v", err)
	} else {
		if len(resp.Kvs) != 1 {
			t.Fatalf("range response length error, got %d, expected 2", len(resp.Kvs))
		}
		if bytes.Compare(resp.Kvs[0].Key, key2) != 0 {
			t.Fatalf("range response key not matched, got %s, expected %s", resp.Kvs[0].Key, key2)
		}
		if bytes.Compare(resp.Kvs[0].Value, val4) != 0 {
			t.Fatalf("range response val not matched, got %s, expected %s", resp.Kvs[0].Value, val4)
		}
	}
}
