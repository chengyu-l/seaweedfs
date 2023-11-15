package toraft

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/api"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftrelay"
	pb "github.com/seaweedfs/seaweedfs/weed/raftstore/serverpb"
	"google.golang.org/grpc"
)

type UiPeer struct {
	Name string `json:"name"`
	Addr string `json:"Addr"`
}

type Peer struct {
	Name          string
	ClientAddress []string
	IsLearner     bool
}

type Peers struct {
	Peers []*Peer
}

func (ps *Peers) Append(p *Peer) {
	ps.Peers = append(ps.Peers, p)
}

func (ps *Peers) PeerAddrs() []string {
	var addrs []string
	for _, p := range ps.Peers {
		addrs = append(addrs, p.ClientAddress...)
	}
	return addrs
}

func (ps Peers) UiPeers() map[string]UiPeer {
	ret := make(map[string]UiPeer)
	for _, p := range ps.Peers {
		var (
			addr string
			name string
		)
		if len(p.ClientAddress) > 0 {
			name = p.ClientAddress[0]
			addr = "http://" + name
		}
		up := UiPeer{
			Name: name,
			Addr: addr,
		}
		ret[p.Name] = up
	}
	return ret
}

type Relay interface {
	RegisterServers(gs *grpc.Server)

	Name() string
	Peers() Peers
	Wait()
	Stop()
	IsLeader() bool
	LeaderClientAddr() string
	RoleChaneNotify() <-chan struct{}

	PutMaxVolumeId(value string) error
	GetMaxVolumeId() (string, error)
	PutMaxFid(value string) error
	GetMaxFid() (string, error)

	AllocateVolumeIdsRange(startVolumeId needle.VolumeId, count uint) (rangeFrom, rangeTo needle.VolumeId, err error)
	AllocateFileIdsRange(startFileId, count uint64) (rangeFrom, rangeTo uint64, err error)

	SetCompactStatus(collection, volume, node, value string) error
	UnsetCompactStatus(collection, volume, node string) error

	GetDataNodeCompactStatus(ip string) (string, bool, error)
	SetDataNodeCompactStatus(ip, statusJson string) error
	UnsetDataNodeCompactStatus(ip string) error

	GetCompactGlobalConfig() (string, error)
	GetCompactCollectionConfig(collection string) (string, error)
	GetCompactVolumeConfig(volume string) (string, error)
	GetCompactNodeConfig(node string) (string, error)
	GetCompactInterval() (string, error)
}

type relay struct {
	clusterId              string
	isInitedMaxVolumeIdKey bool
	isInitedMaxFileIdKey   bool

	rr *raftrelay.RaftRelay
	// API to backend raftstore
	raftKV api.KV

	// stoppedChan report the underlying raft engine stopped
	stoppedChan <-chan struct{}
	// stoppedChan report the underlying raft engine error occurred
	errorChan <-chan error
	// closeHandle used to close the underlying raft engine
	closeHandle func()
}

func NewRelay(clusterId string, cfg *raftrelay.LaunchConfig) (Relay, error) {
	raftrelay, stoppedChan, errChan, closeHandle, err := raftrelay.StartRaftRelay(cfg)
	if err != nil {
		return nil, fmt.Errorf("start raftrelay error %v", err)
	}
	s := &relay{
		clusterId:              clusterId,
		isInitedMaxVolumeIdKey: false,
		isInitedMaxFileIdKey:   false,
		rr:                     raftrelay,
		raftKV:                 api.NewKV(raftrelay),

		stoppedChan: stoppedChan,
		errorChan:   errChan,
		closeHandle: closeHandle,
	}
	return s, nil
}

func (s *relay) maxVolumeIdKey() string {
	return "/" + s.clusterId + "/maxVolumeId"
}

func (s *relay) maxFidKey() string {
	return "/" + s.clusterId + "/maxFid"
}

func (s *relay) compactGlobalConfigKey() string {
	return "/" + s.clusterId + "/compactConfig/global"
}

func (s *relay) compactCollectionConfigKey(collection string) string {
	return "/" + s.clusterId + "/compactConfig/collection/" + collection
}

func (s *relay) compactVolumeConfigKey(volume string) string {
	return "/" + s.clusterId + "/compactConfig/volume/" + volume
}

func (s *relay) compactNodeConfigKey(node string) string {
	return "/" + s.clusterId + "/compactConfig/node/" + node
}

func (s *relay) compactStatusKey(collection, volume, node string) string {
	return "/" + s.clusterId + "/compactStatus/" + collection + "/" + volume + "/" + node
}

func (s *relay) compactIntervalKey() string {
	return "/clusterId/" + s.clusterId + "/compactConfig/interval"
}

func (s *relay) compactDataNodeStatusKey(nodeAdminURL string) string {
	return "/" + s.clusterId + "/compactStatus/node/" + nodeAdminURL
}

func (s *relay) RegisterServers(gs *grpc.Server) {
	pb.RegisterKVServer(gs, api.NewKVServer(s.rr))
	pb.RegisterClusterServer(gs, api.NewClusterServer(s.rr))
	pb.RegisterMaintenanceServer(gs, api.NewMaintenanceServer(s.rr))
}

func (s *relay) Name() string {
	return s.rr.Name()
}

func (s *relay) IsLeader() bool {
	return s.rr.IsLeader()
}

func (s *relay) LeaderClientAddr() string {
	addrs := s.rr.LeaderClientAddrs()
	if addrs != nil {
		return strings.TrimPrefix(addrs[0], "http://")
	}
	return ""
}

func (s *relay) RoleChaneNotify() <-chan struct{} {
	return s.rr.LeaderChangedNotify()
}

func (s *relay) Peers() Peers {
	ps := &Peers{}
	members := s.rr.Peers()
	for _, m := range members {
		var clientAddrs []string
		for _, addr := range m.ClientURLs {
			clientAddrs = append(clientAddrs, strings.TrimPrefix(addr, "http://"))
		}
		ps.Append(&Peer{
			Name:          m.Name,
			ClientAddress: clientAddrs,
		})
	}
	return *ps
}

func (s *relay) Wait() {
	select {
	case <-s.stoppedChan:
		glog.Errorln("Raft storage closed")
	case err := <-s.errorChan:
		glog.Errorln("Raft peer listener error,", err)
	}
}

func (s *relay) Stop() {
	if s.closeHandle != nil {
		s.closeHandle()
	}
}

func (s *relay) PutMaxVolumeId(value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := s.raftKV.Put(ctx, s.maxVolumeIdKey(), value)
	cancel()
	if err != nil {
		glog.Errorf("PutMaxVolumeId(%v) error: %v", s.maxVolumeIdKey(), err)
	}
	return nil
}

func (s *relay) GetMaxVolumeId() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.maxVolumeIdKey())
	cancel()
	if err != nil {
		glog.Errorf("GetMaxVolumeId(%v) error: %v", s.maxVolumeIdKey(), err)
		return "", err
	}
	value := ""
	for _, ev := range resp.Kvs {
		glog.V(4).Infof("[GetMaxVolumeId] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		value = string(ev.Value)
		break
	}
	return value, nil
}

func (s *relay) PutMaxFid(value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := s.raftKV.Put(ctx, s.maxFidKey(), value)
	cancel()
	if err != nil {
		glog.Errorf("PutMaxFid(%v) error: %v", s.maxFidKey(), err)
		return err
	}
	return nil
}

func (s *relay) GetMaxFid() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.maxFidKey())
	cancel()
	if err != nil {
		glog.Errorf("GetMaxFid(%v) error: %v", s.maxFidKey(), err)
		return "", err
	}
	value := ""
	for _, ev := range resp.Kvs {
		glog.V(4).Infof("[GetMaxFid] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		value = string(ev.Value)
		break
	}
	return value, nil
}

func (s *relay) AllocateVolumeIdsRange(startVolumeId needle.VolumeId, count uint) (rangeFrom, rangeTo needle.VolumeId, err error) {
	etcdMaxVolumeId := startVolumeId
	spinFailNum, maxSpinFailNum := 0, 100

	//execute only once
	if !s.isInitedMaxVolumeIdKey {
		glog.V(0).Infoln("[AllocateVolumeIdsRange] init leader set maxVolumeIdKey ...")
		kvc := api.NewKV(s.rr)
		ctxTxn, cancelTxn := context.WithTimeout(context.Background(), 10*time.Second)
		txResp, txErr := kvc.Txn(ctxTxn).
			If(api.Judgement(api.Key(s.maxVolumeIdKey()), "!e")).
			Then(api.OpPut(s.maxVolumeIdKey(), startVolumeId.String())).
			Else(api.OpGet(s.maxVolumeIdKey())).
			Commit()
		cancelTxn()
		if txErr != nil {
			err = txErr
			rangeFrom = startVolumeId
			rangeTo = startVolumeId
			return
		}
		if txResp.Succeeded {
			etcdMaxVolumeId = startVolumeId
			glog.V(0).Infoln("[AllocateVolumeIdsRange] init set etcdMaxVolumeId", etcdMaxVolumeId.String())
		} else {
			var er error
			etcdMaxVolumeIdStr := string(txResp.Responses[0].GetResponseRange().Kvs[0].Value)
			etcdMaxVolumeId, er = needle.NewVolumeId(etcdMaxVolumeIdStr)
			if er != nil {
				glog.V(0).Infoln("[AllocateVolumeIdsRange] init get etcdMaxVolumeId,parse fail,etcdMaxVolumeIdStr:", etcdMaxVolumeIdStr)
				err = er
				rangeFrom = startVolumeId
				rangeTo = startVolumeId
				return
			}
			glog.V(0).Infoln("[AllocateVolumeIdsRange] init get etcdMaxVolumeId", etcdMaxVolumeIdStr)
		}

		s.isInitedMaxVolumeIdKey = true
	}

	//spin loop
	for spinFailNum <= maxSpinFailNum {
		maxVolumeIdFrom := etcdMaxVolumeId
		maxVolumeIdTo := etcdMaxVolumeId.Skip(count)
		spinKvc := api.NewKV(s.rr)
		spinCtxTxn, spinCancelTxn := context.WithTimeout(context.Background(), 10*time.Second)
		spinTxResp, spinTxErr := spinKvc.Txn(spinCtxTxn).
			If(api.Compare(api.Value(s.maxVolumeIdKey()), "=", etcdMaxVolumeId.String())).
			Then(api.OpPut(s.maxVolumeIdKey(), maxVolumeIdTo.String())).
			Else(api.OpGet(s.maxVolumeIdKey())).
			Commit()
		spinCancelTxn()
		if spinTxErr != nil {
			err = spinTxErr
			rangeFrom = startVolumeId
			rangeTo = startVolumeId
			return
		}
		if spinTxResp.Succeeded {
			glog.V(0).Infoln("[AllocateVolumeIdsRange] Spin Loop Success, rangeFrom:", maxVolumeIdFrom, ",rangeTo:", maxVolumeIdTo)
			rangeFrom = maxVolumeIdFrom
			rangeTo = maxVolumeIdTo
			err = nil
			return
		} else {
			respCount := spinTxResp.Responses[0].GetResponseRange().GetCount()
			if respCount == 0 {
				glog.Errorln("[AllocateVolumeIdsRange] Spin Loop Error, maxVolumeId key is empty.")
				rangeFrom = startVolumeId
				rangeTo = startVolumeId
				err = errors.New("AllocateVolumeIdsRange  Spin Loop Error, maxVolumeId key is empty.")
				return
			}

			var er error
			spinMaxVolumeIdStr := string(spinTxResp.Responses[0].GetResponseRange().Kvs[0].Value)
			etcdMaxVolumeId, er = needle.NewVolumeId(spinMaxVolumeIdStr)
			if er != nil {
				glog.V(0).Infoln("[AllocateVolumeIdsRange] Spin Loop. Get new max volume id parse fail,", spinMaxVolumeIdStr)
				err = er
				rangeFrom = startVolumeId
				rangeTo = startVolumeId
				return
			}
			glog.V(0).Infoln("[AllocateVolumeIdsRange] Spin Loop Continue. Get new max volume id:", spinMaxVolumeIdStr)
			spinFailNum++
		}
	}

	//error return
	rangeFrom = startVolumeId
	rangeTo = startVolumeId
	err = errors.New("AllocateVolumeIdsRange Spin Loop Fail Too Many Times")
	return
}

func (s *relay) AllocateFileIdsRange(startFileId, count uint64) (rangeFrom, rangeTo uint64, err error) {
	etcdMaxFileId := startFileId
	spinFailNum, maxSpinFailNum := 0, 100

	//execute only once
	if !s.isInitedMaxFileIdKey {
		glog.V(0).Infoln("[AllocateFileIdsRange] prepare leader set maxVolumeIdKey ...")
		ctxTxn, cancelTxn := context.WithTimeout(context.Background(), 10*time.Second)
		txResp, txErr := s.raftKV.Txn(ctxTxn).
			If(api.Judgement(api.Key(s.maxFidKey()), "!e")).
			Then(api.OpPut(s.maxFidKey(), strconv.FormatUint(startFileId, 10))).
			Else(api.OpGet(s.maxFidKey())).
			Commit()
		cancelTxn()
		if txErr != nil {
			err = txErr
			rangeFrom = startFileId
			rangeTo = startFileId
			return
		}
		if txResp.Succeeded {
			etcdMaxFileId = startFileId
			glog.V(0).Infoln("[AllocateFileIdsRange] prepare set etcdMaxFileId", etcdMaxFileId)
		} else {
			var er error
			etcdMaxFileIdStr := string(txResp.Responses[0].GetResponseRange().Kvs[0].Value)
			etcdMaxFileId, er = strconv.ParseUint(etcdMaxFileIdStr, 0, 64)
			if er != nil {
				glog.V(0).Infoln("[AllocateFileIdsRange] prepare get etcdMaxFileId,parse fail,etcdMaxFileId:", etcdMaxFileIdStr)
				err = er
				rangeFrom = startFileId
				rangeTo = startFileId
				return
			}
			glog.V(0).Infoln("[AllocateFileIdsRange] prepare get etcdMaxFileId", etcdMaxFileIdStr)
		}

		s.isInitedMaxFileIdKey = true
	}

	//spin loop
	for spinFailNum <= maxSpinFailNum {
		maxFileIdFromStr := strconv.FormatUint(etcdMaxFileId, 10)
		maxFileIdToStr := strconv.FormatUint(etcdMaxFileId+count, 10)
		spinCtxTxn, spinCancelTxn := context.WithTimeout(context.Background(), 10*time.Second)
		spinTxResp, spinTxErr := s.raftKV.Txn(spinCtxTxn).
			If(api.Compare(api.Value(s.maxFidKey()), "=", maxFileIdFromStr)).
			Then(api.OpPut(s.maxFidKey(), maxFileIdToStr)).
			Else(api.OpGet(s.maxFidKey())).
			Commit()
		spinCancelTxn()
		if spinTxErr != nil {
			err = spinTxErr
			rangeFrom = startFileId
			rangeTo = startFileId
			return
		}
		if spinTxResp.Succeeded {
			glog.V(0).Infoln("[AllocateFileIdsRange] Spin Loop Success, rangeFrom:", maxFileIdFromStr, ",rangeTo:", maxFileIdToStr)
			rangeFrom = etcdMaxFileId
			rangeTo = etcdMaxFileId + count
			err = nil
			return
		} else {
			respCount := spinTxResp.Responses[0].GetResponseRange().GetCount()
			if respCount == 0 {
				glog.Errorln("[AllocateFileIdsRange] Spin Loop Error, maxFileId key is empty.")
				rangeFrom = startFileId
				rangeTo = startFileId
				err = errors.New("AllocateFileIdsRange  Spin Loop Error, maxFileId key is empty.")
				return
			}

			var er error
			etcdMaxFileIdStr := string(spinTxResp.Responses[0].GetResponseRange().Kvs[0].Value)
			etcdMaxFileId, er = strconv.ParseUint(etcdMaxFileIdStr, 0, 64)
			if er != nil {
				glog.V(0).Infoln("[AllocateFileIdsRange] Spin Loop. Get new max volume id parse fail,", etcdMaxFileIdStr)
				err = er
				rangeFrom = startFileId
				rangeTo = startFileId
				return
			}
			glog.V(0).Infoln("[AllocateFileIdsRange] Spin Loop Continue. Get new max volume id:", etcdMaxFileIdStr)
			spinFailNum++
		}
	}

	//error return
	rangeFrom = startFileId
	rangeTo = startFileId
	err = errors.New("AllocateFileIdsRange Spin Loop Fail Too Many Times")
	return
}

func (s *relay) SetCompactStatus(collection, volume, node, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := s.raftKV.Put(ctx, s.compactStatusKey(collection, volume, node), value)
	cancel()
	if err != nil {
		glog.Errorln("SetCompactStatus error,", err)
		return err
	}
	return nil
}

func (s *relay) GetDataNodeCompactStatus(nodeAdminURL string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.compactDataNodeStatusKey(nodeAdminURL))
	cancel()
	if err != nil {
		glog.Errorln("GetDataNodeCompactStatus error,", err)
		return "", false, err
	}
	key := ""
	exists := false
	for _, ev := range resp.Kvs {
		glog.V(4).Infof("[GetDataNodeCompactStatus] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		key = string(ev.Value)
		break
	}
	if key == "" {
		exists = false
		return "", exists, nil
	} else {
		exists = true
		return key, exists, nil
	}
}

func (s *relay) SetDataNodeCompactStatus(nodeAdminURL, statusJson string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := s.raftKV.Put(ctx, s.compactDataNodeStatusKey(nodeAdminURL), statusJson)
	cancel()
	if err != nil {
		glog.Errorln("SetDataNodeCompactStatus error,", err)
		return err
	}
	return nil
}

func (s *relay) UnsetDataNodeCompactStatus(nodeAdminURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := s.raftKV.Delete(ctx, s.compactDataNodeStatusKey(nodeAdminURL))
	cancel()
	if err != nil {
		glog.Errorln("UnsetDataNodeCompactStatus error,", err)
		return err
	}
	return nil
}

func (s *relay) UnsetCompactStatus(collection, volume, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.raftKV.Delete(ctx, s.compactStatusKey(collection, volume, node))
	if err != nil {
		glog.Errorln("UnsetCompactStatus error,", err)
		return err
	}
	return nil
}

func (s *relay) GetCompactGlobalConfig() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.compactGlobalConfigKey(), api.WithSerializable())
	cancel()
	if err != nil {
		glog.Errorln("GetCompactGlobalConfig error,", err)
		return "", err
	}
	value := ""
	for _, ev := range resp.Kvs {
		glog.V(4).Infof("[GetCompactGlobalConfig] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		value = string(ev.Value)
		break
	}
	if value == "" {
		return value, errors.New("not found")
	}
	return value, nil
}

func (s *relay) GetCompactCollectionConfig(collection string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.compactCollectionConfigKey(collection), api.WithSerializable())
	cancel()
	if err != nil {
		glog.Errorln("GetCompactCollectionConfig error,", err)
		return "", err
	}
	value := ""
	for _, ev := range resp.Kvs {
		glog.V(4).Infof("[GetCompactCollectionConfig] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		value = string(ev.Value)
		break
	}
	if value == "" {
		return value, errors.New("not found")
	}
	return value, nil
}

func (s *relay) GetCompactVolumeConfig(volume string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.compactVolumeConfigKey(volume), api.WithSerializable())
	cancel()
	if err != nil {
		glog.Errorln("GetCompactVolumeIdConfig error,", err)
		return "", err
	}
	value := ""
	for _, ev := range resp.Kvs {
		glog.V(4).Infof("[GetCompactVolumeConfig] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		value = string(ev.Value)
		break
	}
	if value == "" {
		return value, errors.New("not found")
	}
	return value, nil
}

func (s *relay) GetCompactNodeConfig(node string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.compactNodeConfigKey(node), api.WithSerializable())
	cancel()
	if err != nil {
		glog.Errorln("GetCompactNodeConfig error,", err)
		return "", err
	}
	value := ""
	for _, ev := range resp.Kvs {
		glog.V(4).Infof("[GetCompactNodeConfig] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		value = string(ev.Value)
		break
	}
	if value == "" {
		return value, errors.New("not found")
	}
	return value, nil
}

func (s *relay) GetCompactInterval() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := s.raftKV.Get(ctx, s.compactIntervalKey(), api.WithSerializable())
	cancel()
	if err != nil {
		glog.Errorln("GetCompactInterval error,", err)
		return "", err
	}
	value := ""
	for _, ev := range resp.Kvs {
		glog.V(5).Infof("[GetCompactInterval] ev.Key:%s ,ev.Value:%s", ev.Key, ev.Value)
		value = string(ev.Value)
		break
	}
	if value == "" {
		return value, errors.New("not found")
	}
	return value, nil
}
