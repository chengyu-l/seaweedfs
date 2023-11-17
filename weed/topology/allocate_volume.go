package topology

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"net/url"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId, option *VolumeGrowOption) error {

	// 暂时注释掉，是为了升级
	//return operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
	//
	//	_, allocateErr := client.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{
	//		VolumeId:           uint32(vid),
	//		Collection:         option.Collection,
	//		Replication:        option.ReplicaPlacement.String(),
	//		Ttl:                option.Ttl.String(),
	//		Preallocate:        option.Preallocate,
	//		MemoryMapMaxSizeMb: option.MemoryMapMaxSizeMb,
	//		DiskType:           string(option.DiskType),
	//	})
	//	return allocateErr
	//})

	// 为了升级master, 临时采用HTTP接口创建Volume。等Master和VolumeServer完成升级后，将会再次升级，删除该代码
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("collection", option.Collection)
	values.Add("replication", option.ReplicaPlacement.String())
	values.Add("ttl", option.Ttl.String())
	values.Add("preallocate", fmt.Sprintf("%d", option.Preallocate))
	jsonBlob, err := util.Post("http://"+string(dn.ServerAddress())+"/admin/assign_volume", values)
	if err != nil {
		return err
	}
	var ret AllocateVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		glog.Errorf("Invalid JSON result for %s: %s", "/admin/assign_volum", string(jsonBlob))
		return fmt.Errorf("invalid JSON result for %s: %s", "/admin/assign_volum", string(jsonBlob))
	}
	if ret.Error != "" {
		glog.Errorf(ret.Error)
		return fmt.Errorf(ret.Error)
	}
	return nil

}

func DeleteVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId) error {

	return operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		_, allocateErr := client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
			VolumeId: uint32(vid),
		})
		return allocateErr
	})

}
