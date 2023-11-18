package shell

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"io"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

type ShellOptions struct {
	Masters        *string
	GrpcDialOption grpc.DialOption
	// shell transient context
	FilerHost    string
	FilerPort    int64
	FilerGroup   *string
	FilerAddress pb.ServerAddress
	Directory    string
}

type CommandEnv struct {
	env          map[string]string
	MasterClient *wdclient.MasterClient
	option       *ShellOptions
	locker       *exclusive_locks.ExclusiveLocker
}

type command interface {
	Name() string
	Help() string
	Do([]string, *CommandEnv, io.Writer) error
}

var (
	Commands = []command{}
)

func NewCommandEnv(options *ShellOptions) *CommandEnv {
	ce := &CommandEnv{
		env:          make(map[string]string),
		MasterClient: wdclient.NewMasterClient(options.GrpcDialOption, *options.FilerGroup, pb.AdminShellClient, "", "", "", pb.ServerAddresses(*options.Masters).ToAddressMap()),
		option:       options,
	}
	ce.locker = exclusive_locks.NewExclusiveLocker(ce.MasterClient, "shell")
	return ce
}

func (ce *CommandEnv) parseUrl(input string) (path string, err error) {
	if strings.HasPrefix(input, "http") {
		err = fmt.Errorf("http://<filer>:<port> prefix is not supported any more")
		return
	}
	if !strings.HasPrefix(input, "/") {
		input = util.Join(ce.option.Directory, input)
	}
	return input, err
}

func (ce *CommandEnv) confirmIsLocked(args []string) error {

	if ce.locker.IsLocked() {
		return nil
	}
	ce.locker.SetMessage(fmt.Sprintf("%v", args))

	return fmt.Errorf("need to run \"lock\" first to continue")

}

func (ce *CommandEnv) isLocked() bool {
	if ce == nil {
		return true
	}
	return ce.locker.IsLocked()
}

func (ce *CommandEnv) GetDataCenter() string {
	return ce.MasterClient.DataCenter
}

func readNeedleMeta(grpcDialOption grpc.DialOption, volumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (resp *volume_server_pb.ReadNeedleMetaResponse, err error) {
	err = operation.WithVolumeServerClient(false, volumeServer, grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if resp, err = client.ReadNeedleMeta(context.Background(), &volume_server_pb.ReadNeedleMetaRequest{
				VolumeId: volumeId,
				NeedleId: uint64(needleValue.Key),
				Offset:   needleValue.Offset.ToActualOffset(),
				Size:     int32(needleValue.Size),
			}); err != nil {
				return err
			}
			return nil
		},
	)
	return
}

func readNeedleStatus(grpcDialOption grpc.DialOption, sourceVolumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (resp *volume_server_pb.VolumeNeedleStatusResponse, err error) {
	err = operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if resp, err = client.VolumeNeedleStatus(context.Background(), &volume_server_pb.VolumeNeedleStatusRequest{
				VolumeId: volumeId,
				NeedleId: uint64(needleValue.Key),
			}); err != nil {
				return err
			}
			return nil
		},
	)
	return
}

func infoAboutSimulationMode(writer io.Writer, forceMode bool, forceModeOption string) {
	if forceMode {
		return
	}
	fmt.Fprintf(writer, "Running in simulation mode. Use \"%s\" option to apply the changes.\n", forceModeOption)
}
