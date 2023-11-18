package command

import (
	_ "net/http/pprof"

	_ "github.com/seaweedfs/seaweedfs/weed/remote_storage/azure"
	_ "github.com/seaweedfs/seaweedfs/weed/remote_storage/gcs"
	_ "github.com/seaweedfs/seaweedfs/weed/remote_storage/s3"
)
