package filer_pb

import (
	"bytes"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TotalSize(chunks []*FileChunk) (size uint64) {
	for _, c := range chunks {
		t := uint64(c.Offset + int64(c.Size))
		if size < t {
			size = t
		}
	}
	return
}

func FileSize(entry *Entry) (size uint64) {
	if entry == nil || entry.Attributes == nil {
		return 0
	}
	fileSize := entry.Attributes.FileSize
	if entry.RemoteEntry != nil {
		if entry.RemoteEntry.RemoteMtime > entry.Attributes.Mtime {
			fileSize = maxUint64(fileSize, uint64(entry.RemoteEntry.RemoteSize))
		}
	}
	return maxUint64(TotalSize(entry.GetChunks()), fileSize)
}

func ETag(entry *Entry) (etag string) {
	if entry.Attributes == nil || entry.Attributes.Md5 == nil {
		return ETagChunks(entry.GetChunks())
	}
	return fmt.Sprintf("%x", entry.Attributes.Md5)
}

func ETagChunks(chunks []*FileChunk) (etag string) {
	if len(chunks) == 1 {
		return fmt.Sprintf("%x", util.Base64Md5ToBytes(chunks[0].ETag))
	}
	var md5Digests [][]byte
	for _, c := range chunks {
		md5Digests = append(md5Digests, util.Base64Md5ToBytes(c.ETag))
	}
	return fmt.Sprintf("%x-%d", util.Md5(bytes.Join(md5Digests, nil)), len(chunks))
}

func maxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}
