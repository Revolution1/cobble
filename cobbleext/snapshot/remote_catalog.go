// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/remoteobjcat"
	"github.com/cockroachdb/pebble/vfs"
)

// RemoteFileInfo contains information about a file that is stored remotely.
type RemoteFileInfo struct {
	// LocalName is the local file name (e.g., "000001.sst")
	LocalName string

	// RemotePath is the path in remote storage (e.g., "1a3f-2-000001.sst")
	RemotePath string

	// FileNum is the local file number
	FileNum base.DiskFileNum

	// CreatorID is the ID of the node that created the file
	CreatorID objstorage.CreatorID

	// CreatorFileNum is the file number on the creator node
	CreatorFileNum base.DiskFileNum
}

// LoadRemoteObjectCatalog loads the REMOTE-OBJ-CATALOG from a checkpoint directory
// and returns a map from local file names to remote file info.
func LoadRemoteObjectCatalog(checkpointDir string) (map[string]RemoteFileInfo, error) {
	fs := vfs.Default

	// Open the catalog (read-only mode - we use the Open function but won't write)
	catalog, contents, err := remoteobjcat.Open(fs, checkpointDir)
	if err != nil {
		// If the catalog doesn't exist, return empty map (no remote objects)
		if strings.Contains(err.Error(), "no such file") ||
			strings.Contains(err.Error(), "does not exist") {
			return make(map[string]RemoteFileInfo), nil
		}
		return nil, fmt.Errorf("failed to open remote object catalog: %w", err)
	}
	defer catalog.Close()

	result := make(map[string]RemoteFileInfo)
	for _, obj := range contents.Objects {
		localName := makeLocalFileName(obj)
		remotePath := makeRemoteObjectName(obj)

		result[localName] = RemoteFileInfo{
			LocalName:      localName,
			RemotePath:     remotePath,
			FileNum:        obj.FileNum,
			CreatorID:      obj.CreatorID,
			CreatorFileNum: obj.CreatorFileNum,
		}
	}

	return result, nil
}

// makeLocalFileName generates the local file name from remote object metadata.
func makeLocalFileName(meta remoteobjcat.RemoteObjectMetadata) string {
	switch meta.FileType {
	case base.FileTypeTable:
		return fmt.Sprintf("%06d.sst", meta.FileNum)
	case base.FileTypeBlob:
		return fmt.Sprintf("%06d.blob", meta.FileNum)
	default:
		return fmt.Sprintf("%06d", meta.FileNum)
	}
}

// makeRemoteObjectName generates the remote object name from metadata.
// This mirrors the logic in objstorageprovider/remote_obj_name.go
func makeRemoteObjectName(meta remoteobjcat.RemoteObjectMetadata) string {
	if meta.CustomObjectName != "" {
		return meta.CustomObjectName
	}

	hash := remoteObjHash(meta.CreatorID, meta.CreatorFileNum)

	switch meta.FileType {
	case base.FileTypeTable:
		return fmt.Sprintf("%04x-%d-%06d.sst", hash, meta.CreatorID, meta.CreatorFileNum)
	case base.FileTypeBlob:
		return fmt.Sprintf("%04x-%d-%06d.blob", hash, meta.CreatorID, meta.CreatorFileNum)
	default:
		return fmt.Sprintf("%04x-%d-%06d", hash, meta.CreatorID, meta.CreatorFileNum)
	}
}

// remoteObjHash returns a 16-bit hash value derived from the creator ID and
// creator file num. This is used to ensure balanced partitioning with AWS.
// This mirrors the logic in objstorageprovider/remote_obj_name.go
func remoteObjHash(creatorID objstorage.CreatorID, creatorFileNum base.DiskFileNum) uint16 {
	const prime1 = 7459
	const prime2 = 17539
	return uint16(uint64(creatorID)*prime1 + uint64(creatorFileNum)*prime2)
}

// IsRemoteFile checks if a file name corresponds to a remote file.
func IsRemoteFile(fileName string, remoteFiles map[string]RemoteFileInfo) bool {
	_, ok := remoteFiles[fileName]
	return ok
}

// GetRemoteFileInfo returns information about a remote file.
func GetRemoteFileInfo(fileName string, remoteFiles map[string]RemoteFileInfo) (RemoteFileInfo, bool) {
	info, ok := remoteFiles[fileName]
	return info, ok
}

// FindRemoteObjectCatalogInCheckpoint looks for the REMOTE-OBJ-CATALOG marker
// in a checkpoint directory.
func FindRemoteObjectCatalogInCheckpoint(checkpointDir string) (string, bool) {
	// Look for the marker file
	pattern := filepath.Join(checkpointDir, "marker.remote-obj-catalog.*")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return "", false
	}
	return matches[0], true
}
