// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cobbleext

import "context"

// StorageCopier is an optional interface for storage backends that support
// server-side copy operations. S3, GCS, MinIO all support this.
//
// Server-side copy is much faster than download+upload (milliseconds vs minutes
// for large files) because data doesn't need to pass through the client.
type StorageCopier interface {
	// Copy performs a server-side copy from src to dst within the same bucket.
	// Both src and dst are relative to the storage's prefix.
	Copy(ctx context.Context, srcName, dstName string) error
}

// StorageETagGetter is an optional interface for storage backends that support
// retrieving ETags (or equivalent content identifiers) for objects.
//
// ETags are useful for:
// - Deduplication of objects already in storage
// - Conditional requests
// - Content verification
type StorageETagGetter interface {
	// GetETag returns the ETag of the object at the given path.
	// The ETag format varies by storage backend:
	// - S3: MD5 for single-part uploads, "md5-partcount" for multipart
	// - GCS: MD5 or CRC32C
	// - MinIO: Same as S3
	GetETag(ctx context.Context, objName string) (string, error)
}
