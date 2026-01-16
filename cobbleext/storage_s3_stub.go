// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build !s3 && !cloud

package cobbleext

import (
	"fmt"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// createS3Factory returns an error when S3 support is not compiled in.
func createS3Factory(cfg *S3Config) (remote.StorageFactory, remote.Locator, error) {
	return nil, "", fmt.Errorf("S3 support not compiled in (build with -tags s3 or -tags cloud)")
}
