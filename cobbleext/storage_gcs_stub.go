// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build !gcs && !cloud

package cobbleext

import (
	"fmt"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// createGCSFactory returns an error when GCS support is not compiled in.
func createGCSFactory(cfg *GCSConfig) (remote.StorageFactory, remote.Locator, error) {
	return nil, "", fmt.Errorf("GCS support not compiled in (build with -tags gcs or -tags cloud)")
}
