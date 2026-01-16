// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build gcs || cloud

package cobbleext

import (
	"fmt"

	"github.com/cockroachdb/pebble/cobbleext/gcs"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// createGCSFactory creates a GCS storage factory.
func createGCSFactory(cfg *GCSConfig) (remote.StorageFactory, remote.Locator, error) {
	if cfg.Bucket == "" {
		return nil, "", fmt.Errorf("GCS bucket is required")
	}

	gcsCfg := gcs.Config{
		Bucket:          cfg.Bucket,
		Prefix:          cfg.Prefix,
		CredentialsFile: cfg.CredentialsFile,
		CredentialsJSON: cfg.CredentialsJSON,
	}

	factory := gcs.NewFactory(gcsCfg)

	// Create locator string for identification
	locator := remote.Locator(fmt.Sprintf("gs://%s", cfg.Bucket))

	return factory, locator, nil
}
