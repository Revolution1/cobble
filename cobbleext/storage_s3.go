// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build s3 || cloud

package cobbleext

import (
	"fmt"

	"github.com/cockroachdb/pebble/cobbleext/s3"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// createS3Factory creates an S3 storage factory.
func createS3Factory(cfg *S3Config) (remote.StorageFactory, remote.Locator, error) {
	if cfg.Bucket == "" {
		return nil, "", fmt.Errorf("S3 bucket is required")
	}

	s3Cfg := s3.Config{
		Bucket:          cfg.Bucket,
		Prefix:          cfg.Prefix,
		Region:          cfg.Region,
		Endpoint:        cfg.Endpoint,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		ForcePathStyle:  cfg.ForcePathStyle,
	}

	factory := s3.NewFactory(s3Cfg)

	// Create locator string for identification
	locator := remote.Locator(fmt.Sprintf("s3://%s", cfg.Bucket))

	return factory, locator, nil
}
