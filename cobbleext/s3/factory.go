// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build s3 || cloud

package s3

import (
	"fmt"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// Factory implements remote.StorageFactory for S3.
type Factory struct {
	config Config
}

var _ remote.StorageFactory = (*Factory)(nil)

// NewFactory creates a new S3 storage factory.
func NewFactory(cfg Config) *Factory {
	return &Factory{config: cfg}
}

// CreateStorage implements remote.StorageFactory.
func (f *Factory) CreateStorage(locator remote.Locator) (remote.Storage, error) {
	// For now, we ignore the locator and use our config.
	// In the future, we could parse the locator to support multiple buckets.
	storage, err := NewStorage(f.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 storage: %w", err)
	}
	return storage, nil
}
