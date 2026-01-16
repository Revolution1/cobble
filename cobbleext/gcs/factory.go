// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build gcs || cloud

package gcs

import (
	"fmt"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// Factory implements remote.StorageFactory for GCS.
type Factory struct {
	config Config
}

var _ remote.StorageFactory = (*Factory)(nil)

// NewFactory creates a new GCS storage factory.
func NewFactory(cfg Config) *Factory {
	return &Factory{config: cfg}
}

// CreateStorage implements remote.StorageFactory.
func (f *Factory) CreateStorage(locator remote.Locator) (remote.Storage, error) {
	storage, err := NewStorage(f.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS storage: %w", err)
	}
	return storage, nil
}
