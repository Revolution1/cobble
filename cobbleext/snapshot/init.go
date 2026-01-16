// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"github.com/cockroachdb/pebble/cobbleext"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// globalScheduler holds the running auto-snapshot scheduler, if any.
var globalScheduler *Scheduler

// snapshotDBAdapter adapts cobbleext.DBAdapter to snapshot.DBAdapter.
type snapshotDBAdapter struct {
	db cobbleext.DBAdapter
}

func (a *snapshotDBAdapter) Checkpoint(destDir string) error {
	return a.db.Checkpoint(destDir)
}

func (a *snapshotDBAdapter) Path() string {
	return a.db.Path()
}

func init() {
	cobbleext.RegisterAutoSnapshot(startAutoSnapshot)
}

func startAutoSnapshot(cfg *cobbleext.Config, db cobbleext.DBAdapter) error {
	// Get storage from configuration
	storage, err := getSnapshotStorage(cfg)
	if err != nil {
		return err
	}
	if storage == nil {
		// No storage configured, skip auto-snapshot
		return nil
	}

	prefix := cfg.Snapshot.Prefix
	if prefix == "" {
		prefix = "snapshots/"
	}

	schedCfg := SchedulerConfig{
		Interval:    cfg.AutoSnapshot.Interval,
		Incremental: cfg.AutoSnapshot.Incremental,
		KeepLast:    cfg.AutoSnapshot.KeepLast,
		Storage:     storage,
		Prefix:      prefix,
	}

	adapter := &snapshotDBAdapter{db: db}
	globalScheduler = NewScheduler(schedCfg, adapter)

	// Register cleanup on database close
	db.OnClose(func() {
		if globalScheduler != nil {
			globalScheduler.Stop()
			globalScheduler = nil
		}
	})

	return globalScheduler.Start()
}

// getSnapshotStorage creates remote storage from configuration.
func getSnapshotStorage(cfg *cobbleext.Config) (remote.Storage, error) {
	// If UseTieredConfig is set, use the tiered storage config
	if cfg.Snapshot.UseTieredConfig {
		// This would reuse the tiered storage configuration
		// For now, return nil to indicate no storage configured
		// The actual implementation would create storage from TieredStorage config
		return nil, nil
	}

	// Use snapshot-specific bucket config if available
	if cfg.Snapshot.Bucket == "" {
		return nil, nil
	}

	// The actual storage creation would happen here
	// For now, return nil as the storage creation depends on build tags
	return nil, nil
}

// GetScheduler returns the running auto-snapshot scheduler, if any.
func GetScheduler() *Scheduler {
	return globalScheduler
}
