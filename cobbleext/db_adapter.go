// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cobbleext

import (
	"github.com/cockroachdb/pebble/cobbleext/server"
)

// DBAdapter is an interface that abstracts pebble.DB to avoid import cycles.
// The pebble package provides a concrete implementation.
type DBAdapter interface {
	// Metrics returns the database metrics.
	// The returned value is *pebble.Metrics but typed as interface{} to avoid cycles.
	Metrics() interface{}

	// Checkpoint creates a checkpoint of the database at the given directory.
	Checkpoint(destDir string) error

	// Path returns the path to the database directory.
	Path() string

	// OnClose registers a function to be called when the database closes.
	OnClose(fn func())
}

// globalDB holds the current database adapter for the admin server.
// This is set by OnDBOpen and cleared when the database closes.
var globalDB DBAdapter

// globalAdminServer holds the running admin server, if any.
var globalAdminServer *server.Server

// OnDBOpen is called after a database is successfully opened.
// It starts the admin server and auto-snapshot scheduler if configured.
func OnDBOpen(db DBAdapter) error {
	cfg, err := Load()
	if err != nil {
		return err
	}

	globalDB = db

	// Start admin server if enabled
	if cfg.Admin.Enabled {
		serverCfg := server.Config{
			Enabled:               true,
			Addr:                  cfg.Admin.Addr,
			Token:                 cfg.Admin.Token,
			TieredStorageEnabled:  cfg.TieredStorage.Enabled,
			TieredStorageStrategy: string(cfg.TieredStorage.Strategy),
			AutoSnapshotEnabled:   cfg.AutoSnapshot.Enabled,
			AutoSnapshotInterval:  cfg.AutoSnapshot.Interval,
		}
		globalAdminServer = server.New(serverCfg, db)
		if err := globalAdminServer.Start(); err != nil {
			return err
		}
	}

	// Start auto-snapshot scheduler if enabled
	if cfg.AutoSnapshot.Enabled {
		if err := startAutoSnapshot(cfg, db); err != nil {
			return err
		}
	}

	return nil
}

// startAutoSnapshot starts the auto-snapshot scheduler.
// This is a placeholder that will be implemented by the snapshot package.
var startAutoSnapshot = func(cfg *Config, db DBAdapter) error {
	// Will be implemented by snapshot package
	return nil
}

// RegisterAutoSnapshot registers the auto-snapshot scheduler starter function.
// Called by the snapshot package's init function.
func RegisterAutoSnapshot(fn func(*Config, DBAdapter) error) {
	startAutoSnapshot = fn
}

// GetDB returns the current database adapter, if any.
func GetDB() DBAdapter {
	return globalDB
}

// GetAdminServer returns the running admin server, if any.
func GetAdminServer() *server.Server {
	return globalAdminServer
}
