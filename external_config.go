// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/cobbleext"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// ExternalConfigHook is a function that can modify Options before they are used.
// It is called by Open() after Clone() but before EnsureDefaults().
//
// This hook allows external packages (like cobbleext) to inject configuration
// from environment variables or config files without creating import cycles.
//
// By default, this is set to use cobbleext.ApplyConfig which loads configuration
// from environment variables (COBBLE_*) and config files.
//
// To disable automatic configuration loading, set this to nil:
//
//	pebble.ExternalConfigHook = nil
var ExternalConfigHook func(opts *Options) error = defaultExternalConfigHook

func defaultExternalConfigHook(opts *Options) error {
	return cobbleext.ApplyConfig(&optionsAdapter{opts: opts})
}

// PostOpenHook is a function that is called after the database is successfully opened.
// It allows external packages (like cobbleext) to initialize services that require
// a running DB instance, such as the admin server or auto-snapshot scheduler.
//
// By default, this is set to use cobbleext.OnDBOpen which starts the admin server
// and auto-snapshot scheduler if configured.
//
// To disable post-open initialization, set this to nil:
//
//	pebble.PostOpenHook = nil
var PostOpenHook func(db *DB) error = defaultPostOpenHook

func defaultPostOpenHook(db *DB) error {
	return cobbleext.OnDBOpen(&dbAdapter{db: db})
}

// optionsAdapter adapts *Options to cobbleext.OptionsAdapter.
type optionsAdapter struct {
	opts *Options
}

// SetRemoteStorage implements cobbleext.OptionsAdapter.
func (a *optionsAdapter) SetRemoteStorage(factory remote.StorageFactory, locator remote.Locator, strategy remote.CreateOnSharedStrategy) {
	a.opts.Experimental.RemoteStorage = factory
	a.opts.Experimental.CreateOnSharedLocator = locator
	a.opts.Experimental.CreateOnShared = strategy
}

// SetSecondaryCacheSize implements cobbleext.OptionsAdapter.
func (a *optionsAdapter) SetSecondaryCacheSize(size int64) {
	a.opts.Experimental.SecondaryCacheSizeBytes = size
}

// Logf implements cobbleext.OptionsAdapter.
func (a *optionsAdapter) Logf(format string, args ...interface{}) {
	if a.opts.Logger != nil {
		a.opts.Logger.Infof(format, args...)
	}
}

// dbAdapter adapts *DB to cobbleext.DBAdapter.
type dbAdapter struct {
	db *DB
}

// Metrics implements cobbleext.DBAdapter.
func (a *dbAdapter) Metrics() interface{} {
	return a.db.Metrics()
}

// Checkpoint implements cobbleext.DBAdapter.
func (a *dbAdapter) Checkpoint(destDir string) error {
	return a.db.Checkpoint(destDir, WithFlushedWAL())
}

// Path implements cobbleext.DBAdapter.
func (a *dbAdapter) Path() string {
	return a.db.dirname
}

// Close callback for cleanup.
func (a *dbAdapter) OnClose(fn func()) {
	// Register cleanup function to be called when DB closes.
	// We use a wrapper around the existing close mechanism.
	a.db.mu.Lock()
	defer a.db.mu.Unlock()
	if a.db.opts.EventListener != nil && a.db.opts.EventListener.TableDeleted != nil {
		// Store original for chaining if needed in the future
	}
	// For now, we'll handle cleanup via the admin server's context cancellation
}
