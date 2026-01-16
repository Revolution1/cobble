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
