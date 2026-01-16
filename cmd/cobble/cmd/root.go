// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"github.com/spf13/cobra"
)

var (
	adminAddr  string
	adminToken string
	configFile string
)

var rootCmd = &cobra.Command{
	Use:   "cobble",
	Short: "Cobble CLI - manage Pebble databases with cloud storage",
	Long: `Cobble is a CLI tool for managing Pebble databases with tiered storage
and snapshot capabilities.

It can be used to:
  - Create, list, and restore snapshots
  - Query the admin server for status and metrics
  - Manage tiered storage configuration`,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&adminAddr, "admin", "a", "", "Admin server address (e.g., localhost:6060 or unix:///path/to/socket)")
	rootCmd.PersistentFlags().StringVarP(&adminToken, "token", "t", "", "Admin server authentication token")
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "Path to config file")
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}
