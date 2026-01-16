// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Command cobble provides a CLI tool for managing Cobble databases.
//
// Usage:
//
//	cobble <command> [flags]
//
// Commands:
//
//	snapshot   Manage database snapshots
//	status     Show admin server status
//	config     Show configuration
package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/cmd/cobble/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
