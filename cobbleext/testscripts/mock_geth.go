//go:build ignore

// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// mock_geth is a simulated Geth application for testing cobble's multi-database
// snapshot capabilities with the admin API.
//
// Usage:
//
//	go run -tags "s3 cloud" mock_geth.go [command] [args...]
//
// Commands:
//
//	init <data-dir>               Initialize databases
//	write <data-dir> <count>      Write data to databases
//	serve <data-dir> <addr>       Start admin server
//	verify <data-dir>             Verify database contents
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
)

// Database names (simulating Geth's multi-database structure)
var dbNames = []string{"chaindata", "state", "ancient"}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "init":
		if len(os.Args) < 3 {
			fmt.Println("Usage: mock_geth init <data-dir>")
			os.Exit(1)
		}
		runInit(os.Args[2])

	case "write":
		if len(os.Args) < 4 {
			fmt.Println("Usage: mock_geth write <data-dir> <count>")
			os.Exit(1)
		}
		count, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Printf("Invalid count: %v\n", err)
			os.Exit(1)
		}
		runWrite(os.Args[2], count)

	case "serve":
		if len(os.Args) < 4 {
			fmt.Println("Usage: mock_geth serve <data-dir> <addr>")
			os.Exit(1)
		}
		runServe(os.Args[2], os.Args[3])

	case "verify":
		if len(os.Args) < 3 {
			fmt.Println("Usage: mock_geth verify <data-dir>")
			os.Exit(1)
		}
		runVerify(os.Args[2])

	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`mock_geth - Simulated Geth for testing cobble

Usage:
  mock_geth init <data-dir>           Initialize databases
  mock_geth write <data-dir> <count>  Write <count> records to each database
  mock_geth serve <data-dir> <addr>   Start admin server at <addr>
  mock_geth verify <data-dir>         Verify database contents

Environment variables for S3/MinIO:
  COBBLE_S3_BUCKET          S3 bucket name
  COBBLE_S3_ENDPOINT        S3 endpoint URL
  COBBLE_S3_ACCESS_KEY_ID   Access key
  COBBLE_S3_SECRET_ACCESS_KEY Secret key
  COBBLE_S3_FORCE_PATH_STYLE Set to "true" for MinIO
  COBBLE_S3_PREFIX          Storage prefix`)
}

func runInit(dataDir string) {
	fmt.Printf("Initializing databases in %s\n", dataDir)

	// Disable external config hook to avoid tiered storage issues during init
	pebble.ExternalConfigHook = nil
	pebble.PostOpenHook = nil

	for _, name := range dbNames {
		dbPath := filepath.Join(dataDir, name)
		fmt.Printf("  Creating %s...\n", name)

		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			fmt.Printf("Failed to create %s: %v\n", name, err)
			os.Exit(1)
		}

		// Write initial marker
		key := []byte("__init__")
		value := []byte(fmt.Sprintf("initialized at %s", time.Now().Format(time.RFC3339)))
		if err := db.Set(key, value, pebble.Sync); err != nil {
			fmt.Printf("Failed to write marker to %s: %v\n", name, err)
			db.Close()
			os.Exit(1)
		}

		if err := db.Close(); err != nil {
			fmt.Printf("Failed to close %s: %v\n", name, err)
			os.Exit(1)
		}

		fmt.Printf("    Created %s\n", dbPath)
	}

	fmt.Println("All databases initialized successfully")
}

func runWrite(dataDir string, count int) {
	fmt.Printf("Writing %d records to each database in %s\n", count, dataDir)

	// Disable external config hook
	pebble.ExternalConfigHook = nil
	pebble.PostOpenHook = nil

	prefixes := map[string]string{
		"chaindata": "block-",
		"state":     "account-",
		"ancient":   "ancient-",
	}

	for _, name := range dbNames {
		dbPath := filepath.Join(dataDir, name)
		prefix := prefixes[name]

		fmt.Printf("  Writing to %s...\n", name)

		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			fmt.Printf("Failed to open %s: %v\n", name, err)
			os.Exit(1)
		}

		// Find the highest existing key number
		startNum := 0
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte(prefix),
			UpperBound: []byte(prefix + "\xff"),
		})
		if err != nil {
			fmt.Printf("Failed to create iterator: %v\n", err)
			db.Close()
			os.Exit(1)
		}
		if iter.Last() {
			key := string(iter.Key())
			if len(key) > len(prefix) {
				if num, err := strconv.Atoi(key[len(prefix):]); err == nil {
					startNum = num + 1
				}
			}
		}
		iter.Close()

		// Write new records
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("%s%08d", prefix, startNum+i)
			value := fmt.Sprintf("value-%s-%08d-data", name, startNum+i)
			if err := db.Set([]byte(key), []byte(value), pebble.NoSync); err != nil {
				fmt.Printf("Failed to write: %v\n", err)
				db.Close()
				os.Exit(1)
			}
		}

		if err := db.Flush(); err != nil {
			fmt.Printf("Failed to flush: %v\n", err)
			db.Close()
			os.Exit(1)
		}

		if err := db.Close(); err != nil {
			fmt.Printf("Failed to close: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("    Wrote %d records (keys %s%08d to %s%08d)\n",
			count, prefix, startNum, prefix, startNum+count-1)
	}

	fmt.Println("Write completed successfully")
}

func runServe(dataDir string, addr string) {
	fmt.Printf("Starting mock Geth with admin server at %s\n", addr)
	fmt.Printf("Data directory: %s\n", dataDir)

	// Open all databases
	databases := make(map[string]*pebble.DB)
	dbPaths := make(map[string]string)
	for _, name := range dbNames {
		dbPath := filepath.Join(dataDir, name)
		fmt.Printf("  Opening %s...\n", name)

		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			fmt.Printf("Failed to open %s: %v\n", name, err)
			os.Exit(1)
		}
		databases[name] = db
		dbPaths[name] = dbPath
		fmt.Printf("    Opened %s\n", name)
	}

	// Ensure cleanup on exit
	defer func() {
		for name, db := range databases {
			fmt.Printf("Closing %s...\n", name)
			db.Close()
		}
	}()

	// Create a simple HTTP admin server for multi-database checkpoints
	mux := http.NewServeMux()

	// GET /health - health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	})

	// GET /status - list databases and their status
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		dbStatus := make(map[string]interface{})
		for name, db := range databases {
			metrics := db.Metrics()
			dbStatus[name] = map[string]interface{}{
				"path":    dbPaths[name],
				"metrics": metrics,
			}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"databases": dbStatus,
			"data_dir":  dataDir,
			"time":      time.Now().UTC().Format(time.RFC3339),
		})
	})

	// POST /checkpoint - create checkpoints for all databases
	mux.HandleFunc("/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		// Parse request body for checkpoint destination
		var req struct {
			DestDir string `json:"dest_dir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": fmt.Sprintf("invalid request: %v", err),
			})
			return
		}

		if req.DestDir == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "dest_dir is required",
			})
			return
		}

		// Create checkpoints for all databases
		results := make(map[string]interface{})
		for name, db := range databases {
			checkpointDir := filepath.Join(req.DestDir, name)
			if err := os.MkdirAll(filepath.Dir(checkpointDir), 0755); err != nil {
				results[name] = map[string]interface{}{
					"status": "error",
					"error":  fmt.Sprintf("failed to create directory: %v", err),
				}
				continue
			}

			if err := db.Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
				results[name] = map[string]interface{}{
					"status": "error",
					"error":  fmt.Sprintf("checkpoint failed: %v", err),
				}
			} else {
				results[name] = map[string]interface{}{
					"status": "ok",
					"path":   checkpointDir,
				}
			}
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"checkpoints": results,
			"time":        time.Now().UTC().Format(time.RFC3339),
		})
	})

	// GET /db/{name}/stats - get stats for a specific database
	mux.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Parse path: /db/{name}/stats or /db/{name}/checkpoint
		path := strings.TrimPrefix(r.URL.Path, "/db/")
		parts := strings.Split(path, "/")
		if len(parts) < 2 {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		dbName := parts[0]
		action := parts[1]

		db, ok := databases[dbName]
		if !ok {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": fmt.Sprintf("database %s not found", dbName),
			})
			return
		}

		switch action {
		case "stats":
			if r.Method != http.MethodGet {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			json.NewEncoder(w).Encode(db.Metrics())

		case "checkpoint":
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				DestDir string `json:"dest_dir"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": fmt.Sprintf("invalid request: %v", err),
				})
				return
			}

			if req.DestDir == "" {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "dest_dir is required",
				})
				return
			}

			if err := db.Checkpoint(req.DestDir, pebble.WithFlushedWAL()); err != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "error",
					"error":  fmt.Sprintf("checkpoint failed: %v", err),
				})
			} else {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "ok",
					"path":   req.DestDir,
				})
			}

		default:
			http.Error(w, "Unknown action", http.StatusBadRequest)
		}
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		fmt.Printf("Admin server listening on %s\n", addr)
		fmt.Println("Endpoints:")
		fmt.Println("  GET  /health                    - health check")
		fmt.Println("  GET  /status                    - database status")
		fmt.Println("  POST /checkpoint                - checkpoint all databases")
		fmt.Println("  GET  /db/{name}/stats           - database stats")
		fmt.Println("  POST /db/{name}/checkpoint      - checkpoint specific database")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal %v, shutting down...\n", sig)
	case err := <-serverErr:
		fmt.Printf("Server error: %v\n", err)
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	fmt.Println("Server stopped")
}

func runVerify(dataDir string) {
	fmt.Printf("Verifying databases in %s\n", dataDir)

	// Disable external config hook
	pebble.ExternalConfigHook = nil
	pebble.PostOpenHook = nil

	allPassed := true
	for _, name := range dbNames {
		dbPath := filepath.Join(dataDir, name)
		fmt.Printf("  Verifying %s...\n", name)

		db, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true})
		if err != nil {
			fmt.Printf("    FAIL: Cannot open: %v\n", err)
			allPassed = false
			continue
		}

		// Count keys
		count := 0
		iter, err := db.NewIter(nil)
		if err != nil {
			fmt.Printf("    FAIL: Cannot create iterator: %v\n", err)
			db.Close()
			allPassed = false
			continue
		}

		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
		iter.Close()
		db.Close()

		fmt.Printf("    OK: %d keys\n", count)
	}

	if allPassed {
		fmt.Println("All verifications passed")
	} else {
		fmt.Println("Some verifications failed")
		os.Exit(1)
	}
}

