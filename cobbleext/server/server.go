// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Config holds the admin server configuration.
type Config struct {
	// Enabled enables the admin server.
	Enabled bool

	// Addr is the address to bind to.
	// Can be a TCP address (e.g., "127.0.0.1:6060", ":6060")
	// or a Unix socket path (e.g., "unix:///var/run/cobble.sock")
	Addr string

	// Token is an optional Bearer token for authentication.
	Token string

	// TieredStorageEnabled indicates if tiered storage is enabled.
	TieredStorageEnabled bool

	// TieredStorageStrategy is the tiering strategy.
	TieredStorageStrategy string

	// AutoSnapshotEnabled indicates if auto snapshot is enabled.
	AutoSnapshotEnabled bool

	// AutoSnapshotInterval is the interval between snapshots.
	AutoSnapshotInterval time.Duration
}

// DBAdapter is an interface for database operations.
type DBAdapter interface {
	// Metrics returns the database metrics.
	Metrics() interface{}

	// Checkpoint creates a checkpoint of the database at the given directory.
	Checkpoint(destDir string) error

	// Path returns the path to the database directory.
	Path() string
}

// Server is the admin HTTP server for Cobble.
type Server struct {
	cfg      Config
	db       DBAdapter
	listener net.Listener
	server   *http.Server
	mu       sync.Mutex
	running  bool
}

// New creates a new admin server.
func New(cfg Config, db DBAdapter) *Server {
	return &Server{
		cfg: cfg,
		db:  db,
	}
}

// Start starts the admin server.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("server already running")
	}

	addr := s.cfg.Addr
	if addr == "" {
		addr = "127.0.0.1:6060"
	}

	// Create listener based on address type
	listener, err := createListener(addr)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	s.listener = listener

	// Create HTTP server
	mux := http.NewServeMux()
	s.registerHandlers(mux)

	s.server = &http.Server{
		Handler:      s.withMiddleware(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	s.running = true

	// Start server in background
	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			// Log error but don't panic - the database should continue working
			fmt.Fprintf(os.Stderr, "cobble admin server error: %v\n", err)
		}
	}()

	return nil
}

// Stop stops the admin server.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	s.running = false
	return nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// createListener creates a listener based on the address.
// Supports Unix sockets (unix:///path/to/socket) and TCP addresses.
func createListener(addr string) (net.Listener, error) {
	if strings.HasPrefix(addr, "unix://") {
		socketPath := strings.TrimPrefix(addr, "unix://")
		// Remove existing socket file if it exists
		if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to remove existing socket: %w", err)
		}
		listener, err := net.Listen("unix", socketPath)
		if err != nil {
			return nil, err
		}
		// Set socket permissions to be readable/writable by owner only
		if err := os.Chmod(socketPath, 0600); err != nil {
			listener.Close()
			return nil, fmt.Errorf("failed to set socket permissions: %w", err)
		}
		return listener, nil
	}

	return net.Listen("tcp", addr)
}

// registerHandlers registers all HTTP handlers.
func (s *Server) registerHandlers(mux *http.ServeMux) {
	// Health and status
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/status", s.handleStatus)

	// Database operations
	mux.HandleFunc("/db/stats", s.handleDBStats)
	mux.HandleFunc("/db/compact", s.handleDBCompact)

	// Snapshot operations
	mux.HandleFunc("/snapshots", s.handleSnapshots)
	mux.HandleFunc("/snapshots/", s.handleSnapshotByID)

	// Metrics (cobble-specific, not Pebble metrics)
	mux.HandleFunc("/metrics", s.handleMetrics)
}

// withMiddleware wraps the handler with middleware.
func (s *Server) withMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Auth middleware
		if s.cfg.Token != "" {
			auth := r.Header.Get("Authorization")
			expected := "Bearer " + s.cfg.Token
			if auth != expected {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		// Add common headers
		w.Header().Set("Content-Type", "application/json")

		handler.ServeHTTP(w, r)
	})
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

// handleStatus handles GET /status
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := map[string]interface{}{
		"db_path": s.db.Path(),
		"time":    time.Now().UTC().Format(time.RFC3339),
	}

	// Add config info (without secrets)
	status["tiered_storage_enabled"] = s.cfg.TieredStorageEnabled
	status["tiered_storage_strategy"] = s.cfg.TieredStorageStrategy
	status["auto_snapshot_enabled"] = s.cfg.AutoSnapshotEnabled
	if s.cfg.AutoSnapshotEnabled {
		status["auto_snapshot_interval"] = s.cfg.AutoSnapshotInterval.String()
	}

	json.NewEncoder(w).Encode(status)
}

// handleDBStats handles GET /db/stats
func (s *Server) handleDBStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metrics := s.db.Metrics()
	json.NewEncoder(w).Encode(metrics)
}

// handleDBCompact handles POST /db/compact
func (s *Server) handleDBCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Note: This would need the DB to expose a Compact method
	// For now, return not implemented
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "not_implemented",
		"message": "Manual compaction not yet implemented",
	})
}

// handleSnapshots handles GET/POST /snapshots
func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleListSnapshots(w, r)
	case http.MethodPost:
		s.handleCreateSnapshot(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleListSnapshots handles GET /snapshots
func (s *Server) handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement snapshot listing
	json.NewEncoder(w).Encode(map[string]interface{}{
		"snapshots": []interface{}{},
		"message":   "Snapshot listing not yet implemented",
	})
}

// handleCreateSnapshot handles POST /snapshots
func (s *Server) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement snapshot creation
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "not_implemented",
		"message": "Snapshot creation not yet implemented",
	})
}

// handleSnapshotByID handles GET/DELETE /snapshots/{id}
func (s *Server) handleSnapshotByID(w http.ResponseWriter, r *http.Request) {
	// Extract snapshot ID from path
	path := strings.TrimPrefix(r.URL.Path, "/snapshots/")
	if path == "" {
		http.Error(w, "Snapshot ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGetSnapshot(w, r, path)
	case http.MethodDelete:
		s.handleDeleteSnapshot(w, r, path)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetSnapshot handles GET /snapshots/{id}
func (s *Server) handleGetSnapshot(w http.ResponseWriter, r *http.Request, id string) {
	// TODO: Implement snapshot details
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":      id,
		"status":  "not_implemented",
		"message": "Snapshot details not yet implemented",
	})
}

// handleDeleteSnapshot handles DELETE /snapshots/{id}
func (s *Server) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request, id string) {
	// TODO: Implement snapshot deletion
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":      id,
		"status":  "not_implemented",
		"message": "Snapshot deletion not yet implemented",
	})
}

// handleMetrics handles GET /metrics (cobble-specific metrics in Prometheus format)
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	// Output cobble-specific metrics in Prometheus format
	fmt.Fprintf(w, "# HELP pebble_cobble_admin_up Whether the cobble admin server is up\n")
	fmt.Fprintf(w, "# TYPE pebble_cobble_admin_up gauge\n")
	fmt.Fprintf(w, "pebble_cobble_admin_up 1\n")

	fmt.Fprintf(w, "# HELP pebble_cobble_tiered_storage_enabled Whether tiered storage is enabled\n")
	fmt.Fprintf(w, "# TYPE pebble_cobble_tiered_storage_enabled gauge\n")
	if s.cfg.TieredStorageEnabled {
		fmt.Fprintf(w, "pebble_cobble_tiered_storage_enabled 1\n")
	} else {
		fmt.Fprintf(w, "pebble_cobble_tiered_storage_enabled 0\n")
	}

	fmt.Fprintf(w, "# HELP pebble_cobble_auto_snapshot_enabled Whether auto snapshot is enabled\n")
	fmt.Fprintf(w, "# TYPE pebble_cobble_auto_snapshot_enabled gauge\n")
	if s.cfg.AutoSnapshotEnabled {
		fmt.Fprintf(w, "pebble_cobble_auto_snapshot_enabled 1\n")
	} else {
		fmt.Fprintf(w, "pebble_cobble_auto_snapshot_enabled 0\n")
	}

	// TODO: Add more metrics as features are implemented
	// - pebble_cobble_snapshot_total
	// - pebble_cobble_snapshot_last_success_timestamp
	// - pebble_cobble_snapshot_last_duration_seconds
	// - pebble_cobble_snapshot_last_size_bytes
}
