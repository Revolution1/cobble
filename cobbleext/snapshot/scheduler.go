// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// SchedulerConfig configures the auto snapshot scheduler.
type SchedulerConfig struct {
	// Interval between snapshots.
	Interval time.Duration

	// Incremental enables incremental snapshots after the first full snapshot.
	Incremental bool

	// KeepLast is the number of snapshots to keep. 0 means keep all.
	KeepLast int

	// Storage is the remote storage for snapshots.
	Storage remote.Storage

	// Prefix is the path prefix in the bucket for snapshots.
	Prefix string

	// OnError is called when a snapshot fails.
	OnError func(error)

	// OnSuccess is called when a snapshot succeeds.
	OnSuccess func(*SnapshotResult)
}

// Scheduler manages automatic periodic snapshots.
type Scheduler struct {
	cfg SchedulerConfig
	db  DBAdapter

	mu        sync.Mutex
	running   bool
	stopCh    chan struct{}
	doneCh    chan struct{}
	lastSnap  *SnapshotResult
	lastError error
}

// NewScheduler creates a new auto snapshot scheduler.
func NewScheduler(cfg SchedulerConfig, db DBAdapter) *Scheduler {
	return &Scheduler{
		cfg: cfg,
		db:  db,
	}
}

// Start starts the scheduler.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("scheduler already running")
	}

	if s.cfg.Interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}

	if s.cfg.Storage == nil {
		return fmt.Errorf("storage is required")
	}

	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})
	s.running = true

	go s.run()

	return nil
}

// Stop stops the scheduler and waits for it to finish.
func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	// Wait for goroutine to finish
	<-s.doneCh
	return nil
}

// Running returns whether the scheduler is running.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// LastSnapshot returns the result of the last successful snapshot.
func (s *Scheduler) LastSnapshot() *SnapshotResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSnap
}

// LastError returns the error from the last failed snapshot.
func (s *Scheduler) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastError
}

// TriggerSnapshot triggers an immediate snapshot.
// Returns an error if a snapshot is already in progress.
func (s *Scheduler) TriggerSnapshot(ctx context.Context) (*SnapshotResult, error) {
	return s.doSnapshot(ctx)
}

func (s *Scheduler) run() {
	defer close(s.doneCh)

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	// Do an initial snapshot immediately
	ctx := context.Background()
	s.doSnapshot(ctx)

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.doSnapshot(context.Background())
		}
	}
}

func (s *Scheduler) doSnapshot(ctx context.Context) (*SnapshotResult, error) {
	// Determine if this should be incremental
	s.mu.Lock()
	isIncremental := s.cfg.Incremental && s.lastSnap != nil
	s.mu.Unlock()

	opts := SnapshotOptions{
		Storage:     s.cfg.Storage,
		Prefix:      s.cfg.Prefix,
		Incremental: isIncremental,
		FlushWAL:    true,
	}

	result, err := CreateSnapshot(ctx, s.db, opts)
	if err != nil {
		s.mu.Lock()
		s.lastError = err
		s.mu.Unlock()

		if s.cfg.OnError != nil {
			s.cfg.OnError(err)
		}
		fmt.Fprintf(os.Stderr, "cobble auto-snapshot failed: %v\n", err)
		return nil, err
	}

	s.mu.Lock()
	s.lastSnap = result
	s.lastError = nil
	s.mu.Unlock()

	if s.cfg.OnSuccess != nil {
		s.cfg.OnSuccess(result)
	}

	// Garbage collect old snapshots if configured
	if s.cfg.KeepLast > 0 {
		gcOpts := GCOptions{
			Storage:           s.cfg.Storage,
			Prefix:            s.cfg.Prefix,
			KeepLast:          s.cfg.KeepLast,
			KeepFullSnapshots: 1, // Always keep at least one full snapshot
		}
		if _, err := GarbageCollect(ctx, gcOpts); err != nil {
			fmt.Fprintf(os.Stderr, "cobble snapshot GC failed: %v\n", err)
		}
	}

	return result, nil
}
