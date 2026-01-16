// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerStartStop(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	cfg := SchedulerConfig{
		Interval: 100 * time.Millisecond,
		Storage:  storage,
		Prefix:   "test-snapshots/",
	}

	sched := NewScheduler(cfg, db)

	// Start scheduler
	if err := sched.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !sched.Running() {
		t.Error("expected scheduler to be running")
	}

	// Wait for at least one snapshot
	time.Sleep(150 * time.Millisecond)

	// Stop scheduler
	if err := sched.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if sched.Running() {
		t.Error("expected scheduler to be stopped")
	}

	// Verify at least one snapshot was created
	lastSnap := sched.LastSnapshot()
	if lastSnap == nil {
		t.Error("expected at least one snapshot")
	} else {
		t.Logf("Last snapshot: ID=%s, Files=%d", lastSnap.SnapshotID, lastSnap.FileCount)
	}
}

func TestSchedulerIncrementalSnapshots(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	cfg := SchedulerConfig{
		Interval:    100 * time.Millisecond,
		Incremental: true,
		Storage:     storage,
		Prefix:      "test-snapshots/",
	}

	sched := NewScheduler(cfg, db)

	if err := sched.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for multiple snapshots
	time.Sleep(250 * time.Millisecond)

	if err := sched.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify snapshots were created
	catalog, err := LoadCatalog(context.Background(), storage, "test-snapshots/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	if len(catalog.Snapshots) < 2 {
		t.Errorf("expected at least 2 snapshots, got %d", len(catalog.Snapshots))
	}

	// Check that incremental snapshots were created
	hasIncremental := false
	for _, snap := range catalog.Snapshots {
		if snap.Incremental {
			hasIncremental = true
			break
		}
	}

	if !hasIncremental && len(catalog.Snapshots) > 1 {
		t.Error("expected at least one incremental snapshot")
	}

	t.Logf("Created %d snapshots", len(catalog.Snapshots))
}

func TestSchedulerTriggerSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	cfg := SchedulerConfig{
		Interval: 1 * time.Hour, // Long interval so timer doesn't fire
		Storage:  storage,
		Prefix:   "test-snapshots/",
	}

	sched := NewScheduler(cfg, db)

	if err := sched.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer sched.Stop()

	// Wait for initial snapshot
	time.Sleep(50 * time.Millisecond)

	// Trigger manual snapshot
	result, err := sched.TriggerSnapshot(context.Background())
	if err != nil {
		t.Fatalf("TriggerSnapshot failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected snapshot result")
	}

	t.Logf("Triggered snapshot: ID=%s", result.SnapshotID)

	// Verify catalog has snapshots
	catalog, err := LoadCatalog(context.Background(), storage, "test-snapshots/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	if len(catalog.Snapshots) < 2 {
		t.Errorf("expected at least 2 snapshots (initial + triggered), got %d", len(catalog.Snapshots))
	}
}

func TestSchedulerCallbacks(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	var successCount atomic.Int32
	var errorCount atomic.Int32

	cfg := SchedulerConfig{
		Interval: 50 * time.Millisecond,
		Storage:  storage,
		Prefix:   "test-snapshots/",
		OnSuccess: func(result *SnapshotResult) {
			successCount.Add(1)
		},
		OnError: func(err error) {
			errorCount.Add(1)
		},
	}

	sched := NewScheduler(cfg, db)

	if err := sched.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for a few snapshots
	time.Sleep(180 * time.Millisecond)

	if err := sched.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if successCount.Load() < 2 {
		t.Errorf("expected at least 2 success callbacks, got %d", successCount.Load())
	}

	if errorCount.Load() != 0 {
		t.Errorf("expected 0 error callbacks, got %d", errorCount.Load())
	}

	t.Logf("Success callbacks: %d, Error callbacks: %d", successCount.Load(), errorCount.Load())
}

func TestSchedulerGarbageCollection(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	cfg := SchedulerConfig{
		Interval: 30 * time.Millisecond,
		KeepLast: 2,
		Storage:  storage,
		Prefix:   "test-snapshots/",
	}

	sched := NewScheduler(cfg, db)

	if err := sched.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for several snapshots plus GC
	time.Sleep(200 * time.Millisecond)

	if err := sched.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify only KeepLast snapshots remain
	catalog, err := LoadCatalog(context.Background(), storage, "test-snapshots/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	if len(catalog.Snapshots) > cfg.KeepLast {
		t.Errorf("expected at most %d snapshots, got %d", cfg.KeepLast, len(catalog.Snapshots))
	}

	t.Logf("Snapshots remaining after GC: %d", len(catalog.Snapshots))
}

func TestSchedulerDoubleStart(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	cfg := SchedulerConfig{
		Interval: 1 * time.Hour,
		Storage:  storage,
		Prefix:   "test-snapshots/",
	}

	sched := NewScheduler(cfg, db)

	if err := sched.Start(); err != nil {
		t.Fatalf("First Start failed: %v", err)
	}
	defer sched.Stop()

	// Second start should fail
	if err := sched.Start(); err == nil {
		t.Error("expected second Start to fail")
	}
}

func TestSchedulerInvalidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	db := newMockDB(tmpDir)

	// Missing storage
	cfg1 := SchedulerConfig{
		Interval: 1 * time.Hour,
		Prefix:   "test-snapshots/",
	}
	sched1 := NewScheduler(cfg1, db)
	if err := sched1.Start(); err == nil {
		t.Error("expected Start to fail without storage")
		sched1.Stop()
	}

	// Invalid interval
	cfg2 := SchedulerConfig{
		Interval: 0,
		Storage:  newMockStorage(),
		Prefix:   "test-snapshots/",
	}
	sched2 := NewScheduler(cfg2, db)
	if err := sched2.Start(); err == nil {
		t.Error("expected Start to fail with zero interval")
		sched2.Stop()
	}
}
