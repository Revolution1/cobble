// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// =============================================================================
// Distributed Lock using Object Storage
//
// This provides a simple distributed lock mechanism using object storage.
// It's suitable for snapshot operations where:
// - Operations take seconds to minutes (lock latency is acceptable)
// - Frequency is low (a few times per hour at most)
// - Strong consistency is needed for catalog updates
//
// The lock uses a lease-based approach with automatic expiration.
// =============================================================================

var (
	// ErrLockHeld is returned when trying to acquire a lock that's already held.
	ErrLockHeld = errors.New("lock is held by another process")

	// ErrLockNotHeld is returned when trying to release a lock not held by us.
	ErrLockNotHeld = errors.New("lock is not held by this process")

	// DefaultLockTTL is the default time-to-live for locks.
	DefaultLockTTL = 5 * time.Minute

	// DefaultLockRetryInterval is how often to retry acquiring a lock.
	DefaultLockRetryInterval = 1 * time.Second
)

// LockInfo contains information about a lock.
type LockInfo struct {
	// HolderID identifies who holds the lock.
	HolderID string `json:"holder_id"`

	// AcquiredAt is when the lock was acquired.
	AcquiredAt time.Time `json:"acquired_at"`

	// ExpiresAt is when the lock expires.
	ExpiresAt time.Time `json:"expires_at"`

	// Purpose describes why the lock was acquired.
	Purpose string `json:"purpose,omitempty"`
}

// IsExpired returns true if the lock has expired.
func (l *LockInfo) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

// DistributedLock provides distributed locking using object storage.
type DistributedLock struct {
	storage  remote.Storage
	lockPath string
	holderID string
	ttl      time.Duration
}

// LockOptions configures lock behavior.
type LockOptions struct {
	// TTL is how long the lock is valid. Default is 5 minutes.
	TTL time.Duration

	// RetryInterval is how often to retry when lock is held. Default is 1 second.
	RetryInterval time.Duration

	// MaxRetries is the maximum number of retries. 0 means no retries.
	MaxRetries int

	// Purpose describes why the lock is being acquired.
	Purpose string
}

// NewDistributedLock creates a new distributed lock.
func NewDistributedLock(storage remote.Storage, prefix, lockName string) *DistributedLock {
	// Generate a unique holder ID
	hostname, _ := os.Hostname()
	holderID := fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())

	return &DistributedLock{
		storage:  storage,
		lockPath: path.Join(prefix, ".locks", lockName+".lock"),
		holderID: holderID,
		ttl:      DefaultLockTTL,
	}
}

// Acquire attempts to acquire the lock.
func (l *DistributedLock) Acquire(ctx context.Context, opts LockOptions) error {
	if opts.TTL > 0 {
		l.ttl = opts.TTL
	}

	retryInterval := opts.RetryInterval
	if retryInterval <= 0 {
		retryInterval = DefaultLockRetryInterval
	}

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		err := l.tryAcquire(ctx, opts.Purpose)
		if err == nil {
			return nil
		}

		if !errors.Is(err, ErrLockHeld) {
			return err
		}

		if attempt < opts.MaxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
				continue
			}
		}
	}

	return ErrLockHeld
}

// tryAcquire makes a single attempt to acquire the lock.
func (l *DistributedLock) tryAcquire(ctx context.Context, purpose string) error {
	// Check if lock exists and is still valid
	existingLock, err := l.readLock(ctx)
	if err == nil && !existingLock.IsExpired() {
		// Lock is held by someone else
		if existingLock.HolderID != l.holderID {
			return ErrLockHeld
		}
		// We already hold the lock, refresh it
	}

	// Create or update lock
	lockInfo := LockInfo{
		HolderID:   l.holderID,
		AcquiredAt: time.Now(),
		ExpiresAt:  time.Now().Add(l.ttl),
		Purpose:    purpose,
	}

	return l.writeLock(ctx, lockInfo)
}

// Release releases the lock.
func (l *DistributedLock) Release(ctx context.Context) error {
	existingLock, err := l.readLock(ctx)
	if err != nil {
		if l.storage.IsNotExistError(err) {
			return nil // Lock doesn't exist, nothing to release
		}
		return err
	}

	if existingLock.HolderID != l.holderID {
		return ErrLockNotHeld
	}

	return l.storage.Delete(l.lockPath)
}

// Refresh extends the lock's TTL.
func (l *DistributedLock) Refresh(ctx context.Context) error {
	existingLock, err := l.readLock(ctx)
	if err != nil {
		return fmt.Errorf("failed to read lock: %w", err)
	}

	if existingLock.HolderID != l.holderID {
		return ErrLockNotHeld
	}

	existingLock.ExpiresAt = time.Now().Add(l.ttl)
	return l.writeLock(ctx, *existingLock)
}

// ForceRelease releases the lock regardless of who holds it.
// Use with caution - only for administrative cleanup.
func (l *DistributedLock) ForceRelease(ctx context.Context) error {
	return l.storage.Delete(l.lockPath)
}

// GetInfo returns information about the current lock holder.
func (l *DistributedLock) GetInfo(ctx context.Context) (*LockInfo, error) {
	return l.readLock(ctx)
}

// IsHeldByUs returns true if we currently hold the lock.
func (l *DistributedLock) IsHeldByUs(ctx context.Context) bool {
	info, err := l.readLock(ctx)
	if err != nil {
		return false
	}
	return info.HolderID == l.holderID && !info.IsExpired()
}

func (l *DistributedLock) readLock(ctx context.Context) (*LockInfo, error) {
	size, err := l.storage.Size(l.lockPath)
	if err != nil {
		return nil, err
	}

	reader, objSize, err := l.storage.ReadObject(ctx, l.lockPath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	if objSize > 0 {
		size = objSize
	}

	data := make([]byte, size)
	if err := reader.ReadAt(ctx, data, 0); err != nil {
		return nil, err
	}

	var info LockInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

func (l *DistributedLock) writeLock(ctx context.Context, info LockInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}

	writer, err := l.storage.CreateObject(l.lockPath)
	if err != nil {
		return err
	}

	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		writer.Close()
		return err
	}

	return writer.Close()
}

// =============================================================================
// Locking Wrapper for CAS Operations
// =============================================================================

// LockedCASOptions adds locking to CAS snapshot options.
type LockedCASOptions struct {
	CASSnapshotOptions

	// LockTTL is the lock timeout. Default is 5 minutes.
	LockTTL time.Duration

	// LockRetryInterval is how often to retry when lock is held.
	LockRetryInterval time.Duration

	// LockMaxRetries is the maximum retries for acquiring lock.
	LockMaxRetries int
}

// CreateCASSnapshotWithLock creates a CAS snapshot with distributed locking.
// This ensures only one process can update the catalog at a time.
func CreateCASSnapshotWithLock(ctx context.Context, db DBAdapter, opts LockedCASOptions) (*CASManifest, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "cas/"
	}

	// Acquire lock
	lock := NewDistributedLock(opts.Storage, prefix, "catalog")

	lockOpts := LockOptions{
		TTL:           opts.LockTTL,
		RetryInterval: opts.LockRetryInterval,
		MaxRetries:    opts.LockMaxRetries,
		Purpose:       fmt.Sprintf("create snapshot %s", opts.SnapshotID),
	}

	if err := lock.Acquire(ctx, lockOpts); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer lock.Release(ctx)

	// Create snapshot while holding lock
	return CreateCASSnapshot(ctx, db, opts.CASSnapshotOptions)
}

// DeleteCASSnapshotWithLock deletes a CAS snapshot with distributed locking.
func DeleteCASSnapshotWithLock(ctx context.Context, storage remote.Storage, prefix, snapshotID string, lockOpts LockOptions) error {
	if prefix == "" {
		prefix = "cas/"
	}

	lock := NewDistributedLock(storage, prefix, "catalog")
	lockOpts.Purpose = fmt.Sprintf("delete snapshot %s", snapshotID)

	if err := lock.Acquire(ctx, lockOpts); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer lock.Release(ctx)

	return DeleteCASSnapshot(ctx, storage, prefix, snapshotID)
}

// GarbageCollectCASWithLock runs garbage collection with distributed locking.
func GarbageCollectCASWithLock(ctx context.Context, opts CASGCOptions, lockOpts LockOptions) ([]string, []string, error) {
	if opts.Storage == nil {
		return nil, nil, fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "cas/"
	}

	lock := NewDistributedLock(opts.Storage, prefix, "catalog")
	lockOpts.Purpose = "garbage collection"

	if err := lock.Acquire(ctx, lockOpts); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer lock.Release(ctx)

	return GarbageCollectCAS(ctx, opts)
}

// CompactCASSnapshotWithLock compacts a snapshot with distributed locking.
func CompactCASSnapshotWithLock(ctx context.Context, opts CompactCASOptions, lockOpts LockOptions) (*CASManifest, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "cas/"
	}

	lock := NewDistributedLock(opts.Storage, prefix, "catalog")
	lockOpts.Purpose = fmt.Sprintf("compact snapshot %s", opts.SnapshotID)

	if err := lock.Acquire(ctx, lockOpts); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer lock.Release(ctx)

	return CompactCASSnapshot(ctx, opts)
}
