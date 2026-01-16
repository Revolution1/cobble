// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

const snapshotSetFileName = "snapshot-set.json"

// SnapshotSet represents a coordinated snapshot across multiple databases.
// This is useful for applications like geth that use multiple Pebble databases
// (e.g., chaindata, ancient, state) that need to be snapshotted together
// to maintain consistency.
type SnapshotSet struct {
	// ID is the unique identifier for this snapshot set.
	ID string `json:"id"`

	// CreatedAt is when the snapshot set was created.
	CreatedAt time.Time `json:"created_at"`

	// Databases maps database names to their snapshot IDs.
	// Example: {"chaindata": "20240115T120000Z", "ancient": "20240115T120001Z"}
	Databases map[string]string `json:"databases"`

	// Description is an optional description of this snapshot set.
	Description string `json:"description,omitempty"`

	// Labels are optional key-value pairs for filtering/organizing.
	Labels map[string]string `json:"labels,omitempty"`
}

// SnapshotSetCatalog tracks all snapshot sets.
type SnapshotSetCatalog struct {
	// SnapshotSets is the list of all snapshot sets, newest first.
	SnapshotSets []SnapshotSet `json:"snapshot_sets"`

	// UpdatedAt is when the catalog was last updated.
	UpdatedAt time.Time `json:"updated_at"`
}

// LoadSnapshotSetCatalog loads the snapshot set catalog from remote storage.
func LoadSnapshotSetCatalog(ctx context.Context, storage remote.Storage, prefix string) (*SnapshotSetCatalog, error) {
	catalogPath := path.Join(prefix, snapshotSetFileName)

	size, err := storage.Size(catalogPath)
	if err != nil {
		if storage.IsNotExistError(err) {
			return &SnapshotSetCatalog{
				SnapshotSets: []SnapshotSet{},
				UpdatedAt:    time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to get snapshot set catalog size: %w", err)
	}

	reader, objSize, err := storage.ReadObject(ctx, catalogPath)
	if err != nil {
		if storage.IsNotExistError(err) {
			return &SnapshotSetCatalog{
				SnapshotSets: []SnapshotSet{},
				UpdatedAt:    time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to read snapshot set catalog: %w", err)
	}
	defer reader.Close()

	if objSize > 0 {
		size = objSize
	}

	data := make([]byte, size)
	if err := reader.ReadAt(ctx, data, 0); err != nil {
		return nil, fmt.Errorf("failed to read snapshot set catalog data: %w", err)
	}

	var catalog SnapshotSetCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot set catalog: %w", err)
	}

	return &catalog, nil
}

// SaveSnapshotSetCatalog saves the snapshot set catalog to remote storage.
func SaveSnapshotSetCatalog(ctx context.Context, storage remote.Storage, prefix string, catalog *SnapshotSetCatalog) error {
	catalog.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot set catalog: %w", err)
	}

	catalogPath := path.Join(prefix, snapshotSetFileName)
	writer, err := storage.CreateObject(catalogPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot set catalog: %w", err)
	}

	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write snapshot set catalog: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot set catalog writer: %w", err)
	}

	return nil
}

// AddSnapshotSet adds a snapshot set to the catalog.
func (c *SnapshotSetCatalog) AddSnapshotSet(set SnapshotSet) {
	c.SnapshotSets = append([]SnapshotSet{set}, c.SnapshotSets...)
}

// RemoveSnapshotSet removes a snapshot set from the catalog.
func (c *SnapshotSetCatalog) RemoveSnapshotSet(setID string) bool {
	for i, s := range c.SnapshotSets {
		if s.ID == setID {
			c.SnapshotSets = append(c.SnapshotSets[:i], c.SnapshotSets[i+1:]...)
			return true
		}
	}
	return false
}

// GetSnapshotSet returns a snapshot set by ID.
func (c *SnapshotSetCatalog) GetSnapshotSet(setID string) (*SnapshotSet, bool) {
	for _, s := range c.SnapshotSets {
		if s.ID == setID {
			return &s, true
		}
	}
	return nil, false
}

// GetLatestSnapshotSet returns the most recent snapshot set.
func (c *SnapshotSetCatalog) GetLatestSnapshotSet() (*SnapshotSet, bool) {
	if len(c.SnapshotSets) == 0 {
		return nil, false
	}
	return &c.SnapshotSets[0], true
}

// SnapshotSetOptions configures snapshot set creation.
type SnapshotSetOptions struct {
	// Storage is the remote storage for the snapshot set catalog.
	Storage remote.Storage

	// Prefix is the path prefix in the bucket.
	Prefix string

	// SetID is an optional custom set ID. If empty, generates a timestamp-based ID.
	SetID string

	// Description is an optional description.
	Description string

	// Labels are optional key-value pairs.
	Labels map[string]string
}

// CreateSnapshotSet creates a coordinated snapshot set from multiple database snapshots.
// The databases map should contain the database name to DBAdapter mapping.
// Each database will be snapshotted with its own prefix (prefix/dbname/).
func CreateSnapshotSet(ctx context.Context, databases map[string]DBAdapter, opts SnapshotSetOptions) (*SnapshotSet, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	if len(databases) == 0 {
		return nil, fmt.Errorf("at least one database is required")
	}

	setID := opts.SetID
	if setID == "" {
		setID = time.Now().UTC().Format("20060102T150405Z")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "snapshots/"
	}

	// Create snapshot for each database
	dbSnapshots := make(map[string]string)
	for dbName, db := range databases {
		dbPrefix := path.Join(prefix, dbName) + "/"

		snapOpts := SnapshotOptions{
			Storage:  opts.Storage,
			Prefix:   dbPrefix,
			FlushWAL: true,
		}

		result, err := CreateSnapshot(ctx, db, snapOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create snapshot for %s: %w", dbName, err)
		}

		dbSnapshots[dbName] = result.SnapshotID
	}

	// Create the snapshot set
	set := SnapshotSet{
		ID:          setID,
		CreatedAt:   time.Now().UTC(),
		Databases:   dbSnapshots,
		Description: opts.Description,
		Labels:      opts.Labels,
	}

	// Load and update the catalog
	catalog, err := LoadSnapshotSetCatalog(ctx, opts.Storage, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to load snapshot set catalog: %w", err)
	}

	catalog.AddSnapshotSet(set)

	if err := SaveSnapshotSetCatalog(ctx, opts.Storage, prefix, catalog); err != nil {
		return nil, fmt.Errorf("failed to save snapshot set catalog: %w", err)
	}

	return &set, nil
}

// RestoreSnapshotSet restores all databases from a snapshot set.
// The destDirs map should contain the database name to destination directory mapping.
func RestoreSnapshotSet(ctx context.Context, storage remote.Storage, prefix, setID string, destDirs map[string]string) error {
	if prefix == "" {
		prefix = "snapshots/"
	}

	// Load catalog
	catalog, err := LoadSnapshotSetCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load snapshot set catalog: %w", err)
	}

	// Find the snapshot set
	set, ok := catalog.GetSnapshotSet(setID)
	if !ok {
		return fmt.Errorf("snapshot set %s not found", setID)
	}

	// Restore each database
	for dbName, snapshotID := range set.Databases {
		destDir, ok := destDirs[dbName]
		if !ok {
			return fmt.Errorf("no destination directory specified for database %s", dbName)
		}

		dbPrefix := path.Join(prefix, dbName) + "/"

		restoreOpts := RestoreOptions{
			Storage:         storage,
			Prefix:          dbPrefix,
			SnapshotID:      snapshotID,
			VerifyChecksums: true,
		}

		if err := RestoreSnapshot(ctx, destDir, restoreOpts); err != nil {
			return fmt.Errorf("failed to restore %s: %w", dbName, err)
		}
	}

	return nil
}

// ListSnapshotSets returns all snapshot sets.
func ListSnapshotSets(ctx context.Context, storage remote.Storage, prefix string) ([]SnapshotSet, error) {
	catalog, err := LoadSnapshotSetCatalog(ctx, storage, prefix)
	if err != nil {
		return nil, err
	}
	return catalog.SnapshotSets, nil
}

// DeleteSnapshotSet deletes a snapshot set and optionally its individual snapshots.
func DeleteSnapshotSet(ctx context.Context, storage remote.Storage, prefix, setID string, deleteSnapshots bool) error {
	if prefix == "" {
		prefix = "snapshots/"
	}

	// Load catalog
	catalog, err := LoadSnapshotSetCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load snapshot set catalog: %w", err)
	}

	// Find the snapshot set
	set, ok := catalog.GetSnapshotSet(setID)
	if !ok {
		return fmt.Errorf("snapshot set %s not found", setID)
	}

	// Optionally delete individual snapshots
	if deleteSnapshots {
		for dbName, snapshotID := range set.Databases {
			dbPrefix := path.Join(prefix, dbName) + "/"
			if err := DeleteSnapshot(ctx, storage, dbPrefix, snapshotID); err != nil {
				// Log but don't fail on individual snapshot deletion
				fmt.Printf("Warning: failed to delete snapshot %s for %s: %v\n", snapshotID, dbName, err)
			}
		}
	}

	// Remove from catalog
	catalog.RemoveSnapshotSet(setID)

	if err := SaveSnapshotSetCatalog(ctx, storage, prefix, catalog); err != nil {
		return fmt.Errorf("failed to save snapshot set catalog: %w", err)
	}

	return nil
}
