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
	"sort"
	"time"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

const (
	catalogFileName  = "catalog.json"
	manifestFileName = "manifest.json"
)

// LoadCatalog loads the snapshot catalog from remote storage.
func LoadCatalog(ctx context.Context, storage remote.Storage, prefix string) (*Catalog, error) {
	catalogPath := path.Join(prefix, catalogFileName)

	size, err := storage.Size(catalogPath)
	if err != nil {
		if storage.IsNotExistError(err) {
			return &Catalog{
				Snapshots: []SnapshotInfo{},
				UpdatedAt: time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to get catalog size: %w", err)
	}

	objReader, objSize, err := storage.ReadObject(ctx, catalogPath)
	if err != nil {
		if storage.IsNotExistError(err) {
			return &Catalog{
				Snapshots: []SnapshotInfo{},
				UpdatedAt: time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to read catalog: %w", err)
	}
	defer objReader.Close()

	// Use the actual size from ReadObject if available
	if objSize > 0 {
		size = objSize
	}

	data := make([]byte, size)
	if err := objReader.ReadAt(ctx, data, 0); err != nil {
		return nil, fmt.Errorf("failed to read catalog data: %w", err)
	}

	var catalog Catalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("failed to parse catalog: %w", err)
	}

	return &catalog, nil
}

// SaveCatalog saves the snapshot catalog to remote storage.
func SaveCatalog(ctx context.Context, storage remote.Storage, prefix string, catalog *Catalog) error {
	catalog.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	catalogPath := path.Join(prefix, catalogFileName)
	writer, err := storage.CreateObject(catalogPath)
	if err != nil {
		return fmt.Errorf("failed to create catalog: %w", err)
	}

	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write catalog: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close catalog writer: %w", err)
	}

	return nil
}

// AddSnapshot adds a snapshot to the catalog.
func (c *Catalog) AddSnapshot(info SnapshotInfo) {
	c.Snapshots = append([]SnapshotInfo{info}, c.Snapshots...)
}

// RemoveSnapshot removes a snapshot from the catalog.
func (c *Catalog) RemoveSnapshot(snapshotID string) bool {
	for i, s := range c.Snapshots {
		if s.ID == snapshotID {
			c.Snapshots = append(c.Snapshots[:i], c.Snapshots[i+1:]...)
			return true
		}
	}
	return false
}

// GetSnapshot returns a snapshot by ID.
func (c *Catalog) GetSnapshot(snapshotID string) (*SnapshotInfo, bool) {
	for _, s := range c.Snapshots {
		if s.ID == snapshotID {
			return &s, true
		}
	}
	return nil, false
}

// GetLatestSnapshot returns the most recent snapshot.
func (c *Catalog) GetLatestSnapshot() (*SnapshotInfo, bool) {
	if len(c.Snapshots) == 0 {
		return nil, false
	}
	return &c.Snapshots[0], true
}

// GetLatestFullSnapshot returns the most recent non-incremental snapshot.
func (c *Catalog) GetLatestFullSnapshot() (*SnapshotInfo, bool) {
	for _, s := range c.Snapshots {
		if !s.Incremental {
			return &s, true
		}
	}
	return nil, false
}

// GetSnapshotChain returns the chain of snapshots needed to restore a snapshot.
// For a full snapshot, returns just that snapshot.
// For an incremental snapshot, returns [base, ..., parent, snapshot].
func (c *Catalog) GetSnapshotChain(snapshotID string) ([]SnapshotInfo, error) {
	snapshot, ok := c.GetSnapshot(snapshotID)
	if !ok {
		return nil, fmt.Errorf("snapshot %s not found", snapshotID)
	}

	if !snapshot.Incremental {
		return []SnapshotInfo{*snapshot}, nil
	}

	chain := []SnapshotInfo{*snapshot}
	current := snapshot

	for current.Incremental && current.ParentSnapshotID != "" {
		parent, ok := c.GetSnapshot(current.ParentSnapshotID)
		if !ok {
			return nil, fmt.Errorf("parent snapshot %s not found for %s",
				current.ParentSnapshotID, current.ID)
		}
		chain = append(chain, *parent)
		current = parent
	}

	// Reverse to get [base, ..., parent, snapshot]
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}

	return chain, nil
}

// HasDependents returns true if other snapshots depend on this one.
func (c *Catalog) HasDependents(snapshotID string) bool {
	for _, s := range c.Snapshots {
		if s.ParentSnapshotID == snapshotID || s.BaseSnapshotID == snapshotID {
			return true
		}
	}
	return false
}

// ListSnapshots returns all snapshots, optionally sorted.
func ListSnapshots(ctx context.Context, storage remote.Storage, prefix string) ([]SnapshotInfo, error) {
	catalog, err := LoadCatalog(ctx, storage, prefix)
	if err != nil {
		return nil, err
	}

	// Sort by creation time, newest first
	sort.Slice(catalog.Snapshots, func(i, j int) bool {
		return catalog.Snapshots[i].CreatedAt.After(catalog.Snapshots[j].CreatedAt)
	})

	return catalog.Snapshots, nil
}

// LoadManifest loads the manifest for a specific snapshot.
func LoadManifest(ctx context.Context, storage remote.Storage, prefix, snapshotID string) (*SnapshotManifest, error) {
	manifestPath := path.Join(prefix, snapshotID, manifestFileName)

	size, err := storage.Size(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest size: %w", err)
	}

	reader, objSize, err := storage.ReadObject(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}
	defer reader.Close()

	// Use actual size from ReadObject if available
	if objSize > 0 {
		size = objSize
	}

	data := make([]byte, size)
	if err := reader.ReadAt(ctx, data, 0); err != nil {
		return nil, fmt.Errorf("failed to read manifest data: %w", err)
	}

	var manifest SnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	return &manifest, nil
}

// SaveManifest saves the manifest for a snapshot.
func SaveManifest(ctx context.Context, storage remote.Storage, prefix string, manifest *SnapshotManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	manifestPath := path.Join(prefix, manifest.ID, manifestFileName)
	writer, err := storage.CreateObject(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to create manifest: %w", err)
	}

	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close manifest writer: %w", err)
	}

	return nil
}
