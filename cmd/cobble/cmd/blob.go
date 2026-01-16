// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/pebble/cobbleext/snapshot"
	"github.com/spf13/cobra"
)

var blobCmd = &cobra.Command{
	Use:   "blob",
	Short: "Manage content-addressed blobs",
	Long: `Commands for viewing and managing content-addressed blobs.

Blobs are stored by their SHA256 hash and are shared across snapshots
for deduplication.`,
}

var blobListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all blobs",
	Long:  `List all blobs in storage with their reference counts.`,
	RunE:  runBlobList,
}

var blobInfoCmd = &cobra.Command{
	Use:   "info [hash]",
	Short: "Show blob information",
	Long:  `Show detailed information about a specific blob including which snapshots reference it.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runBlobInfo,
}

var blobVerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify blob integrity",
	Long: `Verify that all blobs match their content hashes.

This downloads each blob and verifies its SHA256 hash matches its name.`,
	RunE: runBlobVerify,
}

var blobOrphansCmd = &cobra.Command{
	Use:   "orphans",
	Short: "List orphaned blobs",
	Long: `List blobs that exist in storage but are not referenced by any snapshot.

These can be cleaned up using 'cobble snapshot gc'.`,
	RunE: runBlobOrphans,
}

var (
	blobJSON    bool
	blobVerbose bool
	blobLimit   int
)

func init() {
	rootCmd.AddCommand(blobCmd)

	blobCmd.AddCommand(blobListCmd)
	blobCmd.AddCommand(blobInfoCmd)
	blobCmd.AddCommand(blobVerifyCmd)
	blobCmd.AddCommand(blobOrphansCmd)

	blobListCmd.Flags().BoolVar(&blobJSON, "json", false, "Output in JSON format")
	blobListCmd.Flags().IntVar(&blobLimit, "limit", 50, "Maximum number of blobs to show")

	blobInfoCmd.Flags().BoolVar(&blobJSON, "json", false, "Output in JSON format")

	blobVerifyCmd.Flags().BoolVarP(&blobVerbose, "verbose", "v", false, "Show progress for each blob")
	blobVerifyCmd.Flags().IntVar(&blobLimit, "limit", 0, "Maximum number of blobs to verify (0 = all)")

	blobOrphansCmd.Flags().BoolVar(&blobJSON, "json", false, "Output in JSON format")
}

func runBlobList(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Collect blob info
	type blobInfo struct {
		Hash     string `json:"hash"`
		RefCount int    `json:"ref_count"`
		Size     int64  `json:"size,omitempty"`
	}

	blobs := make([]blobInfo, 0, len(catalog.BlobRefCounts))
	for hash, count := range catalog.BlobRefCounts {
		blobs = append(blobs, blobInfo{Hash: hash, RefCount: count})
	}

	// Sort by ref count (descending)
	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].RefCount > blobs[j].RefCount
	})

	// Apply limit
	if blobLimit > 0 && len(blobs) > blobLimit {
		blobs = blobs[:blobLimit]
	}

	if blobJSON {
		output, _ := json.MarshalIndent(blobs, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("Blobs (showing %d of %d):\n\n", len(blobs), len(catalog.BlobRefCounts))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "HASH\tREFS")
	for _, b := range blobs {
		fmt.Fprintf(w, "%s\t%d\n", b.Hash[:16]+"...", b.RefCount)
	}
	w.Flush()

	return nil
}

func runBlobInfo(cmd *cobra.Command, args []string) error {
	hash := args[0]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Find the blob
	refCount, exists := catalog.BlobRefCounts[hash]
	if !exists {
		// Try partial match
		for h, c := range catalog.BlobRefCounts {
			if len(hash) < len(h) && h[:len(hash)] == hash {
				hash = h
				refCount = c
				exists = true
				break
			}
		}
	}

	if !exists {
		return fmt.Errorf("blob not found: %s", hash)
	}

	// Get blob size
	blobPath := prefix + "blobs/" + hash[:2] + "/" + hash
	size, err := storage.Size(blobPath)
	if err != nil {
		return fmt.Errorf("failed to get blob size: %w", err)
	}

	// Find referencing snapshots
	var referencingSnapshots []string
	for _, snap := range catalog.Snapshots {
		manifest, err := snapshot.LoadCASManifest(ctx, storage, prefix, snap.ID)
		if err != nil {
			continue
		}
		for _, blob := range manifest.Blobs {
			if blob.Hash == hash {
				referencingSnapshots = append(referencingSnapshots, snap.ID)
				break
			}
		}
	}

	info := struct {
		Hash       string   `json:"hash"`
		Size       int64    `json:"size"`
		RefCount   int      `json:"ref_count"`
		Path       string   `json:"path"`
		Snapshots  []string `json:"snapshots"`
	}{
		Hash:      hash,
		Size:      size,
		RefCount:  refCount,
		Path:      blobPath,
		Snapshots: referencingSnapshots,
	}

	if blobJSON {
		output, _ := json.MarshalIndent(info, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("Blob: %s\n", hash)
	fmt.Printf("  Size:       %s\n", formatBytes(size))
	fmt.Printf("  Ref Count:  %d\n", refCount)
	fmt.Printf("  Path:       %s\n", blobPath)
	fmt.Printf("  Snapshots:  %d\n", len(referencingSnapshots))
	for _, s := range referencingSnapshots {
		fmt.Printf("    - %s\n", s)
	}

	return nil
}

func runBlobVerify(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	hashes := make([]string, 0, len(catalog.BlobRefCounts))
	for hash := range catalog.BlobRefCounts {
		hashes = append(hashes, hash)
	}

	if blobLimit > 0 && len(hashes) > blobLimit {
		hashes = hashes[:blobLimit]
	}

	fmt.Printf("Verifying %d blobs...\n", len(hashes))

	verified := 0
	corrupted := 0
	missing := 0

	for i, hash := range hashes {
		blobPath := prefix + "blobs/" + hash[:2] + "/" + hash

		if blobVerbose {
			fmt.Printf("  [%d/%d] %s...", i+1, len(hashes), hash[:16])
		}

		reader, size, err := storage.ReadObject(ctx, blobPath)
		if err != nil {
			if storage.IsNotExistError(err) {
				missing++
				if blobVerbose {
					fmt.Println(" MISSING")
				}
			} else {
				if blobVerbose {
					fmt.Printf(" ERROR: %v\n", err)
				}
			}
			continue
		}

		// Read and hash the content
		hasher := sha256.New()
		data := make([]byte, size)
		if err := reader.ReadAt(ctx, data, 0); err != nil {
			reader.Close()
			if blobVerbose {
				fmt.Printf(" READ ERROR: %v\n", err)
			}
			continue
		}
		reader.Close()

		hasher.Write(data)
		computedHash := hex.EncodeToString(hasher.Sum(nil))

		if computedHash == hash {
			verified++
			if blobVerbose {
				fmt.Println(" OK")
			}
		} else {
			corrupted++
			if blobVerbose {
				fmt.Println(" CORRUPTED")
			}
		}
	}

	fmt.Println()
	fmt.Printf("Verification complete:\n")
	fmt.Printf("  Verified:  %d\n", verified)
	fmt.Printf("  Corrupted: %d\n", corrupted)
	fmt.Printf("  Missing:   %d\n", missing)

	if corrupted > 0 || missing > 0 {
		return fmt.Errorf("found %d corrupted and %d missing blobs", corrupted, missing)
	}

	return nil
}

func runBlobOrphans(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// List all blobs in storage
	blobPrefix := prefix + "blobs/"
	allBlobs, err := storage.List(blobPrefix, "")
	if err != nil {
		return fmt.Errorf("failed to list blobs: %w", err)
	}

	// Find orphans (blobs not in catalog)
	orphans := make([]string, 0)
	for _, blobPath := range allBlobs {
		// Extract hash from path (format: blobs/xx/xxxx...)
		parts := splitPath(blobPath)
		if len(parts) < 2 {
			continue
		}
		hash := parts[len(parts)-1]

		if _, exists := catalog.BlobRefCounts[hash]; !exists {
			orphans = append(orphans, hash)
		}
	}

	if blobJSON {
		output, _ := json.MarshalIndent(orphans, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	if len(orphans) == 0 {
		fmt.Println("No orphaned blobs found")
		return nil
	}

	fmt.Printf("Found %d orphaned blobs:\n", len(orphans))
	for _, hash := range orphans {
		fmt.Printf("  %s\n", hash[:16]+"...")
	}
	fmt.Println("\nRun 'cobble snapshot gc' to clean up orphaned blobs")

	return nil
}

func splitPath(path string) []string {
	var parts []string
	current := ""
	for _, c := range path {
		if c == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}
