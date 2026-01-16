// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/pebble/cobbleext/snapshot"
	"github.com/spf13/cobra"
)

var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "Manage the snapshot catalog",
	Long: `Commands for viewing and managing the snapshot catalog.

The catalog tracks all snapshots and blob reference counts.`,
}

var catalogShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show the current catalog",
	Long:  `Display the full catalog including all snapshots and blob reference counts.`,
	RunE:  runCatalogShow,
}

var catalogVerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify catalog integrity",
	Long: `Verify the catalog is consistent and all referenced blobs exist.

This command checks:
  - All snapshots have valid manifests
  - All referenced blobs exist in storage
  - Blob reference counts are accurate`,
	RunE: runCatalogVerify,
}

var catalogExportCmd = &cobra.Command{
	Use:   "export [file]",
	Short: "Export catalog to a file",
	Long:  `Export the full catalog to a JSON file for backup or analysis.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runCatalogExport,
}

var (
	catalogJSON    bool
	catalogVerbose bool
)

func init() {
	rootCmd.AddCommand(catalogCmd)

	catalogCmd.AddCommand(catalogShowCmd)
	catalogCmd.AddCommand(catalogVerifyCmd)
	catalogCmd.AddCommand(catalogExportCmd)

	catalogShowCmd.Flags().BoolVar(&catalogJSON, "json", false, "Output in JSON format")
	catalogShowCmd.Flags().BoolVarP(&catalogVerbose, "verbose", "v", false, "Show detailed information")
	catalogVerifyCmd.Flags().BoolVar(&catalogVerbose, "verbose", false, "Show detailed verification results")
}

func runCatalogShow(cmd *cobra.Command, args []string) error {
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

	if catalogJSON {
		output, _ := json.MarshalIndent(catalog, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("Catalog (updated: %s)\n", catalog.UpdatedAt.Format(time.RFC3339))
	fmt.Printf("  Snapshots: %d\n", len(catalog.Snapshots))
	fmt.Printf("  Unique Blobs: %d\n\n", len(catalog.BlobRefCounts))

	if len(catalog.Snapshots) > 0 {
		fmt.Println("Snapshots:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  ID\tCREATED\tSIZE\tBLOBS\tPARENT\tBRANCH")
		for _, snap := range catalog.Snapshots {
			parent := "-"
			if snap.ParentID != "" {
				parent = snap.ParentID
			}
			branch := "-"
			if snap.BranchName != "" {
				branch = snap.BranchName
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%d\t%s\t%s\n",
				snap.ID,
				snap.CreatedAt.Format("2006-01-02 15:04"),
				formatBytes(snap.TotalSize),
				len(snap.Blobs),
				parent,
				branch,
			)
		}
		w.Flush()
	}

	if catalogVerbose && len(catalog.BlobRefCounts) > 0 {
		fmt.Println("\nBlob Reference Counts:")
		for hash, count := range catalog.BlobRefCounts {
			fmt.Printf("  %s: %d\n", hash[:16]+"...", count)
		}
	}

	return nil
}

func runCatalogVerify(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	fmt.Println("Verifying catalog integrity...")

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	var errors []string
	var warnings []string

	// Check 1: Verify all snapshots have valid manifests
	fmt.Printf("  Checking %d snapshots...\n", len(catalog.Snapshots))
	for _, snap := range catalog.Snapshots {
		manifest, err := snapshot.LoadCASManifest(ctx, storage, prefix, snap.ID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Snapshot %s: failed to load manifest: %v", snap.ID, err))
			continue
		}
		if manifest.ID != snap.ID {
			errors = append(errors, fmt.Sprintf("Snapshot %s: manifest ID mismatch", snap.ID))
		}
		if catalogVerbose {
			fmt.Printf("    ✓ %s (%d blobs)\n", snap.ID, len(manifest.Blobs))
		}
	}

	// Check 2: Verify all referenced blobs exist
	fmt.Printf("  Checking %d blobs...\n", len(catalog.BlobRefCounts))
	missingBlobs := 0
	for hash := range catalog.BlobRefCounts {
		blobPath := prefix + "blobs/" + hash[:2] + "/" + hash
		_, err := storage.Size(blobPath)
		if err != nil {
			if storage.IsNotExistError(err) {
				missingBlobs++
				errors = append(errors, fmt.Sprintf("Blob %s: missing from storage", hash[:16]+"..."))
			} else {
				warnings = append(warnings, fmt.Sprintf("Blob %s: error checking: %v", hash[:16]+"...", err))
			}
		}
	}

	// Check 3: Verify reference counts are accurate
	fmt.Println("  Verifying reference counts...")
	computedRefs := make(map[string]int)
	for _, snap := range catalog.Snapshots {
		manifest, err := snapshot.LoadCASManifest(ctx, storage, prefix, snap.ID)
		if err != nil {
			continue
		}
		for _, blob := range manifest.Blobs {
			computedRefs[blob.Hash]++
		}
	}

	for hash, count := range catalog.BlobRefCounts {
		if computed, ok := computedRefs[hash]; ok {
			if computed != count {
				warnings = append(warnings, fmt.Sprintf("Blob %s: ref count mismatch (catalog=%d, computed=%d)", hash[:16]+"...", count, computed))
			}
		} else {
			warnings = append(warnings, fmt.Sprintf("Blob %s: orphaned (ref count=%d but not referenced)", hash[:16]+"...", count))
		}
	}

	for hash := range computedRefs {
		if _, ok := catalog.BlobRefCounts[hash]; !ok {
			warnings = append(warnings, fmt.Sprintf("Blob %s: missing from ref counts", hash[:16]+"..."))
		}
	}

	// Summary
	fmt.Println()
	if len(errors) == 0 && len(warnings) == 0 {
		fmt.Println("✓ Catalog verification passed!")
		return nil
	}

	if len(errors) > 0 {
		fmt.Printf("✗ Found %d errors:\n", len(errors))
		for _, e := range errors {
			fmt.Printf("  - %s\n", e)
		}
	}

	if len(warnings) > 0 {
		fmt.Printf("⚠ Found %d warnings:\n", len(warnings))
		for _, w := range warnings {
			fmt.Printf("  - %s\n", w)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("catalog verification failed with %d errors", len(errors))
	}

	return nil
}

func runCatalogExport(cmd *cobra.Command, args []string) error {
	outputFile := args[0]

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

	output, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	if err := os.WriteFile(outputFile, output, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Catalog exported to %s\n", outputFile)
	fmt.Printf("  Snapshots: %d\n", len(catalog.Snapshots))
	fmt.Printf("  Blobs: %d\n", len(catalog.BlobRefCounts))

	return nil
}
