// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// uploader handles parallel file uploads to remote storage.
type uploader struct {
	storage     remote.Storage
	prefix      string
	snapshotID  string
	parallelism int
	progressFn  func(SnapshotProgress)
}

// newUploader creates a new uploader.
func newUploader(storage remote.Storage, prefix, snapshotID string, parallelism int, progressFn func(SnapshotProgress)) *uploader {
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}
	return &uploader{
		storage:     storage,
		prefix:      prefix,
		snapshotID:  snapshotID,
		parallelism: parallelism,
		progressFn:  progressFn,
	}
}

// uploadResult contains the result of uploading a single file.
type uploadResult struct {
	FileInfo FileInfo
	Error    error
}

// uploadFiles uploads files from srcDir to remote storage in parallel.
// Returns FileInfo for all successfully uploaded files.
func (u *uploader) uploadFiles(ctx context.Context, srcDir string, files []string) ([]FileInfo, error) {
	if len(files) == 0 {
		return nil, nil
	}

	// Calculate total size for progress reporting
	var totalSize int64
	fileSizes := make(map[string]int64)
	for _, f := range files {
		info, err := os.Stat(filepath.Join(srcDir, f))
		if err != nil {
			return nil, fmt.Errorf("failed to stat %s: %w", f, err)
		}
		fileSizes[f] = info.Size()
		totalSize += info.Size()
	}

	// Create work channel and result channel
	workCh := make(chan string, len(files))
	resultCh := make(chan uploadResult, len(files))

	// Fill work channel
	for _, f := range files {
		workCh <- f
	}
	close(workCh)

	// Progress tracking
	var filesCompleted atomic.Int32
	var bytesCompleted atomic.Int64

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < u.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fileName := range workCh {
				select {
				case <-ctx.Done():
					resultCh <- uploadResult{
						FileInfo: FileInfo{Name: fileName},
						Error:    ctx.Err(),
					}
					continue
				default:
				}

				fileInfo, err := u.uploadFile(ctx, srcDir, fileName)
				resultCh <- uploadResult{
					FileInfo: fileInfo,
					Error:    err,
				}

				if err == nil {
					filesCompleted.Add(1)
					bytesCompleted.Add(fileSizes[fileName])

					if u.progressFn != nil {
						u.progressFn(SnapshotProgress{
							Phase:          "upload",
							FilesTotal:     len(files),
							FilesCompleted: int(filesCompleted.Load()),
							BytesTotal:     totalSize,
							BytesCompleted: bytesCompleted.Load(),
							CurrentFile:    fileName,
						})
					}
				}
			}
		}()
	}

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results
	var results []FileInfo
	var errs []error
	for result := range resultCh {
		if result.Error != nil {
			errs = append(errs, fmt.Errorf("upload %s: %w", result.FileInfo.Name, result.Error))
		} else {
			results = append(results, result.FileInfo)
		}
	}

	if len(errs) > 0 {
		// Return first error, but could aggregate
		return results, errs[0]
	}

	return results, nil
}

// uploadFile uploads a single file and returns its FileInfo.
func (u *uploader) uploadFile(ctx context.Context, srcDir, fileName string) (FileInfo, error) {
	srcPath := filepath.Join(srcDir, fileName)
	destPath := path.Join(u.prefix, u.snapshotID, fileName)

	// Open source file
	f, err := os.Open(srcPath)
	if err != nil {
		return FileInfo{}, fmt.Errorf("failed to open: %w", err)
	}
	defer f.Close()

	// Get file info
	stat, err := f.Stat()
	if err != nil {
		return FileInfo{}, fmt.Errorf("failed to stat: %w", err)
	}

	// Create hash writer
	hash := sha256.New()

	// Create remote object
	writer, err := u.storage.CreateObject(destPath)
	if err != nil {
		return FileInfo{}, fmt.Errorf("failed to create object: %w", err)
	}

	// Copy file content, calculating hash simultaneously
	multiWriter := io.MultiWriter(writer, hash)
	if _, err := io.Copy(multiWriter, f); err != nil {
		writer.Close()
		return FileInfo{}, fmt.Errorf("failed to copy: %w", err)
	}

	// Close writer to finish upload
	if err := writer.Close(); err != nil {
		return FileInfo{}, fmt.Errorf("failed to close writer: %w", err)
	}

	return FileInfo{
		Name:     fileName,
		Size:     stat.Size(),
		Checksum: "sha256:" + hex.EncodeToString(hash.Sum(nil)),
		Type:     detectFileType(fileName),
	}, nil
}

// detectFileType determines the file type from the file name.
func detectFileType(fileName string) string {
	switch {
	case strings.HasSuffix(fileName, ".sst"):
		return "sst"
	case strings.HasSuffix(fileName, ".log"):
		return "wal"
	case strings.HasPrefix(fileName, "MANIFEST"):
		return "manifest"
	case strings.HasPrefix(fileName, "OPTIONS"):
		return "options"
	case strings.HasPrefix(fileName, "marker."):
		return "marker"
	case strings.HasSuffix(fileName, ".blob"):
		return "blob"
	default:
		return "other"
	}
}

// scanCheckpointFiles scans a checkpoint directory and returns all file names.
func scanCheckpointFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			// Skip directories (shouldn't be any in a checkpoint)
			continue
		}
		files = append(files, entry.Name())
	}

	return files, nil
}

// calculateTotalSize calculates the total size of files in a directory.
func calculateTotalSize(dir string, files []string) (int64, error) {
	var total int64
	for _, f := range files {
		info, err := os.Stat(filepath.Join(dir, f))
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}
