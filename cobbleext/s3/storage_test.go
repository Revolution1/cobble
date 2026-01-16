// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package s3

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/cobbleext/testutil"
)

func TestS3StorageIntegration(t *testing.T) {
	// Start MinIO container
	minio := testutil.StartMinIO(t)

	ctx := context.Background()
	bucket := "test-bucket"

	// Create test bucket
	if err := minio.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// Create storage
	storage, err := NewStorage(Config{
		Bucket:         bucket,
		Prefix:         "test-prefix/",
		Endpoint:       minio.Endpoint,
		AccessKeyID:    minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle: true,
	})
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	t.Run("CreateAndReadObject", func(t *testing.T) {
		testData := []byte("hello world, this is test data for S3 storage")
		objName := "test-object.txt"

		// Create object
		writer, err := storage.CreateObject(objName)
		if err != nil {
			t.Fatalf("CreateObject failed: %v", err)
		}
		if _, err := writer.Write(testData); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Read object
		reader, size, err := storage.ReadObject(ctx, objName)
		if err != nil {
			t.Fatalf("ReadObject failed: %v", err)
		}
		defer reader.Close()

		if size != int64(len(testData)) {
			t.Errorf("size = %d, want %d", size, len(testData))
		}

		// Read full content
		buf := make([]byte, size)
		if err := reader.ReadAt(ctx, buf, 0); err != nil {
			t.Fatalf("ReadAt failed: %v", err)
		}

		if !bytes.Equal(buf, testData) {
			t.Errorf("data mismatch: got %q, want %q", buf, testData)
		}
	})

	t.Run("ReadAtOffset", func(t *testing.T) {
		testData := []byte("0123456789abcdefghij")
		objName := "offset-test.txt"

		// Create object
		writer, err := storage.CreateObject(objName)
		if err != nil {
			t.Fatalf("CreateObject failed: %v", err)
		}
		if _, err := writer.Write(testData); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Read at different offsets
		reader, _, err := storage.ReadObject(ctx, objName)
		if err != nil {
			t.Fatalf("ReadObject failed: %v", err)
		}
		defer reader.Close()

		tests := []struct {
			offset   int64
			length   int
			expected string
		}{
			{0, 5, "01234"},
			{5, 5, "56789"},
			{10, 5, "abcde"},
			{15, 5, "fghij"},
			{0, 20, "0123456789abcdefghij"},
		}

		for _, tt := range tests {
			buf := make([]byte, tt.length)
			if err := reader.ReadAt(ctx, buf, tt.offset); err != nil {
				t.Errorf("ReadAt(offset=%d, len=%d) failed: %v", tt.offset, tt.length, err)
				continue
			}
			if string(buf) != tt.expected {
				t.Errorf("ReadAt(offset=%d, len=%d) = %q, want %q", tt.offset, tt.length, buf, tt.expected)
			}
		}
	})

	t.Run("Size", func(t *testing.T) {
		testData := []byte("size test data")
		objName := "size-test.txt"

		// Create object
		writer, err := storage.CreateObject(objName)
		if err != nil {
			t.Fatalf("CreateObject failed: %v", err)
		}
		io.WriteString(writer, string(testData))
		writer.Close()

		// Check size
		size, err := storage.Size(objName)
		if err != nil {
			t.Fatalf("Size failed: %v", err)
		}
		if size != int64(len(testData)) {
			t.Errorf("Size = %d, want %d", size, len(testData))
		}
	})

	t.Run("List", func(t *testing.T) {
		// Create multiple objects
		objects := []string{"list/a.txt", "list/b.txt", "list/sub/c.txt"}
		for _, obj := range objects {
			writer, err := storage.CreateObject(obj)
			if err != nil {
				t.Fatalf("CreateObject(%s) failed: %v", obj, err)
			}
			io.WriteString(writer, "test")
			writer.Close()
		}

		// List all under "list/"
		results, err := storage.List("list/", "")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("List returned %d results, want 3: %v", len(results), results)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		objName := "delete-test.txt"

		// Create object
		writer, err := storage.CreateObject(objName)
		if err != nil {
			t.Fatalf("CreateObject failed: %v", err)
		}
		io.WriteString(writer, "to be deleted")
		writer.Close()

		// Verify it exists
		if _, err := storage.Size(objName); err != nil {
			t.Fatalf("object should exist: %v", err)
		}

		// Delete
		if err := storage.Delete(objName); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify it's gone
		_, err = storage.Size(objName)
		if err == nil {
			t.Error("object should not exist after delete")
		}
		if !storage.IsNotExistError(err) {
			t.Errorf("expected not exist error, got: %v", err)
		}
	})

	t.Run("IsNotExistError", func(t *testing.T) {
		_, _, err := storage.ReadObject(ctx, "nonexistent-object.txt")
		if err == nil {
			t.Error("expected error for nonexistent object")
		}
		if !storage.IsNotExistError(err) {
			t.Errorf("expected IsNotExistError to return true, got false for error: %v", err)
		}
	})

	t.Run("LargeObject", func(t *testing.T) {
		// Create a 1MB object
		size := 1024 * 1024
		testData := make([]byte, size)
		for i := range testData {
			testData[i] = byte(i % 256)
		}
		objName := "large-object.bin"

		// Create object
		writer, err := storage.CreateObject(objName)
		if err != nil {
			t.Fatalf("CreateObject failed: %v", err)
		}
		if _, err := writer.Write(testData); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Read back and verify
		reader, gotSize, err := storage.ReadObject(ctx, objName)
		if err != nil {
			t.Fatalf("ReadObject failed: %v", err)
		}
		defer reader.Close()

		if gotSize != int64(size) {
			t.Errorf("size = %d, want %d", gotSize, size)
		}

		// Read in chunks and verify
		chunkSize := 64 * 1024
		for offset := 0; offset < size; offset += chunkSize {
			end := offset + chunkSize
			if end > size {
				end = size
			}
			buf := make([]byte, end-offset)
			if err := reader.ReadAt(ctx, buf, int64(offset)); err != nil {
				t.Fatalf("ReadAt(offset=%d) failed: %v", offset, err)
			}
			if !bytes.Equal(buf, testData[offset:end]) {
				t.Errorf("data mismatch at offset %d", offset)
			}
		}
	})
}

func TestS3Factory(t *testing.T) {
	minio := testutil.StartMinIO(t)

	ctx := context.Background()
	bucket := "factory-test"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	factory := NewFactory(Config{
		Bucket:         bucket,
		Prefix:         "",
		Endpoint:       minio.Endpoint,
		AccessKeyID:    minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle: true,
	})

	// Create storage via factory
	storage, err := factory.CreateStorage("test-locator")
	if err != nil {
		t.Fatalf("CreateStorage failed: %v", err)
	}
	defer storage.Close()

	// Basic operation test
	writer, err := storage.CreateObject("factory-test.txt")
	if err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}
	io.WriteString(writer, "factory test")
	writer.Close()

	size, err := storage.Size("factory-test.txt")
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != 12 {
		t.Errorf("Size = %d, want 12", size)
	}
}
