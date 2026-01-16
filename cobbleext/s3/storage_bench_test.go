// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/cobbleext/testutil"
)

// =============================================================================
// S3 Storage Benchmarks
// =============================================================================

// BenchmarkS3Write measures write throughput to S3/MinIO.
func BenchmarkS3Write(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-write"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	sizes := []int{
		1 << 10,  // 1 KB
		4 << 10,  // 4 KB
		16 << 10, // 16 KB
		64 << 10, // 64 KB
		1 << 20,  // 1 MB
		4 << 20,  // 4 MB
	}

	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.Run(fmt.Sprintf("Size_%dKB", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				objName := fmt.Sprintf("write-test-%d-%d", size, i)
				writer, err := storage.CreateObject(objName)
				if err != nil {
					b.Fatalf("CreateObject failed: %v", err)
				}
				if _, err := writer.Write(data); err != nil {
					b.Fatalf("Write failed: %v", err)
				}
				if err := writer.Close(); err != nil {
					b.Fatalf("Close failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkS3Read measures read throughput from S3/MinIO.
func BenchmarkS3Read(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-read"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	sizes := []int{
		1 << 10,  // 1 KB
		4 << 10,  // 4 KB
		16 << 10, // 16 KB
		64 << 10, // 64 KB
		1 << 20,  // 1 MB
		4 << 20,  // 4 MB
	}

	// Pre-create objects
	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		objName := fmt.Sprintf("read-test-%d", size)
		writer, err := storage.CreateObject(objName)
		if err != nil {
			b.Fatalf("CreateObject failed: %v", err)
		}
		if _, err := writer.Write(data); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("Close failed: %v", err)
		}
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%dKB", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			objName := fmt.Sprintf("read-test-%d", size)
			buf := make([]byte, size)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader, _, err := storage.ReadObject(ctx, objName)
				if err != nil {
					b.Fatalf("ReadObject failed: %v", err)
				}
				if err := reader.ReadAt(ctx, buf, 0); err != nil {
					b.Fatalf("ReadAt failed: %v", err)
				}
				reader.Close()
			}
		})
	}
}

// BenchmarkS3RandomRead measures random read performance (simulates SST block reads).
func BenchmarkS3RandomRead(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-random-read"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create a 16MB object to simulate a large SST file
	objectSize := 16 << 20
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	objName := "large-sst-file"
	writer, err := storage.CreateObject(objName)
	if err != nil {
		b.Fatalf("CreateObject failed: %v", err)
	}
	if _, err := writer.Write(data); err != nil {
		b.Fatalf("Write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("Close failed: %v", err)
	}

	blockSizes := []int{
		4 << 10,  // 4 KB (typical SST block size)
		8 << 10,  // 8 KB
		32 << 10, // 32 KB (larger blocks)
	}

	for _, blockSize := range blockSizes {
		b.Run(fmt.Sprintf("BlockSize_%dKB", blockSize/1024), func(b *testing.B) {
			b.SetBytes(int64(blockSize))
			buf := make([]byte, blockSize)

			reader, _, err := storage.ReadObject(ctx, objName)
			if err != nil {
				b.Fatalf("ReadObject failed: %v", err)
			}
			defer reader.Close()

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Random offset aligned to block size
				offset := int64((i * 7919) % (objectSize - blockSize)) // 7919 is prime for pseudo-random distribution
				offset = (offset / int64(blockSize)) * int64(blockSize)

				if err := reader.ReadAt(ctx, buf, offset); err != nil {
					b.Fatalf("ReadAt failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkS3SequentialRead measures sequential read performance.
func BenchmarkS3SequentialRead(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-seq-read"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create a 4MB object
	objectSize := 4 << 20
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	objName := "seq-read-file"
	writer, err := storage.CreateObject(objName)
	if err != nil {
		b.Fatalf("CreateObject failed: %v", err)
	}
	if _, err := writer.Write(data); err != nil {
		b.Fatalf("Write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("Close failed: %v", err)
	}

	blockSize := 64 << 10 // 64 KB blocks
	numBlocks := objectSize / blockSize

	b.Run("Sequential_64KB_Blocks", func(b *testing.B) {
		b.SetBytes(int64(objectSize))
		buf := make([]byte, blockSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			reader, _, err := storage.ReadObject(ctx, objName)
			if err != nil {
				b.Fatalf("ReadObject failed: %v", err)
			}

			for block := 0; block < numBlocks; block++ {
				offset := int64(block * blockSize)
				if err := reader.ReadAt(ctx, buf, offset); err != nil {
					b.Fatalf("ReadAt failed: %v", err)
				}
			}

			reader.Close()
		}
	})
}

// BenchmarkS3List measures list operation performance.
func BenchmarkS3List(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-list"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	objectCounts := []int{10, 100, 500}

	for _, count := range objectCounts {
		prefix := fmt.Sprintf("list-%d/", count)

		// Create objects
		for i := 0; i < count; i++ {
			objName := fmt.Sprintf("%sobj-%05d.sst", prefix, i)
			writer, err := storage.CreateObject(objName)
			if err != nil {
				b.Fatalf("CreateObject failed: %v", err)
			}
			io.WriteString(writer, "test")
			writer.Close()
		}

		b.Run(fmt.Sprintf("Objects_%d", count), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				results, err := storage.List(prefix, "")
				if err != nil {
					b.Fatalf("List failed: %v", err)
				}
				if len(results) != count {
					b.Fatalf("expected %d results, got %d", count, len(results))
				}
			}
		})
	}
}

// BenchmarkS3ConcurrentReads measures concurrent read performance.
func BenchmarkS3ConcurrentReads(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-concurrent"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create test objects
	numObjects := 10
	objectSize := 64 << 10 // 64 KB each
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for i := 0; i < numObjects; i++ {
		objName := fmt.Sprintf("concurrent-%d.sst", i)
		writer, err := storage.CreateObject(objName)
		if err != nil {
			b.Fatalf("CreateObject failed: %v", err)
		}
		writer.Write(data)
		writer.Close()
	}

	concurrencyLevels := []int{1, 2, 4, 8}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			b.SetBytes(int64(objectSize * concurrency))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				for c := 0; c < concurrency; c++ {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()

						objName := fmt.Sprintf("concurrent-%d.sst", idx%numObjects)
						reader, _, err := storage.ReadObject(ctx, objName)
						if err != nil {
							return
						}
						defer reader.Close()

						buf := make([]byte, objectSize)
						reader.ReadAt(ctx, buf, 0)
					}(c)
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkS3Latency measures operation latency.
func BenchmarkS3Latency(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-latency"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create a small object
	objName := "latency-test.txt"
	writer, err := storage.CreateObject(objName)
	if err != nil {
		b.Fatalf("CreateObject failed: %v", err)
	}
	io.WriteString(writer, "latency test data")
	writer.Close()

	b.Run("HeadObject", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := storage.Size(objName)
			if err != nil {
				b.Fatalf("Size failed: %v", err)
			}
		}
	})

	b.Run("GetObject_Small", func(b *testing.B) {
		buf := make([]byte, 17) // "latency test data"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader, _, err := storage.ReadObject(ctx, objName)
			if err != nil {
				b.Fatalf("ReadObject failed: %v", err)
			}
			reader.ReadAt(ctx, buf, 0)
			reader.Close()
		}
	})

	b.Run("PutObject_Small", func(b *testing.B) {
		data := []byte("small write test")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("latency-write-%d", i)
			writer, err := storage.CreateObject(name)
			if err != nil {
				b.Fatalf("CreateObject failed: %v", err)
			}
			writer.Write(data)
			writer.Close()
		}
	})
}

// =============================================================================
// Latency Histogram Helper (for detailed latency analysis)
// =============================================================================

// LatencyStats collects latency statistics.
type LatencyStats struct {
	samples []time.Duration
	mu      sync.Mutex
}

func (s *LatencyStats) Add(d time.Duration) {
	s.mu.Lock()
	s.samples = append(s.samples, d)
	s.mu.Unlock()
}

func (s *LatencyStats) Report() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.samples) == 0 {
		return "no samples"
	}

	// Sort samples
	sorted := make([]time.Duration, len(s.samples))
	copy(sorted, s.samples)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	var total time.Duration
	for _, d := range sorted {
		total += d
	}

	avg := total / time.Duration(len(sorted))
	p50 := sorted[len(sorted)*50/100]
	p90 := sorted[len(sorted)*90/100]
	p99 := sorted[len(sorted)*99/100]
	min := sorted[0]
	max := sorted[len(sorted)-1]

	return fmt.Sprintf("n=%d min=%v avg=%v p50=%v p90=%v p99=%v max=%v",
		len(sorted), min, avg, p50, p90, p99, max)
}

// TestS3LatencyHistogram runs a detailed latency analysis (not a benchmark).
func TestS3LatencyHistogram(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "latency-histogram"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create test objects of various sizes
	sizes := []int{1 << 10, 64 << 10, 1 << 20} // 1KB, 64KB, 1MB
	for _, size := range sizes {
		data := make([]byte, size)
		objName := fmt.Sprintf("histogram-%d", size)
		writer, err := storage.CreateObject(objName)
		if err != nil {
			t.Fatalf("CreateObject failed: %v", err)
		}
		writer.Write(data)
		writer.Close()
	}

	numSamples := 50

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Read_%dKB", size/1024), func(t *testing.T) {
			stats := &LatencyStats{}
			objName := fmt.Sprintf("histogram-%d", size)
			buf := make([]byte, size)

			for i := 0; i < numSamples; i++ {
				start := time.Now()
				reader, _, err := storage.ReadObject(ctx, objName)
				if err != nil {
					t.Fatalf("ReadObject failed: %v", err)
				}
				reader.ReadAt(ctx, buf, 0)
				reader.Close()
				stats.Add(time.Since(start))
			}

			t.Logf("Read latency: %s", stats.Report())
		})
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Write_%dKB", size/1024), func(t *testing.T) {
			stats := &LatencyStats{}
			data := make([]byte, size)

			for i := 0; i < numSamples; i++ {
				start := time.Now()
				objName := fmt.Sprintf("histogram-write-%d-%d", size, i)
				writer, err := storage.CreateObject(objName)
				if err != nil {
					t.Fatalf("CreateObject failed: %v", err)
				}
				writer.Write(data)
				writer.Close()
				stats.Add(time.Since(start))
			}

			t.Logf("Write latency: %s", stats.Report())
		})
	}
}

// BenchmarkS3WriteRead measures round-trip write-then-read performance.
func BenchmarkS3WriteRead(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-writeread"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := NewStorage(Config{
		Bucket:          bucket,
		Prefix:          "bench/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	size := 64 << 10 // 64 KB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	readBuf := make([]byte, size)

	b.SetBytes(int64(size * 2)) // Write + Read
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		objName := fmt.Sprintf("writeread-%d", i)

		// Write
		writer, err := storage.CreateObject(objName)
		if err != nil {
			b.Fatalf("CreateObject failed: %v", err)
		}
		if _, err := writer.Write(data); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("Close failed: %v", err)
		}

		// Read
		reader, _, err := storage.ReadObject(ctx, objName)
		if err != nil {
			b.Fatalf("ReadObject failed: %v", err)
		}
		if err := reader.ReadAt(ctx, readBuf, 0); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
		reader.Close()

		// Verify
		if !bytes.Equal(data, readBuf) {
			b.Fatal("data mismatch")
		}
	}
}
