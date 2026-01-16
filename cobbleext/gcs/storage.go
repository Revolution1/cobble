// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build gcs || cloud

package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Storage implements remote.Storage for Google Cloud Storage.
type Storage struct {
	client *storage.Client
	bucket *storage.BucketHandle
	prefix string
}

var _ remote.Storage = (*Storage)(nil)

// Config holds GCS storage configuration.
type Config struct {
	Bucket          string
	Prefix          string
	CredentialsFile string
	CredentialsJSON string
}

// NewStorage creates a new GCS storage backend.
func NewStorage(cfg Config) (*Storage, error) {
	ctx := context.Background()

	var opts []option.ClientOption
	if cfg.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)))
	} else if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}
	// If neither is specified, uses Application Default Credentials

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &Storage{
		client: client,
		bucket: client.Bucket(cfg.Bucket),
		prefix: cfg.Prefix,
	}, nil
}

// Close implements remote.Storage.
func (s *Storage) Close() error {
	return s.client.Close()
}

// ReadObject implements remote.Storage.
func (s *Storage) ReadObject(ctx context.Context, objName string) (remote.ObjectReader, int64, error) {
	key := s.prefix + objName

	// Get object attributes to determine size
	attrs, err := s.bucket.Object(key).Attrs(ctx)
	if err != nil {
		return nil, 0, err
	}

	return &objectReader{
		bucket: s.bucket,
		key:    key,
		size:   attrs.Size,
	}, attrs.Size, nil
}

// CreateObject implements remote.Storage.
func (s *Storage) CreateObject(objName string) (io.WriteCloser, error) {
	key := s.prefix + objName
	return newObjectWriter(s.bucket, key), nil
}

// List implements remote.Storage.
func (s *Storage) List(prefix, delimiter string) ([]string, error) {
	ctx := context.Background()
	fullPrefix := s.prefix + prefix

	query := &storage.Query{
		Prefix: fullPrefix,
	}
	if delimiter != "" {
		query.Delimiter = delimiter
	}

	var results []string
	it := s.bucket.Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		// Handle both objects and prefixes
		if attrs.Name != "" {
			name := attrs.Name[len(s.prefix):]
			results = append(results, name)
		}
		if attrs.Prefix != "" {
			name := attrs.Prefix[len(s.prefix):]
			results = append(results, name)
		}
	}

	return results, nil
}

// Delete implements remote.Storage.
func (s *Storage) Delete(objName string) error {
	ctx := context.Background()
	key := s.prefix + objName
	return s.bucket.Object(key).Delete(ctx)
}

// Size implements remote.Storage.
func (s *Storage) Size(objName string) (int64, error) {
	ctx := context.Background()
	key := s.prefix + objName

	attrs, err := s.bucket.Object(key).Attrs(ctx)
	if err != nil {
		return 0, err
	}

	return attrs.Size, nil
}

// IsNotExistError implements remote.Storage.
func (s *Storage) IsNotExistError(err error) bool {
	return err == storage.ErrObjectNotExist
}

// objectReader implements remote.ObjectReader for GCS.
type objectReader struct {
	bucket *storage.BucketHandle
	key    string
	size   int64
}

var _ remote.ObjectReader = (*objectReader)(nil)

// ReadAt implements remote.ObjectReader.
func (r *objectReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("negative offset: %d", offset)
	}
	if offset+int64(len(p)) > r.size {
		return fmt.Errorf("read beyond end of object: offset=%d, len=%d, size=%d", offset, len(p), r.size)
	}

	reader, err := r.bucket.Object(r.key).NewRangeReader(ctx, offset, int64(len(p)))
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.ReadFull(reader, p)
	return err
}

// Close implements remote.ObjectReader.
func (r *objectReader) Close() error {
	return nil
}

// objectWriter implements io.WriteCloser for GCS.
// It buffers all data and uploads on Close.
type objectWriter struct {
	bucket *storage.BucketHandle
	key    string
	buf    bytes.Buffer
}

func newObjectWriter(bucket *storage.BucketHandle, key string) *objectWriter {
	return &objectWriter{
		bucket: bucket,
		key:    key,
	}
}

func (w *objectWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *objectWriter) Close() error {
	ctx := context.Background()

	writer := w.bucket.Object(w.key).NewWriter(ctx)
	if _, err := io.Copy(writer, &w.buf); err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}
