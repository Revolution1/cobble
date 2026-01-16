// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build s3 || cloud

package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// Storage implements remote.Storage for Amazon S3.
type Storage struct {
	client *s3.Client
	bucket string
	prefix string
}

var _ remote.Storage = (*Storage)(nil)

// Config holds S3 storage configuration.
type Config struct {
	Bucket          string
	Prefix          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	ForcePathStyle  bool
}

// NewStorage creates a new S3 storage backend.
func NewStorage(cfg Config) (*Storage, error) {
	ctx := context.Background()

	// Build AWS config options
	var opts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with optional custom endpoint
	clientOpts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.ForcePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOpts...)

	return &Storage{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// Close implements remote.Storage.
func (s *Storage) Close() error {
	// S3 client doesn't need explicit close
	return nil
}

// ReadObject implements remote.Storage.
func (s *Storage) ReadObject(ctx context.Context, objName string) (remote.ObjectReader, int64, error) {
	key := s.prefix + objName

	// Get object metadata first to determine size
	headResp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, err
	}

	size := *headResp.ContentLength

	return &objectReader{
		client: s.client,
		bucket: s.bucket,
		key:    key,
		size:   size,
	}, size, nil
}

// CreateObject implements remote.Storage.
func (s *Storage) CreateObject(objName string) (io.WriteCloser, error) {
	key := s.prefix + objName
	return newObjectWriter(s.client, s.bucket, key), nil
}

// List implements remote.Storage.
func (s *Storage) List(prefix, delimiter string) ([]string, error) {
	ctx := context.Background()
	fullPrefix := s.prefix + prefix

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}

	var results []string
	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			// Remove the storage prefix from the key
			name := (*obj.Key)[len(s.prefix):]
			results = append(results, name)
		}

		// Handle common prefixes (directories) when using delimiter
		for _, cp := range page.CommonPrefixes {
			name := (*cp.Prefix)[len(s.prefix):]
			results = append(results, name)
		}
	}

	return results, nil
}

// Delete implements remote.Storage.
func (s *Storage) Delete(objName string) error {
	ctx := context.Background()
	key := s.prefix + objName

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

// Size implements remote.Storage.
func (s *Storage) Size(objName string) (int64, error) {
	ctx := context.Background()
	key := s.prefix + objName

	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}

	return *resp.ContentLength, nil
}

// IsNotExistError implements remote.Storage.
func (s *Storage) IsNotExistError(err error) bool {
	var nsk *types.NoSuchKey
	var notFound *types.NotFound
	return errors.As(err, &nsk) || errors.As(err, &notFound)
}

// Copy implements cobbleext.StorageCopier.
// It performs a server-side copy within the same bucket.
func (s *Storage) Copy(ctx context.Context, srcName, dstName string) error {
	srcKey := s.prefix + srcName
	dstKey := s.prefix + dstName

	// S3 CopyObject requires the source to be in "bucket/key" format
	copySource := s.bucket + "/" + srcKey

	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(dstKey),
	})
	if err != nil {
		return fmt.Errorf("failed to copy %s to %s: %w", srcName, dstName, err)
	}
	return nil
}

// GetETag implements cobbleext.StorageETagGetter.
// Returns the ETag of the object (MD5 for single-part uploads).
func (s *Storage) GetETag(ctx context.Context, objName string) (string, error) {
	key := s.prefix + objName

	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}

	if resp.ETag == nil {
		return "", fmt.Errorf("no ETag returned for %s", objName)
	}

	// ETag comes with quotes, remove them
	etag := *resp.ETag
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		etag = etag[1 : len(etag)-1]
	}
	return etag, nil
}

// objectReader implements remote.ObjectReader for S3.
type objectReader struct {
	client *s3.Client
	bucket string
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

	rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+int64(len(p))-1)

	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.ReadFull(resp.Body, p)
	return err
}

// Close implements remote.ObjectReader.
func (r *objectReader) Close() error {
	return nil
}

// objectWriter implements io.WriteCloser for S3.
// It buffers all data and uploads on Close.
// TODO: Implement multipart upload for large files.
type objectWriter struct {
	client *s3.Client
	bucket string
	key    string
	buf    bytes.Buffer
}

func newObjectWriter(client *s3.Client, bucket, key string) *objectWriter {
	return &objectWriter{
		client: client,
		bucket: bucket,
		key:    key,
	}
}

func (w *objectWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *objectWriter) Close() error {
	ctx := context.Background()

	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   bytes.NewReader(w.buf.Bytes()),
	})
	return err
}
