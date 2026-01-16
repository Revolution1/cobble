// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build s3 || cloud

package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// DefaultMinIOImage is the default MinIO Docker image.
	DefaultMinIOImage = "minio/minio:latest"

	// DefaultAccessKey is the default MinIO access key.
	DefaultAccessKey = "minioadmin"

	// DefaultSecretKey is the default MinIO secret key.
	DefaultSecretKey = "minioadmin"
)

// MinIOContainer represents a running MinIO Docker container.
type MinIOContainer struct {
	ContainerID string
	Endpoint    string
	AccessKey   string
	SecretKey   string
	client      *s3.Client
}

// StartMinIO starts a MinIO Docker container for testing.
// It automatically stops the container when the test completes.
func StartMinIO(t *testing.T) *MinIOContainer {
	t.Helper()

	// Check if Docker is available
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found, skipping MinIO integration test")
	}

	// Find an available port
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Start MinIO container
	containerID, err := startMinIOContainer(port)
	if err != nil {
		t.Fatalf("failed to start MinIO container: %v", err)
	}

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)

	container := &MinIOContainer{
		ContainerID: containerID,
		Endpoint:    endpoint,
		AccessKey:   DefaultAccessKey,
		SecretKey:   DefaultSecretKey,
	}

	// Register cleanup
	t.Cleanup(func() {
		container.Stop()
	})

	// Wait for MinIO to be ready
	if err := container.waitForReady(30 * time.Second); err != nil {
		t.Fatalf("MinIO failed to become ready: %v", err)
	}

	return container
}

// Stop stops the MinIO container.
func (m *MinIOContainer) Stop() {
	if m.ContainerID == "" {
		return
	}
	exec.Command("docker", "rm", "-f", m.ContainerID).Run()
	m.ContainerID = ""
}

// Client returns an S3 client configured for this MinIO instance.
func (m *MinIOContainer) Client() *s3.Client {
	if m.client != nil {
		return m.client
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(m.AccessKey, m.SecretKey, ""),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to load AWS config: %v", err))
	}

	m.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(m.Endpoint)
		o.UsePathStyle = true
	})

	return m.client
}

// CreateBucket creates a bucket in MinIO.
func (m *MinIOContainer) CreateBucket(ctx context.Context, bucket string) error {
	_, err := m.Client().CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	return err
}

// waitForReady waits for MinIO to be ready to accept connections.
func (m *MinIOContainer) waitForReady(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := m.Client()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for MinIO to be ready")
		default:
			// Try to list buckets as a health check
			_, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
			if err == nil {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func findAvailablePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func startMinIOContainer(port int) (string, error) {
	image := os.Getenv("MINIO_IMAGE")
	if image == "" {
		image = DefaultMinIOImage
	}

	// Pull image if not exists (ignore errors, might already exist)
	exec.Command("docker", "pull", image).Run()

	// Start container
	cmd := exec.Command("docker", "run", "-d",
		"-p", fmt.Sprintf("%d:9000", port),
		"-e", fmt.Sprintf("MINIO_ROOT_USER=%s", DefaultAccessKey),
		"-e", fmt.Sprintf("MINIO_ROOT_PASSWORD=%s", DefaultSecretKey),
		image,
		"server", "/data",
	)

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("docker run failed: %s", string(exitErr.Stderr))
		}
		return "", fmt.Errorf("docker run failed: %w", err)
	}

	containerID := strings.TrimSpace(string(output))
	return containerID, nil
}

// SetupTestEnvironment sets up environment variables for testing with MinIO.
// Returns a cleanup function that restores the original environment.
func (m *MinIOContainer) SetupTestEnvironment(bucket string) func() {
	// Save original values
	origBucket := os.Getenv("COBBLE_S3_BUCKET")
	origEndpoint := os.Getenv("COBBLE_S3_ENDPOINT")
	origAccessKey := os.Getenv("COBBLE_S3_ACCESS_KEY_ID")
	origSecretKey := os.Getenv("COBBLE_S3_SECRET_ACCESS_KEY")
	origPathStyle := os.Getenv("COBBLE_S3_FORCE_PATH_STYLE")
	origTiered := os.Getenv("COBBLE_TIERED_STORAGE")

	// Set test values
	os.Setenv("COBBLE_S3_BUCKET", bucket)
	os.Setenv("COBBLE_S3_ENDPOINT", m.Endpoint)
	os.Setenv("COBBLE_S3_ACCESS_KEY_ID", m.AccessKey)
	os.Setenv("COBBLE_S3_SECRET_ACCESS_KEY", m.SecretKey)
	os.Setenv("COBBLE_S3_FORCE_PATH_STYLE", "true")
	os.Setenv("COBBLE_TIERED_STORAGE", "true")

	// Return cleanup function
	return func() {
		setOrUnset("COBBLE_S3_BUCKET", origBucket)
		setOrUnset("COBBLE_S3_ENDPOINT", origEndpoint)
		setOrUnset("COBBLE_S3_ACCESS_KEY_ID", origAccessKey)
		setOrUnset("COBBLE_S3_SECRET_ACCESS_KEY", origSecretKey)
		setOrUnset("COBBLE_S3_FORCE_PATH_STYLE", origPathStyle)
		setOrUnset("COBBLE_TIERED_STORAGE", origTiered)
	}
}

func setOrUnset(key, value string) {
	if value == "" {
		os.Unsetenv(key)
	} else {
		os.Setenv(key, value)
	}
}
