// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/cobbleext"
)

// AdminClient is an HTTP client for the admin server.
type AdminClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// getAdminClient creates an admin client from flags or configuration.
func getAdminClient() (*AdminClient, error) {
	addr := adminAddr
	token := adminToken

	// If not specified on command line, try config file
	if addr == "" {
		cfg, err := loadConfig()
		if err == nil && cfg != nil {
			if cfg.Admin.Addr != "" {
				addr = cfg.Admin.Addr
			}
			if cfg.Admin.Token != "" && token == "" {
				token = cfg.Admin.Token
			}
		}
	}

	// Default address
	if addr == "" {
		addr = "127.0.0.1:6060"
	}

	return NewAdminClient(addr, token)
}

// loadConfig loads the configuration file.
func loadConfig() (*cobbleext.Config, error) {
	if configFile != "" {
		return cobbleext.LoadFile(configFile)
	}

	// Try environment variable
	if envPath := os.Getenv(cobbleext.EnvConfigFile); envPath != "" {
		return cobbleext.LoadFile(envPath)
	}

	return nil, nil
}

// NewAdminClient creates a new admin client.
func NewAdminClient(addr, token string) (*AdminClient, error) {
	var baseURL string
	var transport http.RoundTripper = http.DefaultTransport

	if strings.HasPrefix(addr, "unix://") {
		socketPath := strings.TrimPrefix(addr, "unix://")
		transport = &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		}
		baseURL = "http://unix"
	} else {
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		baseURL = addr
	}

	return &AdminClient{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}, nil
}

// Get performs a GET request.
func (c *AdminClient) Get(ctx context.Context, path string) ([]byte, error) {
	return c.do(ctx, http.MethodGet, path, nil)
}

// Post performs a POST request.
func (c *AdminClient) Post(ctx context.Context, path string, body interface{}) ([]byte, error) {
	return c.do(ctx, http.MethodPost, path, body)
}

// Delete performs a DELETE request.
func (c *AdminClient) Delete(ctx context.Context, path string) ([]byte, error) {
	return c.do(ctx, http.MethodDelete, path, nil)
}

// getDataDirFromAdmin retrieves the database directory path from the admin API.
// This allows commands to auto-detect the data directory when connected to an admin server.
func getDataDirFromAdmin() (string, error) {
	client, err := getAdminClient()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/status")
	if err != nil {
		return "", fmt.Errorf("failed to get status from admin API: %w", err)
	}

	var status struct {
		DBPath string `json:"db_path"`
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return "", fmt.Errorf("failed to parse status response: %w", err)
	}

	if status.DBPath == "" {
		return "", fmt.Errorf("admin API returned empty db_path")
	}

	// The admin server returns the path to a single database.
	// For checkpoint, we typically want the parent directory containing multiple DBs.
	// Check if the parent directory contains multiple Pebble DBs.
	parentDir := filepath.Dir(status.DBPath)
	return parentDir, nil
}

func (c *AdminClient) do(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	url := c.baseURL + path

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("server returned %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}
