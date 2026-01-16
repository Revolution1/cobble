// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show admin server and database status",
	RunE:  runStatus,
}

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check if the admin server is healthy",
	RunE:  runHealth,
}

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Show Prometheus metrics",
	RunE:  runMetrics,
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show database statistics",
	RunE:  runStats,
}

func init() {
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(healthCmd)
	rootCmd.AddCommand(metricsCmd)
	rootCmd.AddCommand(statsCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/status")
	if err != nil {
		return fmt.Errorf("failed to get status: %w", err)
	}

	var status map[string]interface{}
	if err := json.Unmarshal(resp, &status); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	output, _ := json.MarshalIndent(status, "", "  ")
	fmt.Println(string(output))

	return nil
}

func runHealth(cmd *cobra.Command, args []string) error {
	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/health")
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	var health map[string]interface{}
	if err := json.Unmarshal(resp, &health); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if health["status"] == "ok" {
		fmt.Println("OK")
		return nil
	}

	fmt.Printf("UNHEALTHY: %v\n", health)
	return fmt.Errorf("health check failed")
}

func runMetrics(cmd *cobra.Command, args []string) error {
	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/metrics")
	if err != nil {
		return fmt.Errorf("failed to get metrics: %w", err)
	}

	fmt.Println(string(resp))
	return nil
}

func runStats(cmd *cobra.Command, args []string) error {
	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/db/stats")
	if err != nil {
		return fmt.Errorf("failed to get stats: %w", err)
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(resp, &stats); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	output, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Println(string(output))

	return nil
}
