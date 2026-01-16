// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble/cobbleext"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show or manage configuration",
	Long:  `Display the current configuration or manage configuration settings.`,
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current configuration",
	RunE:  runConfigShow,
}

var configEnvCmd = &cobra.Command{
	Use:   "env",
	Short: "Show environment variables",
	RunE:  runConfigEnv,
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configShowCmd)
	configCmd.AddCommand(configEnvCmd)
}

func runConfigShow(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if cfg == nil {
		// Load defaults
		cfg = &cobbleext.Config{}
		cobbleext.LoadFromEnv(cfg)
	}

	output, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	fmt.Println(string(output))
	return nil
}

func runConfigEnv(cmd *cobra.Command, args []string) error {
	envVars := []struct {
		name string
		desc string
	}{
		{cobbleext.EnvConfigFile, "Path to config file"},
		{cobbleext.EnvTieredStorage, "Enable tiered storage"},
		{cobbleext.EnvTieringStrategy, "Tiering strategy (none, lower, all)"},
		{cobbleext.EnvS3Bucket, "S3 bucket name"},
		{cobbleext.EnvS3Prefix, "S3 object prefix"},
		{cobbleext.EnvS3Region, "S3 region"},
		{cobbleext.EnvS3Endpoint, "S3 endpoint URL"},
		{cobbleext.EnvS3AccessKeyID, "S3 access key ID"},
		{cobbleext.EnvS3SecretAccessKey, "S3 secret access key"},
		{cobbleext.EnvS3ForcePathStyle, "Force S3 path-style addressing"},
		{cobbleext.EnvGCSBucket, "GCS bucket name"},
		{cobbleext.EnvGCSPrefix, "GCS object prefix"},
		{cobbleext.EnvGCSCredentialsFile, "GCS credentials file path"},
		{cobbleext.EnvCacheSize, "Remote cache size"},
		{cobbleext.EnvCacheBlockSize, "Remote cache block size"},
		{cobbleext.EnvCacheShardingBlockSize, "Remote cache sharding block size"},
		{cobbleext.EnvAdminEnabled, "Enable admin server"},
		{cobbleext.EnvAdminAddr, "Admin server address"},
		{cobbleext.EnvAdminToken, "Admin server auth token"},
		{cobbleext.EnvAutoSnapshotEnabled, "Enable auto snapshots"},
		{cobbleext.EnvAutoSnapshotInterval, "Auto snapshot interval"},
		{cobbleext.EnvAutoSnapshotIncremental, "Enable incremental auto snapshots"},
		{cobbleext.EnvAutoSnapshotKeepLast, "Number of auto snapshots to keep"},
		{cobbleext.EnvSnapshotBucket, "Snapshot bucket name"},
		{cobbleext.EnvSnapshotPrefix, "Snapshot prefix"},
		{cobbleext.EnvSnapshotUseTieredConfig, "Use tiered storage config for snapshots"},
	}

	fmt.Println("Cobble Environment Variables:")
	fmt.Println()
	for _, v := range envVars {
		fmt.Printf("  %-40s %s\n", v.name, v.desc)
	}

	return nil
}
