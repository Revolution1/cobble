// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	Short: "Show database metrics",
	Long: `Show database metrics in various formats.

By default, shows metrics in human-readable format.
Use --json for JSON output or --prometheus for Prometheus format.`,
	RunE: runMetrics,
}

var (
	metricsJSON       bool
	metricsPrometheus bool
)

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

	metricsCmd.Flags().BoolVar(&metricsJSON, "json", false, "Output in JSON format")
	metricsCmd.Flags().BoolVar(&metricsPrometheus, "prometheus", false, "Output in Prometheus format")
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

	// Prometheus format requested
	if metricsPrometheus {
		fmt.Println(string(resp))
		return nil
	}

	// Parse Prometheus format to structured data
	metrics := parsePrometheusMetrics(string(resp))

	// JSON format requested
	if metricsJSON {
		output, _ := json.MarshalIndent(metrics, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	// Default: human-readable format
	printHumanReadableMetrics(metrics)
	return nil
}

// MetricValue represents a parsed metric.
type MetricValue struct {
	Name   string            `json:"name"`
	Help   string            `json:"help,omitempty"`
	Type   string            `json:"type,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  float64           `json:"value"`
}

// parsePrometheusMetrics parses Prometheus format metrics into structured data.
func parsePrometheusMetrics(input string) []MetricValue {
	var metrics []MetricValue
	lines := strings.Split(input, "\n")

	var currentHelp, currentType string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse HELP comments
		if strings.HasPrefix(line, "# HELP ") {
			parts := strings.SplitN(line[7:], " ", 2)
			if len(parts) == 2 {
				currentHelp = parts[1]
			}
			continue
		}

		// Parse TYPE comments
		if strings.HasPrefix(line, "# TYPE ") {
			parts := strings.SplitN(line[7:], " ", 2)
			if len(parts) == 2 {
				currentType = parts[1]
			}
			continue
		}

		// Skip other comments
		if strings.HasPrefix(line, "#") {
			continue
		}

		// Parse metric line: name{labels} value
		metric := parseMetricLine(line)
		if metric != nil {
			metric.Help = currentHelp
			metric.Type = currentType
			metrics = append(metrics, *metric)
		}
	}

	return metrics
}

func parseMetricLine(line string) *MetricValue {
	// Find the value at the end
	lastSpace := strings.LastIndex(line, " ")
	if lastSpace == -1 {
		return nil
	}

	valueStr := line[lastSpace+1:]
	nameAndLabels := line[:lastSpace]

	var value float64
	if _, err := fmt.Sscanf(valueStr, "%f", &value); err != nil {
		return nil
	}

	// Parse name and labels
	name := nameAndLabels
	labels := make(map[string]string)

	if idx := strings.Index(nameAndLabels, "{"); idx != -1 {
		name = nameAndLabels[:idx]
		labelStr := nameAndLabels[idx+1 : len(nameAndLabels)-1]

		// Parse labels like: key="value",key2="value2"
		for _, pair := range strings.Split(labelStr, ",") {
			if eq := strings.Index(pair, "="); eq != -1 {
				k := pair[:eq]
				v := strings.Trim(pair[eq+1:], "\"")
				labels[k] = v
			}
		}
	}

	return &MetricValue{
		Name:   name,
		Labels: labels,
		Value:  value,
	}
}

func printHumanReadableMetrics(metrics []MetricValue) {
	// Group metrics by category
	categories := make(map[string][]MetricValue)
	for _, m := range metrics {
		category := getMetricCategory(m.Name)
		categories[category] = append(categories[category], m)
	}

	// Print each category
	order := []string{"Database", "Compaction", "Cache", "Storage", "Snapshot", "Other"}
	for _, cat := range order {
		if metricList, ok := categories[cat]; ok {
			fmt.Printf("\n=== %s ===\n", cat)
			for _, m := range metricList {
				printMetric(m)
			}
		}
	}
}

func getMetricCategory(name string) string {
	switch {
	case strings.HasPrefix(name, "pebble_db") || strings.HasPrefix(name, "pebble_level"):
		return "Database"
	case strings.HasPrefix(name, "pebble_compact"):
		return "Compaction"
	case strings.HasPrefix(name, "pebble_cache") || strings.HasPrefix(name, "pebble_block"):
		return "Cache"
	case strings.HasPrefix(name, "pebble_remote") || strings.HasPrefix(name, "cobble_s3"):
		return "Storage"
	case strings.HasPrefix(name, "cobble_snapshot"):
		return "Snapshot"
	default:
		return "Other"
	}
}

func printMetric(m MetricValue) {
	// Format the name more readably
	name := strings.TrimPrefix(m.Name, "pebble_")
	name = strings.TrimPrefix(name, "cobble_")
	name = strings.ReplaceAll(name, "_", " ")

	if len(m.Labels) > 0 {
		var labelParts []string
		for k, v := range m.Labels {
			labelParts = append(labelParts, fmt.Sprintf("%s=%s", k, v))
		}
		fmt.Printf("  %-40s %s: %.2f\n", name, strings.Join(labelParts, ", "), m.Value)
	} else {
		fmt.Printf("  %-40s %.2f\n", name, m.Value)
	}
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
