package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/bruin-data/bruin/integration-tests/cloud-integration-tests/bigquery"
	"github.com/bruin-data/bruin/integration-tests/cloud-integration-tests/snowflake"
	"github.com/bruin-data/bruin/pkg/e2e"
	"gopkg.in/yaml.v3"
)

// CloudConfig represents the structure of .bruin.cloud.yml.
type CloudConfig struct {
	DefaultEnvironment string                 `yaml:"default_environment"`
	Environments       map[string]Environment `yaml:"environments"`
}

type Environment struct {
	Connections map[string]interface{} `yaml:"connections"`
}

// PlatformTestProvider defines the interface for platform-specific test providers.
type PlatformTestProvider interface {
	GetTasks(binary string, currentFolder string) []e2e.Task
	GetWorkflows(binary string, currentFolder string) []e2e.Workflow
}

// BigQueryProvider implements PlatformTestProvider for BigQuery.
type BigQueryProvider struct{}

func (p BigQueryProvider) GetTasks(binary string, currentFolder string) []e2e.Task {
	return bigquery.GetTasks(binary, currentFolder)
}

func (p BigQueryProvider) GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
	return bigquery.GetWorkflows(binary, currentFolder)
}

// SnowflakeProvider implements PlatformTestProvider for Snowflake.
type SnowflakeProvider struct{}

func (p SnowflakeProvider) GetTasks(binary string, currentFolder string) []e2e.Task {
	return snowflake.GetTasks(binary, currentFolder)
}

func (p SnowflakeProvider) GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
	return snowflake.GetWorkflows(binary, currentFolder)
}

// platformConnectionMap maps platform names to their connection type names in the config.
var platformConnectionMap = map[string]string{
	"bigquery":  "gcp",
	"snowflake": "snowflake",
	// Add more platforms as needed:
	// "athena": "athena",
	// "redshift": "redshift",
}

// getAvailablePlatforms reads the cloud config and returns available platforms.
func getAvailablePlatforms(configPath string) (map[string]bool, error) {
	available := make(map[string]bool)

	// Read the config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return available, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	// Parse the YAML
	var config CloudConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return available, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	// Get the default environment connections
	defaultEnv := config.DefaultEnvironment
	if defaultEnv == "" {
		defaultEnv = "default"
	}

	env, exists := config.Environments[defaultEnv]
	if !exists {
		return available, fmt.Errorf("default environment '%s' not found in config", defaultEnv)
	}

	// Check which platforms have connections configured
	for platform, connectionType := range platformConnectionMap {
		if _, hasConnection := env.Connections[connectionType]; hasConnection {
			available[platform] = true
		}
	}

	return available, nil
}

func main() {
	currentFolder, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current working directory: %v", err)
	}

	binary := "./bin/bruin"

	// Check if cloud config exists
	configPath := filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Cloud configuration file not found at %s\n", configPath)
		fmt.Println("Skipping all cloud integration tests")
		return
	}

	// Get available platforms from config
	availablePlatforms, err := getAvailablePlatforms(configPath)
	if err != nil {
		fmt.Printf("Failed to parse cloud configuration: %v\n", err)
		fmt.Println("Skipping all cloud integration tests")
		return
	}

	fmt.Println("Running Cloud Integration Tests")
	fmt.Println("=====================================")

	// Map platform names to their providers
	platformProviders := map[string]PlatformTestProvider{
		"bigquery":  BigQueryProvider{},
		"snowflake": SnowflakeProvider{},
		// Add more platforms here as they're implemented
	}

	totalTests := 0
	totalWorkflows := 0
	skippedPlatforms := 0

	// Run tests for each available platform
	for platformName, provider := range platformProviders {
		if !availablePlatforms[platformName] {
			fmt.Printf("Skipping %s tests (no connection configured)\n", platformName)
			skippedPlatforms++
			continue
		}

		fmt.Printf("\nRunning %s tests...\n", platformName)

		// Get and run tasks
		tasks := provider.GetTasks(binary, currentFolder)
		if len(tasks) > 0 {
			fmt.Printf("Running %d tasks\n", len(tasks))
			totalTests += len(tasks)
			for _, task := range tasks {
				if err := task.Run(); err != nil {
					log.Fatalf("Task failed: %v", err)
				}
			}
		}

		// Get and run workflows
		workflows := provider.GetWorkflows(binary, currentFolder)
		if len(workflows) > 0 {
			fmt.Printf("Running %d workflows\n", len(workflows))
			totalWorkflows += len(workflows)
			for _, workflow := range workflows {
				if err := workflow.Run(); err != nil {
					log.Fatalf("Workflow failed: %v", err)
				}
			}
		}

		fmt.Printf("%s tests completed successfully\n", platformName)
	}

	// Summary
	fmt.Println("\nCloud Integration Test Summary")
	fmt.Println("==================================")
	fmt.Printf("Platforms tested: %d\n", len(platformProviders)-skippedPlatforms)
	fmt.Printf("Platforms skipped: %d\n", skippedPlatforms)
	fmt.Printf("Total tests: %d\n", totalTests)
	fmt.Printf("Total workflows: %d\n", totalWorkflows)
	fmt.Println("\nAll cloud integration tests completed successfully!")
}
