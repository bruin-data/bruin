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

type CloudConfig struct {
	DefaultEnvironment string                 `yaml:"default_environment"`
	Environments       map[string]Environment `yaml:"environments"`
}

type Environment struct {
	Connections map[string]interface{} `yaml:"connections"`
}

type PlatformTestProvider interface {
	GetWorkflows(binary string, currentFolder string) []e2e.Workflow
}

type BigQueryProvider struct{}

func (p BigQueryProvider) GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
	return bigquery.GetWorkflows(binary, currentFolder)
}

type SnowflakeProvider struct{}

func (p SnowflakeProvider) GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
	return snowflake.GetWorkflows(binary, currentFolder)
}

var platformConnectionMap = map[string]string{
	"bigquery":  "gcp",
	"snowflake": "snowflake",
	// Add more platforms as needed:
	// "athena": "athena",
	// "redshift": "redshift",
}

func getAvailablePlatforms(configPath string) (map[string]bool, error) {
	available := make(map[string]bool)

	data, err := os.ReadFile(configPath)
	if err != nil {
		return available, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	var config CloudConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return available, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	defaultEnv := config.DefaultEnvironment
	if defaultEnv == "" {
		defaultEnv = "default"
	}

	env, exists := config.Environments[defaultEnv]
	if !exists {
		return available, fmt.Errorf("default environment '%s' not found in config", defaultEnv)
	}

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

	configPath := filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Cloud configuration file not found at %s\n", configPath)
		fmt.Println("Skipping all cloud integration tests")
		return
	}

	availablePlatforms, err := getAvailablePlatforms(configPath)
	if err != nil {
		fmt.Printf("Failed to parse cloud configuration: %v\n", err)
		fmt.Println("Skipping all cloud integration tests")
		return
	}

	fmt.Println("Running Cloud Integration Tests")
	fmt.Println("=====================================")

	platformProviders := map[string]PlatformTestProvider{
		"bigquery":  BigQueryProvider{},
		"snowflake": SnowflakeProvider{},
	}

	totalWorkflows := 0
	skippedPlatforms := 0

	for platformName, provider := range platformProviders {
		if !availablePlatforms[platformName] {
			fmt.Printf("Skipping %s tests (no connection configured)\n", platformName)
			skippedPlatforms++
			continue
		}

		fmt.Printf("\nRunning %s tests...\n", platformName)

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

	fmt.Println("\nCloud Integration Test Summary")
	fmt.Println("==================================")
	fmt.Printf("Platforms tested: %d\n", len(platformProviders)-skippedPlatforms)
	fmt.Printf("Platforms skipped: %d\n", skippedPlatforms)
	fmt.Printf("Workflows: %d\n", totalWorkflows)
	fmt.Println("\nAll cloud integration tests completed successfully!")
}
