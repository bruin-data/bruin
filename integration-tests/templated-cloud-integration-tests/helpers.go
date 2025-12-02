package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type CloudConfig struct {
	DefaultEnvironment string                 `yaml:"default_environment"`
	Environments       map[string]Environment `yaml:"environments"`
}

type Environment struct {
	Connections map[string]interface{} `yaml:"connections"`
}

var platformConnectionMap = map[string]string{
	"bigquery":  "google_cloud_platform",
	"snowflake": "snowflake",
	"postgres":  "postgres",
	"redshift":  "redshift",
	"athena":    "athena",
}

func getAvailablePlatforms(configPath string) (map[string]bool, error) {
	available := make(map[string]bool)

	data, err := os.ReadFile(configPath)
	if err != nil {
		return available, err
	}

	var config CloudConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return available, err
	}

	defaultEnv := config.DefaultEnvironment
	if defaultEnv == "" {
		defaultEnv = "default"
	}

	env, exists := config.Environments[defaultEnv]
	if !exists {
		return available, nil
	}

	for platform, connectionType := range platformConnectionMap {
		if _, hasConnection := env.Connections[connectionType]; hasConnection {
			available[platform] = true
		}
	}

	return available, nil
}
