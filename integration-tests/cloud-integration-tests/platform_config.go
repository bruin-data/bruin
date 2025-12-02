package main

// PlatformConfig defines platform-specific configuration for templated tests
type PlatformConfig struct {
	// Name is the platform identifier (e.g., "postgres", "snowflake", "bigquery")
	Name string

	// Connection is the connection name used in --connection flag
	Connection string

	// SchemaPrefix is how to reference schema in queries (e.g., "test", "public", or "" for no prefix)
	SchemaPrefix string

	// DropTableExitCode is the expected exit code for DROP TABLE operations
	// 0 for Snowflake/Athena, 1 for BigQuery/Postgres/Redshift
	DropTableExitCode int

	// ErrorPatterns are expected error message patterns for various operations
	ErrorPatterns map[string][]string

	// FinishedMessagePattern is the pattern to match in "Finished:" messages
	FinishedMessagePattern string
}

// GetPlatformConfigs returns configurations for all supported platforms
func GetPlatformConfigs() map[string]PlatformConfig {
	return map[string]PlatformConfig{
		"postgres": {
			Name:                   "postgres",
			Connection:             "postgres-default",
			SchemaPrefix:           "test",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {"relation", "does not exist"}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"snowflake": {
			Name:                   "snowflake",
			Connection:             "snowflake-default",
			SchemaPrefix:           "test",
			DropTableExitCode:      0,
			ErrorPatterns:          map[string][]string{"table_not_exists": {"Object", "does not exist or not authorized"}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"bigquery": {
			Name:                   "bigquery",
			Connection:             "gcp-default",
			SchemaPrefix:           "test",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
	}
}

// GetPlatformConfig returns the configuration for a specific platform
func GetPlatformConfig(platform string) (PlatformConfig, bool) {
	configs := GetPlatformConfigs()
	config, ok := configs[platform]
	return config, ok
}
