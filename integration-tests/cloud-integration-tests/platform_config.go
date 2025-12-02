package main

// PlatformConfig defines platform-specific configuration for templated tests
type PlatformConfig struct {
	// Name is the platform identifier (e.g., "postgres", "snowflake", "bigquery")
	Name string

	// Connection is the connection name used in --connection flag
	Connection string

	// SchemaPrefix is how to reference schema in queries (e.g., "test", "public", or "" for no prefix)
	SchemaPrefix string

	// AssetType is the default asset type for this platform (e.g., "pg.sql", "sf.sql", "bq.sql")
	AssetType string

	// PlatformConnection is the platform connection type name for pipeline.yml (e.g., "postgres", "snowflake", "google_cloud_platform")
	PlatformConnection string

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
			AssetType:              "pg.sql",
			PlatformConnection:     "postgres",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {"relation", "does not exist"}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"snowflake": {
			Name:                   "snowflake",
			Connection:             "snowflake-default",
			SchemaPrefix:           "test",
			AssetType:              "sf.sql",
			PlatformConnection:     "snowflake",
			DropTableExitCode:      0,
			ErrorPatterns:          map[string][]string{"table_not_exists": {"Object", "does not exist or not authorized"}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"bigquery": {
			Name:                   "bigquery",
			Connection:             "gcp-default",
			SchemaPrefix:           "test",
			AssetType:              "bq.sql",
			PlatformConnection:     "google_cloud_platform",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"redshift": {
			Name:                   "redshift",
			Connection:             "redshift-default",
			SchemaPrefix:           "test",
			AssetType:              "rs.sql",
			PlatformConnection:     "redshift",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"athena": {
			Name:                   "athena",
			Connection:             "athena-default",
			SchemaPrefix:           "test",
			AssetType:              "athena.sql",
			PlatformConnection:     "athena",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"duckdb": {
			Name:                   "duckdb",
			Connection:             "duckdb-default",
			SchemaPrefix:           "test",
			AssetType:              "duckdb.sql",
			PlatformConnection:     "duckdb",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"mssql": {
			Name:                   "mssql",
			Connection:             "mssql-default",
			SchemaPrefix:           "test",
			AssetType:              "mssql.sql",
			PlatformConnection:     "mssql",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"mysql": {
			Name:                   "mysql",
			Connection:             "mysql-default",
			SchemaPrefix:           "test",
			AssetType:              "mysql.sql",
			PlatformConnection:     "mysql",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"clickhouse": {
			Name:                   "clickhouse",
			Connection:             "clickhouse-default",
			SchemaPrefix:           "test",
			AssetType:              "clickhouse.sql",
			PlatformConnection:     "clickhouse",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
			FinishedMessagePattern: "Finished: test.menu",
		},
		"databricks": {
			Name:                   "databricks",
			Connection:             "databricks-default",
			SchemaPrefix:           "test",
			AssetType:              "databricks.sql",
			PlatformConnection:     "databricks",
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
