package main

// PlatformConfig defines platform-specific configuration for templated tests
type PlatformConfig struct {
	// Name is the platform identifier (e.g., "postgres", "snowflake", "bigquery")
	Name string

	// Connection is the connection name used in --connection flag
	Connection string

	// AssetType is the default asset type for this platform (e.g., "pg.sql", "sf.sql", "bq.sql")
	AssetType string

	// PlatformConnection is the platform connection type name for pipeline.yml (e.g., "postgres", "snowflake", "google_cloud_platform")
	PlatformConnection string

	// DropTableExitCode is the expected exit code for DROP TABLE operations
	// 0 for Snowflake/Athena, 1 for BigQuery/Postgres/Redshift
	DropTableExitCode int

	// ErrorPatterns are expected error message patterns for various operations
	ErrorPatterns map[string][]string
}

// GetPlatformConfigs returns configurations for all supported platforms
func GetPlatformConfigs() map[string]PlatformConfig {
	return map[string]PlatformConfig{
		"postgres": {
			Name:                   "postgres",
			Connection:             "postgres-default",
			AssetType:              "pg.sql",
			PlatformConnection:     "postgres",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {"relation", "does not exist"}},
		},
		"snowflake": {
			Name:                   "snowflake",
			Connection:             "snowflake-default",
			AssetType:              "sf.sql",
			PlatformConnection:     "snowflake",
			DropTableExitCode:      0,
			ErrorPatterns:          map[string][]string{"table_not_exists": {"Object", "does not exist or not authorized"}},
		},
		"bigquery": {
			Name:                   "bigquery",
			Connection:             "gcp-default",
			AssetType:              "bq.sql",
			PlatformConnection:     "google_cloud_platform",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
		"redshift": {
			Name:                   "redshift",
			Connection:             "redshift-default",
			AssetType:              "rs.sql",
			PlatformConnection:     "redshift",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
		"athena": {
			Name:                   "athena",
			Connection:             "athena-default",
			AssetType:              "athena.sql",
			PlatformConnection:     "athena",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
		"duckdb": {
			Name:                   "duckdb",
			Connection:             "duckdb-default",
			AssetType:              "duckdb.sql",
			PlatformConnection:     "duckdb",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
		"mssql": {
			Name:                   "mssql",
			Connection:             "mssql-default",
			AssetType:              "mssql.sql",
			PlatformConnection:     "mssql",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
		"mysql": {
			Name:                   "mysql",
			Connection:             "mysql-default",
			AssetType:              "mysql.sql",
			PlatformConnection:     "mysql",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
		"clickhouse": {
			Name:                   "clickhouse",
			Connection:             "clickhouse-default",
			AssetType:              "clickhouse.sql",
			PlatformConnection:     "clickhouse",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
		"databricks": {
			Name:                   "databricks",
			Connection:             "databricks-default",
			AssetType:              "databricks.sql",
			PlatformConnection:     "databricks",
			DropTableExitCode:      1,
			ErrorPatterns:          map[string][]string{"table_not_exists": {}},
		},
	}
}

// GetPlatformConfig returns the configuration for a specific platform
func GetPlatformConfig(platform string) (PlatformConfig, bool) {
	configs := GetPlatformConfigs()
	config, ok := configs[platform]
	return config, ok
}
