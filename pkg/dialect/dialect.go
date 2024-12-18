package dialect

import (
	"fmt"
)

// Define constants for known SQL dialects.
const (
	BigQueryDialect   = "bigquery"
	SnowflakeDialect  = "snowflake"
	DuckDBDialect     = "duckdb"
	RedshiftDialect   = "redshift"
	PostgresDialect   = "postgres"
	MssqlDialect      = "mssql"
	DatabricksDialect = "databricks"
	AthenaDialect     = "athena"
	SynapseDialect    = "synapse"
)

// Make the map immutable and use constants.
var assetTypeDialectMap = map[string]string{
	"bq.sql":         BigQueryDialect,
	"sf.sql":         SnowflakeDialect,
	"duckdb.sql":     DuckDBDialect,
	"rs.sql":         RedshiftDialect,
	"pg.sql":         PostgresDialect,
	"ms.sql":         MssqlDialect,
	"databricks.sql": DatabricksDialect,
	"athena.sql":     AthenaDialect,
	"synapse.sql":    SynapseDialect,
}

// GetDialectByAssetType checks if the asset type has a valid SQL dialect.
func GetDialectByAssetType(assetType string) (string, error) {
	dialect, ok := assetTypeDialectMap[assetType]
	if !ok {
		return "", fmt.Errorf("unsupported asset type: %s", assetType)
	}
	return dialect, nil
}
