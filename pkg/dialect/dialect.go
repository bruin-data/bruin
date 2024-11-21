package dialect

import (
	"fmt"
)

// Define constants for known SQL dialects.
const (
	BigQueryDialect  = "bigquery"
	SnowflakeDialect = "snowflake"
	DuckDBDialect    = "duckdb"
)

// Make the map immutable and use constants.
var assetTypeDialectMap = map[string]string{
	"bq.sql":     BigQueryDialect,
	"sf.sql":     SnowflakeDialect,
	"duckdb.sql": DuckDBDialect,
}

// GetDialectByAssetType checks if the asset type has a valid SQL dialect.
func GetDialectByAssetType(assetType string) (string, error) {
	dialect, ok := assetTypeDialectMap[assetType]
	if !ok {
		return "", fmt.Errorf("unsupported asset type: %s", assetType)
	}
	return dialect, nil
}
