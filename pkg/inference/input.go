package inference

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/tablename"
)

// resolveInputQuery resolves input_asset after the complete pipeline has been built.
// input_query is returned byte-for-byte unchanged.
func resolveInputQuery(pipe *pipeline.Pipeline, asset *pipeline.Asset, conn config.ConnectionGetter) (string, error) {
	cfg, err := readConfig(asset)
	if err != nil {
		return "", err
	}
	if cfg.inputAsset == "" {
		return cfg.inputQuery, nil
	}
	if cfg.inputAsset == asset.Name {
		return "", fmt.Errorf("inference input_asset %q cannot reference itself", cfg.inputAsset)
	}
	source := pipe.GetAssetByName(cfg.inputAsset)
	if source == nil {
		return "", fmt.Errorf("inference input_asset %q does not exist in pipeline", cfg.inputAsset)
	}
	sourceConnection, err := pipe.GetConnectionNameForAsset(source)
	if err != nil {
		return "", err
	}
	if sourceConnection != asset.Connection {
		return "", fmt.Errorf("inference input_asset %q uses connection %q, but inference asset uses %q; cross-connection inference is not supported", cfg.inputAsset, sourceConnection, asset.Connection)
	}

	platform := ""
	if details, ok := conn.(config.ConnectionDetailsGetter); ok {
		platform = details.GetConnectionType(asset.Connection)
	}
	if platform == "" {
		platform = pipeline.AssetTypeConnectionMapping[source.Type]
	}
	capability, ok := tablename.For(platform)
	if !ok {
		return "", fmt.Errorf("inference cannot quote input_asset for unsupported SQL platform %q", platform)
	}
	if err := capability.CheckName(source.Name); err != nil {
		return "", fmt.Errorf("invalid inference input_asset name: %w", err)
	}

	var quoted string
	switch platform {
	case "google_cloud_platform", "mysql", "doris", "starrocks", "clickhouse", "databricks", "spark":
		quoted = ansisql.QuoteIdentifierWithBackticks(source.Name)
	case "mssql", "fabric":
		quoted = ansisql.QuoteIdentifierWithBrackets(source.Name)
	case "postgres", "redshift", "snowflake", "duckdb", "motherduck", "trino", "vertica", "oracle", "sail", "hana", "synapse", "athena":
		quoted = ansisql.QuoteIdentifierWithDoubleQuotes(source.Name)
	default:
		return "", fmt.Errorf("inference has no identifier quoting rule for SQL platform %q", platform)
	}
	return "SELECT * FROM " + quoted, nil
}
