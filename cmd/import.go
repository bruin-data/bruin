package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/telemetry"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Import() *cli.Command {
	return &cli.Command{
		Name:      "import",
		Usage:     "Import database tables as Bruin assets",
		ArgsUsage: "[pipeline path]",
		Before:    telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "database",
				Aliases: []string{"d"},
				Usage:   "filter by specific database/dataset name",
			},
			&cli.StringFlag{
				Name:    "schema",
				Aliases: []string{"s"},
				Usage:   "filter by specific schema name",
			},
			&cli.BoolFlag{
				Name:    "fill-columns",
				Aliases: []string{"f"},
				Usage:   "automatically fill column metadata from database schema",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				return cli.Exit("pipeline path is required", 1)
			}

			connectionName := c.String("connection")
			database := c.String("database")
			schema := c.String("schema")
			fillColumns := c.Bool("fill-columns")
			environment := c.String("environment")
			configFile := c.String("config-file")

			return runImport(c.Context, pipelinePath, connectionName, database, schema, fillColumns, environment, configFile)
		},
	}
}

func runImport(ctx context.Context, pipelinePath, connectionName, database, schema string, fillColumns bool, environment, configFile string) error {
	fs := afero.NewOsFs()

	// Get connection from config
	conn, err := getConnectionFromConfig(environment, connectionName, fs, configFile)
	if err != nil {
		return errors2.Wrap(err, "failed to get database connection")
	}

	// Check if connection supports GetDatabaseSummary
	summarizer, ok := conn.(interface {
		GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
	})
	if !ok {
		return fmt.Errorf("connection type '%s' does not support database summary", connectionName)
	}

	// Get database summary
	summary, err := summarizer.GetDatabaseSummary(ctx)
	if err != nil {
		return errors2.Wrap(err, "failed to retrieve database summary")
	}

	// Check database filter if specified
	if database != "" && !strings.EqualFold(summary.Name, database) {
		return fmt.Errorf("database '%s' not found. Current database is '%s'", database, summary.Name)
	}

	// Validate pipeline path and ensure assets directory exists
	assetsPath := filepath.Join(pipelinePath, "assets")
	err = fs.MkdirAll(assetsPath, 0755)
	if err != nil {
		return errors2.Wrap(err, "failed to create assets directory")
	}

	// Determine asset type based on connection type
	assetType := determineAssetTypeFromConnection(connectionName, conn)

	// Import tables as SQL assets with filtering
	totalTables := 0
	for _, schemaObj := range summary.Schemas {
		// Skip schema if schema filter is specified and doesn't match
		if schema != "" && !strings.EqualFold(schemaObj.Name, schema) {
			continue
		}

		for _, table := range schemaObj.Tables {
			err := createSQLAsset(ctx, fs, assetsPath, schemaObj.Name, table.Name, assetType, conn, fillColumns)
			if err != nil {
				return errors2.Wrapf(err, "failed to create asset for table %s.%s", schemaObj.Name, table.Name)
			}
			totalTables++
		}
	}

	// Build filter description for output message
	filterDesc := ""
	if schema != "" {
		filterDesc = fmt.Sprintf(" (schema: %s)", schema)
	}

	fmt.Printf("Successfully imported %d tables from database '%s'%s into pipeline '%s'\n",
		totalTables, summary.Name, filterDesc, pipelinePath)

	return nil
}

func fillAssetColumnsFromDB(ctx context.Context, asset *pipeline.Asset, conn interface{}, schemaName, tableName string) error {
	// Check if connection supports schema introspection
	querier, ok := conn.(interface {
		SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
	})
	if !ok {
		return errors2.New("connection does not support schema introspection")
	}

	// Query to get column information
	queryStr := fmt.Sprintf("SELECT * FROM %s.%s WHERE 1=0 LIMIT 0", schemaName, tableName)
	q := &query.Query{Query: queryStr}
	result, err := querier.SelectWithSchema(ctx, q)
	if err != nil {
		return errors2.Wrapf(err, "failed to query columns for table %s.%s", schemaName, tableName)
	}

	if len(result.Columns) == 0 {
		return fmt.Errorf("no columns found for table %s.%s", schemaName, tableName)
	}

	// Skip special column names (from patch.go)
	skipColumns := map[string]bool{
		"_IS_CURRENT":  true,
		"_VALID_UNTIL": true,
		"_VALID_FROM":  true,
	}

	// Create column definitions
	columns := make([]pipeline.Column, 0, len(result.Columns))
	for i, colName := range result.Columns {
		if skipColumns[colName] {
			continue
		}
		columns = append(columns, pipeline.Column{
			Name:      colName,
			Type:      result.ColumnTypes[i],
			Checks:    []pipeline.ColumnCheck{},
			Upstreams: []*pipeline.UpstreamColumn{},
		})
	}

	asset.Columns = columns
	return nil
}

func createSQLAsset(ctx context.Context, fs afero.Fs, assetsPath, schemaName, tableName string, assetType pipeline.AssetType, conn interface{}, fillColumns bool) error {
	fileName := fmt.Sprintf("%s.%s.sql", strings.ToLower(schemaName), strings.ToLower(tableName))
	filePath := filepath.Join(assetsPath, fileName)

	// Create basic SQL select query
	query := fmt.Sprintf("SELECT *\nFROM %s.%s", schemaName, tableName)

	// Create asset
	asset := &pipeline.Asset{
		Type: assetType,
		ExecutableFile: pipeline.ExecutableFile{
			Name:    fileName,
			Path:    filePath,
			Content: query,
		},
		Name:        fmt.Sprintf("%s_%s", strings.ToLower(schemaName), strings.ToLower(tableName)),
		Description: fmt.Sprintf("Imported table %s.%s", schemaName, tableName),
	}

	// Fill columns from database if requested
	if fillColumns {
		err := fillAssetColumnsFromDB(ctx, asset, conn, schemaName, tableName)
		if err != nil {
			// Log warning but don't fail the import
			fmt.Printf("Warning: Could not fill columns for %s.%s: %v\n", schemaName, tableName, err)
		}
	}

	// Persist the asset
	err := asset.Persist(fs)
	if err != nil {
		return errors2.Wrap(err, "failed to persist asset")
	}

	return nil
}

func determineAssetTypeFromConnection(connectionName string, conn interface{}) pipeline.AssetType {
	// First, try to determine from the actual connection type
	if _, ok := conn.(interface {
		GetDatabases(ctx context.Context) ([]string, error)
	}); ok {
		// Check the package path or type name to determine the specific connection type
		connType := fmt.Sprintf("%T", conn)

		if strings.Contains(connType, "snowflake") {
			return pipeline.AssetTypeSnowflakeQuery
		}
		if strings.Contains(connType, "bigquery") {
			return pipeline.AssetTypeBigqueryQuery
		}
		if strings.Contains(connType, "postgres") {
			return pipeline.AssetTypePostgresQuery
		}
		if strings.Contains(connType, "athena") {
			return pipeline.AssetTypeAthenaQuery
		}
		if strings.Contains(connType, "databricks") {
			return pipeline.AssetTypeDatabricksQuery
		}
		if strings.Contains(connType, "duckdb") {
			return pipeline.AssetTypeDuckDBQuery
		}
		if strings.Contains(connType, "clickhouse") {
			return pipeline.AssetTypeClickHouse
		}
	}

	// Fallback: try to detect the connection type from the connection name
	connectionLower := strings.ToLower(connectionName)

	if strings.Contains(connectionLower, "snowflake") || strings.Contains(connectionLower, "sf") {
		return pipeline.AssetTypeSnowflakeQuery
	}
	if strings.Contains(connectionLower, "bigquery") || strings.Contains(connectionLower, "bq") {
		return pipeline.AssetTypeBigqueryQuery
	}
	if strings.Contains(connectionLower, "postgres") || strings.Contains(connectionLower, "pg") {
		return pipeline.AssetTypePostgresQuery
	}
	if strings.Contains(connectionLower, "redshift") || strings.Contains(connectionLower, "rs") {
		return pipeline.AssetTypeRedshiftQuery
	}
	if strings.Contains(connectionLower, "athena") {
		return pipeline.AssetTypeAthenaQuery
	}
	if strings.Contains(connectionLower, "databricks") {
		return pipeline.AssetTypeDatabricksQuery
	}
	if strings.Contains(connectionLower, "duckdb") {
		return pipeline.AssetTypeDuckDBQuery
	}
	if strings.Contains(connectionLower, "clickhouse") {
		return pipeline.AssetTypeClickHouse
	}
	if strings.Contains(connectionLower, "synapse") {
		return pipeline.AssetTypeSynapseQuery
	}
	if strings.Contains(connectionLower, "mssql") || strings.Contains(connectionLower, "sqlserver") {
		return pipeline.AssetTypeMsSQLQuery
	}

	// Default to Snowflake if we can't determine the type
	return pipeline.AssetTypeSnowflakeQuery
}
