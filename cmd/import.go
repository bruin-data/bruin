package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/oracle"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/telemetry"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Import() *cli.Command {
	return &cli.Command{
		Name: "import",
		Subcommands: []*cli.Command{
			ImportDatabase(),
		},
	}
}

func ImportDatabase() *cli.Command {
	return &cli.Command{
		Name:      "database",
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
				Name:    "schema",
				Aliases: []string{"s"},
				Usage:   "filter by specific schema name",
			},
			&cli.BoolFlag{
				Name:    "no-columns",
				Aliases: []string{"n"},
				Usage:   "skip filling column metadata from database schema",
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
			schema := c.String("schema")
			noColumns := c.Bool("no-columns")
			environment := c.String("environment")
			configFile := c.String("config-file")

			return runImport(c.Context, pipelinePath, connectionName, schema, !noColumns, environment, configFile)
		},
	}
}

func runImport(ctx context.Context, pipelinePath, connectionName, schema string, fillColumns bool, environment, configFile string) error {
	fs := afero.NewOsFs()

	conn, err := getConnectionFromConfig(environment, connectionName, fs, configFile)
	if err != nil {
		return errors2.Wrap(err, "failed to get database connection")
	}

	summarizer, ok := conn.(interface {
		GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
	})
	if !ok {
		return fmt.Errorf("connection type '%s' does not support database summary", connectionName)
	}

	summary, err := summarizer.GetDatabaseSummary(ctx)
	if err != nil {
		return errors2.Wrap(err, "failed to retrieve database summary")
	}

	pathParts := strings.Split(pipelinePath, "/")
	if pathParts[len(pathParts)-1] == "pipeline.yml" || pathParts[len(pathParts)-1] == "pipeline.yaml" {
		pipelinePath = strings.Join(pathParts[:len(pathParts)-2], "/")
	}
	pipelineFound, err := GetPipelinefromPath(ctx, pipelinePath)
	if err != nil {
		return errors2.Wrap(err, "failed to get pipeline from path")
	}
	existingAssets := make(map[string]*pipeline.Asset, len(pipelineFound.Assets))
	for _, asset := range pipelineFound.Assets {
		existingAssets[asset.Name] = asset
	}

	assetsPath := filepath.Join(pipelinePath, "assets")
	assetType := determineAssetTypeFromConnection(connectionName, conn)
	totalTables := 0
	mergedTableCount := 0
	for _, schemaObj := range summary.Schemas {
		if schema != "" && !strings.EqualFold(schemaObj.Name, schema) {
			continue
		}
		for _, table := range schemaObj.Tables {
			createdAsset, err := createAsset(ctx, assetsPath, schemaObj.Name, table.Name, assetType, conn, fillColumns)
			if err != nil {
				return errors2.Wrapf(err, "failed to create asset for table %s.%s", schemaObj.Name, table.Name)
			}

			assetName := fmt.Sprintf("%s.%s", strings.ToLower(schemaObj.Name), strings.ToLower(table.Name))

			if existingAssets[assetName] == nil {

				schemaFolder := filepath.Join(assetsPath, strings.ToLower(schemaObj.Name))
				if err := fs.MkdirAll(schemaFolder, 0o755); err != nil {
					return errors2.Wrapf(err, "failed to create schema directory %s", schemaFolder)
				}

				err = createdAsset.Persist(fs)
				if err != nil {
					return err
				}
				existingAssets[assetName] = createdAsset
				totalTables++
			} else {
				existingAsset := existingAssets[assetName]
				existingColumns := make(map[string]pipeline.Column, len(existingAsset.Columns))
				for _, column := range existingAsset.Columns {
					existingColumns[column.Name] = column
				}
				for _, c := range createdAsset.Columns {
					if _, ok := existingColumns[c.Name]; !ok {
						existingAsset.Columns = append(existingAsset.Columns, c)
					}
				}
				err = existingAsset.Persist(fs)
				mergedTableCount++
				if err != nil {
					return err
				}
			}
		}
	}

	filterDesc := ""
	if schema != "" {
		filterDesc = fmt.Sprintf(" (schema: %s)", schema)
	}

	fmt.Printf("Imported %d tables and Merged %d from data warehouse '%s'%s into pipeline '%s'\n",
		totalTables, mergedTableCount, summary.Name, filterDesc, pipelinePath)

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

	if _, ok := conn.(*mssql.DB); ok {
		queryStr = "SELECT TOP 0 * FROM " + schemaName + "." + tableName
	} else if _, ok := conn.(*oracle.Client); ok {
		queryStr = "SELECT * FROM " + schemaName + "." + tableName + " WHERE 1=0"
	}
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

func createAsset(ctx context.Context, assetsPath, schemaName, tableName string, assetType pipeline.AssetType, conn interface{}, fillColumns bool) (*pipeline.Asset, error) {
	// Create schema subfolder
	schemaFolder := filepath.Join(assetsPath, strings.ToLower(schemaName))

	fileName := fmt.Sprintf("%s.asset.yml", strings.ToLower(tableName))
	filePath := filepath.Join(schemaFolder, fileName)
	asset := &pipeline.Asset{
		Type: assetType,
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
		},
		Description: fmt.Sprintf("Imported table %s.%s", schemaName, tableName),
	}

	if fillColumns {
		err := fillAssetColumnsFromDB(ctx, asset, conn, schemaName, tableName)
		if err != nil {
			warningPrinter.Printf("Warning: Could not fill columns for %s.%s: %v\n", schemaName, tableName, err)
			if err != nil {
				return nil, err
			}
		}
	}

	return asset, nil
}

func determineAssetTypeFromConnection(connectionName string, conn interface{}) pipeline.AssetType {
	// First, try to determine from the actual connection type
	if _, ok := conn.(interface {
		GetDatabaseSummary(ctx context.Context) ([]string, error)
	}); ok {
		connType := fmt.Sprintf("%T", conn)
		if strings.Contains(connType, "snowflake") {
			return pipeline.AssetTypeSnowflakeSource
		}
		if strings.Contains(connType, "bigquery") {
			return pipeline.AssetTypeBigquerySource
		}
		if strings.Contains(connType, "postgres") {
			return pipeline.AssetTypePostgresSource
		}
		if strings.Contains(connType, "athena") {
			return pipeline.AssetTypeAthenaSource
		}
		if strings.Contains(connType, "databricks") {
			return pipeline.AssetTypeDatabricksSource
		}
		if strings.Contains(connType, "duckdb") {
			return pipeline.AssetTypeDuckDBSource
		}
		if strings.Contains(connType, "clickhouse") {
			return pipeline.AssetTypeClickHouseSource
		}
		if strings.Contains(connType, "oracle") {
			return pipeline.AssetTypeOracleSource
		}
	}

	// Fallback: try to detect the connection type from the connection name
	connectionLower := strings.ToLower(connectionName)

	if strings.Contains(connectionLower, "snowflake") || strings.Contains(connectionLower, "sf") {
		return pipeline.AssetTypeSnowflakeSource
	}
	if strings.Contains(connectionLower, "bigquery") || strings.Contains(connectionLower, "bq") {
		return pipeline.AssetTypeBigquerySource
	}
	if strings.Contains(connectionLower, "postgres") || strings.Contains(connectionLower, "pg") {
		return pipeline.AssetTypePostgresSource
	}
	if strings.Contains(connectionLower, "redshift") || strings.Contains(connectionLower, "rs") {
		return pipeline.AssetTypeRedshiftSource
	}
	if strings.Contains(connectionLower, "athena") {
		return pipeline.AssetTypeAthenaSource
	}
	if strings.Contains(connectionLower, "databricks") {
		return pipeline.AssetTypeDatabricksSource
	}
	if strings.Contains(connectionLower, "duckdb") {
		return pipeline.AssetTypeDuckDBSource
	}
	if strings.Contains(connectionLower, "clickhouse") {
		return pipeline.AssetTypeClickHouseSource
	}
	if strings.Contains(connectionLower, "synapse") {
		return pipeline.AssetTypeSynapseSource
	}
	if strings.Contains(connectionLower, "mssql") || strings.Contains(connectionLower, "sqlserver") {
		return pipeline.AssetTypeMsSQLSource
	}
	if strings.Contains(connectionLower, "oracle") {
		return pipeline.AssetTypeOracleSource
	}

	// Default to Snowflake if we can't determine the type
	return pipeline.AssetTypeSnowflakeSource
}

func GetPipelinefromPath(ctx context.Context, inputPath string) (*pipeline.Pipeline, error) {
	pipelinePath, err := path.GetPipelineRootFromTask(inputPath, PipelineDefinitionFiles)
	if err != nil {
		errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
		return nil, err
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		errorPrinter.Println("failed to get the pipeline this asset belongs to, are you sure you have referred the right path?")
		errorPrinter.Println("\nHint: You need to run this command with a path to the asset file itself directly.")
		return nil, err
	}
	return foundPipeline, nil
}
