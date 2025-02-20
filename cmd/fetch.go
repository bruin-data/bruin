package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	path2 "path"
	"path/filepath"
	"time"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

type ppInfo struct {
	Pipeline *pipeline.Pipeline
	Asset    *pipeline.Asset
	Config   *config.Config
}

func Query() *cli.Command {
	return &cli.Command{
		Name:   "query",
		Usage:  "Execute a query on a specified connection and retrieve results",
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "query",
				Aliases:  []string{"q"},
				Usage:    "the SQL query to execute",
				Required: false,
			},
			&cli.Int64Flag{
				Name:        "limit",
				Aliases:     []string{"l"},
				Usage:       "limit the number of rows returned",
				Value:       1000,
				DefaultText: "1000",
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:  "asset",
				Usage: "Path to a SQL asset file within a Bruin pipeline. This file should contain the query to be executed.",
			},
			&cli.StringFlag{
				Name:  "env",
				Usage: "Target environment name as defined in .bruin.yml. Specifies the configuration environment for executing the query.",
			},
		},
		Action: func(c *cli.Context) error {
			// Check for connection/query mode
			hasConnection := c.String("connection") != ""
			hasQuery := c.String("query") != ""
			hasAsset := c.String("asset") != ""

			// Check for any other flags besides the allowed ones
			for _, flag := range c.FlagNames() {
				if hasConnection && hasQuery {
					// In connection/query mode, only allow connection, query, and limit
					if flag != "connection" && flag != "query" && flag != "limit" && flag != "output" {
						return handleError(c.String("output"),
							errors.New("when using connection/query mode, only --connection, --query, --limit, and --output flags are allowed"))
					}
				} else if hasAsset {
					// In asset mode, only allow asset, env, limit, and output
					if flag != "asset" && flag != "env" && flag != "limit" && flag != "output" {
						return handleError(c.String("output"),
							errors.New("when using asset mode, only --asset, --env, --limit, and --output flags are allowed"))
					}
				}
			}

			// Validate required combinations
			if hasConnection || hasQuery {
				// Both connection and query are required together
				if !(hasConnection && hasQuery) {
					return handleError(c.String("output"),
						errors.New("when using direct query mode, both --connection and --query are required"))
				}
			} else if !hasAsset {
				return handleError(c.String("output"),
					errors.New("must provide either (--connection and --query) OR --asset"))
			}

			connectionName := c.String("connection")
			queryStr := c.String("query")
			limit := c.Int64("limit")
			output := c.String("output")
			assetPath := c.String("asset")
			env := c.String("env")
			conn := interface{}(nil)
			if assetPath == "" {
				repoRoot, err := git.FindRepoFromPath(".")
				if err != nil {
					return handleError(output, errors.Wrap(err, "failed to find the git repository root"))
				}
				configFilePath := filepath.Join(repoRoot.Path, ".bruin.yml")
				fs := afero.NewOsFs()
				cm, err := config.LoadOrCreate(fs, configFilePath)
				if err != nil {
					return handleError(output, errors.Wrap(err, "failed to load or create config"))
				}

				manager, errs := connection.NewManagerFromConfig(cm)
				if len(errs) > 0 {
					for _, err := range errs {
						return handleError(output, errors.Wrap(err, "failed to create connection manager"))
					}
					return cli.Exit("", 1)
				}
				conn, err = manager.GetConnection(connectionName)
				if err != nil {
					return handleError(output, errors.Wrap(err, "failed to get connection"))
				}

			} else {
				pipelineInfo, err := GetPipelineAndAsset(assetPath)
				if err != nil {
					return handleError(output, errors.Wrap(err, "failed to get pipeline info"))
				}

				// Verify that the asset is a SQL asset
				if !pipelineInfo.Asset.IsSQLAsset() {
					return handleError(output,
						errors.Errorf("asset '%s' is not a SQL asset (type: %s). Only SQL assets can be queried",
							assetPath,
							pipelineInfo.Asset.Type))
				}

				if env != "" {
					err = pipelineInfo.Config.SelectEnvironment(env)
					if err != nil {
						return handleError(output,
							errors.Wrapf(err, "failed to use the environment '%s'", env))
					}
				}

				// Create extractor with jinja renderer
				startDate := time.Now() // You might want to make these configurable
				endDate := time.Now()
				extractor := &query.WholeFileExtractor{
					Fs:       afero.NewOsFs(),
					Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, "your-pipeline-name", "your-run-id"),
				}

				// Extract the query from the asset
				queries, err := extractor.ExtractQueriesFromString(pipelineInfo.Asset.ExecutableFile.Content)
				if err != nil {
					return handleError(output, errors.Wrap(err, "failed to extract query"))
				}

				if len(queries) == 0 {
					return handleError(output, errors.New("no query found in asset"))
				}

				queryStr = queries[0].Query

				// Get connection info
				manager, errs := connection.NewManagerFromConfig(pipelineInfo.Config)
				if len(errs) > 0 {
					for _, err := range errs {
						return handleError(output, errors.Wrap(err, "failed to create connection manager"))
					}
					return cli.Exit("", 1)
				}

				conn_name, err := pipelineInfo.Pipeline.GetConnectionNameForAsset(pipelineInfo.Asset)
				if err != nil {
					return handleError(output, errors.Wrap(err, "failed to get connection"))
				}

				conn, err = manager.GetConnection(conn_name)
				if err != nil {
					return handleError(output, errors.Wrap(err, fmt.Sprintf("failed to get connection '%s'", conn_name)))
				}
			}

			// Check if the connection supports querying with schema
			if querier, ok := conn.(interface {
				SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
			}); ok {
				// Prepare context and query
				ctx := context.Background()
				q := query.Query{Query: queryStr}

				// Call SelectWithSchema and retrieve the result
				result, err := querier.SelectWithSchema(ctx, &q)
				if err != nil {
					return handleError(output, errors.Wrap(err, "query execution failed"))
				}

				// Limit the results after query execution
				if len(result.Rows) > int(limit) {
					result.Rows = result.Rows[:limit]
				}

				// Output result based on format specified
				if output == "json" {
					type jsonResponse struct {
						Columns []map[string]string `json:"columns"`
						Rows    [][]interface{}     `json:"rows"`
					}

					// Construct JSON response with structured columns
					jsonCols := make([]map[string]string, len(result.Columns))
					for i, colName := range result.Columns {
						jsonCols[i] = map[string]string{"name": colName}
					}

					// Prepare the final output struct
					finalOutput := jsonResponse{
						Columns: jsonCols,
						Rows:    result.Rows,
					}

					jsonData, err := json.Marshal(finalOutput)
					if err != nil {
						return handleError(output, errors.Wrap(err, "failed to marshal result to JSON"))
					}
					fmt.Println(string(jsonData))
				} else {
					printTable(result.Columns, result.Rows)
				}
			} else {
				fmt.Printf("Connection type %s does not support querying.\n", connectionName)
			}
			return nil
		},
	}
}

func printTable(columnNames []string, rows [][]interface{}) {
	if len(rows) == 0 {
		fmt.Println("No data available")
		return
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	headers := make(table.Row, len(columnNames))
	for i, colName := range columnNames {
		headers[i] = colName
	}
	t.AppendHeader(headers)

	for _, row := range rows {
		rowData := make(table.Row, len(row))
		for i, cell := range row {
			rowData[i] = fmt.Sprintf("%v", cell)
		}
		t.AppendRow(rowData)
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}

func handleError(output string, err error) error {
	if output == "json" {
		jsonError, err := json.Marshal(map[string]string{"error": err.Error()})
		if err != nil {
			fmt.Println("Error:", err.Error())
			return cli.Exit("", 1)
		}
		fmt.Println(string(jsonError))
	} else {
		fmt.Println("Error:", err.Error())
	}
	return cli.Exit("", 1)
}

func GetPipelineAndAsset(inputPath string) (*ppInfo, error) {
	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
		return nil, err
	}

	runningForAnAsset := isPathReferencingAsset(inputPath)
	if !runningForAnAsset {
		errorPrinter.Printf("Please provide a valid asset path\n")
		return nil, err
	}
	pipelinePath, err := path.GetPipelineRootFromTask(inputPath, pipelineDefinitionFiles)
	if err != nil {
		errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
		return nil, err
	}
	configFilePath := path2.Join(repoRoot.Path, ".bruin.yml")
	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		errorPrinter.Printf("Failed to load the config file at '%s': %v\n", configFilePath, err)
		return nil, err
	}
	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath, true)
	if err != nil {
		errorPrinter.Println("failed to get the pipeline this asset belongs to, are you sure you have referred the right path?")
		errorPrinter.Println("\nHint: You need to run this command with a path to the asset file itself directly.")
		return nil, err
	}
	task, err := DefaultPipelineBuilder.CreateAssetFromFile(inputPath, foundPipeline)
	if err != nil {
		errorPrinter.Printf("Failed to build asset: %v. Are you sure you used the correct path?\n", err.Error())
		return nil, err
	}
	if task == nil {
		errorPrinter.Printf("The given file path doesn't seem to be a Bruin task definition: '%s'\n", inputPath)
		return nil, err
	}
	return &ppInfo{
		Pipeline: foundPipeline,
		Asset:    task,
		Config:   cm,
	}, nil
}
