package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	path2 "path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/telemetry"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

const (
	outputFormatPlain = "plain"
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
		// Slice flags such as --var and --filter accept JSON values that contain
		// commas (e.g. --var filters='{"start_date":"x","end_date":"y"}'). Disable
		// the default comma separator so those values are not split into fragments.
		DisableSliceFlagSeparator: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: false,
			},
			startDateFlag,
			endDateFlag,
			&cli.StringFlag{
				Name:     "query",
				Aliases:  []string{"q"},
				Usage:    "the SQL query to execute",
				Required: false,
			},
			&cli.Int64Flag{
				Name:    "limit",
				Aliases: []string{"l"},
				Usage:   "limit the number of rows returned",
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: outputFormatPlain,
				Value:       outputFormatPlain,
				Usage:       "the output type, possible values are: plain, json, csv",
			},
			&cli.IntFlag{
				Name:    "timeout",
				Aliases: []string{"t"},
				Usage:   "timeout for query execution in seconds",
				Value:   1000,
			},
			&cli.StringFlag{
				Name:  "asset",
				Usage: "Path to a SQL asset file within a Bruin pipeline. This file should contain the query to be executed.",
			},
			&cli.StringFlag{
				Name:  "pipeline",
				Usage: "Path to a Bruin pipeline. Used with --semantic-model when no asset is provided.",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for executing the query.",
			},
			&cli.StringFlag{
				Name:  "semantic-model",
				Usage: "Name of the semantic model to compile and query from the repository semantic directory.",
			},
			&cli.StringSliceFlag{
				Name:  "metric",
				Usage: "Semantic metric to select. Can be passed multiple times.",
			},
			&cli.StringSliceFlag{
				Name:  "dimension",
				Usage: "Semantic dimension to select. Use name:granularity for time dimensions.",
			},
			&cli.StringSliceFlag{
				Name:  "filter",
				Usage: `Semantic filter as JSON, e.g. '{"dimension":"country","operator":"equals","value":"US"}'.`,
			},
			&cli.StringSliceFlag{
				Name:  "segment",
				Usage: "Semantic segment to apply. Can be passed multiple times.",
			},
			&cli.StringSliceFlag{
				Name:  "sort",
				Usage: "Semantic sort field. Use name:asc or name:desc.",
			},
			&cli.BoolFlag{
				Name:  "export",
				Usage: "export results to a CSV file ",
			},
			&cli.IntFlag{
				Name:  "split-rows",
				Usage: "split export into multiple CSV files with at most this many rows per file (requires --export)",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "validate the query without executing it; show estimated cost and metadata when available",
			},
			&cli.BoolFlag{
				Name:  "dangerously-bypass-soft-limits",
				Usage: "bypass BigQuery soft query limits configured on the connection",
			},
			&cli.StringFlag{
				Name:    "query-annotations",
				Sources: cli.EnvVars("BRUIN_QUERY_ANNOTATIONS"),
				Usage:   fmt.Sprintf("JSON string containing annotations to be attached to the query for tracking purposes. Use '%s' to only include the default annotations.", ansisql.DefaultQueryAnnotations),
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "if you are an AI agent, use this flag to describe why you ran the query",
			},
			&cli.StringSliceFlag{
				Name:    "var",
				Usage:   "set Jinja template variables for query rendering. Supports flat (--var key=value), dot-notation nested (--var filters.start_date=2026-05-20) and JSON object/array values (--var filters='{\"start_date\":\"2026-05-20\"}').",
				Sources: cli.EnvVars("BRUIN_VARS"),
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			fs := afero.NewOsFs()
			if err := validateQueryCommandFlags(c); err != nil {
				return handleError(c.String("output"), err)
			}

			vars, err := parseQueryVars(c.StringSlice("var"))
			if err != nil {
				return handleError(c.String("output"), err)
			}

			connName, conn, queryStr, dialect, pipelineInfo, err := prepareQueryExecution(ctx, c, fs, vars)
			if err != nil {
				return handleError(c.String("output"), err)
			}

			var parser *sqlparser.SQLParser
			needsParser := c.IsSet("limit") || (pipelineInfo != nil && pipelineInfo.Config.SelectedEnvironment.SchemaPrefix != "")

			if needsParser {
				parser, err = sqlparser.NewSQLParser(false)
				if err != nil {
					return handleError(c.String("output"), errors.Wrap(err, "failed to initialize SQL parser"))
				}
				defer parser.Close()

				err = parser.Start()
				if err != nil {
					return handleError(c.String("output"), errors.Wrap(err, "failed to start SQL parser"))
				}
			}

			// Apply schema prefix if configured
			if pipelineInfo != nil && pipelineInfo.Config.SelectedEnvironment.SchemaPrefix != "" && parser != nil {
				queryStr, err = applySchemaPrefix(ctx, queryStr, dialect, parser, pipelineInfo, conn)
				if err != nil {
					return handleError(c.String("output"), errors.Wrap(err, "failed to apply schema prefix"))
				}
			}

			if c.IsSet("limit") && parser != nil {
				queryStr = addLimitToQuery(queryStr, c.Int64("limit"), conn, parser, dialect)
			}

			// Validate split-rows is only used with export
			if c.IsSet("split-rows") && !c.Bool("export") {
				return handleError(c.String("output"), errors.New("--split-rows requires --export flag"))
			}

			if c.Bool("dry-run") {
				ctx = query.WithQueryType(ctx, query.QueryTypeDryRun)
				if c.Bool("export") {
					return handleError(c.String("output"), errors.New("cannot combine --dry-run with --export"))
				}
				if c.IsSet("limit") {
					return handleError(c.String("output"), errors.New("cannot combine --dry-run with --limit; the limit would distort cost estimates"))
				}
				output := c.String("output")
				if output == "csv" {
					fmt.Fprintln(os.Stderr, "CSV output is not supported for --dry-run; falling back to plain.")
					output = outputFormatPlain
				}

				dryRunner, ok := conn.(query.QueryDryRunner)
				if !ok {
					return handleError(output, errors.Errorf("connection '%s' does not support dry-run", connName))
				}

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(c.Int("timeout"))*time.Second)
				defer timeoutCancel()

				q := query.Query{Query: queryStr}
				result, dryRunErr := dryRunner.DryRunQuery(timeoutCtx, &q)
				if dryRunErr != nil {
					return handleError(output, errors.Wrap(dryRunErr, "dry-run failed"))
				}

				return outputDryRunResult(output, connName, queryStr, result)
			}

			//nolint:nestif
			if querier, ok := conn.(schemaQuerier); ok {
				ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
				defer cancel()
				ctx = query.WithQueryType(ctx, query.QueryTypeQuery)

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(c.Int("timeout"))*time.Second)
				defer timeoutCancel()
				if !c.Bool("dangerously-bypass-soft-limits") {
					timeoutCtx = bigquery.WithSoftQueryLimits(timeoutCtx)
				}

				q := query.Query{Query: queryStr}
				annotationsInput := c.String("query-annotations")
				if os.Getenv("BRUIN_QUERY_ANNOTATIONS") != "" {
					annotationsInput = injectAgentName(annotationsInput)
				}
				tag, err := ansisql.BuildAdhocQueryTag(annotationsInput)
				if err != nil {
					return handleError(c.String("output"), err)
				}
				if tag != "" {
					if _, isSnowflake := conn.(snowflake.SfClient); isSnowflake {
						// Snowflake strips leading SQL comments, so use QUERY_TAG instead.
						timeoutCtx = gosnowflake.WithQueryTag(timeoutCtx, tag)
					} else {
						q.Query = "-- @bruin.config: " + tag + "\n" + q.Query
					}
				}

				queryStart := time.Now()
				result, queryErr := querier.SelectWithSchema(timeoutCtx, &q)

				// Save query log (for both success and error cases)
				inputPath := c.String("asset")
				logOpts := QueryLogOptions{
					QueryStartTimestamp: queryStart,
					Asset:               inputPath,
					Environment:         c.String("environment"),
					Limit:               c.Int64("limit"),
					Timeout:             c.Int("timeout"),
					Description:         c.String("description"),
				}
				if err := saveQueryLog(queryStr, connName, result, queryErr, logOpts); err != nil {
					// Log the error but don't fail the command
					fmt.Fprintf(os.Stderr, "Warning: failed to save query log: %v\n", err)
				}

				if queryErr != nil {
					return handleError(c.String("output"), errors.Wrap(queryErr, "query execution failed"))
				}

				// Output result based on format specified
				if c.Bool("export") {
					splitRows := c.Int("split-rows")
					if splitRows > 0 {
						resultsPaths, exportErr := exportResultsToMultipleCSV(result, inputPath, splitRows)
						if exportErr != nil {
							return handleError(c.String("output"), errors.Wrap(exportErr, "failed to export results to CSV"))
						}
						return handleMultipleFilesSuccess(c.String("output"), resultsPaths)
					}
					resultsPath, exportErr := exportResultsToCSV(result, inputPath)
					if exportErr != nil {
						return handleError(c.String("output"), errors.Wrap(exportErr, "failed to export results to CSV"))
					}
					successMessage := "Results Successfully exported to " + resultsPath
					return handleSuccess(c.String("output"), successMessage)
				}
				output := c.String("output")
				switch output {
				case outputFormatPlain:
					if shouldOutputExecutionSummary(result) {
						printQueryExecutionSummary(result.Execution)
					} else {
						printTable(result.Columns, result.Rows)
					}
				case "json":
					type jsonResponse struct {
						Columns   []map[string]string          `json:"columns"`
						Rows      [][]interface{}              `json:"rows"`
						ConnName  string                       `json:"connectionName"`
						Query     string                       `json:"query"`
						Execution *query.QueryExecutionSummary `json:"execution,omitempty"`
					}

					// Construct JSON response with structured columns
					jsonCols := make([]map[string]string, len(result.Columns))
					for i, colName := range result.Columns {
						jsonCols[i] = map[string]string{
							"name": colName,
							"type": result.ColumnTypes[i],
						}
					}

					// Prepare the final output struct
					finalOutput := jsonResponse{
						Columns:  jsonCols,
						Rows:     formatQueryRowsForJSON(result.Rows),
						ConnName: connName,
						Query:    queryStr,
					}
					if shouldOutputExecutionSummary(result) {
						finalOutput.Execution = result.Execution
					}

					jsonData, err := json.Marshal(finalOutput)
					if err != nil {
						return handleError(output, errors.Wrap(err, "failed to marshal result to JSON"))
					}
					fmt.Println(string(jsonData))
				case "csv":
					writer := csv.NewWriter(os.Stdout)
					defer writer.Flush()
					if shouldOutputExecutionSummary(result) {
						if err = writeExecutionSummaryCSV(writer, result.Execution); err != nil {
							return handleError(output, err)
						}
					} else {
						if err = writer.Write(result.Columns); err != nil {
							return handleError(output, errors.Wrap(err, "failed to write CSV header"))
						}
						for _, row := range result.Rows {
							rowStrings := make([]string, len(row))
							for i, val := range row {
								if val == nil {
									rowStrings[i] = ""
								} else {
									rowStrings[i] = fmt.Sprintf("%v", formatQueryCellForDisplay(val))
								}
							}
							if err = writer.Write(rowStrings); err != nil {
								return handleError(output, errors.Wrap(err, "failed to write CSV row"))
							}
						}
					}
				default:
					fmt.Printf("Invalid output type: %s\n", output)
				}
			} else {
				fmt.Printf("Connection type %s does not support querying.\n", c.String("connection"))
			}
			return nil
		},
	}
}

func validateQueryCommandFlags(c *cli.Command) error {
	return validateFlags(
		c.String("connection"),
		c.String("query"),
		c.String("asset"),
		c.String("pipeline"),
		hasSemanticQueryFlags(c),
		c.String("semantic-model"),
	)
}

func hasSemanticQueryFlags(c *cli.Command) bool {
	return c.String("semantic-model") != "" ||
		len(c.StringSlice("metric")) > 0 ||
		len(c.StringSlice("dimension")) > 0 ||
		len(c.StringSlice("filter")) > 0 ||
		len(c.StringSlice("segment")) > 0 ||
		len(c.StringSlice("sort")) > 0
}

func validateFlags(connection, query, asset, pipelinePath string, hasSemanticFlags bool, semanticModel string) error {
	hasConnection := connection != ""
	hasQuery := query != ""
	hasAsset := asset != ""
	hasPipeline := pipelinePath != ""

	if hasSemanticFlags {
		if semanticModel == "" {
			return errors.New("--semantic-model is required when using semantic query flags")
		}
		if hasQuery {
			return errors.New("semantic query mode cannot be combined with --query")
		}
		if !hasAsset && !hasPipeline {
			return errors.New("semantic query mode requires --asset or --pipeline")
		}
		if hasPipeline && !hasConnection && !hasAsset {
			return errors.New("semantic query mode with --pipeline requires --connection")
		}
		return nil
	}

	if hasPipeline {
		return errors.New("--pipeline can only be used with --semantic-model")
	}

	switch {
	case hasConnection:
		if !hasConnection || !hasQuery {
			return errors.New("direct query mode requires both --connection and --query flags")
		}
		if hasAsset {
			return errors.New("direct query mode (--connection and --query) cannot be combined with asset mode (--asset and --environment)")
		}
		return nil

	case hasAsset:
		if hasConnection {
			return errors.New("asset mode (--asset) cannot be combined with direct query mode (--connection and --query)")
		}
		return nil
	default:
		return errors.New("must use either:\n" +
			"1. Direct query mode (--connection and --query), or\n" +
			"2. Asset mode (--asset with optional --environment), or\n" +
			"3. Auto-detect mode (--asset to detect the connection and --query to run arbitrary queries)")
	}
}

func prepareQueryExecution(ctx context.Context, c *cli.Command, fs afero.Fs, vars map[string]any) (string, interface{}, string, string, *ppInfo, error) {
	assetPath := c.String("asset")
	queryStr := c.String("query")
	env := c.String("environment")
	connectionName := c.String("connection")
	s := c.String("start-date")
	e := c.String("end-date")
	logger := makeLogger(false)
	startDate, endDate, err := ParseDate(s, e, logger)
	if err != nil {
		return "", nil, "", "", nil, err
	}

	renderer := newQueryRenderer(startDate, endDate, "your-pipeline-name", vars, "")
	extractor := &query.WholeFileExtractor{
		Fs:       fs,
		Renderer: renderer,
	}

	if c.String("semantic-model") != "" {
		return prepareSemanticQueryExecution(ctx, c, fs, vars, startDate, endDate)
	}

	// Direct query mode (no asset path)
	if assetPath == "" {
		conn, connType, err := getConnectionAndTypeFromConfigWithContext(ctx, env, connectionName, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", nil, err
		}
		queryStr, err = extractQuery(queryStr, extractor)
		if err != nil {
			return "", nil, "", "", nil, err
		}
		return connectionName, conn, queryStr, sqlparser.ConnectionTypeToDialect(connType), nil, nil
	}

	if queryStr != "" {
		pipelineInfo, err := GetPipelineAndAsset(ctx, assetPath, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", nil, errors.Wrap(err, "failed to get pipeline info")
		}
		fetchCtx := context.WithValue(ctx, pipeline.RunConfigStartDate, startDate)
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigEndDate, endDate)
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigExecutionDate, defaultExecutionDate)
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigRunID, "your-run-id")
		fetchCtx = context.WithValue(fetchCtx, config.EnvironmentContextKey, pipelineInfo.Config.SelectedEnvironment)
		// Auto-detect mode (both asset path and query)
		autoRenderer := newQueryRenderer(startDate, endDate, pipelineInfo.Pipeline.Name, vars, pipelineMacroContent(pipelineInfo.Pipeline))
		extractor = &query.WholeFileExtractor{
			Fs:       fs,
			Renderer: autoRenderer,
		}

		newExtractor, err := extractor.CloneForAsset(fetchCtx, pipelineInfo.Pipeline, pipelineInfo.Asset)
		if err != nil {
			return "", nil, "", "", nil, errors.Wrapf(err, "failed to clone extractor for asset %s", pipelineInfo.Asset.Name)
		}
		if clonedRenderer, ok := newExtractor.(*query.WholeFileExtractor); ok {
			if r, ok := clonedRenderer.Renderer.(*jinja.Renderer); ok {
				for k, v := range vars {
					r.SetContextValue(k, v)
				}
			}
		}

		connName, conn, err := getConnectionFromPipelineInfoWithContext(ctx, pipelineInfo, env)
		if err != nil {
			return "", nil, "", "", nil, err
		}

		queryStr, err = extractQuery(queryStr, newExtractor)
		if err != nil {
			return "", nil, "", "", nil, err
		}

		return connName, conn, queryStr, dialectForAssetType(pipelineInfo.Asset.Type), pipelineInfo, nil
	}
	// Asset query mode (only asset path)
	pipelineInfo, err := GetPipelineAndAsset(ctx, assetPath, fs, c.String("config-file"))
	if err != nil {
		return "", nil, "", "", nil, errors.Wrap(err, "failed to get pipeline info")
	}

	fetchCtx := context.WithValue(ctx, pipeline.RunConfigStartDate, startDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigEndDate, endDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigExecutionDate, defaultExecutionDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigRunID, "your-run-id")
	fetchCtx = context.WithValue(fetchCtx, config.EnvironmentContextKey, pipelineInfo.Config.SelectedEnvironment)
	assetRenderer := newQueryRenderer(startDate, endDate, pipelineInfo.Pipeline.Name, vars, pipelineMacroContent(pipelineInfo.Pipeline))
	extractor = &query.WholeFileExtractor{
		Fs:       fs,
		Renderer: assetRenderer,
	}
	newExtractor, err := extractor.CloneForAsset(fetchCtx, pipelineInfo.Pipeline, pipelineInfo.Asset)
	if err != nil {
		return "", nil, "", "", nil, errors.Wrapf(err, "failed to clone extractor for asset %s", pipelineInfo.Asset.Name)
	}
	if clonedRenderer, ok := newExtractor.(*query.WholeFileExtractor); ok {
		if r, ok := clonedRenderer.Renderer.(*jinja.Renderer); ok {
			for k, v := range vars {
				r.SetContextValue(k, v)
			}
		}
	}
	// Verify that the asset is a SQL asset
	if !pipelineInfo.Asset.IsSQLAsset() {
		return "", nil, "", "", nil, errors.Errorf("asset '%s' is not a SQL asset (type: %s). Only SQL assets can be queried",
			assetPath,
			pipelineInfo.Asset.Type)
	}
	queryStr, err = extractQuery(pipelineInfo.Asset.ExecutableFile.Content, newExtractor)
	if err != nil {
		return "", nil, "", "", nil, err
	}
	connName, conn, err := getConnectionFromPipelineInfoWithContext(ctx, pipelineInfo, env)
	if err != nil {
		return "", nil, "", "", nil, err
	}

	return connName, conn, queryStr, dialectForAssetType(pipelineInfo.Asset.Type), pipelineInfo, nil
}

// dialectForAssetType returns the SQL parser dialect for the given asset type,
// or the empty string when the asset type has no registered dialect.
func dialectForAssetType(assetType pipeline.AssetType) string {
	dialect, err := sqlparser.AssetTypeToDialect(assetType)
	if err != nil {
		return ""
	}
	return dialect
}

func prepareSemanticQueryExecution(ctx context.Context, c *cli.Command, fs afero.Fs, vars map[string]any, startDate time.Time, endDate time.Time) (string, interface{}, string, string, *ppInfo, error) {
	semanticModelName := c.String("semantic-model")
	assetPath := c.String("asset")
	pipelinePath := c.String("pipeline")
	env := c.String("environment")
	connectionName := c.String("connection")

	var pipelineInfo *ppInfo
	var err error
	if assetPath != "" {
		pipelineInfo, err = GetPipelineAndAsset(ctx, assetPath, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", nil, errors.Wrap(err, "failed to get pipeline info")
		}
	} else {
		pipelineInfo, err = GetPipelineForQuery(ctx, pipelinePath, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", nil, errors.Wrap(err, "failed to get pipeline info")
		}
	}

	semanticInputPath := pipelinePath
	if assetPath != "" {
		semanticInputPath = assetPath
	}
	models, semanticPath, err := loadRepoSemanticModels(fs, pipelineInfo.Config.Path(), semanticInputPath)
	if err != nil {
		return "", nil, "", "", nil, errors.Wrap(err, "failed to load repo semantic models")
	}

	model, ok := models[semanticModelName]
	if !ok {
		return "", nil, "", "", nil, errors.Errorf("semantic model %q not found in %s", semanticModelName, semanticPath)
	}

	semanticQuery, err := semanticQueryFromCommand(c)
	if err != nil {
		return "", nil, "", "", nil, err
	}

	engine, err := semantic.NewEngineWithModels(model, models)
	if err != nil {
		return "", nil, "", "", nil, errors.Wrapf(err, "failed to initialize semantic model %q", semanticModelName)
	}
	compiledQuery, err := engine.GenerateSQL(semanticQuery)
	if err != nil {
		return "", nil, "", "", nil, errors.Wrap(err, "failed to compile semantic query")
	}

	connName, conn, err := getSemanticQueryConnection(ctx, pipelineInfo, env, connectionName, assetPath != "")
	if err != nil {
		return "", nil, "", "", nil, err
	}

	if assetPath != "" {
		fetchCtx := semanticQueryContext(ctx, pipelineInfo, startDate, endDate)
		assetRenderer := newSemanticRenderer(pipelineInfo, vars, startDate, endDate)
		var extractor query.QueryExtractor = &query.WholeFileExtractor{
			Fs:       fs,
			Renderer: assetRenderer,
		}
		clonedExtractor, err := extractor.CloneForAsset(fetchCtx, pipelineInfo.Pipeline, pipelineInfo.Asset)
		if err != nil {
			return "", nil, "", "", nil, errors.Wrapf(err, "failed to clone extractor for asset %s", pipelineInfo.Asset.Name)
		}
		extractor = clonedExtractor
		if clonedRenderer, ok := clonedExtractor.(*query.WholeFileExtractor); ok {
			if r, ok := clonedRenderer.Renderer.(*jinja.Renderer); ok {
				for k, v := range vars {
					r.SetContextValue(k, v)
				}
			}
		}
		queryStr, err := extractQuery(compiledQuery, extractor)
		if err != nil {
			return "", nil, "", "", nil, err
		}

		return connName, conn, queryStr, dialectForAssetType(pipelineInfo.Asset.Type), pipelineInfo, nil
	} else {
		renderer := newSemanticRenderer(pipelineInfo, vars, startDate, endDate)
		extractor := &query.WholeFileExtractor{
			Fs:       fs,
			Renderer: renderer,
		}
		queryStr, err := extractQuery(compiledQuery, extractor)
		if err != nil {
			return "", nil, "", "", nil, err
		}
		dialect := sqlparser.ConnectionTypeToDialect(pipelineInfo.Config.SelectedEnvironment.Connections.ConnectionsSummaryList()[connName])
		return connName, conn, queryStr, dialect, pipelineInfo, nil
	}
}

func getSemanticQueryConnection(ctx context.Context, pipelineInfo *ppInfo, env, connectionName string, hasAsset bool) (string, interface{}, error) {
	if connectionName != "" {
		return getConnectionByNameFromPipelineInfoWithContext(ctx, pipelineInfo, env, connectionName)
	}
	if hasAsset {
		return getConnectionFromPipelineInfoWithContext(ctx, pipelineInfo, env)
	}
	return "", nil, errors.New("semantic query mode with --pipeline requires --connection")
}

func loadRepoSemanticModels(fs afero.Fs, configFilePath, inputPath string) (map[string]*semantic.Model, string, error) {
	semanticRoot := ""
	if configFilePath != "" {
		semanticRoot = filepath.Dir(configFilePath)
	}
	if semanticRoot == "" {
		repoRoot, err := git.FindRepoFromPath(inputPath)
		if err != nil {
			return nil, "", err
		}
		semanticRoot = repoRoot.Path
	}

	semanticPath := filepath.Join(semanticRoot, "semantic")
	models, err := semantic.LoadDirFS(fs, semanticPath)
	if err != nil {
		return nil, semanticPath, err
	}
	return models, semanticPath, nil
}

func semanticQueryContext(ctx context.Context, pipelineInfo *ppInfo, startDate time.Time, endDate time.Time) context.Context {
	fetchCtx := context.WithValue(ctx, pipeline.RunConfigStartDate, startDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigEndDate, endDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigExecutionDate, defaultExecutionDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigRunID, "your-run-id")
	fetchCtx = context.WithValue(fetchCtx, config.EnvironmentContextKey, pipelineInfo.Config.SelectedEnvironment)
	return fetchCtx
}

func newSemanticRenderer(pipelineInfo *ppInfo, vars map[string]any, startDate time.Time, endDate time.Time) *jinja.Renderer {
	renderer := jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, &defaultExecutionDate, pipelineInfo.Pipeline.Name, "your-run-id", pipelineInfo.Pipeline.Variables.Value(), pipelineMacroContent(pipelineInfo.Pipeline))
	for k, v := range vars {
		renderer.SetContextValue(k, v)
	}
	return renderer
}

func newQueryRenderer(startDate, endDate time.Time, pipelineName string, vars map[string]any, macroContent string) *jinja.Renderer {
	renderer := jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, &defaultExecutionDate, pipelineName, "your-run-id", nil, macroContent)
	for k, v := range vars {
		renderer.SetContextValue(k, v)
	}
	return renderer
}

func pipelineMacroContent(pl *pipeline.Pipeline) string {
	if pl == nil || len(pl.Macros) == 0 {
		return ""
	}

	var macroContent strings.Builder
	for _, macro := range pl.Macros {
		macroContent.WriteString(string(macro))
		macroContent.WriteString("\n")
	}

	return macroContent.String()
}

func semanticQueryFromCommand(c *cli.Command) (*semantic.Query, error) {
	q := &semantic.Query{}

	for _, raw := range c.StringSlice("dimension") {
		dim, err := parseSemanticDimensionRef(raw)
		if err != nil {
			return nil, err
		}
		q.Dimensions = append(q.Dimensions, dim)
	}
	q.Metrics = append(q.Metrics, c.StringSlice("metric")...)
	filters, err := semanticFilterInputs(c.StringSlice("filter"))
	if err != nil {
		return nil, err
	}
	for _, raw := range filters {
		var filter semantic.Filter
		if err := json.Unmarshal([]byte(raw), &filter); err != nil {
			return nil, errors.Wrapf(err, "failed to parse semantic filter %q", raw)
		}
		q.Filters = append(q.Filters, filter)
	}
	q.Segments = append(q.Segments, c.StringSlice("segment")...)
	for _, raw := range c.StringSlice("sort") {
		sortSpec, err := parseSemanticSortSpec(raw)
		if err != nil {
			return nil, err
		}
		q.Sort = append(q.Sort, sortSpec)
	}

	return q, nil
}

func semanticFilterInputs(rawFilters []string) ([]string, error) {
	var filters []string
	var current strings.Builder
	depth := 0
	inString := false
	escaped := false

	for _, raw := range rawFilters {
		part := strings.TrimSpace(raw)
		if part == "" {
			continue
		}

		if current.Len() > 0 {
			current.WriteByte(',')
		}
		current.WriteString(part)

		for i := range len(part) {
			ch := part[i]
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' && inString {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = !inString
				continue
			}
			if inString {
				continue
			}

			switch ch {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}

		if depth < 0 {
			return nil, errors.Errorf("invalid semantic filter JSON: %s", current.String())
		}
		if depth == 0 && !inString {
			filters = append(filters, current.String())
			current.Reset()
		}
	}

	if current.Len() > 0 || depth != 0 || inString {
		return nil, errors.Errorf("incomplete semantic filter JSON: %s", current.String())
	}

	return filters, nil
}

func parseSemanticDimensionRef(raw string) (semantic.DimensionRef, error) {
	parts := strings.Split(raw, ":")
	switch len(parts) {
	case 1:
		if strings.TrimSpace(parts[0]) == "" {
			return semantic.DimensionRef{}, errors.New("semantic dimension name cannot be empty")
		}
		return semantic.DimensionRef{Name: strings.TrimSpace(parts[0])}, nil
	case 2:
		name := strings.TrimSpace(parts[0])
		granularity := strings.TrimSpace(parts[1])
		if name == "" || granularity == "" {
			return semantic.DimensionRef{}, errors.Errorf("invalid semantic dimension %q: expected name or name:granularity", raw)
		}
		return semantic.DimensionRef{Name: name, Granularity: granularity}, nil
	default:
		return semantic.DimensionRef{}, errors.Errorf("invalid semantic dimension %q: expected name or name:granularity", raw)
	}
}

func parseSemanticSortSpec(raw string) (semantic.SortSpec, error) {
	parts := strings.Split(raw, ":")
	switch len(parts) {
	case 1:
		name := strings.TrimSpace(parts[0])
		if name == "" {
			return semantic.SortSpec{}, errors.New("semantic sort name cannot be empty")
		}
		return semantic.SortSpec{Name: name}, nil
	case 2:
		name := strings.TrimSpace(parts[0])
		direction := strings.ToLower(strings.TrimSpace(parts[1]))
		if name == "" || direction == "" {
			return semantic.SortSpec{}, errors.Errorf("invalid semantic sort %q: expected name, name:asc, or name:desc", raw)
		}
		if direction != "asc" && direction != "desc" {
			return semantic.SortSpec{}, errors.Errorf("invalid semantic sort direction %q: expected asc or desc", direction)
		}
		return semantic.SortSpec{Name: name, Direction: direction}, nil
	default:
		return semantic.SortSpec{}, errors.Errorf("invalid semantic sort %q: expected name, name:asc, or name:desc", raw)
	}
}

func getConnectionFromConfigWithContext(ctx context.Context, env string, connectionName string, fs afero.Fs, configFilePath string) (interface{}, error) {
	conn, _, err := getConnectionAndTypeFromConfigWithContext(ctx, env, connectionName, fs, configFilePath)
	return conn, err
}

func getConnectionAndTypeFromConfigWithContext(ctx context.Context, env string, connectionName string, fs afero.Fs, configFilePath string) (interface{}, string, error) {
	repoRoot, err := git.FindRepoFromPath(".")
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to find the git repository root")
	}

	if configFilePath == "" {
		configFilePath = filepath.Join(repoRoot.Path, ".bruin.yml")
	}
	cm, err := config.LoadOrCreate(fs, configFilePath)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to load or create config")
	}

	if env != "" {
		err := cm.SelectEnvironment(env)
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to use the environment '%s'", env)
		}
	}

	manager, errs := connection.NewManagerFromConfigWithContext(ctx, cm)
	if len(errs) > 0 {
		return nil, "", errors.Wrap(errs[0], "failed to create connection manager")
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return nil, "", &config.MissingConnectionError{
			Name:            connectionName,
			ConfigFilePath:  configFilePath,
			EnvironmentName: cm.SelectedEnvironmentName,
		}
	}

	return conn, manager.GetConnectionType(connectionName), nil
}

func extractQuery(content string, extractor query.QueryExtractor) (string, error) {
	// Extract the query from the asset
	queries, err := extractor.ExtractQueriesFromString(content)
	if err != nil {
		return "", errors.Wrap(err, "failed to extract query")
	}

	if len(queries) == 0 {
		return "", errors.New("no query found in asset")
	}

	return queries[0].Query, nil
}

func getConnectionFromPipelineInfoWithContext(ctx context.Context, pipelineInfo *ppInfo, env string) (string, interface{}, error) {
	if env != "" {
		err := pipelineInfo.Config.SelectEnvironment(env)
		if err != nil {
			return "", nil, errors.Wrapf(err, "failed to use the environment '%s'", env)
		}
	}

	// Get connection info
	manager, errs := connection.NewManagerFromConfigWithContext(ctx, pipelineInfo.Config)
	if len(errs) > 0 {
		return "", nil, errors.Wrap(errs[0], "failed to create connection manager")
	}

	connName, err := pipelineInfo.Pipeline.GetConnectionNameForAsset(pipelineInfo.Asset)
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to get connection")
	}

	conn := manager.GetConnection(connName)
	if conn == nil {
		return "", nil, &config.MissingConnectionError{
			Name:            connName,
			ConfigFilePath:  pipelineInfo.Config.Path(),
			EnvironmentName: pipelineInfo.Config.SelectedEnvironmentName,
		}
	}

	return connName, conn, nil
}

func getConnectionByNameFromPipelineInfoWithContext(ctx context.Context, pipelineInfo *ppInfo, env string, connName string) (string, interface{}, error) {
	if env != "" {
		err := pipelineInfo.Config.SelectEnvironment(env)
		if err != nil {
			return "", nil, errors.Wrapf(err, "failed to use the environment '%s'", env)
		}
	}

	manager, errs := connection.NewManagerFromConfigWithContext(ctx, pipelineInfo.Config)
	if len(errs) > 0 {
		return "", nil, errors.Wrap(errs[0], "failed to create connection manager")
	}

	conn := manager.GetConnection(connName)
	if conn == nil {
		return "", nil, &config.MissingConnectionError{
			Name:            connName,
			ConfigFilePath:  pipelineInfo.Config.Path(),
			EnvironmentName: pipelineInfo.Config.SelectedEnvironmentName,
		}
	}

	return connName, conn, nil
}

type Limiter interface {
	Limit(query string, limit int64) string
}

func addLimitToQuery(query string, limit int64, conn interface{}, parser *sqlparser.SQLParser, dialect string) string {
	// Check if the query is a single SELECT statement before applying limit
	if parser != nil {
		isSingleSelect, err := parser.IsSingleSelectQuery(query, dialect)
		if err == nil && !isSingleSelect {
			// Not a single SELECT query, return the original query without limit
			return query
		}
		// If there's an error checking or it is a single SELECT, proceed with adding limit
	}

	var err error
	var limitedQuery string
	if parser != nil {
		limitedQuery, err = parser.AddLimit(query, int(limit), dialect)
	}
	if err != nil || parser == nil {
		l, ok := conn.(Limiter)
		if ok {
			return l.Limit(query, limit)
		} else {
			query = strings.TrimRight(query, "; \n\t")
			return fmt.Sprintf("SELECT * FROM (\n%s\n) as t LIMIT %d", query, limit)
		}
	}
	return limitedQuery
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
			rowData[i] = fmt.Sprintf("%v", formatQueryCellForDisplay(cell))
		}
		t.AppendRow(rowData)
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}

func shouldOutputExecutionSummary(result *query.QueryResult) bool {
	return result != nil && result.Execution != nil && len(result.Rows) == 0
}

func printQueryExecutionSummary(summary *query.QueryExecutionSummary) {
	fmt.Println("Statement executed successfully")

	for _, row := range queryExecutionSummaryRows(summary) {
		fmt.Printf("%s: %s\n", row[0], row[1])
	}
}

func writeExecutionSummaryCSV(writer *csv.Writer, summary *query.QueryExecutionSummary) error {
	if err := writer.Write([]string{"metric", "value"}); err != nil {
		return errors.Wrap(err, "failed to write CSV header")
	}

	for _, row := range queryExecutionSummaryRows(summary) {
		if err := writer.Write(row); err != nil {
			return errors.Wrap(err, "failed to write CSV row")
		}
	}

	return nil
}

func queryExecutionSummaryRows(summary *query.QueryExecutionSummary) [][]string {
	if summary == nil {
		return nil
	}

	rows := make([][]string, 0)
	if summary.StatementType != "" {
		rows = append(rows, []string{"Statement type", summary.StatementType})
	}
	if summary.DMLAffectedRows != nil {
		rows = append(rows, []string{"Rows affected", formatNumber(*summary.DMLAffectedRows)})
	}
	if summary.DMLStats != nil {
		if summary.DMLStats.InsertedRowCount > 0 {
			rows = append(rows, []string{"Rows inserted", formatNumber(summary.DMLStats.InsertedRowCount)})
		}
		if summary.DMLStats.DeletedRowCount > 0 {
			rows = append(rows, []string{"Rows deleted", formatNumber(summary.DMLStats.DeletedRowCount)})
		}
		if summary.DMLStats.UpdatedRowCount > 0 {
			rows = append(rows, []string{"Rows modified", formatNumber(summary.DMLStats.UpdatedRowCount)})
		}
	}
	if summary.DDLOperationPerformed != "" {
		rows = append(rows, []string{"DDL operation", summary.DDLOperationPerformed})
	}
	if summary.DDLTargetTable != "" {
		rows = append(rows, []string{"DDL target table", summary.DDLTargetTable})
	}
	if summary.DDLTargetRoutine != "" {
		rows = append(rows, []string{"DDL target routine", summary.DDLTargetRoutine})
	}
	if summary.TotalBytesProcessed > 0 {
		rows = append(rows, []string{"Bytes processed", formatBytes(summary.TotalBytesProcessed)})
	}
	if summary.TotalBytesBilled > 0 {
		rows = append(rows, []string{"Bytes billed", formatBytes(summary.TotalBytesBilled)})
	}
	if summary.SlotMillis > 0 {
		rows = append(rows, []string{"Slot time", formatSlotMillis(summary.SlotMillis)})
	}
	if summary.JobID != "" {
		rows = append(rows, []string{"Job", summary.JobID})
	}

	return rows
}

func formatSlotMillis(slotMillis int64) string {
	return (time.Duration(slotMillis) * time.Millisecond).String()
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

func GetPipelineAndAsset(ctx context.Context, inputPath string, fs afero.Fs, configFilePath string) (*ppInfo, error) {
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
	pipelinePath, err := path.GetPipelineRootFromTask(inputPath, PipelineDefinitionFiles)
	if err != nil {
		errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
		return nil, err
	}
	if configFilePath == "" {
		configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
	}
	cm, err := config.LoadOrCreate(fs, configFilePath)
	if err != nil {
		errorPrinter.Printf("Failed to load the config file at '%s': %v\n", configFilePath, err)
		return nil, err
	}
	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
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

	task, err = DefaultPipelineBuilder.MutateAsset(ctx, task, foundPipeline)
	if err != nil {
		errorPrinter.Printf("Failed to mutate asset '%s': %v\n", task.Name, err)
		return nil, err
	}

	return &ppInfo{
		Pipeline: foundPipeline,
		Asset:    task,
		Config:   cm,
	}, nil
}

func GetPipelineForQuery(ctx context.Context, inputPath string, fs afero.Fs, configFilePath string) (*ppInfo, error) {
	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
		return nil, err
	}

	if configFilePath == "" {
		configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
	}
	cm, err := config.LoadOrCreate(fs, configFilePath)
	if err != nil {
		errorPrinter.Printf("Failed to load the config file at '%s': %v\n", configFilePath, err)
		return nil, err
	}
	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, inputPath, pipeline.WithMutate())
	if err != nil {
		errorPrinter.Println("failed to get the pipeline, are you sure you have referred the right path?")
		return nil, err
	}

	return &ppInfo{
		Pipeline: foundPipeline,
		Config:   cm,
	}, nil
}

func exportResultsToCSV(results *query.QueryResult, inputPath string) (string, error) {
	if inputPath == "" {
		inputPath = "."
	}
	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		return "", err
	}
	resultName := fmt.Sprintf("query_result_%d.csv", time.Now().UnixMilli())
	resultsPath := filepath.Join(repoRoot.Path, "logs/exports", resultName)
	err = git.EnsureGivenPatternIsInGitignore(afero.NewOsFs(), repoRoot.Path, "logs/exports")
	if err != nil {
		return "", err
	}

	err = os.MkdirAll(filepath.Dir(resultsPath), 0o755)
	if err != nil {
		return "", err
	}

	file, err := os.Create(resultsPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err = writer.Write(results.Columns); err != nil {
		return "", err
	}
	for _, row := range results.Rows {
		rowStrings := make([]string, len(row))
		for i, val := range row {
			if val == nil {
				rowStrings[i] = ""
			} else {
				rowStrings[i] = fmt.Sprintf("%v", formatQueryCellForDisplay(val))
			}
		}
		if err = writer.Write(rowStrings); err != nil {
			return "", err
		}
	}

	return resultsPath, nil
}

func exportResultsToMultipleCSV(results *query.QueryResult, inputPath string, splitRows int) ([]string, error) {
	if inputPath == "" {
		inputPath = "."
	}
	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		return nil, err
	}

	err = git.EnsureGivenPatternIsInGitignore(afero.NewOsFs(), repoRoot.Path, "logs/exports")
	if err != nil {
		return nil, err
	}

	exportsDir := filepath.Join(repoRoot.Path, "logs/exports")
	err = os.MkdirAll(exportsDir, 0o755)
	if err != nil {
		return nil, err
	}

	totalRows := len(results.Rows)
	if totalRows == 0 {
		resultName := fmt.Sprintf("query_result_%d_part1.csv", time.Now().UnixMilli())
		resultsPath := filepath.Join(exportsDir, resultName)
		if err = writeCSVFile(resultsPath, results.Columns, nil); err != nil {
			return nil, err
		}
		return []string{resultsPath}, nil
	}

	numFiles := (totalRows + splitRows - 1) / splitRows
	timestamp := time.Now().UnixMilli()
	resultsPaths := make([]string, 0, numFiles)

	for i := range numFiles {
		startIdx := i * splitRows
		endIdx := startIdx + splitRows
		if endIdx > totalRows {
			endIdx = totalRows
		}

		resultName := fmt.Sprintf("query_result_%d_part%d.csv", timestamp, i+1)
		resultsPath := filepath.Join(exportsDir, resultName)

		if err = writeCSVFile(resultsPath, results.Columns, results.Rows[startIdx:endIdx]); err != nil {
			return nil, err
		}
		resultsPaths = append(resultsPaths, resultsPath)
	}

	return resultsPaths, nil
}

func writeCSVFile(path string, columns []string, rows [][]interface{}) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err = writer.Write(columns); err != nil {
		return err
	}

	for _, row := range rows {
		rowStrings := make([]string, len(row))
		for i, val := range row {
			if val == nil {
				rowStrings[i] = ""
			} else {
				rowStrings[i] = fmt.Sprintf("%v", formatQueryCellForDisplay(val))
			}
		}
		if err = writer.Write(rowStrings); err != nil {
			return err
		}
	}

	return nil
}

func applySchemaPrefix(_ context.Context, queryStr, dialect string, parser *sqlparser.SQLParser, pipelineInfo *ppInfo, conn interface{}) (string, error) {
	if dialect == "" {
		// If no dialect, we can't rewrite the query
		return queryStr, nil
	}

	env := pipelineInfo.Config.SelectedEnvironment
	if env.SchemaPrefix == "" {
		return queryStr, nil
	}

	// Get used tables
	usedTables, err := parser.UsedTables(queryStr, dialect)
	if err != nil {
		// If we can't parse tables, return the original query without modification
		return queryStr, nil //nolint:nilerr // intentionally ignoring parse errors to return original query
	}

	if len(usedTables) == 0 {
		return queryStr, nil
	}

	// Check if connection supports getting database summary for schema validation
	// Note: This could be extended in the future to validate table existence
	_ = conn

	renameMapping := map[string]string{}

	// Build rename mapping for tables that should use prefixed schema
	for _, tableReference := range usedTables {
		parts := strings.Split(tableReference, ".")
		if len(parts) != 2 {
			continue
		}
		schema := parts[0]
		table := parts[1]
		devSchema := env.SchemaPrefix + schema
		devTable := fmt.Sprintf("%s.%s", devSchema, table)

		// For now, we'll rename all schema.table references to prefix_schema.table
		// In a production implementation, you might want to check if the prefixed table exists
		renameMapping[tableReference] = devTable
	}

	if len(renameMapping) == 0 {
		return queryStr, nil
	}

	// Rewrite the query with the new table names
	rewrittenQuery, err := parser.RenameTables(queryStr, dialect, renameMapping)
	if err != nil {
		return "", errors.Wrap(err, "failed to rewrite query with schema prefix")
	}

	return rewrittenQuery, nil
}

func handleSuccess(output string, message string) error {
	if output == "json" {
		message = strings.TrimPrefix(message, "Results Successfully exported to ")
		jsonSuccessMessage, err := json.Marshal(map[string]string{"Results Successfully exported to": message})
		if err != nil {
			fmt.Println("Error:", err.Error())
			return cli.Exit("", 1)
		}
		fmt.Println(string(jsonSuccessMessage))
	} else {
		successPrinter.Printf("%s\n", message)
	}
	return nil
}

func handleMultipleFilesSuccess(output string, paths []string) error {
	if output == "json" {
		jsonSuccessMessage, err := json.Marshal(map[string]interface{}{
			"message": "Results successfully exported",
			"files":   paths,
			"count":   len(paths),
		})
		if err != nil {
			fmt.Println("Error:", err.Error())
			return cli.Exit("", 1)
		}
		fmt.Println(string(jsonSuccessMessage))
	} else {
		successPrinter.Printf("Results successfully exported to %d files:\n", len(paths))
		for _, p := range paths {
			successPrinter.Printf("  - %s\n", p)
		}
	}
	return nil
}

// QueryLog represents the structure of a query log entry.
type QueryLog struct {
	Query               string          `json:"query"`
	QueryStartTimestamp time.Time       `json:"query_start_timestamp"`
	Timestamp           time.Time       `json:"timestamp"`
	Connection          string          `json:"connection"`
	Success             bool            `json:"success"`
	Columns             []string        `json:"columns,omitempty"`
	Rows                [][]interface{} `json:"rows,omitempty"`
	Error               string          `json:"error,omitempty"`
	Asset               string          `json:"asset,omitempty"`
	Environment         string          `json:"environment,omitempty"`
	Limit               int64           `json:"limit,omitempty"`
	Timeout             int             `json:"timeout,omitempty"`
	Description         string          `json:"description,omitempty"`
}

// QueryLogOptions contains optional parameters for query logging.
type QueryLogOptions struct {
	QueryStartTimestamp time.Time
	Asset               string
	Environment         string
	Limit               int64
	Timeout             int
	Description         string
}

func saveQueryLog(queryStr string, connName string, result *query.QueryResult, queryErr error, opts QueryLogOptions) error {
	basePath := opts.Asset
	if basePath == "" {
		basePath = "."
	}
	repoRoot, err := git.FindRepoFromPath(basePath)
	if err != nil {
		return errors.Wrap(err, "failed to find repo root")
	}

	logDir := filepath.Join(repoRoot.Path, "logs/queries")

	err = git.EnsureGivenPatternIsInGitignore(afero.NewOsFs(), repoRoot.Path, "logs/queries")
	if err != nil {
		return errors.Wrap(err, "failed to add logs/queries to .gitignore")
	}

	err = os.MkdirAll(logDir, 0o755)
	if err != nil {
		return errors.Wrap(err, "failed to create logs/queries directory")
	}

	timestamp := time.Now()
	logFileName := fmt.Sprintf("query_%d.json", timestamp.UnixMilli())
	logPath := filepath.Join(logDir, logFileName)

	logEntry := QueryLog{
		Query:               queryStr,
		QueryStartTimestamp: opts.QueryStartTimestamp,
		Timestamp:           timestamp,
		Connection:          connName,
		Success:             queryErr == nil,
		Asset:               opts.Asset,
		Environment:         opts.Environment,
		Limit:               opts.Limit,
		Timeout:             opts.Timeout,
		Description:         opts.Description,
	}

	if queryErr != nil {
		logEntry.Error = queryErr.Error()
	} else if result != nil {
		logEntry.Columns = result.Columns
		logEntry.Rows = formatQueryRowsForJSON(result.Rows)
	}

	jsonData, err := json.MarshalIndent(logEntry, "", "  ")
	if err != nil {
		return errors.Wrap(err, "failed to marshal query log to JSON")
	}

	err = os.WriteFile(logPath, jsonData, 0o600)
	if err != nil {
		return errors.Wrap(err, "failed to write query log file")
	}

	return nil
}

func formatQueryCellForDisplay(cell interface{}) interface{} {
	switch v := cell.(type) {
	case *big.Rat:
		return formatBigRatAsDecimal(v)
	case big.Rat:
		vcopy := v
		return formatBigRatAsDecimal(&vcopy)
	default:
		return cell
	}
}

func formatQueryCellForJSON(cell interface{}) interface{} {
	switch v := cell.(type) {
	case *big.Rat:
		if v == nil {
			return nil
		}
		return json.Number(formatBigRatAsDecimal(v))
	case big.Rat:
		vcopy := v
		return json.Number(formatBigRatAsDecimal(&vcopy))
	default:
		return cell
	}
}

func formatQueryRowsForJSON(rows [][]interface{}) [][]interface{} {
	formattedRows := make([][]interface{}, len(rows))
	for rowIdx, row := range rows {
		formattedRow := make([]interface{}, len(row))
		for colIdx, cell := range row {
			formattedRow[colIdx] = formatQueryCellForJSON(cell)
		}
		formattedRows[rowIdx] = formattedRow
	}

	return formattedRows
}

func formatBigRatAsDecimal(rat *big.Rat) string {
	if rat == nil {
		return ""
	}

	// BigQuery NUMERIC/BIGNUMERIC scale is up to 38 decimal points.
	return trimDecimalString(rat.FloatString(38))
}

// injectAgentName forces type=bruin_agent in the annotations payload so the
// baseline adhoc_query type is overridden. Used when the annotations value
// came from the BRUIN_QUERY_ANNOTATIONS env var so downstream systems can
// distinguish agent-initiated queries.
func injectAgentName(annotations string) string {
	merged := map[string]interface{}{}
	if annotations != "" && annotations != ansisql.DefaultQueryAnnotations {
		if err := json.Unmarshal([]byte(annotations), &merged); err != nil {
			merged = map[string]interface{}{}
		}
	}
	merged["type"] = "bruin_agent"
	b, err := json.Marshal(merged)
	if err != nil {
		return annotations
	}
	return string(b)
}

// parseQueryVars parses --var flags into a (possibly nested) map of values.
//
// It supports three forms, matching how the dashboard runtime injects variables:
//   - flat:         --var start_date=2026-05-20            => {"start_date": "2026-05-20"}
//   - dot-notation: --var filters.start_date=2026-05-20    => {"filters": {"start_date": "2026-05-20"}}
//   - JSON values:  --var filters='{"start_date":"x"}'     => {"filters": {"start_date": "x"}}
//
// Scalar values are kept as literal strings (matching how pipeline variables work
// in YAML); only values that look like a JSON object or array are parsed as JSON.
func parseQueryVars(rawVars []string) (map[string]any, error) {
	vars := make(map[string]any)

	for _, v := range rawVars {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid variable %q: must be in key=value format", v)
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			return nil, fmt.Errorf("invalid variable %q: key must not be empty", v)
		}

		value := parseQueryVarValue(strings.TrimSpace(parts[1]))
		if err := setNestedVar(vars, key, value); err != nil {
			return nil, fmt.Errorf("invalid variable %q: %w", v, err)
		}
	}

	return vars, nil
}

// parseQueryVarValue parses a raw --var value. Values that look like a JSON object
// or array are parsed as JSON so callers can pass nested structures; everything else
// is kept as a literal string.
func parseQueryVarValue(value string) any {
	if value == "" {
		return value
	}

	switch value[0] {
	case '{', '[':
		var parsed any
		if err := json.Unmarshal([]byte(value), &parsed); err == nil {
			return parsed
		}
	}

	return value
}

// mergeVarValue merges an incoming value onto an existing one. When both are
// objects they are deep-merged (so dot-notation and JSON assignments to the same
// key combine in either order), with incoming leaf values winning on conflicts.
// Otherwise the incoming value replaces the existing one.
func mergeVarValue(existing, incoming any) any {
	existingMap, ok := existing.(map[string]any)
	if !ok {
		return incoming
	}
	incomingMap, ok := incoming.(map[string]any)
	if !ok {
		return incoming
	}

	for key, value := range incomingMap {
		existingMap[key] = mergeVarValue(existingMap[key], value)
	}
	return existingMap
}

// setNestedVar assigns value to a dot-notation key inside vars, creating
// intermediate maps as needed and merging into existing ones.
func setNestedVar(vars map[string]any, key string, value any) error {
	segments := strings.Split(key, ".")
	current := vars

	for i, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return fmt.Errorf("key %q has an empty segment", key)
		}

		if i == len(segments)-1 {
			current[segment] = mergeVarValue(current[segment], value)
			return nil
		}

		existing, ok := current[segment]
		if !ok {
			next := make(map[string]any)
			current[segment] = next
			current = next
			continue
		}

		next, ok := existing.(map[string]any)
		if !ok {
			return fmt.Errorf("key segment %q is already set to a non-object value", segment)
		}
		current = next
	}

	return nil
}

func trimDecimalString(value string) string {
	if !strings.Contains(value, ".") {
		return value
	}

	trimmed := strings.TrimRight(strings.TrimRight(value, "0"), ".")
	if trimmed == "" || trimmed == "-" {
		return "0"
	}

	return trimmed
}

func outputDryRunResult(output, connName, queryStr string, result *query.DryRunResult) error {
	switch output {
	case "json":
		return outputDryRunJSON(connName, queryStr, result)
	case outputFormatPlain:
		return outputDryRunPlain(connName, result)
	default:
		return handleError(output, errors.Errorf("unsupported output format for dry-run: %s", output))
	}
}

func outputDryRunPlain(connName string, result *query.DryRunResult) error {
	fmt.Println("Dry Run Results")
	fmt.Printf("Connection:  %s (%s)\n", connName, result.ConnectionType)
	fmt.Printf("Valid:       %s\n", formatBool(result.Valid))

	if result.TotalBytesProcessed > 0 || result.EstimatedCostUSD > 0 {
		fmt.Println()
		fmt.Printf("Estimated bytes processed: %s\n", formatBytes(result.TotalBytesProcessed))
		fmt.Printf("Estimated cost:            %s\n", formatCost(result.EstimatedCostUSD))
	}

	if result.StatementType != "" {
		fmt.Printf("Statement type:            %s\n", result.StatementType)
	}

	if len(result.ReferencedTables) > 0 {
		fmt.Println()
		fmt.Println("Referenced tables:")
		for _, t := range result.ReferencedTables {
			fmt.Printf("  - %s\n", t)
		}
	}

	if len(result.Schema) > 0 {
		fmt.Println()
		fmt.Println("Output schema:")
		cols := []string{"Column Name", "Type"}
		rows := make([][]interface{}, len(result.Schema))
		for i, col := range result.Schema {
			rows[i] = []interface{}{col.Name, col.Type}
		}
		printTable(cols, rows)
	}

	if result.ExplainRows != nil && len(result.ExplainRows.Rows) > 0 {
		fmt.Println()
		fmt.Println("Query plan:")
		printTable(result.ExplainRows.Columns, result.ExplainRows.Rows)
	}

	return nil
}

func outputDryRunJSON(connName, queryStr string, result *query.DryRunResult) error {
	type dryRunJSONResponse struct {
		ConnectionName      string               `json:"connectionName"`
		ConnectionType      string               `json:"connectionType"`
		Query               string               `json:"query"`
		Valid               bool                 `json:"valid"`
		TotalBytesProcessed int64                `json:"totalBytesProcessed,omitempty"`
		EstimatedCostUSD    float64              `json:"estimatedCostUSD,omitempty"`
		StatementType       string               `json:"statementType,omitempty"`
		ReferencedTables    []string             `json:"referencedTables,omitempty"`
		Schema              []query.DryRunColumn `json:"schema,omitempty"`
		ExplainColumns      []string             `json:"explainColumns,omitempty"`
		ExplainPlan         [][]interface{}      `json:"explainPlan,omitempty"`
	}

	resp := dryRunJSONResponse{
		ConnectionName:      connName,
		ConnectionType:      result.ConnectionType,
		Query:               queryStr,
		Valid:               result.Valid,
		TotalBytesProcessed: result.TotalBytesProcessed,
		EstimatedCostUSD:    result.EstimatedCostUSD,
		StatementType:       result.StatementType,
		ReferencedTables:    result.ReferencedTables,
		Schema:              result.Schema,
	}

	if result.ExplainRows != nil {
		resp.ExplainColumns = result.ExplainRows.Columns
		resp.ExplainPlan = result.ExplainRows.Rows
	}

	jsonData, err := json.Marshal(resp)
	if err != nil {
		return handleError("json", errors.Wrap(err, "failed to marshal dry-run result to JSON"))
	}
	fmt.Println(string(jsonData))
	return nil
}

func formatCost(cost float64) string {
	switch {
	case cost == 0:
		return "$0.00"
	case cost < 0.01:
		return fmt.Sprintf("$%.6f", cost)
	default:
		return fmt.Sprintf("$%.2f", cost)
	}
}

func formatBool(v bool) string {
	if v {
		return "Yes"
	}
	return "No"
}
