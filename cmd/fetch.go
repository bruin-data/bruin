package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	path2 "path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
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
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for executing the query.",
			},
			&cli.BoolFlag{
				Name:  "export",
				Usage: "export results to a CSV file ",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			fs := afero.NewOsFs()
			if err := validateFlags(c.String("connection"), c.String("query"), c.String("asset")); err != nil {
				return handleError(c.String("output"), err)
			}

			connName, conn, queryStr, assetType, err := prepareQueryExecution(c, fs)
			if err != nil {
				return handleError(c.String("output"), err)
			}

			dialect, err := sqlparser.AssetTypeToDialect(assetType)
			if err != nil {
				dialect = ""
			}
			if c.IsSet("limit") {
				parser, err := sqlparser.NewSQLParser(false)
				if err != nil {
					return handleError(c.String("output"), errors.Wrap(err, "failed to initialize SQL parser"))
				}
				defer parser.Close()

				err = parser.Start()
				if err != nil {
					return handleError(c.String("output"), errors.Wrap(err, "failed to start SQL parser"))
				}

				queryStr = addLimitToQuery(queryStr, c.Int64("limit"), conn, parser, dialect)
			}
			if querier, ok := conn.(interface {
				SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
			}); ok {
				ctx := context.Background()
				ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
				defer cancel()

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(c.Int("timeout"))*time.Second)
				defer timeoutCancel()

				q := query.Query{Query: queryStr}
				result, err := querier.SelectWithSchema(timeoutCtx, &q)
				if err != nil {
					return handleError(c.String("output"), errors.Wrap(err, "query execution failed"))
				}
				// Output result based on format specified
				inputPath := c.String("asset")
				var resultsPath string
				if c.Bool("export") {
					resultsPath, err = exportResultsToCSV(result, inputPath)
					if err != nil {
						return handleError(c.String("output"), errors.Wrap(err, "failed to export results to CSV"))
					}
					successMessage := "Results Successfully exported to " + resultsPath
					return handleSuccess(c.String("output"), successMessage)
				}
				output := c.String("output")
				switch output {
				case outputFormatPlain:
					printTable(result.Columns, result.Rows)
				case "json":
					type jsonResponse struct {
						Columns  []map[string]string `json:"columns"`
						Rows     [][]interface{}     `json:"rows"`
						ConnName string              `json:"connectionName"`
						Query    string              `json:"query"`
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
						Rows:     result.Rows,
						ConnName: connName,
						Query:    queryStr,
					}

					jsonData, err := json.Marshal(finalOutput)
					if err != nil {
						return handleError(output, errors.Wrap(err, "failed to marshal result to JSON"))
					}
					fmt.Println(string(jsonData))
				case "csv":
					writer := csv.NewWriter(os.Stdout)
					defer writer.Flush()
					if err = writer.Write(result.Columns); err != nil {
						return handleError(output, errors.Wrap(err, "failed to write CSV header"))
					}
					for _, row := range result.Rows {
						rowStrings := make([]string, len(row))
						for i, val := range row {
							rowStrings[i] = fmt.Sprintf("%v", val)
						}
						if err = writer.Write(rowStrings); err != nil {
							return handleError(output, errors.Wrap(err, "failed to write CSV row"))
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

func validateFlags(connection, query, asset string) error {
	hasConnection := connection != ""
	hasQuery := query != ""
	hasAsset := asset != ""

	switch {
	case hasConnection:
		if !(hasConnection && hasQuery) {
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

func prepareQueryExecution(c *cli.Context, fs afero.Fs) (string, interface{}, string, pipeline.AssetType, error) {
	assetPath := c.String("asset")
	queryStr := c.String("query")
	env := c.String("environment")
	connectionName := c.String("connection")
	s := c.String("start-date")
	e := c.String("end-date")
	logger := makeLogger(false)
	startDate, endDate, err := ParseDate(s, e, logger)
	if err != nil {
		return "", nil, "", "", err
	}

	extractor := &query.WholeFileExtractor{
		Fs: fs,
		// note: we don't support variables for now
		Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, "your-pipeline-name", "your-run-id", nil),
	}

	// Direct query mode (no asset path)
	if assetPath == "" {
		conn, err := getConnectionFromConfig(env, connectionName, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", err
		}
		queryStr, err = extractQuery(queryStr, extractor)
		if err != nil {
			return "", nil, "", "", err
		}
		return connectionName, conn, queryStr, "", nil
	}

	if queryStr != "" {
		pipelineInfo, err := GetPipelineAndAsset(c.Context, assetPath, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", errors.Wrap(err, "failed to get pipeline info")
		}
		fetchCtx := context.Background()
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigStartDate, startDate)
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigEndDate, endDate)
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigRunID, "your-run-id")
		// Auto-detect mode (both asset path and query)
		extractor = &query.WholeFileExtractor{
			Fs: fs,
			// note: we don't support variables for now
			Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, pipelineInfo.Pipeline.Name, "your-run-id", nil),
		}

		newExtractor, err := extractor.CloneForAsset(fetchCtx, pipelineInfo.Pipeline, pipelineInfo.Asset)
		if err != nil {
			return "", nil, "", "", errors.Wrapf(err, "failed to clone extractor for asset %s", pipelineInfo.Asset.Name)
		}

		connName, conn, err := getConnectionFromPipelineInfo(pipelineInfo, env)
		if err != nil {
			return "", nil, "", "", err
		}

		queryStr, err = extractQuery(queryStr, newExtractor)
		if err != nil {
			return "", nil, "", "", err
		}

		return connName, conn, queryStr, pipelineInfo.Asset.Type, nil
	}
	// Asset query mode (only asset path)
	pipelineInfo, err := GetPipelineAndAsset(c.Context, assetPath, fs, c.String("config-file"))
	if err != nil {
		return "", nil, "", "", errors.Wrap(err, "failed to get pipeline info")
	}

	fetchCtx := context.Background()
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigStartDate, startDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigEndDate, endDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigRunID, "your-run-id")
	extractor = &query.WholeFileExtractor{
		Fs: fs,
		// note: we don't support variables for now
		Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, pipelineInfo.Pipeline.Name, "your-run-id", nil),
	}
	newExtractor, err := extractor.CloneForAsset(fetchCtx, pipelineInfo.Pipeline, pipelineInfo.Asset)
	if err != nil {
		return "", nil, "", "", errors.Wrapf(err, "failed to clone extractor for asset %s", pipelineInfo.Asset.Name)
	}
	// Verify that the asset is a SQL asset
	if !pipelineInfo.Asset.IsSQLAsset() {
		return "", nil, "", "", errors.Errorf("asset '%s' is not a SQL asset (type: %s). Only SQL assets can be queried",
			assetPath,
			pipelineInfo.Asset.Type)
	}
	queryStr, err = extractQuery(pipelineInfo.Asset.ExecutableFile.Content, newExtractor)
	if err != nil {
		return "", nil, "", "", err
	}
	connName, conn, err := getConnectionFromPipelineInfo(pipelineInfo, env)
	if err != nil {
		return "", nil, "", "", err
	}

	return connName, conn, queryStr, pipelineInfo.Asset.Type, nil
}

func getConnectionFromConfig(env string, connectionName string, fs afero.Fs, configFilePath string) (interface{}, error) {
	repoRoot, err := git.FindRepoFromPath(".")
	if err != nil {
		return nil, errors.Wrap(err, "failed to find the git repository root")
	}

	if configFilePath == "" {
		configFilePath = filepath.Join(repoRoot.Path, ".bruin.yml")
	}
	cm, err := config.LoadOrCreate(fs, configFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load or create config")
	}

	if env != "" {
		err := cm.SelectEnvironment(env)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to use the environment '%s'", env)
		}
	}

	manager, errs := connection.NewManagerFromConfig(cm)
	if len(errs) > 0 {
		return nil, errors.Wrap(errs[0], "failed to create connection manager")
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return nil, errors.Errorf("failed to get connection '%s'", connectionName)
	}

	return conn, nil
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

func getConnectionFromPipelineInfo(pipelineInfo *ppInfo, env string) (string, interface{}, error) {
	if env != "" {
		err := pipelineInfo.Config.SelectEnvironment(env)
		if err != nil {
			return "", nil, errors.Wrapf(err, "failed to use the environment '%s'", env)
		}
	}

	// Get connection info
	manager, errs := connection.NewManagerFromConfig(pipelineInfo.Config)
	if len(errs) > 0 {
		return "", nil, errors.Wrap(errs[0], "failed to create connection manager")
	}

	connName, err := pipelineInfo.Pipeline.GetConnectionNameForAsset(pipelineInfo.Asset)
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to get connection")
	}

	conn := manager.GetConnection(connName)
	if conn == nil {
		return "", nil, errors.Errorf("failed to get connection '%s'", connName)
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

func exportResultsToCSV(results *query.QueryResult, inputPath string) (string, error) {
	if inputPath == "" {
		inputPath = "."
	}
	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		return "", err
	}
	resultName := fmt.Sprintf("query_result_%s.csv", time.Now().Format("2006-01-02_15-04-05"))
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
			rowStrings[i] = fmt.Sprintf("%v", val)
		}
		if err = writer.Write(rowStrings); err != nil {
			return "", err
		}
	}

	return resultsPath, nil
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
