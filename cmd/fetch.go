package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	path2 "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
				Name:    "limit",
				Aliases: []string{"l"},
				Usage:   "limit the number of rows returned",
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
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for executing the query.",
			},
			&cli.BoolFlag{
				Name:  "export",
				Usage: "export results to a CSV file ",
			},
		},
		Action: func(c *cli.Context) error {
			fs := afero.NewOsFs()
			if err := validateCliFlags(c); err != nil {
				return handleError(c.String("output"), err)
			}

			connName, conn, queryStr, err := prepareQueryExecution(c, fs)
			if err != nil {
				return handleError(c.String("output"), err)
			}
			if c.IsSet("limit") {
				queryStr = addLimitToQuery(queryStr, c.Int64("limit"), conn)
			}
			if querier, ok := conn.(interface {
				SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
			}); ok {
				ctx := context.Background()
				q := query.Query{Query: queryStr}
				result, err := querier.SelectWithSchema(ctx, &q)
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
					successMessage := fmt.Sprintf("Results Successfully exported to %s", resultsPath)
					return handleSuccess(c.String("output"), successMessage)
				}
				output := c.String("output")
				if output == "json" {
					type jsonResponse struct {
						Columns  []map[string]string `json:"columns"`
						Rows     [][]interface{}     `json:"rows"`
						ConnName string              `json:"connectionName"`
					}

					// Construct JSON response with structured columns
					jsonCols := make([]map[string]string, len(result.Columns))
					for i, colName := range result.Columns {
						jsonCols[i] = map[string]string{"name": colName}
					}

					// Prepare the final output struct
					finalOutput := jsonResponse{
						Columns:  jsonCols,
						Rows:     result.Rows,
						ConnName: connName,
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
				fmt.Printf("Connection type %s does not support querying.\n", c.String("connection"))
			}
			return nil
		},
	}
}

func validateCliFlags(c *cli.Context) error {
	return validateFlags(
		c.String("connection"),
		c.String("query"),
		c.String("asset"),
		c.String("environment"),
	)
}

func validateFlags(connection, query, asset, environment string) error {
	hasConnection := connection != ""
	hasQuery := query != ""
	hasAsset := asset != ""
	hasEnvironment := environment != ""

	switch {
	case hasConnection:
		if !(hasConnection && hasQuery) {
			return errors.New("direct query mode requires both --connection and --query flags")
		}
		if hasAsset || hasEnvironment {
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

func prepareQueryExecution(c *cli.Context, fs afero.Fs) (string, interface{}, string, error) {
	assetPath := c.String("asset")
	queryStr := c.String("query")

	// Direct query mode (no asset path)
	if assetPath == "" {
		return prepareDirectQuery(c, fs)
	}

	// Auto-detect mode (both asset path and query)
	if queryStr != "" {
		return prepareAutoDetectQuery(c, fs)
	}

	// Asset query mode (only asset path)
	return prepareAssetQuery(c, fs)
}

func prepareDirectQuery(c *cli.Context, fs afero.Fs) (string, interface{}, string, error) {
	connectionName := c.String("connection")
	queryStr := c.String("query")

	conn, err := getConnectionFromConfig(connectionName, fs)
	if err != nil {
		return "", nil, "", err
	}

	return connectionName, conn, queryStr, nil
}

func getConnectionFromConfig(connectionName string, fs afero.Fs) (interface{}, error) {
	repoRoot, err := git.FindRepoFromPath(".")
	if err != nil {
		return nil, errors.Wrap(err, "failed to find the git repository root")
	}

	configFilePath := filepath.Join(repoRoot.Path, ".bruin.yml")
	cm, err := config.LoadOrCreate(fs, configFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load or create config")
	}

	manager, errs := connection.NewManagerFromConfig(cm)
	if len(errs) > 0 {
		return nil, errors.Wrap(errs[0], "failed to create connection manager")
	}

	conn, err := manager.GetConnection(connectionName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get connection")
	}

	return conn, nil
}

func prepareAssetQuery(c *cli.Context, fs afero.Fs) (string, interface{}, string, error) {
	assetPath := c.String("asset")
	env := c.String("env")

	pipelineInfo, err := GetPipelineAndAsset(c.Context, assetPath, fs)
	if err != nil {
		return "", nil, "", errors.Wrap(err, "failed to get pipeline info")
	}

	// Verify that the asset is a SQL asset
	if !pipelineInfo.Asset.IsSQLAsset() {
		return "", nil, "", errors.Errorf("asset '%s' is not a SQL asset (type: %s). Only SQL assets can be queried",
			assetPath,
			pipelineInfo.Asset.Type)
	}

	queryStr, err := extractQueryFromAsset(pipelineInfo.Asset, fs)
	if err != nil {
		return "", nil, "", err
	}

	connName, conn, err := getConnectionFromPipelineInfo(pipelineInfo, env)
	if err != nil {
		return "", nil, "", err
	}

	return connName, conn, queryStr, nil
}

func extractQueryFromAsset(asset *pipeline.Asset, fs afero.Fs) (string, error) {
	startDate := time.Now()
	endDate := time.Now()
	extractor := &query.WholeFileExtractor{
		Fs:       fs,
		Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, "your-pipeline-name", "your-run-id"),
	}

	// Extract the query from the asset
	queries, err := extractor.ExtractQueriesFromString(asset.ExecutableFile.Content)
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

	conn, err := manager.GetConnection(connName)
	if err != nil {
		return "", nil, errors.Wrap(err, fmt.Sprintf("failed to get connection '%s'", connName))
	}

	return connName, conn, nil
}

type Limiter interface {
	Limit(query string, limit int64) string
}

func addLimitToQuery(query string, limit int64, conn interface{}) string {
	l, ok := conn.(Limiter)
	if ok {
		return l.Limit(query, limit)
	} else {
		query = strings.TrimRight(query, "; \n\t")

		return fmt.Sprintf("SELECT * FROM (\n%s\n) as t LIMIT %d", query, limit)
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

func GetPipelineAndAsset(ctx context.Context, inputPath string, fs afero.Fs) (*ppInfo, error) {
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
	return &ppInfo{
		Pipeline: foundPipeline,
		Asset:    task,
		Config:   cm,
	}, nil
}

func prepareAutoDetectQuery(c *cli.Context, fs afero.Fs) (string, interface{}, string, error) {
	assetPath := c.String("asset")
	queryStr := c.String("query")
	env := c.String("env")

	pipelineInfo, err := GetPipelineAndAsset(c.Context, assetPath, fs)
	if err != nil {
		return "", nil, "", errors.Wrap(err, "failed to get pipeline info")
	}

	connName, conn, err := getConnectionFromPipelineInfo(pipelineInfo, env)
	if err != nil {
		return "", nil, "", err
	}

	return connName, conn, queryStr, nil
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

	err = os.MkdirAll(filepath.Dir(resultsPath), 0755)
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
		jsonSuccessMessage, err := json.Marshal(map[string]string{"Success": message})
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
