package cmd

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	path2 "path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
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
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
			&cli.StringFlag{
				Name:    "agent-id",
				Sources: cli.EnvVars("BRUIN_AGENT_ID"),
				Usage:   "agent ID to include in query annotations for tracking purposes",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			fs := afero.NewOsFs()
			if err := validateFlags(c.String("connection"), c.String("query"), c.String("asset")); err != nil {
				return handleError(c.String("output"), err)
			}

			connName, conn, queryStr, assetType, pipelineInfo, err := prepareQueryExecution(ctx, c, fs)
			if err != nil {
				return handleError(c.String("output"), err)
			}

			dialect, err := sqlparser.AssetTypeToDialect(assetType)
			if err != nil {
				dialect = ""
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
			//nolint:nestif
			if querier, ok := conn.(interface {
				SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
			}); ok {
				ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
				defer cancel()

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(c.Int("timeout"))*time.Second)
				defer timeoutCancel()

				q := query.Query{Query: queryStr}
				agentID := c.String("agent-id")

				// Apply agent-id annotation based on connection type
				_, isSnowflake := conn.(snowflake.SfClient)
				if isSnowflake && agentID != "" {
					// Snowflake: use query tag via context (Snowflake strips leading SQL comments)
					timeoutCtx = gosnowflake.WithQueryTag(timeoutCtx, ansisql.BuildAgentIDQueryTag(agentID))
				} else if agentID != "" {
					// BigQuery and others: prepend comment to query
					q = *ansisql.AddAgentIDAnnotationComment(&q, agentID)
				}

				result, queryErr := querier.SelectWithSchema(timeoutCtx, &q)

				// Save query log (for both success and error cases)
				inputPath := c.String("asset")
				logOpts := QueryLogOptions{
					Asset:       inputPath,
					Environment: c.String("environment"),
					Limit:       c.Int64("limit"),
					Timeout:     c.Int("timeout"),
				}
				if err := saveQueryLog(queryStr, connName, result, queryErr, logOpts); err != nil {
					// Log the error but don't fail the command
					fmt.Fprintf(os.Stderr, "Warning: failed to save query log: %v\n", err)
				}

				if queryErr != nil {
					return handleError(c.String("output"), errors.Wrap(queryErr, "query execution failed"))
				}

				// Output result based on format specified
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
							if val == nil {
								rowStrings[i] = ""
							} else {
								rowStrings[i] = fmt.Sprintf("%v", val)
							}
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

func prepareQueryExecution(ctx context.Context, c *cli.Command, fs afero.Fs) (string, interface{}, string, pipeline.AssetType, *ppInfo, error) {
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

	extractor := &query.WholeFileExtractor{
		Fs: fs,
		// note: we don't support variables for now
		Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, "your-pipeline-name", "your-run-id", nil),
	}

	// Direct query mode (no asset path)
	if assetPath == "" {
		conn, err := getConnectionFromConfigWithContext(ctx, env, connectionName, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", nil, err
		}
		queryStr, err = extractQuery(queryStr, extractor)
		if err != nil {
			return "", nil, "", "", nil, err
		}
		return connectionName, conn, queryStr, "", nil, nil
	}

	if queryStr != "" {
		pipelineInfo, err := GetPipelineAndAsset(ctx, assetPath, fs, c.String("config-file"))
		if err != nil {
			return "", nil, "", "", nil, errors.Wrap(err, "failed to get pipeline info")
		}
		fetchCtx := context.WithValue(ctx, pipeline.RunConfigStartDate, startDate)
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigEndDate, endDate)
		fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigRunID, "your-run-id")
		fetchCtx = context.WithValue(fetchCtx, config.EnvironmentContextKey, pipelineInfo.Config.SelectedEnvironment)
		// Auto-detect mode (both asset path and query)
		extractor = &query.WholeFileExtractor{
			Fs: fs,
			// note: we don't support variables for now
			Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, pipelineInfo.Pipeline.Name, "your-run-id", nil),
		}

		newExtractor, err := extractor.CloneForAsset(fetchCtx, pipelineInfo.Pipeline, pipelineInfo.Asset)
		if err != nil {
			return "", nil, "", "", nil, errors.Wrapf(err, "failed to clone extractor for asset %s", pipelineInfo.Asset.Name)
		}

		connName, conn, err := getConnectionFromPipelineInfoWithContext(ctx, pipelineInfo, env)
		if err != nil {
			return "", nil, "", "", nil, err
		}

		queryStr, err = extractQuery(queryStr, newExtractor)
		if err != nil {
			return "", nil, "", "", nil, err
		}

		return connName, conn, queryStr, pipelineInfo.Asset.Type, pipelineInfo, nil
	}
	// Asset query mode (only asset path)
	pipelineInfo, err := GetPipelineAndAsset(ctx, assetPath, fs, c.String("config-file"))
	if err != nil {
		return "", nil, "", "", nil, errors.Wrap(err, "failed to get pipeline info")
	}

	fetchCtx := context.WithValue(ctx, pipeline.RunConfigStartDate, startDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigEndDate, endDate)
	fetchCtx = context.WithValue(fetchCtx, pipeline.RunConfigRunID, "your-run-id")
	fetchCtx = context.WithValue(fetchCtx, config.EnvironmentContextKey, pipelineInfo.Config.SelectedEnvironment)
	extractor = &query.WholeFileExtractor{
		Fs: fs,
		// note: we don't support variables for now
		Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, pipelineInfo.Pipeline.Name, "your-run-id", nil),
	}
	newExtractor, err := extractor.CloneForAsset(fetchCtx, pipelineInfo.Pipeline, pipelineInfo.Asset)
	if err != nil {
		return "", nil, "", "", nil, errors.Wrapf(err, "failed to clone extractor for asset %s", pipelineInfo.Asset.Name)
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

	return connName, conn, queryStr, pipelineInfo.Asset.Type, pipelineInfo, nil
}

func getConnectionFromConfigWithContext(ctx context.Context, env string, connectionName string, fs afero.Fs, configFilePath string) (interface{}, error) {
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

	manager, errs := connection.NewManagerFromConfigWithContext(ctx, cm)
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
				rowStrings[i] = fmt.Sprintf("%v", val)
			}
		}
		if err = writer.Write(rowStrings); err != nil {
			return "", err
		}
	}

	return resultsPath, nil
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

// QueryLog represents the structure of a query log entry.
type QueryLog struct {
	Query          string          `json:"query"`
	FormattedQuery string          `json:"formatted_query,omitempty"`
	Timestamp      time.Time       `json:"timestamp"`
	Connection     string          `json:"connection"`
	Success        bool            `json:"success"`
	Columns        []string        `json:"columns,omitempty"`
	Rows           [][]interface{} `json:"rows,omitempty"`
	Error          string          `json:"error,omitempty"`
	Asset          string          `json:"asset,omitempty"`
	Environment    string          `json:"environment,omitempty"`
	Limit          int64           `json:"limit,omitempty"`
	Timeout        int             `json:"timeout,omitempty"`
}

// QueryLogOptions contains optional parameters for query logging.
type QueryLogOptions struct {
	Asset       string
	Environment string
	Limit       int64
	Timeout     int
}

// formatSQL formats a SQL query using the sql-formatter npm package.
// It tries to run Node.js with the sql-formatter library inline.
// If formatting fails, it returns the original query.
func formatSQL(sqlQuery string) string {
	// Try to format using Node.js with sql-formatter inline
	// This approach doesn't require a separate script file
	nodeScript := `
const { format } = require('sql-formatter');
let sql = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => { sql += chunk; });
process.stdin.on('end', () => {
    try {
        const formatted = format(sql, {
            language: 'sql',
            tabWidth: 2,
            useTabs: false,
            keywordCase: 'upper',
            linesBetweenQueries: 2,
        });
        process.stdout.write(formatted);
    } catch (e) {
        process.stdout.write(sql);
    }
});
`
	cmd := exec.Command("node", "-e", nodeScript)
	cmd.Stdin = strings.NewReader(sqlQuery)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		// If Node.js is not available or formatting fails, return original query
		return sqlQuery
	}

	formatted := strings.TrimSpace(stdout.String())
	if formatted == "" {
		return sqlQuery
	}

	return formatted
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

	// Format the SQL query for better display
	formattedQuery := formatSQL(queryStr)

	logEntry := QueryLog{
		Query:          queryStr,
		FormattedQuery: formattedQuery,
		Timestamp:      timestamp,
		Connection:     connName,
		Success:        queryErr == nil,
		Asset:          opts.Asset,
		Environment:    opts.Environment,
		Limit:          opts.Limit,
		Timeout:        opts.Timeout,
	}

	if queryErr != nil {
		logEntry.Error = queryErr.Error()
	} else if result != nil {
		logEntry.Columns = result.Columns
		logEntry.Rows = result.Rows
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
