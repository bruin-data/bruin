package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	lineagepackage "github.com/bruin-data/bruin/pkg/lineage"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/telemetry"
	color2 "github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	errors2 "github.com/pkg/errors"
	"github.com/sourcegraph/conc"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Internal() *cli.Command {
	return &cli.Command{
		Name:   "internal",
		Hidden: true,
		Subcommands: []*cli.Command{
			ParseAsset(),
			ParsePipeline(),
			PatchAsset(),
			ConnectionSchemas(),
			DBSummary(),
			FetchDatabases(),
			FetchTables(),
			FetchColumns(),
		},
	}
}

func ParseAsset() *cli.Command {
	return &cli.Command{
		Name:      "parse-asset",
		Usage:     "parse a single Bruin asset",
		ArgsUsage: "[path to the asset definition]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "column-lineage",
				Aliases:     []string{"c"},
				Usage:       "return the column lineage for the given asset",
				Required:    false,
				DefaultText: "false",
			},
		},
		Action: func(c *cli.Context) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.Run(c.Context, c.Args().Get(0), c.Bool("column-lineage"))
		},
	}
}

func ParsePipeline() *cli.Command {
	return &cli.Command{
		Name:      "parse-pipeline",
		Usage:     "parse a full Bruin pipeline",
		ArgsUsage: "[path to the any asset or anywhere in the pipeline]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "column-lineage",
				Aliases:     []string{"c"},
				Usage:       "return the column lineage for the given asset",
				Required:    false,
				DefaultText: "false",
			},
			&cli.BoolFlag{
				Name:        "exp-slim-response",
				Usage:       "experimental flag to return a slim response",
				Required:    false,
				DefaultText: "false",
			},
		},
		Action: func(c *cli.Context) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.ParsePipeline(c.Context, c.Args().Get(0), c.Bool("column-lineage"), c.Bool("exp-slim-response"))
		},
	}
}

func ConnectionSchemas() *cli.Command {
	return &cli.Command{
		Name:  "connections",
		Usage: "return all the possible connection types and their schemas",
		Action: func(c *cli.Context) error {
			jsonStringSchema, err := config.GetConnectionsSchema()
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			fmt.Println(jsonStringSchema)
			return nil
		},
	}
}

func DBSummary() *cli.Command {
	return &cli.Command{
		Name:   "db-summary",
		Usage:  "Get a summary of database schemas and tables for a specified connection",
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Value:       "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for the database summary.",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			fs := afero.NewOsFs()
			connectionName := c.String("connection")
			environment := c.String("environment")
			output := c.String("output")

			// Get connection from config
			conn, err := getConnectionFromConfig(environment, connectionName, fs, c.String("config-file"))
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to get database connection"))
			}

			// Check if connection supports GetDatabaseSummary
			summarizer, ok := conn.(interface {
				GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
			})
			if !ok {
				return handleError(output, fmt.Errorf("connection type '%s' does not support database summary", connectionName))
			}

			// Get database summary
			ctx := context.Background()
			summary, err := summarizer.GetDatabaseSummary(ctx)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve database summary"))
			}

			// Output result based on format specified
			switch output {
			case "plain":
				printDatabaseSummary(summary)
			case "json":
				type jsonResponse struct {
					DatabaseName string              `json:"database_name"`
					Schemas      []*ansisql.DBSchema `json:"schemas"`
					ConnName     string              `json:"connection_name"`
					Summary      *SummaryStats       `json:"summary"`
				}

				stats := calculateSummaryStats(summary)
				finalOutput := jsonResponse{
					DatabaseName: summary.Name,
					Schemas:      summary.Schemas,
					ConnName:     connectionName,
					Summary:      stats,
				}

				jsonData, err := json.Marshal(finalOutput)
				if err != nil {
					return handleError(output, errors2.Wrap(err, "failed to marshal result to JSON"))
				}
				fmt.Println(string(jsonData))
			default:
				return handleError(output, fmt.Errorf("invalid output type: %s", output))
			}

			return nil
		},
	}
}

type SummaryStats struct {
	TotalSchemas int `json:"total_schemas"`
	TotalTables  int `json:"total_tables"`
}

func calculateSummaryStats(summary *ansisql.DBDatabase) *SummaryStats {
	totalTables := 0
	for _, schema := range summary.Schemas {
		totalTables += len(schema.Tables)
	}

	return &SummaryStats{
		TotalSchemas: len(summary.Schemas),
		TotalTables:  totalTables,
	}
}

func printDatabaseSummary(summary *ansisql.DBDatabase) {
	if len(summary.Schemas) == 0 {
		fmt.Printf("Database '%s' contains no schemas or tables\n", summary.Name)
		return
	}

	fmt.Printf("Database Summary for: %s\n", summary.Name)
	fmt.Println(strings.Repeat("=", 50))

	stats := calculateSummaryStats(summary)
	fmt.Printf("Total Schemas: %d\n", stats.TotalSchemas)
	fmt.Printf("Total Tables: %d\n\n", stats.TotalTables)

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Schema", "Table", "Type"})

	for _, schema := range summary.Schemas {
		if len(schema.Tables) == 0 {
			t.AppendRow(table.Row{schema.Name, "(no tables)", ""})
			continue
		}

		for i, tbl := range schema.Tables {
			if i == 0 {
				t.AppendRow(table.Row{schema.Name, tbl.Name, "table"})
			} else {
				t.AppendRow(table.Row{"", tbl.Name, "table"})
			}
		}
		t.AppendSeparator()
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}

type ParseCommand struct {
	builder      taskCreator
	errorPrinter *color2.Color
}

func (r *ParseCommand) ParsePipeline(ctx context.Context, assetPath string, lineage bool, slimResponse bool) error {
	// defer RecoverFromPanic()
	var lineageWg conc.WaitGroup
	var sqlParser *sqlparser.SQLParser

	if lineage {
		lineageWg.Go(func() {
			var err error
			sqlParser, err = sqlparser.NewSQLParser(false)
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}

			err = sqlParser.Start()
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}
		})
	}

	if assetPath == "" {
		errorPrinter.Printf("Please give an asset path to parse: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, PipelineDefinitionFiles)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		printErrorJSON(err)

		return cli.Exit("", 1)
	}

	if lineage {
		panics := lineageWg.WaitAndRecover()
		if panics != nil {
			return cli.Exit("", 1)
		}

		issues := &lineagepackage.LineageError{
			Issues:   make([]*lineagepackage.LineageIssue, 0),
			Pipeline: foundPipeline,
		}

		defer sqlParser.Close()
		processedAssets := make(map[string]bool)
		lineage := lineagepackage.NewLineageExtractor(sqlParser)
		for _, asset := range foundPipeline.Assets {
			errIssues := lineage.ColumnLineage(foundPipeline, asset, processedAssets)
			if errIssues != nil {
				issues.Issues = append(issues.Issues, errIssues.Issues...)
			}
		}
	}

	foundPipeline.WipeContentOfAssets()

	if !slimResponse {
		js, err := json.Marshal(foundPipeline)
		if err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}

		fmt.Println(string(js))
		return nil
	}

	type assetSummary struct {
		ID             string                       `json:"id"`
		Name           string                       `json:"name"`
		Type           pipeline.AssetType           `json:"type"`
		ExecutableFile *pipeline.ExecutableFile     `json:"executable_file"`
		DefinitionFile *pipeline.TaskDefinitionFile `json:"definition_file"`
		Upstreams      []pipeline.Upstream          `json:"upstreams"`
	}

	type pipelineSummary struct {
		*pipeline.Pipeline
		Assets []*assetSummary `json:"assets"`
	}

	ps := pipelineSummary{
		Pipeline: foundPipeline,
		Assets:   make([]*assetSummary, len(foundPipeline.Assets)),
	}

	for i, asset := range foundPipeline.Assets {
		ps.Assets[i] = &assetSummary{
			ID:             asset.ID,
			Name:           asset.Name,
			Type:           asset.Type,
			ExecutableFile: &asset.ExecutableFile,
			DefinitionFile: &asset.DefinitionFile,
			Upstreams:      asset.Upstreams,
		}
	}

	js, err := json.Marshal(ps)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	fmt.Println(string(js))

	return err
}

func (r *ParseCommand) Run(ctx context.Context, assetPath string, lineage bool) error {
	defer RecoverFromPanic()

	var lineageWg conc.WaitGroup
	var sqlParser *sqlparser.SQLParser

	if lineage {
		lineageWg.Go(func() {
			var err error
			sqlParser, err = sqlparser.NewSQLParser(false)
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}

			err = sqlParser.Start()
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}
		})
	}

	if assetPath == "" {
		errorPrinter.Printf("Please give an asset path to parse: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	absoluteAssetPath, err := filepath.Abs(assetPath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(absoluteAssetPath, PipelineDefinitionFiles)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	repoRoot, err := git.FindRepoFromPath(absoluteAssetPath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	pipelineDefinitionPath, err := getPipelineDefinitionFullPath(pipelinePath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	var foundPipeline *pipeline.Pipeline
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(absoluteAssetPath, nil)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}
	// column-level lineage requires the whole pipeline to be parsed by nature, therefore we need to use the default pipeline builder.
	// however, since the primary usecase of this command requires speed, we'll use a faster alternative if there's no lineage requested.
	if lineage {
		foundPipeline, err = DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelineDefinitionPath, pipeline.WithMutate())
	} else {
		foundPipeline, err = pipeline.PipelineFromPath(pipelineDefinitionPath, afero.NewOsFs())
		if err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}
		err = DefaultPipelineBuilder.SetAssetColumnFromGlossary(asset, pipelineDefinitionPath)
	}

	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	asset, err = DefaultPipelineBuilder.MutateAsset(ctx, asset, foundPipeline)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	type lineageIssueSummary struct {
		Asset string `json:"name"`
		Error string `json:"error"`
	}

	lineageIssues := make([]*lineageIssueSummary, 0)

	if lineage {
		panics := lineageWg.WaitAndRecover()
		if panics != nil {
			return cli.Exit("", 1)
		}
		defer sqlParser.Close()

		processedAssets := make(map[string]bool)
		lineageExtractor := lineagepackage.NewLineageExtractor(sqlParser)
		lineageErrors := lineageExtractor.ColumnLineage(foundPipeline, asset, processedAssets)
		if lineageErrors != nil {
			for _, issue := range lineageErrors.Issues {
				lineageIssues = append(lineageIssues, &lineageIssueSummary{
					Asset: issue.Task.Name,
					Error: issue.Description,
				})
			}
		}
	}

	type pipelineSummary struct {
		Name     string            `json:"name"`
		Schedule pipeline.Schedule `json:"schedule"`
	}

	js, err := json.Marshal(struct {
		Asset         *pipeline.Asset        `json:"asset"`
		Pipeline      pipelineSummary        `json:"pipeline"`
		Repo          *git.Repo              `json:"repo"`
		LineageIssues []*lineageIssueSummary `json:"lineage_issues,omitempty"`
	}{
		Asset: asset,
		Pipeline: pipelineSummary{
			Name:     foundPipeline.Name,
			Schedule: foundPipeline.Schedule,
		},
		LineageIssues: lineageIssues,
		Repo:          repoRoot,
	})
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	fmt.Println(string(js))

	return err
}

func PatchAsset() *cli.Command {
	return &cli.Command{
		Name:      "patch-asset",
		Usage:     "patch a single Bruin asset with the given fields",
		ArgsUsage: "[path to the asset definition]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "body",
				Usage:    "the JSON object containing the patch body",
				Required: false,
			},
			&cli.BoolFlag{
				Name:        "convert",
				Usage:       "convert a SQL or Python file into a Bruin asset",
				Required:    false,
				DefaultText: "false",
			},
		},
		Action: func(c *cli.Context) error {
			assetPath := c.Args().Get(0)
			if assetPath == "" {
				printErrorJSON(errors2.New("empty asset path given, you must provide an existing asset path"))
				return cli.Exit("", 1)
			}

			if c.Bool("convert") {
				return convertToBruinAsset(afero.NewOsFs(), assetPath)
			}

			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath, nil)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to create asset from the given path"))
				return cli.Exit("", 1)
			}

			asset, err = DefaultPipelineBuilder.MutateAsset(c.Context, asset, nil)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to patch the asset with the given json body"))
				return cli.Exit("", 1)
			}

			if asset == nil {
				printErrorJSON(errors2.New("the file in the given path does not seem to be an asset"))
				return cli.Exit("", 1)
			}

			err = json.Unmarshal([]byte(c.String("body")), &asset)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to patch the asset with the given json body"))
				return cli.Exit("", 1)
			}

			err = asset.Persist(afero.NewOsFs())
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to save the asset to the file"))
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}

func convertToBruinAsset(fs afero.Fs, filePath string) error {
	// Check if file exists
	exists, err := afero.Exists(fs, filePath)
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to check if file exists"))
		return cli.Exit("", 1)
	}
	if !exists {
		printErrorJSON(errors2.New("file does not exist"))
		return cli.Exit("", 1)
	}

	content, err := afero.ReadFile(fs, filePath)
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to read file"))
		return cli.Exit("", 1)
	}

	fileName := filepath.Base(filePath)
	assetName := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	ext := strings.ToLower(filepath.Ext(filePath))

	// Try to determine the majority asset type from the pipeline
	var assetType = pipeline.AssetTypeBigqueryQuery // default fallback
	pipelineRootPath, err := path.GetPipelineRootFromTask(filePath, PipelineDefinitionFiles)
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to get pipeline root path"))
	}
	if foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(context.Background(), pipelineRootPath); err == nil {
		assetType = foundPipeline.GetMajorityAssetTypesFromSQLAssets(pipeline.AssetTypeBigqueryQuery)
	}
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to create asset"))
	}

	if ext != ".sql" && ext != ".py" {
		return nil
	}
	asset := &pipeline.Asset{
		Name: assetName,
		Type: assetType,
		ExecutableFile: pipeline.ExecutableFile{
			Name:    fileName,
			Path:    filePath,
			Content: string(content),
		},
	}

	if ext == ".py" {
		asset.Type = pipeline.AssetTypePython
	}

	err = asset.Persist(fs)
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to persist asset"))
		return cli.Exit("", 1)
	}

	return nil
}

func FetchDatabases() *cli.Command {
	return &cli.Command{
		Name:   "fetch-databases",
		Usage:  "Fetch available databases/datasets for a specified connection",
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Value:       "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for the database fetch.",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			fs := afero.NewOsFs()
			connectionName := c.String("connection")
			environment := c.String("environment")
			output := c.String("output")

			// Get connection from config
			conn, err := getConnectionFromConfig(environment, connectionName, fs, c.String("config-file"))
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to get database connection"))
			}

			// Check if connection supports GetDatabases
			fetcher, ok := conn.(interface {
				GetDatabases(ctx context.Context) ([]string, error)
			})
			if !ok {
				return handleError(output, fmt.Errorf("connection type '%s' does not support database fetching", connectionName))
			}

			// Get databases
			ctx := context.Background()
			databases, err := fetcher.GetDatabases(ctx)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve databases"))
			}

			// Output result based on format specified
			switch output {
			case "plain":
				printDatabases(databases)
			case "json":
				type jsonResponse struct {
					Databases []string `json:"databases"`
					ConnName  string   `json:"connection_name"`
					Count     int      `json:"count"`
				}

				finalOutput := jsonResponse{
					Databases: databases,
					ConnName:  connectionName,
					Count:     len(databases),
				}

				jsonData, err := json.Marshal(finalOutput)
				if err != nil {
					return handleError(output, errors2.Wrap(err, "failed to marshal result to JSON"))
				}
				fmt.Println(string(jsonData))
			default:
				return handleError(output, fmt.Errorf("invalid output type: %s", output))
			}

			return nil
		},
	}
}

func printDatabases(databases []string) {
	if len(databases) == 0 {
		fmt.Println("No databases found")
		return
	}

	fmt.Printf("Found %d database(s):\n", len(databases))
	fmt.Println(strings.Repeat("=", 30))

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Database"})

	for _, db := range databases {
		t.AppendRow(table.Row{db})
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}

func FetchTables() *cli.Command {
	return &cli.Command{
		Name:   "fetch-tables",
		Usage:  "Fetch table names for a specified database/dataset",
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "database",
				Aliases:  []string{"d"},
				Usage:    "the name of the database/dataset to fetch columns from",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Value:       "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for the table fetch.",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			fs := afero.NewOsFs()
			connectionName := c.String("connection")
			databaseName := c.String("database")
			environment := c.String("environment")
			output := c.String("output")

			// Get connection from config
			conn, err := getConnectionFromConfig(environment, connectionName, fs, c.String("config-file"))
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to get database connection"))
			}

			// Check if connection supports GetTables
			fetcher, ok := conn.(interface {
				GetTables(ctx context.Context, databaseName string) ([]string, error)
			})
			if !ok {
				return handleError(output, fmt.Errorf("connection type '%s' does not support table fetching", connectionName))
			}

			// Get tables
			ctx := context.Background()
			tables, err := fetcher.GetTables(ctx, databaseName)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve tables"))
			}

			// Output result based on format specified
			switch output {
			case "plain":
				printTableNames(databaseName, tables)
			case "json":
				type jsonResponse struct {
					Database   string   `json:"database"`
					Tables     []string `json:"tables"`
					ConnName   string   `json:"connection_name"`
					TableCount int      `json:"table_count"`
				}

				finalOutput := jsonResponse{
					Database:   databaseName,
					Tables:     tables,
					ConnName:   connectionName,
					TableCount: len(tables),
				}

				jsonData, err := json.Marshal(finalOutput)
				if err != nil {
					return handleError(output, errors2.Wrap(err, "failed to marshal result to JSON"))
				}
				fmt.Println(string(jsonData))
			default:
				return handleError(output, fmt.Errorf("invalid output type: %s", output))
			}

			return nil
		},
	}
}

func printTableNames(databaseName string, tables []string) {
	if len(tables) == 0 {
		fmt.Printf("No tables found in database '%s'\n", databaseName)
		return
	}

	fmt.Printf("Database: %s\n", databaseName)
	fmt.Printf("Found %d table(s):\n", len(tables))
	fmt.Println(strings.Repeat("=", 30))

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Table"})

	for _, tableName := range tables {
		t.AppendRow(table.Row{tableName})
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}

func FetchColumns() *cli.Command {
	return &cli.Command{
		Name:   "fetch-columns",
		Usage:  "Fetch column information for a specified table in a database/dataset",
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "database",
				Aliases:  []string{"d"},
				Usage:    "the name of the database/dataset",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "table",
				Aliases:  []string{"t"},
				Usage:    "the name of the table to fetch columns from",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Value:       "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for the column fetch.",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			fs := afero.NewOsFs()
			connectionName := c.String("connection")
			databaseName := c.String("database")
			tableName := c.String("table")
			environment := c.String("environment")
			output := c.String("output")

			// Get connection from config
			conn, err := getConnectionFromConfig(environment, connectionName, fs, c.String("config-file"))
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to get database connection"))
			}

			// Check if connection supports GetColumns
			fetcher, ok := conn.(interface {
				GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error)
			})
			if !ok {
				return handleError(output, fmt.Errorf("connection type '%s' does not support column fetching", connectionName))
			}

			// Get columns
			ctx := context.Background()
			columns, err := fetcher.GetColumns(ctx, databaseName, tableName)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve columns"))
			}

			// Output result based on format specified
			switch output {
			case "plain":
				printColumns(databaseName, tableName, columns)
			case "json":
				type jsonResponse struct {
					Database    string              `json:"database"`
					Table       string              `json:"table"`
					Columns     []*ansisql.DBColumn `json:"columns"`
					ConnName    string              `json:"connection_name"`
					ColumnCount int                 `json:"column_count"`
				}

				finalOutput := jsonResponse{
					Database:    databaseName,
					Table:       tableName,
					Columns:     columns,
					ConnName:    connectionName,
					ColumnCount: len(columns),
				}

				jsonData, err := json.Marshal(finalOutput)
				if err != nil {
					return handleError(output, errors2.Wrap(err, "failed to marshal result to JSON"))
				}
				fmt.Println(string(jsonData))
			default:
				return handleError(output, fmt.Errorf("invalid output type: %s", output))
			}

			return nil
		},
	}
}

func printColumns(databaseName, tableName string, columns []*ansisql.DBColumn) {
	if len(columns) == 0 {
		fmt.Printf("No columns found in table '%s.%s'\n", databaseName, tableName)
		return
	}

	fmt.Printf("Database: %s\n", databaseName)
	fmt.Printf("Table: %s\n", tableName)
	fmt.Printf("Found %d column(s):\n", len(columns))
	fmt.Println(strings.Repeat("=", 60))

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Column", "Type", "Nullable", "Primary Key", "Unique"})

	for _, col := range columns {
		nullable := "NO"
		if col.Nullable {
			nullable = "YES"
		}
		primaryKey := "NO"
		if col.PrimaryKey {
			primaryKey = "YES"
		}
		unique := "NO"
		if col.Unique {
			unique = "YES"
		}

		t.AppendRow(table.Row{col.Name, col.Type, nullable, primaryKey, unique})
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}
