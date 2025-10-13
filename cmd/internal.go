package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/bigquery" //nolint:unused
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/jinja"
	lineagepackage "github.com/bruin-data/bruin/pkg/lineage"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/templates"
	color2 "github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	errors2 "github.com/pkg/errors"
	"github.com/sourcegraph/conc"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func Internal() *cli.Command {
	return &cli.Command{
		Name:   "internal",
		Hidden: true,
		Commands: []*cli.Command{
			ParseAsset(),
			ParsePipeline(),
			PatchAsset(),
			PatchPipeline(),
			ParseGlossary(),
			ConnectionSchemas(),
			DBSummary(),
			FetchDatabases(),
			FetchTables(),
			FetchColumns(),
			ListTemplates(),
			AssetMetadata(),
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
		Action: func(ctx context.Context, c *cli.Command) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.Run(ctx, c.Args().Get(0), c.Bool("column-lineage"))
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
		Action: func(ctx context.Context, c *cli.Command) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.ParsePipeline(ctx, c.Args().Get(0), c.Bool("column-lineage"), c.Bool("exp-slim-response"))
		},
	}
}

func ParseGlossary() *cli.Command {
	return &cli.Command{
		Name:      "parse-glossary",
		Usage:     "parse a glossary file",
		ArgsUsage: "[path to the glossary.yml file]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "pretty",
				Usage: "pretty print the JSON output",
			},
			&cli.BoolFlag{
				Name:  "entities-only",
				Usage: "show only entities information",
			},
			&cli.BoolFlag{
				Name:  "domains-only",
				Usage: "show only domains information",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			glossaryPath := c.Args().Get(0)
			if glossaryPath == "" {
				return errors.New("glossary file path is required")
			}

			// Load the glossary file
			loadedGlossary, err := glossary.LoadGlossaryFromFile(glossaryPath)
			if err != nil {
				return errors2.Wrap(err, "failed to load glossary file")
			}

			// Prepare output based on flags
			var output interface{}
			switch {
			case c.Bool("entities-only"):
				output = loadedGlossary.Entities
			case c.Bool("domains-only"):
				output = loadedGlossary.Domains
			default:
				output = loadedGlossary
			}

			// Convert to JSON
			var jsonBytes []byte
			if c.Bool("pretty") {
				jsonBytes, err = json.MarshalIndent(output, "", "  ")
			} else {
				jsonBytes, err = json.Marshal(output)
			}

			if err != nil {
				return errors2.Wrap(err, "failed to marshal glossary to JSON")
			}

			fmt.Println(string(jsonBytes))
			return nil
		},
	}
}

func ConnectionSchemas() *cli.Command {
	return &cli.Command{
		Name:  "connections",
		Usage: "return all the possible connection types and their schemas",
		Action: func(ctx context.Context, c *cli.Command) error {
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
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
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
			summary, err := summarizer.GetDatabaseSummary(ctx)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve database summary"))
			}

			// Output result based on format specified
			switch output {
			case outputFormatPlain:
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
		Action: func(ctx context.Context, c *cli.Command) error {
			assetPath := c.Args().Get(0)
			if assetPath == "" {
				printErrorJSON(errors2.New("empty asset path given, you must provide an existing asset path"))
				return cli.Exit("", 1)
			}

			if c.Bool("convert") {
				return convertToBruinAsset(afero.NewOsFs(), assetPath) //nolint:contextcheck
			}

			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath, nil)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to create asset from the given path"))
				return cli.Exit("", 1)
			}

			asset, err = DefaultPipelineBuilder.MutateAsset(ctx, asset, nil)
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

func PatchPipeline() *cli.Command {
	return &cli.Command{
		Name:      "patch-pipeline",
		Usage:     "patch a single Bruin pipeline with the given fields",
		ArgsUsage: "[path to the pipeline definition]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "body",
				Usage:    "the JSON object containing the patch body",
				Required: false,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				printErrorJSON(errors2.New("empty pipeline path given, you must provide an existing pipeline path"))
				return cli.Exit("", 1)
			}

			p, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate(), pipeline.WithOnlyPipeline())
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to load pipeline"))
				return cli.Exit("", 1)
			}

			if err := json.Unmarshal([]byte(c.String("body")), &p); err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to apply patch to pipeline"))
				return cli.Exit("", 1)
			}

			p.DefinitionFile.Path = pipelinePath

			fs := afero.NewOsFs()
			err = p.Persist(fs)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to save the pipeline to the file"))
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
	ext := strings.ToLower(filepath.Ext(filePath))

	// Try to determine the majority asset type from the pipeline
	assetType := pipeline.AssetTypeBigqueryQuery // default fallback
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
		Type: assetType,
		ExecutableFile: pipeline.ExecutableFile{
			Name:    fileName,
			Path:    filePath,
			Content: string(content),
		},
	}

	if ext == ".py" {
		asset.Type = ""
		asset.Description = "this is a python asset"
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
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
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
			// ctx is already available from function signature
			databases, err := fetcher.GetDatabases(ctx)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve databases"))
			}

			// Output result based on format specified
			switch output {
			case outputFormatPlain:
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
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
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
			// ctx is already available from function signature
			tables, err := fetcher.GetTables(ctx, databaseName)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve tables"))
			}

			// Output result based on format specified
			switch output {
			case outputFormatPlain:
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
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
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
			// ctx is already available from function signature
			columns, err := fetcher.GetColumns(ctx, databaseName, tableName)
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to retrieve columns"))
			}

			// Output result based on format specified
			switch output {
			case outputFormatPlain:
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

func ListTemplates() *cli.Command {
	return &cli.Command{
		Name:   "list-templates",
		Usage:  "List all available Bruin templates",
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Value:       "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			output := c.String("output")

			folders, err := templates.Templates.ReadDir(".")
			if err != nil {
				return handleError(output, errors2.Wrap(err, "failed to read templates directory"))
			}

			templateList := make([]string, 0)
			for _, entry := range folders {
				if entry.IsDir() {
					templateList = append(templateList, entry.Name())
				}
			}

			// Output result based on format specified
			switch output {
			case outputFormatPlain:
				printTemplates(templateList)
			case "json":
				type jsonResponse struct {
					Templates []string `json:"templates"`
					Count     int      `json:"count"`
				}

				finalOutput := jsonResponse{
					Templates: templateList,
					Count:     len(templateList),
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

func printTemplates(templates []string) {
	if len(templates) == 0 {
		fmt.Println("No templates found")
		return
	}

	fmt.Printf("Found %d template(s):\n", len(templates))
	fmt.Println(strings.Repeat("=", 30))

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Template"})

	for _, template := range templates {
		t.AppendRow(table.Row{template})
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}

func AssetMetadata() *cli.Command {
	return &cli.Command{
		Name:      "asset-metadata",
		Usage:     "run a dry-run for a BigQuery asset or sensor and return query metadata",
		ArgsUsage: "[path to the asset sql file or sensor yaml file]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"e", "env"},
				Usage:   "the environment to use",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
			&cli.StringFlag{
				Name:        "start-date",
				Usage:       "the start date of the range the asset will run for in YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff format",
				DefaultText: "beginning of yesterday, e.g. " + defaultStartDate.Format("2006-01-02 15:04:05.000000"),
				Value:       defaultStartDate.Format("2006-01-02 15:04:05.000000"),
				Sources:     cli.EnvVars("BRUIN_START_DATE"),
			},
			&cli.StringFlag{
				Name:        "end-date",
				Usage:       "the end date of the range the asset will run for in YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff format",
				DefaultText: "end of yesterday, e.g. " + defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
				Value:       defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
				Sources:     cli.EnvVars("BRUIN_END_DATE"),
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			assetPath := c.Args().Get(0)
			if assetPath == "" {
				return cli.Exit("asset path is required", 1)
			}

			environment := c.String("environment")

			fs := afero.NewOsFs()

			pp, err := GetPipelineAndAsset(ctx, assetPath, fs, c.String("config-file"))
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			// Load config and switch environment if specified
			cm := pp.Config
			if environment != "" {
				err = switchEnvironment(environment, false, cm, os.Stdin)
				if err != nil {
					return err
				}
			}

			manager, errs := connection.NewManagerFromConfig(cm)
			if len(errs) > 0 {
				printErrorJSON(errs[0])
				return cli.Exit("", 1)
			}

			startDateStr := c.String("start-date")
			endDateStr := c.String("end-date")

			startDate, endDate, err := ParseDate(startDateStr, endDateStr, makeLogger(false))
			if err != nil {
				return cli.Exit("", 1)
			}

			renderer := jinja.NewRendererWithStartEndDates(&startDate, &endDate, pp.Pipeline.Name, "asset-metadata-run", pp.Pipeline.Variables.Value())
			whole := &query.WholeFileExtractor{Fs: afero.NewOsFs(), Renderer: renderer}
			extractor, err := whole.CloneForAsset(ctx, pp.Pipeline, pp.Asset)
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			dryRunner := bigquery.DryRunner{
				ConnectionGetter: manager,
				QueryExtractor:   extractor,
			}

			response, err := dryRunner.DryRun(ctx, *pp.Pipeline, *pp.Asset, pp.Config)
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			out, err := json.Marshal(response)
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}
			fmt.Println(string(out))
			return nil
		},
	}
}
