package cmd

import (
	"context"
	"fmt"
	"os"
	path2 "path"
	"time"

	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/databricks"
	"github.com/bruin-data/bruin/pkg/date"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func RenderDDL() *cli.Command {
	return &cli.Command{
		Name:                      "render-ddl",
		Usage:                     "render a single Bruin SQL asset as DDL",
		ArgsUsage:                 "[path to the asset definition]",
		DisableSliceFlagSeparator: true,
		Flags: []cli.Flag{
			startDateFlag,
			endDateFlag,
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "output format (json)",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
			&cli.BoolFlag{
				Name:  "apply-interval-modifiers",
				Usage: "applies interval modifiers if flag is given",
			},
			&cli.StringSliceFlag{
				Name:  "var",
				Usage: "override pipeline variables with custom values",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if vars := c.StringSlice("var"); len(vars) > 0 {
				DefaultPipelineBuilder.AddPipelineMutator(variableOverridesMutator(vars))
			}

			inputPath := c.Args().Get(0)
			if inputPath == "" {
				if c.String("output") == "json" {
					printErrorJSON(errors.New("Please give an asset path to render: bruin render-ddl <path to the asset file>)"))
				} else {
					errorPrinter.Printf("Please give an asset path to render: bruin render-ddl <path to the asset file>)\n")
				}

				return cli.Exit("", 1)
			}
			if _, err := os.Stat(inputPath); os.IsNotExist(err) {
				if c.String("output") == "json" {
					printErrorJSON(errors.New("The specified asset path does not exist: " + inputPath))
				} else {
					errorPrinter.Printf("The specified asset path does not exist: %s\n", inputPath)
				}
				return cli.Exit("", 1)
			}
			pipelinePath, err := path.GetPipelineRootFromTask(inputPath, PipelineDefinitionFiles)
			if err != nil {
				printError(err, c.String("output"), "Failed to get the pipeline path:")
				return cli.Exit("", 1)
			}

			pipelineDefinitionFullPath, err := getPipelineDefinitionFullPath(pipelinePath)
			if err != nil {
				printError(err, c.String("output"), "Failed to locate a valid pipeline definition file")
				return cli.Exit("", 1)
			}

			pl, err := pipeline.PipelineFromPath(pipelineDefinitionFullPath, fs)
			if err != nil {
				printError(err, c.String("output"), "Failed to read the pipeline definition file:")
				return cli.Exit("", 1)
			}

			pl, err = DefaultPipelineBuilder.MutatePipeline(ctx, pl)
			if err != nil {
				printError(err, c.String("output"), "Failed to mutate the pipeline:")
				return cli.Exit("", 1)
			}

			// For DDL rendering, we use a default start/end date since DDL doesn't depend on time ranges
			startDate := time.Now().AddDate(0, 0, -1) // Yesterday as default
			if c.String("start-date") != "" {
				startDate, err = date.ParseTime(c.String("start-date"))
				if err != nil {
					if c.String("output") == "json" {
						printErrorJSON(errors.New("Please give a valid start date: bruin render-ddl --start-date <start date>), A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats."))
					} else {
						errorPrinter.Printf("Please give a valid start date: bruin render-ddl --start-date <start date>)\n")
						errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
						errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
						errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
					}
					return cli.Exit("", 1)
				}
			}

			endDate := time.Now() // Today as default
			if c.String("end-date") != "" {
				endDate, err = date.ParseTime(c.String("end-date"))
				if err != nil {
					if c.String("output") == "json" {
						printErrorJSON(errors.New("Please give a valid end date: bruin render-ddl --end-date <end date>), A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats."))
					} else {
						errorPrinter.Printf("Please give a valid end date: bruin render-ddl --start-date <start date>)\n")
						errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
						errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
						errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
					}
					return cli.Exit("", 1)
				}
			}

			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(inputPath, pl)
			if err != nil {
				printError(err, c.String("output"), "Failed to read the asset definition file:")
				return cli.Exit("", 1)
			}

			asset, err = DefaultPipelineBuilder.MutateAsset(ctx, asset, pl)
			if err != nil {
				printError(errors.New("failed to mutate the asset"), c.String("output"), "Failed to mutate the asset:")
				return cli.Exit("", 1)
			}

			if asset == nil {
				printError(errors.New("no asset found"), c.String("output"), "Failed to read the asset definition file:")
				return cli.Exit("", 1)
			}

			resultsLocation := "s3://{destination-bucket}"
			if asset.Type == pipeline.AssetTypeAthenaQuery {
				connName, err := pl.GetConnectionNameForAsset(asset)
				if err != nil {
					printError(err, c.String("output"), "Failed to get the connection name for the asset:")
					return cli.Exit("", 1)
				}

				configFilePath := c.String("config-file")
				if configFilePath == "" {
					repoRoot, err := git.FindRepoFromPath(inputPath)
					if err != nil {
						printError(err, c.String("output"), "Failed to find the git repository root:")
						return cli.Exit("", 1)
					}
					configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
				}

				cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
				if err != nil {
					printError(err, c.String("output"), fmt.Sprintf("Failed to load the config file at '%s':", configFilePath))
					return cli.Exit("", 1)
				}

				for _, conn := range cm.SelectedEnvironment.Connections.AthenaConnection {
					if conn.Name == connName {
						resultsLocation = conn.QueryResultsPath
						break
					}
				}
			}

			runCtx := context.WithValue(ctx, pipeline.RunConfigFullRefresh, false) // DDL doesn't use full refresh
			runCtx = context.WithValue(runCtx, pipeline.RunConfigRunID, "your-run-id")
			runCtx = context.WithValue(runCtx, pipeline.RunConfigStartDate, startDate)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigEndDate, endDate)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigApplyIntervalModifiers, c.Bool("apply-interval-modifiers"))

			// Load macros from the pipeline's macros directory
			macroContent, err := jinja.LoadMacros(fs, pl.MacrosPath)
			if err != nil {
				printError(err, c.String("output"), "Failed to load macros:")
				return cli.Exit("", 1)
			}

			renderer := jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, pl.Name, "your-run-id", pl.Variables.Value(), macroContent)
			forAsset, err := renderer.CloneForAsset(runCtx, pl, asset)
			if err != nil {
				return err
			}

			r := RenderCommand{
				extractor: &query.WholeFileExtractor{
					Fs:       fs,
					Renderer: forAsset,
				},
				materializers: map[pipeline.AssetType]queryMaterializer{
					pipeline.AssetTypeMySQLQuery:            mysql.NewDDLMaterializer(),
					pipeline.AssetTypeBigqueryQuery:         bigquery.NewDDLMaterializer(),
					pipeline.AssetTypeBigqueryQuerySensor:   bigquery.NewDDLMaterializer(),
					pipeline.AssetTypeSnowflakeQuery:        snowflake.NewDDLMaterializer(),
					pipeline.AssetTypeSnowflakeQuerySensor:  snowflake.NewDDLMaterializer(),
					pipeline.AssetTypeRedshiftQuery:         postgres.NewDDLMaterializer(),
					pipeline.AssetTypeRedshiftQuerySensor:   postgres.NewDDLMaterializer(),
					pipeline.AssetTypePostgresQuery:         postgres.NewDDLMaterializer(),
					pipeline.AssetTypePostgresQuerySensor:   postgres.NewDDLMaterializer(),
					pipeline.AssetTypeDatabricksQuery:       databricks.NewDDLRenderer(),
					pipeline.AssetTypeDatabricksQuerySensor: databricks.NewDDLRenderer(),
					pipeline.AssetTypeAthenaQuery:           athena.NewDDLRenderer(resultsLocation),
					pipeline.AssetTypeAthenaSQLSensor:       athena.NewDDLRenderer(resultsLocation),
					pipeline.AssetTypeDuckDBQuery:           duck.NewDDLMaterializer(),
					pipeline.AssetTypeDuckDBQuerySensor:     duck.NewDDLMaterializer(),
					pipeline.AssetTypeClickHouse:            clickhouse.NewDDLRenderer(),
					pipeline.AssetTypeClickHouseQuerySensor: clickhouse.NewDDLRenderer(),
				},
				builder: DefaultPipelineBuilder,
				writer:  os.Stdout,
				output:  c.String("output"),
			}
			modifierInfo := ModifierInfo{
				StartDate:      startDate,
				EndDate:        endDate,
				ApplyModifiers: c.Bool("apply-interval-modifiers"),
			}

			return r.Run(pl, asset, modifierInfo)
		},
	}
}
