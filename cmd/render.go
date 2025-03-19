package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	path2 "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/databricks"
	"github.com/bruin-data/bruin/pkg/date"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/bruin-data/bruin/pkg/synapse"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Render() *cli.Command {
	return &cli.Command{
		Name:      "render",
		Usage:     "render a single Bruin SQL asset",
		ArgsUsage: "[path to the asset definition]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "full-refresh",
				Aliases: []string{"r"},
				Usage:   "truncate the table before running",
			},
			startDateFlag,
			endDateFlag,
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "output format (json)",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			fullRefresh := c.Bool("full-refresh")

			startDate, err := date.ParseTime(c.String("start-date"))
			if err != nil {
				if c.String("output") == "json" {
					printErrorJSON(errors.New("Please give a valid start date: bruin run --start-date <start date>), A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats."))
				} else {
					errorPrinter.Printf("Please give a valid start date: bruin run --start-date <start date>)\n")
					errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
					errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
					errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				}
				return cli.Exit("", 1)
			}

			endDate, err := date.ParseTime(c.String("end-date"))
			if err != nil {
				if c.String("output") == "json" {
					printErrorJSON(errors.New("Please give a valid end date: bruin run --end-date <end date>), A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats."))
				} else {
					errorPrinter.Printf("Please give a valid end date: bruin run --start-date <start date>)\n")
					errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
					errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
					errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				}
				return cli.Exit("", 1)
			}

			inputPath := c.Args().Get(0)
			if inputPath == "" {
				if c.String("output") == "json" {
					printErrorJSON(errors.New("Please give an asset path to render: bruin render <path to the asset file>)"))
				} else {
					errorPrinter.Printf("Please give an asset path to render: bruin render <path to the asset file>)\n")
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
			pipelinePath, err := path.GetPipelineRootFromTask(inputPath, pipelineDefinitionFiles)
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

			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(inputPath, pl)
			if err != nil {
				printError(err, c.String("output"), "Failed to read the asset definition file:")
				return cli.Exit("", 1)
			}

			asset, err = DefaultPipelineBuilder.MutateAsset(asset, pl)
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

			r := RenderCommand{
				extractor: &query.WholeFileExtractor{
					Fs:       fs,
					Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, "your-pipeline-name", "your-run-id"),
				},
				materializers: map[pipeline.AssetType]queryMaterializer{
					pipeline.AssetTypeBigqueryQuery:   bigquery.NewMaterializer(fullRefresh),
					pipeline.AssetTypeSnowflakeQuery:  snowflake.NewMaterializer(fullRefresh),
					pipeline.AssetTypeRedshiftQuery:   postgres.NewMaterializer(fullRefresh),
					pipeline.AssetTypePostgresQuery:   postgres.NewMaterializer(fullRefresh),
					pipeline.AssetTypeMsSQLQuery:      mssql.NewMaterializer(fullRefresh),
					pipeline.AssetTypeDatabricksQuery: databricks.NewRenderer(fullRefresh),
					pipeline.AssetTypeSynapseQuery:    synapse.NewRenderer(fullRefresh),
					pipeline.AssetTypeAthenaQuery:     athena.NewRenderer(fullRefresh, resultsLocation),
					pipeline.AssetTypeDuckDBQuery:     duck.NewMaterializer(fullRefresh),
					pipeline.AssetTypeClickHouse:      clickhouse.NewRenderer(fullRefresh),
				},
				builder: DefaultPipelineBuilder,
				writer:  os.Stdout,
				output:  c.String("output"),
			}

			return r.Run(asset, pl)
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

type queryExtractor interface {
	ExtractQueriesFromString(content string) ([]*query.Query, error)
}

type queryMaterializer interface {
	Render(asset *pipeline.Asset, query string) (string, error)
}

type taskCreator interface {
	CreateAssetFromFile(path string, foundPipeline *pipeline.Pipeline) (*pipeline.Asset, error)
}

type RenderCommand struct {
	extractor     queryExtractor
	materializers map[pipeline.AssetType]queryMaterializer
	builder       taskCreator

	output string
	writer io.Writer
}

func (r *RenderCommand) Run(task *pipeline.Asset, foundPipeline *pipeline.Pipeline) error {
	defer RecoverFromPanic()
	var err error
	if task == nil {
		return errors.New("failed to find the asset: asset cannot be nil")
	}

	queries, err := r.extractor.ExtractQueriesFromString(task.ExecutableFile.Content)
	if err != nil {
		r.printErrorOrJSON(err.Error())
		return cli.Exit("", 1)
	}

	qq := queries[0]

	if materializer, ok := r.materializers[task.Type]; ok {
		materialized, err := materializer.Render(task, qq.Query)
		if err != nil {
			r.printErrorOrJsonf("Failed to materialize the query: %v\n", err.Error())
			return cli.Exit("", 1)
		}

		qq.Query = materialized
		if task.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval {
			queries, err = r.extractor.ExtractQueriesFromString(materialized)
			if err != nil {
				r.printErrorOrJSON(err.Error())
				return cli.Exit("", 1)
			}
		}
		if r.output != "json" {
			qq.Query = highlightCode(qq.Query, "sql")
		}
	}

	if r.output == "json" {
		js, err := json.Marshal(map[string]string{"query": qq.Query})
		if err != nil {
			r.printErrorOrJsonf("Failed to render the query: %v\n", err.Error())
			return cli.Exit("", 1)
		}
		_, err = r.writer.Write(js)
		if err != nil {
			r.printErrorOrJsonf("Failed to write the query: %v\n", err.Error())
			return cli.Exit("", 1)
		}

		return nil
	} else {
		_, err = r.writer.Write([]byte(fmt.Sprintf("%s\n", qq)))
	}

	return err
}

func highlightCode(code string, language string) string {
	o, err := os.Stdout.Stat()
	if err != nil {
		return code
	}

	if (o.Mode() & os.ModeCharDevice) != os.ModeCharDevice {
		return code
	}
	b := new(strings.Builder)
	err = quick.Highlight(b, code, language, "terminal16m", "monokai")
	if err != nil {
		errorPrinter.Printf("Failed to highlight the query: %v\n", err.Error())
		return code
	}

	return b.String()
}

func (r *RenderCommand) printErrorOrJSON(msg string) {
	if r.output == "json" {
		js, err := json.Marshal(map[string]string{"error": msg})
		if err != nil {
			errorPrinter.Printf("Failed to render error message '%s': %v\n", msg, err.Error())
			return
		}
		_, err = r.writer.Write(js)
		if err != nil {
			errorPrinter.Printf("Failed to write error message: %v\n", err.Error())
		}

		return
	}

	errorPrinter.Println(msg)
}

func (r *RenderCommand) printErrorOrJsonf(msg string, args ...interface{}) {
	r.printErrorOrJSON(fmt.Sprintf(msg, args...))
}

func getPipelineDefinitionFullPath(pipelinePath string) (string, error) {
	for _, pipelineDefinitionfile := range pipelineDefinitionFiles {
		fullPath := filepath.Join(pipelinePath, pipelineDefinitionfile)
		if _, err := os.Stat(fullPath); err == nil {
			// File exists, return the full path
			return fullPath, nil
		}
	}
	return "", errors.Errorf("no pipeline definition file found in '%s'. Supported files: %v", pipelinePath, pipelineDefinitionFiles)
}
