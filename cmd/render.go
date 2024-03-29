package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/date"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/bruin-data/bruin/pkg/synapse"
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
		},
		Action: func(c *cli.Context) error {
			fullRefresh := c.Bool("full-refresh")

			startDate, err := date.ParseTime(c.String("start-date"))
			if err != nil {
				errorPrinter.Printf("Please give a valid start date: bruin run --start-date <start date>)\n")
				errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				return cli.Exit("", 1)
			}

			endDate, err := date.ParseTime(c.String("end-date"))
			if err != nil {
				errorPrinter.Printf("Please give a valid end date: bruin run --start-date <start date>)\n")
				errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				return cli.Exit("", 1)
			}

			r := RenderCommand{
				extractor: &query.WholeFileExtractor{
					Fs:       fs,
					Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate),
				},
				materializers: map[pipeline.AssetType]queryMaterializer{
					pipeline.AssetTypeBigqueryQuery:  bigquery.NewMaterializer(fullRefresh),
					pipeline.AssetTypeSnowflakeQuery: snowflake.NewMaterializer(fullRefresh),
					pipeline.AssetTypeRedshiftQuery:  postgres.NewMaterializer(fullRefresh),
					pipeline.AssetTypePostgresQuery:  postgres.NewMaterializer(fullRefresh),
					pipeline.AssetTypeMsSQLQuery:     mssql.NewMaterializer(fullRefresh),
					pipeline.AssetTypeSynapseQuery:   synapse.NewRenderer(fullRefresh),
				},
				builder: DefaultPipelineBuilder,
				writer:  os.Stdout,
			}

			return r.Run(c.Args().Get(0))
		},
	}
}

type queryExtractor interface {
	ExtractQueriesFromString(content string) ([]*query.Query, error)
}

type queryMaterializer interface {
	Render(asset *pipeline.Asset, query string) (string, error)
}

type taskCreator interface {
	CreateAssetFromFile(path string) (*pipeline.Asset, error)
}

type RenderCommand struct {
	extractor     queryExtractor
	materializers map[pipeline.AssetType]queryMaterializer
	builder       taskCreator

	writer io.Writer
}

func (r *RenderCommand) Run(taskPath string) error {
	defer RecoverFromPanic()

	if taskPath == "" {
		errorPrinter.Printf("Please give an asset path to render: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	task, err := r.builder.CreateAssetFromFile(taskPath)
	if err != nil {
		errorPrinter.Printf("Failed to build asset: %v\n", err.Error())
		return cli.Exit("", 1)
	}

	if task == nil {
		errorPrinter.Printf("The given file path doesn't seem to be a Bruin asset definition: '%s'\n", taskPath)
		return cli.Exit("", 1)
	}

	queries, err := r.extractor.ExtractQueriesFromString(task.ExecutableFile.Content)
	if err != nil {
		errorPrinter.Printf("Failed to extract queries from file '%s': %v\n", task.ExecutableFile.Path, err.Error())
		return cli.Exit("", 1)
	}

	qq := queries[0]

	if materializer, ok := r.materializers[task.Type]; ok {
		materialized, err := materializer.Render(task, qq.Query)
		if err != nil {
			errorPrinter.Printf("Failed to materialize the query: %v\n", err.Error())
			return cli.Exit("", 1)
		}

		qq.Query = materialized
		qq.Query = highlightCode(qq.Query, "sql")
	}

	_, err = r.writer.Write([]byte(fmt.Sprintf("%s\n", qq)))

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
