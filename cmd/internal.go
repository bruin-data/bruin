package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/bruin-data/bruin/pkg/sqlparser"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	color2 "github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func Internal() *cli.Command {
	return &cli.Command{
		Name:   "internal",
		Hidden: true,
		Subcommands: []*cli.Command{
			ParseAsset(),
			SqlParseTest(),
		},
	}
}

func ParseAsset() *cli.Command {
	return &cli.Command{
		Name:      "parse-asset",
		Usage:     "parse a single Bruin asset",
		ArgsUsage: "[path to the asset definition]",
		Action: func(c *cli.Context) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.Run(c.Args().Get(0))
		},
	}
}

func SqlParseTest() *cli.Command {
	return &cli.Command{
		Name: "sqlparse-test",
		Action: func(c *cli.Context) error {
			p, err := sqlparser.NewSqlParser()
			if err != nil {
				errorPrinter.Printf("failed to initialize sqlparser module: %+v\n", err)
				return cli.Exit("", 1)
			}

			err = p.Start()
			if err != nil {
				errorPrinter.Printf("failed to start sqlparser module: %+v\n", err)
				return cli.Exit("", 1)
			}

			query := `SELECT
					sales.id,
					CASE
						WHEN sales.amount > 500 THEN 'large'
						WHEN sales.amount > 100 THEN 'medium'
						ELSE 'small'
					END as sale_size,
					CASE
						WHEN regions.name = 'North' THEN 'N'
						WHEN regions.name = 'South' THEN 'S'
						ELSE 'Other'
					END as region_abbr
				FROM sales
				JOIN regions ON sales.region_id = regions.id`

			schema := sqlparser.Schema{
				"sales":   {"id": "str", "amount": "int64", "region_id": "str"},
				"regions": {"id": "str", "name": "str"},
			}

			res, err := p.ColumnLineage(query, "bigquery", schema)
			if err != nil {
				errorPrinter.Printf("failed to get column lineage: %+v\n", err)
				return cli.Exit("", 1)
			}

			js, err := json.Marshal(res)
			if err != nil {
				errorPrinter.Printf("failed to marshal result: %+v\n", err)
				return cli.Exit("", 1)
			}

			expected := "{\"columns\":[{\"name\":\"id\",\"upstream\":[{\"column\":\"id\",\"table\":\"sales\"}]},{\"name\":\"region_abbr\",\"upstream\":[{\"column\":\"name\",\"table\":\"regions\"}]},{\"name\":\"sale_size\",\"upstream\":[{\"column\":\"amount\",\"table\":\"sales\"}]}]}"
			if string(js) != expected {
				errorPrinter.Printf("expected: %s, got: %s\n", expected, string(js))
				return cli.Exit("", 1)
			}

			fmt.Println()
			successPrinter.Println("Success, it seems like the query parsing has worked! Please let Burak know that it worked.")
			fmt.Println()
			return nil
		},
	}
}

type ParseCommand struct {
	builder      taskCreator
	errorPrinter *color2.Color
}

func (r *ParseCommand) Run(assetPath string) error {
	defer RecoverFromPanic()

	if assetPath == "" {
		errorPrinter.Printf("Please give an asset path to parse: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	asset, err := r.builder.CreateAssetFromFile(assetPath)
	if err != nil {
		errorPrinter.Printf("Failed to build asset: %v\n", err.Error())
		return cli.Exit("", 1)
	}

	if asset == nil {
		errorPrinter.Printf("The given file path doesn't seem to be a Bruin asset definition: '%s'\n", assetPath)
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, pipelineDefinitionFile)
	if err != nil {
		r.errorPrinter.Printf("Failed to find the pipeline this asset belongs to: '%s'\n", assetPath)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath)
	if err != nil {
		r.errorPrinter.Println("failed to build pipeline, are you sure you have referred the right path?")
		r.errorPrinter.Println("\nHint: You need to run this command with a path to the asset file itself directly, and it needs to be inside a pipeline.")

		return cli.Exit("", 1)
	}

	repo, err := git.FindRepoFromPath(pipelinePath)
	if err != nil {
		r.errorPrinter.Println(err)
		return cli.Exit("", 1)
	}

	foundPipeline.Assets = nil

	js, err := json.Marshal(struct {
		Asset    *pipeline.Asset    `json:"asset"`
		Pipeline *pipeline.Pipeline `json:"pipeline"`
		Repo     *git.Repo          `json:"repo"`
	}{
		Asset:    asset,
		Pipeline: foundPipeline,
		Repo:     repo,
	})

	fmt.Println(string(js))

	return err
}
