package cmd

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	color2 "github.com/fatih/color"
	errors2 "github.com/pkg/errors"
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

			return r.Run(c.Args().Get(0), c.Bool("column-lineage"))
		},
	}
}

func ParsePipeline() *cli.Command {
	return &cli.Command{
		Name:      "parse-pipeline",
		Usage:     "parse a full Bruin pipeline",
		ArgsUsage: "[path to the any asset or anywhere in the pipeline]",
		Action: func(c *cli.Context) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.ParsePipeline(c.Args().Get(0))
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

type ParseCommand struct {
	builder      taskCreator
	errorPrinter *color2.Color
}

func (r *ParseCommand) ParsePipeline(assetPath string) error {
	defer RecoverFromPanic()

	if assetPath == "" {
		errorPrinter.Printf("Please give an asset path to parse: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, pipelineDefinitionFile)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath)
	if err != nil {
		printErrorJSON(err)

		return cli.Exit("", 1)
	}

	foundPipeline.WipeContentOfAssets()

	js, err := json.MarshalIndent(foundPipeline, "", "  ")
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	fmt.Println(string(js))

	return err
}

func (r *ParseCommand) Run(assetPath string, lineage bool) error {
	defer RecoverFromPanic()

	if assetPath == "" {
		errorPrinter.Printf("Please give an asset path to parse: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, pipelineDefinitionFile)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	repoRoot, err := git.FindRepoFromPath(assetPath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath)
	if err != nil {
		printErrorJSON(err)

		return cli.Exit("", 1)
	}

	asset := foundPipeline.GetAssetByPath(assetPath)

	if lineage {
		if err := ParseLineage(foundPipeline, asset); err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}
	}

	result := struct {
		Asset    *pipeline.Asset    `json:"asset"`
		Pipeline *pipeline.Pipeline `json:"pipeline"`
		Repo     *git.Repo          `json:"repo"`
	}{
		Asset:    asset,
		Pipeline: foundPipeline,
		Repo:     repoRoot,
	}

	js, err := json.MarshalIndent(result, "", "  ")
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
				Required: true,
			},
		},
		Action: func(c *cli.Context) error {
			assetPath := c.Args().Get(0)
			if assetPath == "" {
				printErrorJSON(errors.New("empty asset path given, you must provide an existing asset path"))
				return cli.Exit("", 1)
			}

			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to create asset from the given path"))
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

// ParseLineage analyzes the column lineage for a given asset within a pipeline.
// It traces column relationships between the asset and its upstream dependencies.
func ParseLineage(pipe *pipeline.Pipeline, asset *pipeline.Asset) error {
	parser, err := sqlparser.NewSQLParser()
	if err != nil {
		return fmt.Errorf("failed to create SQL parser: %w", err)
	}

	if err := parser.Start(); err != nil {
		return fmt.Errorf("failed to start SQL parser: %w", err)
	}

	columnMetadata := make(sqlparser.Schema)
	for _, upstream := range asset.Upstreams {
		upstreamAsset := pipe.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			return fmt.Errorf("upstream asset not found: %s", upstream.Value)
		}
		columnMetadata[upstreamAsset.Name] = makeColumnMap(upstreamAsset.Columns)
	}

	lineage, err := parser.ColumnLineage(asset.ExecutableFile.Content, "", columnMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse column lineage: %w", err)
	}

	for _, lineageCol := range lineage.Columns {
		for _, upstream := range lineageCol.Upstream {
			upstreamAsset := pipe.GetAssetByName(upstream.Table)
			if upstreamAsset == nil {
				return fmt.Errorf("upstream asset not found: %s", upstream.Table)
			}

			upstreamCol := upstreamAsset.GetColumnWithName(upstream.Column)
			if upstreamCol == nil {
				return fmt.Errorf("column %s not found in asset %s", upstream.Column, upstream.Table)
			}

			newCol := *upstreamCol
			newCol.Name = lineageCol.Name

			if col := asset.GetColumnWithName(lineageCol.Name); col == nil {
				asset.Columns = append(asset.Columns, newCol)
			} else {
				*col = newCol
			}
		}
	}

	return nil
}

// makeColumnMap creates a map of column names to their types from a slice of columns.
func makeColumnMap(columns []pipeline.Column) map[string]string {
	columnMap := make(map[string]string, len(columns))
	for _, col := range columns {
		columnMap[col.Name] = col.Type
	}
	return columnMap
}
