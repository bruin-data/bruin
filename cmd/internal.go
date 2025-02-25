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
	"github.com/bruin-data/bruin/pkg/telemetry"
	color2 "github.com/fatih/color"
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
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
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
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
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

			return r.ParsePipeline(c.Args().Get(0), c.Bool("column-lineage"), c.Bool("exp-slim-response"))
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
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
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

type ParseCommand struct {
	builder      taskCreator
	errorPrinter *color2.Color
}

func (r *ParseCommand) ParsePipeline(assetPath string, lineage bool, slimResponse bool) error {
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

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, pipelineDefinitionFiles)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath, true)
	if err != nil {
		printErrorJSON(err)

		return cli.Exit("", 1)
	}

	if lineage {
		panics := lineageWg.WaitAndRecover()
		if panics != nil {
			return cli.Exit("", 1)
		}

		defer sqlParser.Close()
		processedAssets := make(map[string]bool)
		lineage := pipeline.NewLineageExtractor(sqlParser)
		for _, asset := range foundPipeline.Assets {
			if err := lineage.ColumnLineage(foundPipeline, asset, processedAssets); err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
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

func (r *ParseCommand) Run(assetPath string, lineage bool) error {
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

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, pipelineDefinitionFiles)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	repoRoot, err := git.FindRepoFromPath(assetPath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath, true)
	if err != nil {
		printErrorJSON(err)

		return cli.Exit("", 1)
	}

	asset := foundPipeline.GetAssetByPath(assetPath)

	if lineage {
		panics := lineageWg.WaitAndRecover()
		if panics != nil {
			return cli.Exit("", 1)
		}

		processedAssets := make(map[string]bool)
		lineageExtractor := pipeline.NewLineageExtractor(sqlParser)
		if err := lineageExtractor.ColumnLineage(foundPipeline, asset, processedAssets); err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}
	}

	type pipelineSummary struct {
		Name     string            `json:"name"`
		Schedule pipeline.Schedule `json:"schedule"`
	}

	js, err := json.Marshal(struct {
		Asset    *pipeline.Asset `json:"asset"`
		Pipeline pipelineSummary `json:"pipeline"`
		Repo     *git.Repo       `json:"repo"`
	}{
		Asset: asset,
		Pipeline: pipelineSummary{
			Name:     foundPipeline.Name,
			Schedule: foundPipeline.Schedule,
		},
		Repo: repoRoot,
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
				Required: true,
			},
		},
		Action: func(c *cli.Context) error {
			assetPath := c.Args().Get(0)
			if assetPath == "" {
				printErrorJSON(errors.New("empty asset path given, you must provide an existing asset path"))
				return cli.Exit("", 1)
			}

			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath, nil)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to create asset from the given path"))
				return cli.Exit("", 1)
			}

			asset, err = DefaultPipelineBuilder.MutateAsset(asset, nil)
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
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}
