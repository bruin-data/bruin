package cmd

import (
	"encoding/json"
	"fmt"

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
