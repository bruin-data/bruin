package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

func Lineage() *cli.Command {
	return &cli.Command{
		Name:      "lineage",
		Usage:     "dump the lineage for a given asset",
		ArgsUsage: "[path to the asset definition]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "full",
				Usage: "display all the upstream and downstream dependencies even if they are not direct dependencies",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
		},
		Action: func(c *cli.Context) error {
			r := LineageCommand{
				builder:      DefaultPipelineBuilder,
				infoPrinter:  infoPrinter,
				errorPrinter: errorPrinter,
			}

			return r.Run(c.Args().Get(0), c.Bool("full"), c.String("output"))
		},
	}
}

type printer interface {
	Println(a ...interface{}) (n int, err error)
	Printf(format string, a ...interface{}) (n int, err error)
	Print(a ...interface{}) (n int, err error)
}

type LineageCommand struct {
	builder      taskCreator
	infoPrinter  printer
	errorPrinter printer
}

func (r *LineageCommand) Run(assetPath string, fullLineage bool, output string) error {
	if assetPath == "" {
		r.errorPrinter.Printf("Please give an asset path to get lineage of: bruin lineage <path to the asset definition>)\n")
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, pipelineDefinitionFile)
	if err != nil {
		r.errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", assetPath)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath)
	if err != nil {
		r.errorPrinter.Println("failed to build pipeline, are you sure you have referred the right path?")
		r.errorPrinter.Println("\nHint: You need to run this command with a path to the asset file itself directly, and it needs to be inside a pipeline.")

		return cli.Exit("", 1)
	}

	asset := foundPipeline.GetAssetByPath(assetPath)
	if asset == nil {
		r.errorPrinter.Println("failed to find the asset with the given path, are you sure you have referred the right file?")
		r.errorPrinter.Println("\nHint: You need to run this command with a path to the asset file itself directly, and it needs to be inside a pipeline.")

		return cli.Exit("", 1)
	}

	upstream := asset.GetUpstream()
	downstream := asset.GetDownstream()
	if fullLineage {
		upstream = asset.GetFullUpstream()
		downstream = asset.GetFullDownstream()
	}

	if output == "json" {
		return r.printLineageJSON(asset, upstream, downstream)
	}

	r.infoPrinter.Printf("\nLineage: '%s'", asset.Name)

	r.printLineageSummary(foundPipeline, upstream, "Upstream Dependencies", "Asset has no upstream dependencies.")
	r.printLineageSummary(foundPipeline, downstream, "Downstream Dependencies", "Asset has no downstream dependencies.")

	return err
}

func (r *LineageCommand) printLineageJSON(asset *pipeline.Asset, upstream, downstream []*pipeline.Asset) error {
	type dependencySummary struct {
		Name           string                       `json:"name"`
		Type           pipeline.AssetType           `json:"type"`
		ExecutableFile *pipeline.ExecutableFile     `json:"executable_file"`
		DefinitionFile *pipeline.TaskDefinitionFile `json:"definition_file"`
	}

	type jsonSummary struct {
		AssetName  string               `json:"name"`
		Type       pipeline.AssetType   `json:"type"`
		Upstream   []*dependencySummary `json:"upstream"`
		Downstream []*dependencySummary `json:"downstream"`
	}

	summary := jsonSummary{
		AssetName:  asset.Name,
		Type:       asset.Type,
		Upstream:   make([]*dependencySummary, len(upstream)),
		Downstream: make([]*dependencySummary, len(downstream)),
	}

	for i, u := range upstream {
		summary.Upstream[i] = &dependencySummary{
			Name: u.Name,
			Type: u.Type,
			ExecutableFile: &pipeline.ExecutableFile{
				Name:    u.ExecutableFile.Name,
				Path:    u.ExecutableFile.Path,
				Content: "",
			},
			DefinitionFile: &u.DefinitionFile,
		}
	}

	for i, d := range downstream {
		summary.Downstream[i] = &dependencySummary{
			Name: d.Name,
			Type: d.Type,
			ExecutableFile: &pipeline.ExecutableFile{
				Name:    d.ExecutableFile.Name,
				Path:    d.ExecutableFile.Path,
				Content: "",
			},
			DefinitionFile: &d.DefinitionFile,
		}
	}

	jsonVersion, err := json.Marshal(summary)
	if err != nil {
		return errors.Wrap(err, "failed to marshal the lineage summary to json")
	}

	fmt.Println(string(jsonVersion))
	return nil
}

func (r *LineageCommand) printLineageSummary(p *pipeline.Pipeline, assets []*pipeline.Asset, title string, absenceMessage string) {
	r.infoPrinter.Print("\n\n")
	r.infoPrinter.Println(title)
	r.infoPrinter.Println("========================")
	if len(assets) == 0 {
		r.infoPrinter.Println(absenceMessage)
	} else {
		for _, u := range assets {
			r.infoPrinter.Printf("- %s %s\n", u.Name, faint(fmt.Sprintf("(%s)", p.RelativeAssetPath(u))))
		}
		r.infoPrinter.Printf("\nTotal: %d\n", len(assets))
	}
}
