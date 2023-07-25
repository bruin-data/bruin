package cmd

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
		},
		Action: func(c *cli.Context) error {
			r := LineageCommand{
				builder:      builder,
				infoPrinter:  infoPrinter,
				errorPrinter: errorPrinter,
			}

			return r.Run(c.Args().Get(0), c.Bool("full"))
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

func (r *LineageCommand) Run(assetPath string, fullLineage bool) error {
	if assetPath == "" {
		r.errorPrinter.Printf("Please give an asset path to get lineage of: bruin lineage <path to the asset definition>)\n")
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, pipelineDefinitionFile)
	if err != nil {
		r.errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", assetPath)
		return cli.Exit("", 1)
	}

	foundPipeline, err := builder.CreatePipelineFromPath(pipelinePath)
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
	r.infoPrinter.Printf("\nLineage: '%s'", asset.Name)

	upstream := asset.GetUpstream()
	downstream := asset.GetDownstream()
	if fullLineage {
		upstream = asset.GetFullUpstream()
		downstream = asset.GetFullDownstream()
	}

	r.printLineageSummary(foundPipeline, upstream, "Upstream Dependencies", "Asset has no upstream dependencies.")
	r.printLineageSummary(foundPipeline, downstream, "Downstream Dependencies", "Asset has no downstream dependencies.")

	return err
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
