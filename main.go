package main

import (
	"os"
	"time"

	"github.com/bruin-data/bruin/cmd"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

var (
	version = "dev"
	commit  = ""
)

func main() {
	isDebug := false
	color.NoColor = false

	versionCommand := cmd.VersionCmd(commit)

	cli.VersionPrinter = func(cCtx *cli.Context) {
		err := versionCommand.Action(cCtx)
		if err != nil {
			panic(err)
		}
	}

	app := &cli.App{
		Name:     "bruin",
		Version:  version,
		Usage:    "The CLI used for managing Bruin-powered data pipelines",
		Compiled: time.Now(),
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "debug",
				Value:       false,
				Usage:       "show debug information",
				Destination: &isDebug,
			},
		},
		Commands: []*cli.Command{
			cmd.Lint(&isDebug),
			cmd.Run(&isDebug),
			cmd.Render(),
			cmd.Lineage(),
			cmd.CleanCmd(),
			cmd.Format(&isDebug),
			cmd.Init(),
			cmd.Internal(),
			cmd.Environments(&isDebug),
			cmd.Connections(),
			cmd.Fetch(),
			versionCommand,
		},
	}

	_ = app.Run(os.Args)
}
