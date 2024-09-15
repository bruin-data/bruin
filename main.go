package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/bruin-data/bruin/cmd"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

var Version = "development"

func main() {
	isDebug := false
	color.NoColor = false

	cli.VersionPrinter = func(cCtx *cli.Context) {
		var commit = func() string {
			if info, ok := debug.ReadBuildInfo(); ok {
				for _, setting := range info.Settings {
					if setting.Key == "vcs.revision" {
						return setting.Value
					}
				}
			}
			return ""
		}()

		fmt.Printf("bruin CLI %s (%s)\n", cCtx.App.Version, commit)
	}

	app := &cli.App{
		Name:     "bruin",
		Version:  Version,
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
		},
	}

	_ = app.Run(os.Args)
}
