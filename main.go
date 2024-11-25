package main

import (
	"os"
	"time"

	"github.com/bruin-data/bruin/cmd"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/fatih/color"
	"github.com/rudderlabs/analytics-go/v4"
	"github.com/urfave/cli/v2"
)

var (
	version = "dev"
	commit  = ""
)

func main() {
	start := time.Now()
	isDebug := false
	color.NoColor = false

	var optOut bool
	if os.Getenv("TELEMETRY_OPTOUT") != "" {
		optOut = true
	}

	telemetry.TelemetryKey = os.Getenv("TELEMETRY_KEY")
	telemetry.OptOut = optOut
	telemetry.AppVersion = version

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
		Before: func(context *cli.Context) error {
			telemetry.SendEvent("command", analytics.Properties{
				"command_start": context.Command.Name,
				"args":          context.Args().Slice(),
			})
			return nil
		},
		After: func(context *cli.Context) error {
			telemetry.SendEvent("command", analytics.Properties{
				"command_finish": context.Command.Name,
				"duration":       time.Since(start).Seconds(),
			})
			return nil
		},
		ExitErrHandler: func(context *cli.Context, err error) {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}
			telemetry.SendEvent("command", analytics.Properties{
				"command_error": context.Command.Name,
				"args":          context.Args().Slice(),
				"error":         errMsg,
				"duration":      time.Since(start).Seconds(),
			})
			cli.HandleExitCoder(err)
		},
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
			cmd.Docs(),
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
