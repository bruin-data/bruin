package main

import (
	"context"
	"os"

	"github.com/bruin-data/bruin/cmd"
	"github.com/bruin-data/bruin/cmd/mcp"
	"github.com/bruin-data/bruin/pkg/telemetry"
	v "github.com/bruin-data/bruin/pkg/version"
	"github.com/fatih/color"
	"github.com/urfave/cli/v3"
)

var (
	version      = "dev"
	commit       = ""
	telemetryKey string
)

func main() {
	isDebug := false
	color.NoColor = false
	var optOut bool
	if os.Getenv("TELEMETRY_OPTOUT") != "" {
		optOut = true
	}

	v.Version = version
	v.Commit = commit
	if telemetryKey == "" {
		telemetryKey = os.Getenv("TELEMETRY_KEY")
	}
	telemetry.TelemetryKey = telemetryKey
	telemetry.OptOut = optOut
	telemetry.AppVersion = v.Version
	client := telemetry.Init()
	defer client.Close()

	versionCommand := cmd.VersionCmd(v.Commit)

	cli.VersionPrinter = func(cmd *cli.Command) {
		err := versionCommand.Action(context.Background(), cmd)
		if err != nil {
			panic(err)
		}
	}

	app := &cli.Command{
		Name:    "bruin",
		Version: version,
		Usage:   "The CLI used for managing Bruin-powered data pipelines",
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
			cmd.RenderDDL(),
			cmd.Lineage(),
			cmd.CleanCmd(),
			cmd.Format(&isDebug),
			cmd.Docs(),
			cmd.Init(),
			cmd.Internal(),
			cmd.Environments(&isDebug),
			cmd.Connections(),
			cmd.Query(),
			cmd.Patch(),
			cmd.DataDiffCmd(),
			cmd.Import(),
			cmd.Update(),
			mcp.MCPCmd(),
			versionCommand,
		},
		DisableSliceFlagSeparator: true,
	}

	err := app.Run(context.Background(), os.Args)

	if err != nil {
		cli.HandleExitCoder(err)
		// Close the telemetry client manually as the defer is not called on os.Exit(1)
		client.Close()
		os.Exit(1) //nolint:gocritic
	}
}
