package main

import (
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
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

	cli.VersionPrinter = func(cCtx *cli.Context) {
		hash := commit
		if hash == "" {
			hash = func() string {
				if info, ok := debug.ReadBuildInfo(); ok {
					for _, setting := range info.Settings {
						if setting.Key == "vcs.revision" {
							return setting.Value
						}
					}
				}
				return ""
			}()
		}

		fmt.Printf("bruin CLI %s (%s)\n", cCtx.App.Version, hash)
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
			&cli.Command{
				Name: "version",
				Action: func(c *cli.Context) error {
					fmt.Printf("Current version: %s (%s)\n", c.App.Version, commit)
					res, err := http.Get("https://github.com/bruin-data/bruin/releases/latest")
					if err != nil {
						fmt.Println("Failed to check the latest version: " + err.Error())
						return nil
					}

					fmt.Println("Latest version: " + strings.TrimPrefix(res.Request.URL.String(), "https://github.com/bruin-data/bruin/releases/tag/"))
					return nil
				},
			},
		},
	}

	_ = app.Run(os.Args)
}
