package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bruin-data/bruin/pkg/docsgen"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

func Docs() *cli.Command {
	return &cli.Command{
		Name:      "docs",
		Usage:     "Generate a self-contained documentation website for Bruin pipelines",
		ArgsUsage: "[path to a repo, pipeline, or asset]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Value:   "bruin-docs.html",
				Usage:   "Path to write the generated documentation HTML file",
			},
			&cli.StringFlag{
				Name:  "title",
				Value: "Bruin Docs",
				Usage: "Title to display in the generated documentation website",
			},
			&cli.StringFlag{
				Name:  "variant",
				Usage: "Only materialize the given variant for variant pipelines",
			},
			&cli.BoolFlag{
				Name:  "exclude-code",
				Usage: "Omit asset source code from the generated documentation",
			},
			&cli.BoolFlag{
				Name:  "open",
				Usage: "Open the generated documentation file in your default web browser",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			inputPath := c.Args().Get(0)
			result, err := docsgen.Generate(ctx, DefaultPipelineBuilder, docsgen.Options{
				InputPath:               inputPath,
				OutputPath:              c.String("output"),
				Title:                   c.String("title"),
				Variant:                 c.String("variant"),
				ExcludeCode:             c.Bool("exclude-code"),
				PipelineDefinitionFiles: PipelineDefinitionFiles,
			})
			if err != nil {
				return err
			}

			fmt.Printf("Generated documentation for %d pipelines and %d assets: %s\n", result.PipelineCount, result.AssetCount, result.OutputPath)
			if c.Bool("open") {
				if err := openBrowser(ctx, fileURL(result.OutputPath)); err != nil {
					return errors.Wrap(err, "failed to open the generated documentation")
				}
				fmt.Println("Opened generated documentation in your default browser.")
			}

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

func fileURL(path string) string {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		absolutePath = path
	}

	slashed := filepath.ToSlash(absolutePath)
	// Windows drive paths (e.g. "C:/x") need a leading slash so the result is
	// "file:///C:/x" rather than treating the drive letter as a host.
	if !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}

	return (&url.URL{Scheme: "file", Path: slashed}).String()
}

func openBrowser(ctx context.Context, url string) error {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.CommandContext(ctx, "xdg-open", url).Start()
	case osWindows:
		err = exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.CommandContext(ctx, "open", url).Start()
	default:
		return errors.New("unsupported platform")
	}
	return err
}
