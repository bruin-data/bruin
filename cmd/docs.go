package cmd

import (
	"fmt"
	"os/exec"
	"runtime"

	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

func Docs() *cli.Command {
	return &cli.Command{
		Name:  "docs",
		Usage: "Display the link to the Bruin documentation or open it in your browser",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "open",
				Usage: "Open the documentation in your default web browser",
			},
		},
		Action: func(c *cli.Context) error {
			const docsURL = "https://bruin-data.github.io/bruin/"
			openFlag := c.Bool("open")

			if openFlag {
				err := openBrowser(docsURL)
				if err != nil {
					return errors.Wrap(err, "failed to open the browser")
				}
				fmt.Println("Opened documentation in your default browser.")
			} else {
				fmt.Printf("Documentation: %s\n", docsURL)
			}
			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

func openBrowser(url string) error {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		return errors.New("unsupported platform")
	}
	return err
}
