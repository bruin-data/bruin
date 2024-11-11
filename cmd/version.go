package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

func VersionCmd(commit string) *cli.Command {
	return &cli.Command{
		Name: "version",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
		},
		Action: func(c *cli.Context) error {
			outputFormat := c.String("output")
			version := c.App.Version
			res, err := http.Get("https://github.com/bruin-data/bruin/releases/latest") //nolint
			if err != nil {
				return errors.Wrap(err, "failed to check the latest version")
			}
			defer res.Body.Close()
			latest := strings.TrimPrefix(res.Request.URL.String(), "https://github.com/bruin-data/bruin/releases/tag/")

			if outputFormat == "json" {
				output := struct {
					Version string `json:"version"`
					Commit  string `json:"commit"`
					Latest  string `json:"latest"`
				}{
					Version: version,
					Commit:  commit,
					Latest:  latest,
				}

				outputString, err := json.Marshal(output)
				if err != nil {
					return errors.Wrap(err, "failed to marshal the output")
				}
				fmt.Println(string(outputString))

				return nil
			}

			fmt.Printf("Current: %s (%s)\n", c.App.Version, commit)
			fmt.Println("Latest: " + latest)
			return nil
		},
	}
}
