package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

type VersionInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Latest  string `json:"latest"`
}

func VersionCmd(commit string) *cli.Command {
	return &cli.Command{
		Name: "version",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Usage: "timeout for fetching version info",
				Value: 5 * time.Second,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			timeout := c.Duration("timeout")
			debug := c.Bool("debug")
			latest := fetchLatestVersion(timeout, makeLogger(debug))
			version := c.Root().Version
			outputFormat := c.String("output")

			if outputFormat == "json" {
				outputString, err := json.Marshal(
					VersionInfo{version, commit, latest},
				)
				if err != nil {
					return errors.Wrap(err, "failed to marshal the output")
				}
				fmt.Println(string(outputString))

				return nil
			}

			fmt.Printf("Current: %s (%s)\n", c.Root().Version, commit)
			fmt.Println("Latest: " + latest)
			return nil
		},
	}
}

func fetchLatestVersion(timeout time.Duration, logger logger.Logger) string {
	httpClient := &http.Client{
		Timeout: timeout,
	}
	res, err := httpClient.Get("https://github.com/bruin-data/bruin/releases/latest") //nolint
	if err != nil {
		logger.Debugf("error fetching version information: %v", err)
		return "<unknown: error fetching version information>"
	}
	res.Body.Close()
	return strings.TrimPrefix(res.Request.URL.String(), "https://github.com/bruin-data/bruin/releases/tag/")
}
