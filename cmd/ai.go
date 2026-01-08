package cmd

import (
	"github.com/urfave/cli/v3"
)

// AI returns the parent command for AI-related subcommands.
func AI(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "ai",
		Usage: "AI-powered commands for enhancing and analyzing assets",
		Commands: []*cli.Command{
			enhanceCommand(isDebug),
		},
	}
}
