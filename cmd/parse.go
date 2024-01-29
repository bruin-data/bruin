package cmd

import (
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/urfave/cli/v2"
)

func Parse() *cli.Command {
	return &cli.Command{
		Name:      "parse",
		Usage:     "parse a single Bruin SQL asset",
		ArgsUsage: "[path to the asset definition]",
		Action: func(c *cli.Context) error {
			sqlparser.ParseUsedTables()
			return nil
		},
	}
}
