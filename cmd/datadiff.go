package cmd

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/sourcegraph/conc"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

// TableSummarizer defines an interface for connections that can provide table summaries.
type TableSummarizer interface {
	GetTableSummary(ctx context.Context, tableName string) (*ansisql.TableSummaryResult, error)
}

// DataDiffCmd defines the 'data-diff' command.
func DataDiffCmd() *cli.Command {
	var connectionName string
	// configFilePath is added to allow overriding the default .bruin.yml path, similar to other commands
	var configFilePath string

	return &cli.Command{
		Name:    "data-diff",
		Aliases: []string{"diff"},
		Usage:   "Compares data between two environments or sources. Requires two arguments: <table1> <table2>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "connection",
				Aliases:     []string{"c"},
				Usage:       "Name of the connection to use",
				Destination: &connectionName,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "config-file",
				EnvVars:     []string{"BRUIN_CONFIG_FILE"},
				Usage:       "the path to the .bruin.yml file",
				Destination: &configFilePath,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return errors.New("incorrect number of arguments, please provide two table names")
			}
			table1 := c.Args().Get(0)
			table2 := c.Args().Get(1)

			fs := afero.NewOsFs()

			// Get repository root
			repoRoot, err := git.FindRepoFromPath(".")
			if err != nil {
				return fmt.Errorf("failed to find the git repository root: %w", err)
			}

			// Determine config file path
			if configFilePath == "" {
				configFilePath = filepath.Join(repoRoot.Path, ".bruin.yml")
			}

			// Load config
			cm, err := config.LoadOrCreate(fs, configFilePath)
			if err != nil {
				return fmt.Errorf("failed to load or create config from '%s': %w", configFilePath, err)
			}

			// Create connection manager
			manager, errs := connection.NewManagerFromConfig(cm)
			if len(errs) > 0 {
				// Handle multiple errors, e.g. by joining them or returning the first one
				return fmt.Errorf("failed to create connection manager: %w", errs[0])
			}

			// Get the connection
			conn, err := manager.GetConnection(connectionName)
			if err != nil {
				return fmt.Errorf("failed to get connection '%s': %w", connectionName, err)
			}

			fmt.Printf("Successfully fetched connection: %s (Type: %T)\n", connectionName, conn)
			fmt.Printf("Table 1: %s\n", table1)
			fmt.Printf("Table 2: %s\n", table2)

			ctx := context.Background()

			if summarizer, ok := conn.(TableSummarizer); ok {
				var summary1, summary2 *ansisql.TableSummaryResult
				var err1, err2 error
				var group conc.WaitGroup

				group.Go(func() {
					summary1, err1 = summarizer.GetTableSummary(ctx, table1)
				})

				group.Go(func() {
					summary2, err2 = summarizer.GetTableSummary(ctx, table2)
				})

				group.Wait()

				if err1 != nil {
					fmt.Printf("  Error getting summary for %s: %v\n", table1, err1)
				}
				if err2 != nil {
					fmt.Printf("  Error getting summary for %s: %v\n", table2, err2)
				}

				if summary1 != nil && summary2 != nil {
					fmt.Printf("\nComparing table summaries:\n")
					fmt.Printf("Table 1 (%s): %s\n", table1, summary1)
					fmt.Printf("Table 2 (%s): %s\n", table2, summary2)

					rowCountDiff := summary1.RowCount - summary2.RowCount
					if rowCountDiff != 0 {
						errorPrinter.Printf("\nRow count difference: %d rows\n", rowCountDiff)
						if rowCountDiff > 0 {
							errorPrinter.Printf("Table 1 has %d more rows than Table 2\n", rowCountDiff)
						} else {
							errorPrinter.Printf("Table 2 has %d more rows than Table 1\n", -rowCountDiff)
						}
						return fmt.Errorf("Tables '%s' and '%s' have different row counts", table1, table2)
					} else {
						fmt.Printf("\nTables have the same number of rows\n")
					}
				} else {
					errorPrinter.Printf("\nUnable to compare summaries - one or both summaries could not be fetched or are nil\n")
					return errors.New("failed to compare table summaries due to missing data")
				}

			} else {
				fmt.Printf("\nConnection type %T does not support GetTableSummary.\n", conn)
			}

			return nil
		},
	}
}
