package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/spf13/afero" // Importing afero for filesystem operations
	"github.com/urfave/cli/v2"
	"path/filepath"
)

// FetchCommand defines the 'fetch' CLI command with subcommands
func Fetch() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "Manage data fetching operations",
		Subcommands: []*cli.Command{
			queryCommand(),
		},
	}
}

// querySubcommand defines the 'fetch query' subcommand for executing SQL queries
func queryCommand() *cli.Command {
	return &cli.Command{
		Name:  "query",
		Usage: "Execute a query on a specified connection and retrieve results",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"n"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "query",
				Usage:    "the SQL query to execute",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
		},
		Action: func(c *cli.Context) error {
			// Step 1: Extract arguments
			connectionName := c.String("connection")
			queryStr := c.String("query")
			output := c.String("output")

			// Step 2: Find the repo root and load configuration
			repoRoot, err := git.FindRepoFromPath(".")
			if err != nil {
				return handleError(output, errors.Wrap(err, "failed to find the git repository root"))
			}

			configFilePath := filepath.Join(repoRoot.Path, ".bruin.yml")
			fs := afero.NewOsFs()
			cm, err := config.LoadOrCreate(fs, configFilePath)
			if err != nil {
				return handleError(output, errors.Wrap(err, "failed to load or create config"))
			}

			// Step 3: Retrieve the connection using the connection manager
			manager, errs := connection.NewManagerFromConfig(cm)
			if len(errs) > 0 {
				for _, err := range errs {
					handleError(output, errors.Wrap(err, "failed to create connection manager"))
				}
				return cli.Exit("", 1)
			}

			conn, err := manager.GetConnection(connectionName)
			if err != nil {
				return handleError(output, errors.Wrap(err, "failed to get connection"))
			}

			// Step 4: Check if the connection supports querying using inline interface
			if querier, ok := conn.(interface {
				Select(ctx context.Context, q *query.Query) ([][]interface{}, error)
			}); ok {
				// Prepare the query and execute it
				ctx := context.Background()
				q := query.Query{Query: queryStr}

				result, err := querier.Select(ctx, &q)
				if err != nil {
					return handleError(output, errors.Wrap(err, "query execution failed"))
				}

				// Step 5: Format and display the result based on output type
				if output == "json" {
					jsonData, err := json.Marshal(result)
					if err != nil {
						return handleError(output, errors.Wrap(err, "failed to marshal result to JSON"))
					}
					fmt.Println(string(jsonData))
				} else {
					printTable(result)
				}
			} else {
				fmt.Printf("Connection type %s does not support querying.\n", connectionName)
			}

			return nil
		},
	}
}

// Helper for printing errors based on output format
func handleError(output string, err error) error {
	if output == "json" {
		jsonError, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Println(string(jsonError))
	} else {
		fmt.Println("Error:", err.Error())
	}
	return cli.Exit("", 1)
}

// Helper for printing in table format
func printTable(result [][]interface{}) {
	for _, row := range result {
		for _, col := range row {
			fmt.Printf("%v\t", col)
		}
		fmt.Println()
	}
}
