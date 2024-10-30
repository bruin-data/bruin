package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
	"os"
	"path/filepath"
)

func Fetch() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "Manage data fetching operations",
		Subcommands: []*cli.Command{
			queryCommand(),
		},
	}
}

func queryCommand() *cli.Command {
	return &cli.Command{
		Name:  "query",
		Usage: "Execute a query on a specified connection and retrieve results",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"n"},
				Usage:    "The name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "query",
				Usage:    "The SQL query to execute",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Usage:       "The output type; possible values are: plain, json",
			},
		},
		Action: func(c *cli.Context) error {
			connectionName := c.String("connection")
			queryStr := c.String("query")
			output := c.String("output")

			// Step 1: Locate the repo root
			repoRoot, err := git.FindRepoFromPath(".")
			if err != nil {
				return handleError(output, fmt.Errorf("failed to find the git repository root: %w", err))
			}

			// Step 2: Load or create config
			configFilePath := filepath.Join(repoRoot.Path, ".bruin.yml")
			fs := afero.NewOsFs()
			cm, err := config.LoadOrCreate(fs, configFilePath)
			if err != nil {
				return handleError(output, fmt.Errorf("failed to load or create config: %w", err))
			}

			// Step 3: Initialize connection manager
			manager, errs := connection.NewManagerFromConfig(cm)
			if len(errs) > 0 {
				for _, err := range errs {
					handleError(output, fmt.Errorf("failed to create connection manager: %w", err))
				}
				return cli.Exit("", 1)
			}

			// Step 4: Retrieve the specified connection
			conn, err := manager.GetConnection(connectionName)
			if err != nil {
				return handleError(output, fmt.Errorf("failed to get connection: %w", err))
			}

			// Step 5: Ensure the connection supports querying with schema
			if querier, ok := conn.(interface {
				SelectWithSchema(ctx context.Context, q *query.Query) ([]string, [][]interface{}, error)
			}); ok {
				ctx := context.Background()
				q := query.Query{Query: queryStr}

				// Step 6: Execute the query and retrieve columns and data
				columnNames, result, err := querier.SelectWithSchema(ctx, &q)
				if err != nil {
					return handleError(output, fmt.Errorf("query execution failed: %w", err))
				}

				// Step 7: Output the result in the requested format
				if output == "json" {
					jsonData, err := json.Marshal(map[string]interface{}{
						"columns": columnNames,
						"rows":    result,
					})
					if err != nil {
						return handleError(output, fmt.Errorf("failed to marshal result to JSON: %w", err))
					}
					fmt.Println(string(jsonData))
				} else {
					printTable(columnNames, result)
				}
			} else {
				fmt.Printf("Connection type %s does not support querying.\n", connectionName)
			}

			return nil
		},
	}
}

// handleError outputs error messages in plain or JSON format based on the output type.
func handleError(output string, err error) error {
	if output == "json" {
		jsonError, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Println(string(jsonError))
	} else {
		fmt.Println("Error:", err.Error())
	}
	return cli.Exit("", 1)
}

// printTable displays the results in a tabular format with headers.
func printTable(columnNames []string, result [][]interface{}) {
	if len(result) == 0 {
		fmt.Println("No data available")
		return
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Set column headers using the provided column names
	headers := make(table.Row, len(columnNames))
	for i, colName := range columnNames {
		headers[i] = colName
	}
	t.AppendHeader(headers)

	// Append rows to the table
	for _, row := range result {
		rowData := make(table.Row, len(row))
		for i, cell := range row {
			rowData[i] = fmt.Sprintf("%v", cell)
		}
		t.AppendRow(rowData)
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}
