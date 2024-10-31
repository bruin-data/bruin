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
	"github.com/pkg/errors"
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
			connectionName := c.String("connection")
			queryStr := c.String("query")
			output := c.String("output")

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

			// Check if the connection supports querying with schema
			if querier, ok := conn.(interface {
				SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
			}); ok {
				// Prepare context and query
				ctx := context.Background()
				q := query.Query{Query: queryStr}

				// Call SelectWithSchema and retrieve the result
				result, err := querier.SelectWithSchema(ctx, &q)
				if err != nil {
					return handleError(output, errors.Wrap(err, "query execution failed"))
				}

				// Output result based on format specified
				if output == "json" {
					type jsonResponse struct {
						Columns []map[string]string `json:"columns"`
						Rows    [][]interface{}     `json:"rows"`
					}

					// Construct JSON response with structured columns
					jsonCols := make([]map[string]string, len(result.Columns))
					for i, colName := range result.Columns {
						jsonCols[i] = map[string]string{"name": colName}
					}

					// Prepare the final output struct
					finalOutput := jsonResponse{
						Columns: jsonCols,
						Rows:    result.Rows,
					}

					jsonData, err := json.Marshal(finalOutput)
					if err != nil {
						return handleError(output, errors.Wrap(err, "failed to marshal result to JSON"))
					}
					fmt.Println(string(jsonData))
				} else {
					printTable(result.Columns, result.Rows)
				}
			} else {
				fmt.Printf("Connection type %s does not support querying.\n", connectionName)
			}
			return nil
		},
	}
}

func printTable(columnNames []string, rows [][]interface{}) {
	if len(rows) == 0 {
		fmt.Println("No data available")
		return
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	headers := make(table.Row, len(columnNames))
	for i, colName := range columnNames {
		headers[i] = colName
	}
	t.AppendHeader(headers)

	for _, row := range rows {
		rowData := make(table.Row, len(row))
		for i, cell := range row {
			rowData[i] = fmt.Sprintf("%v", cell)
		}
		t.AppendRow(rowData)
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}

func handleError(output string, err error) error {
	if output == "json" {
		jsonError, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Println(string(jsonError))
	} else {
		fmt.Println("Error:", err.Error())
	}
	return cli.Exit("", 1)
}
