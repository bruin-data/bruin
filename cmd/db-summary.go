package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func DBSummary() *cli.Command {
	return &cli.Command{
		Name:   "db-summary",
		Usage:  "Get a summary of database schemas and tables for a specified connection",
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Value:       "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml. Specifies the configuration environment for the database summary.",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			fs := afero.NewOsFs()
			connectionName := c.String("connection")
			environment := c.String("environment")
			output := c.String("output")

			// Get connection from config
			conn, err := getConnectionFromConfig(environment, connectionName, fs, c.String("config-file"))
			if err != nil {
				return handleError(output, errors.Wrap(err, "failed to get database connection"))
			}

			// Check if connection supports GetDatabaseSummary
			summarizer, ok := conn.(interface {
				GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
			})
			if !ok {
				return handleError(output, fmt.Errorf("connection type '%s' does not support database summary", connectionName))
			}

			// Get database summary
			ctx := context.Background()
			summary, err := summarizer.GetDatabaseSummary(ctx)
			if err != nil {
				return handleError(output, errors.Wrap(err, "failed to retrieve database summary"))
			}

			// Output result based on format specified
			switch output {
			case "plain":
				printDatabaseSummary(summary)
			case "json":
				type jsonResponse struct {
					DatabaseName string              `json:"database_name"`
					Schemas      []*ansisql.DBSchema `json:"schemas"`
					ConnName     string              `json:"connection_name"`
					Summary      *SummaryStats       `json:"summary"`
				}

				stats := calculateSummaryStats(summary)
				finalOutput := jsonResponse{
					DatabaseName: summary.Name,
					Schemas:      summary.Schemas,
					ConnName:     connectionName,
					Summary:      stats,
				}

				jsonData, err := json.Marshal(finalOutput)
				if err != nil {
					return handleError(output, errors.Wrap(err, "failed to marshal result to JSON"))
				}
				fmt.Println(string(jsonData))
			default:
				return handleError(output, fmt.Errorf("invalid output type: %s", output))
			}

			return nil
		},
	}
}

type SummaryStats struct {
	TotalSchemas int `json:"total_schemas"`
	TotalTables  int `json:"total_tables"`
}

func calculateSummaryStats(summary *ansisql.DBDatabase) *SummaryStats {
	totalTables := 0
	for _, schema := range summary.Schemas {
		totalTables += len(schema.Tables)
	}

	return &SummaryStats{
		TotalSchemas: len(summary.Schemas),
		TotalTables:  totalTables,
	}
}

func printDatabaseSummary(summary *ansisql.DBDatabase) {
	if len(summary.Schemas) == 0 {
		fmt.Printf("Database '%s' contains no schemas or tables\n", summary.Name)
		return
	}

	fmt.Printf("Database Summary for: %s\n", summary.Name)
	fmt.Println(strings.Repeat("=", 50))

	stats := calculateSummaryStats(summary)
	fmt.Printf("Total Schemas: %d\n", stats.TotalSchemas)
	fmt.Printf("Total Tables: %d\n\n", stats.TotalTables)

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Schema", "Table", "Type"})

	for _, schema := range summary.Schemas {
		if len(schema.Tables) == 0 {
			t.AppendRow(table.Row{schema.Name, "(no tables)", ""})
			continue
		}

		for i, tbl := range schema.Tables {
			if i == 0 {
				t.AppendRow(table.Row{schema.Name, tbl.Name, "table"})
			} else {
				t.AppendRow(table.Row{"", tbl.Name, "table"})
			}
		}
		t.AppendSeparator()
	}

	t.SetStyle(table.StyleLight)
	t.Render()
}
