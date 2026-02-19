package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bruin-data/bruin/pkg/rustparser"
	"github.com/urfave/cli/v3"
)

func ParseSQLRust() *cli.Command {
	return &cli.Command{
		Name:  "parse-sql-rust",
		Usage: "parse SQL using the embedded Rust polyglot-sql parser",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "query",
				Aliases:  []string{"q"},
				Usage:    "the SQL query to parse",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "dialect",
				Aliases:     []string{"d"},
				Usage:       "the SQL dialect (bigquery, snowflake, postgres, mysql, etc.)",
				DefaultText: "bigquery",
				Value:       "bigquery",
			},
			&cli.StringFlag{
				Name:        "operation",
				Aliases:     []string{"op"},
				Usage:       "the operation to perform: tables, lineage, limit, single-select, rename",
				DefaultText: "tables",
				Value:       "tables",
			},
			&cli.IntFlag{
				Name:  "limit",
				Usage: "limit value for add-limit operation",
				Value: 100,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			query := c.String("query")
			dialect := c.String("dialect")
			operation := c.String("operation")

			parser := rustparser.NewRustSQLParserWithConfig(100000)

			var result interface{}
			var err error

			switch operation {
			case "tables":
				tables, e := parser.UsedTables(query, dialect)
				if e != nil {
					printErrorJSON(e)
					return cli.Exit("", 1)
				}
				result = map[string]interface{}{
					"tables":  tables,
					"dialect": dialect,
					"parser":  "rust-polyglot",
				}

			case "lineage":
				lineage, e := parser.ColumnLineage(query, dialect, rustparser.Schema{})
				if e != nil {
					printErrorJSON(e)
					return cli.Exit("", 1)
				}
				result = map[string]interface{}{
					"lineage": lineage,
					"dialect": dialect,
					"parser":  "rust-polyglot",
				}

			case "limit":
				limited, e := parser.AddLimit(query, int(c.Int("limit")), dialect)
				if e != nil {
					printErrorJSON(e)
					return cli.Exit("", 1)
				}
				result = map[string]interface{}{
					"query":   limited,
					"dialect": dialect,
					"parser":  "rust-polyglot",
				}

			case "single-select":
				isSingle, e := parser.IsSingleSelectQuery(query, dialect)
				if e != nil {
					printErrorJSON(e)
					return cli.Exit("", 1)
				}
				result = map[string]interface{}{
					"is_single_select": isSingle,
					"dialect":          dialect,
					"parser":           "rust-polyglot",
				}

			case "rename":
				// For the rename demo, we don't have a mapping from CLI flags,
				// so we just output the parsed tables for now
				tables, e := parser.UsedTables(query, dialect)
				if e != nil {
					printErrorJSON(e)
					return cli.Exit("", 1)
				}
				result = map[string]interface{}{
					"tables":  tables,
					"dialect": dialect,
					"parser":  "rust-polyglot",
					"note":    "rename operation requires a table mapping, use the Go API directly",
				}

			default:
				err = fmt.Errorf("unknown operation: %s, supported: tables, lineage, limit, single-select, rename", operation)
			}

			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			js, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}
			fmt.Println(string(js))

			return nil
		},
	}
}
