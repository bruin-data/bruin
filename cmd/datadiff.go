package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/sourcegraph/conc"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

// TableSummarizer defines an interface for connections that can provide table summaries.
type TableSummarizer interface {
	GetTableSummary(ctx context.Context, tableName string) (*diff.TableSummaryResult, error)
}

// TypeDifference represents a difference in column types between two tables.
type TypeDifference struct {
	Table1Type string
	Table2Type string
}

// NullabilityDifference represents a difference in nullability between two tables.
type NullabilityDifference struct {
	Table1Nullable bool
	Table2Nullable bool
}

// UniquenessDifference represents a difference in uniqueness constraints between two tables.
type UniquenessDifference struct {
	Table1Unique bool
	Table2Unique bool
}

// ColumnDifference represents differences for a column that exists in both tables.
type ColumnDifference struct {
	ColumnName            string
	TypeDifference        *TypeDifference
	NullabilityDifference *NullabilityDifference
	UniquenessDifference  *UniquenessDifference
}

// MissingColumn represents a column that exists in only one table.
type MissingColumn struct {
	ColumnName  string
	Type        string
	Nullable    bool
	PrimaryKey  bool
	Unique      bool
	TableName   string // The table where this column exists
	MissingFrom string // The table where this column is missing
}

// SchemaComparisonResult holds the detailed results of a table schema comparison.
type SchemaComparisonResult struct {
	Table1 *diff.TableSummaryResult
	Table2 *diff.TableSummaryResult

	SamePropertiesCount      int                // Columns with identical properties in both tables
	DifferentPropertiesCount int                // Columns present in both tables but with different properties
	InTable1OnlyCount        int                // Columns present in table1 but not in table2
	InTable2OnlyCount        int                // Columns present in table2 but not in table1
	ColumnDifferences        []ColumnDifference // Structured differences for columns present in both tables
	MissingColumns           []MissingColumn    // Columns missing from one of the tables
	HasSchemaDifferences     bool               // True if DifferentPropertiesCount, InTable1OnlyCount, or InTable2OnlyCount > 0
	RowCountDiff             int64              // Difference in row count between the two tables
	HasRowCountDifference    bool               // True if RowCountDiff != 0
}

func (c *SchemaComparisonResult) GetSummaryTable() string {
	t := table.NewWriter()
	t.SetRowPainter(func(row table.Row) text.Colors {
		if len(row) == 0 {
			return text.Colors{}
		}
		if row[0] == "Row Count" {
			if c.RowCountDiff != 0 {
				return text.Colors{text.FgRed}
			} else {
				return text.Colors{text.FgGreen}
			}
		}
		if row[0] == "Columns" {
			if row[3] != 0 {
				return text.Colors{text.FgRed}
			} else {
				return text.Colors{text.FgGreen}
			}
		}
		return text.Colors{}
	})

	t.AppendHeader(table.Row{"", c.Table1.Table.Name, c.Table2.Table.Name, "Diff"})
	if c.RowCountDiff != 0 {
		t.AppendRow(table.Row{"Row Count", c.Table1.RowCount, c.Table2.RowCount, c.RowCountDiff})
	} else {
		t.AppendRow(table.Row{"Row Count", c.Table1.RowCount, c.Table2.RowCount, 0})
	}
	t.AppendRow(table.Row{"Columns", len(c.Table1.Table.Columns), len(c.Table2.Table.Columns), len(c.Table1.Table.Columns) - len(c.Table2.Table.Columns)})
	return t.Render()
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

			ctx := c.Context
			if summarizer, ok := conn.(TableSummarizer); ok {
				var summary1, summary2 *diff.TableSummaryResult
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
					errorPrinter.Printf("error getting summary for '%s':\n\n%v", table1, err1)
					return cli.Exit("", 1)
				}
				if err2 != nil {
					errorPrinter.Printf("error getting summary for '%s':\n\n%v", table2, err2)
					return cli.Exit("", 1)
				}

				if summary1 != nil && summary2 != nil {
					schemaComparison := compareTableSchemas(summary1, summary2, table1, table2)
					printSchemaComparisonOutput(schemaComparison, table1, table2, summary1.Table, summary2.Table, c.App.Writer)
				} else {
					fmt.Fprintf(c.App.ErrWriter, "\nUnable to compare summaries - one or both summaries could not be fetched or are nil\n")
					return errors.New("failed to compare table summaries due to missing data")
				}

			} else {
				fmt.Printf("\nConnection type %T does not support GetTableSummary.\n", conn)
			}

			return nil
		},
	}
}

func printSchemaComparisonOutput(schemaComparison SchemaComparisonResult, table1Name, table2Name string, t1Schema, t2Schema *diff.Table, errOut io.Writer) {
	fmt.Fprintf(errOut, schemaComparison.GetSummaryTable()+"\n")
	redPrinter := color.New(color.FgRed)
	greenPrinter := color.New(color.FgGreen)

	var overallSchemaMessage string

	if schemaComparison.HasSchemaDifferences {
		summaryPoints := []string{}
		if schemaComparison.SamePropertiesCount > 0 {
			summaryPoints = append(summaryPoints, fmt.Sprintf("%d columns have matching schemas", schemaComparison.SamePropertiesCount))
		}
		if schemaComparison.DifferentPropertiesCount > 0 {
			summaryPoints = append(summaryPoints, fmt.Sprintf("%d columns have schema differences", schemaComparison.DifferentPropertiesCount))
		}
		if schemaComparison.InTable1OnlyCount > 0 {
			summaryPoints = append(summaryPoints, fmt.Sprintf("%d columns from '%s' are missing in '%s'", schemaComparison.InTable1OnlyCount, table1Name, table2Name))
		}
		if schemaComparison.InTable2OnlyCount > 0 {
			summaryPoints = append(summaryPoints, fmt.Sprintf("%d columns from '%s' are extra (not in '%s')", schemaComparison.InTable2OnlyCount, table2Name, table1Name))
		}

		if len(summaryPoints) > 0 {
			overallSchemaMessage = fmt.Sprintf("\n\nSchema Comparison:\n")
			for _, point := range summaryPoints {
				overallSchemaMessage += fmt.Sprintf("  - %s\n", point)
			}
			fmt.Fprintf(errOut, "\n%s\n", overallSchemaMessage)
		} else {
			// This case should ideally be covered if HasSchemaDifferences is true due to nil/empty table scenarios handled in compareTableSchemas
			overallSchemaMessage = "Schema differences detected."
			fmt.Fprintf(errOut, "\n%s\n", overallSchemaMessage)
		}

		if len(schemaComparison.ColumnDifferences) > 0 {
			fmt.Fprintf(errOut, "  Column Differences:\n")
			for _, diff := range schemaComparison.ColumnDifferences {
				fmt.Fprintf(errOut, "    - Column '%s':\n", diff.ColumnName)
				if diff.TypeDifference != nil {
					fmt.Fprintf(errOut, "      - Type: '%s' in %s vs '%s' in %s\n", diff.TypeDifference.Table1Type, table1Name, diff.TypeDifference.Table2Type, table2Name)
				}
				if diff.NullabilityDifference != nil {
					fmt.Fprintf(errOut, "      - Nullability: %t in %s vs %t in %s\n", diff.NullabilityDifference.Table1Nullable, table1Name, diff.NullabilityDifference.Table2Nullable, table2Name)
				}
				if diff.UniquenessDifference != nil {
					fmt.Fprintf(errOut, "      - Uniqueness: %t in %s vs %t in %s\n", diff.UniquenessDifference.Table1Unique, table1Name, diff.UniquenessDifference.Table2Unique, table2Name)
				}
			}
		}

		if len(schemaComparison.MissingColumns) > 0 {
			fmt.Fprintf(errOut, "Missing Columns:\n")
			for _, missing := range schemaComparison.MissingColumns {
				fmt.Fprintf(errOut, "  - %s\n", missing.ColumnName)
				fmt.Fprintf(errOut, "    - exists in %s\n", missing.TableName)
				fmt.Fprintln(errOut, redPrinter.Sprintf("    - missing from '%s'", missing.MissingFrom))
			}
		}
	} else { // No schema differences
		if t1Schema != nil && t2Schema != nil {
			// Both tables are non-nil and have no schema differences
			if schemaComparison.SamePropertiesCount == 0 && len(t1Schema.Columns) == 0 && len(t2Schema.Columns) == 0 {
				overallSchemaMessage = greenPrinter.Sprintf("Tables '%s' and '%s' both have no columns and are considered schema-identical.", table1Name, table2Name)
			} else {
				overallSchemaMessage = greenPrinter.Sprintf("Tables '%s' and '%s' have the same schema with %d columns.", table1Name, table2Name, schemaComparison.SamePropertiesCount)
			}
		} else if t1Schema == nil && t2Schema == nil {
			overallSchemaMessage = fmt.Sprintf("Schemas for '%s' and '%s' are both nil (e.g., tables might not exist or failed to fetch).", table1Name, table2Name)
		} else {
			if t1Schema == nil && t2Schema != nil && len(t2Schema.Columns) == 0 {
				overallSchemaMessage = fmt.Sprintf("Schema for '%s' is nil, and '%s' has no columns. Schemas considered identical.", table1Name, table2Name)
			} else if t2Schema == nil && t1Schema != nil && len(t1Schema.Columns) == 0 {
				overallSchemaMessage = fmt.Sprintf("Schema for '%s' is nil, and '%s' has no columns. Schemas considered identical.", table2Name, table1Name)
			} else {
				// Fallback, should be covered by previous conditions
				overallSchemaMessage = "Table schemas are considered identical."
			}
		}
		fmt.Printf("\n%s\n", overallSchemaMessage) // Standard output for no differences
	}
}

func compareTableSchemas(summary1, summary2 *diff.TableSummaryResult, t1Name, t2Name string) SchemaComparisonResult {
	res := SchemaComparisonResult{
		Table1: summary1,
		Table2: summary2,
	}

	if summary1 == nil || summary2 == nil {
		return res
	}

	if summary1.Table == nil || summary2.Table == nil {
		return res
	}

	res.RowCountDiff = summary1.RowCount - summary2.RowCount
	res.HasRowCountDifference = res.RowCountDiff != 0

	t1 := summary1.Table
	t2 := summary2.Table

	table1Columns := make(map[string]*diff.Column)
	for _, col := range t1.Columns {
		table1Columns[col.Name] = col
	}

	table2Columns := make(map[string]*diff.Column)
	for _, col := range t2.Columns {
		table2Columns[col.Name] = col
	}

	processedColsT2 := make(map[string]bool) // Tracks columns from t2 that are matched

	// Iterate through columns of table 1
	for _, col1 := range t1.Columns {
		col2, existsInT2 := table2Columns[col1.Name]
		if existsInT2 {
			processedColsT2[col1.Name] = true
			columnIsDifferent := false
			colDiff := ColumnDifference{
				ColumnName: col1.Name,
			}

			if col1.Type != col2.Type {
				colDiff.TypeDifference = &TypeDifference{
					Table1Type: col1.Type,
					Table2Type: col2.Type,
				}
				columnIsDifferent = true
			}
			if col1.Nullable != col2.Nullable {
				colDiff.NullabilityDifference = &NullabilityDifference{
					Table1Nullable: col1.Nullable,
					Table2Nullable: col2.Nullable,
				}
				columnIsDifferent = true
			}
			if col1.Unique != col2.Unique {
				colDiff.UniquenessDifference = &UniquenessDifference{
					Table1Unique: col1.Unique,
					Table2Unique: col2.Unique,
				}
				columnIsDifferent = true
			}

			if columnIsDifferent {
				res.DifferentPropertiesCount++
				res.ColumnDifferences = append(res.ColumnDifferences, colDiff)
			} else {
				res.SamePropertiesCount++
			}
		} else {
			// Column from t1 does not exist in t2
			res.InTable1OnlyCount++
			res.MissingColumns = append(res.MissingColumns, MissingColumn{
				ColumnName:  col1.Name,
				Type:        col1.Type,
				Nullable:    col1.Nullable,
				PrimaryKey:  col1.PrimaryKey,
				Unique:      col1.Unique,
				TableName:   t1Name,
				MissingFrom: t2Name,
			})
		}
	}

	// Identify columns that are only in table 2 (extra in t2)
	for _, col2 := range t2.Columns {
		if _, foundAndProcessed := processedColsT2[col2.Name]; !foundAndProcessed {
			res.InTable2OnlyCount++
			res.MissingColumns = append(res.MissingColumns, MissingColumn{
				ColumnName:  col2.Name,
				Type:        col2.Type,
				Nullable:    col2.Nullable,
				PrimaryKey:  col2.PrimaryKey,
				Unique:      col2.Unique,
				TableName:   t2Name,
				MissingFrom: t1Name,
			})
		}
	}

	res.HasSchemaDifferences = res.DifferentPropertiesCount > 0 || res.InTable1OnlyCount > 0 || res.InTable2OnlyCount > 0
	return res
}
