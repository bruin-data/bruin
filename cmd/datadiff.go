package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

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

// TableComparer defines an interface for connections that can compare tables.
type TableComparer interface {
	CompareTables(ctx context.Context, table1, table2 string) (*diff.SchemaComparisonResult, error)
}

func parseTableIdentifier(identifier string, defaultConnection string) (string, string, error) {
	parts := strings.SplitN(identifier, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	if defaultConnection != "" {
		return defaultConnection, identifier, nil
	}
	return "", "", errors.New("connection not specified for table and no default connection is available")
}

func compareTables(ctx context.Context, summarizer1, summarizer2 diff.TableSummarizer, table1, table2 string) (*diff.SchemaComparisonResult, error) {
	var summary1, summary2 *diff.TableSummaryResult
	var err1, err2 error
	var wg conc.WaitGroup

	wg.Go(func() {
		summary1, err1 = summarizer1.GetTableSummary(ctx, table1)
	})

	wg.Go(func() {
		summary2, err2 = summarizer2.GetTableSummary(ctx, table2)
	})

	wg.Wait()

	if err1 != nil {
		return nil, fmt.Errorf("failed to get summary for table '%s': %w", table1, err1)
	}
	if err2 != nil {
		return nil, fmt.Errorf("failed to get summary for table '%s': %w", table2, err2)
	}

	result := diff.CompareTableSchemas(summary1, summary2, table1, table2)
	return &result, nil
}

// DataDiffCmd defines the 'data-diff' command.
func DataDiffCmd() *cli.Command {
	var connectionName string
	// configFilePath is added to allow overriding the default .bruin.yml path, similar to other commands
	var configFilePath string

	return &cli.Command{
		Name:    "data-diff",
		Aliases: []string{"diff"},
		Usage:   "Compares data between two environments or sources. Table names can be provided as 'connection:table' or just 'table' if a default connection is set via --connection flag.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "connection",
				Aliases:     []string{"c"},
				Usage:       "Name of the default connection to use, if not specified in the table argument (e.g. conn:table)",
				Destination: &connectionName,
				Required:    false,
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
			table1Identifier := c.Args().Get(0)
			table2Identifier := c.Args().Get(1)

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

			conn1Name, table1Name, err := parseTableIdentifier(table1Identifier, connectionName)
			if err != nil {
				return fmt.Errorf("invalid identifier for table 1: %w", err)
			}

			conn2Name, table2Name, err := parseTableIdentifier(table2Identifier, connectionName)
			if err != nil {
				return fmt.Errorf("invalid identifier for table 2: %w", err)
			}

			// Get the connection
			conn1, err := manager.GetConnection(conn1Name)
			if err != nil {
				return fmt.Errorf("failed to get connection '%s': %w", conn1Name, err)
			}

			conn2, err := manager.GetConnection(conn2Name)
			if err != nil {
				return fmt.Errorf("failed to get connection '%s': %w", conn2Name, err)
			}

			ctx := c.Context

			s1, ok1 := conn1.(diff.TableSummarizer)
			s2, ok2 := conn2.(diff.TableSummarizer)

			if !ok1 {
				return fmt.Errorf("connection type %T for '%s' does not support table summarization", conn1, conn1Name)
			}

			if !ok2 {
				return fmt.Errorf("connection type %T for '%s' does not support table summarization", conn2, conn2Name)
			}

			schemaComparison, err := compareTables(ctx, s1, s2, table1Name, table2Name)
			if err != nil {
				errorPrinter.Printf("error comparing tables '%s' and '%s':\n\n%v", table1Identifier, table2Identifier, err)
				return cli.Exit("", 1)
			}

			if schemaComparison != nil {
				printSchemaComparisonOutput(*schemaComparison, table1Identifier, table2Identifier, c.App.Writer)
			} else {
				fmt.Fprintf(c.App.ErrWriter, "\nUnable to compare summaries - the comparison result is nil\n")
				return errors.New("failed to compare table summaries due to missing data")
			}

			return nil
		},
	}
}

func printSchemaComparisonOutput(schemaComparison diff.SchemaComparisonResult, table1Name, table2Name string, errOut io.Writer) {
	fmt.Fprintf(errOut, schemaComparison.GetSummaryTable()+"\n")
	redPrinter := color.New(color.FgRed)
	greenPrinter := color.New(color.FgGreen)

	var overallSchemaMessage string
	t1Schema := schemaComparison.Table1.Table
	t2Schema := schemaComparison.Table2.Table

	if !schemaComparison.HasSchemaDifferences && !schemaComparison.HasRowCountDifference {
		if schemaComparison.SamePropertiesCount == 0 && len(t1Schema.Columns) == 0 && len(t2Schema.Columns) == 0 {
			overallSchemaMessage = fmt.Sprintf("Tables '%s' and '%s' both have no columns and are considered schema-identical.", table1Name, table2Name)
		} else {
			overallSchemaMessage = "Table schemas are considered identical."
		}
		greenPrinter.Fprintf(errOut, "\n%s\n", overallSchemaMessage)
		return
	}

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
	}

	t1Columns := make(map[string]*diff.Column)
	for _, column := range t1Schema.Columns {
		t1Columns[column.Name] = column
	}

	t2Columns := make(map[string]*diff.Column)
	for _, column := range t2Schema.Columns {
		t2Columns[column.Name] = column
	}

	if len(t1Schema.Columns) > 0 && len(t2Schema.Columns) > 0 {
		fmt.Fprintf(errOut, "\n\nColumns that exist in both tables:\n")
		for _, t1Column := range t1Schema.Columns {
			if t2Column, ok := t2Columns[t1Column.Name]; ok {
				if t1Column.Stats != nil && t2Column.Stats != nil {
					fmt.Fprintf(errOut, "\n%s\n", t1Column.Name)
					fmt.Fprintf(errOut, "%s\n", tableStatsToTable(t1Column.Stats, t2Column.Stats, table1Name, table2Name))
				}
			}
		}
	}
}

func tableStatsToTable(stats1 diff.ColumnStatistics, stats2 diff.ColumnStatistics, table1Name, table2Name string) string {
	t := table.NewWriter()
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, Align: text.AlignLeft},
		{Number: 2, Align: text.AlignRight},
		{Number: 3, Align: text.AlignRight},
		{Number: 4, Align: text.AlignRight},
	})

	t.SetRowPainter(func(row table.Row) text.Colors {
		if len(row) == 0 {
			return text.Colors{}
		}
		if len(row) != 4 {
			return text.Colors{}
		}

		diffStr := fmt.Sprintf("%v", row[3])
		diffVal, _ := strconv.ParseFloat(diffStr, 64)
		if diffVal != 0 {
			return text.Colors{text.FgRed}
		}
		return text.Colors{text.FgGreen}
	})

	t.AppendHeader(table.Row{"", table1Name, table2Name, "Diff"})
	switch stats1.Type() {
	case "numerical":
		numStats1 := stats1.(*diff.NumericalStatistics)
		numStats2 := stats2.(*diff.NumericalStatistics)

		if numStats1.Min != nil && numStats2.Min != nil {
			t.AppendRow(table.Row{"Min", fmt.Sprintf("%.4g", *numStats1.Min), fmt.Sprintf("%.2g", *numStats2.Min), fmt.Sprintf("%.2g", *numStats1.Min-*numStats2.Min)})
		}
		if numStats1.Max != nil && numStats2.Max != nil {
			t.AppendRow(table.Row{"Max", fmt.Sprintf("%.4g", *numStats1.Max), fmt.Sprintf("%.2g", *numStats2.Max), fmt.Sprintf("%.2g", *numStats1.Max-*numStats2.Max)})
		}
		if numStats1.Avg != nil && numStats2.Avg != nil {
			t.AppendRow(table.Row{"Avg", fmt.Sprintf("%.4g", *numStats1.Avg), fmt.Sprintf("%.2g", *numStats2.Avg), fmt.Sprintf("%.2g", *numStats1.Avg-*numStats2.Avg)})
		}
		if numStats1.Sum != nil && numStats2.Sum != nil {
			t.AppendRow(table.Row{"Sum", fmt.Sprintf("%.4g", *numStats1.Sum), fmt.Sprintf("%.2g", *numStats2.Sum), fmt.Sprintf("%.2g", *numStats1.Sum-*numStats2.Sum)})
		}
		t.AppendRow(table.Row{"Count", numStats1.Count, numStats2.Count, numStats1.Count - numStats2.Count})
		t.AppendRow(table.Row{"Null Count", numStats1.NullCount, numStats2.NullCount, numStats1.NullCount - numStats2.NullCount})
		if numStats1.StdDev != nil && numStats2.StdDev != nil {
			t.AppendRow(table.Row{"StdDev", fmt.Sprintf("%.4g", *numStats1.StdDev), fmt.Sprintf("%.2g", *numStats2.StdDev), fmt.Sprintf("%.2g", *numStats1.StdDev-*numStats2.StdDev)})
		}

	case "string":
		strStats1 := stats1.(*diff.StringStatistics)
		strStats2 := stats2.(*diff.StringStatistics)
		if strStats1 == nil || strStats2 == nil {
			return ""
		}

		t.AppendRow(table.Row{"Distinct Count", strStats1.DistinctCount, strStats2.DistinctCount, strStats1.DistinctCount - strStats2.DistinctCount})
		t.AppendRow(table.Row{"Max Length", strStats1.MaxLength, strStats2.MaxLength, strStats1.MaxLength - strStats2.MaxLength})
		t.AppendRow(table.Row{"Min Length", strStats1.MinLength, strStats2.MinLength, strStats1.MinLength - strStats2.MinLength})
		t.AppendRow(table.Row{"Avg Length", fmt.Sprintf("%.4g", strStats1.AvgLength), fmt.Sprintf("%.2g", strStats2.AvgLength), fmt.Sprintf("%.2g", strStats1.AvgLength-strStats2.AvgLength)})
		t.AppendRow(table.Row{"Null Count", strStats1.NullCount, strStats2.NullCount, strStats1.NullCount - strStats2.NullCount})
		t.AppendRow(table.Row{"Empty Count", strStats1.EmptyCount, strStats2.EmptyCount, strStats1.EmptyCount - strStats2.EmptyCount})

	case "boolean":
		boolStats1 := stats1.(*diff.BooleanStatistics)
		boolStats2 := stats2.(*diff.BooleanStatistics)
		if boolStats1 == nil || boolStats2 == nil {
			return ""
		}

		t.AppendRow(table.Row{"True Count", boolStats1.TrueCount, boolStats2.TrueCount, boolStats1.TrueCount - boolStats2.TrueCount})
		t.AppendRow(table.Row{"False Count", boolStats1.FalseCount, boolStats2.FalseCount, boolStats1.FalseCount - boolStats2.FalseCount})
		t.AppendRow(table.Row{"Null Count", boolStats1.NullCount, boolStats2.NullCount, boolStats1.NullCount - boolStats2.NullCount})
		t.AppendRow(table.Row{"Count", boolStats1.Count, boolStats2.Count, boolStats1.Count - boolStats2.Count})

	case "datetime":
		dtStats1 := stats1.(*diff.DateTimeStatistics)
		dtStats2 := stats2.(*diff.DateTimeStatistics)
		if dtStats1 == nil || dtStats2 == nil {
			return ""
		}

		if dtStats1.EarliestDate != nil && dtStats2.EarliestDate != nil {
			t.AppendRow(table.Row{"Earliest Date", *dtStats1.EarliestDate, *dtStats2.EarliestDate, ""})
		}
		if dtStats1.LatestDate != nil && dtStats2.LatestDate != nil {
			t.AppendRow(table.Row{"Latest Date", *dtStats1.LatestDate, *dtStats2.LatestDate, ""})
		}
		t.AppendRow(table.Row{"Count", dtStats1.Count, dtStats2.Count, dtStats1.Count - dtStats2.Count})
		t.AppendRow(table.Row{"Null Count", dtStats1.NullCount, dtStats2.NullCount, dtStats1.NullCount - dtStats2.NullCount})
		t.AppendRow(table.Row{"Unique Count", dtStats1.UniqueCount, dtStats2.UniqueCount, dtStats1.UniqueCount - dtStats2.UniqueCount})
	}
	return t.Render()
}
