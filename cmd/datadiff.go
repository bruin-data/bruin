package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
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

func compareTables(ctx context.Context, summarizer1, summarizer2 diff.TableSummarizer, table1, table2 string, schemaOnly bool) (*diff.SchemaComparisonResult, error) {
	var summary1, summary2 *diff.TableSummaryResult
	var err1, err2 error
	var wg conc.WaitGroup

	wg.Go(func() {
		summary1, err1 = summarizer1.GetTableSummary(ctx, table1, schemaOnly)
	})

	wg.Go(func() {
		summary2, err2 = summarizer2.GetTableSummary(ctx, table2, schemaOnly)
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
	var tolerance float64
	var schemaOnly bool

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
			&cli.Float64Flag{
				Name:        "tolerance",
				Aliases:     []string{"t"},
				Usage:       "Tolerance percentage for considering values equal (default: 0.001%). Values with percentage difference below this threshold are considered equal.",
				Destination: &tolerance,
				Value:       0.001,
			},
			&cli.BoolFlag{
				Name:        "schema-only",
				Usage:       "Compare only table schemas without analyzing row counts or column distributions",
				Destination: &schemaOnly,
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
			conn1 := manager.GetConnection(conn1Name)
			if conn1 == nil {
				return fmt.Errorf("failed to get connection '%s'", conn1Name)
			}

			conn2 := manager.GetConnection(conn2Name)
			if conn2 == nil {
				return fmt.Errorf("failed to get connection '%s'", conn2Name)
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

			schemaComparison, err := compareTables(ctx, s1, s2, table1Name, table2Name, schemaOnly)
			if err != nil {
				errorPrinter.Printf("error comparing tables '%s' and '%s':\n\n%v", table1Identifier, table2Identifier, err)
				return cli.Exit("", 1)
			}

			if schemaComparison != nil {
				hasDifferences := printSchemaComparisonOutput(*schemaComparison, table1Identifier, table2Identifier, tolerance, schemaOnly, c.App.Writer)
				if hasDifferences {
					return cli.Exit("", 1) // Exit with code 1 when differences are found
				}
			} else {
				fmt.Fprintf(c.App.ErrWriter, "\nUnable to compare summaries - the comparison result is nil\n")
				return errors.New("failed to compare table summaries due to missing data")
			}

			return nil // Exit with code 0 when no differences are found
		},
	}
}

func printSchemaComparisonOutput(schemaComparison diff.SchemaComparisonResult, table1Name, table2Name string, tolerance float64, schemaOnly bool, errOut io.Writer) bool {
	fmt.Fprint(errOut, schemaComparison.GetSummaryTable()+"\n")

	// Print column types comparison table
	fmt.Fprintf(errOut, "\n%s\n", getColumnTypesComparisonTable(schemaComparison, table1Name, table2Name))

	greenPrinter := color.New(color.FgGreen)
	hasDifferences := false

	var overallSchemaMessage string
	t1Schema := schemaComparison.Table1.Table
	t2Schema := schemaComparison.Table2.Table

	// Check for schema differences
	if schemaComparison.HasSchemaDifferences || schemaComparison.HasRowCountDifference {
		hasDifferences = true
	}

	if !schemaComparison.HasSchemaDifferences && !schemaComparison.HasRowCountDifference {
		if schemaComparison.SamePropertiesCount == 0 && len(t1Schema.Columns) == 0 && len(t2Schema.Columns) == 0 {
			overallSchemaMessage = fmt.Sprintf("Tables '%s' and '%s' both have no columns and are considered schema-identical.", table1Name, table2Name)
		} else {
			overallSchemaMessage = "Table schemas are considered identical."
		}

		greenPrinter.Fprintf(errOut, "\n%s\n", overallSchemaMessage)
		if schemaOnly {
			return false // No differences found in schema-only mode
		}
	}

	if schemaComparison.HasSchemaDifferences {
		errorPrinter.Fprintf(errOut, "\n%s\n", "Schema differences detected, see the table above.")
	}

	// Skip detailed column statistics in schema-only mode
	if !schemaOnly { //nolint:nestif
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
			someColumnsExist := false
			for _, t1Column := range t1Schema.Columns {
				if t2Column, ok := t2Columns[t1Column.Name]; ok {
					if t1Column.Stats != nil && t2Column.Stats != nil {
						var typeInfo string
						if t1Column.Type == t2Column.Type {
							// Same types - use faint color
							faintPrinter := color.New(color.Faint)
							typeInfo = faintPrinter.Sprintf(" (%s | %s)", t1Column.Type, t2Column.Type)
						} else {
							// Different types - use faint red color
							faintRedPrinter := color.New(color.Faint, color.FgRed)
							typeInfo = faintRedPrinter.Sprintf(" (%s | %s)", t1Column.Type, t2Column.Type)
						}
						fmt.Fprintf(errOut, "\n%s%s\n", t1Column.Name, typeInfo)
						fmt.Fprintf(errOut, "%s\n", tableStatsToTable(t1Column.Stats, t2Column.Stats, table1Name, table2Name, tolerance))
						someColumnsExist = true

						// Check if this column has data differences beyond tolerance
						if hasDataDifferences(t1Column.Stats, t2Column.Stats, tolerance) {
							hasDifferences = true
						}
					}
				}
			}
			if !someColumnsExist {
				fmt.Fprintf(errOut, "No columns exist in both tables.\n")
			}
		}

		// If no schema differences but we're not in schema-only mode, check if only identical
		if !schemaComparison.HasSchemaDifferences && !schemaComparison.HasRowCountDifference && !hasDifferences {
			greenPrinter.Fprintf(errOut, "\n%s\n", overallSchemaMessage)
		}
	}

	// Return true if any differences were found
	return hasDifferences
}

func hasDataDifferences(stats1, stats2 diff.ColumnStatistics, tolerance float64) bool {
	if stats1 == nil || stats2 == nil {
		return false
	}

	if stats1.Type() != stats2.Type() {
		return true // Different stat types indicate data differences
	}

	switch stats1.Type() {
	case "numerical":
		numStats1 := stats1.(*diff.NumericalStatistics)
		numStats2 := stats2.(*diff.NumericalStatistics)

		// Check count differences
		if hasSignificantDifference(float64(numStats1.Count), float64(numStats2.Count), tolerance) {
			return true
		}
		// Check null count differences
		if hasSignificantDifference(float64(numStats1.NullCount), float64(numStats2.NullCount), tolerance) {
			return true
		}
		// Check statistical differences if available
		if numStats1.Avg != nil && numStats2.Avg != nil {
			if hasSignificantDifference(*numStats1.Avg, *numStats2.Avg, tolerance) {
				return true
			}
		}

	case "string":
		strStats1 := stats1.(*diff.StringStatistics)
		strStats2 := stats2.(*diff.StringStatistics)

		if hasSignificantDifference(float64(strStats1.Count), float64(strStats2.Count), tolerance) {
			return true
		}
		if hasSignificantDifference(float64(strStats1.NullCount), float64(strStats2.NullCount), tolerance) {
			return true
		}
		if hasSignificantDifference(float64(strStats1.DistinctCount), float64(strStats2.DistinctCount), tolerance) {
			return true
		}

	case "boolean":
		boolStats1 := stats1.(*diff.BooleanStatistics)
		boolStats2 := stats2.(*diff.BooleanStatistics)

		if hasSignificantDifference(float64(boolStats1.Count), float64(boolStats2.Count), tolerance) {
			return true
		}
		if hasSignificantDifference(float64(boolStats1.TrueCount), float64(boolStats2.TrueCount), tolerance) {
			return true
		}

	case "datetime":
		dtStats1 := stats1.(*diff.DateTimeStatistics)
		dtStats2 := stats2.(*diff.DateTimeStatistics)

		if hasSignificantDifference(float64(dtStats1.Count), float64(dtStats2.Count), tolerance) {
			return true
		}
		if hasSignificantDifference(float64(dtStats1.UniqueCount), float64(dtStats2.UniqueCount), tolerance) {
			return true
		}
	}

	return false
}

func hasSignificantDifference(val1, val2, tolerance float64) bool {
	if val1 == val2 {
		return false
	}
	if val2 == 0 {
		return val1 != 0 // Any non-zero value is significant when comparing to zero
	}
	diffPercent := abs(((val1 - val2) / val2) * 100)
	return diffPercent > tolerance
}

func calculatePercentageDiff(val1, val2, tolerance float64) string {
	if val1 == val2 {
		return "-"
	}
	if val2 == 0 {
		return "∞%"
	}
	diffPercent := ((val1 - val2) / val2) * 100

	// Check if the absolute percentage difference is within tolerance
	if abs(diffPercent) <= tolerance {
		return fmt.Sprintf("<%.3g%%", tolerance)
	}

	formatted := fmt.Sprintf("%.1f%%", diffPercent)
	// Fix the "-0.0%" display issue - treat as within tolerance
	if formatted == "-0.0%" {
		return fmt.Sprintf("<%.3g%%", tolerance)
	}
	return formatted
}

func calculatePercentageDiffInt(val1, val2 int64, tolerance float64) string {
	return calculatePercentageDiff(float64(val1), float64(val2), tolerance)
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// formatDiffValue returns the actual formatted raw difference value,
// showing "-" only for truly equal values.
func formatDiffValue(rawDiff float64, percentageDiff string) string {
	if percentageDiff == "-" {
		return "-"
	}
	if rawDiff == math.Trunc(rawDiff) {
		return strconv.FormatInt(int64(rawDiff), 10)
	}
	return fmt.Sprintf("%.4g", rawDiff)
}

func tableStatsToTable(stats1 diff.ColumnStatistics, stats2 diff.ColumnStatistics, table1Name, table2Name string, tolerance float64) string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, Align: text.AlignLeft},
		{Number: 2, Align: text.AlignRight},
		{Number: 3, Align: text.AlignRight},
		{Number: 4, Align: text.AlignRight},
		{Number: 5, Align: text.AlignRight},
	})

	t.SetRowPainter(func(row table.Row) text.Colors {
		if len(row) == 0 {
			return text.Colors{}
		}
		if len(row) != 5 {
			return text.Colors{}
		}

		// Check if this is a datetime row with different values
		diffStr := fmt.Sprintf("%v", row[3])
		if diffStr == "-" && fmt.Sprintf("%v", row[4]) == "N/A" {
			// This might be a datetime row, check if the actual values are different
			val1 := fmt.Sprintf("%v", row[1])
			val2 := fmt.Sprintf("%v", row[2])
			if val1 != val2 {
				return text.Colors{text.FgRed}
			}
			return text.Colors{text.FgGreen}
		}

		diffVal, _ := strconv.ParseFloat(diffStr, 64)
		if abs(diffVal) > tolerance {
			return text.Colors{text.FgRed}
		}
		return text.Colors{text.FgGreen}
	})

	t.AppendHeader(table.Row{"", table1Name, table2Name, "Diff", "DIFF %"})

	// Check if statistics types are compatible
	if stats1.Type() != stats2.Type() {
		t.AppendRow(table.Row{"Type Mismatch", fmt.Sprintf("(%s)", stats1.Type()), fmt.Sprintf("(%s)", stats2.Type()), "N/A", "N/A"})
		return t.Render()
	}

	switch stats1.Type() {
	case "numerical":
		numStats1 := stats1.(*diff.NumericalStatistics)
		numStats2 := stats2.(*diff.NumericalStatistics)

		// General counts first
		countDiff := numStats1.Count - numStats2.Count
		countDiffPercent := calculatePercentageDiffInt(numStats1.Count, numStats2.Count, tolerance)
		countDiffStr := formatDiffValue(float64(countDiff), countDiffPercent)
		t.AppendRow(table.Row{"Count", numStats1.Count, numStats2.Count, countDiffStr, countDiffPercent})

		nullDiff := numStats1.NullCount - numStats2.NullCount
		nullDiffPercent := calculatePercentageDiffInt(numStats1.NullCount, numStats2.NullCount, tolerance)
		nullDiffStr := formatDiffValue(float64(nullDiff), nullDiffPercent)
		t.AppendRow(table.Row{"Null Count", numStats1.NullCount, numStats2.NullCount, nullDiffStr, nullDiffPercent})

		// Calculate fill rates
		fillRate1 := float64(numStats1.Count-numStats1.NullCount) / float64(numStats1.Count) * 100
		fillRate2 := float64(numStats2.Count-numStats2.NullCount) / float64(numStats2.Count) * 100
		fillRateDiff := fillRate1 - fillRate2
		fillRateDiffPercent := calculatePercentageDiff(fillRate1, fillRate2, tolerance)
		fillRateDiffStr := formatDiffValue(fillRateDiff, fillRateDiffPercent)
		t.AppendRow(table.Row{"Fill Rate", fmt.Sprintf("%.6g%%", fillRate1), fmt.Sprintf("%.6g%%", fillRate2), fillRateDiffStr, fillRateDiffPercent})

		// Descriptive statistics
		if numStats1.Min != nil && numStats2.Min != nil {
			diff := *numStats1.Min - *numStats2.Min
			diffPercent := calculatePercentageDiff(*numStats1.Min, *numStats2.Min, tolerance)
			diffStr := formatDiffValue(diff, diffPercent)
			t.AppendRow(table.Row{"Min", fmt.Sprintf("%.4g", *numStats1.Min), fmt.Sprintf("%.4g", *numStats2.Min), diffStr, diffPercent})
		}
		if numStats1.Max != nil && numStats2.Max != nil {
			diff := *numStats1.Max - *numStats2.Max
			diffPercent := calculatePercentageDiff(*numStats1.Max, *numStats2.Max, tolerance)
			diffStr := formatDiffValue(diff, diffPercent)
			t.AppendRow(table.Row{"Max", fmt.Sprintf("%.4g", *numStats1.Max), fmt.Sprintf("%.4g", *numStats2.Max), diffStr, diffPercent})
		}
		if numStats1.Avg != nil && numStats2.Avg != nil {
			diff := *numStats1.Avg - *numStats2.Avg
			diffPercent := calculatePercentageDiff(*numStats1.Avg, *numStats2.Avg, tolerance)
			diffStr := formatDiffValue(diff, diffPercent)
			t.AppendRow(table.Row{"Avg", fmt.Sprintf("%.4g", *numStats1.Avg), fmt.Sprintf("%.4g", *numStats2.Avg), diffStr, diffPercent})
		}
		if numStats1.Sum != nil && numStats2.Sum != nil {
			diff := *numStats1.Sum - *numStats2.Sum
			diffPercent := calculatePercentageDiff(*numStats1.Sum, *numStats2.Sum, tolerance)
			diffStr := formatDiffValue(diff, diffPercent)
			t.AppendRow(table.Row{"Sum", fmt.Sprintf("%.4g", *numStats1.Sum), fmt.Sprintf("%.4g", *numStats2.Sum), diffStr, diffPercent})
		}
		if numStats1.StdDev != nil && numStats2.StdDev != nil {
			stdDevDiff := *numStats1.StdDev - *numStats2.StdDev
			stdDevDiffPercent := calculatePercentageDiff(*numStats1.StdDev, *numStats2.StdDev, tolerance)
			stdDevDiffStr := formatDiffValue(stdDevDiff, stdDevDiffPercent)
			t.AppendRow(table.Row{"StdDev", fmt.Sprintf("%.4g", *numStats1.StdDev), fmt.Sprintf("%.4g", *numStats2.StdDev), stdDevDiffStr, stdDevDiffPercent})
		}

	case "string":
		strStats1 := stats1.(*diff.StringStatistics)
		strStats2 := stats2.(*diff.StringStatistics)
		if strStats1 == nil || strStats2 == nil {
			return ""
		}

		// General counts first
		countDiff := strStats1.Count - strStats2.Count
		countDiffPercent := calculatePercentageDiffInt(strStats1.Count, strStats2.Count, tolerance)
		countDiffStr := formatDiffValue(float64(countDiff), countDiffPercent)
		t.AppendRow(table.Row{"Count", strStats1.Count, strStats2.Count, countDiffStr, countDiffPercent})

		nullDiff := strStats1.NullCount - strStats2.NullCount
		nullDiffPercent := calculatePercentageDiffInt(strStats1.NullCount, strStats2.NullCount, tolerance)
		nullDiffStr := formatDiffValue(float64(nullDiff), nullDiffPercent)
		t.AppendRow(table.Row{"Null Count", strStats1.NullCount, strStats2.NullCount, nullDiffStr, nullDiffPercent})

		// Calculate fill rates using proper count
		fillRate1 := float64(strStats1.Count-strStats1.NullCount) / float64(strStats1.Count) * 100
		fillRate2 := float64(strStats2.Count-strStats2.NullCount) / float64(strStats2.Count) * 100
		fillRateDiff := fillRate1 - fillRate2
		fillRateDiffPercent := calculatePercentageDiff(fillRate1, fillRate2, tolerance)
		fillRateDiffStr := formatDiffValue(fillRateDiff, fillRateDiffPercent)
		t.AppendRow(table.Row{"Fill Rate", fmt.Sprintf("%.6g%%", fillRate1), fmt.Sprintf("%.6g%%", fillRate2), fillRateDiffStr, fillRateDiffPercent})

		// Value-specific statistics
		distinctDiff := strStats1.DistinctCount - strStats2.DistinctCount
		distinctDiffPercent := calculatePercentageDiffInt(strStats1.DistinctCount, strStats2.DistinctCount, tolerance)
		distinctDiffStr := formatDiffValue(float64(distinctDiff), distinctDiffPercent)
		t.AppendRow(table.Row{"Distinct Count", strStats1.DistinctCount, strStats2.DistinctCount, distinctDiffStr, distinctDiffPercent})

		emptyDiff := strStats1.EmptyCount - strStats2.EmptyCount
		emptyDiffPercent := calculatePercentageDiffInt(strStats1.EmptyCount, strStats2.EmptyCount, tolerance)
		emptyDiffStr := formatDiffValue(float64(emptyDiff), emptyDiffPercent)
		t.AppendRow(table.Row{"Empty Count", strStats1.EmptyCount, strStats2.EmptyCount, emptyDiffStr, emptyDiffPercent})

		// Length statistics
		minLenDiff := strStats1.MinLength - strStats2.MinLength
		minLenDiffPercent := calculatePercentageDiffInt(int64(strStats1.MinLength), int64(strStats2.MinLength), tolerance)
		minLenDiffStr := formatDiffValue(float64(minLenDiff), minLenDiffPercent)
		t.AppendRow(table.Row{"Min Length", strStats1.MinLength, strStats2.MinLength, minLenDiffStr, minLenDiffPercent})

		maxLenDiff := strStats1.MaxLength - strStats2.MaxLength
		maxLenDiffPercent := calculatePercentageDiffInt(int64(strStats1.MaxLength), int64(strStats2.MaxLength), tolerance)
		maxLenDiffStr := formatDiffValue(float64(maxLenDiff), maxLenDiffPercent)
		t.AppendRow(table.Row{"Max Length", strStats1.MaxLength, strStats2.MaxLength, maxLenDiffStr, maxLenDiffPercent})

		avgLenDiff := strStats1.AvgLength - strStats2.AvgLength
		avgLenDiffPercent := calculatePercentageDiff(strStats1.AvgLength, strStats2.AvgLength, tolerance)
		avgLenDiffStr := formatDiffValue(avgLenDiff, avgLenDiffPercent)
		t.AppendRow(table.Row{"Avg Length", fmt.Sprintf("%.4g", strStats1.AvgLength), fmt.Sprintf("%.4g", strStats2.AvgLength), avgLenDiffStr, avgLenDiffPercent})

	case "boolean":
		boolStats1 := stats1.(*diff.BooleanStatistics)
		boolStats2 := stats2.(*diff.BooleanStatistics)
		if boolStats1 == nil || boolStats2 == nil {
			return ""
		}

		// General counts first
		countDiff := boolStats1.Count - boolStats2.Count
		countDiffPercent := calculatePercentageDiffInt(boolStats1.Count, boolStats2.Count, tolerance)
		countDiffStr := formatDiffValue(float64(countDiff), countDiffPercent)
		t.AppendRow(table.Row{"Count", boolStats1.Count, boolStats2.Count, countDiffStr, countDiffPercent})

		nullDiff := boolStats1.NullCount - boolStats2.NullCount
		nullDiffPercent := calculatePercentageDiffInt(boolStats1.NullCount, boolStats2.NullCount, tolerance)
		nullDiffStr := formatDiffValue(float64(nullDiff), nullDiffPercent)
		t.AppendRow(table.Row{"Null Count", boolStats1.NullCount, boolStats2.NullCount, nullDiffStr, nullDiffPercent})

		// Calculate fill rates
		fillRate1 := float64(boolStats1.Count-boolStats1.NullCount) / float64(boolStats1.Count) * 100
		fillRate2 := float64(boolStats2.Count-boolStats2.NullCount) / float64(boolStats2.Count) * 100
		fillRateDiff := fillRate1 - fillRate2
		fillRateDiffPercent := calculatePercentageDiff(fillRate1, fillRate2, tolerance)
		fillRateDiffStr := formatDiffValue(fillRateDiff, fillRateDiffPercent)
		t.AppendRow(table.Row{"Fill Rate", fmt.Sprintf("%.6g%%", fillRate1), fmt.Sprintf("%.6g%%", fillRate2), fillRateDiffStr, fillRateDiffPercent})

		// Value-specific statistics
		trueDiff := boolStats1.TrueCount - boolStats2.TrueCount
		trueDiffPercent := calculatePercentageDiffInt(boolStats1.TrueCount, boolStats2.TrueCount, tolerance)
		trueDiffStr := formatDiffValue(float64(trueDiff), trueDiffPercent)
		t.AppendRow(table.Row{"True Count", boolStats1.TrueCount, boolStats2.TrueCount, trueDiffStr, trueDiffPercent})

		falseDiff := boolStats1.FalseCount - boolStats2.FalseCount
		falseDiffPercent := calculatePercentageDiffInt(boolStats1.FalseCount, boolStats2.FalseCount, tolerance)
		falseDiffStr := formatDiffValue(float64(falseDiff), falseDiffPercent)
		t.AppendRow(table.Row{"False Count", boolStats1.FalseCount, boolStats2.FalseCount, falseDiffStr, falseDiffPercent})

	case "datetime":
		dtStats1 := stats1.(*diff.DateTimeStatistics)
		dtStats2 := stats2.(*diff.DateTimeStatistics)
		if dtStats1 == nil || dtStats2 == nil {
			return ""
		}

		// General counts first
		countDiff := dtStats1.Count - dtStats2.Count
		countDiffPercent := calculatePercentageDiffInt(dtStats1.Count, dtStats2.Count, tolerance)
		countDiffStr := formatDiffValue(float64(countDiff), countDiffPercent)
		t.AppendRow(table.Row{"Count", dtStats1.Count, dtStats2.Count, countDiffStr, countDiffPercent})

		nullDiff := dtStats1.NullCount - dtStats2.NullCount
		nullDiffPercent := calculatePercentageDiffInt(dtStats1.NullCount, dtStats2.NullCount, tolerance)
		nullDiffStr := formatDiffValue(float64(nullDiff), nullDiffPercent)
		t.AppendRow(table.Row{"Null Count", dtStats1.NullCount, dtStats2.NullCount, nullDiffStr, nullDiffPercent})

		// Calculate fill rates
		fillRate1 := float64(dtStats1.Count-dtStats1.NullCount) / float64(dtStats1.Count) * 100
		fillRate2 := float64(dtStats2.Count-dtStats2.NullCount) / float64(dtStats2.Count) * 100
		fillRateDiff := fillRate1 - fillRate2
		fillRateDiffPercent := calculatePercentageDiff(fillRate1, fillRate2, tolerance)
		fillRateDiffStr := formatDiffValue(fillRateDiff, fillRateDiffPercent)
		t.AppendRow(table.Row{"Fill Rate", fmt.Sprintf("%.6g%%", fillRate1), fmt.Sprintf("%.6g%%", fillRate2), fillRateDiffStr, fillRateDiffPercent})

		// Value-specific statistics
		uniqueDiff := dtStats1.UniqueCount - dtStats2.UniqueCount
		uniqueDiffPercent := calculatePercentageDiffInt(dtStats1.UniqueCount, dtStats2.UniqueCount, tolerance)
		uniqueDiffStr := formatDiffValue(float64(uniqueDiff), uniqueDiffPercent)
		t.AppendRow(table.Row{"Distinct Count", dtStats1.UniqueCount, dtStats2.UniqueCount, uniqueDiffStr, uniqueDiffPercent})

		// Date range statistics
		if dtStats1.EarliestDate != nil && dtStats2.EarliestDate != nil {
			t.AppendRow(table.Row{
				"Earliest Date",
				dtStats1.EarliestDate.Format("2006-01-02 15:04:05"),
				dtStats2.EarliestDate.Format("2006-01-02 15:04:05"),
				"-", "N/A",
			})
		}
		if dtStats1.LatestDate != nil && dtStats2.LatestDate != nil {
			t.AppendRow(table.Row{
				"Latest Date",
				dtStats1.LatestDate.Format("2006-01-02 15:04:05"),
				dtStats2.LatestDate.Format("2006-01-02 15:04:05"),
				"-", "N/A",
			})
		}

	case "json":
		jsonStats1 := stats1.(*diff.JSONStatistics)
		jsonStats2 := stats2.(*diff.JSONStatistics)
		if jsonStats1 == nil || jsonStats2 == nil {
			return ""
		}

		countDiff := jsonStats1.Count - jsonStats2.Count
		countDiffPercent := calculatePercentageDiffInt(jsonStats1.Count, jsonStats2.Count, tolerance)
		countDiffStr := formatDiffValue(float64(countDiff), countDiffPercent)
		t.AppendRow(table.Row{"Count", jsonStats1.Count, jsonStats2.Count, countDiffStr, countDiffPercent})

		nullDiff := jsonStats1.NullCount - jsonStats2.NullCount
		nullDiffPercent := calculatePercentageDiffInt(jsonStats1.NullCount, jsonStats2.NullCount, tolerance)
		nullDiffStr := formatDiffValue(float64(nullDiff), nullDiffPercent)
		t.AppendRow(table.Row{"Null Count", jsonStats1.NullCount, jsonStats2.NullCount, nullDiffStr, nullDiffPercent})

		// Calculate fill rates
		fillRate1 := float64(jsonStats1.Count-jsonStats1.NullCount) / float64(jsonStats1.Count) * 100
		fillRate2 := float64(jsonStats2.Count-jsonStats2.NullCount) / float64(jsonStats2.Count) * 100
		fillRateDiff := fillRate1 - fillRate2
		fillRateDiffPercent := calculatePercentageDiff(fillRate1, fillRate2, tolerance)
		fillRateDiffStr := formatDiffValue(fillRateDiff, fillRateDiffPercent)
		t.AppendRow(table.Row{"Fill Rate", fmt.Sprintf("%.6g%%", fillRate1), fmt.Sprintf("%.6g%%", fillRate2), fillRateDiffStr, fillRateDiffPercent})
	}
	return t.Render()
}

func getColumnTypesComparisonTable(schemaComparison diff.SchemaComparisonResult, table1Name, table2Name string) string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, Align: text.AlignLeft},
		{Number: 2, Align: text.AlignLeft},
		{Number: 3, Align: text.AlignLeft},
		{Number: 4, Align: text.AlignLeft},
	})

	t.AppendHeader(table.Row{"COLUMN", "PROP", table1Name, table2Name})

	t1Schema := schemaComparison.Table1.Table
	t2Schema := schemaComparison.Table2.Table

	// Create maps for quick lookup
	t1Columns := make(map[string]*diff.Column)
	for _, column := range t1Schema.Columns {
		t1Columns[column.Name] = column
	}

	t2Columns := make(map[string]*diff.Column)
	for _, column := range t2Schema.Columns {
		t2Columns[column.Name] = column
	}

	// Collect all unique column names
	allColumnNames := make(map[string]bool)
	for _, column := range t1Schema.Columns {
		allColumnNames[column.Name] = true
	}
	for _, column := range t2Schema.Columns {
		allColumnNames[column.Name] = true
	}

	// Convert to sorted slice for consistent ordering
	columnNames := make([]string, 0, len(allColumnNames))
	for name := range allColumnNames {
		columnNames = append(columnNames, name)
	}

	// Set row painter to highlight differences
	t.SetRowPainter(func(row table.Row) text.Colors {
		if len(row) >= 4 {
			table1Value := fmt.Sprintf("%v", row[2])
			table2Value := fmt.Sprintf("%v", row[3])

			// If one is dash (missing) or values are different, highlight in red
			if table1Value != table2Value {
				return text.Colors{text.FgRed}
			}
			// If values match, highlight in green
			return text.Colors{text.FgGreen}
		}
		return text.Colors{}
	})

	// Prepare all rows for sorting
	type tableRow struct {
		columnName string
		prop       string
		value1     string
		value2     string
	}

	allRows := make([]tableRow, 0)

	// Add rows for each column with detailed properties
	for _, columnName := range columnNames {
		var t1Col, t2Col *diff.Column
		var t1Exists, t2Exists bool

		if col, exists := t1Columns[columnName]; exists {
			t1Col = col
			t1Exists = true
		}
		if col, exists := t2Columns[columnName]; exists {
			t2Col = col
			t2Exists = true
		}

		// Add three rows for each column
		allRows = append(allRows, tableRow{
			columnName: columnName,
			prop:       "Type",
			value1:     getColumnValue(t1Col, t1Exists, "type"),
			value2:     getColumnValue(t2Col, t2Exists, "type"),
		})

		allRows = append(allRows, tableRow{
			columnName: columnName,
			prop:       "Nullable",
			value1:     getColumnValue(t1Col, t1Exists, "nullable"),
			value2:     getColumnValue(t2Col, t2Exists, "nullable"),
		})

		allRows = append(allRows, tableRow{
			columnName: columnName,
			prop:       "Constraints",
			value1:     getColumnValue(t1Col, t1Exists, "constraints"),
			value2:     getColumnValue(t2Col, t2Exists, "constraints"),
		})
	}

	// Add all rows to table
	lastColName := ""
	for _, row := range allRows {
		if lastColName == "" {
			lastColName = row.columnName
		} else if lastColName != row.columnName {
			t.AppendSeparator()
			lastColName = row.columnName
		}
		t.AppendRow(table.Row{row.columnName, row.prop, row.value1, row.value2})
	}

	// Enable auto-merge for column names
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, Align: text.AlignLeft, AutoMerge: true},
		{Number: 2, Align: text.AlignLeft},
		{Number: 3, Align: text.AlignLeft},
		{Number: 4, Align: text.AlignLeft},
	})

	return t.Render()
}

func getColumnValue(col *diff.Column, exists bool, property string) string {
	if !exists || col == nil {
		return "-"
	}

	switch property {
	case "type":
		return col.Type
	case "nullable":
		if col.Nullable {
			return "NULLABLE"
		}
		return "NOT NULL"
	case "constraints":
		var constraints []string
		if col.PrimaryKey {
			constraints = append(constraints, "PK")
		}
		if col.Unique {
			constraints = append(constraints, "UNIQUE")
		}
		if len(constraints) == 0 {
			return "-"
		}
		return strings.Join(constraints, ", ")
	default:
		return "-"
	}
}
