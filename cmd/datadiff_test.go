package cmd

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculatePercentageDiff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		val1      float64
		val2      float64
		tolerance float64
		expected  string
	}{
		{
			name:      "zero difference",
			val1:      100.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "positive difference",
			val1:      150.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "50.0%",
		},
		{
			name:      "negative difference",
			val1:      75.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "-25.0%",
		},
		{
			name:      "divide by zero with non-zero numerator",
			val1:      100.0,
			val2:      0.0,
			tolerance: 0.001,
			expected:  "∞%",
		},
		{
			name:      "both values zero",
			val1:      0.0,
			val2:      0.0,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "small decimal values",
			val1:      1.1,
			val2:      1.0,
			tolerance: 0.001,
			expected:  "10.0%",
		},
		{
			name:      "large values",
			val1:      1000000.0,
			val2:      900000.0,
			tolerance: 0.001,
			expected:  "11.1%",
		},
		{
			name:      "negative values",
			val1:      -50.0,
			val2:      -100.0,
			tolerance: 0.001,
			expected:  "-50.0%",
		},
		{
			name:      "mixed sign values",
			val1:      50.0,
			val2:      -100.0,
			tolerance: 0.001,
			expected:  "-150.0%",
		},
		{
			name:      "difference within tolerance",
			val1:      100.0005,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "<0.001%",
		},
		{
			name:      "difference just above tolerance",
			val1:      100.002,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "0.0%",
		},
		{
			name:      "negative difference within tolerance",
			val1:      99.9995,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "<0.001%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := calculatePercentageDiff(tt.val1, tt.val2, tt.tolerance)
			if result != tt.expected {
				t.Errorf("calculatePercentageDiff(%.6f, %.6f, %.3f) = %q, want %q", tt.val1, tt.val2, tt.tolerance, result, tt.expected)
			}
		})
	}
}

func TestCalculatePercentageDiffInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		val1      int64
		val2      int64
		tolerance float64
		expected  string
	}{
		{
			name:      "zero difference integers",
			val1:      100,
			val2:      100,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "positive difference integers",
			val1:      150,
			val2:      100,
			tolerance: 0.001,
			expected:  "50.0%",
		},
		{
			name:      "negative difference integers",
			val1:      75,
			val2:      100,
			tolerance: 0.001,
			expected:  "-25.0%",
		},
		{
			name:      "divide by zero with integers",
			val1:      100,
			val2:      0,
			tolerance: 0.001,
			expected:  "∞%",
		},
		{
			name:      "both integers zero",
			val1:      0,
			val2:      0,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "large integer values",
			val1:      1000000,
			val2:      900000,
			tolerance: 0.001,
			expected:  "11.1%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := calculatePercentageDiffInt(tt.val1, tt.val2, tt.tolerance)
			if result != tt.expected {
				t.Errorf("calculatePercentageDiffInt(%d, %d, %.3f) = %q, want %q", tt.val1, tt.val2, tt.tolerance, result, tt.expected)
			}
		})
	}
}

func TestCalculatePercentageDiffEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		val1      float64
		val2      float64
		tolerance float64
		expected  string
	}{
		{
			name:      "very small positive difference",
			val1:      1.0001,
			val2:      1.0,
			tolerance: 0.001,
			expected:  "0.0%", // Should round to 0.0%
		},
		{
			name:      "very small negative difference",
			val1:      0.9999,
			val2:      1.0,
			tolerance: 0.001,
			expected:  "<0.001%", // Should be treated as within tolerance
		},
		{
			name:      "precise decimal calculation",
			val1:      33.0,
			val2:      30.0,
			tolerance: 0.001,
			expected:  "10.0%",
		},
		{
			name:      "one third increase",
			val1:      4.0,
			val2:      3.0,
			tolerance: 0.001,
			expected:  "33.3%",
		},
		{
			name:      "double the value",
			val1:      200.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "100.0%",
		},
		{
			name:      "half the value",
			val1:      50.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "-50.0%",
		},
		{
			name:      "high tolerance test",
			val1:      102.0,
			val2:      100.0,
			tolerance: 5.0,
			expected:  "<5%", // 2% difference should be considered within 5% tolerance
		},
		{
			name:      "very high tolerance test",
			val1:      150.0,
			val2:      100.0,
			tolerance: 100.0,
			expected:  "<100%", // 50% difference should be considered within 100% tolerance
		},
		{
			name:      "custom tolerance test",
			val1:      100.05,
			val2:      100.0,
			tolerance: 0.1,
			expected:  "<0.1%", // 0.05% difference should be considered within 0.1% tolerance
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := calculatePercentageDiff(tt.val1, tt.val2, tt.tolerance)
			if result != tt.expected {
				t.Errorf("calculatePercentageDiff(%.4f, %.4f, %.3f) = %q, want %q", tt.val1, tt.val2, tt.tolerance, result, tt.expected)
			}
		})
	}
}

func TestFormatDiffValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		rawDiff        float64
		percentageDiff string
		expected       string
	}{
		{
			name:           "difference within tolerance - very small",
			rawDiff:        5.274e-16,
			percentageDiff: "<0.001%",
			expected:       "5.274e-16",
		},
		{
			name:           "difference within tolerance - small",
			rawDiff:        0.0005,
			percentageDiff: "<0.001%",
			expected:       "0.0005",
		},
		{
			name:           "difference above tolerance",
			rawDiff:        0.5,
			percentageDiff: "10.0%",
			expected:       "0.5",
		},
		{
			name:           "zero difference",
			rawDiff:        0.0,
			percentageDiff: "-",
			expected:       "-",
		},
		{
			name:           "large difference",
			rawDiff:        1000.0,
			percentageDiff: "50.0%",
			expected:       "1000",
		},
		{
			name:           "very small but significant difference",
			rawDiff:        0.001,
			percentageDiff: "0.1%",
			expected:       "0.001",
		},
		{
			name:           "negative difference within tolerance",
			rawDiff:        -1e-15,
			percentageDiff: "<0.001%",
			expected:       "-1e-15",
		},
		{
			name:           "negative difference above tolerance",
			rawDiff:        -0.25,
			percentageDiff: "-5.0%",
			expected:       "-0.25",
		},
		{
			name:           "difference within tolerance - medium",
			rawDiff:        0.05,
			percentageDiff: "<5%",
			expected:       "0.05",
		},
		{
			name:           "difference within tolerance - micro",
			rawDiff:        1e-7,
			percentageDiff: "<0.001%",
			expected:       "1e-07",
		},
		{
			name:           "difference within tolerance - milli",
			rawDiff:        0.0002,
			percentageDiff: "<0.1%",
			expected:       "0.0002",
		},
		{
			name:           "difference within tolerance - large scale",
			rawDiff:        5.0,
			percentageDiff: "<10%",
			expected:       "5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := formatDiffValue(tt.rawDiff, tt.percentageDiff)
			if result != tt.expected {
				t.Errorf("formatDiffValue(%.6g, %q) = %q, want %q", tt.rawDiff, tt.percentageDiff, result, tt.expected)
			}
		})
	}
}

func TestGenerateAlterStatements(t *testing.T) {
	t.Parallel()

	t.Run("returns empty when no schema differences", func(t *testing.T) {
		t.Parallel()
		result := diff.SchemaComparisonResult{
			HasSchemaDifferences: false,
			Table1: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "table1"},
			},
			Table2: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "table2"},
			},
		}

		statements := generateAlterStatements(result, "postgres", "postgres", "", false)
		assert.Empty(t, statements)
	})

	t.Run("uses explicit dialect when provided", func(t *testing.T) {
		t.Parallel()
		result := diff.SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "users"},
			},
			Table2: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "users_copy"},
			},
			MissingColumns: []diff.MissingColumn{
				{
					ColumnName:  "email",
					Type:        "VARCHAR(255)",
					Nullable:    false,
					MissingFrom: "users_copy",
					TableName:   "users",
				},
			},
		}

		statements := generateAlterStatements(result, "postgres", "duckdb", "bigquery", false)
		require.Len(t, statements, 1)
		// BigQuery uses backticks
		assert.Contains(t, statements[0], "`users_copy`")
	})

	t.Run("auto-detects dialect from same connection types", func(t *testing.T) {
		t.Parallel()
		result := diff.SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "products"},
			},
			Table2: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "products_v2"},
			},
			MissingColumns: []diff.MissingColumn{
				{
					ColumnName:  "description",
					Type:        "TEXT",
					Nullable:    true,
					MissingFrom: "products_v2",
					TableName:   "products",
				},
			},
		}

		statements := generateAlterStatements(result, "postgres", "postgres", "", false)
		require.Len(t, statements, 1)
		// PostgreSQL uses double quotes
		assert.Contains(t, statements[0], `"products_v2"`)
	})

	t.Run("auto-detects dialect from different connection types", func(t *testing.T) {
		t.Parallel()
		result := diff.SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "orders"},
			},
			Table2: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "orders_staging"},
			},
			ColumnDifferences: []diff.ColumnDifference{
				{
					ColumnName: "status",
					TypeDifference: &diff.TypeDifference{
						Table1Type: "VARCHAR(20)",
						Table2Type: "VARCHAR(10)",
					},
				},
			},
		}

		// Should use second connection's dialect (snowflake)
		statements := generateAlterStatements(result, "postgres", "snowflake", "", false)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], `"orders_staging"`)
		assert.Contains(t, statements[0], "SET DATA TYPE") // Snowflake syntax
	})

	t.Run("respects reverse flag", func(t *testing.T) {
		t.Parallel()
		result := diff.SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "source"},
			},
			Table2: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "target"},
			},
			MissingColumns: []diff.MissingColumn{
				{
					ColumnName:  "new_column",
					Type:        "INTEGER",
					Nullable:    false,
					MissingFrom: "target",
					TableName:   "source",
				},
			},
		}

		// Without reverse: modifies target to match source (add new_column to target)
		statements := generateAlterStatements(result, "duckdb", "duckdb", "", false)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], `"target"`)
		assert.Contains(t, statements[0], "ADD COLUMN")

		// With reverse: modifies source to match target (drop new_column from source)
		statementsReverse := generateAlterStatements(result, "duckdb", "duckdb", "", true)
		require.Len(t, statementsReverse, 1)
		assert.Contains(t, statementsReverse[0], `"source"`)
		assert.Contains(t, statementsReverse[0], "DROP COLUMN")
	})

	t.Run("generates statements for multiple changes", func(t *testing.T) {
		t.Parallel()
		result := diff.SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "employees"},
			},
			Table2: &diff.TableSummaryResult{
				Table: &diff.Table{Name: "employees_temp"},
			},
			MissingColumns: []diff.MissingColumn{
				{
					ColumnName:  "department",
					Type:        "VARCHAR(100)",
					Nullable:    false,
					MissingFrom: "employees_temp",
					TableName:   "employees",
				},
			},
			ColumnDifferences: []diff.ColumnDifference{
				{
					ColumnName: "salary",
					TypeDifference: &diff.TypeDifference{
						Table1Type: "DECIMAL(12,2)",
						Table2Type: "INTEGER",
					},
				},
			},
		}

		statements := generateAlterStatements(result, "duckdb", "duckdb", "", false)
		// DuckDB generates separate statements for each change
		require.Len(t, statements, 2)

		// Check that statements contain the expected operations
		allStatements := strings.Join(statements, " ")
		assert.Contains(t, allStatements, `"employees_temp"`)
		assert.Contains(t, allStatements, "ADD COLUMN")
		assert.Contains(t, allStatements, "ALTER COLUMN")
	})
}
