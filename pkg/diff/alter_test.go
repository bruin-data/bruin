package diff

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAlterStatementGenerator(t *testing.T) {
	t.Parallel()

	t.Run("creates generator with correct dialect", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		assert.Equal(t, DialectPostgreSQL, gen.dialect)
		assert.False(t, gen.reverse)
	})

	t.Run("creates generator with reverse flag", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectDuckDB, true)
		assert.Equal(t, DialectDuckDB, gen.dialect)
		assert.True(t, gen.reverse)
	})
}

func TestGenerateAlterStatements_NoChanges(t *testing.T) {
	t.Parallel()

	t.Run("returns empty slice when result is nil", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		statements := gen.GenerateAlterStatements(nil)
		assert.Empty(t, statements)
	})

	t.Run("returns empty slice when no schema differences", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: false,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "table1"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "table2"},
			},
		}
		statements := gen.GenerateAlterStatements(result)
		assert.Empty(t, statements)
	})
}

func TestGenerateAlterStatements_AddColumn(t *testing.T) {
	t.Parallel()

	t.Run("adds missing column to table2", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "users"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "users_copy"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "email",
					Type:        "VARCHAR(255)",
					Nullable:    false,
					MissingFrom: "users_copy",
					TableName:   "users",
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ALTER TABLE \"users_copy\"")
		assert.Contains(t, statements[0], "ADD COLUMN \"email\" VARCHAR(255) NOT NULL")
	})

	t.Run("adds nullable column", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectDuckDB, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "products"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "products_v2"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "description",
					Type:        "TEXT",
					Nullable:    true,
					MissingFrom: "products_v2",
					TableName:   "products",
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ADD COLUMN \"description\" TEXT")
		assert.NotContains(t, statements[0], "NOT NULL")
	})

	t.Run("adds unique column", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "accounts"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "accounts_backup"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "account_number",
					Type:        "VARCHAR(50)",
					Nullable:    false,
					Unique:      true,
					MissingFrom: "accounts_backup",
					TableName:   "accounts",
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ADD COLUMN \"account_number\" VARCHAR(50) NOT NULL UNIQUE")
	})
}

func TestGenerateAlterStatements_DropColumn(t *testing.T) {
	t.Parallel()

	t.Run("drops column that only exists on source table", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "users"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "users_copy"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "legacy_code",
					Type:        "TEXT",
					Nullable:    true,
					TableName:   "users_copy",
					MissingFrom: "users",
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ALTER TABLE \"users_copy\"")
		assert.Contains(t, statements[0], "DROP COLUMN \"legacy_code\"")
	})

	t.Run("drops column with reverse direction", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectSnowflake, true)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "table_a"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "table_b"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "obsolete_flag",
					Type:        "BOOLEAN",
					Nullable:    false,
					TableName:   "table_a",
					MissingFrom: "table_b",
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ALTER TABLE \"table_a\"")
		assert.Contains(t, statements[0], "DROP COLUMN \"obsolete_flag\"")
	})
}

func TestGenerateAlterStatements_TypeChange(t *testing.T) {
	t.Parallel()

	t.Run("changes column type in PostgreSQL", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "orders"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "orders_staging"},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "amount",
					TypeDifference: &TypeDifference{
						Table1Type:           "DECIMAL(10,2)",
						Table2Type:           "INTEGER",
						Table1NormalizedType: CommonTypeNumeric,
						Table2NormalizedType: CommonTypeNumeric,
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ALTER TABLE \"orders_staging\"")
		assert.Contains(t, statements[0], "ALTER COLUMN \"amount\" TYPE DECIMAL(10,2)")
	})

	t.Run("changes column type in Snowflake", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectSnowflake, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "transactions"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "transactions_tmp"},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "status",
					TypeDifference: &TypeDifference{
						Table1Type: "VARCHAR(20)",
						Table2Type: "VARCHAR(10)",
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ALTER COLUMN \"status\" SET DATA TYPE VARCHAR(20)")
	})

	t.Run("changes column type in BigQuery", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectBigQuery, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "events"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "events_copy"},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "event_id",
					TypeDifference: &TypeDifference{
						Table1Type: "INT64",
						Table2Type: "STRING",
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		// BigQuery generates separate statements
		require.GreaterOrEqual(t, len(statements), 1)
		assert.Contains(t, statements[0], "ALTER TABLE `events_copy`")
		assert.Contains(t, statements[0], "ALTER COLUMN `event_id` SET DATA TYPE INT64")
	})
}

func TestGenerateAlterStatements_NullabilityChange(t *testing.T) {
	t.Parallel()

	t.Run("makes column nullable in PostgreSQL", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "customers"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "customers_v2"},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "phone",
					NullabilityDifference: &NullabilityDifference{
						Table1Nullable: true,
						Table2Nullable: false,
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ALTER COLUMN \"phone\" DROP NOT NULL")
	})

	t.Run("makes column not nullable in DuckDB", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectDuckDB, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "items"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "items_staging"},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "sku",
					NullabilityDifference: &NullabilityDifference{
						Table1Nullable: false,
						Table2Nullable: true,
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "ALTER COLUMN \"sku\" SET NOT NULL")
	})

	t.Run("handles BigQuery nullability limitation", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectBigQuery, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "logs"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "logs_archive"},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "message",
					NullabilityDifference: &NullabilityDifference{
						Table1Nullable: false,
						Table2Nullable: true,
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		assert.Contains(t, statements[0], "/* BigQuery does not support changing nullability")
	})
}

func TestGenerateAlterStatements_MultipleChanges(t *testing.T) {
	t.Parallel()

	t.Run("combines multiple changes in PostgreSQL", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "employees"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "employees_temp"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "department",
					Type:        "VARCHAR(100)",
					Nullable:    false,
					MissingFrom: "employees_temp",
					TableName:   "employees",
				},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "salary",
					TypeDifference: &TypeDifference{
						Table1Type: "DECIMAL(12,2)",
						Table2Type: "INTEGER",
					},
				},
				{
					ColumnName: "middle_name",
					NullabilityDifference: &NullabilityDifference{
						Table1Nullable: true,
						Table2Nullable: false,
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)

		stmt := statements[0]
		assert.Contains(t, stmt, "ALTER TABLE \"employees_temp\"")
		assert.Contains(t, stmt, "ADD COLUMN \"department\" VARCHAR(100) NOT NULL")
		assert.Contains(t, stmt, "ALTER COLUMN \"salary\" TYPE DECIMAL(12,2)")
		assert.Contains(t, stmt, "ALTER COLUMN \"middle_name\" DROP NOT NULL")

		// Should be combined with commas
		assert.GreaterOrEqual(t, strings.Count(stmt, ","), 2)
	})

	t.Run("separates statements in BigQuery", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectBigQuery, false)
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "analytics"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "analytics_staging"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "user_id",
					Type:        "STRING",
					Nullable:    true,
					MissingFrom: "analytics_staging",
					TableName:   "analytics",
				},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "value",
					TypeDifference: &TypeDifference{
						Table1Type: "FLOAT64",
						Table2Type: "INT64",
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		// BigQuery should generate separate statements
		require.GreaterOrEqual(t, len(statements), 2)

		for _, stmt := range statements {
			assert.Contains(t, stmt, "ALTER TABLE `analytics_staging`")
		}
	})
}

func TestGenerateAlterStatements_Reverse(t *testing.T) {
	t.Parallel()

	t.Run("reverses direction of changes", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectPostgreSQL, true) // reverse = true
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "source"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "target"},
			},
			MissingColumns: []MissingColumn{
				{
					ColumnName:  "new_field",
					Type:        "TEXT",
					Nullable:    true,
					MissingFrom: "source",
					TableName:   "target",
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		// Should modify "source" table instead of "target"
		assert.Contains(t, statements[0], "ALTER TABLE \"source\"")
		assert.Contains(t, statements[0], "ADD COLUMN \"new_field\" TEXT")
	})

	t.Run("reverses type change direction", func(t *testing.T) {
		t.Parallel()
		gen := NewAlterStatementGenerator(DialectDuckDB, true) // reverse = true
		result := &SchemaComparisonResult{
			HasSchemaDifferences: true,
			Table1: &TableSummaryResult{
				Table: &Table{Name: "table_a"},
			},
			Table2: &TableSummaryResult{
				Table: &Table{Name: "table_b"},
			},
			ColumnDifferences: []ColumnDifference{
				{
					ColumnName: "id",
					TypeDifference: &TypeDifference{
						Table1Type: "INTEGER",
						Table2Type: "BIGINT",
					},
				},
			},
		}

		statements := gen.GenerateAlterStatements(result)
		require.Len(t, statements, 1)
		// Should use Table2Type (BIGINT) when reverse=true
		assert.Contains(t, statements[0], "ALTER TABLE \"table_a\"")
		assert.Contains(t, statements[0], "ALTER COLUMN \"id\" TYPE BIGINT")
	})
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dialect    DatabaseDialect
		identifier string
		expected   string
	}{
		{"PostgreSQL simple", DialectPostgreSQL, "users", `"users"`},
		{"PostgreSQL already quoted", DialectPostgreSQL, `"users"`, `"users"`},
		{"BigQuery simple", DialectBigQuery, "orders", "`orders`"},
		{"BigQuery already quoted", DialectBigQuery, "`orders`", "`orders`"},
		{"Snowflake simple", DialectSnowflake, "products", `"products"`},
		{"DuckDB simple", DialectDuckDB, "items", `"items"`},
		{"Generic simple", DialectGeneric, "table1", `"table1"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gen := NewAlterStatementGenerator(tt.dialect, false)
			result := gen.quoteIdentifier(tt.identifier)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetectDialectFromConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		conn1Type string
		conn2Type string
		expected  DatabaseDialect
	}{
		{"both PostgreSQL", "postgres", "postgres", DialectPostgreSQL},
		{"both PostgreSQL mixed case", "PostgreSQL", "POSTGRES", DialectPostgreSQL},
		{"both Snowflake", "snowflake", "snowflake", DialectSnowflake},
		{"both BigQuery", "bigquery", "bigquery", DialectBigQuery},
		{"both DuckDB", "duckdb", "duckdb", DialectDuckDB},
		{"postgres to snowflake", "postgres", "snowflake", DialectSnowflake},
		{"snowflake to postgres", "snowflake", "postgres", DialectPostgreSQL},
		{"bigquery to duckdb", "bigquery", "duckdb", DialectDuckDB},
		{"unknown to postgres", "unknown", "postgres", DialectPostgreSQL},
		{"postgres to unknown", "postgres", "unknown", DialectGeneric},
		{"both unknown", "mongodb", "redis", DialectGeneric},
		{"pg abbreviation", "pg", "pg", DialectPostgreSQL},
		{"bq abbreviation", "bq", "bq", DialectBigQuery},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := DetectDialectFromConnection(tt.conn1Type, tt.conn2Type)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCanCombineAlterClauses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dialect  DatabaseDialect
		expected bool
	}{
		{"PostgreSQL can combine", DialectPostgreSQL, true},
		{"DuckDB cannot combine", DialectDuckDB, false},
		{"Snowflake can combine", DialectSnowflake, true},
		{"BigQuery cannot combine", DialectBigQuery, false},
		{"Generic can combine", DialectGeneric, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gen := NewAlterStatementGenerator(tt.dialect, false)
			result := gen.canCombineAlterClauses()
			assert.Equal(t, tt.expected, result)
		})
	}
}
