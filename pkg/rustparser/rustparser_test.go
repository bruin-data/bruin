package rustparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRustSQLParser_UsedTables(t *testing.T) {
	t.Parallel()
	parser := NewRustSQLParser()

	tests := []struct {
		name    string
		sql     string
		dialect string
		want    []string
		wantErr bool
	}{
		{
			name:    "simple join",
			sql:     "SELECT a, b FROM table1 JOIN table2 ON table1.a = table2.b",
			dialect: "bigquery",
			want:    []string{"table1", "table2"},
		},
		{
			name:    "bigquery backtick-quoted tables",
			sql:     "SELECT a FROM `project.dataset.table1` JOIN `project.dataset.table2` ON a = b",
			dialect: "bigquery",
			want:    []string{"project.dataset.table1", "project.dataset.table2"},
		},
		{
			name:    "subquery",
			sql:     "SELECT * FROM (SELECT a FROM inner_table) sub JOIN outer_table ON sub.a = outer_table.a",
			dialect: "bigquery",
			want:    []string{"inner_table", "outer_table"},
		},
		{
			name:    "CTE should not be in tables",
			sql:     "WITH cte AS (SELECT a FROM source_table) SELECT a FROM cte",
			dialect: "bigquery",
			want:    []string{"source_table"},
		},
		{
			name:    "snowflake dialect",
			sql:     "SELECT a FROM schema1.table1",
			dialect: "snowflake",
			want:    []string{"schema1.table1"},
		},
		{
			name:    "postgres dialect",
			sql:     "SELECT a FROM public.users JOIN public.orders ON users.id = orders.user_id",
			dialect: "postgres",
			want:    []string{"public.orders", "public.users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tables, err := parser.UsedTables(tt.sql, tt.dialect)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, tables)
		})
	}
}

func TestRustSQLParser_IsSingleSelectQuery(t *testing.T) {
	t.Parallel()
	parser := NewRustSQLParser()

	tests := []struct {
		name    string
		sql     string
		dialect string
		want    bool
	}{
		{
			name:    "simple select",
			sql:     "SELECT 1",
			dialect: "bigquery",
			want:    true,
		},
		{
			name:    "CTE select",
			sql:     "WITH cte AS (SELECT 1) SELECT * FROM cte",
			dialect: "bigquery",
			want:    true,
		},
		{
			name:    "multiple statements",
			sql:     "SELECT 1; SELECT 2",
			dialect: "bigquery",
			want:    false,
		},
		{
			name:    "create table",
			sql:     "CREATE TABLE foo (id INT)",
			dialect: "bigquery",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parser.IsSingleSelectQuery(tt.sql, tt.dialect)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRustSQLParser_AddLimit(t *testing.T) {
	t.Parallel()
	parser := NewRustSQLParser()

	result, err := parser.AddLimit("SELECT a FROM table1", 10, "bigquery")
	require.NoError(t, err)
	assert.Contains(t, result, "LIMIT")
	assert.Contains(t, result, "10")
}

func TestRustSQLParser_RenameTables(t *testing.T) {
	t.Parallel()
	parser := NewRustSQLParser()

	result, err := parser.RenameTables("SELECT a FROM old_table", "bigquery", map[string]string{
		"old_table": "new_table",
	})
	require.NoError(t, err)
	assert.Contains(t, result, "new_table")
	assert.NotContains(t, result, "old_table")
}

func TestRustSQLParser_ColumnLineage(t *testing.T) {
	t.Parallel()
	parser := NewRustSQLParser()

	lineage, err := parser.ColumnLineage(
		"SELECT a, b FROM table1",
		"bigquery",
		Schema{},
	)
	require.NoError(t, err)
	assert.Len(t, lineage.Columns, 2)
	assert.Equal(t, "a", lineage.Columns[0].Name)
	assert.Equal(t, "b", lineage.Columns[1].Name)
}

func TestRustSQLParser_ColumnLineageTooLong(t *testing.T) {
	t.Parallel()
	parser := NewRustSQLParserWithConfig(10)

	lineage, err := parser.ColumnLineage(
		"SELECT a, b FROM table1 WHERE a > 1 AND b < 100",
		"bigquery",
		Schema{},
	)
	require.NoError(t, err)
	assert.Len(t, lineage.Errors, 1)
	assert.Contains(t, lineage.Errors[0], "too long")
}
