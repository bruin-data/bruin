package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLParserContractLifecycleAndErrorRecovery(t *testing.T) {
	t.Parallel()
	parser, err := NewSQLParserCached()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	require.NoError(t, parser.Close()) // unused instances need not extract or start Python

	for range 2 {
		// UsedTables starts lazily, including after Close. Start remains idempotent.
		tables, err := parser.UsedTables("SELECT * FROM first_table", "postgres")
		require.NoError(t, err)
		require.Equal(t, []string{"first_table"}, tables)
		require.NoError(t, parser.Start())
		require.NoError(t, parser.Start())

		query, err := parser.RenameTables("SELECT * FROM t", "postgres", nil)
		require.EqualError(t, err, "'NoneType' object has no attribute 'items'")
		require.Empty(t, query)
		// A command that raises in Python must not desynchronize the next response.
		tables, err = parser.UsedTables("SELECT 'π\n; FROM fake' FROM actual_table", "postgres")
		require.NoError(t, err)
		require.Equal(t, []string{"actual_table"}, tables)
		require.NoError(t, parser.Close())
		require.NoError(t, parser.Close())
	}
}

func TestSQLParserContractRequestIsolation(t *testing.T) {
	t.Parallel()
	// Reuse the same SQL across schemas and dialects, then repeat the original.
	// A replacement's cache must not be keyed by SQL alone or retain old schemas.
	for _, tc := range []struct {
		name, dialect, typ string
		want               *Lineage
	}{
		{"int", "postgres", "int", &Lineage{[]ColumnLineage{{"id", uc("id", "t"), "INT"}}, []ColumnLineage{}, []string{}}},
		{"text", "postgres", "text", &Lineage{[]ColumnLineage{{"id", uc("id", "t"), "TEXT"}}, []ColumnLineage{}, []string{}}},
		{"uppercase dialect", "snowflake", "int", &Lineage{[]ColumnLineage{{"ID", uc("ID", "T"), "INT"}}, []ColumnLineage{}, []string{}}},
		{"original again", "postgres", "int", &Lineage{[]ColumnLineage{{"id", uc("id", "t"), "INT"}}, []ColumnLineage{}, []string{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := Schema{"t": {"id": tc.typ}}
			got, err := sharedSQLParser.ColumnLineage("SELECT id FROM t", tc.dialect, schema)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, Schema{"t": {"id": tc.typ}}, schema)
		})
	}
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"USE reporting; SELECT * FROM orders", []string{"reporting.dbo.orders"}},
		{"SELECT * FROM orders", []string{"orders"}},
	} {
		got, err := sharedSQLParser.UsedTables(tc.query, "tsql")
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestSQLParserContractSameInputRewriteIsolation(t *testing.T) {
	t.Parallel()
	const query = "SELECT CURRENT_TIMESTAMP FROM t"
	assertOriginalTables := func() {
		tables, err := sharedSQLParser.UsedTables(query, "postgres")
		require.NoError(t, err)
		require.Equal(t, []string{"t"}, tables)
	}

	rename := map[string]string{"t": "x"}
	got, err := sharedSQLParser.RenameTables(query, "postgres", rename)
	require.NoError(t, err)
	require.Equal(t, "SELECT CURRENT_TIMESTAMP FROM x AS t", got)
	require.Equal(t, map[string]string{"t": "x"}, rename)
	assertOriginalTables()
	got, err = sharedSQLParser.RenameTables(query, "postgres", map[string]string{"t": "y"})
	require.NoError(t, err)
	require.Equal(t, "SELECT CURRENT_TIMESTAMP FROM y AS t", got)
	assertOriginalTables()
	got, err = sharedSQLParser.RenameTables(query, "postgres", map[string]string{})
	require.NoError(t, err)
	require.Equal(t, query, got)
	assertOriginalTables()

	for _, tc := range []struct {
		limit int
		want  string
	}{{1, query + " LIMIT 1"}, {2, query + " LIMIT 2"}, {1, query + " LIMIT 1"}} {
		got, err = sharedSQLParser.AddLimit(query, tc.limit, "postgres")
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
		assertOriginalTables()
	}

	got, err = sharedSQLParser.FreezeTime(query, "postgres", "2024-01-02T03:04:05Z")
	require.NoError(t, err)
	require.Equal(t, "SELECT CAST('2024-01-02T03:04:05Z' AS TIMESTAMP) FROM t", got)
	assertOriginalTables()
	got, err = sharedSQLParser.FreezeTime(query, "postgres", "2025-06-07T08:09:10Z")
	require.NoError(t, err)
	require.Equal(t, "SELECT CAST('2025-06-07T08:09:10Z' AS TIMESTAMP) FROM t", got)
	assertOriginalTables()
	got, err = sharedSQLParser.FreezeTime(query, "postgres", "2024-01-02T03:04:05Z")
	require.NoError(t, err)
	require.Equal(t, "SELECT CAST('2024-01-02T03:04:05Z' AS TIMESTAMP) FROM t", got)
	assertOriginalTables()

	ctes := []CTE{{Name: "fixture", Query: "SELECT 1 AS id"}}
	got, err = sharedSQLParser.PrependCTEs(query, "postgres", ctes)
	require.NoError(t, err)
	require.Equal(t, "WITH fixture AS (SELECT 1 AS id) SELECT CURRENT_TIMESTAMP FROM t", got)
	require.Equal(t, []CTE{{Name: "fixture", Query: "SELECT 1 AS id"}}, ctes)
	assertOriginalTables()
	got, err = sharedSQLParser.PrependCTEs(query, "postgres", []CTE{})
	require.NoError(t, err)
	require.Equal(t, query, got)
	assertOriginalTables()
}

func TestSQLParserContractLineageLengthCountsBytes(t *testing.T) {
	t.Parallel()
	parser, err := NewSQLParserCached()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	require.NoError(t, parser.Start())
	parser.MaxQueryLength = len("SELECT 'a'")
	got, err := parser.ColumnLineage("SELECT 'a'", "postgres", Schema{})
	require.NoError(t, err)
	require.Equal(t, &Lineage{[]ColumnLineage{{"a", []UpstreamColumn{}, "VARCHAR"}}, []ColumnLineage{}, []string{}}, got)
	// Same rune count, one more byte. Rejection happens before dialect/schema validation.
	got, err = parser.ColumnLineage("SELECT 'é'", "not-a-dialect", nil)
	require.NoError(t, err)
	require.Equal(t, &Lineage{[]ColumnLineage{}, []ColumnLineage{}, []string{"query is too long skipping column lineage analysis"}}, got)
	// The bound applies only to lineage, not other parsing operations.
	tables, err := parser.UsedTables("SELECT * FROM a_long_table_name", "postgres")
	require.NoError(t, err)
	require.Equal(t, []string{"a_long_table_name"}, tables)
}
