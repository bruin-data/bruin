package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectFromCTEContract(t *testing.T) {
	query := "WITH a AS (SELECT 1 AS id), b AS (SELECT id + 1 AS id FROM a) SELECT id * 9 AS marker FROM b"
	want := "WITH a AS (SELECT 1 AS id), b AS (SELECT id + 1 AS id FROM a) SELECT * FROM a"
	for _, dialect := range contractDialects {
		t.Run("dialect/"+dialect, func(t *testing.T) {
			got, err := sharedSQLParser.SelectFromCTE(query, dialect, "a")
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
	for _, tc := range []struct{ name, query, cte, want string }{
		{"case insensitive", "WITH Mixed AS (SELECT 1 AS id) SELECT id * 9 FROM Mixed", "mIxEd", "WITH Mixed AS (SELECT 1 AS id) SELECT * FROM Mixed"},
		// Rendering the target without quoting changes Odd Name into table Odd
		// with alias Name. Characterize the bug rather than reparsing expectations.
		{"quoted", "WITH \"Odd Name\" AS (SELECT 1 AS id) SELECT id * 9 FROM \"Odd Name\"", "Odd Name", "WITH \"Odd Name\" AS (SELECT 1 AS id) SELECT * FROM Odd AS Name"},
		{"recursive", "WITH RECURSIVE walk AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM walk WHERE n < 2) SELECT n * 9 FROM walk", "walk", "WITH RECURSIVE walk AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM walk WHERE n < 2) SELECT * FROM walk"},
		// Only the top-level WITH is considered; nested CTEs are not assertion targets.
		{"top level over nested", "WITH outer_cte AS (WITH inner_cte AS (SELECT 1 AS id) SELECT * FROM inner_cte) SELECT id * 9 FROM outer_cte", "outer_cte", "WITH outer_cte AS (WITH inner_cte AS (SELECT 1 AS id) SELECT * FROM inner_cte) SELECT * FROM outer_cte"},
		{"last case-insensitive match wins", "WITH a AS (SELECT 1 AS x), A AS (SELECT 2 AS x) SELECT * FROM a", "a", "WITH a AS (SELECT 1 AS x), A AS (SELECT 2 AS x) SELECT * FROM A"},
		{"outer clauses discarded", "WITH a AS (SELECT 1 AS x) SELECT * FROM a ORDER BY x LIMIT 2", "a", "WITH a AS (SELECT 1 AS x) SELECT * FROM a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.SelectFromCTE(tc.query, "duckdb", tc.cte)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct{ name, query, dialect, cte, fragment string }{
		{"empty", "", "duckdb", "a", "cannot parse query"},
		{"malformed", "SELECT * FROM", "duckdb", "a", "Expected table name"},
		{"no with", "SELECT 1", "duckdb", "a", "defines no CTEs"},
		{"unknown", "WITH a AS (SELECT 1) SELECT * FROM a", "duckdb", "b", "available: ['a']"},
		{"nested unavailable", "SELECT * FROM (WITH a AS (SELECT 1) SELECT * FROM a) AS x", "duckdb", "a", "defines no CTEs"},
		{"invalid dialect", "WITH a AS (SELECT 1) SELECT * FROM a", "not-a-dialect", "a", "Unknown dialect"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sharedSQLParser.SelectFromCTE(tc.query, tc.dialect, tc.cte)
			require.ErrorContains(t, err, tc.fragment)
		})
	}
}

func TestPrependCTEsContract(t *testing.T) {
	query := "WITH e AS (SELECT id FROM seed) SELECT id * 9 AS marker FROM e"
	want := map[string]string{}
	for _, d := range contractDialects {
		want[d] = "WITH seed AS (SELECT CAST(1 AS BIGINT) AS id), e AS (SELECT id FROM seed) SELECT id * 9 AS marker FROM e"
	}
	want["bigquery"] = "WITH seed AS (SELECT CAST(1 AS INT64) AS id), e AS (SELECT id FROM seed) SELECT id * 9 AS marker FROM e"
	want["clickhouse"] = "WITH seed AS (SELECT CAST(1 AS Int64) AS id), e AS (SELECT id FROM seed) SELECT id * 9 AS marker FROM e"
	want["mysql"] = "WITH seed AS (SELECT CAST(1 AS SIGNED) AS id), e AS (SELECT id FROM seed) SELECT id * 9 AS marker FROM e"
	want["oracle"] = "WITH seed AS (SELECT CAST(1 AS INT) AS id), e AS (SELECT id FROM seed) SELECT id * 9 AS marker FROM e"
	want["fabric"] = "WITH seed AS (SELECT CAST(1 AS BIGINT) AS id), e AS (SELECT id AS id FROM seed) SELECT id * 9 AS marker FROM e"
	want["tsql"] = want["fabric"]
	for _, dialect := range contractDialects {
		t.Run("dialect/"+dialect, func(t *testing.T) {
			got, err := sharedSQLParser.PrependCTEs(query, dialect, []CTE{{Name: "seed", Query: "SELECT CAST(1 AS BIGINT) AS id"}})
			require.NoError(t, err)
			require.Equal(t, want[dialect], got)
		})
	}
	for _, tc := range []struct {
		name, query string
		ctes        []CTE
		want        string
	}{
		{"order", "SELECT * FROM a JOIN b ON a.id = b.id", []CTE{{"a", "SELECT 1 AS id"}, {"b", "SELECT 2 AS id"}}, "WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id) SELECT * FROM a JOIN b ON a.id = b.id"},
		{"recursive", "WITH RECURSIVE walk AS (SELECT id FROM nodes UNION ALL SELECT id + 1 FROM walk) SELECT * FROM walk", []CTE{{"nodes", "SELECT 1 AS id"}}, "WITH RECURSIVE nodes AS (SELECT 1 AS id), walk AS (SELECT id FROM nodes UNION ALL SELECT id + 1 FROM walk) SELECT * FROM walk"},
		// Name collisions are not rejected; both definitions are emitted in order.
		{"collision", "WITH a AS (SELECT 2 AS id) SELECT * FROM a", []CTE{{"a", "SELECT 1 AS id"}}, "WITH a AS (SELECT 1 AS id), a AS (SELECT 2 AS id) SELECT * FROM a"},
		{"no ctes still reformats", "select  1", nil, "SELECT 1"},
		// The existing-WITH code path auto-quotes names with spaces, unlike
		// the no-WITH path (whose rejection is tested below).
		{"name with space and existing with", "WITH x AS (SELECT 2) SELECT * FROM x", []CTE{{"Odd Name", "SELECT 1"}}, `WITH "Odd Name" AS (SELECT 1), x AS (SELECT 2) SELECT * FROM x`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.PrependCTEs(tc.query, "duckdb", tc.ctes)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct {
		name, query, dialect string
		ctes                 []CTE
		fragment             string
	}{
		{"empty", "", "duckdb", []CTE{{"a", "SELECT 1"}}, "cannot parse query"},
		{"malformed outer", "SELECT * FROM", "duckdb", []CTE{{"a", "SELECT 1"}}, "Expected table name"},
		{"malformed cte", "SELECT 1", "duckdb", []CTE{{"a", "SELECT * FROM"}}, "Expected table name"},
		{"quoted name rejected", "SELECT 1", "duckdb", []CTE{{"Odd Name", "SELECT 1"}}, "Failed to parse 'Odd Name'"},
		{"multiple statements", "SELECT 1; SELECT 2", "duckdb", []CTE{{"a", "SELECT 3"}}, "object has no attribute 'with_'"},
		{"invalid dialect", "SELECT 1", "not-a-dialect", nil, "Unknown dialect"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sharedSQLParser.PrependCTEs(tc.query, tc.dialect, tc.ctes)
			require.ErrorContains(t, err, tc.fragment)
		})
	}
}
