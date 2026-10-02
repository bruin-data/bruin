package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractSelectContract(t *testing.T) {
	base := "WITH a AS (SELECT AccountDimId FROM dbo.DimAccount) SELECT AccountDimId FROM a"
	for _, dialect := range contractDialects {
		t.Run("dialect/"+dialect, func(t *testing.T) {
			want := base
			if dialect == "fabric" {
				want = "WITH a AS (SELECT AccountDimId AS AccountDimId FROM dbo.DimAccount) SELECT AccountDimId FROM a"
			}
			if dialect == "tsql" {
				want = "WITH a AS (SELECT AccountDimId AS accountdimid FROM dbo.DimAccount) SELECT AccountDimId FROM a"
			}
			for _, tc := range []struct{ name, query string }{
				{"plain select", base},
				{"create table", "CREATE TABLE dest AS " + base},
				{"insert select", "INSERT INTO dest " + base},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := sharedSQLParser.ExtractSelect(tc.query, dialect)
					require.NoError(t, err)
					require.Equal(t, want, got)
				})
			}
		})
	}
	for _, tc := range []struct{ name, query, dialect, want string }{
		{"view", "CREATE OR REPLACE VIEW analytics.v AS SELECT id FROM orders", "duckdb", "SELECT id FROM orders"},
		{"ctas", "CREATE TABLE analytics.t AS SELECT id FROM orders", "postgres", "SELECT id FROM orders"},
		{"insert", "INSERT INTO analytics.t SELECT id FROM orders", "duckdb", "SELECT id FROM orders"},
		{"select into", "SELECT id INTO archive FROM orders", "tsql", "SELECT id FROM orders"},
		{"union", "SELECT 1 AS n UNION ALL SELECT 2 AS n", "duckdb", "SELECT 1 AS n UNION ALL SELECT 2 AS n"},
		{"materialized view", "CREATE MATERIALIZED VIEW dest AS SELECT * FROM source", "postgres", "SELECT * FROM source"},
		{"fabric derived column case", "SELECT x.MixedCase FROM (SELECT MixedCase FROM dbo.t) x", "fabric", "SELECT x.MixedCase FROM (SELECT MixedCase AS MixedCase FROM dbo.t) AS x"},
		{"fabric named derived output bypasses case preservation", "WITH a(x) AS (SELECT MixedCase FROM dbo.t) SELECT x FROM a", "fabric", "WITH a(x) AS (SELECT MixedCase FROM dbo.t) SELECT x FROM a"},
		// The WITH belongs to INSERT and is currently lost during unwrapping.
		{"with on insert is lost", "WITH x AS (SELECT id FROM source) INSERT INTO dest SELECT * FROM x", "postgres", "SELECT * FROM x"},
		// Only a top-level INTO is removed. This is a known safety limitation,
		// not a promise that the returned SQL is safe to execute.
		{"nested into is retained", "SELECT * FROM (SELECT id INTO hidden FROM t) x", "duckdb", "SELECT * FROM (CREATE TABLE hidden AS SELECT id FROM t) AS x"},
		{"locking clause retained", "SELECT * FROM t FOR UPDATE", "postgres", "SELECT * FROM t FOR UPDATE"},
		{"materialized cte retained", "WITH x AS MATERIALIZED (SELECT 1 AS id) SELECT id FROM x", "postgres", "WITH x AS MATERIALIZED (SELECT 1 AS id) SELECT id FROM x"},
		{"named ctas output columns discarded with wrapper", "CREATE TABLE dest (a, b) AS SELECT x, y FROM src", "postgres", "SELECT x, y FROM src"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.ExtractSelect(tc.query, tc.dialect)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct{ name, query, dialect, errorText string }{
		{"empty", "", "duckdb", "cannot parse query"},
		{"malformed", "SELECT * FROM", "duckdb", contractSelectStarFromError},
		{"ddl no query", "CREATE TABLE t (id BIGINT)", "duckdb", "asset has no SELECT to unit test"},
		{"delete", "DELETE FROM t WHERE id IN (SELECT id FROM x)", "duckdb", "asset is not a SELECT and has no inner SELECT to unit test"},
		{"insert values", "INSERT INTO dest VALUES (1)", "postgres", "asset is not a SELECT and has no inner SELECT to unit test"},
		{"write cte", "WITH gone AS (DELETE FROM t RETURNING id) SELECT * FROM gone", "postgres", "asset contains a write statement and cannot be unit tested read-only"},
		{"multiple statements form block", "SELECT 1; SELECT 2", "duckdb", "asset is not a SELECT and has no inner SELECT to unit test"},
		{"invalid dialect", "SELECT 1", "not-a-dialect", "Unknown dialect 'not-a-dialect'."},
		{"invalid dialect wins over malformed query", "SELECT * FROM", "not-a-dialect", "Unknown dialect 'not-a-dialect'."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.ExtractSelect(tc.query, tc.dialect)
			require.EqualError(t, err, tc.errorText)
			require.Equal(t, "", got)
		})
	}
}
