package sqlparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenameTablesContractAcrossDialects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, query string
		mapping     map[string]string
		want        string
	}{
		{"one part destination clears qualifiers", "SELECT * FROM prod.raw.orders", map[string]string{"prod.raw.orders": "fixture"}, "SELECT * FROM fixture AS orders"},
		{"two part destination retains catalog", "SELECT * FROM prod.raw.orders", map[string]string{"prod.raw.orders": "dev.replacements"}, "SELECT * FROM prod.dev.replacements AS orders"},
		{"same leaf has no generated alias", "SELECT * FROM raw.orders", map[string]string{"raw.orders": "dev.orders"}, "SELECT * FROM dev.orders"},
		{"explicit alias retained", "SELECT o.id FROM raw.orders o", map[string]string{"raw.orders": "fixture"}, "SELECT o.id FROM fixture AS o"},
		{"schema match is exact", "SELECT * FROM other.orders", map[string]string{"raw.orders": "fixture"}, "SELECT * FROM other.orders"},
		{"catalog match is exact", "SELECT * FROM prod.raw.orders", map[string]string{"other.raw.orders": "fixture"}, "SELECT * FROM prod.raw.orders"},
		{"leaf mapping matches any schema", "SELECT * FROM raw.orders JOIN archive.orders a ON orders.id=a.id", map[string]string{"orders": "fixture"}, "SELECT * FROM fixture AS orders JOIN fixture AS a ON orders.id = a.id"},
		{"empty mapping still formats", "select  * from orders", map[string]string{}, "SELECT * FROM orders"},
		{"multiple statements", "SELECT * FROM raw.orders; SELECT * FROM raw.orders", map[string]string{"raw.orders": "fixture"}, "SELECT * FROM fixture AS orders; SELECT * FROM fixture AS orders"},
		{"case sensitive matching", "SELECT * FROM Orders JOIN orders ON Orders.id=orders.id", map[string]string{"orders": "new_orders"}, "SELECT * FROM Orders JOIN new_orders AS orders ON Orders.id = orders.id"},
	}
	for _, dialect := range contractDialects {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					want := tc.want
					if dialect == "oracle" {
						// Oracle omits AS for table aliases, unlike column aliases.
						want = strings.ReplaceAll(want, " AS ", " ")
					}
					if dialect == "redshift" && tc.name == "catalog match is exact" {
						want = `SELECT * FROM prod."raw".orders`
					}
					got, err := sharedSQLParser.RenameTables(tc.query, dialect, tc.mapping)
					require.NoError(t, err)
					require.Equal(t, want, got)
				})
			}
		})
	}
}

func TestRenameTablesContractPlatformFeatures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, dialect, query string
		mapping              map[string]string
		want                 string
	}{
		{"qualified columns collapse to leaf", "postgres", "SELECT prod.raw.orders.amount, raw.orders.id FROM prod.raw.orders", map[string]string{"prod.raw.orders": "fixture"}, "SELECT orders.amount, orders.id FROM fixture AS orders"},
		{"cte references are not exempt", "postgres", "WITH orders AS (SELECT * FROM raw.orders) SELECT * FROM orders", map[string]string{"orders": "fixture"}, "WITH orders AS (SELECT * FROM fixture AS orders) SELECT * FROM fixture AS orders"},
		// BigQuery quoted paths currently lose the destination schema. This is
		// deliberately not repaired by normalizing the expected SQL through SQLGlot.
		{"quoted three part path quirk", "bigquery", "SELECT * FROM `proj.raw.orders`", map[string]string{"proj.raw.orders": "dev.backup"}, "SELECT * FROM `proj.backup` AS orders"},
		{"quoted spaces", "postgres", `SELECT * FROM "raw"."Order Details" o`, map[string]string{"raw.Order Details": "dev.Order Copy"}, `SELECT * FROM dev."Order Copy" AS o`},
		{"generated alias is not quoted", "tsql", "SELECT * FROM [db].[dbo].[Order Details] WITH (NOLOCK)", map[string]string{"db.dbo.Order Details": "fixture"}, "SELECT * FROM [fixture] AS Order Details WITH (NOLOCK)"},
		{"write targets", "postgres", "INSERT INTO output SELECT * FROM input; DELETE FROM output WHERE id=1", map[string]string{"output": "new_output", "input": "fixture"}, "INSERT INTO new_output AS output SELECT * FROM fixture AS input; DELETE FROM new_output AS output WHERE id = 1"},
		{"literals and comments untouched", "postgres", "SELECT 'orders' AS s /* orders */ FROM orders", map[string]string{"orders": "fixture"}, "SELECT 'orders' AS s /* orders */ FROM fixture AS orders"},
		// Go's JSON encoder sorts map keys; mappings are then applied sequentially,
		// so a -> b -> c is a cascade, not a simultaneous replacement.
		{"cascading mappings", "postgres", "SELECT * FROM a JOIN b ON a.id=b.id", map[string]string{"a": "b", "b": "c"}, "SELECT * FROM c AS a JOIN c AS b ON a.id = b.id"},
		{"unnest", "bigquery", "SELECT e.id, x FROM `p.raw.events` e, UNNEST(e.items) x", map[string]string{"p.raw.events": "fixture"}, "SELECT e.id, x FROM `fixture` AS e CROSS JOIN UNNEST(e.items) AS x"},
		{"flatten inferred columns", "snowflake", "SELECT f.value FROM raw.events e, LATERAL FLATTEN(input => e.payload) f", map[string]string{"raw.events": "fixture"}, "SELECT f.value FROM fixture AS e, LATERAL FLATTEN(input => e.payload) AS f(SEQ, KEY, PATH, INDEX, VALUE, THIS)"},
		{"final prewhere", "clickhouse", "SELECT * FROM db.events FINAL PREWHERE active=1", map[string]string{"db.events": "fixture"}, "SELECT * FROM fixture AS events FINAL PREWHERE active = 1"},
		{"lateral view", "spark", "SELECT x FROM events LATERAL VIEW explode(items) AS x", map[string]string{"events": "fixture"}, "SELECT x FROM fixture AS events LATERAL VIEW EXPLODE(items) AS x"},
		{"delta version", "databricks", "SELECT * FROM catalog.raw.events VERSION AS OF 7", map[string]string{"catalog.raw.events": "fixture"}, "SELECT * FROM fixture VERSION AS OF 7 AS events"},
		{"unnest", "trino", "SELECT item FROM events CROSS JOIN UNNEST(items) AS u(item)", map[string]string{"events": "fixture"}, "SELECT item FROM fixture AS events CROSS JOIN UNNEST(items) AS u(item)"},
		{"index hints", "mysql", "SELECT * FROM orders USE INDEX (ix_id)", map[string]string{"orders": "fixture"}, "SELECT * FROM fixture AS orders USE INDEX (ix_id)"},
		{"bitmap", "doris", "SELECT BITMAP_COUNT(users) AS n FROM metrics", map[string]string{"metrics": "fixture"}, "SELECT BITMAP_COUNT(users) AS n FROM fixture AS metrics"},
		{"array", "starrocks", "SELECT ARRAY_LENGTH(tags) AS n FROM metrics", map[string]string{"metrics": "fixture"}, "SELECT ARRAY_LENGTH(tags) AS n FROM fixture AS metrics"},
		// RenameTables preserves inferred case for T-SQL, but not Fabric (where
		// ExtractSelect, a different operation, has the case-preservation hook).
		{"inferred column case", "fabric", "WITH a AS (SELECT MixedCase FROM dbo.orders) SELECT * FROM a", map[string]string{"dbo.orders": "fixture"}, "WITH a AS (SELECT MixedCase AS mixedcase FROM fixture AS orders) SELECT * FROM a"},
		{"inferred column case", "tsql", "WITH a AS (SELECT MixedCase FROM dbo.orders) SELECT * FROM a", map[string]string{"dbo.orders": "fixture"}, "WITH a AS (SELECT MixedCase AS MixedCase FROM fixture AS orders) SELECT * FROM a"},
	}
	for _, tc := range cases {
		t.Run(tc.dialect+"/"+tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.RenameTables(tc.query, tc.dialect, tc.mapping)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRenameTablesContractInvalidInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, query, dialect, errorText string }{
		{"empty", "", "postgres", "object has no attribute 'find_all'"},
		{"comment only", "-- comment", "postgres", "object has no attribute 'find_all'"},
		{"extra separator", "SELECT 1;;", "postgres", "object has no attribute 'find_all'"},
		{"malformed", "SELECT * FROM", "postgres", "Expected table name"},
		{"unknown dialect", "SELECT 1", "not-a-dialect", "Unknown dialect"},
		// Unlike other operations, RenameTables does not normalize vertica.
		{"vertica alias unsupported", "SELECT 1", "vertica", "Unknown dialect"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.RenameTables(tc.query, tc.dialect, map[string]string{})
			require.ErrorContains(t, err, tc.errorText)
			require.Empty(t, got)
		})
	}
	t.Run("nil mapping differs from empty mapping", func(t *testing.T) {
		got, err := sharedSQLParser.RenameTables("SELECT * FROM orders", "postgres", nil)
		require.ErrorContains(t, err, "object has no attribute 'items'")
		require.Empty(t, got)
	})
}
