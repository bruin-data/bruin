package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsedTablesContractAcrossDialects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, query string
		want        []string
	}{
		{"no tables", "SELECT 'FROM fake; JOIN imaginary' AS text", []string{}},
		{"empty", "", []string{}},
		{"comments and separators", "; /* FROM fake */ ; -- JOIN imaginary\n", []string{}},
		{"deduplicated and sorted", "SELECT * FROM z.orders a JOIN a.customers b ON a.id=b.id JOIN z.orders c ON c.id=a.id", []string{"a.customers", "z.orders"}},
		{"all statements", "SELECT * FROM z.orders; SELECT * FROM a.customers;;", []string{"a.customers", "z.orders"}},
		{"cte and qualified namesake", "WITH orders AS (SELECT * FROM raw.orders) SELECT * FROM orders JOIN archive.orders a ON orders.id=a.id", []string{"archive.orders", "raw.orders"}},
		{"unused cte still counts", "WITH unused AS (SELECT * FROM hidden) SELECT * FROM visible", []string{"hidden", "visible"}},
		{"scalar and correlated subqueries", "SELECT (SELECT MAX(amount) FROM prices) FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.id=o.id)", []string{"orders", "payments", "prices"}},
		{"set operation", "SELECT id FROM left_rows EXCEPT DISTINCT SELECT id FROM right_rows", []string{"left_rows", "right_rows"}},
		// UsedTables includes write targets, not just tables supplying rows.
		{"ctas includes destination", "CREATE TABLE target_rows AS SELECT * FROM source_rows", []string{"source_rows", "target_rows"}},
		{"insert includes destination", "INSERT INTO target_rows SELECT * FROM source_rows", []string{"source_rows", "target_rows"}},
		{"delete includes destination", "DELETE FROM target_rows WHERE id IN (SELECT id FROM source_rows)", []string{"source_rows", "target_rows"}},
	}
	for _, dialect := range contractDialects {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got, err := sharedSQLParser.UsedTables(tc.query, dialect)
					require.NoError(t, err)
					require.Equal(t, tc.want, got)
				})
			}
			t.Run("malformed is not an empty dependency set", func(t *testing.T) {
				got, err := sharedSQLParser.UsedTables("SELECT * FROM", dialect)
				require.ErrorContains(t, err, "Expected table name")
				require.Nil(t, got)
			})
		})
	}
}

func TestUsedTablesContractPlatformFeatures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, dialect, query string
		want                 []string
	}{
		{"unnest", "bigquery", "SELECT e.id, x.sku FROM `proj.raw.events` e, UNNEST(e.items) x", []string{"proj.raw.events"}},
		{"wildcard", "bigquery", "SELECT * FROM `proj.raw.events_*` WHERE _TABLE_SUFFIX > '20240101'", []string{"proj.raw.events_*"}},
		{"snapshot", "bigquery", "SELECT * FROM `proj.raw.events` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)", []string{"proj.raw.events"}},
		// Function names (or empty names) are currently reported as tables; the
		// string containing the remote SQL is not parsed recursively.
		{"external query function", "bigquery", "SELECT * FROM EXTERNAL_QUERY('proj.us.connection', 'SELECT * FROM remote_table')", []string{"EXTERNAL_QUERY"}},
		{"flatten", "snowflake", "SELECT f.value FROM raw.events e, LATERAL FLATTEN(input => e.payload) f", []string{"raw.events"}},
		{"time travel", "snowflake", "SELECT * FROM raw.events AT (TIMESTAMP => '2024-01-01'::TIMESTAMP)", []string{"raw.events"}},
		{"stage", "snowflake", "SELECT $1 FROM @raw.stage/path (FILE_FORMAT => 'csv')", []string{"@raw.stage/path"}},
		{"pivot", "snowflake", "SELECT * FROM sales PIVOT(SUM(amount) FOR quarter IN ('Q1','Q2'))", []string{"sales"}},
		{"json lateral", "postgres", "SELECT j.value FROM raw.events e CROSS JOIN LATERAL jsonb_array_elements(e.payload) j(value)", []string{"raw.events"}},
		{"generate series empty name", "postgres", "SELECT * FROM generate_series(1, 7) AS n", []string{""}},
		{"only parent", "postgres", "SELECT * FROM ONLY public.parent", []string{"public.parent"}},
		{"recursive self reference excluded", "postgres", "WITH RECURSIVE walk AS (SELECT id FROM nodes UNION ALL SELECT n.id FROM nodes n JOIN walk w ON n.parent=w.id) SELECT * FROM walk", []string{"nodes"}},
		// CTE exclusion is currently global to the statement, not scope-aware:
		// the real outer table named shadow disappears as well.
		{"nested cte shadows real outer table", "postgres", "SELECT * FROM shadow WHERE EXISTS (WITH shadow AS (SELECT * FROM real_source) SELECT * FROM shadow)", []string{"real_source"}},
		{"json table empty name", "mysql", "SELECT jt.id FROM events e, JSON_TABLE(e.payload, '$[*]' COLUMNS(id INT PATH '$.id')) jt", []string{"", "events"}},
		{"partition and index hint", "mysql", "SELECT * FROM orders PARTITION (p2024) USE INDEX (ix_id)", []string{"orders"}},
		{"dual retained", "mysql", "SELECT 1 FROM DUAL", []string{"DUAL"}},
		{"parquet empty name", "duckdb", "SELECT * FROM read_parquet('data/*.parquet')", []string{""}},
		{"from first", "duckdb", "FROM events SELECT * EXCLUDE(payload)", []string{"events"}},
		{"simplified pivot", "duckdb", "PIVOT sales ON quarter USING SUM(amount) GROUP BY region", []string{"sales"}},
		{"positional join", "duckdb", "SELECT * FROM left_rows POSITIONAL JOIN right_rows", []string{"left_rows", "right_rows"}},
		{"array join", "clickhouse", "SELECT id, x FROM events ARRAY JOIN items AS x", []string{"events"}},
		{"remote function name", "clickhouse", "SELECT * FROM remote('host', 'db', 'events')", []string{"remote"}},
		{"final prewhere limit by", "clickhouse", "SELECT * FROM db.events FINAL PREWHERE active = 1 LIMIT 1 BY id", []string{"db.events"}},
		{"asof join", "clickhouse", "SELECT * FROM ticks ASOF LEFT JOIN quotes ON ticks.symbol = quotes.symbol AND ticks.ts >= quotes.ts", []string{"quotes", "ticks"}},
		{"lateral view", "spark", "SELECT x FROM events LATERAL VIEW explode(items) AS x", []string{"events"}},
		{"file path", "spark", "SELECT * FROM parquet.`/data/events.parquet`", []string{"parquet./data/events.parquet"}},
		{"delta version", "databricks", "SELECT * FROM catalog.raw.events VERSION AS OF 7", []string{"catalog.raw.events"}},
		{"qualify", "databricks", "SELECT * FROM events QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY ts DESC)=1", []string{"events"}},
		{"unnest ordinality", "trino", "SELECT * FROM events CROSS JOIN UNNEST(items) WITH ORDINALITY AS u(item, n)", []string{"events"}},
		{"tablesample", "trino", "SELECT * FROM raw.events TABLESAMPLE BERNOULLI (13)", []string{"raw.events"}},
		{"partition metadata", "athena", `SELECT * FROM "raw"."events$partitions"`, []string{"raw.events$partitions"}},
		{"unnest ordinality", "athena", "SELECT * FROM events CROSS JOIN UNNEST(items) WITH ORDINALITY AS u(item, n)", []string{"events"}},
		{"super iteration", "redshift", "SELECT e.id, item FROM raw.events e, e.payload.items item", []string{"raw.events"}},
		{"unload is opaque", "redshift", "UNLOAD ('SELECT * FROM orders') TO 's3://bucket/path' IAM_ROLE 'arn:aws:iam::123:role/x'", []string{}},
		{"hierarchy", "oracle", "SELECT id, LEVEL FROM nodes START WITH parent IS NULL CONNECT BY PRIOR id = parent", []string{"nodes"}},
		{"database link", "oracle", "SELECT * FROM orders@warehouse", []string{"orders@warehouse"}},
		{"temp tables and variables lose sigils", "tsql", "SELECT * FROM #temp JOIN @rows r ON #temp.id=r.id", []string{"rows", "temp"}},
		{"openjson apply", "tsql", "SELECT j.value FROM dbo.events e CROSS APPLY OPENJSON(e.payload) j", []string{"dbo.events"}},
		{"temporal table", "tsql", "SELECT * FROM dbo.events FOR SYSTEM_TIME ALL", []string{"dbo.events"}},
		// The last USE applies retroactively, even to references before any USE.
		{"last use wins globally", "tsql", "SELECT * FROM before_use; USE firstdb; SELECT * FROM orders; USE lastdb; SELECT * FROM other..items", []string{"lastdb.dbo.before_use", "lastdb.dbo.orders", "other.dbo.items"}},
		{"brackets and hints", "fabric", "SELECT * FROM [lake].[dbo].[Order Details] WITH (NOLOCK)", []string{"lake.dbo.Order Details"}},
		{"openjson apply", "fabric", "SELECT j.value FROM dbo.events e CROSS APPLY OPENJSON(e.payload) j", []string{"dbo.events"}},
		{"explode split", "doris", "SELECT x FROM events LATERAL VIEW EXPLODE_SPLIT(tags, ',') e AS x", []string{"events"}},
		{"unnest", "starrocks", "SELECT * FROM events, UNNEST(items) AS x", []string{"events"}},
		{"vertica alias", "vertica", "SELECT * FROM ONLY public.parent", []string{"public.parent"}},
		{"generic dialect", "", "SELECT * FROM raw.orders", []string{"raw.orders"}},
	}
	for _, tc := range cases {
		t.Run(tc.dialect+"/"+tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.UsedTables(tc.query, tc.dialect)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct{ name, query, dialect, errorText string }{
		{"unknown dialect", "SELECT 1", "not-a-dialect", "Unknown dialect"},
		{"unterminated identifier", "SELECT * FROM `oops", "bigquery", "Error tokenizing"},
		{"reserved output keyword", "SELECT * FROM output", "tsql", "Expected table name"},
		{"except needs qualifier", "SELECT id FROM a EXCEPT SELECT id FROM b", "bigquery", "Expected DISTINCT or ALL"},
		{"unsupported flashback syntax", "SELECT * FROM orders AS OF TIMESTAMP TO_TIMESTAMP('2024-01-01', 'YYYY-MM-DD')", "oracle", "Invalid expression"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.UsedTables(tc.query, tc.dialect)
			require.ErrorContains(t, err, tc.errorText)
			require.Nil(t, got)
		})
	}
}
