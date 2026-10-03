package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddLimitContract(t *testing.T) {
	t.Parallel()
	want := map[string]string{
		"athena": "SELECT id FROM orders OFFSET 2 LIMIT 7", "bigquery": "SELECT id FROM orders LIMIT 7 OFFSET 2",
		"clickhouse": "SELECT id FROM orders LIMIT 7 OFFSET 2", "databricks": "SELECT id FROM orders LIMIT 7 OFFSET 2",
		"doris": "SELECT id FROM orders LIMIT 7 OFFSET 2", "duckdb": "SELECT id FROM orders LIMIT 7 OFFSET 2",
		"fabric": "SELECT id FROM orders ORDER BY (SELECT NULL) OFFSET 2 ROWS FETCH FIRST 7 ROWS ONLY",
		"mysql":  "SELECT id FROM orders LIMIT 7 OFFSET 2", "oracle": "SELECT id FROM orders OFFSET 2 ROWS FETCH FIRST 7 ROWS ONLY",
		"postgres": "SELECT id FROM orders LIMIT 7 OFFSET 2", "redshift": "SELECT id FROM orders LIMIT 7 OFFSET 2",
		"snowflake": "SELECT id FROM orders LIMIT 7 OFFSET 2", "spark": "SELECT id FROM orders LIMIT 7 OFFSET 2",
		"starrocks": "SELECT id FROM orders LIMIT 7 OFFSET 2", "trino": "SELECT id FROM orders OFFSET 2 LIMIT 7",
		"tsql": "SELECT id FROM orders ORDER BY (SELECT NULL) OFFSET 2 ROWS FETCH FIRST 7 ROWS ONLY",
	}
	for _, dialect := range contractDialects {
		t.Run("dialect/"+dialect, func(t *testing.T) {
			got, err := sharedSQLParser.AddLimit("SELECT id FROM orders OFFSET 2", 7, dialect)
			require.NoError(t, err)
			require.Equal(t, want[dialect], got)
		})
	}

	cases := []struct {
		name, query, dialect, want string
		limit                      int
	}{
		{"replace existing", "SELECT id FROM t LIMIT 99", "duckdb", "SELECT id FROM t LIMIT 3", 3},
		{"union outer not branch", "SELECT 1 AS n UNION ALL SELECT 2 AS n LIMIT 9", "postgres", "SELECT 1 AS n UNION ALL SELECT 2 AS n LIMIT 4", 4},
		{"subquery outer not inner", "SELECT * FROM (SELECT * FROM t LIMIT 8) AS x", "duckdb", "SELECT * FROM (SELECT * FROM t LIMIT 8) AS x LIMIT 2", 2},
		{"zero", "SELECT * FROM t", "mysql", "SELECT * FROM t LIMIT 0", 0},
		// SQLGlot currently emits a negative LIMIT rather than rejecting it.
		{"negative", "SELECT * FROM t", "postgres", "SELECT * FROM t LIMIT -1", -1},
		{"tsql top replaced", "SELECT TOP 20 id FROM t", "tsql", "SELECT TOP 6 id FROM t", 6},
		{"tsql percent and ties removed", "SELECT TOP 10 PERCENT WITH TIES id FROM t ORDER BY id", "tsql", "SELECT TOP 7 id FROM t ORDER BY id", 7},
		{"mysql offset comma syntax", "SELECT * FROM t LIMIT 2, 10", "mysql", "SELECT * FROM t LIMIT 7 OFFSET 2", 7},
		// Replacing a per-group limit currently drops BY, making it global.
		{"clickhouse limit by removed", "SELECT * FROM t LIMIT 3 BY id", "clickhouse", "SELECT * FROM t LIMIT 7", 7},
		{"parenthesized set keeps branch limit and gains outer", "(SELECT 1 UNION ALL SELECT 2 LIMIT 8)", "postgres", "(SELECT 1 UNION ALL SELECT 2 LIMIT 8) LIMIT 3", 3},
		{"placeholder limit replaced offset retained", "SELECT * FROM t LIMIT ? OFFSET ?", "postgres", "SELECT * FROM t LIMIT 5 OFFSET ?", 5},
		{"fetch ties replaced with plain limit", "SELECT * FROM t ORDER BY id FETCH FIRST 10 ROWS WITH TIES", "postgres", "SELECT * FROM t ORDER BY id LIMIT 5", 5},
		{"oracle rownum is not recognized as limit", "SELECT * FROM t WHERE ROWNUM <= 10", "oracle", "SELECT * FROM t WHERE ROWNUM <= 10 FETCH FIRST 5 ROWS ONLY", 5},
		// ClickHouse's comma offset plus BY keeps both modifiers when its count is replaced.
		{"clickhouse comma offset and by retained", "SELECT * FROM t LIMIT 4, 10 BY k", "clickhouse", "SELECT * FROM t LIMIT 7 OFFSET 4 BY k", 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.AddLimit(tc.query, tc.limit, tc.dialect)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct{ name, query, dialect, errorText string }{
		{"empty", "", "duckdb", "cannot parse query"},
		{"malformed", "SELECT * FROM", "duckdb", "cannot parse query"},
		{"multiple statements form block", "SELECT 1; SELECT 2", "duckdb", "'Block' object has no attribute 'limit'"},
		{"insert is not limitable", "INSERT INTO dest SELECT id FROM src", "postgres", "'Insert' object has no attribute 'limit'"},
		{"invalid dialect", "SELECT 1", "not-a-dialect", "cannot parse query"},
		{"malformed query wins no detail over invalid dialect", "SELECT * FROM", "not-a-dialect", "cannot parse query"},
		// SQLGlot currently rejects ClickHouse's valid per-key plus global LIMIT form.
		{"clickhouse per-key and global limit", "SELECT * FROM t LIMIT 2 BY id LIMIT 9", "clickhouse", "cannot parse query"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := sharedSQLParser.AddLimit(tc.query, 1, tc.dialect)
			require.EqualError(t, err, tc.errorText)
			require.Empty(t, got)
		})
	}
}
