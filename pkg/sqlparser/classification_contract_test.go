package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type classificationContractCase struct {
	name    string
	query   string
	want    bool
	wantErr bool
}

func TestIsSingleSelectQueryContractAcrossDialects(t *testing.T) {
	t.Parallel()
	cases := []classificationContractCase{
		{"select", "SELECT 1", true, false},
		{"cte union", "WITH x AS (SELECT 1) SELECT * FROM x UNION ALL SELECT 2", true, false},
		{"semicolon in literal", "SELECT 'a;b'", true, false},
		{"multiple reads", "SELECT 1; SELECT 2", false, false},
		{"write", "DELETE FROM t", false, false},
		{"comments only", "-- SELECT 1", false, false},
		{"empty", "", false, true},
		{"malformed", "SELECT FROM", false, true},
	}
	for _, dialect := range contractDialects {
		dialect := dialect
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got, err := sharedSQLParser.IsSingleSelectQuery(tc.query, dialect)
					require.Equal(t, tc.wantErr, err != nil)
					require.Equal(t, tc.want, got)
				})
			}
		})
	}
}

func TestIsSingleSelectQueryContractStatementShape(t *testing.T) {
	t.Parallel()
	cases := []classificationContractCase{
		{"array and lambda", "SELECT TRANSFORM(ARRAY(1, 2), x -> x + 1)", true, false},
		{"table function", "SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[1,2]')))", true, false},
		{"select into is still select", "SELECT * INTO backup FROM t", true, false},
		{"write CTE is still select", "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", true, false},
		{"locking select is still select", "SELECT * FROM t FOR UPDATE", true, false},
		{"show is not select", "SHOW TABLES", false, false},
		{"describe is not select", "DESCRIBE orders", false, false},
		{"explain is not select", "EXPLAIN SELECT 1", false, false},
		{"NUL does not itself error", "SELECT 1\x00; DELETE FROM t", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.IsSingleSelectQuery(tc.query, "snowflake")
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.want, got)
		})
	}
	got, err := sharedSQLParser.IsSingleSelectQuery("SELECT 1", "not-a-dialect")
	require.False(t, got)
	require.Error(t, err)
}

func TestIsReadOnlyQueryContractAcrossDialects(t *testing.T) {
	t.Parallel()
	cases := []classificationContractCase{
		{"select", "SELECT 1", true, false},
		{"multiple reads", "SELECT 1; SELECT 2", true, false},
		{"semicolon in literal", "SELECT 'DELETE; DROP'", true, false},
		{"write", "DELETE FROM t", false, false},
		{"select into", "SELECT * INTO backup FROM t", false, false},
		{"NUL", "SELECT 1\x00; SELECT 2", false, false},
		{"comments only", "/* SELECT 1 */", false, true},
		{"empty statements", ";;", false, true},
		{"malformed", "SELECT FROM", false, true},
	}
	for _, dialect := range contractDialects {
		dialect := dialect
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got, err := sharedSQLParser.IsReadOnlyQuery(tc.query, dialect)
					require.Equal(t, tc.wantErr, err != nil)
					require.Equal(t, tc.want, got)
				})
			}
			// Bruin's current read-only policy accepts arbitrary function calls
			// only in Snowflake. Freeze that dialect exception explicitly.
			for _, query := range []string{"SELECT my_udf(1)", "SELECT db.schema.my_udf(1)"} {
				got, err := sharedSQLParser.IsReadOnlyQuery(query, dialect)
				require.NoError(t, err)
				require.Equal(t, dialect == "snowflake", got)
			}
		})
	}
}

func TestIsReadOnlyQueryContractPlatformStatements(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, dialect, query string
		want                 bool
	}{
		{"snowflake show", "snowflake", "SHOW TABLES", true},
		{"snowflake describe", "snowflake", "DESCRIBE TABLE orders", true},
		{"snowflake explain read", "snowflake", "EXPLAIN SELECT 1", true},
		{"snowflake explain write", "snowflake", "EXPLAIN DELETE FROM t", false},
		{"snowflake flow operator", "snowflake", "SELECT 1 ->> SELECT 2", false},
		{"snowflake table function", "snowflake", "SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[1]')))", true},
		{"bigquery array", "bigquery", "SELECT * FROM UNNEST([1, 2])", true},
		{"spark lambda", "spark", "SELECT transform(array(1, 2), x -> x + 1)", true},
		{"postgres returning", "postgres", "INSERT INTO t VALUES (1) RETURNING *", false},
		{"postgres write CTE", "postgres", "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", false},
		{"postgres lock", "postgres", "SELECT * FROM t FOR UPDATE", false},
		{"oracle sequence", "oracle", "SELECT seq.NEXTVAL FROM dual", false},
		{"athena unnest cardinality", "athena", "SELECT cardinality(items) FROM events CROSS JOIN UNNEST(items) AS u(item)", true},
		{"trino reduce lambda", "trino", "SELECT reduce(ARRAY[1, 2], 0, (s, x) -> s + x, s -> s)", true},
		{"clickhouse aggregate combinator", "clickhouse", "SELECT sumIf(amount, active) FROM events", true},
		{"clickhouse remote unknown function", "clickhouse", "SELECT * FROM remote('host', 'db', 'events')", false},
		{"databricks time travel", "databricks", "SELECT * FROM events VERSION AS OF 3", true},
		{"doris bitmap", "doris", "SELECT BITMAP_COUNT(users) FROM metrics", true},
		{"starrocks array", "starrocks", "SELECT ARRAY_LENGTH(tags) FROM metrics", true},
		{"duckdb file scan", "duckdb", "SELECT * FROM read_parquet('events.parquet')", true},
		{"duckdb pragma", "duckdb", "PRAGMA version", false},
		{"mysql named lock", "mysql", "SELECT GET_LOCK('name', 10)", false},
		{"redshift json", "redshift", "SELECT JSON_EXTRACT_PATH_TEXT(payload, 'name') FROM events", true},
		{"oracle hierarchy", "oracle", "SELECT LEVEL FROM nodes CONNECT BY PRIOR id=parent", true},
		{"fabric openjson", "fabric", "SELECT value FROM OPENJSON('[1,2]')", true},
		{"tsql sequence", "tsql", "SELECT NEXT VALUE FOR seq", false},
		// A table hint is not an exp.Lock, so this currently passes the gate.
		{"tsql update lock hint accepted", "tsql", "SELECT * FROM t WITH (UPDLOCK)", true},
		{"postgres sequence function", "postgres", "SELECT nextval('seq')", false},
		{"postgres shared lock", "postgres", "SELECT * FROM t FOR SHARE", false},
		{"postgres explain differs from snowflake", "postgres", "EXPLAIN SELECT 1", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.IsReadOnlyQuery(tc.query, tc.dialect)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	got, err := sharedSQLParser.IsReadOnlyQuery("SELECT 1", "not-a-dialect")
	require.False(t, got)
	require.Error(t, err)
}

func TestValidateReadOnlyQueryContractErrors(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateReadOnlyQuery("SELECT 1; SELECT 2", "snowflake"))
	require.EqualError(t, ValidateReadOnlyQuery("DELETE FROM t", "snowflake"), "query is not allowed on a read-only connection")
	parseErr := ValidateReadOnlyQuery("SELECT FROM", "snowflake")
	require.Error(t, parseErr)
	require.ErrorContains(t, parseErr, "read-only query validation failed: cannot determine whether query is read-only:")
	dialectErr := ValidateReadOnlyQuery("SELECT 1", "not-a-dialect")
	require.Error(t, dialectErr)
	require.ErrorContains(t, dialectErr, "read-only query validation failed: cannot determine whether query is read-only:")
}
