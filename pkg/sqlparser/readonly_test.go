package sqlparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLParserIsReadOnlyQuery(t *testing.T) {
	t.Parallel()
	testReadOnlyQueries(t, sharedSQLParser.IsReadOnlyQuery, false)
}

func TestRustSQLParserIsReadOnlyQuery(t *testing.T) {
	t.Parallel()
	if err := ensureRustSQLParserFFI(); err != nil {
		t.Skip(err)
	}
	parser, err := NewRustSQLParser(false)
	require.NoError(t, err)
	testReadOnlyQueries(t, parser.IsReadOnlyQuery, true)
}

func testReadOnlyQueries(t *testing.T, validate func(string, string) (bool, error), allowRejectionError bool) {
	t.Helper()
	tests := []struct {
		name    string
		query   string
		dialect string
		want    bool
		wantErr bool
	}{
		{name: "select", query: "SELECT 1", want: true},
		{name: "CTE and aggregation", query: "WITH x AS (SELECT amount FROM orders) SELECT SUM(amount) FROM x", want: true},
		{name: "union", query: "SELECT 1 UNION ALL SELECT 2", want: true},
		{name: "multiple reads", query: "SELECT 1; SELECT 2;", want: true},
		{name: "comments and literals", query: "/* DELETE FROM t ->> */ SELECT 'DROP TABLE t; ->>'; -- INSERT INTO t", want: true},
		{name: "show", query: "SHOW TABLES", want: true},
		{name: "describe", query: "DESCRIBE TABLE orders", want: true},
		{name: "explain", query: "EXPLAIN SELECT COUNT(*) FROM orders", want: true},
		{name: "built-in functions", query: "SELECT ABS(-1), LOWER('A'), COALESCE(NULL, 1), COUNT(*) FROM t", want: true},
		{name: "built-in aliases", query: "SELECT LEN('abc'), NVL(NULL, 1), POW(2, 3), TO_VARCHAR(1)", want: true},
		{name: "date functions", query: "SELECT DATEADD(day, 1, CURRENT_DATE()), CURRENT_TIMESTAMP, EXTRACT(year FROM CURRENT_DATE)", want: true},
		{name: "casts and case", query: "SELECT CASE WHEN x > 0 THEN CAST(x AS INT) ELSE TRY_CAST('1' AS INT) END FROM t", want: true},
		{name: "predicates", query: "SELECT * FROM t WHERE x IN (1, 2) AND EXISTS(SELECT 1 FROM s) AND x > ALL(SELECT x FROM s)", want: true},
		{name: "window function", query: "SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) FROM t", want: true},
		{name: "implicit table alias columns", query: "SELECT * FROM t y(a)", want: true},
		{name: "subquery alias columns", query: "SELECT * FROM (SELECT 1) y(a)", want: true},
		{name: "multiple CTE alias columns", query: "WITH x(a) AS (SELECT 1), y(b) AS (SELECT 2) SELECT * FROM x, y", want: true},
		{name: "Unicode before alias", query: "WITH x AS (SELECT 'ç') SELECT * FROM x AS y(a)", want: true},
		{name: "Unicode before UDF sharing alias", query: "WITH READ_CSV(x) AS (SELECT 'ç') SELECT READ_CSV(x) FROM READ_CSV"},
		{name: "Unicode quoted alias", query: `WITH "ç"(x) AS (SELECT 1) SELECT * FROM "ç"`, want: true},
		{name: "alias marker collision", query: "WITH BRUIN_READ_ONLY_ALIAS AS (SELECT 1), x(a) AS (SELECT 2) SELECT * FROM x", want: true},
		{name: "function sharing table alias", query: "SELECT READ_CSV(x) FROM t AS READ_CSV(x)"},
		{name: "function sharing quoted alias", query: `SELECT "ABS"(x) FROM t AS "ABS"(x)`},
		{name: "table UDF sharing CTE alias", query: "WITH READ_CSV(x) AS (SELECT 1) SELECT * FROM TABLE(READ_CSV(x))"},
		{name: "CTE column names", query: "WITH x(a) AS (SELECT 1) SELECT a FROM x AS y(a)", want: true},
		{name: "table function", query: "SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[1,2]')))", want: true},
		{name: "escaped built-in function", query: "SELECT {fn ABS(-1)}", want: true},
		{name: "parameterized casts", query: "SELECT CAST(x AS NUMBER(10,2)), x::VARCHAR(10) FROM t", want: true},
		{name: "structured casts", query: "SELECT CAST(x AS ARRAY(NUMBER(10,2))) FROM t", want: true},
		{name: "quoted function-like alias", query: `WITH "CAST"(x) AS (SELECT 1) SELECT CAST(x AS INT) FROM "CAST"`, want: true},
		{name: "SQLGlot special function", query: "SELECT ARG_MAX(1,2)"},
		{name: "SQLGlot cast alias", query: "SELECT SAFE_CAST(1 AS INT)"},
		{name: "UDF in normalized argument", query: "SELECT DATEADD(TIME_TO_STR(1, 'x'), 1, CURRENT_DATE())"},
		{name: "UDF sharing CTE name", query: "WITH READ_CSV(x) AS (SELECT 1) SELECT READ_CSV('x')"},
		{name: "UDF after parameterized cast", query: "SELECT CAST(x AS NUMBER(10,2)), READ_CSV('x') FROM t"},
		{name: "cross-dialect function", query: "SELECT TIME_TO_STR(1, 'x')"},
		{name: "cross-dialect table function", query: "SELECT * FROM TABLE(READ_CSV('x'))"},
		{name: "cross-dialect scalar function", query: "SELECT READ_CSV('x')"},
		{name: "normalized function alias", query: "SELECT LOG10(10)"},
		{name: "quoted built-in name", query: `SELECT "ABS"(1)`},
		{name: "quoted mixed-case name", query: `SELECT "aBs"(1)`},
		{name: "quoted unknown function", query: `SELECT "my_udf"(1)`},
		{name: "dynamic function", query: "SELECT IDENTIFIER('my_udf')(1)"},
		{name: "dynamic zero-argument function", query: "SELECT IDENTIFIER('my_udf')()"},
		{name: "dynamic table function", query: "SELECT * FROM TABLE(IDENTIFIER('my_udf')(1))"},
		{name: "escaped UDF", query: "SELECT {fn READ_CSV('x')}"},
		{name: "UDF with intervening comment", query: "SELECT READ_CSV /* harmless */ ('x')"},
		{name: "qualified built-in with comments", query: "SELECT db./* harmless */ABS(1)"},
		{name: "built-in with intervening comment", query: "SELECT ABS /* harmless */ (-1)", want: true},
		{name: "nested UDF", query: "SELECT COALESCE(TIME_TO_STR(1, 'x'), 'fallback')"},
		{name: "UDF in CTE", query: "WITH x AS (SELECT READ_CSV('x')) SELECT * FROM x"},
		{name: "UDF in predicate", query: "SELECT * FROM t WHERE x = TIME_TO_STR(1, 'x')"},
		{name: "UDF in later statement", query: "SELECT 1; SELECT READ_CSV('x')"},
		{name: "UDF in explain", query: "EXPLAIN SELECT READ_CSV('x')"},
		{name: "UDF text in literal", query: `SELECT 'READ_CSV(''x'')', '"ABS"(1)'`, want: true},
		{name: "UDF text in comment", query: "SELECT 1 /* READ_CSV('x') */", want: true},
		{name: "insert", query: "INSERT INTO t VALUES (1)"},
		{name: "update", query: "UPDATE t SET a = 1"},
		{name: "delete", query: "DELETE FROM t"},
		{name: "merge", query: "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE"},
		{name: "create", query: "CREATE TABLE t AS SELECT 1"},
		{name: "drop", query: "DROP TABLE t"},
		{name: "alter", query: "ALTER TABLE t ADD COLUMN a INT"},
		{name: "truncate", query: "TRUNCATE TABLE t"},
		{name: "copy out", query: "COPY INTO @stage FROM t"},
		{name: "call", query: "CALL write_data()"},
		{name: "execute immediate", query: "EXECUTE IMMEDIATE 'DELETE FROM t'"},
		{name: "session change", query: "ALTER SESSION SET QUERY_TAG = 'test'"},
		{name: "use", query: "USE ROLE ACCOUNTADMIN"},
		{name: "trailing write", query: "SELECT 1; DROP TABLE t"},
		{name: "leading write", query: "DELETE FROM t; SELECT 1"},
		{name: "select into", query: "SELECT * INTO backup FROM t"},
		{name: "write CTE", query: "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", dialect: "postgres"},
		{name: "lock", query: "SELECT * FROM t FOR UPDATE", dialect: "postgres"},
		{name: "sequence", query: "SELECT seq.NEXTVAL"},
		{name: "sequence function", query: "SELECT * FROM TABLE(GETNEXTVAL(seq))"},
		{name: "system function", query: "SELECT SYSTEM$CANCEL_QUERY('id')"},
		{name: "unknown function", query: "SELECT my_udf(1)"},
		{name: "qualified function", query: "SELECT db.schema.ABS(1)"},
		{name: "explain write", query: "EXPLAIN DELETE FROM t"},
		{name: "explain trailing write", query: "EXPLAIN SELECT 1; DELETE FROM t"},
		{name: "Snowflake flow operator", query: "SELECT 1 ->> DELETE FROM t"},
		{name: "empty", query: "", wantErr: true},
		{name: "only comments", query: "-- SELECT 1", wantErr: true},
		{name: "malformed", query: "SELECT FROM", wantErr: true},
		{name: "unknown dialect", query: "SELECT 1", dialect: "unknown", wantErr: true},
		{name: "NUL before write", query: "SELECT 1\x00; DELETE FROM t"},
		{name: "NUL before UDF", query: "SELECT 1\x00; SELECT my_udf(1)"},
		{name: "long write", query: "SELECT '" + strings.Repeat("a", 11000) + "'; DELETE FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dialect := tt.dialect
			if dialect == "" {
				dialect = "snowflake"
			}
			got, err := validate(tt.query, dialect)
			if tt.wantErr {
				require.Error(t, err)
			} else if tt.want || !allowRejectionError {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValidateReadOnlyQueryBackend(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateReadOnlyQuery("SELECT 1; SELECT 2", "snowflake"))
	if ensureRustSQLParserFFI() == nil {
		require.IsType(t, &RustSQLParser{}, readOnlyParser)
	} else {
		require.IsType(t, &SQLParser{}, readOnlyParser)
	}
	require.ErrorContains(t, ValidateReadOnlyQuery("SELECT 1; SELECT my_udf(1)", "snowflake"), "read-only")
	require.ErrorContains(t, ValidateReadOnlyQuery("SELECT FROM", "snowflake"), "read-only")
}
