package sqlparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLParserIsReadOnlyQuery(t *testing.T) {
	t.Parallel()

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
		{name: "long write", query: "SELECT '" + strings.Repeat("a", 11000) + "'; DELETE FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dialect := tt.dialect
			if dialect == "" {
				dialect = "snowflake"
			}
			got, err := sharedSQLParser.IsReadOnlyQuery(tt.query, dialect)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}
