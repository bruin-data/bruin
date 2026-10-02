package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreezeTimeContract(t *testing.T) {
	want := map[string]string{
		"athena":     "SELECT CAST('2024-02-03 04:05:06' AS TIMESTAMP) AS ts, CAST('2024-02-03' AS DATE) AS d, CAST('04:05:06' AS TIME) AS tm",
		"bigquery":   "SELECT CAST('2024-02-03 04:05:06' AS DATETIME) AS ts, CAST('2024-02-03' AS DATE) AS d, CAST('04:05:06' AS TIME) AS tm",
		"clickhouse": "SELECT CURRENT_TIMESTAMP AS ts, CAST('2024-02-03' AS Nullable(DATE)) AS d, CAST('04:05:06' AS Nullable(TIME)) AS tm",
		"databricks": "SELECT CAST('2024-02-03 04:05:06' AS TIMESTAMP) AS ts, CAST('2024-02-03' AS DATE) AS d, CURRENT_TIME AS tm",
		"doris":      "SELECT CAST('2024-02-03 04:05:06' AS DATETIME) AS ts, `CURRENT_DATE` AS d, CAST('04:05:06' AS TIME) AS tm",
		"fabric":     "SELECT CAST('2024-02-03 04:05:06' AS DATETIME2(6)) AS ts, CAST('2024-02-03' AS DATE) AS d, CAST('04:05:06' AS TIME(6)) AS tm",
		"mysql":      "SELECT CAST('2024-02-03 04:05:06' AS DATETIME) AS ts, CAST('2024-02-03' AS DATE) AS d, CAST('04:05:06' AS TIME) AS tm",
		"starrocks":  "SELECT CAST('2024-02-03 04:05:06' AS DATETIME) AS ts, CAST('2024-02-03' AS DATE) AS d, CAST('04:05:06' AS TIME) AS tm",
		"tsql":       "SELECT CAST('2024-02-03 04:05:06' AS DATETIME2) AS ts, CAST('2024-02-03' AS DATE) AS d, CAST('04:05:06' AS TIME) AS tm",
	}
	standard := "SELECT CAST('2024-02-03 04:05:06' AS TIMESTAMP) AS ts, CAST('2024-02-03' AS DATE) AS d, CAST('04:05:06' AS TIME) AS tm"
	for _, d := range []string{"duckdb", "oracle", "postgres", "redshift", "trino"} {
		want[d] = standard
	}
	for _, d := range []string{"snowflake", "spark"} {
		want[d] = "SELECT CAST('2024-02-03 04:05:06' AS TIMESTAMP) AS ts, CAST('2024-02-03' AS DATE) AS d, CURRENT_TIME AS tm"
	}
	for _, dialect := range contractDialects {
		t.Run("dialect/"+dialect, func(t *testing.T) {
			got, err := sharedSQLParser.FreezeTime("SELECT CURRENT_TIMESTAMP AS ts, CURRENT_DATE AS d, CURRENT_TIME AS tm", dialect, "2024-02-03 04:05:06")
			require.NoError(t, err)
			require.Equal(t, want[dialect], got)
		})
	}
	for _, tc := range []struct{ name, query, dialect, at, want string }{
		{"date only", "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME", "duckdb", "2024-02-03", "SELECT CAST('2024-02-03' AS TIMESTAMP), CAST('2024-02-03' AS DATE), CAST('00:00:00' AS TIME)"},
		{"iso preserved timestamp", "SELECT CURRENT_TIMESTAMP", "postgres", "2024-02-03T04:05:06", "SELECT CAST('2024-02-03T04:05:06' AS TIMESTAMP)"},
		{"timezone preserved literal", "SELECT CURRENT_TIMESTAMP", "postgres", "2024-02-03T04:05:06+02:00", "SELECT CAST('2024-02-03T04:05:06+02:00' AS TIMESTAMP)"},
		{"aliases", "SELECT NOW(), CURRENT_DATE(), CURRENT_TIME", "mysql", "2024-02-03 04:05:06", "SELECT NOW(), CAST('2024-02-03' AS DATE), CAST('04:05:06' AS TIME)"},
		{"tsql getdate", "SELECT GETDATE(), SYSDATETIME()", "tsql", "2024-02-03 04:05:06", "SELECT CAST('2024-02-03 04:05:06' AS DATETIME2), CAST('2024-02-03 04:05:06' AS DATETIME2)"},
		{"oracle sysdate alias", "SELECT SYSDATE, SYSTIMESTAMP FROM dual", "oracle", "2024-02-03 04:05:06", "SELECT CAST('2024-02-03 04:05:06' AS TIMESTAMP), SYSTIMESTAMP FROM dual"},
		{"literal comment and nesting", "SELECT 'CURRENT_TIMESTAMP' AS s /* CURRENT_DATE */, COALESCE(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", "duckdb", "2024-02-03 04:05:06", "SELECT 'CURRENT_TIMESTAMP' AS s /* CURRENT_DATE */, COALESCE(CAST('2024-02-03 04:05:06' AS TIMESTAMP), CAST('2024-02-03 04:05:06' AS TIMESTAMP))"},
		// SQLGlot parses this as a block and transforms both statements.
		{"all statements", "SELECT CURRENT_DATE; SELECT CURRENT_TIMESTAMP", "duckdb", "2024-02-03 04:05:06", "SELECT CAST('2024-02-03' AS DATE); SELECT CAST('2024-02-03 04:05:06' AS TIMESTAMP)"},
		{"bigquery datetime unchanged and timezone discarded", "SELECT CURRENT_DATETIME(), CURRENT_DATE('Europe/Istanbul'), CURRENT_TIMESTAMP()", "bigquery", "2024-02-03T04:05:06Z", "SELECT CURRENT_DATETIME(), CAST('2024-02-03' AS DATE), CAST('2024-02-03T04:05:06Z' AS DATETIME)"},
		{"snowflake aliases", "SELECT CURRENT_TIME(), LOCALTIMESTAMP(), SYSDATE()", "snowflake", "2024-02-03 04:05:06", "SELECT CURRENT_TIME, CAST('2024-02-03 04:05:06' AS TIMESTAMP), CAST('2024-02-03 04:05:06' AS TIMESTAMP)"},
		// Execution time is not validated, and clock_timestamp is not replaced.
		{"invalid time string accepted", "SELECT NOW(), clock_timestamp(), CURRENT_TIME(3)", "postgres", "not-a-date", "SELECT CAST('not-a-date' AS TIMESTAMP), CLOCK_TIMESTAMP(), CAST('00:00:00' AS TIME)"},
		{"clickhouse clock functions unchanged", "SELECT now(), today(), now64()", "clickhouse", "2024-02-03 04:05:06", "SELECT now(), today(), now64()"},
		{"cte and predicate", "WITH x AS (SELECT CURRENT_DATE AS d) SELECT d FROM x WHERE d < CURRENT_DATE", "duckdb", "2024-02-03", "WITH x AS (SELECT CAST('2024-02-03' AS DATE) AS d) SELECT d FROM x WHERE d < CAST('2024-02-03' AS DATE)"},
		{"precision retained from execution time not function", "SELECT CURRENT_TIMESTAMP(3), CURRENT_TIME(2)", "postgres", "2024-02-03 04:05:06.789123", "SELECT CAST('2024-02-03 04:05:06.789123' AS TIMESTAMP), CAST('04:05:06.789123' AS TIME)"},
		{"local time functions unchanged", "SELECT LOCALTIME, LOCALTIMESTAMP", "postgres", "2024-02-03T04:05:06+05:30", "SELECT LOCALTIME, LOCALTIMESTAMP"},
		{"tsql utc and offset functions unchanged", "SELECT GETUTCDATE(), SYSDATETIMEOFFSET()", "tsql", "2024-02-03T04:05:06.123456+02:00", "SELECT GETUTCDATE(), SYSDATETIMEOFFSET()"},
		{"aliases replaced expressions but qualified identifier retained", "SELECT current_timestamp AS current_date, current_date AS current_timestamp, t.current_time FROM t", "postgres", "2024-02-03 04:05:06", "SELECT CAST('2024-02-03 04:05:06' AS TIMESTAMP) AS current_date, CAST('2024-02-03' AS DATE) AS current_timestamp, t.current_time FROM t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.FreezeTime(tc.query, tc.dialect, tc.at)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct{ name, query, dialect, at, errorText string }{
		{"missing time", "SELECT CURRENT_TIMESTAMP", "duckdb", "", "execution_time is required"},
		// Argument validation precedes SQL and dialect parsing.
		{"missing time wins over malformed query and dialect", "SELECT * FROM", "not-a-dialect", "", "execution_time is required"},
		{"empty", "", "duckdb", "2024-01-01", "cannot parse query"},
		{"malformed", "SELECT * FROM", "duckdb", "2024-01-01", contractSelectStarFromError},
		{"invalid dialect", "SELECT 1", "not-a-dialect", "2024-01-01", "Unknown dialect 'not-a-dialect'."},
		{"invalid dialect wins over malformed query", "SELECT * FROM", "not-a-dialect", "2024-01-01", "Unknown dialect 'not-a-dialect'."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.FreezeTime(tc.query, tc.dialect, tc.at)
			require.EqualError(t, err, tc.errorText)
			require.Equal(t, "", got)
		})
	}
}
