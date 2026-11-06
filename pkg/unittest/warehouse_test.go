package unittest

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// fakeRewriter stands in for the SQL parser: RenameTables does a naive textual
// replace and PrependCTEs wraps the query in a WITH, which is enough to assert
// the builder's orchestration (which CTEs, what bodies, what mapping) without
// the parser subprocess. Real SQL correctness is covered by the DuckDB
// execution test.
type fakeRewriter struct {
	used          []string
	gotCTEs       []sqlparser.CTE
	gotRemap      map[string]string
	extracted     string // when set, ExtractSelect returns it (simulates unwrapping DDL)
	gotCTEName    string // last cte name passed to SelectFromCTE
	gotFreezeTime string // last execution_time passed to FreezeTime
}

// ExtractSelect returns the configured inner SELECT when set, else the input
// unchanged — mirroring the real parser, which only rewrites DDL-wrapped assets.
func (f *fakeRewriter) ExtractSelect(sql, _ string) (string, error) {
	if f.extracted != "" {
		return f.extracted, nil
	}
	return sql, nil
}

// SelectFromCTE records the requested CTE and returns a recognizable rewrite, so
// the builder's CTE path can be asserted without a real parser.
func (f *fakeRewriter) SelectFromCTE(_, _, cteName string) (string, error) {
	f.gotCTEName = cteName
	return "SELECT * FROM " + cteName, nil
}

// FreezeTime records the timestamp and returns the SQL unchanged; real freezing
// is covered by the parser and DuckDB tests.
func (f *fakeRewriter) FreezeTime(sql, _, executionTime string) (string, error) {
	f.gotFreezeTime = executionTime
	return sql, nil
}

func (f *fakeRewriter) UsedTables(_, _ string) ([]string, error) { return f.used, nil }

func (f *fakeRewriter) RenameTables(sql, _ string, mapping map[string]string) (string, error) {
	f.gotRemap = mapping
	out := sql
	for k, v := range mapping {
		out = strings.ReplaceAll(out, k, v)
	}
	return out, nil
}

func (f *fakeRewriter) PrependCTEs(sql, _ string, ctes []sqlparser.CTE) (string, error) {
	f.gotCTEs = ctes
	parts := make([]string, len(ctes))
	for i, c := range ctes {
		parts[i] = c.Name + " AS (" + c.Query + ")"
	}
	return "WITH " + strings.Join(parts, ", ") + " " + sql, nil
}

func TestBuildWarehouseQuery(t *testing.T) {
	t.Parallel()

	orders := pipeline.UnitTestInput{
		Asset: "analytics.orders",
		Rows: []map[string]interface{}{
			{"id": 1, "amount": 100},
			{"id": 2, "amount": 50},
		},
	}

	t.Run("mocked input becomes a fixture CTE and refs are rewritten, no DDL", func(t *testing.T) {
		t.Parallel()
		f := &fakeRewriter{used: []string{"analytics.orders"}}
		got, err := BuildWarehouseQuery(f, "duckdb",
			"SELECT SUM(amount) AS revenue FROM analytics.orders", pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
		require.NoError(t, err)

		require.Len(t, f.gotCTEs, 1)
		require.Equal(t, "__bruin_ut_analytics_orders", f.gotCTEs[0].Name)
		// Rows become UNION ALL'd SELECTs; columns are emitted in sorted order
		// (amount, id), named on the first SELECT.
		require.Contains(t, f.gotCTEs[0].Query, "SELECT 100 AS amount, 1 AS id")
		require.Contains(t, f.gotCTEs[0].Query, "UNION ALL SELECT 50, 2")
		require.Equal(t, "__bruin_ut_analytics_orders", f.gotRemap["analytics.orders"])
		require.Contains(t, got, "FROM __bruin_ut_analytics_orders")
		// Read-only: nothing that writes to the target.
		for _, ddl := range []string{"CREATE ", "INSERT ", "ATTACH "} {
			require.NotContains(t, got, ddl)
		}
	})

	t.Run("rewrite targets the query's table spelling, not the mock's", func(t *testing.T) {
		t.Parallel()
		// The mock is declared with different casing than the query writes the
		// table. RenameTables matches case-sensitively, so the rewrite key must be
		// the query's spelling (from used), or the real table would leak through.
		mixedCase := pipeline.UnitTestInput{Asset: "Analytics.Orders", Rows: orders.Rows}
		f := &fakeRewriter{used: []string{"analytics.orders"}}
		_, err := BuildWarehouseQuery(f, "duckdb",
			"SELECT SUM(amount) AS revenue FROM analytics.orders",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{mixedCase}}, nil)
		require.NoError(t, err)

		// The mock still applies (matched by normalized name), but the rewrite is
		// keyed by the query's exact spelling so RenameTables finds it.
		require.Len(t, f.gotCTEs, 1)
		require.Equal(t, "__bruin_ut_analytics_orders", f.gotRemap["analytics.orders"])
		require.NotContains(t, f.gotRemap, "Analytics.Orders")
	})

	t.Run("a DDL-wrapped asset is reduced to its inner SELECT, no CREATE reaches the target", func(t *testing.T) {
		t.Parallel()
		// A materialization: none asset whose body is CREATE ... AS SELECT. The
		// builder must test the inner SELECT (ExtractSelect), never the CREATE.
		f := &fakeRewriter{
			used:      []string{"analytics.orders"},
			extracted: "SELECT SUM(amount) AS revenue FROM analytics.orders",
		}
		got, err := BuildWarehouseQuery(f, "duckdb",
			"CREATE OR REPLACE VIEW analytics.revenue AS SELECT SUM(amount) AS revenue FROM analytics.orders",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
		require.NoError(t, err)

		require.Contains(t, got, "FROM __bruin_ut_analytics_orders")
		for _, ddl := range []string{"CREATE ", "INSERT ", "ATTACH "} {
			require.NotContains(t, got, ddl)
		}
	})

	t.Run("declared columns are pinned with CAST", func(t *testing.T) {
		t.Parallel()
		f := &fakeRewriter{used: []string{"analytics.orders"}}
		schemas := map[string][]pipeline.Column{
			"analytics.orders": {{Name: "id", Type: "BIGINT"}, {Name: "amount", Type: "DECIMAL(10,2)"}},
		}
		_, err := BuildWarehouseQuery(f, "duckdb",
			"SELECT SUM(amount) AS revenue FROM analytics.orders", pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, schemas)
		require.NoError(t, err)
		require.Contains(t, f.gotCTEs[0].Query, `CAST(100 AS DECIMAL(10,2)) AS amount`)
		require.Contains(t, f.gotCTEs[0].Query, `CAST(1 AS BIGINT) AS id`)
	})

	t.Run("nested values become JSON text, parsed with PARSE_JSON on BigQuery", func(t *testing.T) {
		t.Parallel()
		invoices := pipeline.UnitTestInput{
			Asset: "raw.invoices",
			Rows: []map[string]interface{}{
				{"id": "1", "customer_ref": map[string]interface{}{"value": "7"}, "line": []interface{}{map[string]interface{}{"Amount": 10}}},
				{"id": "2", "customer_ref": nil, "line": []interface{}{map[string]interface{}{"Description": `say "hi"`, "Note": "it's"}}},
				{"id": "3", "customer_ref": 7, "line": "[]"},
			},
		}
		schemas := map[string][]pipeline.Column{
			"raw.invoices": {{Name: "id", Type: "varchar"}, {Name: "customer_ref", Type: "json"}, {Name: "line", Type: "json"}},
		}
		f := &fakeRewriter{used: []string{"raw.invoices"}}
		_, err := BuildWarehouseQuery(f, "bigquery", "SELECT id FROM raw.invoices",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{invoices}}, schemas)
		require.NoError(t, err)
		require.Equal(t,
			`SELECT PARSE_JSON('{"value":"7"}') AS customer_ref, CAST('1' AS varchar) AS id, PARSE_JSON('[{"Amount":10}]') AS line`+
				` UNION ALL SELECT NULL, '2', PARSE_JSON('[{"Description":"say \\"hi\\"","Note":"it\'s"}]')`+
				` UNION ALL SELECT PARSE_JSON('7'), '3', PARSE_JSON('[]')`,
			f.gotCTEs[0].Query)

		// Other dialects keep the portable CAST, now over JSON text.
		f = &fakeRewriter{used: []string{"raw.invoices"}}
		_, err = BuildWarehouseQuery(f, "duckdb", "SELECT id FROM raw.invoices",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{invoices}}, schemas)
		require.NoError(t, err)
		require.Contains(t, f.gotCTEs[0].Query, `CAST('{"value":"7"}' AS json) AS customer_ref`)
	})

	t.Run("unmocked read with a declared schema becomes an empty typed CTE", func(t *testing.T) {
		t.Parallel()
		f := &fakeRewriter{used: []string{"analytics.orders", "analytics.dim"}}
		schemas := map[string][]pipeline.Column{
			"analytics.dim": {{Name: "id", Type: "BIGINT"}, {Name: "label", Type: "VARCHAR"}},
		}
		_, err := BuildWarehouseQuery(f, "duckdb",
			"SELECT o.id FROM analytics.orders o LEFT JOIN analytics.dim d ON d.id = o.id",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, schemas)
		require.NoError(t, err)

		var dim *sqlparser.CTE
		for i := range f.gotCTEs {
			if f.gotCTEs[i].Name == "__bruin_ut_analytics_dim" {
				dim = &f.gotCTEs[i]
			}
		}
		require.NotNil(t, dim)
		require.Contains(t, dim.Query, "WHERE 1 = 0")
		require.Contains(t, dim.Query, `CAST(NULL AS BIGINT) AS id`)
	})

	t.Run("unmocked read without a declared schema is an error", func(t *testing.T) {
		t.Parallel()
		f := &fakeRewriter{used: []string{"analytics.orders", "analytics.dim"}}
		_, err := BuildWarehouseQuery(f, "duckdb",
			"SELECT o.id FROM analytics.orders o LEFT JOIN analytics.dim d ON d.id = o.id",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "analytics.dim")
	})
}

func TestBuildWarehouseQuery_FreezesWhenExecutionTimeSet(t *testing.T) {
	t.Parallel()
	orders := pipeline.UnitTestInput{Asset: "analytics.orders", Rows: []map[string]interface{}{{"id": 1}}}
	f := &fakeRewriter{used: []string{"analytics.orders"}}

	_, err := BuildWarehouseQuery(f, "duckdb", "SELECT * FROM analytics.orders",
		pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}, ExecutionTime: "2023-01-01 00:00:00"}, nil)
	require.NoError(t, err)
	require.Equal(t, "2023-01-01 00:00:00", f.gotFreezeTime)

	// Without execution_time, FreezeTime is not invoked.
	f2 := &fakeRewriter{used: []string{"analytics.orders"}}
	_, err = BuildWarehouseQuery(f2, "duckdb", "SELECT * FROM analytics.orders",
		pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
	require.NoError(t, err)
	require.Empty(t, f2.gotFreezeTime)
}

func TestBuildWarehouseQuery_FixtureTimestampLiterals(t *testing.T) {
	t.Parallel()

	const (
		written  = "2026-10-09T11:00:00Z"
		noZone   = "'2026-10-09 11:00:00'"
		dateOnly = "'2026-10-09'"
	)

	tests := []struct {
		name    string
		dialect string
		typ     string
		value   string
		want    string
	}{
		{"duckdb TIMESTAMPTZ keeps Z", "duckdb", "TIMESTAMPTZ", written, "CAST('2026-10-09T11:00:00Z' AS TIMESTAMPTZ) AS ts"},
		{"duckdb long type name keeps an offset", "duckdb", "timestamp with time zone", "2026-10-09T11:00:00+01:00", "CAST('2026-10-09T11:00:00+01:00' AS timestamp with time zone) AS ts"},
		{"duckdb keeps a zero offset", "duckdb", "TIMESTAMPTZ", "2026-10-09T11:00:00+00:00", "CAST('2026-10-09T11:00:00Z' AS TIMESTAMPTZ) AS ts"},
		{"duckdb keeps fractional seconds with a zone", "duckdb", "TIMESTAMPTZ", "2026-10-09T11:00:00.25Z", "CAST('2026-10-09T11:00:00.25Z' AS TIMESTAMPTZ) AS ts"},
		{"duckdb midnight with a zone is not a date", "duckdb", "TIMESTAMPTZ", "2026-10-09T00:00:00Z", "CAST('2026-10-09T00:00:00Z' AS TIMESTAMPTZ) AS ts"},
		{"duckdb undeclared column keeps Z", "duckdb", "", written, "'2026-10-09T11:00:00Z' AS ts"},
		{"duckdb no zone is unchanged", "duckdb", "TIMESTAMPTZ", "2026-10-09 11:00:00", "CAST(" + noZone + " AS TIMESTAMPTZ) AS ts"},
		{"duckdb date is unchanged", "duckdb", "TIMESTAMPTZ", "2026-10-09", "CAST(" + dateOnly + " AS TIMESTAMPTZ) AS ts"},
		{"duckdb undeclared no zone is unchanged", "duckdb", "", "2026-10-09 11:00:00", noZone + " AS ts"},
		{"duckdb undeclared date is unchanged", "duckdb", "", "2026-10-09", dateOnly + " AS ts"},
		{"duckdb TIMESTAMP stays zone-less", "duckdb", "TIMESTAMP", written, "CAST(" + noZone + " AS TIMESTAMP) AS ts"},
		{"duckdb DATE stays a date", "duckdb", "DATE", "2026-10-09T00:00:00Z", "CAST(" + dateOnly + " AS DATE) AS ts"},
		{"bigquery TIMESTAMP stays zone-less", "bigquery", "TIMESTAMP", written, "CAST(" + noZone + " AS TIMESTAMP) AS ts"},
		{"bigquery DATE stays a date", "bigquery", "DATE", "2026-10-09T00:00:00Z", "CAST(" + dateOnly + " AS DATE) AS ts"},
		{"other dialects stay zone-less", "snowflake", "TIMESTAMP_TZ", written, "CAST(" + noZone + " AS TIMESTAMP_TZ) AS ts"},
		{"a quoted value goes as written", "bigquery", "DATETIME", `"2026-10-09T11:00:00Z"`, "CAST('2026-10-09T11:00:00Z' AS DATETIME) AS ts"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var input pipeline.UnitTestInput
			require.NoError(t, yaml.Unmarshal([]byte("asset: analytics.events\nrows:\n  - ts: "+tt.value+"\n  - ts: "+tt.value+"\n"), &input))

			var schemas map[string][]pipeline.Column
			if tt.typ != "" {
				schemas = map[string][]pipeline.Column{"analytics.events": {{Name: "ts", Type: tt.typ}}}
			}
			f := &fakeRewriter{used: []string{"analytics.events"}}
			_, err := BuildWarehouseQuery(f, tt.dialect, "SELECT ts FROM analytics.events",
				pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{input}}, schemas)
			require.NoError(t, err)

			require.Len(t, f.gotCTEs, 1)
			first, later, ok := strings.Cut(f.gotCTEs[0].Query, " UNION ALL SELECT ")
			require.True(t, ok)
			require.Equal(t, "SELECT "+tt.want, first)
			// Later rows carry the same literal, without the CAST and the alias.
			require.Contains(t, tt.want, later)
		})
	}
}
