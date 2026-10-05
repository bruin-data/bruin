//nolint:paralleltest // all cases intentionally share TestMain's subprocess.
package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLParserColumnLineageContractScopesAndDialectFeatures(t *testing.T) {
	empty := []ColumnLineage{}
	noErrors := []string{}
	tests := []struct {
		name, dialect, query string
		schema               Schema
		want                 *Lineage
	}{
		{"nested CTE shadowing", "postgres", "WITH x AS (SELECT id FROM outer_t), y AS (WITH x AS (SELECT id FROM inner_t) SELECT id FROM x) SELECT x.id AS outer_id,y.id AS inner_id FROM x CROSS JOIN y", Schema{"outer_t": {"id": "int"}, "inner_t": {"id": "bigint"}}, &Lineage{[]ColumnLineage{{"inner_id", uc("id", "inner_t"), "BIGINT"}, {"outer_id", uc("id", "outer_t"), "INT"}}, empty, noErrors}},
		{"named CTE columns", "postgres", "WITH x(a,b) AS (SELECT id,name FROM users) SELECT a,b FROM x", Schema{"users": {"id": "int", "name": "varchar"}}, &Lineage{[]ColumnLineage{{"a", uc("id", "users"), "INT"}, {"b", uc("name", "users"), "VARCHAR"}}, empty, noErrors}},
		{"named derived columns", "postgres", "SELECT d.a,d.b FROM (SELECT id,name FROM users) AS d(a,b)", Schema{"users": {"id": "int", "name": "varchar"}}, &Lineage{[]ColumnLineage{{"a", uc("id", "users"), "INT"}, {"b", uc("name", "users"), "VARCHAR"}}, empty, noErrors}},
		// The recursive arm and its filter disappear from both selected and non-selected lineage.
		{"recursive materialized CTE", "postgres", "WITH RECURSIVE x(n) AS MATERIALIZED (SELECT id FROM seed UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT n FROM x", Schema{"seed": {"id": "int"}}, &Lineage{[]ColumnLineage{{"n", uc("id", "seed"), "INT"}}, empty, noErrors}},
		{"intersect", "postgres", "SELECT a AS x FROM l INTERSECT SELECT b FROM r", Schema{"l": {"a": "int"}, "r": {"b": "bigint"}}, &Lineage{[]ColumnLineage{{"x", []UpstreamColumn{{"a", "l"}, {"b", "r"}}, "INT"}}, empty, noErrors}},
		{"except all", "postgres", "SELECT a AS x FROM l EXCEPT ALL SELECT b FROM r", Schema{"l": {"a": "int"}, "r": {"b": "bigint"}}, &Lineage{[]ColumnLineage{{"x", []UpstreamColumn{{"a", "l"}, {"b", "r"}}, "INT"}}, empty, noErrors}},
		// BY NAME is treated positionally: a incorrectly absorbs r.b and b absorbs r.c.
		{"union all by name asymmetric", "duckdb", "SELECT a,b FROM l UNION ALL BY NAME SELECT b,c FROM r", Schema{"l": {"a": "int", "b": "varchar"}, "r": {"b": "varchar", "c": "bool"}}, &Lineage{[]ColumnLineage{{"a", []UpstreamColumn{{"a", "l"}, {"b", "r"}}, "INT"}, {"b", []UpstreamColumn{{"b", "l"}, {"c", "r"}}, "TEXT"}}, empty, noErrors}},
		{"natural join", "postgres", "SELECT * FROM a NATURAL JOIN b", Schema{"a": {"id": "int", "av": "text"}, "b": {"id": "int", "bv": "text"}}, &Lineage{[]ColumnLineage{{"av", uc("av", "a"), "TEXT"}, {"bv", uc("bv", "b"), "TEXT"}, {"id", []UpstreamColumn{{"id", "a"}, {"id", "b"}}, "INT"}}, []ColumnLineage{{"id", []UpstreamColumn{{"id", "a"}, {"id", "b"}}, ""}}, noErrors}},
		{"full join using", "postgres", "SELECT id,a.av,b.bv FROM a FULL JOIN b USING(id)", Schema{"a": {"id": "int", "av": "text"}, "b": {"id": "bigint", "bv": "text"}}, &Lineage{[]ColumnLineage{{"av", uc("av", "a"), "TEXT"}, {"bv", uc("bv", "b"), "TEXT"}, {"id", []UpstreamColumn{{"id", "a"}, {"id", "b"}}, "BIGINT"}}, []ColumnLineage{{"id", []UpstreamColumn{{"id", "a"}, {"id", "b"}}, ""}}, noErrors}},
		{"snowflake pivot", "snowflake", "SELECT * FROM sales PIVOT(SUM(amount) FOR quarter IN ('Q1','Q2'))", Schema{"sales": {"customer": "int", "quarter": "varchar", "amount": "int"}}, &Lineage{[]ColumnLineage{{"'Q1'", uc("AMOUNT", "SALES"), "BIGINT"}, {"'Q2'", uc("AMOUNT", "SALES"), "BIGINT"}, {"CUSTOMER", uc("CUSTOMER", "SALES"), "INT"}}, empty, noErrors}},
		{"snowflake unpivot", "snowflake", "SELECT customer,quarter,amount FROM quarterly UNPIVOT(amount FOR quarter IN (q1,q2))", Schema{"quarterly": {"customer": "int", "q1": "decimal(10,2)", "q2": "decimal(10,2)"}}, &Lineage{[]ColumnLineage{{"AMOUNT", []UpstreamColumn{{"Q1", "QUARTERLY"}, {"Q2", "QUARTERLY"}}, "DECIMAL(10, 2)"}, {"CUSTOMER", uc("CUSTOMER", "QUARTERLY"), "INT"}, {"QUARTER", []UpstreamColumn{{"Q1", "QUARTERLY"}, {"Q2", "QUARTERLY"}}, "VARCHAR"}}, empty, noErrors}},
		// Nested COLUMNS is retained as a synthetic unconnected output.
		{"nested star and columns", "duckdb", "SELECT s.*, COLUMNS(c -> c LIKE '%id') FROM (SELECT * FROM t) s", Schema{"t": {"id": "int", "other_id": "bigint", "name": "varchar"}}, &Lineage{[]ColumnLineage{{"_col_3", []UpstreamColumn{}, "UNKNOWN"}, {"id", uc("id", "t"), "INT"}, {"name", uc("name", "t"), "TEXT"}, {"other_id", uc("other_id", "t"), "BIGINT"}}, empty, noErrors}},
		{"json map array lambda capture", "duckdb", "SELECT payload->>'name' AS n, attrs['key'] AS v, list_transform(nums, x -> x + offset) AS ys FROM t", Schema{"t": {"payload": "json", "attrs": "map(varchar,integer)", "nums": "integer[]", "offset": "integer"}}, &Lineage{[]ColumnLineage{{"n", uc("payload", "t"), "UNKNOWN"}, {"v", uc("attrs", "t"), "UNKNOWN"}, {"ys", []UpstreamColumn{{"nums", "t"}, {"offset", "t"}}, "UNKNOWN"}}, empty, noErrors}},
		{"correlated exists", "postgres", "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM items i WHERE i.order_id=o.id AND i.active)", Schema{"orders": {"id": "int"}, "items": {"order_id": "int", "active": "bool"}}, &Lineage{[]ColumnLineage{{"id", uc("id", "orders"), "INT"}}, []ColumnLineage{{"active", uc("active", "items"), ""}, {"id", uc("id", "orders"), ""}, {"order_id", uc("order_id", "items"), ""}}, noErrors}},
		{"grouping sets", "postgres", "SELECT region,product,SUM(amount) total FROM sales GROUP BY GROUPING SETS ((region),(product),())", Schema{"sales": {"region": "text", "product": "text", "amount": "int"}}, &Lineage{[]ColumnLineage{{"product", uc("product", "sales"), "TEXT"}, {"region", uc("region", "sales"), "TEXT"}, {"total", uc("amount", "sales"), "BIGINT"}}, []ColumnLineage{{"product", uc("product", "sales"), ""}, {"region", uc("region", "sales"), ""}}, noErrors}},
		// PREWHERE is valid ClickHouse SQL but is omitted; only WHERE is reported.
		{"clickhouse prewhere omission", "clickhouse", "SELECT id FROM t PREWHERE active WHERE region=1", Schema{"t": {"id": "int", "active": "bool", "region": "int"}}, &Lineage{[]ColumnLineage{{"id", uc("id", "t"), "INT"}}, []ColumnLineage{{"region", uc("region", "t"), ""}}, noErrors}},
		{"doris maps", "doris", "SELECT attrs['k'] AS v, MAP_KEYS(attrs) AS ks FROM t", Schema{"t": {"attrs": "MAP<STRING,INT>"}}, &Lineage{[]ColumnLineage{{"ks", uc("attrs", "t"), "UNKNOWN"}, {"v", uc("attrs", "t"), "UNKNOWN"}}, empty, noErrors}},
		{"starrocks lambda and json", "starrocks", "SELECT ARRAY_MAP(x -> x + offset, nums) ys, JSON_QUERY(payload, '$.a') j FROM t", Schema{"t": {"nums": "ARRAY<INT>", "offset": "INT", "payload": "JSON"}}, &Lineage{[]ColumnLineage{{"j", uc("payload", "t"), "UNKNOWN"}, {"ys", []UpstreamColumn{{"nums", "t"}, {"offset", "t"}}, "UNKNOWN"}}, empty, noErrors}},
		{"redshift super", "redshift", "SELECT payload.user.id AS user_id, payload.items[0] AS item FROM events", Schema{"events": {"payload": "SUPER"}}, &Lineage{[]ColumnLineage{{"item", uc("payload", "events"), "UNKNOWN"}, {"user_id", uc("payload", "events"), "UNKNOWN"}}, empty, noErrors}},
		// posexplode's position incorrectly inherits ARRAY<TEXT>; value is UNKNOWN.
		{"spark multi output generator", "spark", "SELECT id,pos,val FROM src LATERAL VIEW posexplode(items) e AS pos,val", Schema{"src": {"id": "bigint", "items": "array<string>"}}, &Lineage{[]ColumnLineage{{"id", uc("id", "src"), "BIGINT"}, {"pos", uc("items", "src"), "ARRAY<TEXT>"}, {"val", uc("items", "src"), "UNKNOWN"}}, empty, noErrors}},
		{"databricks variant", "databricks", "SELECT payload:user.id::BIGINT AS user_id FROM events", Schema{"events": {"payload": "VARIANT"}}, &Lineage{[]ColumnLineage{{"user_id", uc("payload", "events"), "BIGINT"}}, empty, noErrors}},
		{"athena nested rows", "athena", "SELECT profile.name AS name, profile.address.zip AS zip FROM users", Schema{"users": {"profile": "ROW(name VARCHAR,address ROW(zip BIGINT))"}}, &Lineage{[]ColumnLineage{{"name", uc("profile", "users"), "VARCHAR"}, {"zip", uc("profile", "users"), "BIGINT"}}, empty, noErrors}},
		// Unknown schema type strings are passed through verbatim rather than rejected.
		{"invalid schema type passes through", "postgres", "SELECT id FROM t", Schema{"t": {"id": "definitely_not_a_type"}}, &Lineage{[]ColumnLineage{{"id", uc("id", "t"), "definitely_not_a_type"}}, empty, noErrors}},
		{"empty schema type", "postgres", "SELECT id FROM t", Schema{"t": {"id": ""}}, &Lineage{[]ColumnLineage{{"id", uc("id", "t"), "USER-DEFINED"}}, empty, noErrors}},
		{"nil table schema", "postgres", "SELECT id FROM t", Schema{"t": nil}, &Lineage{empty, empty, []string{"Schema Error: Unknown t: t"}}},
		{"empty table schema", "postgres", "SELECT id FROM t", Schema{"t": {}}, &Lineage{empty, empty, []string{"Schema Error: Table  must have at least one column"}}},
		{"case alignment", "bigquery", "SELECT ID FROM Raw.Teams", Schema{"raw.teams": {"id": "int64"}}, &Lineage{[]ColumnLineage{{"id", uc("id", "Raw.Teams"), "BIGINT"}}, empty, noErrors}},
		// Selected upstreams sort column-first, while nonselected groups sort by
		// concatenated name+table ("aaa" before "az"), not by a (name, table) tuple.
		{"different output and upstream sort orders", "postgres", `SELECT CAST(z.a + a.aa AS INT) AS "Z", z.a AS "a" FROM z CROSS JOIN a WHERE z.a > 0 AND a.aa > 0`, Schema{"z": {"a": "int"}, "a": {"aa": "int"}}, &Lineage{[]ColumnLineage{{"Z", []UpstreamColumn{{"a", "z"}, {"aa", "a"}}, "INT"}, {"a", uc("a", "z"), "INT"}}, []ColumnLineage{{"aa", uc("aa", "a"), ""}, {"a", uc("a", "z"), ""}}, noErrors}},
		// A missing branch position triggers per-column recovery: b disappears
		// without an in-band error, while a keeps upstreams from both branches.
		{"shorter second union branch silently loses output", "postgres", "SELECT a, b FROM l UNION ALL SELECT c FROM r", Schema{"l": {"a": "int", "b": "int"}, "r": {"c": "int"}}, &Lineage{[]ColumnLineage{{"a", []UpstreamColumn{{"a", "l"}, {"c", "r"}}, "INT"}}, empty, noErrors}},
		{"longer second union branch ignores extra output", "postgres", "SELECT a FROM l UNION ALL SELECT c, d FROM r", Schema{"l": {"a": "int"}, "r": {"c": "int", "d": "int"}}, &Lineage{[]ColumnLineage{{"a", []UpstreamColumn{{"a", "l"}, {"c", "r"}}, "INT"}}, empty, noErrors}},
		// Source-column extraction splits on dots and retains doubled quotes.
		{"quoted column names lose dots and retain escaping", "postgres", `SELECT t."a.b" AS dotted, t."a""b" AS quoted FROM t`, Schema{"t": {"a.b": "int", `a"b`: "bigint"}}, &Lineage{[]ColumnLineage{{"dotted", uc("b", "t"), "INT"}, {"quoted", uc(`a""b`, "t"), "BIGINT"}}, empty, noErrors}},
		{"unused cte contributes nonselected filters", "postgres", "WITH unused AS (SELECT id FROM hidden WHERE flag) SELECT id FROM visible", Schema{"hidden": {"id": "int", "flag": "bool"}, "visible": {"id": "bigint"}}, &Lineage{[]ColumnLineage{{"id", uc("id", "visible"), "BIGINT"}}, []ColumnLineage{{"flag", uc("flag", "hidden"), ""}}, noErrors}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.ColumnLineage(tc.query, tc.dialect, tc.schema)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSQLParserColumnLineageContractCaseCollision(t *testing.T) {
	// align_schema_casing iterates a Python set and copies only one spelling.
	// Seeds 1 and 2 produce opposite type omissions. Permit exactly those two
	// complete observed results, without weakening assertions for other cases.
	query := "SELECT a.id AS x, b.id AS y FROM raw.Teams a JOIN raw.TEAMS b ON a.id=b.id"
	got, err := sharedSQLParser.ColumnLineage(query, "bigquery", Schema{"raw.teams": {"id": "int64"}})
	require.NoError(t, err)
	filters := []ColumnLineage{{"id", []UpstreamColumn{{"id", "raw.TEAMS"}, {"id", "raw.Teams"}}, ""}}
	wantOneOf := []*Lineage{
		{[]ColumnLineage{{"x", uc("id", "raw.Teams"), "UNKNOWN"}, {"y", uc("id", "raw.TEAMS"), "BIGINT"}}, filters, []string{}},
		{[]ColumnLineage{{"x", uc("id", "raw.Teams"), "BIGINT"}, {"y", uc("id", "raw.TEAMS"), "UNKNOWN"}}, filters, []string{}},
	}
	require.Contains(t, wantOneOf, got)
}
