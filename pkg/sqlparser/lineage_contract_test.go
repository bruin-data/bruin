//nolint:paralleltest // all cases intentionally share TestMain's subprocess.
package sqlparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func uc(column, table string) []UpstreamColumn {
	return []UpstreamColumn{{Column: column, Table: table}}
}

func TestSQLParserColumnLineageContractCTEAndUnionAcrossDialects(t *testing.T) {
	query := "WITH x AS (SELECT a + b AS subtotal FROM left_rows) SELECT subtotal AS result FROM x UNION ALL SELECT c AS other FROM right_rows"
	schema := Schema{"left_rows": {"a": "bigint", "b": "bigint"}, "right_rows": {"c": "bigint"}}
	for _, dialect := range contractDialects {
		t.Run(dialect, func(t *testing.T) {
			want := []ColumnLineage{{"result", []UpstreamColumn{{"a", "left_rows"}, {"b", "left_rows"}, {"c", "right_rows"}}, "BIGINT"}}
			if uppercasesIdentifiers(dialect) {
				want = []ColumnLineage{{"RESULT", []UpstreamColumn{{"A", "LEFT_ROWS"}, {"B", "LEFT_ROWS"}, {"C", "RIGHT_ROWS"}}, "BIGINT"}}
			}
			got, err := sharedSQLParser.ColumnLineage(query, dialect, schema)
			require.NoError(t, err)
			require.Equal(t, &Lineage{Columns: want, NonSelectedColumns: []ColumnLineage{}, Errors: []string{}}, got)
		})
	}
}

func TestSQLParserColumnLineageContractAllDialects(t *testing.T) {
	query := `SELECT CAST(o.amount AS DECIMAL(12,2)) AS gross, CAST(c.name AS VARCHAR(40)) AS customer FROM sales.orders o JOIN crm.customers c ON o.customer_id=c.id WHERE o.active`
	schema := Schema{
		"sales.orders":  {"amount": "double", "customer_id": "int", "active": "bool"},
		"crm.customers": {"id": "int", "name": "string"},
	}
	for _, dialect := range contractDialects {
		t.Run(dialect, func(t *testing.T) {
			got, err := sharedSQLParser.ColumnLineage(query, dialect, schema)
			require.NoError(t, err)
			upper := uppercasesIdentifiers(dialect)
			customerType := "VARCHAR(40)"
			if dialect == "databricks" || dialect == "duckdb" || dialect == "spark" {
				customerType = "TEXT"
			}
			want := []ColumnLineage{
				{Name: "customer", Upstream: uc("name", "crm.customers"), Type: customerType},
				{Name: "gross", Upstream: uc("amount", "sales.orders"), Type: "DECIMAL(12, 2)"},
			}
			if upper {
				want = []ColumnLineage{
					{Name: "CUSTOMER", Upstream: uc("NAME", "CRM.CUSTOMERS"), Type: "VARCHAR(40)"},
					{Name: "GROSS", Upstream: uc("AMOUNT", "SALES.ORDERS"), Type: "DECIMAL(12, 2)"},
				}
			}
			wantNonSelected := []ColumnLineage{{"active", uc("active", "sales.orders"), ""}, {"customer_id", uc("customer_id", "sales.orders"), ""}, {"id", uc("id", "crm.customers"), ""}}
			if upper {
				wantNonSelected = []ColumnLineage{{"ACTIVE", uc("ACTIVE", "SALES.ORDERS"), ""}, {"CUSTOMER_ID", uc("CUSTOMER_ID", "SALES.ORDERS"), ""}, {"ID", uc("ID", "CRM.CUSTOMERS"), ""}}
			}
			require.Equal(t, &Lineage{Columns: want, NonSelectedColumns: wantNonSelected, Errors: []string{}}, got)
		})
	}
}

func TestSQLParserColumnLineageContractPlatformFeatures(t *testing.T) {
	tests := []struct {
		name, dialect, query string
		schema               Schema
		want                 []ColumnLineage
		wantErrors           []string
	}{
		{"bigquery arrays structs unnest qualify", "bigquery", `SELECT u.id, item.sku, ARRAY_LENGTH(u.tags) n FROM app.users u, UNNEST(u.items) item QUALIFY ROW_NUMBER() OVER(PARTITION BY u.id)=1`, Schema{"app.users": {"id": "INT64", "tags": "ARRAY<STRING>", "items": "ARRAY<STRUCT<sku STRING, qty INT64>>"}}, []ColumnLineage{{"id", uc("id", "app.users"), "BIGINT"}, {"n", uc("tags", "app.users"), "BIGINT"}, {"sku", uc("items", "app.users"), "TEXT"}}, nil},
		{"snowflake variant flatten qualify", "snowflake", `SELECT e.id, f.value:name::STRING AS name FROM events e, LATERAL FLATTEN(input => e.payload:items) f QUALIFY ROW_NUMBER() OVER(PARTITION BY e.id ORDER BY f.index)=1`, Schema{"events": {"id": "NUMBER", "payload": "VARIANT"}}, []ColumnLineage{{"ID", uc("ID", "EVENTS"), "DECIMAL(38, 0)"}, {"NAME", uc("PAYLOAD", "EVENTS"), "TEXT"}}, nil},
		{"postgres json lateral distinct on", "postgres", `SELECT DISTINCT ON (u.id) u.id, j.value->>'name' AS name FROM app.users u CROSS JOIN LATERAL jsonb_array_elements(u.payload->'items') j(value) ORDER BY u.id`, Schema{"app.users": {"id": "integer", "payload": "jsonb"}}, []ColumnLineage{{"id", uc("id", "app.users"), "INT"}, {"name", uc("payload", "app.users"), "UNKNOWN"}}, nil},
		{"duckdb struct list star exclude", "duckdb", `SELECT * EXCLUDE(secret), profile.name AS profile_name, list_transform(scores, x -> x + 1) AS bumped FROM users`, Schema{"users": {"id": "INTEGER", "secret": "VARCHAR", "profile": "STRUCT(name VARCHAR, age INTEGER)", "scores": "INTEGER[]"}}, []ColumnLineage{{"bumped", uc("scores", "users"), "UNKNOWN"}, {"id", uc("id", "users"), "INT"}, {"profile", uc("profile", "users"), "STRUCT<name TEXT, age INT>"}, {"profile_name", uc("profile", "users"), "TEXT"}, {"scores", uc("scores", "users"), "ARRAY<INT>"}}, nil},
		{"spark explode lambda", "spark", `SELECT id, x, transform(nums, n -> n + 1) AS bumped FROM src LATERAL VIEW explode(items) e AS x`, Schema{"src": {"id": "bigint", "items": "array<string>", "nums": "array<int>"}}, []ColumnLineage{{"bumped", uc("nums", "src"), "UNKNOWN"}, {"id", uc("id", "src"), "BIGINT"}, {"x", uc("items", "src"), "ARRAY<TEXT>"}}, nil},
		{"databricks explode lambda", "databricks", `SELECT id, x, transform(nums, n -> n + 1) AS bumped FROM src LATERAL VIEW explode(items) e AS x`, Schema{"src": {"id": "bigint", "items": "array<string>", "nums": "array<int>"}}, []ColumnLineage{{"bumped", uc("nums", "src"), "UNKNOWN"}, {"id", uc("id", "src"), "BIGINT"}, {"x", uc("items", "src"), "ARRAY<TEXT>"}}, nil},
		// SQLGlot currently cannot resolve the ARRAY JOIN alias; freeze the omission rather than claiming support.
		{"clickhouse array join sumIf unsupported", "clickhouse", `SELECT id, x, sumIf(amount, active) AS total FROM src ARRAY JOIN items AS x GROUP BY id, x`, Schema{"src": {"id": "UInt64", "items": "Array(String)", "amount": "Float64", "active": "Bool"}}, []ColumnLineage{}, []string{"Schema Error: Column 'x' could not be resolved. Line: 1, Col: 12"}},
		{"oracle hierarchy level", "oracle", `SELECT id, parent_id, LEVEL AS depth FROM nodes START WITH parent_id IS NULL CONNECT BY PRIOR id = parent_id`, Schema{"nodes": {"id": "NUMBER", "parent_id": "NUMBER"}}, []ColumnLineage{{"DEPTH", []UpstreamColumn{}, "UNKNOWN"}, {"ID", uc("ID", "NODES"), "DECIMAL"}, {"PARENT_ID", uc("PARENT_ID", "NODES"), "DECIMAL"}}, nil},
		{"mysql json", "mysql", `SELECT id, JSON_UNQUOTE(JSON_EXTRACT(payload, '$.name')) AS name FROM users`, Schema{"users": {"id": "int", "payload": "json"}}, []ColumnLineage{{"id", uc("id", "users"), "INT"}, {"name", uc("payload", "users"), "UNKNOWN"}}, nil},
		{"doris arrays bitmap", "doris", `SELECT id, ARRAY_SIZE(tags) AS n, BITMAP_COUNT(users) AS users FROM metrics`, Schema{"metrics": {"id": "BIGINT", "tags": "ARRAY<STRING>", "users": "BITMAP"}}, []ColumnLineage{{"id", uc("id", "metrics"), "BIGINT"}, {"n", uc("tags", "metrics"), "BIGINT"}, {"users", uc("users", "metrics"), "UNKNOWN"}}, nil},
		{"starrocks arrays bitmap", "starrocks", `SELECT id, ARRAY_LENGTH(tags) AS n, BITMAP_COUNT(users) AS users FROM metrics`, Schema{"metrics": {"id": "BIGINT", "tags": "ARRAY<STRING>", "users": "BITMAP"}}, []ColumnLineage{{"id", uc("id", "metrics"), "BIGINT"}, {"n", uc("tags", "metrics"), "BIGINT"}, {"users", uc("users", "metrics"), "UNKNOWN"}}, nil},
		// Ordinality currently inherits the array element type, not an integer type.
		{"trino unnest ordinality", "trino", `SELECT e.id, u.item, u.n FROM events e CROSS JOIN UNNEST(e.items) WITH ORDINALITY AS u(item,n)`, Schema{"events": {"id": "bigint", "items": "array(varchar)"}}, []ColumnLineage{{"id", uc("id", "events"), "BIGINT"}, {"item", uc("items", "events"), "VARCHAR"}, {"n", uc("items", "events"), "VARCHAR"}}, nil},
		{"athena row cast", "athena", `SELECT id, CAST(ROW(id, name) AS ROW(k BIGINT, label VARCHAR)) AS record FROM users`, Schema{"users": {"id": "bigint", "name": "varchar"}}, []ColumnLineage{{"id", uc("id", "users"), "BIGINT"}, {"record", []UpstreamColumn{{"id", "users"}, {"name", "users"}}, "STRUCT<k BIGINT, label VARCHAR>"}}, nil},
		// The correlated aggregate loses the payment amount dependency today.
		{"tsql cross apply", "tsql", `SELECT o.id, x.total FROM orders o CROSS APPLY (SELECT SUM(amount) AS total FROM payments p WHERE p.order_id=o.id) x`, Schema{"orders": {"id": "int"}, "payments": {"order_id": "int", "amount": "decimal(10,2)"}}, []ColumnLineage{{"id", uc("id", "orders"), "INT"}, {"total", uc("id", "orders"), "UNKNOWN"}}, nil},
		{"fabric pivot unresolved", "fabric", `SELECT customer, [Q1] FROM sales PIVOT(SUM(amount) FOR quarter IN ([Q1], [Q2])) p`, Schema{"sales": {"customer": "int", "quarter": "varchar", "amount": "int"}}, []ColumnLineage{}, []string{"Schema Error: Ambiguous column 'Q1' (Line: 1, Col: 71)"}},
		{"redshift json path", "redshift", `SELECT id, JSON_EXTRACT_PATH_TEXT(payload, 'name') AS name FROM events`, Schema{"events": {"id": "bigint", "payload": "varchar"}}, []ColumnLineage{{"id", uc("id", "events"), "BIGINT"}, {"name", uc("payload", "events"), "UNKNOWN"}}, nil},
		{"clickhouse aggregate combinators", "clickhouse", `SELECT sumIf(amount, active) AS total, uniqExact(user_id) AS users FROM events`, Schema{"events": {"amount": "Float64", "active": "Bool", "user_id": "UInt64"}}, []ColumnLineage{{"total", []UpstreamColumn{{"active", "events"}, {"amount", "events"}}, "UNKNOWN"}, {"users", uc("user_id", "events"), "UNKNOWN"}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sharedSQLParser.ColumnLineage(tt.query, tt.dialect, tt.schema)
			require.NoError(t, err)
			wantNonSelected := []ColumnLineage{}
			switch tt.name {
			case "bigquery arrays structs unnest qualify":
				wantNonSelected = []ColumnLineage{{"items", uc("items", "app.users"), ""}}
			case "snowflake variant flatten qualify":
				wantNonSelected = []ColumnLineage{{"PAYLOAD", uc("PAYLOAD", "EVENTS"), ""}}
			case "postgres json lateral distinct on":
				wantNonSelected = []ColumnLineage{{"payload", uc("payload", "app.users"), ""}}
			case "trino unnest ordinality":
				wantNonSelected = []ColumnLineage{{"items", uc("items", "events"), ""}}
			case "tsql cross apply":
				wantNonSelected = []ColumnLineage{{"id", uc("id", "orders"), ""}, {"order_id", uc("order_id", "payments"), ""}}
			}
			if tt.wantErrors == nil {
				tt.wantErrors = []string{}
			}
			require.Equal(t, &Lineage{Columns: tt.want, NonSelectedColumns: wantNonSelected, Errors: tt.wantErrors}, got)
		})
	}
}

func TestSQLParserNonSelectedLineageContractAllDialects(t *testing.T) {
	query := `SELECT o.amount FROM sales.orders o JOIN crm.customers c ON o.customer_id=c.id WHERE o.active GROUP BY o.amount`
	schema := Schema{"sales.orders": {"amount": "double", "customer_id": "int", "active": "bool"}, "crm.customers": {"id": "int"}}
	for _, dialect := range contractDialects {
		t.Run(dialect, func(t *testing.T) {
			got, err := sharedSQLParser.ColumnLineage(query, dialect, schema)
			require.NoError(t, err)
			want := []ColumnLineage{{"active", uc("active", "sales.orders"), ""}, {"amount", uc("amount", "sales.orders"), ""}, {"customer_id", uc("customer_id", "sales.orders"), ""}, {"id", uc("id", "crm.customers"), ""}}
			if uppercasesIdentifiers(dialect) {
				want = []ColumnLineage{{"ACTIVE", uc("ACTIVE", "SALES.ORDERS"), ""}, {"AMOUNT", uc("AMOUNT", "SALES.ORDERS"), ""}, {"CUSTOMER_ID", uc("CUSTOMER_ID", "SALES.ORDERS"), ""}, {"ID", uc("ID", "CRM.CUSTOMERS"), ""}}
			}
			columns := []ColumnLineage{{"amount", uc("amount", "sales.orders"), "DOUBLE"}}
			if uppercasesIdentifiers(dialect) {
				columns = []ColumnLineage{{"AMOUNT", uc("AMOUNT", "SALES.ORDERS"), "DOUBLE"}}
			}
			require.Equal(t, &Lineage{Columns: columns, NonSelectedColumns: want, Errors: []string{}}, got)
		})
	}
}

func TestSQLParserColumnLineageProjectionContracts(t *testing.T) {
	cases := []struct {
		name, dialect, query string
		schema               Schema
		want                 []ColumnLineage
		wantErrors           []string
	}{
		// Duplicate output names use the FIRST expression's upstream for both
		// outputs, but retain each expression's independently inferred type.
		{"duplicate aliases", "postgres", "SELECT a AS x, b AS x FROM t", Schema{"t": {"a": "int", "b": "varchar"}}, []ColumnLineage{{"x", uc("a", "t"), "INT"}, {"x", uc("a", "t"), "VARCHAR"}}, nil},
		{"missing schema infers unknown type", "postgres", "SELECT missing AS x FROM t", Schema{}, []ColumnLineage{{"x", uc("missing", "t"), "UNKNOWN"}}, nil},
		{"star without schema stays star", "postgres", "SELECT * FROM t", Schema{}, []ColumnLineage{{"*", uc("*", "t"), "UNKNOWN"}}, nil},
		{"star expands and sorts names", "postgres", "SELECT t.* FROM t", Schema{"t": {"z": "bigint", "a": "boolean"}}, []ColumnLineage{{"a", uc("a", "t"), "BOOLEAN"}, {"z", uc("z", "t"), "BIGINT"}}, nil},
		{"known schema missing column", "postgres", "SELECT id FROM known", Schema{"known": {"other": "int"}}, []ColumnLineage{}, []string{"Schema Error: Column 'id' could not be resolved. Line: 1, Col: 9"}},
		{"ambiguous join", "postgres", "SELECT id FROM a JOIN b ON a.id=b.id", Schema{"a": {"id": "int"}, "b": {"id": "int"}}, []ColumnLineage{}, []string{"Schema Error: Column 'id' could not be resolved. Line: 1, Col: 9"}},
		{"window dependencies include partition and ordering", "postgres", "SELECT SUM(amount) OVER(PARTITION BY region ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rolling FROM t", Schema{"t": {"amount": "int", "region": "varchar", "ts": "timestamp"}}, []ColumnLineage{{"rolling", []UpstreamColumn{{"amount", "t"}, {"region", "t"}, {"ts", "t"}}, "BIGINT"}}, nil},
		{"count star has no upstream", "postgres", "SELECT COUNT(*) AS n FROM t", Schema{"t": {"id": "int"}}, []ColumnLineage{{"n", []UpstreamColumn{}, "BIGINT"}}, nil},
		{"union takes first branch type", "postgres", "SELECT a AS out FROM left_rows UNION ALL SELECT b AS other FROM right_rows", Schema{"left_rows": {"a": "int"}, "right_rows": {"b": "bigint"}}, []ColumnLineage{{"out", []UpstreamColumn{{"a", "left_rows"}, {"b", "right_rows"}}, "INT"}}, nil},
		{"star except replace", "bigquery", "SELECT * EXCEPT(secret) REPLACE(amount * 2 AS amount) FROM t", Schema{"t": {"id": "int64", "secret": "string", "amount": "numeric"}}, []ColumnLineage{{"amount", uc("amount", "t"), "DECIMAL"}, {"id", uc("id", "t"), "BIGINT"}}, nil},
		{"quoted mixed case schema unresolved", "postgres", `SELECT "Mixed" AS "Output" FROM "raw"."Orders"`, Schema{"raw.Orders": {"Mixed": "int"}}, []ColumnLineage{}, []string{"Schema Error: Column 'Mixed' could not be resolved. Line: 1, Col: 14"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.ColumnLineage(tc.query, tc.dialect, tc.schema)
			require.NoError(t, err)
			wantErrors := tc.wantErrors
			if wantErrors == nil {
				wantErrors = []string{}
			}
			require.Equal(t, &Lineage{Columns: tc.want, NonSelectedColumns: []ColumnLineage{}, Errors: wantErrors}, got)
		})
	}
}

func TestSQLParserNonSelectedLineageClauseContracts(t *testing.T) {
	cases := []struct {
		name, dialect, query string
		schema               Schema
		want                 []ColumnLineage
	}{
		{"where deduplicates", "postgres", "SELECT amount FROM t WHERE active AND (id>2 OR id<0)", Schema{"t": {"amount": "int", "active": "bool", "id": "int"}}, []ColumnLineage{{"active", uc("active", "t"), ""}, {"id", uc("id", "t"), ""}}},
		// This API only tracks WHERE, JOIN and GROUP BY, not every column used
		// outside the projection. Keep the omissions visible to a replacement.
		{"having and order omitted", "postgres", "SELECT SUM(amount) AS total FROM t HAVING MAX(flag)>0 ORDER BY MIN(sortkey)", Schema{"t": {"amount": "int", "flag": "int", "sortkey": "int"}}, []ColumnLineage{}},
		{"qualify omitted", "bigquery", "SELECT amount FROM t QUALIFY ROW_NUMBER() OVER(PARTITION BY grp ORDER BY ts)=1", Schema{"t": {"amount": "int", "grp": "int", "ts": "timestamp"}}, []ColumnLineage{}},
		{"window clauses omitted", "postgres", "SELECT SUM(amount) OVER(PARTITION BY region ORDER BY ts) AS total FROM t", Schema{"t": {"amount": "int", "region": "varchar", "ts": "timestamp"}}, []ColumnLineage{}},
		{"cte filters reach physical table", "postgres", "WITH x AS (SELECT id, amount FROM t WHERE active) SELECT amount FROM x WHERE id>2", Schema{"t": {"id": "int", "amount": "int", "active": "bool"}}, []ColumnLineage{{"active", uc("active", "t"), ""}, {"id", uc("id", "t"), ""}}},
		{"same column from different tables", "postgres", "SELECT a.value FROM a JOIN b ON a.id=b.id WHERE a.id>2 OR b.id<9", Schema{"a": {"id": "int", "value": "int"}, "b": {"id": "int"}}, []ColumnLineage{{"id", []UpstreamColumn{{"id", "a"}, {"id", "b"}}, ""}}},
		{"using join becomes predicate", "postgres", "SELECT id FROM a FULL JOIN b USING(id)", Schema{"a": {"id": "int"}, "b": {"id": "int"}}, []ColumnLineage{{"id", []UpstreamColumn{{"id", "a"}, {"id", "b"}}, ""}}},
		{"unnest input is join dependency", "trino", "SELECT u.item FROM events e CROSS JOIN UNNEST(e.items) AS u(item)", Schema{"events": {"items": "array(varchar)"}}, []ColumnLineage{{"items", uc("items", "events"), ""}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.ColumnLineage(tc.query, tc.dialect, tc.schema)
			require.NoError(t, err)
			var columns []ColumnLineage
			switch tc.name {
			case "where deduplicates":
				columns = []ColumnLineage{{"amount", uc("amount", "t"), "INT"}}
			case "having and order omitted":
				columns = []ColumnLineage{{"total", uc("amount", "t"), "BIGINT"}}
			case "qualify omitted":
				columns = []ColumnLineage{{"amount", uc("amount", "t"), "INT"}}
			case "window clauses omitted":
				columns = []ColumnLineage{{"total", []UpstreamColumn{{"amount", "t"}, {"region", "t"}, {"ts", "t"}}, "BIGINT"}}
			case "cte filters reach physical table":
				columns = []ColumnLineage{{"amount", uc("amount", "t"), "INT"}}
			case "same column from different tables":
				columns = []ColumnLineage{{"value", uc("value", "a"), "INT"}}
			case "using join becomes predicate":
				columns = []ColumnLineage{{"id", []UpstreamColumn{{"id", "a"}, {"id", "b"}}, "INT"}}
			case "unnest input is join dependency":
				columns = []ColumnLineage{{"item", uc("items", "events"), "VARCHAR"}}
			}
			require.Equal(t, &Lineage{Columns: columns, NonSelectedColumns: tc.want, Errors: []string{}}, got)
		})
	}
}

func TestSQLParserColumnLineageContractBoundaryAndInvalidInputs(t *testing.T) {
	parser, err := NewSQLParserCached()
	require.NoError(t, err)
	parser.MaxQueryLength = 8
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	require.NoError(t, parser.Start())

	atBoundary, err := parser.ColumnLineage("SELECT 1", "postgres", Schema{})
	require.NoError(t, err)
	// The exact boundary is sent to the subprocess (rather than rejected by Go).
	require.Equal(t, &Lineage{Columns: []ColumnLineage{{"1", []UpstreamColumn{}, "INT"}}, NonSelectedColumns: []ColumnLineage{}, Errors: []string{}}, atBoundary)

	overBoundary, err := parser.ColumnLineage(strings.Repeat("x", 9), "postgres", Schema{})
	require.NoError(t, err)
	require.Equal(t, &Lineage{Columns: []ColumnLineage{}, NonSelectedColumns: []ColumnLineage{}, Errors: []string{"query is too long skipping column lineage analysis"}}, overBoundary)

	// nil serializes to null. Python fails before producing a lineage response;
	// Go ignores the singular "error" field and returns the zero value, not a Go error.
	nilSchema, err := sharedSQLParser.ColumnLineage("SELECT 1 AS one", "postgres", nil)
	require.NoError(t, err)
	require.Equal(t, &Lineage{}, nilSchema)
	emptySchema, err := sharedSQLParser.ColumnLineage("SELECT 1 AS one", "postgres", Schema{})
	require.NoError(t, err)
	require.Equal(t, &Lineage{Columns: []ColumnLineage{{"one", []UpstreamColumn{}, "INT"}}, NonSelectedColumns: []ColumnLineage{}, Errors: []string{}}, emptySchema)

	malformed, err := sharedSQLParser.ColumnLineage("SELECT FROM", "postgres", Schema{})
	require.NoError(t, err)
	require.Equal(t, &Lineage{Columns: []ColumnLineage{}, NonSelectedColumns: []ColumnLineage{}, Errors: []string{"Parse error: " + contractSelectFromError}}, malformed)

	// Multiple statements become a Block rather than a Query, rejected in-band.
	multi, err := sharedSQLParser.ColumnLineage("SELECT 1 AS first; SELECT 2 AS second", "postgres", Schema{})
	require.NoError(t, err)
	require.Equal(t, &Lineage{Columns: []ColumnLineage{}, NonSelectedColumns: []ColumnLineage{}, Errors: []string{"Failed to parse query"}}, multi)

	unknown, err := sharedSQLParser.ColumnLineage("SELECT 1", "not-a-dialect", Schema{})
	require.NoError(t, err)
	require.Equal(t, &Lineage{Columns: []ColumnLineage{}, NonSelectedColumns: []ColumnLineage{}, Errors: []string{"Parse error: Unknown dialect 'not-a-dialect'."}}, unknown)
}

// uppercasesIdentifiers reports whether the dialect normalizes unquoted identifiers to upper case.
func uppercasesIdentifiers(dialect string) bool {
	return dialect == "oracle" || dialect == "snowflake" //nolint:goconst // dialect names read better inline in the case tables
}
