package sqlengine

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// regressionLineage computes column lineage the way Bruin's commands do: the lineage optimizer
// rules, then every rule if those fail. It returns each output column as name=[table.column ...]
// (the table columns it reads, sorted) and the optimized query, or "error: ..." and "".
func regressionLineage(dialect, sql string, schema *SchemaMap) (columns, optimized string) {
	d := MustDialect(dialect)
	if schema == nil {
		schema = NewSchemaMap()
	}
	parsed, err := d.ParseOne(sql, nil)
	if err != nil {
		return "error: " + err.Error(), ""
	}
	opt, err := OptimizeSafe(parsed.Copy(), schema, d, lineageRules)
	if err != nil {
		if opt, err = OptimizeSafe(parsed, schema, d, nil); err != nil {
			return "error: " + err.Error(), ""
		}
	}
	_, graph, err := LineageAll(opt, LineageOptions{
		Schema: NewMappingSchema(schema, nil, d, true, nil), Dialect: d, Scope: BuildScope(opt),
	})
	if err != nil {
		return "error: " + err.Error(), ""
	}

	var out []string
	for _, s := range opt.Selects() {
		name := s.AliasOrName()
		var reads []string
		if node, ok := graph[name]; ok {
			seen := map[string]bool{}
			for _, n := range node.Walk() {
				// Leaves that are tables, leaving out table functions
				if len(n.Downstream) > 0 || !n.Expression.IsA(KTable) || n.Expression.This().IsA(KFunc) {
					continue
				}
				var parts []string
				for _, p := range n.Expression.Parts() {
					parts = append(parts, p.Name())
				}
				read := strings.Join(parts, ".") + "." + n.Name[strings.LastIndexByte(n.Name, '.')+1:]
				if !seen[read] {
					seen[read] = true
					reads = append(reads, read)
				}
			}
		}
		sort.Strings(reads)
		out = append(out, fmt.Sprintf("%s=[%s]", name, strings.Join(reads, " ")))
	}
	optimized, err = d.Generate(opt, nil)
	if err != nil {
		return "error: " + err.Error(), ""
	}
	return strings.Join(out, " "), optimized
}

type lineageRegressionCase struct {
	name, dialect, sql string
	schema             *SchemaMap
	// want is the lineage summary of regressionLineage
	want string
	// wantSQL, when set, is the optimized query
	wantSQL string
}

func runLineageRegressionCases(t *testing.T, cases []lineageRegressionCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, gotSQL := regressionLineage(c.dialect, c.sql, c.schema)
			if got != c.want {
				t.Errorf("[%s] %s\n  want: %s\n  got:  %s", c.dialect, c.sql, c.want, got)
			}
			if c.wantSQL != "" && gotSQL != c.wantSQL {
				t.Errorf("[%s] %s\n  want SQL: %s\n  got SQL:  %s", c.dialect, c.sql, c.wantSQL, gotSQL)
			}
		})
	}
}

// TestLineagePseudocolumnHasNoSource checks that pseudocolumns, which the query computes, are
// never attributed to a source, even when the query reads a single source.
func TestLineagePseudocolumnHasNoSource(t *testing.T) {
	t.Parallel()
	nodes := qtSchema("nodes", qtSchema("id", "NUMBER", "parent_id", "NUMBER"))
	runLineageRegressionCases(t, []lineageRegressionCase{
		{
			name: "level with schema", dialect: "oracle", schema: nodes,
			sql:  "SELECT id, parent_id, LEVEL AS depth FROM nodes START WITH parent_id IS NULL CONNECT BY PRIOR id = parent_id",
			want: "ID=[NODES.ID] PARENT_ID=[NODES.PARENT_ID] DEPTH=[]",
		},
		{
			name: "level without schema", dialect: "oracle",
			sql:  "SELECT id, LEVEL AS depth FROM nodes CONNECT BY PRIOR id = parent_id",
			want: "ID=[NODES.ID] DEPTH=[]",
		},
		{
			name: "rownum", dialect: "oracle",
			sql:  "SELECT ROWNUM AS rn, name FROM people",
			want: "RN=[] NAME=[PEOPLE.NAME]",
		},
	})
}

// TestLineageFunctionSourceReferencedByName checks that a column naming a source produced by a
// function (UNNEST, a table function, LATERAL) or a source with unknown columns refers to that
// source, so the query still resolves.
func TestLineageFunctionSourceReferencedByName(t *testing.T) {
	t.Parallel()
	x := qtSchema("x", qtSchema("id", "INT", "a", "INT[]", "payload", "JSONB"))
	manufacturers := qtSchema("manufacturers", qtSchema("id", "INT", "name", "TEXT"))
	runLineageRegressionCases(t, []lineageRegressionCase{
		{
			name: "lateral unnest", dialect: "postgres", schema: x,
			sql:  "SELECT x.id, elem FROM x CROSS JOIN LATERAL UNNEST(x.a) AS elem",
			want: "id=[x.id] elem=[]",
		},
		{
			name: "lateral unnest without schema", dialect: "postgres",
			sql:  "SELECT x.id, elem FROM x CROSS JOIN LATERAL UNNEST(x.a) AS elem",
			want: "id=[x.id] elem=[]",
		},
		{
			name: "table function", dialect: "postgres", schema: x,
			sql:  "SELECT x.id, elem FROM x, jsonb_array_elements(x.payload) AS elem",
			want: "id=[x.id] elem=[]",
		},
		{
			name: "table function without schema", dialect: "postgres",
			sql:  "SELECT x.id, elem FROM x, jsonb_array_elements(x.payload) AS elem",
			want: "id=[x.id] elem=[]",
		},
		{
			name: "lateral table function in where", dialect: "postgres", schema: manufacturers,
			sql:  "SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) pname ON TRUE WHERE pname IS NULL",
			want: "name=[manufacturers.name]",
		},
		{
			name: "lateral table function in where without schema", dialect: "postgres",
			sql:  "SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) AS pname ON TRUE WHERE pname IS NULL",
			want: "name=[manufacturers.name]",
		},
		{
			name: "table function in a correlated subquery", dialect: "postgres", schema: x,
			sql:  "SELECT x.id FROM x WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(x.payload) AS e WHERE e IS NOT NULL)",
			want: "id=[x.id]",
		},
		{
			name: "array path", dialect: "bigquery",
			sql:  "SELECT results FROM Coordinates, `Coordinates.position` AS results",
			want: "results=[]",
		},
		{
			name: "array path of a cte", dialect: "bigquery", schema: qtSchema("w", qtSchema("a", "INT64")),
			sql:  "WITH Coordinates AS (SELECT [1, 2] AS position) SELECT results FROM Coordinates, `Coordinates.position` AS results",
			want: "results=[]",
		},
		{
			// t has known columns but read_parquet's could include t, so t stays ambiguous
			name: "known source shadowed by an unknown one", dialect: "duckdb", schema: qtSchema("t", qtSchema("a", "INT")),
			sql:  "SELECT t FROM t, read_parquet('f.parquet') AS p",
			want: "error: Column 't' could not be resolved. Line: 1, Col: 8",
		},
	})
}

// TestLineageAliasOverUnknownTableFunction checks that, with a table function whose columns are
// unknown, a name that is also a projection alias is taken to be the alias.
func TestLineageAliasOverUnknownTableFunction(t *testing.T) {
	t.Parallel()
	runLineageRegressionCases(t, []lineageRegressionCase{
		{
			name: "group by alias over read_parquet", dialect: "duckdb",
			sql:     "SELECT date_trunc('day', ts) AS day, count(*) AS n FROM read_parquet('s3://b/x.parquet') GROUP BY day",
			want:    "day=[] n=[]",
			wantSQL: "SELECT DATE_TRUNC('DAY', _0.ts) AS day, COUNT(*) AS n FROM READ_PARQUET('s3://b/x.parquet') AS _0 GROUP BY DATE_TRUNC('DAY', _0.ts)",
		},
		{
			name: "where alias over read_csv", dialect: "duckdb",
			sql:     "SELECT a + 1 AS y FROM read_csv('f.csv') WHERE y > 1",
			want:    "y=[]",
			wantSQL: "SELECT _0.a + 1 AS y FROM READ_CSV('f.csv') AS _0 WHERE (_0.a + 1) > 1",
		},
		{
			name: "having alias over range", dialect: "duckdb",
			sql:     "SELECT SUM(r.range) AS x FROM range(3) AS r HAVING x > 1",
			want:    "x=[]",
			wantSQL: "SELECT SUM(r.range) AS x FROM RANGE(0, 3) AS r HAVING SUM(r.range) > 1",
		},
		{
			name: "group by alias over s3", dialect: "clickhouse",
			sql:     "SELECT toDate(ts) AS d, count() FROM s3('https://b/x.csv') GROUP BY d",
			want:    "d=[] _col_1=[]",
			wantSQL: "SELECT toDate(_0.ts) AS d, count() AS _col_1 FROM s3('https://b/x.csv') AS _0 GROUP BY toDate(_0.ts)",
		},
		{
			name: "qualify alias over numbers", dialect: "clickhouse",
			sql:  "SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count FROM numbers(10) WINDOW window_name AS (PARTITION BY number) QUALIFY partition_count = 4 ORDER BY number",
			want: "number=[] partition_count=[]",
		},
		{
			name: "group by alias over a joined external query", dialect: "bigquery", schema: qtSchema("t", qtSchema("id", "INT64")),
			sql:     "SELECT t.id, DATE(e.ts) AS day FROM t JOIN EXTERNAL_QUERY('conn', 'select 1') e ON e.id = t.id GROUP BY t.id, day",
			want:    "id=[t.id] day=[]",
			wantSQL: "SELECT t.id AS id, DATE(e.ts) AS day FROM t AS t JOIN EXTERNAL_QUERY('conn', 'select 1') AS e ON e.id = t.id GROUP BY t.id, DATE(e.ts)",
		},
		{
			name: "alias reading a table", dialect: "duckdb", schema: qtSchema("t", qtSchema("id", "INT", "v", "INT")),
			sql:  "SELECT t.v * 2 AS w FROM t JOIN read_parquet('f.parquet') AS p ON p.id = t.id WHERE w > 1",
			want: "w=[t.v]",
		},
		{
			// The subquery's range may be q's column or p's, which a correlated reference can see
			name: "correlated reference stays ambiguous", dialect: "duckdb", schema: qtSchema("t", qtSchema("a", "INT")),
			sql:  "SELECT p.range FROM (VALUES (0), (6)) AS p(range) WHERE EXISTS (SELECT 1 FROM range(1) AS q WHERE range > 5)",
			want: "error: Cannot resolve 'range' due to missing source schemas",
		},
	})
}

func TestDuckDBQuotesReservedKeywordIdentifiers(t *testing.T) {
	t.Parallel()
	d := MustDialect("duckdb")
	for sql, want := range map[string]string{
		"SELECT * FROM t1 AS IN":                  `SELECT * FROM t1 AS "IN"`,
		"SELECT t.group, t.desc FROM t":           `SELECT t."group", t."desc" FROM t`,
		"REGEXP_EXTRACT(subject, pattern, group)": `REGEXP_EXTRACT(subject, pattern, "group")`,
		"SELECT a AS qualify FROM t":              `SELECT a AS "qualify" FROM t`,
		"SELECT * FROM t1 AS x":                   "SELECT * FROM t1 AS x",
	} {
		e, err := d.ParseOne(sql, nil)
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			continue
		}
		if got, err := d.Generate(e, nil); err != nil || got != want {
			t.Errorf("%s\n  want: %s\n  got:  %s (error %v)", sql, want, got, err)
		}
	}
}
