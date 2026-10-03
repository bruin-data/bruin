package sqlengine

import (
	"strings"
	"testing"
)

// Cases from sqlglot tests/fixtures/optimizer/simplify.sql (dialect, sql, expected). Like the
// Python test harness, each case is parsed, annotated with the test schema, simplified with
// constant_propagation=True and coalesce_simplification=True, and generated.
var simplifyFixtureCases = [][3]string{
	{"", "x AND x", "x AND TRUE"},
	{"", "x AND NOT x", "NOT x AND x"},
	{"", "x OR NOT x", "NOT x OR x"},
	{"", "FALSE AND TRUE AND TRUE", "FALSE"},
	{"", "1.0 = 1", "TRUE"},
	{"", "CAST('2023-01-01' AS DATE) = CAST('2023-01-01' AS DATE)", "TRUE"},
	{"", "NULL AND TRUE", "NULL AND TRUE"},
	{"", "NOT TRUE", "FALSE"},
	{"", "NOT FALSE", "TRUE"},
	{"", "NULL = NULL", "NULL = NULL"},
	{"", "SELECT (EXISTS(SELECT 1 WHERE FALSE)) AND NULL", "SELECT EXISTS(SELECT 1 WHERE FALSE) AND NULL"},
	{"", "SELECT NULL AND (EXISTS(SELECT 1 WHERE FALSE))", "SELECT EXISTS(SELECT 1 WHERE FALSE) AND NULL"},
	{"", "NULL AND 0", "FALSE"},
	{"mysql", "A XOR A", "A XOR A"},
	{"mysql", "SELECT DISTINCT GREATEST(EXISTS(SELECT 1 WHERE FALSE), (EXISTS(SELECT 1 WHERE FALSE)) XOR ((0.08) IN ((t1.c0) XOR (t1.c0)))) AS ref0 FROM (SELECT NULL AS c0 UNION ALL SELECT 1 AS c0) AS t1, (SELECT 0.01 AS c1) AS t0", "SELECT DISTINCT GREATEST(EXISTS(SELECT 1 WHERE FALSE), 0.08 IN (t1.c0 XOR t1.c0) XOR EXISTS(SELECT 1 WHERE FALSE)) AS ref0 FROM (SELECT NULL AS c0 UNION ALL SELECT 1 AS c0) AS t1, (SELECT 0.01 AS c1) AS t0"},
	{"", "COALESCE(x, y) <> ALL (SELECT z FROM w)", "COALESCE(x, y) <> ALL (SELECT z FROM w)"},
	{"", "SELECT t_bool.a AND TRUE FROM t_bool", "SELECT t_bool.a FROM t_bool"},
	{"", "SELECT TRUE AND t_bool.a FROM t_bool", "SELECT t_bool.a FROM t_bool"},
	{"", "SELECT t_bool.a OR FALSE FROM t_bool", "SELECT t_bool.a FROM t_bool"},
	{"", "SELECT FALSE OR t_bool.a FROM t_bool", "SELECT t_bool.a FROM t_bool"},
	{"", "(A AND B) OR A", "A AND TRUE"},
	{"", "A AND (B AND C) AND (D AND E)", "A AND B AND C AND D AND E"},
	{"", "(A OR NOT B) AND (A OR B)", "A AND TRUE"},
	{"", "SELECT t_bool.a OR t_bool.a FROM t_bool", "SELECT t_bool.a FROM t_bool"},
	{"", "SELECT t_bool.a AND t_bool.a FROM t_bool", "SELECT t_bool.a FROM t_bool"},
	{"", "SELECT SUM(t.x AND t.x) FROM t", "SELECT SUM(t.x AND TRUE) FROM t"},
	{"", "(x * 2) * 4 + (1 + 3) + 5", "x * 8 + 9"},
	{"", "(x - 1) - 2", "(x - 1) - 2"},
	{"", "x - (3 - 2)", "x - 1"},
	{"mysql", "A XOR D XOR B XOR E XOR F XOR G XOR C", "A XOR B XOR C XOR D XOR E XOR F XOR G"},
	{"", "SELECT x WHERE TRUE", "SELECT x"},
	{"", "SELECT x FROM y JOIN z ON TRUE", "SELECT x FROM y CROSS JOIN z"},
	{"", "SELECT x FROM y RIGHT JOIN z ON TRUE", "SELECT x FROM y CROSS JOIN z"},
	{"", "SELECT x FROM y LEFT JOIN z ON TRUE", "SELECT x FROM y LEFT JOIN z ON TRUE"},
	{"", "SELECT x FROM y FULL OUTER JOIN z ON TRUE", "SELECT x FROM y FULL OUTER JOIN z ON TRUE"},
	{"", "(FALSE OR TRUE)", "TRUE"},
	{"", "x * (1 - y)", "x * (1 - y)"},
	{"", "ANY(t.value)", "ANY(t.value)"},
	{"", "SELECT -(x.a > x.b) FROM x", "SELECT -(x.a > x.b) FROM x"},
	{"", "SELECT (-((x.a) IS NULL)) FROM x", "SELECT -(x.a IS NULL) FROM x"},
	{"", "SELECT * FROM A WHERE a - (b < c) < 0 AND a + (b > c) >= 0", "SELECT * FROM A WHERE a + (b > c) >= 0 AND a - (b < c) < 0"},
	{"", "1.2E+1 + 15E-3", "12.015"},
	{"", "1.2E1 + 15E-3", "12.015"},
	{"", "3 * 4", "12"},
	{"", "1 / 3", "1 / 3"},
	{"", "1 / 3.0", "0.3333333333333333333333333333"},
	{"", "20.0 / 6", "3.333333333333333333333333333"},
	{"", "10 / 5", "10 / 5"},
	{"", "(1.0 * 3) * 4 - 2 * (5 / 2)", "12.0 - 2 * (5 / 2)"},
	{"", "a * 0.5 - 10 - (2.0 + 3)", "a * 0.5 - 10 - 5.0"},
	{"", "2 <= 2", "TRUE"},
	{"", "1 IS NULL", "FALSE"},
	{"", "NULL IS NULL", "TRUE"},
	{"", "NULL IS NOT NULL", "FALSE"},
	{"", "CAST(x AS DATE) + interval '1' week", "CAST(x AS DATE) + INTERVAL '1' WEEK"},
	{"", "CAST('2008-11-11' AS DATETIME) + INTERVAL '5' MONTH", "CAST('2009-04-11 00:00:00' AS DATETIME)"},
	{"", "CAST(x AS DATETIME) + interval '1' WEEK", "CAST(x AS DATETIME) + INTERVAL '1' WEEK"},
	{"bigquery", "CAST('2023-01-01' AS TIMESTAMP) + INTERVAL 1 DAY", "CAST('2023-01-02 00:00:00' AS TIMESTAMP)"},
	{"bigquery", "INTERVAL 1 DAY + CAST('2023-01-01' AS TIMESTAMP)", "CAST('2023-01-02 00:00:00' AS TIMESTAMP)"},
	{"bigquery", "CAST('2023-01-02' AS TIMESTAMP) - INTERVAL 1 DAY", "CAST('2023-01-01 00:00:00' AS TIMESTAMP)"},
	{"", "DATETIME_SUB(CAST('2023-01-02' AS DATETIME), 1 + 1, 'HOUR')", "CAST('2023-01-01 22:00:00' AS DATETIME)"},
	{"", "x <= 1 OR x > 0", "x <= 1 OR x > 0"},
	{"", "x <= 1 OR x > 0", "x <= 1 OR x > 0"},
	{"", "x <= 0 AND x >= 0", "x <= 0 AND x >= 0"},
	{"", "x < 1 AND x <= 1", "x < 1"},
	{"", "x >= 1 OR x > 1", "x >= 1"},
	{"", "x < 2 AND x > 1", "x < 2 AND x > 1"},
	{"", "x = 1 AND x <> 2", "x = 1"},
	{"", "x BETWEEN 0 AND 5 AND x > 3", "x <= 5 AND x > 3"},
	{"", "x > 3 AND 5 > x AND x BETWEEN 0 AND 10", "x < 5 AND x > 3"},
	{"", "x > 3 AND 5 < x AND x BETWEEN 9 AND 10", "x <= 10 AND x >= 9"},
	{"", "NOT x BETWEEN 0 AND 1", "x < 0 OR x > 1"},
	{"", "t0.x = t1.x AND t0.y < t1.y AND t0.y <= t1.y", "t0.x = t1.x AND t0.y < t1.y AND t0.y <= t1.y"},
	{"", "NOT 1 > x", "x >= 1"},
	{"", "CAST(-1 AS TINYINT) <= 0", "TRUE"},
	{"", "CASE WHEN CAST(1 AS TINYINT) = 1 THEN FALSE ELSE TRUE END", "FALSE"},
	{"", "CAST(x AS TINYINT) = 1", "CAST(x AS TINYINT) = 1"},
	{"", "-500 > CAST(x AS INT) AND -1 <= CAST(x AS INT)", "FALSE"},
	{"", "COALESCE(x)", "x"},
	{"", "COALESCE(x, 1) = 2", "NOT x IS NULL AND x = 2"},
	{"redshift", "COALESCE(x, 1) = 2", "COALESCE(x, 1) = 2"},
	{"", "2 = COALESCE(x, 1)", "NOT x IS NULL AND x = 2"},
	{"", "COALESCE(x, 1, 2) = 2", "NOT x IS NULL AND x = 2"},
	{"", "COALESCE(x, 1) IS NULL", "FALSE"},
	{"", "COALESCE(CAST(CAST('2023-01-01' AS TIMESTAMP) AS DATE), x)", "CAST(CAST('2023-01-01' AS TIMESTAMP) AS DATE)"},
	{"", "CONCAT(x, y)", "CONCAT(x, y)"},
	{"", "CONCAT_WS(sep, x, y)", "CONCAT_WS(sep, x, y)"},
	{"", "CONCAT(x)", "CONCAT(x)"},
	{"", "CONCAT('a', 'b', 'c')", "'abc'"},
	{"", "CONCAT('a', x, y, 'b', 'c')", "CONCAT('a', x, y, 'bc')"},
	{"", "'a' || 'b'", "'ab'"},
	{"", "'a' || 'b' || x", "'ab' || x"},
	{"", "CONCAT(a, b) IN (SELECT * FROM foo WHERE cond)", "CONCAT(a, b) IN (SELECT * FROM foo WHERE cond)"},
	{"", "DATE_TRUNC('week', CAST('2023-12-15' AS DATE))", "CAST('2023-12-11' AS DATE)"},
	{"", "DATE_TRUNC('week', CAST('2023-12-16' AS DATE))", "CAST('2023-12-11' AS DATE)"},
	{"bigquery", "DATE_TRUNC(CAST('2023-12-15' AS DATE), WEEK)", "CAST('2023-12-10' AS DATE)"},
	{"bigquery", "DATE_TRUNC(CAST('2023-10-01' AS TIMESTAMP), QUARTER)", "CAST('2023-10-01 00:00:00' AS TIMESTAMP)"},
	{"bigquery", "DATE_TRUNC(CAST('2023-12-16' AS DATE), WEEK)", "CAST('2023-12-10' AS DATE)"},
	{"", "DATE_TRUNC('year', x) = CAST('2021-01-01' AS DATE)", "x < CAST('2022-01-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE)"},
	{"bigquery", "DATE_TRUNC(x, year) = CAST('2021-01-01' AS TIMESTAMP)", "x < CAST('2022-01-01 00:00:00' AS TIMESTAMP) AND x >= CAST('2021-01-01 00:00:00' AS TIMESTAMP)"},
	{"", "DATE_TRUNC('quarter', x) = CAST('2021-01-01' AS DATE)", "x < CAST('2021-04-01' AS DATE) AND x >= CAST('2021-01-01' AS DATE)"},
	{"bigquery", "DATE_TRUNC(x, quarter) = CAST('2021-01-01' AS TIMESTAMP)", "x < CAST('2021-04-01 00:00:00' AS TIMESTAMP) AND x >= CAST('2021-01-01 00:00:00' AS TIMESTAMP)"},
	{"bigquery", "DATE_TRUNC(x, month) = CAST('2021-01-01' AS TIMESTAMP)", "x < CAST('2021-02-01 00:00:00' AS TIMESTAMP) AND x >= CAST('2021-01-01 00:00:00' AS TIMESTAMP)"},
	{"bigquery", "DATE_TRUNC(x, DAY) = CAST('2021-01-01' AS TIMESTAMP)", "x < CAST('2021-01-02 00:00:00' AS TIMESTAMP) AND x >= CAST('2021-01-01 00:00:00' AS TIMESTAMP)"},
	{"bigquery", "CAST('2021-01-01' AS TIMESTAMP) = DATE_TRUNC(x, year)", "x < CAST('2022-01-01 00:00:00' AS TIMESTAMP) AND x >= CAST('2021-01-01 00:00:00' AS TIMESTAMP)"},
	{"", "DATE_TRUNC('quarter', x) = CAST('2021-01-02' AS DATE)", "DATE_TRUNC('QUARTER', x) = CAST('2021-01-02' AS DATE)"},
	{"bigquery", "DATE_TRUNC(x, year) <= CAST('2021-01-01' AS TIMESTAMP)", "x < CAST('2022-01-01 00:00:00' AS TIMESTAMP)"},
	{"bigquery", "CAST('2021-01-01' AS TIMESTAMP) >= DATE_TRUNC(x, year)", "x < CAST('2022-01-01 00:00:00' AS TIMESTAMP)"},
	{"", "DATE_TRUNC('year', x) >= CAST('2021-01-02' AS DATE)", "x >= CAST('2022-01-01' AS DATE)"},
	{"bigquery", "DATE_TRUNC(x, year) IN (CAST('2021-01-01' AS TIMESTAMP), CAST('2023-01-01' AS TIMESTAMP))", "(x < CAST('2022-01-01 00:00:00' AS TIMESTAMP) AND x >= CAST('2021-01-01 00:00:00' AS TIMESTAMP)) OR (x < CAST('2024-01-01 00:00:00' AS TIMESTAMP) AND x >= CAST('2023-01-01 00:00:00' AS TIMESTAMP))"},
	{"", "TIMESTAMP_TRUNC(x, YEAR) = CAST('2021-01-01' AS DATETIME)", "x < CAST('2022-01-01 00:00:00' AS DATETIME) AND x >= CAST('2021-01-01 00:00:00' AS DATETIME)"},
	{"", "x + 1 > 3", "x > 2"},
	{"", "5 - x >= 2", "x <= 3"},
	{"", "2 <> 5 - x", "x <> 3"},
	{"", "x - INTERVAL 1 DAY = CAST('2021-01-01' AS DATE)", "x = CAST('2021-01-02' AS DATE)"},
	{"", "x - INTERVAL 1 DAY = TS_OR_DS_TO_DATE('2021-01-01 00:00:01')", "x = CAST('2021-01-02' AS DATE)"},
	{"", "x - INTERVAL 1 HOUR > CAST('2021-01-01' AS DATETIME)", "x > CAST('2021-01-01 01:00:00' AS DATETIME)"},
	{"", "x - INTERVAL '1' day = CAST(y AS DATE)", "CAST(y AS DATE) = x - INTERVAL '1' DAY"},
	{"", "x = 5 AND (y = x OR z = 1)", "x = 5 AND (x = y OR z = 1)"},
	{"", "x = 1 AND CASE WHEN x = 5 THEN FALSE ELSE TRUE END", "x = 1"},
	{"", "x = 1 AND IF(x = 5, FALSE, TRUE)", "x = 1"},
	{"", "x = 1 AND CASE x WHEN 5 THEN FALSE ELSE TRUE END", "x = 1"},
	{"", "x = y AND CASE WHEN x = 5 THEN FALSE ELSE TRUE END", "CASE WHEN x = 5 THEN FALSE ELSE TRUE END AND x = y"},
	{"", "IF(TRUE, x, y)", "x"},
	{"", "IF(FALSE, x, y)", "y"},
	{"", "IF(FALSE, x)", "NULL"},
	{"", "CASE WHEN FALSE THEN x WHEN FALSE THEN y WHEN TRUE THEN z END", "z"},
	{"", "CASE x WHEN y THEN z ELSE w END", "CASE WHEN x = y THEN z ELSE w END"},
	{"", "STARTS_WITH(x, y)", "STARTS_WITH(x, y)"},
	{"", "SELECT NOT(NOT(NOT(NOT t_bool.a))) FROM t_bool", "SELECT t_bool.a FROM t_bool"},
	{"mysql", "SELECT NOT(NOT(NOT(NOT t_bool.a))) FROM t_bool", "SELECT NOT NOT NOT NOT t_bool.a FROM t_bool"},
	{"sqlite", "SELECT NOT(NOT(NOT(NOT t_bool.a))) FROM t_bool", "SELECT NOT NOT NOT NOT t_bool.a FROM t_bool"},
	{"mysql", "WITH t0 AS (SELECT 1 AS a, 'foo' AS p) SELECT NOT(NOT(CASE WHEN t0.a > 1 THEN t0.a ELSE t0.p END)) AS res FROM t0", "WITH t0 AS (SELECT 1 AS a, 'foo' AS p) SELECT NOT NOT CASE WHEN t0.a > 1 THEN t0.a ELSE t0.p END AS res FROM t0"},
	{"sqlite", "WITH t0 AS (SELECT 1 AS a, 'foo' AS p) SELECT NOT (NOT(CASE WHEN t0.a > 1 THEN t0.a ELSE t0.p END)) AS res FROM t0", "WITH t0 AS (SELECT 1 AS a, 'foo' AS p) SELECT NOT NOT CASE WHEN t0.a > 1 THEN t0.a ELSE t0.p END AS res FROM t0"},
	{"", "'a' OR NOT 'a'", "TRUE"},
	{"", "'a' AND NOT 'a'", "FALSE"},
	{"", "NULL AND NOT NULL", "NULL AND TRUE"},
	{"snowflake", "SELECT * FROM o ASOF JOIN e MATCH_CONDITION (o.observed_date >= e.metric_date) ON o.id = e.id", "SELECT * FROM o ASOF JOIN e MATCH_CONDITION (o.observed_date >= e.metric_date) ON e.id = o.id"},
}

// simplifyTestSchema mirrors TestOptimizer.schema in sqlglot's tests/test_optimizer.py.
func simplifyTestSchema() *SchemaMap {
	s := NewSchemaMap()
	tbl := func(name string, kv ...string) {
		m := NewSchemaMap()
		for i := 0; i < len(kv); i += 2 {
			m.Set(kv[i], kv[i+1])
		}
		s.Set(name, m)
	}
	tbl("x", "a", "INT", "b", "INT")
	tbl("y", "b", "INT", "c", "INT")
	tbl("z", "b", "INT", "c", "INT")
	tbl("w", "d", "TEXT", "e", "TEXT")
	tbl("temporal", "d", "DATE", "t", "DATETIME")
	tbl("structs", "one", "STRUCT<a_1 INT, b_1 VARCHAR>",
		"nested_0", "STRUCT<a_1 INT, nested_1 STRUCT<a_2 INT, nested_2 STRUCT<a_3 INT>>>",
		"quoted", `STRUCT<"foo bar" INT>`)
	tbl("t_bool", "a", "BOOLEAN")
	return s
}

func smpTestParse(t *testing.T, dialect, sql string) (*Dialect, *Expr) {
	t.Helper()
	d, err := GetDialect(dialect)
	if err != nil {
		t.Fatalf("dialect %q: %v", dialect, err)
	}
	e, err := d.ParseOne(sql, nil)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return d, e
}

func smpTestSQL(t *testing.T, d *Dialect, e *Expr) string {
	t.Helper()
	s, err := d.Generate(e, nil)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return s
}

func TestSimplifyFixtures(t *testing.T) {
	for _, c := range simplifyFixtureCases {
		dialect, sql, want := c[0], c[1], c[2]
		t.Run(sql, func(t *testing.T) {
			d, e := smpTestParse(t, dialect, sql)
			e = AnnotateTypes(e, AnnotateOptions{SchemaMap: simplifyTestSchema()})
			got := smpTestSQL(t, d, Simplify(e, SimplifyOptions{ConstantPropagation: true, Coalesce: true, Dialect: d}))
			if got != want {
				t.Errorf("simplify(%q)\n want: %s\n  got: %s", sql, want, got)
			}
		})
	}
}

func TestSimplifyAPI(t *testing.T) {
	// Stress test with huge union query
	union := strings.Repeat("SELECT 1 UNION ALL ", 1000) + "SELECT 1"
	d, e := smpTestParse(t, "", union)
	if got := smpTestSQL(t, d, Simplify(e, SimplifyOptions{})); got != union {
		t.Errorf("union stress test changed the query")
	}

	// Ensure simplify mutates the AST properly
	d, e = smpTestParse(t, "", "SELECT 1 + 2")
	Simplify(e.Selects()[0], SimplifyOptions{ConstantPropagation: true, Coalesce: true})
	if got := smpTestSQL(t, d, e); got != "SELECT 3" {
		t.Errorf("got %s", got)
	}

	d, e = smpTestParse(t, "", "SELECT a, c, b FROM table1 WHERE 1 = 1")
	opts := SimplifyOptions{ConstantPropagation: true, Coalesce: true}
	if got := smpTestSQL(t, d, Simplify(Simplify(e.Find(KWhere), opts), opts)); got != "WHERE TRUE" {
		t.Errorf("got %s", got)
	}

	_, e = smpTestParse(t, "", "TRUE AND TRUE AND TRUE")
	if got := Simplify(e, SimplifyOptions{}); !got.Equal(Boolean(true)) {
		t.Errorf("TRUE AND TRUE AND TRUE did not simplify to TRUE")
	}

	// simplify_concat preserves the expression types
	presto, e := smpTestParse(t, "presto", "CONCAT('a', x, 'b', 'c')")
	concat := Simplify(e, SimplifyOptions{})
	d, e = smpTestParse(t, "", "CONCAT('a', x, 'b', 'c')")
	safeConcat := Simplify(e, SimplifyOptions{})
	if concat.Arg("safe") != false || safeConcat.Arg("safe") != true {
		t.Errorf("safe flags: %v %v", concat.Arg("safe"), safeConcat.Arg("safe"))
	}
	if got := smpTestSQL(t, presto, concat); got != "CONCAT('a', x, 'bc')" {
		t.Errorf("got %s", got)
	}
	if got := smpTestSQL(t, d, safeConcat); got != "CONCAT('a', x, 'bc')" {
		t.Errorf("got %s", got)
	}

	_, e = smpTestParse(t, "databricks", "CONCAT_WS(' ', a, NULL, 'b', 'c')")
	concatWs := Simplify(e, SimplifyOptions{})
	if concatWs.Arg("coalesce") != true {
		t.Errorf("coalesce flag: %v", concatWs.Arg("coalesce"))
	}
	if got := smpTestSQL(t, MustDialect("duckdb"), concatWs); got != "CONCAT_WS(' ', a, NULL, 'b c')" {
		t.Errorf("got %s", got)
	}

	// Python exceptions escape simplify as panics
	func() {
		defer func() {
			if r, ok := recover().(*smpPyError); !ok || r.Type != "decimal.DivisionByZero" {
				t.Errorf("expected decimal.DivisionByZero, got %v", r)
			}
		}()
		_, e := smpTestParse(t, "", "SELECT 1.0 / 0")
		Simplify(e, SimplifyOptions{})
	}()
}

func TestSimplifyNested(t *testing.T) {
	d, e := smpTestParse(t, "", `
        SELECT x, 1 + 1
        FROM foo
        WHERE x > (((select x + 1 + 1, sum(y + 1 + 1) FROM bar GROUP BY x + 1 + 1)))
        `)
	_, want := smpTestParse(t, "", `
            SELECT x, 2
            FROM foo
            WHERE x > (((
                select x + 1 + 1, sum(y + 2)
                FROM bar
                GROUP BY x + 1 + 1
            )))
            `)
	got := Simplify(e, SimplifyOptions{})
	if smpTestSQL(t, d, got) != smpTestSQL(t, d, want) {
		t.Errorf("got %s", smpTestSQL(t, d, got))
	}
}

func TestSimplifyGen(t *testing.T) {
	_, e := smpTestParse(t, "", "anonymous(x, y)")
	if got := smpGen(e, false); got != "ANONYMOUS(x,y)" {
		t.Errorf("got %s", got)
	}

	_, e = smpTestParse(t, "", "SELECT x FROM t")
	if smpGen(e, false) != smpGen(e.Copy(), false) {
		t.Errorf("gen(copy) differs")
	}

	anon := New(KAnonymous, "this", ToIdentifier("anonymous", nil), "expressions", []*Expr{
		New(KColumn, "this", ToIdentifier("x", nil)),
		New(KColumn, "this", ToIdentifier("y", nil)),
	})
	if got := smpGen(anon, false); got != "ANONYMOUS(x,y)" {
		t.Errorf("got %s", got)
	}

	_, e = smpTestParse(t, "", `"anonymous"(x, y)`)
	if got := smpGen(e, false); got != `"anonymous"(x,y)` {
		t.Errorf("got %s", got)
	}

	func() {
		defer func() {
			r, ok := recover().(*ValueError)
			if !ok || !strings.Contains(r.Msg, "Anonymous.this expects a str or an Identifier, got 'int'.") {
				t.Errorf("unexpected: %v", r)
			}
		}()
		smpGen(New(KAnonymous, "this", 5), false)
	}()

	_, e = smpTestParse(t, "", `
        WITH cte AS (select 1 union select 2), cte2 AS (
            SELECT ROW() OVER (PARTITION BY y) FROM (
                (select 1) limit 10
            )
        )
        SELECT
          *,
          a + 1,
          a div 1,
          filter("B", (x, y) -> x + y)
          FROM (z AS z CROSS JOIN z) AS f(a) LEFT JOIN a.b.c.d.e.f.g USING(n) ORDER BY 1
        `)
	want := `SELECT :with_,WITH :expressions,CTE :this,UNION :this,SELECT :expressions,1,:expression,SELECT :expressions,2,:distinct,True,:alias, AS cte,CTE :this,SELECT :expressions,WINDOW :this,ROW(),:partition_by,y,:over,OVER,:from_,FROM ((SELECT :expressions,1):limit,LIMIT :expression,10),:alias, AS cte2,:expressions,STAR,a + 1,a DIV 1,FILTER("B",LAMBDA :this,x + y,:expressions,x,y),:from_,FROM (z AS z:joins,JOIN :this,z,:kind,CROSS) AS f(a),:joins,JOIN :this,a.b.c.d.e.f.g,:side,LEFT,:using,n,:order,ORDER :expressions,ORDERED :this,1,:nulls_first,True`
	if got := smpGen(e, false); got != want {
		t.Errorf("gen\n want: %s\n  got: %s", want, got)
	}

	_, e = smpTestParse(t, "", "select item_id /* description */")
	if got := smpGen(e, true); got != "SELECT :expressions,item_id /* description */" {
		t.Errorf("got %s", got)
	}
}

func TestSimplifyNumbersAndDates(t *testing.T) {
	cases := [][2]string{
		{"SELECT 1e5 * 1", "SELECT 1E+5"},
		{"SELECT 1e5 + 1", "SELECT 100001"},
		{"SELECT 0.1 + 0.2", "SELECT 0.3"},
		{"SELECT 1.0/3.0 FROM x", "SELECT 0.3333333333333333333333333333 FROM x"},
		{"SELECT 2.50 * 4", "SELECT 10.00"},
		{"SELECT 1 - 1.000", "SELECT 0.000"},
		{"SELECT 0.000001 * 0.0001", "SELECT 1E-10"},
		{"SELECT CAST('2020-01-31' AS DATE) + INTERVAL '1' MONTH", "SELECT CAST('2020-02-29' AS DATE)"},
		{"SELECT CAST('2020-01-01' AS DATE) + INTERVAL '1' HOUR", "SELECT CAST('2020-01-01 01:00:00' AS DATE)"},
		{"SELECT CAST('2020-03-15T10:20:30+05:30' AS TIMESTAMP) - INTERVAL '1' DAY", "SELECT CAST('2020-03-14 10:20:30+05:30' AS TIMESTAMP)"},
		{"SELECT * FROM t WHERE DATE_TRUNC('month', x) IN (CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE))", "SELECT * FROM t WHERE x < CAST('2020-03-01' AS DATE) AND x >= CAST('2020-01-01' AS DATE)"},
	}
	for _, c := range cases {
		d, e := smpTestParse(t, "", c[0])
		if got := smpTestSQL(t, d, Simplify(e, SimplifyOptions{})); got != c[1] {
			t.Errorf("simplify(%q)\n want: %s\n  got: %s", c[0], c[1], got)
		}
	}
}
