package sqlengine

import "testing"

// Expected values were generated with the reference Python sqlglot 30.13.0:
//
//	qualify(parse_one(sql, read=dialect), schema=schema, dialect=dialect, **opts).sql(dialect)
//
// (optimize cases: optimize(parse_one(sql), schema=schema, dialect=dialect, rules=[qualify])).

// qtSchema builds a nested schema mapping from alternating key/value pairs (values are column
// types or nested mappings).
func qtSchema(kv ...any) *SchemaMap {
	m := NewSchemaMap()
	for i := 0; i+1 < len(kv); i += 2 {
		m.Set(kv[i].(string), kv[i+1])
	}
	return m
}

type qualifyTestCase struct {
	dialect    string
	sql        string
	schema     *SchemaMap
	db         string
	noIdentify bool
	noValidate bool
	highlight  bool
	optimize   bool
	want       string
	err        string
}

func runQualifyTestCase(c qualifyTestCase) (out string, errMsg string) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case *OptimizeError:
				errMsg = x.Msg
			case *ValueError:
				errMsg = "ValueError: " + x.Msg
			case error:
				errMsg = "unexpected error: " + x.Error()
			default:
				panic(r)
			}
		}
	}()

	d := MustDialect(c.dialect)
	e, err := d.ParseOne(c.sql, nil)
	if err != nil {
		return "", "parse error: " + err.Error()
	}
	var schema *MappingSchema
	if c.schema != nil {
		schema = NewMappingSchema(c.schema, nil, d, true, nil)
	}

	var result *Expr
	if c.optimize {
		result = Optimize(e, schema, d, []OptimizerRule{RuleQualify})
	} else {
		o := DefaultQualifyOptions()
		o.Dialect = d
		o.Schema = schema
		o.Db = c.db
		o.Identify = !c.noIdentify
		o.ValidateQualifyColumns = !c.noValidate
		if c.highlight {
			o.SQL = c.sql
		}
		result = Qualify(e, o)
	}

	s, err := d.Generate(result, nil)
	if err != nil {
		return "", "generate error: " + err.Error()
	}
	return s, ""
}

func TestQualifyMatchesPython(t *testing.T) {
	s1 := qtSchema(
		"t", qtSchema("a", "INT", "b", "INT"),
		"u", qtSchema("a", "INT", "c", "INT"),
		"x", qtSchema("a", "INT", "b", "TEXT"),
	)

	cases := []qualifyTestCase{
		{dialect: "", sql: "SELECT a FROM t", schema: s1, want: "SELECT \"t\".\"a\" AS \"a\" FROM \"t\" AS \"t\""},
		{dialect: "", sql: "SELECT * FROM t JOIN u USING (a)", schema: s1, want: "SELECT COALESCE(\"t\".\"a\", \"u\".\"a\") AS \"a\", \"t\".\"b\" AS \"b\", \"u\".\"c\" AS \"c\" FROM \"t\" AS \"t\" JOIN \"u\" AS \"u\" ON \"t\".\"a\" = \"u\".\"a\""},
		{dialect: "", sql: "SELECT a AS x, x + 1 AS y FROM t WHERE x > 1 GROUP BY 1 ORDER BY 2", schema: s1, want: "SELECT \"t\".\"a\" AS \"x\", \"t\".\"a\" + 1 AS \"y\" FROM \"t\" AS \"t\" WHERE \"t\".\"a\" > 1 GROUP BY \"t\".\"a\" ORDER BY \"y\""},
		{dialect: "", sql: "WITH c(x) AS (SELECT a FROM t) SELECT * FROM c", schema: s1, want: "WITH \"c\" AS (SELECT \"t\".\"a\" AS \"x\" FROM \"t\" AS \"t\") SELECT \"c\".\"x\" AS \"x\" FROM \"c\" AS \"c\""},
		{dialect: "", sql: "SELECT s.a FROM (SELECT a FROM t) AS s JOIN u ON s.a = u.a", schema: s1, want: "SELECT \"s\".\"a\" AS \"a\" FROM (SELECT \"t\".\"a\" AS \"a\" FROM \"t\" AS \"t\") AS \"s\" JOIN \"u\" AS \"u\" ON \"s\".\"a\" = \"u\".\"a\""},
		{dialect: "", sql: "SELECT 1 FROM (t1 JOIN t2) AS t", schema: nil, want: "SELECT 1 AS \"1\" FROM (SELECT * FROM \"t1\" AS \"t1\", \"t2\" AS \"t2\") AS \"t\""},
		{dialect: "", sql: "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE c = t.a)", schema: s1, want: "SELECT \"t\".\"a\" AS \"a\" FROM \"t\" AS \"t\" WHERE EXISTS(SELECT 1 AS \"1\" FROM \"u\" AS \"u\" WHERE \"u\".\"c\" = \"t\".\"a\")"},
		{dialect: "", sql: "SELECT a FROM t UNION SELECT c FROM u ORDER BY 1", schema: s1, want: "SELECT \"t\".\"a\" AS \"a\" FROM \"t\" AS \"t\" UNION SELECT \"u\".\"c\" AS \"c\" FROM \"u\" AS \"u\" ORDER BY \"a\""},
		{dialect: "", sql: "SELECT a FROM t", schema: qtSchema("db", qtSchema("t", qtSchema("a", "INT"))), db: "db", noIdentify: true, want: "SELECT t.a AS a FROM db.t AS t"},
		{dialect: "", sql: "SELECT zz FROM t", schema: s1, noValidate: true, noIdentify: true, want: "SELECT zz AS zz FROM t AS t"},
		{dialect: "snowflake", sql: "SELECT a FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1", schema: s1, want: "SELECT \"T\".\"A\" AS \"A\" FROM \"T\" AS \"T\" QUALIFY ROW_NUMBER() OVER (PARTITION BY \"T\".\"A\" ORDER BY \"T\".\"B\") = 1"},
		{dialect: "snowflake", sql: "SELECT * RENAME (a AS z) FROM t", schema: s1, want: "SELECT \"T\".\"A\" AS \"Z\", \"T\".\"B\" AS \"B\" FROM \"T\" AS \"T\""},
		{dialect: "bigquery", sql: "SELECT * EXCEPT (a) FROM t", schema: s1, want: "SELECT `t`.`b` AS `b` FROM `t` AS `t`"},
		{dialect: "bigquery", sql: "SELECT a AS t FROM t GROUP BY 1", schema: s1, want: "SELECT `t`.`a` AS `t` FROM `t` AS `t` GROUP BY 1"},
		{dialect: "duckdb", sql: "SELECT a + b AS c FROM t WHERE c > 0", schema: s1, want: "SELECT \"t\".\"a\" + \"t\".\"b\" AS \"c\" FROM \"t\" AS \"t\" WHERE (\"t\".\"a\" + \"t\".\"b\") > 0"},
		{dialect: "postgres", sql: "SELECT DISTINCT ON (1) a, b FROM t ORDER BY 1", schema: s1, want: "SELECT DISTINCT ON (\"a\") \"t\".\"a\" AS \"a\", \"t\".\"b\" AS \"b\" FROM \"t\" AS \"t\" ORDER BY \"a\""},
		{dialect: "postgres", sql: "SELECT * FROM t", schema: qtSchema("t", qtSchema("Col", "INT", "x", "INT")), want: "SELECT \"t\".\"col\" AS \"col\", \"t\".\"x\" AS \"x\" FROM \"t\" AS \"t\""},
		{dialect: "oracle", sql: "SELECT ROWNUM, a FROM t", schema: s1, want: "SELECT ROWNUM AS \"ROWNUM\", \"T\".\"A\" AS \"A\" FROM \"T\" \"T\""},
		{dialect: "", sql: "SELECT zz FROM t", schema: s1, err: "Column 'zz' could not be resolved. Line: 1, Col: 9"},
		{dialect: "", sql: "SELECT a FROM t, u", schema: s1, err: "Column 'a' could not be resolved. Line: 1, Col: 8"},
		{dialect: "", sql: "SELECT t.zz FROM t", schema: s1, err: "Unknown column: zz"},
		{dialect: "", sql: "SELECT * FROM t JOIN u USING (zz)", schema: s1, err: "Cannot automatically join: zz"},
		{dialect: "", sql: "SELECT 1 FROM t ORDER BY 5", schema: s1, err: "Unknown output column: 5"},
		{dialect: "", sql: "SELECT\n  a,\n  zz\nFROM t", schema: s1, highlight: true, err: "Column 'zz' could not be resolved. Line: 3, Col: 4\n  SELECT\n  a,\n  \u001b[4mzz\u001b[0m\nFROM t"},
		{dialect: "snowflake", sql: "SELECT * FROM t PIVOT(SUM(b) FOR q1 IN (1, 2)) AS p", schema: s1, highlight: true, err: "Ambiguous column 'Q1' (Line: 1, Col: 35)\n  SELECT * FROM t PIVOT(SUM(b) FOR \u001b[4mq1\u001b[0m IN (1, 2)) AS p"},
		{dialect: "", sql: "SELECT t.a, c FROM t JOIN u ON t.a = u.c", schema: s1, optimize: true, want: "SELECT t.a AS a, u.c AS c FROM (SELECT t.a AS a, t.b AS b FROM t AS t) AS t JOIN (SELECT u.a AS a, u.c AS c FROM u AS u) AS u ON t.a = u.c"},
		{dialect: "", sql: "SELECT a FROM t", schema: nil, optimize: true, want: "SELECT t.a AS a FROM t AS t"},
		{dialect: "bigquery", sql: "SELECT `p.d.t`.`c`.`f` FROM `p.d.t`", schema: nil, err: "ValueError: 'Dot' object has no attribute 'quoted'"},
	}

	for _, c := range cases {
		got, gotErr := runQualifyTestCase(c)
		if c.err != "" {
			if gotErr != c.err {
				t.Errorf("[%s] %q\n  want error: %q\n  got: %q (error %q)", c.dialect, c.sql, c.err, got, gotErr)
			}
			continue
		}
		if gotErr != "" || got != c.want {
			t.Errorf("[%s] %q\n  want: %s\n  got:  %s (error %q)", c.dialect, c.sql, c.want, got, gotErr)
		}
	}
}

// TestQualifyLineageDefaults checks the options lineage uses (Defaults with
// validate_qualify_columns=False, identify=False).
func TestQualifyLineageDefaults(t *testing.T) {
	d := MustDialect("")
	e, err := d.ParseOne("SELECT zz, a AS x FROM t ORDER BY x", nil)
	if err != nil {
		t.Fatal(err)
	}
	schema := NewMappingSchema(qtSchema("t", qtSchema("a", "INT")), nil, d, true, nil)
	got, err := d.Generate(Qualify(e, QualifyOptions{Dialect: d, Schema: schema, ValidateQualifyColumns: false, Identify: false, Defaults: true}), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT zz AS zz, t.a AS x FROM t AS t ORDER BY x"
	if got != want {
		t.Errorf("want: %s\ngot:  %s", want, got)
	}
}
