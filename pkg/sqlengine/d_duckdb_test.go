package sqlengine

import "testing"

func transpileOne(t *testing.T, read, write, sql string) string {
	t.Helper()
	e, err := MustDialect(read).ParseOne(sql, nil)
	if err != nil {
		t.Fatalf("[%s] %s: %v", read, sql, err)
	}
	out, err := MustDialect(write).Generate(e, nil)
	if err != nil {
		t.Fatalf("[%s -> %s] %s: %v", read, write, sql, err)
	}
	return out
}

// TestDuckDBTildeRoundTrip checks that DuckDB's ~ and !~ regenerate as the operators whatever the
// DuckDB version: ~ is a whole-string match before DuckDB 2.0 and a match anywhere from 2.0 on,
// so rewriting it as either function would change what the query returns on the other versions.
// Explicit REGEXP_MATCHES and REGEXP_FULL_MATCH calls keep their names.
func TestDuckDBTildeRoundTrip(t *testing.T) {
	t.Parallel()
	same := []string{
		"SELECT a ~ 'b'",
		"SELECT a !~ 'b'",
		"SELECT x ~ y, x !~ y",
		"SELECT * FROM t WHERE a ~ 'b' AND NOT a ~ 'c'",
		"SELECT * FROM t WHERE NOT a !~ 'b' OR c",
		"SELECT a ~ 'b' ~ 'c'",
		"SELECT (a ~ 'b') = TRUE",
		"SELECT a ~ 'b' IS NULL",
		"SELECT a !~ 'b' = b",
		"SELECT a || b ~ c || d",
		"SELECT CASE WHEN a ~ '^x' THEN 1 END",
		"SELECT REGEXP_MATCHES(a, 'b')",
		"SELECT REGEXP_FULL_MATCH(a, 'b')",
		"SELECT REGEXP_MATCHES(a, 'b') ~ 'c'",
		"SELECT NOT REGEXP_MATCHES(a, 'b')",
	}
	rewritten := map[string]string{
		"SELECT a ! ~ 'b'":            "SELECT a !~ 'b'",
		"SELECT a NOT ~ 'b'":          "SELECT a !~ 'b'",
		"SELECT a !~ 'b' ~ 'c'":       "SELECT (a !~ 'b') ~ 'c'",
		"SELECT REGEXP_MATCHES(a, b)": "SELECT REGEXP_MATCHES(a, b)",
	}
	for _, dialect := range []string{"duckdb", "duckdb, version=1.5", "duckdb, version=2.0"} {
		for _, sql := range same {
			if got := transpileOne(t, dialect, dialect, sql); got != sql {
				t.Errorf("[%s] %s:\n got %s\nwant %s", dialect, sql, got, sql)
			}
		}
		for sql, want := range rewritten {
			if got := transpileOne(t, dialect, dialect, sql); got != want {
				t.Errorf("[%s] %s:\n got %s\nwant %s", dialect, sql, got, want)
			}
		}
	}
}

// TestDuckDBTildeMeaning checks what ~ is read as: a whole-string match (REGEXP_FULL_MATCH) before
// DuckDB 2.0 and a match anywhere (REGEXP_MATCHES) from 2.0 on, typed as a boolean predicate; !~ is
// its negation. Other dialects, and DuckDB of the other side of 2.0, get the match it stands for.
func TestDuckDBTildeMeaning(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		dialect string
		kind    Kind
	}{
		{"duckdb", KRegexpLike},
		{"duckdb, version=2.0", KRegexpLike},
		{"duckdb, version=1.5", KRegexpFullMatch},
	} {
		d := MustDialect(c.dialect)
		e, err := d.ParseOne("SELECT a ~ 'b' AS m, a !~ 'b' AS n FROM t", nil)
		if err != nil {
			t.Fatalf("[%s] %v", c.dialect, err)
		}
		sels := e.Selects()
		if m := sels[0].This(); !m.IsA(c.kind) {
			t.Errorf("[%s] a ~ 'b' parsed as %s, want %s", c.dialect, m.Kind().Name(), c.kind.Name())
		}
		if n := sels[1].This(); !n.IsA(KNot) || !n.This().IsA(c.kind) {
			t.Errorf("[%s] a !~ 'b' parsed as %s, want Not(%s)", c.dialect, n.Kind().Name(), c.kind.Name())
		}
		AnnotateTypes(e, AnnotateOptions{Dialect: d})
		for _, s := range sels {
			if got := typeSQL(s); got != "BOOLEAN" {
				t.Errorf("[%s] %s typed %s, want BOOLEAN", c.dialect, s.Alias(), got)
			}
		}
	}

	cases := []struct{ read, write, sql, want string }{
		{"duckdb", "postgres", "SELECT a ~ 'b', a !~ 'b'", "SELECT a ~ 'b', NOT a ~ 'b'"},
		{"duckdb, version=1.5", "postgres", "SELECT a ~ 'b', a !~ 'b'", "SELECT REGEXP_FULL_MATCH(a, 'b'), NOT REGEXP_FULL_MATCH(a, 'b')"},
		{"duckdb", "duckdb, version=1.5", "SELECT a ~ 'b', a !~ 'b'", "SELECT REGEXP_MATCHES(a, 'b'), NOT REGEXP_MATCHES(a, 'b')"},
		{"duckdb, version=1.5", "duckdb", "SELECT a ~ 'b', a !~ 'b'", "SELECT REGEXP_FULL_MATCH(a, 'b'), NOT REGEXP_FULL_MATCH(a, 'b')"},
		{"postgres", "duckdb", "SELECT a ~ 'b', a !~ 'b'", "SELECT REGEXP_MATCHES(a, 'b'), NOT REGEXP_MATCHES(a, 'b')"},
	}
	for _, c := range cases {
		if got := transpileOne(t, c.read, c.write, c.sql); got != c.want {
			t.Errorf("[%s -> %s] %s:\n got %s\nwant %s", c.read, c.write, c.sql, got, c.want)
		}
	}
}
