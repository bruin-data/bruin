package sqlengine

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Fixtures are produced with the reference Python implementation by
// .context/port/gen_annotate_fixtures.py.
type annotateFixture struct {
	Meta   map[string]map[string]string   `json:"meta"`
	Coerce map[string]map[string][]string `json:"coerce"`
	ISO    [][]any                        `json:"iso"`
	Cases  []annotateCase                 `json:"cases"`
}

type annotateCase struct {
	Dialect string   `json:"dialect"`
	SQL     string   `json:"sql"`
	Schema  bool     `json:"schema"`
	Types   []string `json:"types"`
	Error   string   `json:"error"`
}

var (
	annFixtureOnce sync.Once
	annFixture     annotateFixture
	annFixtureErr  error
)

func loadAnnotateFixture(t *testing.T) *annotateFixture {
	t.Helper()
	annFixtureOnce.Do(func() {
		f, err := os.Open("testdata/annotate.json.gz")
		if err != nil {
			annFixtureErr = err
			return
		}
		defer f.Close()
		zr, err := gzip.NewReader(f)
		if err != nil {
			annFixtureErr = err
			return
		}
		annFixtureErr = json.NewDecoder(zr).Decode(&annFixture)
	})
	if annFixtureErr != nil {
		t.Fatalf("loading fixture: %v", annFixtureErr)
	}
	return &annFixture
}

func TestAnnotateTypingMetadataParity(t *testing.T) {
	fx := loadAnnotateFixture(t)
	for dialect, want := range fx.Meta {
		d := MustDialect(dialect)
		got := map[string]string{}
		for k, spec := range expressionMetadataFor(d) {
			if spec.Annotator != nil {
				got[k.Name()] = "A " + spec.src
				continue
			}
			switch r := spec.Returns.(type) {
			case DType:
				got[k.Name()] = "R " + dtypeValues[r]
			case *Expr:
				sql, err := r.SQL("", nil)
				if err != nil {
					t.Fatalf("%s: %v", k.Name(), err)
				}
				got[k.Name()] = "R DT " + sql
			default:
				t.Fatalf("%s: unexpected returns %T", k.Name(), r)
			}
		}
		var diffs []string
		for k, w := range want {
			if g := got[k]; g != w {
				diffs = append(diffs, fmt.Sprintf("%s: want %q got %q", k, w, g))
			}
		}
		for k, g := range got {
			if _, ok := want[k]; !ok {
				diffs = append(diffs, fmt.Sprintf("%s: unexpected %q", k, g))
			}
		}
		sort.Strings(diffs)
		if len(diffs) > 0 {
			t.Errorf("dialect %q metadata mismatch (%d):\n%s", dialect, len(diffs), strings.Join(diffs, "\n"))
		}
	}
}

func annDumpCoerces(m map[DType]DTypeSet) map[string][]string {
	out := map[string][]string{}
	for k, v := range m {
		var items []string
		for _, d := range v.Items() {
			items = append(items, dtypeValues[d])
		}
		sort.Strings(items)
		if items == nil {
			items = []string{}
		}
		out[dtypeValues[k]] = items
	}
	return out
}

func TestAnnotateCoercesToParity(t *testing.T) {
	fx := loadAnnotateFixture(t)
	annInitCoercions()
	tables := map[string]map[DType]DTypeSet{
		"base_pristine": annBaseCoercesTo,
		"hive":          coercesToFor(MustDialect("hive")),
		"spark":         coercesToFor(MustDialect("spark")),
		"databricks":    coercesToFor(MustDialect("databricks")),
		"bigquery":      coercesToFor(MustDialect("bigquery")),
		"postgres":      coercesToFor(MustDialect("postgres")),
	}
	for name, want := range fx.Coerce {
		if name == "base_after_bq" {
			continue // the port does not reproduce BigQuery's mutation of the base table
		}
		got := annDumpCoerces(tables[name])
		wantJSON, _ := json.Marshal(want)
		gotJSON, _ := json.Marshal(got)
		if string(wantJSON) != string(gotJSON) {
			t.Errorf("%s:\nwant %s\ngot  %s", name, wantJSON, gotJSON)
		}
	}
}

func TestAnnotateISODateHelpers(t *testing.T) {
	fx := loadAnnotateFixture(t)
	for _, c := range fx.ISO {
		text := c[0].(string)
		wantDate, wantDatetime := c[1].(bool), c[2].(bool)
		if got := annIsISODate(text); got != wantDate {
			t.Errorf("is_iso_date(%q) = %v, want %v", text, got, wantDate)
		}
		if got := annIsISODatetime(text); got != wantDatetime {
			t.Errorf("is_iso_datetime(%q) = %v, want %v", text, got, wantDatetime)
		}
	}
}

// annTestSchema mirrors SCHEMA in gen_annotate_fixtures.py (key order matters).
func annTestSchema() *SchemaMap {
	mk := func(kv ...string) *SchemaMap {
		m := NewSchemaMap()
		for i := 0; i < len(kv); i += 2 {
			m.Set(kv[i], kv[i+1])
		}
		return m
	}
	s := NewSchemaMap()
	s.Set("t", mk(
		"a", "INT", "b", "VARCHAR", "c", "DOUBLE", "d", "DATE", "e", "TIMESTAMP", "f", "DECIMAL(10, 2)",
		"g", "BIGINT", "h", "BOOLEAN", "s", "STRUCT<x INT, y VARCHAR>", "arr", "ARRAY<INT>", "j", "JSON",
	))
	s.Set("u", mk("a", "BIGINT", "b", "TEXT", "k", "FLOAT", "dt", "DATETIME"))
	return s
}

func annNodeTypes(e *Expr) []string {
	var out []string
	for n := range e.DFS(nil) {
		t := n.Type()
		s := "None"
		if t != nil {
			sql, err := t.SQL("", nil)
			if err != nil {
				s = "ERR " + err.Error()
			} else {
				s = sql
			}
		}
		out = append(out, n.Kind().Name()+":"+s)
	}
	return out
}

func TestAnnotateTypesParity(t *testing.T) {
	fx := loadAnnotateFixture(t)
	// The fixtures were produced after importing the Hive family and then BigQuery (which
	// mutates the base COERCES_TO table).
	for _, name := range []string{"hive", "spark2", "spark", "databricks", "bigquery"} {
		MustDialect(name)
	}

	failures := 0
	for _, c := range fx.Cases {

		name := fmt.Sprintf("%s/%v/%s", c.Dialect, c.Schema, c.SQL)
		func() {
			defer func() {
				if r := recover(); r != nil {
					failures++
					t.Errorf("%s: panic: %v", name, r)
				}
			}()
			d := MustDialect(c.Dialect)
			e, err := d.ParseOne(c.SQL, nil)
			if err != nil {
				failures++
				t.Errorf("%s: parse error: %v", name, err)
				return
			}
			opts := AnnotateOptions{Dialect: d}
			if c.Schema {
				opts.SchemaMap = annTestSchema()
			}
			AnnotateTypes(e, opts)
			if c.Error != "" {
				failures++
				t.Errorf("%s: expected error %s", name, c.Error)
				return
			}
			got := annNodeTypes(e)
			if strings.Join(got, "\n") != strings.Join(c.Types, "\n") {
				failures++
				t.Errorf("%s:\nwant %v\ngot  %v", name, c.Types, got)
			}
		}()
	}
	if failures > 0 {
		t.Logf("%d/%d annotation cases failed", failures, len(fx.Cases))
	}
}
