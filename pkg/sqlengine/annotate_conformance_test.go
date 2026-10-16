package sqlengine

import (
	"fmt"
	"strings"
	"testing"
)

// Type inference conformance: the column types sqlglot infers for codegen/annotate_corpus.json,
// annotated with and without annotateSchema (recorded by codegen/gen_annotate_expected.py).

type annotateCase struct {
	Dialect string   `json:"dialect"`
	SQL     string   `json:"sql"`
	Schema  bool     `json:"schema"`
	Types   []string `json:"types"`
	Error   string   `json:"error"`
}

// annotateSchema must match SCHEMA in codegen/gen_annotate_expected.py (key order matters).
func annotateSchema() *SchemaMap {
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

// annotateColumnTypes returns the inferred type of each output column of sql.
func annotateColumnTypes(c annotateCase) (types []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	d, err := GetDialect(c.Dialect)
	if err != nil {
		return nil, err
	}
	e, err := d.ParseOne(c.SQL, nil)
	if err != nil {
		return nil, err
	}
	opts := AnnotateOptions{Dialect: d}
	if c.Schema {
		opts.SchemaMap = annotateSchema()
	}
	AnnotateTypes(e, opts)
	for _, s := range e.Selects() {
		typ := "None"
		if t := s.Type(); t != nil {
			if typ, err = t.SQL("", nil); err != nil {
				return nil, err
			}
		}
		types = append(types, typ)
	}
	return types, nil
}

func TestConformanceAnnotate(t *testing.T) {
	t.Parallel()
	var cases []annotateCase
	loadGz(t, "testdata/annotate.json.gz", &cases)
	for _, c := range cases {
		got, err := annotateColumnTypes(c)
		name := fmt.Sprintf("[%s schema=%v] %s", c.Dialect, c.Schema, c.SQL)
		switch {
		case c.Error != "":
			if err == nil {
				t.Errorf("%s: want an error (%s), got types %v", name, firstLine(c.Error), got)
			}
		case err != nil:
			t.Errorf("%s: %v", name, err)
		case strings.Join(got, "\n") != strings.Join(c.Types, "\n"):
			t.Errorf("%s:\nwant %v\n got %v", name, c.Types, got)
		}
	}
}
