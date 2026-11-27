package sqlengine

// Golden tests: the engine's observable behavior over a fixed corpus, compared with expectations
// recorded with sqlglot by testdata/golden/record.py (see testdata/golden/README.md).
//
//   - parse: whether each statement of statements.jsonl parses, and the SQL it regenerates in
//     its dialect;
//   - annotate: the inferred types of the output columns of each query of annotate.jsonl;
//   - lineage: for each query of lineage.jsonl, its output columns after the optimizer rules
//     column lineage runs, each with its type and the (table, column) pairs it reads.
//
// Parse trees, token streams and error wording are not compared: they are internals the engine is
// free to change. Where the engine deliberately behaves differently from sqlglot, the input is
// listed in <family>.deviations.jsonl with a reason and the engine's expected record. Rewrite those
// lists from the Go output after an intentional behavior change with
//
//	go test ./pkg/sqlengine -run TestGolden -update
//
// then fill in the reason of every new entry and review the diff. -golden.dump=<file> writes every
// mismatch as a JSON line.

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

var (
	updateGolden = flag.Bool("update", false, "rewrite testdata/golden/*.deviations.jsonl from the current output")
	goldenDump   = flag.String("golden.dump", "", "append every golden mismatch to this file as JSON lines")
)

const goldenDir = "testdata/golden"

// dumpMu serializes writes to the -golden.dump file across the parallel golden tests.
var dumpMu sync.Mutex

func dumpMismatch(t *testing.T, family string, index int, dialect, sql string, want, got any) {
	t.Helper()
	if *goldenDump == "" {
		return
	}
	line, err := json.Marshal(map[string]any{
		"family": family, "index": index, "dialect": dialect, "sql": sql, "want": want, "got": got,
	})
	if err != nil {
		t.Fatal(err)
	}
	dumpMu.Lock()
	defer dumpMu.Unlock()
	f, err := os.OpenFile(*goldenDump, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
}

// goldenStatement is one line of statements.jsonl.
type goldenStatement struct {
	Dialect string `json:"dialect"`
	SQL     string `json:"sql"`
}

var goldenStatements = sync.OnceValues(func() ([]goldenStatement, error) {
	return readJSONL[goldenStatement](filepath.Join(goldenDir, "statements.jsonl"))
})

func readJSONL[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for line := 1; sc.Scan(); line++ {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		out = append(out, v)
	}
	return out, sc.Err()
}

func mustStatements(t testing.TB) []goldenStatement {
	t.Helper()
	stmts, err := goldenStatements()
	if err != nil {
		t.Fatal(err)
	}
	return stmts
}

func readGoldenGz(t testing.TB, name string, v any) {
	t.Helper()
	path := filepath.Join(goldenDir, name)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if err := json.NewDecoder(gz).Decode(v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// checkRecorded fails unless a golden file holds one record per input, for the same input.
func checkRecorded(t *testing.T, name string, n, recorded int, same func(i int) bool) {
	t.Helper()
	if recorded != n {
		t.Fatalf("%s has %d records for %d inputs: re-record it (see testdata/golden/README.md)", name, recorded, n)
	}
	for i := range n {
		if !same(i) {
			t.Fatalf("%s record %d is for a different input: re-record it (see testdata/golden/README.md)", name, i)
		}
	}
}

// mismatches collects failures per dialect and reports a bounded sample of them.
type mismatches struct {
	byDialect map[string]int
	samples   []string
}

func (m *mismatches) add(dialect, sample string) {
	if m.byDialect == nil {
		m.byDialect = map[string]int{}
	}
	m.byDialect[dialect]++
	if len(m.samples) < 20 {
		m.samples = append(m.samples, sample)
	}
}

func (m *mismatches) report(t *testing.T, what string, total int) {
	t.Helper()
	if len(m.byDialect) == 0 {
		return
	}
	dialects := make([]string, 0, len(m.byDialect))
	n := 0
	for d, c := range m.byDialect {
		dialects = append(dialects, fmt.Sprintf("%q: %d", d, c))
		n += c
	}
	sort.Strings(dialects)
	t.Errorf("%d/%d %s differ from the golden file (by dialect: %s)\n%s\n"+
		"If the change is intended, rerun with -update and review the diff.",
		n, total, what, strings.Join(dialects, ", "), strings.Join(m.samples, "\n----\n"))
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// errorText is the first line of an error message; golden files keep it for reading only.
func errorText(err error) string { return firstLine(err.Error()) }

func safeParse(d *Dialect, sql string) (trees []*Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return d.Parse(sql, nil)
}

func safeGenerate(d *Dialect, e *Expr) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return d.Generate(e, nil)
}

// deviation pins the engine's behavior for one input where it deliberately differs from the
// sqlglot recording.
type deviation struct {
	Dialect string          `json:"dialect"`
	SQL     string          `json:"sql"`
	Schema  any             `json:"schema,omitempty"`
	Reason  string          `json:"reason"`
	Want    json.RawMessage `json:"want"`
}

func deviationKey(dialect, sql string, schema any) string {
	return dialect + "\x00" + sql + "\x00" + string(mustJSON(schema))
}

func deviationsPath(family string) string {
	return filepath.Join(goldenDir, family+".deviations.jsonl")
}

func loadDeviations(t *testing.T, family string) map[string]deviation {
	t.Helper()
	devs, err := readJSONL[deviation](deviationsPath(family))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]deviation{}
	}
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]deviation, len(devs))
	for _, d := range devs {
		key := deviationKey(d.Dialect, d.SQL, d.Schema)
		if _, dup := out[key]; dup {
			t.Fatalf("%s lists [%s] %q twice", deviationsPath(family), d.Dialect, d.SQL)
		}
		out[key] = d
	}
	return out
}

func writeDeviations(t *testing.T, family string, devs []deviation) {
	t.Helper()
	path := deviationsPath(family)
	if len(devs) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		return
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	for _, d := range devs {
		if err := enc.Encode(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("rewrote %s (%d deviations)", path, len(devs))
}

// goldenCase is one input with its recorded (sqlglot) and computed records.
type goldenCase[R any] struct {
	index        int
	dialect, sql string
	schema       any
	want, got    R
}

// compareFamily checks the computed records against the recording, or against the input's entry
// in the family's deviations list. A deviation that no longer differs from the recording, one
// without a reason and one for an input that is gone are errors too. With -update it rewrites the
// deviations list from every current mismatch, keeping existing reasons.
func compareFamily[R any](t *testing.T, family, what string, cases []goldenCase[R], same func(w, g R) bool, describe func(R) string) {
	t.Helper()
	devs := loadDeviations(t, family)
	if *updateGolden {
		var out []deviation
		for _, c := range cases {
			if same(c.want, c.got) {
				continue
			}
			reason := devs[deviationKey(c.dialect, c.sql, c.schema)].Reason
			out = append(out, deviation{Dialect: c.dialect, SQL: c.sql, Schema: c.schema, Reason: reason, Want: mustJSON(c.got)})
		}
		writeDeviations(t, family, out)
		return
	}
	var m mismatches
	var stale, unexplained []string
	used := map[string]bool{}
	for _, c := range cases {
		key := deviationKey(c.dialect, c.sql, c.schema)
		want, dev := c.want, false
		if d, ok := devs[key]; ok {
			used[key], dev = true, true
			if same(c.want, c.got) {
				stale = append(stale, fmt.Sprintf("[%s] %s", c.dialect, trunc(c.sql, 200)))
				continue
			}
			if d.Reason == "" {
				unexplained = append(unexplained, fmt.Sprintf("[%s] %s", c.dialect, trunc(c.sql, 200)))
			}
			var w R
			if err := json.Unmarshal(d.Want, &w); err != nil {
				t.Fatalf("%s: [%s] %q: %v", deviationsPath(family), c.dialect, c.sql, err)
			}
			want = w
		}
		if same(want, c.got) {
			continue
		}
		label := "recorded"
		if dev {
			label = "deviation"
		}
		m.add(c.dialect, fmt.Sprintf("[%s] %s\nwant (%s): %s\n got: %s", c.dialect, trunc(c.sql, 500), label,
			trunc(describe(want), 1500), trunc(describe(c.got), 1500)))
		dumpMismatch(t, family, c.index, c.dialect, c.sql, want, c.got)
	}
	m.report(t, what, len(cases))
	for key, d := range devs {
		if !used[key] {
			t.Errorf("%s lists [%s] %q, which is not an input any more", deviationsPath(family), d.Dialect, trunc(d.SQL, 200))
		}
	}
	if len(stale) > 0 {
		t.Errorf("%d deviations in %s now match the recording; remove them (or rerun with -update):\n%s",
			len(stale), deviationsPath(family), strings.Join(stale[:min(len(stale), 20)], "\n"))
	}
	if len(unexplained) > 0 {
		t.Errorf("%d deviations in %s have no reason:\n%s", len(unexplained), deviationsPath(family),
			strings.Join(unexplained[:min(len(unexplained), 20)], "\n"))
	}
}

type parseRecord struct {
	Dialect string `json:"dialect"`
	SQL     string `json:"sql"`
	// Out is the regenerated SQL of each statement (null for an empty one); absent when parsing or
	// generation failed.
	Out      []*string `json:"out,omitempty"`
	Error    string    `json:"error,omitempty"`
	GenError string    `json:"gen_error,omitempty"`
}

func parseFor(s goldenStatement) parseRecord {
	d := MustDialect(s.Dialect)
	rec := parseRecord{Dialect: s.Dialect, SQL: s.SQL}
	trees, err := safeParse(d, s.SQL)
	if err != nil {
		rec.Error = errorText(err)
		return rec
	}
	out := make([]*string, len(trees))
	for i, tr := range trees {
		if tr == nil {
			continue
		}
		sql, err := safeGenerate(d, tr)
		if err != nil {
			rec.GenError = errorText(err)
			return rec
		}
		out[i] = &sql
	}
	rec.Out = out
	return rec
}

func sameParse(w, g parseRecord) bool {
	if (w.Error != "") != (g.Error != "") || (w.GenError != "") != (g.GenError != "") {
		return false
	}
	return (len(w.Out) == 0 && len(g.Out) == 0) || string(mustJSON(w.Out)) == string(mustJSON(g.Out))
}

func describeParse(r parseRecord) string {
	switch {
	case r.Error != "":
		return "parse error: " + r.Error
	case r.GenError != "":
		return "generate error: " + r.GenError
	}
	return string(mustJSON(r.Out))
}

func TestGoldenParse(t *testing.T) {
	t.Parallel()
	stmts := mustStatements(t)
	got := make([]parseRecord, len(stmts))
	for i, s := range stmts {
		got[i] = parseFor(s)
	}
	var want []parseRecord
	readGoldenGz(t, "parse.json.gz", &want)
	checkRecorded(t, "parse.json.gz", len(stmts), len(want), func(i int) bool {
		return want[i].Dialect == stmts[i].Dialect && want[i].SQL == stmts[i].SQL
	})
	cases := make([]goldenCase[parseRecord], len(stmts))
	for i, s := range stmts {
		cases[i] = goldenCase[parseRecord]{index: i, dialect: s.Dialect, sql: s.SQL, want: want[i], got: got[i]}
	}
	compareFamily(t, "parse", "statements", cases, sameParse, describeParse)
}

type annotateInput struct {
	Dialect string `json:"dialect"`
	SQL     string `json:"sql"`
	Schema  bool   `json:"schema"`
}

type annotateCase struct {
	Dialect string `json:"dialect"`
	SQL     string `json:"sql"`
	Schema  bool   `json:"schema"`
	// Types are the output column types of a query, or the type of a bare expression.
	Types []string `json:"types,omitempty"`
	Error string   `json:"error,omitempty"`
}

// annotateSchema must match ANNOTATE_SCHEMA in record.py (key order matters).
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
	s.Set("tbl", mk(
		"bin_col", "BINARY", "str_col", "STRING", "bignum_col", "BIGNUMERIC", "date_col", "DATE",
		"decfloat_col", "DECFLOAT", "float_col", "FLOAT", "timestamp_col", "TIMESTAMP", "double_col", "DOUBLE",
		"bigint_col", "BIGINT", "smallint_col", "SMALLINT", "bit_col", "BIT", "obj_col", "OBJECT",
		"int_col", "INT", "bool_col", "BOOLEAN", "bytes_col", "BYTES", "interval_col", "INTERVAL",
		"array_col", "ARRAY<STRING>",
	))
	return s
}

func typeSQL(e *Expr) string {
	if e.Type() == nil {
		return "None"
	}
	sql, err := e.Type().SQL("", nil)
	if err != nil {
		return "ERR " + err.Error()
	}
	return sql
}

func annotateFor(in annotateInput) (rec annotateCase) {
	rec = annotateCase{Dialect: in.Dialect, SQL: in.SQL, Schema: in.Schema}
	defer func() {
		if r := recover(); r != nil {
			rec.Types, rec.Error = nil, fmt.Sprintf("panic: %v", r)
		}
	}()
	d := MustDialect(in.Dialect)
	e, err := d.ParseOne(in.SQL, nil)
	if err != nil {
		rec.Error = errorText(err)
		return rec
	}
	opts := AnnotateOptions{Dialect: d}
	if in.Schema {
		opts.SchemaMap = annotateSchema()
	}
	AnnotateTypes(e, opts)
	if e.IsA(KQuery) {
		for _, s := range e.Selects() {
			rec.Types = append(rec.Types, typeSQL(s))
		}
		return rec
	}
	rec.Types = []string{typeSQL(e)}
	return rec
}

func TestGoldenAnnotate(t *testing.T) {
	t.Parallel()
	inputs, err := readJSONL[annotateInput](filepath.Join(goldenDir, "annotate.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	got := make([]annotateCase, len(inputs))
	for i, in := range inputs {
		got[i] = annotateFor(in)
	}
	var want []annotateCase
	readGoldenGz(t, "annotate.json.gz", &want)
	checkRecorded(t, "annotate.json.gz", len(inputs), len(want), func(i int) bool {
		return want[i].Dialect == inputs[i].Dialect && want[i].SQL == inputs[i].SQL && want[i].Schema == inputs[i].Schema
	})
	cases := make([]goldenCase[annotateCase], len(inputs))
	for i, in := range inputs {
		cases[i] = goldenCase[annotateCase]{index: i, dialect: in.Dialect, sql: in.SQL, schema: in.Schema, want: want[i], got: got[i]}
	}
	compareFamily(t, "annotate", "annotated queries", cases, func(w, g annotateCase) bool {
		return (w.Error != "") == (g.Error != "") && strings.Join(w.Types, "\n") == strings.Join(g.Types, "\n")
	}, func(r annotateCase) string { return fmt.Sprintf("%v %s", r.Types, r.Error) })
}

type lineageInput struct {
	Dialect string `json:"dialect"`
	SQL     string `json:"sql"`
	Schema  string `json:"schema"`
}

type lineageCase struct {
	Dialect string `json:"dialect"`
	SQL     string `json:"sql"`
	Schema  string `json:"schema"`
	// Columns lists the output columns as [name, type, sources], sources being the sorted
	// [table, column] pairs the column reads, or null when it has no lineage graph.
	Columns [][]any `json:"columns,omitempty"`
	Error   string  `json:"error,omitempty"`
}

var lineageRules = []OptimizerRule{RuleQualify, RuleUnnestSubqueries, RuleMergeSubqueries, RuleAnnotateTypes}

// goldenSchemas reads schemas.json: name -> table -> column -> type. Keys are applied in sorted
// order, the order record.py sees them in (the file is written with sorted keys).
func goldenSchemas(t *testing.T) map[string]*SchemaMap {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(goldenDir, "schemas.json"))
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]map[string]map[string]string
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	out := map[string]*SchemaMap{}
	for name, tables := range raw {
		s := NewSchemaMap()
		for _, table := range sortedKeys(tables) {
			cols := NewSchemaMap()
			for _, col := range sortedKeys(tables[table]) {
				cols.Set(col, tables[table][col])
			}
			s.Set(table, cols)
		}
		out[name] = s
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sourceColumns returns the sorted [table, column] pairs a lineage graph reads: its leaves that
// are tables.
func sourceColumns(n *LineageNode) []any {
	found := map[[2]string]bool{}
	for _, x := range n.Walk() {
		if len(x.Downstream) == 0 && x.Expression.IsA(KTable) {
			var parts []string
			for _, p := range x.Expression.Parts() {
				parts = append(parts, p.Name())
			}
			column := x.Name[strings.LastIndexByte(x.Name, '.')+1:]
			found[[2]string{strings.Join(parts, "."), column}] = true
		}
	}
	pairs := make([][2]string, 0, len(found))
	for p := range found {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	out := make([]any, len(pairs))
	for i, p := range pairs {
		out[i] = []any{p[0], p[1]}
	}
	return out
}

func lineageFor(in lineageInput, schemas map[string]*SchemaMap) (rec lineageCase) {
	rec = lineageCase{Dialect: in.Dialect, SQL: in.SQL, Schema: in.Schema}
	defer func() {
		if r := recover(); r != nil {
			rec.Columns, rec.Error = nil, fmt.Sprintf("panic: %v", r)
		}
	}()
	d := MustDialect(in.Dialect)
	schema := schemas[in.Schema]
	if schema == nil {
		schema = NewSchemaMap()
	}
	parsed, err := d.ParseOne(in.SQL, nil)
	if err != nil {
		rec.Error = errorText(err)
		return rec
	}
	if !parsed.IsA(KQuery) {
		rec.Error = "not a query"
		return rec
	}
	opt, err := OptimizeSafe(parsed.Copy(), schema, d, lineageRules)
	if err != nil {
		// Column lineage falls back to every optimizer rule, under the base dialect, when its own
		// rules fail.
		if opt, err = OptimizeSafe(parsed, schema, MustDialect(""), nil); err != nil {
			rec.Error = errorText(err)
			return rec
		}
	}
	_, graph, err := LineageAll(opt, LineageOptions{
		Schema: NewMappingSchema(schema, nil, d, true, nil), Dialect: d, Scope: BuildScope(opt),
		TrimSelects: false, Copy: false,
	})
	if err != nil {
		graph = nil
	}
	rec.Columns = [][]any{}
	for _, s := range opt.Selects() {
		name := s.AliasOrName()
		typ := ""
		if s.Type() != nil {
			if sql, err := s.Type().SQL(d.Name, nil); err == nil {
				typ = sql
			}
		}
		var sources any
		if n, ok := graph[name]; ok {
			sources = sourceColumns(n)
		}
		rec.Columns = append(rec.Columns, []any{name, typ, sources})
	}
	return rec
}

func TestGoldenLineage(t *testing.T) {
	t.Parallel()
	inputs, err := readJSONL[lineageInput](filepath.Join(goldenDir, "lineage.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	schemas := goldenSchemas(t)
	got := make([]lineageCase, len(inputs))
	for i, in := range inputs {
		got[i] = lineageFor(in, schemas)
	}
	var want []lineageCase
	readGoldenGz(t, "lineage.json.gz", &want)
	checkRecorded(t, "lineage.json.gz", len(inputs), len(want), func(i int) bool {
		return want[i].Dialect == inputs[i].Dialect && want[i].SQL == inputs[i].SQL && want[i].Schema == inputs[i].Schema
	})
	cases := make([]goldenCase[lineageCase], len(inputs))
	for i, in := range inputs {
		// Round-trip the computed case through JSON so it compares like the recorded one.
		var g lineageCase
		if err := json.Unmarshal(mustJSON(got[i]), &g); err != nil {
			t.Fatal(err)
		}
		cases[i] = goldenCase[lineageCase]{index: i, dialect: in.Dialect, sql: in.SQL, schema: in.Schema, want: want[i], got: g}
	}
	compareFamily(t, "lineage", "lineage queries", cases, func(w, g lineageCase) bool {
		return (w.Error != "") == (g.Error != "") && string(mustJSON(w.Columns)) == string(mustJSON(g.Columns))
	}, func(r lineageCase) string { return string(mustJSON(r.Columns)) + " " + r.Error })
}
