package sqlengine

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

type parseCase struct {
	Dialect  string    `json:"dialect"`
	SQL      string    `json:"sql"`
	Trees    []any     `json:"trees"`
	Out      []*string `json:"out"`
	Error    string    `json:"error"`
	GenError string    `json:"gen_error"`
}

// serExpr mirrors gen_parse_expected.py:ser.
func serExpr(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case *Expr:
		if x == nil {
			return nil
		}
		args := make([]any, 0, len(x.args))
		for _, a := range x.args {
			args = append(args, []any{a.key, serExpr(a.val)})
		}
		out := map[string]any{"k": x.kind.Name(), "a": args}
		if len(x.Comments()) > 0 {
			cs := make([]any, len(x.Comments()))
			for i, c := range x.Comments() {
				cs[i] = c
			}
			out["c"] = cs
		}
		return out
	case []*Expr:
		out := make([]any, len(x))
		for i, y := range x {
			out[i] = serExpr(y)
		}
		return out
	case []string:
		out := make([]any, len(x))
		for i, y := range x {
			out[i] = y
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, y := range x {
			out[i] = serExpr(y)
		}
		return out
	case string, bool:
		return x
	case int:
		return map[string]any{"int": x}
	case DType:
		return map[string]any{"dt": dtypeNames[x]}
	case *Dialect:
		return map[string]any{"repr": "<dialect " + x.Name + ">"}
	}
	return map[string]any{"repr": fmt.Sprintf("%T:%v", v, v)}
}

// canon serializes a tree for comparison. False and None are compared exactly (they behave
// differently in Expr.error_messages); SQLGLOT_CONF_LOOSE_FALSE=1 treats them as equal.
func canon(v any) string {
	if os.Getenv("SQLGLOT_CONF_LOOSE_FALSE") != "" {
		return string(mustJSON(normFalse(v)))
	}
	return string(mustJSON(normRepr(v)))
}

// mustJSON encodes values decoded from JSON (or built from such values), which always encode.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// normRepr only normalizes Python object reprs (see normFalse).
func normRepr(v any) any {
	switch x := v.(type) {
	case string:
		if m := pyObjectRepr.FindStringSubmatch(x); m != nil {
			return "<dialect " + m[1] + ">"
		}
		return x
	case []any:
		out := make([]any, len(x))
		for i, y := range x {
			out[i] = normRepr(y)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, y := range x {
			out[k] = normRepr(y)
		}
		return out
	}
	return v
}

// pyObjectRepr matches the repr of a Dialect instance stored as an argument value (e.g. the
// `dialect` arg of DataType.into_expr), which embeds a memory address.
var pyObjectRepr = regexp.MustCompile(`^<sqlglot\.dialects\.(\w+)\.\w+ object at 0x[0-9a-f]+>$`)

func normFalse(v any) any {
	switch x := v.(type) {
	case string:
		if m := pyObjectRepr.FindStringSubmatch(x); m != nil {
			return "<dialect " + m[1] + ">"
		}
		return x
	case bool:
		if !x {
			return nil
		}
		return x
	case []any:
		out := make([]any, len(x))
		for i, y := range x {
			out[i] = normFalse(y)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, y := range x {
			out[k] = normFalse(y)
		}
		return out
	}
	return v
}

func errorTypeName(err error) string {
	switch err.(type) {
	case *ParseError:
		return "ParseError"
	case *TokenError:
		return "TokenError"
	case *ValueError:
		return "ValueError"
	case *UnsupportedError:
		return "UnsupportedError"
	}
	return "Error"
}

type confStats struct {
	total, pass map[string]int
	samples     map[string][]string
}

func newConfStats() *confStats {
	return &confStats{total: map[string]int{}, pass: map[string]int{}, samples: map[string][]string{}}
}

func (s *confStats) add(dialect string, ok bool, sample string) {
	s.total[dialect]++
	if ok {
		s.pass[dialect]++
	} else {
		s.samples[dialect] = append(s.samples[dialect], sample)
	}
}

func (s *confStats) report(t *testing.T, name string) {
	var ds []string
	for d := range s.total {
		ds = append(ds, d)
	}
	sort.Strings(ds)
	var b strings.Builder
	tot, pass := 0, 0
	for _, d := range ds {
		tot += s.total[d]
		pass += s.pass[d]
		fmt.Fprintf(&b, "  %-12s %5d/%5d\n", d, s.pass[d], s.total[d])
	}
	t.Logf("%s: %d/%d (%.1f%%)\n%s", name, pass, tot, 100*float64(pass)/float64(tot), b.String())
	if path := os.Getenv("SQLGLOT_CONF_DUMP"); path != "" {
		var out strings.Builder
		for _, d := range ds {
			for _, smp := range s.samples[d] {
				out.WriteString(smp)
				out.WriteString("\n----\n")
			}
		}
		_ = os.WriteFile(path+"."+name+".txt", []byte(out.String()), 0o644)
	}
	if pass != tot && os.Getenv("SQLGLOT_CONF_STRICT") != "" {
		t.Fail()
	}
}

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

func TestConformanceParse(t *testing.T) {
	t.Parallel()
	var cases []parseCase
	loadGz(t, "testdata/parse.json.gz", &cases)
	only := os.Getenv("SQLGLOT_CONF_DIALECT")
	parseStats, genStats := newConfStats(), newConfStats()
	for _, c := range cases {
		if only != "" && c.Dialect != only {
			continue
		}
		d, err := GetDialect(c.Dialect)
		if err != nil {
			t.Fatalf("dialect %q: %v", c.Dialect, err)
		}
		trees, err := safeParse(d, c.SQL)
		if c.Error != "" {
			got := ""
			if err != nil {
				got = errorTypeName(err) + ": " + err.Error()
			}
			ok := err != nil && firstLine(got) == firstLine(c.Error)
			parseStats.add(c.Dialect, ok, fmt.Sprintf("[%s] %s\nwant error: %s\n got: %s", c.Dialect, c.SQL, firstLine(c.Error), firstLine(got)))
			continue
		}
		if err != nil {
			parseStats.add(c.Dialect, false, fmt.Sprintf("[%s] %s\nunexpected error: %v", c.Dialect, c.SQL, firstLine(err.Error())))
			continue
		}
		got := make([]any, len(trees))
		for i, tr := range trees {
			got[i] = serExpr(tr)
		}
		gs, ws := canon(got), canon(c.Trees)
		parseStats.add(c.Dialect, gs == ws, fmt.Sprintf("[%s] %s\nwant: %s\n got: %s", c.Dialect, c.SQL, trunc(ws, 3000), trunc(gs, 3000)))
		if gs != ws || c.GenError != "" {
			continue
		}
		var outs []string
		var gerr error
		for _, tr := range trees {
			if tr == nil {
				outs = append(outs, "<nil>")
				continue
			}
			o, err := safeGenerate(d, tr)
			if err != nil {
				gerr = err
				break
			}
			outs = append(outs, o)
		}
		var want []string
		for _, o := range c.Out {
			if o == nil {
				want = append(want, "<nil>")
			} else {
				want = append(want, *o)
			}
		}
		ok := gerr == nil && strings.Join(outs, "\x00") == strings.Join(want, "\x00")
		genStats.add(c.Dialect, ok, fmt.Sprintf("[%s] %s\nwant: %q\n got: %q err=%v", c.Dialect, c.SQL, want, outs, gerr))
	}
	parseStats.report(t, "parse")
	genStats.report(t, "generate")
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
