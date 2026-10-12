package sqlengine

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// Parse conformance: for every statement of the corpus (sqlglot's test-suite SQL, recorded by
// codegen/gen_parse_expected.py), the engine must fail to parse exactly when sqlglot does, and
// otherwise generate the same SQL back in the same dialect.
//
// CONFORMANCE_DIALECT=<d> restricts the run to one dialect; CONFORMANCE_DUMP=<prefix> writes the
// mismatches to <prefix>.<check>.txt.

type parseCase struct {
	Dialect  string    `json:"dialect"`
	SQL      string    `json:"sql"`
	Out      []*string `json:"out"`
	Error    string    `json:"error"`
	GenError string    `json:"gen_error"`
}

func loadGz(t testing.TB, path string, v any) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("missing %s: %v", path, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(gz).Decode(v); err != nil {
		t.Fatal(err)
	}
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
	t.Helper()
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
	if path := os.Getenv("CONFORMANCE_DUMP"); path != "" {
		var out strings.Builder
		for _, d := range ds {
			for _, smp := range s.samples[d] {
				out.WriteString(smp)
				out.WriteString("\n----\n")
			}
		}
		_ = os.WriteFile(path+"."+name+".txt", []byte(out.String()), 0o644)
	}
	if pass == tot {
		t.Logf("%s: %d/%d", name, pass, tot)
		return
	}
	var examples []string
	for _, d := range ds {
		for _, smp := range s.samples[d] {
			if len(examples) < 10 {
				examples = append(examples, smp)
			}
		}
	}
	t.Errorf("%s: %d/%d statements match\n%s\n%s", name, pass, tot, b.String(), strings.Join(examples, "\n----\n"))
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
	only := os.Getenv("CONFORMANCE_DIALECT")
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
			parseStats.add(c.Dialect, err != nil, fmt.Sprintf("[%s] %s\nwant an error (%s), parsed fine", c.Dialect, c.SQL, firstLine(c.Error)))
			continue
		}
		parseStats.add(c.Dialect, err == nil, fmt.Sprintf("[%s] %s\nunexpected error: %v", c.Dialect, c.SQL, err))
		if err != nil {
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
		if c.GenError != "" {
			genStats.add(c.Dialect, gerr != nil, fmt.Sprintf("[%s] %s\nwant a generation error (%s), got %q", c.Dialect, c.SQL, firstLine(c.GenError), outs))
			continue
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
