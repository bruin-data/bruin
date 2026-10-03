package sqlengine

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

type tokenCase struct {
	Dialect string  `json:"dialect"`
	SQL     string  `json:"sql"`
	Tokens  [][]any `json:"tokens"`
	Error   string  `json:"error"`
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

func TestConformanceTokens(t *testing.T) {
	var cases []tokenCase
	loadGz(t, "testdata/tokens.json.gz", &cases)
	failures := map[string]int{}
	var samples []string
	for _, c := range cases {
		d, err := GetDialect(c.Dialect)
		if err != nil {
			t.Fatalf("dialect %q: %v", c.Dialect, err)
		}
		toks, err := d.Tokenize(c.SQL)
		got := ""
		if err != nil {
			got = "TokenError: " + err.Error()
		}
		want := c.Error
		if want == "" {
			var b strings.Builder
			for _, tk := range c.Tokens {
				fmt.Fprintf(&b, "%v|%v|%v|%v|%v|%v|%v\n", tk[0], tk[1], tk[2], tk[3], tk[4], tk[5], tk[6])
			}
			want = b.String()
		}
		if err == nil {
			var b strings.Builder
			for _, tk := range toks {
				comments := make([]any, len(tk.Comments))
				for i, c := range tk.Comments {
					comments[i] = c
				}
				fmt.Fprintf(&b, "%v|%v|%v|%v|%v|%v|%v\n", tk.Type.Name(), tk.Text, float64(tk.Line), float64(tk.Col), float64(tk.Start), float64(tk.End), comments)
			}
			got = b.String()
		}
		if got != want {
			failures[c.Dialect]++
			if len(samples) < 15 {
				samples = append(samples, fmt.Sprintf("[%s] %q\n want: %s\n  got: %s", c.Dialect, c.SQL, trunc(want, 600), trunc(got, 600)))
			}
		}
	}
	total := 0
	for _, n := range failures {
		total += n
	}
	if total > 0 {
		t.Errorf("%d/%d token mismatches by dialect: %v\n%s", total, len(cases), failures, strings.Join(samples, "\n"))
	}
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
