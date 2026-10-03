package sqlengine

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
)

// diffFalse walks two serialized trees and records (kind.key want->got) where one side is
// false and the other null.
func diffFalse(w, g any, kind string, out map[string]int) {
	switch wv := w.(type) {
	case map[string]any:
		gv, ok := g.(map[string]any)
		if !ok {
			return
		}
		k, _ := wv["k"].(string)
		wa, _ := wv["a"].([]any)
		ga, _ := gv["a"].([]any)
		gm := map[string]any{}
		for _, p := range ga {
			pp := p.([]any)
			gm[pp[0].(string)] = pp[1]
		}
		wm := map[string]any{}
		for _, p := range wa {
			pp := p.([]any)
			wm[pp[0].(string)] = pp[1]
		}
		for key, x := range wm {
			y, has := gm[key]
			if (x == false && (y == nil)) || (x == nil && y == false) {
				out[fmt.Sprintf("%s.%s want=%v got=%v present=%v", k, key, x, y, has)]++
				continue
			}
			diffFalse(x, y, k, out)
		}
		for key, y := range gm {
			if _, ok := wm[key]; !ok && y == false {
				out[fmt.Sprintf("%s.%s want=<absent> got=false", k, key)]++
			}
		}
	case []any:
		gv, ok := g.([]any)
		if !ok || len(gv) != len(wv) {
			return
		}
		for i := range wv {
			diffFalse(wv[i], gv[i], kind, out)
		}
	}
}

// TestFalseDiff (FALSEDIFF=1) lists parse-tree differences that are only False vs None, grouped
// by (class, arg); a diagnostic for the exact comparison in TestConformanceParse.
func TestFalseDiff(t *testing.T) {
	if os.Getenv("FALSEDIFF") == "" {
		t.Skip()
	}
	var cases []parseCase
	loadGz(t, "testdata/parse.json.gz", &cases)
	out := map[string]int{}
	examples := map[string]string{}
	for _, c := range cases {
		if c.Error != "" {
			continue
		}
		d, _ := GetDialect(c.Dialect)
		trees, err := safeParse(d, c.SQL)
		if err != nil {
			continue
		}
		got := make([]any, len(trees))
		for i, tr := range trees {
			got[i] = serExpr(tr)
		}
		// round-trip through JSON so types match the expected side
		var gj any
		b := canonRaw(got)
		_ = json.Unmarshal(b, &gj)
		before := len(out)
		local := map[string]int{}
		diffFalse(c.Trees, gj, "", local)
		for k, v := range local {
			out[k] += v
			if _, ok := examples[k]; !ok {
				examples[k] = "[" + c.Dialect + "] " + c.SQL
			}
		}
		_ = before
	}
	var keys []string
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%4d %s   e.g. %.120s", out[k], k, examples[k])
	}
}

func canonRaw(v any) []byte { b, _ := json.Marshal(v); return b }
