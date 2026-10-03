package sqlparser

// Golden tests: Bruin's parser commands replayed against the responses recorded from the original
// Python implementation (pythonsrc on sqlglot 30.13.0), see codegen/README.md.
//
//   - commands.json.gz: every statement of the sqlglot conformance corpus (Bruin dialects) through
//     every command Bruin issues.
//   - fixtures.json.gz: sqlglot's optimizer fixtures (TPC-H, TPC-DS, ...) through lineage, tables
//     and rename, with their schemas, for every Bruin dialect.
//   - fuzz.json.gz: malformed variants of the corpus (truncated, dropped, duplicated and swapped
//     tokens). Skipped with -short.
//
// After an intentional behavior change, rewrite them from the Go output with
//
//	go test ./pkg/sqlparser -run TestGolden -update
//
// and review the diff (e.g. by decoding both versions with codegen/golden_diff.py).

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/golden/*.json.gz from the current output")

type goldenRecord struct {
	Cmd  json.RawMessage `json:"cmd"`
	Want any             `json:"want"`
}

func TestGoldenCommands(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"commands", "fixtures"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			replayGolden(t, name)
		})
	}
}

func TestGoldenFuzz(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("fuzz goldens skipped with -short")
	}
	replayGolden(t, "fuzz")
}

// pythonTimeout marks commands on which the Python implementation hung (killed after 5 s). The Go
// port returns an error for them instead (see TRADEOFFS.md); they are not compared.
var pythonTimeout = map[string]any{"__timeout__": true}

func replayGolden(t *testing.T, name string) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name+".json.gz")
	recs := readGolden(t, path)

	// The recordings were made after the BigQuery dialect was loaded, which changes type coercion
	// for every dialect (see TRADEOFFS.md).
	_, _ = sqlengine.GetDialect("bigquery")

	step := 1
	if raceEnabled && !*updateGolden {
		// The race detector makes a full replay too slow; a sample still exercises concurrency.
		step = 25
	}

	got := make([]any, len(recs))
	var next sync.Mutex
	pos := 0
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for {
				next.Lock()
				i := pos
				pos += step
				next.Unlock()
				if i >= len(recs) {
					return
				}
				if reflect.DeepEqual(recs[i].Want, pythonTimeout) {
					got[i] = pythonTimeout
					continue
				}
				var pc parserCommand
				if err := json.Unmarshal(recs[i].Cmd, &pc); err != nil {
					got[i] = map[string]any{"__bad_record__": err.Error()}
					continue
				}
				var v any
				if err := json.Unmarshal([]byte(dispatch(&pc)), &v); err != nil {
					v = map[string]any{"__bad_response__": err.Error()}
				}
				got[i] = v
			}
		})
	}
	wg.Wait()

	if *updateGolden {
		for i := range recs {
			recs[i].Want = got[i]
		}
		writeGolden(t, path, recs)
		return
	}

	checked, hung, failed := 0, 0, map[string]int{}
	var examples []string
	for i := 0; i < len(recs); i += step {
		if reflect.DeepEqual(recs[i].Want, pythonTimeout) {
			hung++
			continue
		}
		checked++
		if sameResponse(got[i], recs[i].Want) {
			continue
		}
		var pc parserCommand
		_ = json.Unmarshal(recs[i].Cmd, &pc)
		failed[fmt.Sprintf("%s/%v", pc.Command, pc.Contents["dialect"])]++
		if len(examples) < 20 {
			wantJSON, _ := json.Marshal(recs[i].Want)
			gotJSON, _ := json.Marshal(got[i])
			examples = append(examples, fmt.Sprintf("%s\n  want: %s\n   got: %s", recs[i].Cmd, wantJSON, gotJSON))
		}
	}
	if len(failed) == 0 {
		t.Logf("%s: %d commands match (%d not compared: Python hung)", name, checked, hung)
		return
	}
	keys := make([]string, 0, len(failed))
	total := 0
	for k, n := range failed {
		keys = append(keys, fmt.Sprintf("%s: %d", k, n))
		total += n
	}
	sort.Strings(keys)
	t.Errorf("%s: %d/%d commands differ from %s (by command/dialect: %s)\n%s\n"+
		"If the change is intended, rerun with -update and review the diff.",
		name, total, checked, path, strings.Join(keys, ", "), strings.Join(examples, "\n"))
}

// requiredKeywordRE matches the message naming one of several missing required arguments. Python
// picks the keyword from a set, so which one it named depends on PYTHONHASHSEED.
var requiredKeywordRE = regexp.MustCompile(`Required keyword: '[^']*' missing for`)

func sameResponse(got, want any) bool {
	if reflect.DeepEqual(got, want) {
		return true
	}
	g, _ := json.Marshal(got)
	w, _ := json.Marshal(want)
	const anyKeyword = "Required keyword: '*' missing for"
	return requiredKeywordRE.ReplaceAllString(string(g), anyKeyword) == requiredKeywordRE.ReplaceAllString(string(w), anyKeyword)
}

func readGolden(t *testing.T, path string) []goldenRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var recs []goldenRecord
	if err := json.NewDecoder(zr).Decode(&recs); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return recs
}

func writeGolden(t *testing.T, path string, recs []goldenRecord) {
	t.Helper()
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(recs); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	zw, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(body.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("rewrote %s (%d commands)", path, len(recs))
}
