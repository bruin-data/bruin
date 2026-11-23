package sqlparser

// Golden tests: Bruin's parser commands replayed against recorded responses. Each record is
// {"cmd": {"command": ..., "contents": ...}, "want": <response>}; the test sends cmd through
// dispatch and compares the JSON response.
//
//   - commands.json.gz: the statements of the engine's golden corpus
//     (pkg/sqlengine/testdata/golden/statements.jsonl) in a Bruin dialect, through every command
//     Bruin issues.
//   - fixtures.json.gz: optimizer fixtures (TPC-H, TPC-DS and smaller query sets) through
//     lineage (with and without their schemas), tables and rename, for every Bruin dialect.
//   - fuzz.json.gz: malformed variants of the corpus (truncated, dropped, duplicated and swapped
//     tokens) through 7 commands. Skipped with -short.
//   - hoist.json.gz: multi-statement scripts and hook lists composed around DECLAREs (procedural
//     blocks, comments, separators, malformed variants) through hoist-declares and
//     hoist-declares-list, in every dialect Bruin hoists for.
//
// The responses were first recorded from Bruin's former Python implementation and now pin the
// Go behavior. After an intentional behavior change, rewrite them with
//
//	go test ./pkg/sqlparser -run TestGolden -update
//
// and review the diff (decode both versions, e.g. `gzip -dc f.json.gz | jq -S .`). The inputs
// exist only in these records: -update keeps every cmd and rewrites the responses. New cases
// belong in the *_contract_test.go suites.

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/golden/*.json.gz from the current output")

type goldenRecord struct {
	Cmd  json.RawMessage `json:"cmd"`
	Want any             `json:"want"`
}

func TestGoldenCommands(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"commands", "fixtures", "hoist"} {
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

func replayGolden(t *testing.T, name string) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name+".json.gz")
	recs := readGolden(t, path)

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

	checked, failed := 0, map[string]int{}
	var examples []string
	for i := 0; i < len(recs); i += step {
		checked++
		if reflect.DeepEqual(got[i], recs[i].Want) {
			continue
		}
		var pc parserCommand
		_ = json.Unmarshal(recs[i].Cmd, &pc)
		failed[fmt.Sprintf("%s/%v", pc.Command, pc.Contents["dialect"])]++
		if len(examples) < 20 {
			examples = append(examples, fmt.Sprintf("%s\n  want: %s\n   got: %s", recs[i].Cmd, mustJSON(recs[i].Want), mustJSON(got[i])))
		}
	}
	if len(failed) == 0 {
		t.Logf("%s: %d commands match", name, checked)
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

// mustJSON encodes a decoded JSON value, which always encodes.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func readGolden(tb testing.TB, path string) []goldenRecord {
	tb.Helper()
	recs, err := loadGolden(path)
	if err != nil {
		tb.Fatal(err)
	}
	return recs
}

func loadGolden(path string) ([]goldenRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var recs []goldenRecord
	if err := json.NewDecoder(zr).Decode(&recs); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return recs, nil
}

// corpusQuery is one statement of the golden corpus in a Bruin dialect.
type corpusQuery struct {
	dialect, sql string
	// parses reports whether get-tables succeeded on it.
	parses bool
}

// goldenCorpus lists the statements of commands.json.gz, taken from its get-tables commands.
var goldenCorpus = sync.OnceValues(func() ([]corpusQuery, error) {
	recs, err := loadGolden(filepath.Join("testdata", "golden", "commands.json.gz"))
	if err != nil {
		return nil, err
	}
	var out []corpusQuery
	for _, r := range recs {
		var pc parserCommand
		if err := json.Unmarshal(r.Cmd, &pc); err != nil {
			return nil, err
		}
		if pc.Command != "get-tables" {
			continue
		}
		d, _ := pc.Contents["dialect"].(string)
		q, _ := pc.Contents["query"].(string)
		want, _ := r.Want.(map[string]any)
		errText, _ := want["error"].(string)
		out = append(out, corpusQuery{dialect: d, sql: q, parses: errText == ""})
	}
	return out, nil
})

func mustGoldenCorpus(tb testing.TB) []corpusQuery {
	tb.Helper()
	qs, err := goldenCorpus()
	if err != nil {
		tb.Fatal(err)
	}
	return qs
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
