package sqlparser

// Golden tests: Bruin's parser commands replayed against the responses recorded from the reference
// implementation (Bruin's former Python parser, pythonsrc on sqlglot 30.13.0), see
// codegen/README.md. Results must match exactly; errors must occur on the same commands, with any
// message.
//
//   - commands.json.gz: every statement of the sqlglot conformance corpus (Bruin dialects) through
//     every command Bruin issues.
//   - fixtures.json.gz: sqlglot's optimizer fixtures (TPC-H, TPC-DS, ...) through lineage, tables
//     and rename, with their schemas, for every Bruin dialect.
//   - fuzz.json.gz: malformed variants of the corpus (truncated, dropped, duplicated and swapped
//     tokens). Skipped with -short.
//   - hoist.json.gz: multi-statement scripts and hook lists composed around DECLAREs (procedural
//     blocks, comments, separators, malformed variants) through hoist-declares and
//     hoist-declares-list, in every dialect Bruin hoists for.
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
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
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

// pythonTimeout marks commands on which the Python implementation hung (killed after 5 s). The Go
// port returns an error for them instead (see TRADEOFFS.md); they are not compared.
var pythonTimeout = map[string]any{"__timeout__": true}

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
			examples = append(examples, fmt.Sprintf("%s\n  want: %s\n   got: %s", recs[i].Cmd, mustJSON(recs[i].Want), mustJSON(got[i])))
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

// sameResponse compares responses with every error message replaced by a placeholder: an error must
// occur exactly when the reference implementation reported one, but its wording is ours.
func sameResponse(got, want any) bool {
	return reflect.DeepEqual(maskErrors(got), maskErrors(want))
}

// maskErrors replaces the non-empty "error" strings and the "errors" list entries of a decoded
// response with "<error>".
func maskErrors(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := make(map[string]any, len(m))
	for k, x := range m {
		switch {
		case k == "error" && x != "" && x != nil:
			x = "<error>"
		case k == "errors":
			if list, ok := x.([]any); ok {
				masked := make([]any, len(list))
				for i := range list {
					masked[i] = "<error>"
				}
				x = masked
			}
		}
		out[k] = x
	}
	return out
}

// mustJSON encodes a decoded JSON value, which always encodes.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
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

// TestPublicMethodsMatchCommands checks, over the golden corpora, that each public method returns
// what decoding its command's JSON response gives. The goldens check the commands; this checks the
// typed responses the methods build from them.
func TestPublicMethodsMatchCommands(t *testing.T) {
	t.Parallel()
	p, err := NewSQLParser()
	if err != nil {
		t.Fatal(err)
	}
	p.MaxQueryLength = 1 << 30 // the length cutoff is not part of the commands

	hoistType := map[string]pipeline.AssetType{}
	for typ, d := range assetTypeDialectMap {
		hoistType[d] = typ
	}

	for _, name := range []string{"commands", "fixtures", "hoist"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Every third command keeps the test fast and still covers each command and dialect.
			step := 3
			if raceEnabled {
				step = 25
			}
			recs := readGolden(t, filepath.Join("testdata", "golden", name+".json.gz"))
			checked := 0
			for i := 0; i < len(recs); i += step {
				if reflect.DeepEqual(recs[i].Want, pythonTimeout) {
					continue
				}
				var pc parserCommand
				if err := json.Unmarshal(recs[i].Cmd, &pc); err != nil {
					t.Fatal(err)
				}
				c := pc.Contents
				q, d := str(c["query"]), str(c["dialect"])
				if pc.Command == "lineage" && c["schema"] == nil {
					// ColumnLineage reads a nil schema as an empty one; the command rejects null.
					c["schema"] = map[string]any{}
				}
				// decode runs the command and json.Unmarshals its response into want.
				decode := func(want any) {
					if err := json.Unmarshal([]byte(dispatch(&pc)), want); err != nil {
						t.Fatal(err)
					}
				}
				var got, want any
				var gotErr error
				var wantErr string
				switch pc.Command {
				case "lineage":
					var r Lineage
					decode(&r)
					var schema Schema
					if err := json.Unmarshal(mustJSON(c["schema"]), &schema); err != nil {
						t.Fatal(err)
					}
					want = &r
					got, gotErr = p.ColumnLineage(q, d, schema)
				case "get-tables":
					var r tablesResponse
					decode(&r)
					sort.Strings(r.Tables)
					want, wantErr = r.Tables, r.Error
					got, gotErr = p.UsedTables(q, d)
				case "is-read-only":
					var r readOnlyResponse
					decode(&r)
					want, wantErr = r.ReadOnly, r.Error
					got, gotErr = p.IsReadOnlyQuery(q, d)
				case "is-single-select":
					var r singleSelectResponse
					decode(&r)
					want, wantErr = r.IsSingleSelect, r.Error
					got, gotErr = p.IsSingleSelectQuery(q, d)
				case "hoist-declares-list":
					typ, ok := hoistType[d]
					if !ok {
						continue
					}
					var r queriesResponse
					decode(&r)
					want, wantErr = r.Queries, r.Error
					got, gotErr = p.HoistDeclaresList(toStrings(c["queries"]), typ)
				default:
					var r queryResponse
					decode(&r)
					want, wantErr = r.Query, r.Error
					switch pc.Command {
					case "replace-table-references":
						var mapping map[string]string
						if err := json.Unmarshal(mustJSON(c["table_mapping"]), &mapping); err != nil {
							t.Fatal(err)
						}
						got, gotErr = p.RenameTables(q, d, mapping)
					case "add-limit":
						got, gotErr = p.AddLimit(q, toInt(c["limit"]), d)
					case "add-ctes":
						var ctes []CTE
						if err := json.Unmarshal(mustJSON(c["ctes"]), &ctes); err != nil {
							t.Fatal(err)
						}
						got, gotErr = p.PrependCTEs(q, d, ctes)
					case "extract-select":
						got, gotErr = p.ExtractSelect(q, d)
					case "select-cte":
						got, gotErr = p.SelectFromCTE(q, d, str(c["cte_name"]))
					case "freeze-time":
						got, gotErr = p.FreezeTime(q, d, str(c["execution_time"]))
					case "hoist-declares":
						typ, ok := hoistType[d]
						if !ok {
							continue
						}
						got, gotErr = p.HoistDeclares(q, typ)
					default:
						t.Fatalf("no public method for command %q", pc.Command)
					}
				}
				checked++
				if (gotErr != nil) != (wantErr != "") {
					t.Fatalf("%s: error %v, command error %q", recs[i].Cmd, gotErr, wantErr)
				}
				if gotErr == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("%s\n  command: %s\n   method: %s", recs[i].Cmd, mustJSON(want), mustJSON(got))
				}
			}
			t.Logf("%s: %d commands match their public methods", name, checked)
		})
	}
}
