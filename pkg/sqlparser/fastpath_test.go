package sqlparser

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFastPathMatchesJSONRoundTrip checks that the in-process fast path is indistinguishable from
// the JSON round trip it replaces, over the engine's conformance corpus.
func TestFastPathMatchesJSONRoundTrip(t *testing.T) {
	t.Parallel()
	f, err := os.Open("../sqlengine/testdata/parse.json.gz")
	if err != nil {
		t.Skip("missing corpus")
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	var cases []struct {
		Dialect string `json:"dialect"`
		SQL     string `json:"sql"`
	}
	require.NoError(t, json.NewDecoder(gz).Decode(&cases))

	type command struct {
		name     string
		contents map[string]any
		out      func() any
	}
	fast, total := 0, 0
	for i, c := range cases {
		if i%3 != 0 {
			continue
		}
		q := c.SQL
		d := c.Dialect
		commands := []command{
			{"lineage", map[string]any{"query": q, "dialect": d, "schema": Schema{}}, func() any { return &Lineage{} }},
			{"lineage", map[string]any{"query": q, "dialect": d, "schema": Schema{"t": {"a": "int", "b": "varchar"}, "x": nil}}, func() any { return &Lineage{} }},
			{"get-tables", map[string]any{"query": q, "dialect": d}, func() any { return &tablesResponse{} }},
			{"is-single-select", map[string]any{"query": q, "dialect": d}, func() any { return &singleSelectResponse{} }},
			{"is-read-only", map[string]any{"query": q, "dialect": d}, func() any { return &readOnlyResponse{} }},
			{"add-limit", map[string]any{"query": q, "dialect": d, "limit": 10}, func() any { return &queryResponse{} }},
			{"extract-select", map[string]any{"query": q, "dialect": d}, func() any { return &queryResponse{} }},
			{"replace-table-references", map[string]any{"query": q, "dialect": d, "table_mapping": map[string]string{"t": "u"}}, func() any { return &queryResponse{} }},
			{"replace-table-references", map[string]any{"query": q, "dialect": d, "table_mapping": map[string]string(nil)}, func() any { return &queryResponse{} }},
			{"add-ctes", map[string]any{"query": q, "dialect": d, "ctes": []CTE{{Name: "c", Query: "SELECT 1"}}}, func() any { return &queryResponse{} }},
			{"hoist-declares-list", map[string]any{"queries": []string{q, "DECLARE x INT64"}, "dialect": d}, func() any { return &queriesResponse{} }},
		}
		for _, cmd := range commands {
			// Request: normalization equals the JSON round trip.
			norm, ok := normalizeRequest(cmd.contents)
			require.True(t, ok)
			b, err := json.Marshal(cmd.contents)
			require.NoError(t, err)
			var viaJSON map[string]any
			require.NoError(t, json.Unmarshal(b, &viaJSON))
			require.True(t, reflect.DeepEqual(expandSchemas(norm), viaJSON), "request %s %q", cmd.name, q)
			if s, ok := norm["schema"]; ok {
				// The lineage command reads a passed-through Schema like its decoded form.
				fromNorm, err1 := toRawSchema(s)
				fromJSON, err2 := toRawSchema(viaJSON["schema"])
				require.Equal(t, err2, err1)
				require.Equal(t, fromJSON, fromNorm)
			}

			// Response: typed decoding equals json.Unmarshal of the encoded response.
			result := runCommand(cmd.name, norm)
			got, want := cmd.out(), cmd.out()
			total++
			if decodeResponse(result, got) {
				fast++
				require.NoError(t, json.Unmarshal(encodeResponse(result), want))
				require.Equal(t, want, got, "response %s %s %q", cmd.name, d, q)
			}
		}
	}
	// Every result shape the commands produce must be handled by the fast path.
	require.Equal(t, total, fast, "responses that fell back to JSON")
}

// expandSchemas converts Schema values to the map[string]any form encoding/json decodes them into.
func expandSchemas(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if s, ok := v.(Schema); ok {
			tables := make(map[string]any, len(s))
			for t, cols := range s {
				if cols == nil {
					tables[t] = nil
					continue
				}
				c := make(map[string]any, len(cols))
				for name, typ := range cols {
					c[name] = typ
				}
				tables[t] = c
			}
			v = tables
		}
		out[k] = v
	}
	return out
}
