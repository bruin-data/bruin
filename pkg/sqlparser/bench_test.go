package sqlparser

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"
)

// Benchmarks of Bruin's parser commands through the public API, over the engine's conformance
// corpus (sqlglot's test-suite SQL for Bruin's dialects) and TPC-H / TPC-DS lineage.

var benchDialects = map[string]bool{
	"": true, "athena": true, "bigquery": true, "clickhouse": true, "databricks": true, "doris": true,
	"duckdb": true, "fabric": true, "mysql": true, "oracle": true, "postgres": true, "redshift": true,
	"snowflake": true, "spark": true, "starrocks": true, "trino": true, "tsql": true,
}

func loadBenchGz(b *testing.B, path string, v any) {
	b.Helper()
	f, err := os.Open(path)
	if err != nil {
		b.Skipf("missing %s: %v", path, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		b.Fatal(err)
	}
	if err := json.NewDecoder(gz).Decode(v); err != nil {
		b.Fatal(err)
	}
}

type benchQuery struct{ dialect, sql string }

func benchQueries(b *testing.B) []benchQuery {
	var cases []struct {
		Dialect string `json:"dialect"`
		SQL     string `json:"sql"`
		Error   string `json:"error"`
	}
	loadBenchGz(b, "../sqlengine/testdata/parse.json.gz", &cases)
	var out []benchQuery
	for _, c := range cases {
		if c.Error == "" && benchDialects[c.Dialect] {
			out = append(out, benchQuery{c.Dialect, c.SQL})
		}
	}
	return out
}

func newBenchParser(b *testing.B) *SQLParser {
	p, err := NewSQLParser(false)
	if err != nil {
		b.Fatal(err)
	}
	if err := p.Start(); err != nil {
		b.Fatal(err)
	}
	return p
}

// BenchmarkCommandMix runs the commands Bruin issues for every SQL asset over the corpus.
func BenchmarkCommandMix(b *testing.B) {
	qs := benchQueries(b)
	p := newBenchParser(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, q := range qs {
			_, _ = p.UsedTables(q.sql, q.dialect)
			_, _ = p.ColumnLineage(q.sql, q.dialect, Schema{})
			_, _ = p.IsSingleSelectQuery(q.sql, q.dialect)
			_, _ = p.AddLimit(q.sql, 10, q.dialect)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(qs)), "ns/query")
}

// BenchmarkLineageTPC runs column lineage with full schemas on TPC-H and TPC-DS.
func BenchmarkLineageTPC(b *testing.B) {
	var sets map[string]struct {
		Schema  map[string]map[string]string `json:"schema"`
		Queries []string                     `json:"queries"`
	}
	loadBenchGz(b, "../sqlengine/testdata/tpc.json.gz", &sets)
	p := newBenchParser(b)
	type q struct {
		sql    string
		schema Schema
	}
	var qs []q
	for _, name := range []string{"tpch", "tpcds"} {
		for _, sql := range sets[name].Queries {
			qs = append(qs, q{sql, Schema(sets[name].Schema)})
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, x := range qs {
			for _, d := range []string{"bigquery", "snowflake", "postgres"} {
				_, _ = p.ColumnLineage(x.sql, d, x.schema)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(qs)*3), "ns/query")
}
