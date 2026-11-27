package sqlparser

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
)

// Benchmarks of Bruin's parser commands through the public API, over the statements of the golden
// corpus (testdata/golden/commands.json.gz) and TPC-H / TPC-DS lineage with schemas
// (testdata/golden/fixtures.json.gz).

type benchQuery struct{ dialect, sql string }

func benchQueries(b *testing.B) []benchQuery {
	b.Helper()
	var out []benchQuery
	for _, q := range mustGoldenCorpus(b) {
		if q.parses {
			out = append(out, benchQuery{q.dialect, q.sql})
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

// tpcDialects are the dialects BenchmarkLineageTPC runs in.
var tpcDialects = []string{"bigquery", dialectSnowflake, "postgres"}

// BenchmarkLineageTPC runs column lineage with full schemas on TPC-H and TPC-DS.
func BenchmarkLineageTPC(b *testing.B) {
	type q struct {
		sql, dialect string
		schema       Schema
	}
	var qs []q
	for _, r := range readGolden(b, filepath.Join("testdata", "golden", "fixtures.json.gz")) {
		var pc parserCommand
		if err := json.Unmarshal(r.Cmd, &pc); err != nil {
			b.Fatal(err)
		}
		d, _ := pc.Contents["dialect"].(string)
		tables, _ := pc.Contents["schema"].(map[string]any)
		_, tpch := tables["lineitem"]
		_, tpcds := tables["store_sales"]
		if pc.Command != "lineage" || (!tpch && !tpcds) || !slices.Contains(tpcDialects, d) {
			continue
		}
		schema := Schema{}
		for table, cols := range tables {
			columns := map[string]string{}
			for name, typ := range cols.(map[string]any) {
				columns[name] = typ.(string)
			}
			schema[table] = columns
		}
		query, _ := pc.Contents["query"].(string)
		qs = append(qs, q{query, d, schema})
	}
	if len(qs) == 0 {
		b.Fatal("no TPC lineage commands in fixtures.json.gz")
	}
	p := newBenchParser(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, x := range qs {
			_, _ = p.ColumnLineage(x.sql, x.dialect, x.schema)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(qs)), "ns/query")
}
