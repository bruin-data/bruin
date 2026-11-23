package sqlengine

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"sync"
	"testing"
)

// Benchmarks over the golden corpus (testdata/golden/statements.jsonl, 21 dialects) and the
// TPC-H / TPC-DS queries. Each op is one pass over the whole input set.

type benchStmt struct {
	d   *Dialect
	sql string
}

// benchCorpus holds the golden statements that parse and regenerate without error.
var benchCorpus = sync.OnceValues(func() ([]benchStmt, error) {
	stmts, err := goldenStatements()
	if err != nil {
		return nil, err
	}
	var out []benchStmt
	for _, s := range stmts {
		d, err := GetDialect(s.Dialect)
		if err != nil {
			return nil, err
		}
		trees, err := safeParse(d, s.SQL)
		if err != nil {
			continue
		}
		ok := true
		for _, tr := range trees {
			if tr != nil {
				if _, err := safeGenerate(d, tr); err != nil {
					ok = false
				}
			}
		}
		if ok {
			out = append(out, benchStmt{d, s.SQL})
		}
	}
	return out, nil
})

type tpcSet struct {
	Schema  map[string]map[string]string `json:"schema"`
	Queries []string                     `json:"queries"`
}

func loadTPC(b *testing.B) map[string]tpcSet {
	b.Helper()
	f, err := os.Open("testdata/tpc.json.gz")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		b.Fatal(err)
	}
	var sets map[string]tpcSet
	if err := json.NewDecoder(gz).Decode(&sets); err != nil {
		b.Fatal(err)
	}
	return sets
}

func corpus(b *testing.B) []benchStmt {
	c, err := benchCorpus()
	if err != nil {
		b.Fatal(err)
	}
	return c
}

func BenchmarkTokenize(b *testing.B) {
	stmts := corpus(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, s := range stmts {
			_, _ = s.d.Tokenize(s.sql)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(stmts)), "ns/stmt")
}

func BenchmarkParse(b *testing.B) {
	stmts := corpus(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, s := range stmts {
			_, _ = s.d.Parse(s.sql, nil)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(stmts)), "ns/stmt")
}

func BenchmarkGenerate(b *testing.B) {
	stmts := corpus(b)
	type parsed struct {
		d     *Dialect
		trees []*Expr
	}
	var ps []parsed
	for _, s := range stmts {
		trees, err := s.d.Parse(s.sql, nil)
		if err == nil {
			ps = append(ps, parsed{s.d, trees})
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, p := range ps {
			for _, t := range p.trees {
				if t != nil {
					_, _ = p.d.Generate(t, nil)
				}
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(ps)), "ns/stmt")
}

func benchSchemaMap(s map[string]map[string]string) *SchemaMap {
	m := NewSchemaMap()
	for t, cols := range s {
		cm := NewSchemaMap()
		for c, typ := range cols {
			cm.Set(c, typ)
		}
		m.Set(t, cm)
	}
	return m
}

func benchOptimize(b *testing.B, rules []OptimizerRule) {
	sets := loadTPC(b)
	d, _ := GetDialect("")
	type q struct {
		e      *Expr
		schema *SchemaMap
	}
	var qs []q
	for _, name := range []string{"tpch", "tpcds"} {
		set := sets[name]
		schema := benchSchemaMap(set.Schema)
		for _, sql := range set.Queries {
			e, err := d.ParseOne(sql, nil)
			if err != nil {
				b.Fatal(err)
			}
			qs = append(qs, q{e, schema})
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, x := range qs {
			_, _ = OptimizeSafe(x.e, x.schema, d, rules)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(qs)), "ns/query")
}

// BenchmarkOptimizeLineageRules uses the rule set Bruin's lineage runs first.
func BenchmarkOptimizeLineageRules(b *testing.B) {
	benchOptimize(b, []OptimizerRule{RuleQualify, RuleUnnestSubqueries, RuleMergeSubqueries, RuleAnnotateTypes})
}

// BenchmarkOptimizeDefaultRules uses the full default optimizer rule list (Bruin's fallback path).
func BenchmarkOptimizeDefaultRules(b *testing.B) {
	benchOptimize(b, nil)
}

func BenchmarkCopy(b *testing.B) {
	stmts := corpus(b)
	var trees []*Expr
	for _, s := range stmts {
		ts, err := s.d.Parse(s.sql, nil)
		if err == nil {
			for _, t := range ts {
				if t != nil {
					trees = append(trees, t)
				}
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, t := range trees {
			_ = t.Copy()
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(trees)), "ns/tree")
}
