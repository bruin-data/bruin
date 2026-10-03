package sqlengine

import (
	"sync"
	"testing"
)

// Benchmarks over the conformance corpus (sqlglot's own test-suite SQL, 21 dialects) and the
// TPC-H / TPC-DS queries. Each op is one pass over the whole input set.

type benchStmt struct {
	d   *Dialect
	sql string
}

var benchCorpus = sync.OnceValues(func() ([]benchStmt, error) {
	var cases []parseCase
	loadGz(&testing.T{}, "testdata/parse.json.gz", &cases)
	var out []benchStmt
	for _, c := range cases {
		if c.Error != "" || c.GenError != "" {
			continue
		}
		d, err := GetDialect(c.Dialect)
		if err != nil {
			return nil, err
		}
		out = append(out, benchStmt{d, c.SQL})
	}
	return out, nil
})

type tpcSet struct {
	Schema  map[string]map[string]string `json:"schema"`
	Queries []string                     `json:"queries"`
}

func loadTPC(b *testing.B) map[string]tpcSet {
	var sets map[string]tpcSet
	loadGz(b, "testdata/tpc.json.gz", &sets)
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

// BenchmarkOptimizeDefaultRules uses sqlglot's full default rule list (Bruin's fallback path).
func BenchmarkOptimizeDefaultRules(b *testing.B) {
	benchOptimize(b, nil)
}
