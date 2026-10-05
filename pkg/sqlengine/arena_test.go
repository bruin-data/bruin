package sqlengine

import (
	"sync"
	"testing"
)

// TestParseConcurrentArenas parses the corpus from several goroutines at once (sharing pooled
// token arenas) and checks every statement round-trips like it does sequentially.
func TestParseConcurrentArenas(t *testing.T) {
	t.Parallel()
	corpus, err := benchCorpus()
	if err != nil {
		t.Fatal(err)
	}
	if len(corpus) > 4000 {
		corpus = corpus[:4000]
	}
	gen := func(c benchStmt) string {
		e, err := c.d.ParseOne(c.sql, nil)
		if err != nil {
			return "error: " + err.Error()
		}
		out, err := c.d.Generate(e, nil)
		if err != nil {
			return "error: " + err.Error()
		}
		return out
	}
	want := make([]string, len(corpus))
	for i, c := range corpus {
		want[i] = gen(c)
	}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < len(corpus); i += 8 {
				if got := gen(corpus[i]); got != want[i] {
					t.Errorf("%q: got %q, want %q", corpus[i].sql, got, want[i])
					return
				}
			}
		}()
	}
	wg.Wait()
}
