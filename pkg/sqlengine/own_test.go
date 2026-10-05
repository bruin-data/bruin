package sqlengine

import "testing"

// TestOwnMatchesCopy checks that handing a tree over with Own generates and transforms exactly like
// working on a copy, over the conformance corpus.
func TestOwnMatchesCopy(t *testing.T) {
	t.Parallel()
	corpus, err := benchCorpus()
	if err != nil {
		t.Fatal(err)
	}
	owned := 0
	for _, c := range corpus {
		a, err1 := c.d.ParseOne(c.sql, nil)
		b, err2 := c.d.ParseOne(c.sql, nil)
		if err1 != nil || err2 != nil || a == nil {
			continue
		}
		want, werr := c.d.Generate(a.Copy(), nil)
		ob := b.Own()
		if ob == b {
			owned++
		}
		got, gerr := c.d.GenerateOwned(ob, nil)
		if (werr == nil) != (gerr == nil) || want != got {
			t.Fatalf("%s %q: copy=%q (%v) own=%q (%v)", c.d.Name, c.sql, want, werr, got, gerr)
		}
	}
	if owned < len(corpus)/2 {
		t.Fatalf("only %d of %d trees were owned without copying", owned, len(corpus))
	}
}

func TestOwnCopiesSubtreesAndSharedNodes(t *testing.T) {
	t.Parallel()
	d := MustDialect("")
	root, err := d.ParseOne("SELECT a FROM t WHERE b = 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if root.Own() != root {
		t.Fatal("a fresh parse tree should be owned as is")
	}
	where := root.ArgE("where")
	if where.Own() == where {
		t.Fatal("a subtree must be copied")
	}
	shared := New(KColumn, "this", ToIdentifier("x", nil))
	tree := New(KTuple, "expressions", []*Expr{shared})
	tree.args[0].val = []*Expr{shared, shared}
	if tree.Own() == tree {
		t.Fatal("a tree holding a node twice must be copied")
	}
}
