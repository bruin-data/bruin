package sqlengine

import (
	"sort"
	"unicode/utf8"
)

// trie mirrors sqlglot.trie: nested maps keyed by a single "character" (rune or word),
// with end marking the presence of a complete key.
type trie struct {
	children map[string]*trie
	order    []string // insertion order of children (Python dict order)
	end      bool
	// kidRunes/kids index the single-rune children by rune, sorted (fast path for keyword
	// scanning; most nodes have a handful of children).
	kidRunes []rune
	kids     []*trie
}

type trieResult int

const (
	trieFailed trieResult = iota + 1
	triePrefix
	trieExists
)

func newTrie() *trie { return &trie{children: map[string]*trie{}} }

// newTrieFromStrings builds a trie keyed by runes.
func newTrieFromStrings(keys ...string) *trie {
	t := newTrie()
	for _, k := range keys {
		t.add(splitRunes(k))
	}
	return t
}

// newTrieFromWords builds a trie keyed by words (e.g. "GLOBAL STATUS" -> ["GLOBAL","STATUS"]).
func newTrieFromWords(keys ...[]string) *trie {
	t := newTrie()
	for _, k := range keys {
		t.add(k)
	}
	return t
}

func splitRunes(s string) []string {
	out := make([]string, 0, len(s))
	for _, r := range s {
		out = append(out, string(r))
	}
	return out
}

func (t *trie) add(key []string) {
	cur := t
	for _, c := range key {
		next, ok := cur.children[c]
		if !ok {
			next = newTrie()
			cur.children[c] = next
			cur.order = append(cur.order, c)
			if r, size := utf8.DecodeRuneInString(c); size == len(c) && size > 0 {
				i := sort.Search(len(cur.kidRunes), func(i int) bool { return cur.kidRunes[i] >= r })
				cur.kidRunes = append(cur.kidRunes, 0)
				copy(cur.kidRunes[i+1:], cur.kidRunes[i:])
				cur.kidRunes[i] = r
				cur.kids = append(cur.kids, nil)
				copy(cur.kids[i+1:], cur.kids[i:])
				cur.kids[i] = next
			}
		}
		cur = next
	}
	cur.end = true
}

// getRune returns the child for the single-character key r.
func (t *trie) getRune(r rune) *trie {
	if t == nil {
		return nil
	}
	rs := t.kidRunes
	if len(rs) <= 8 {
		for i, x := range rs {
			if x == r {
				return t.kids[i]
			}
		}
		return nil
	}
	lo, hi := 0, len(rs)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if rs[m] < r {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(rs) && rs[lo] == r {
		return t.kids[lo]
	}
	return nil
}

// inTrie mirrors sqlglot.trie.in_trie.
func inTrie(t *trie, key []string) (trieResult, *trie) {
	if len(key) == 0 {
		return trieFailed, t
	}
	cur := t
	for _, c := range key {
		next := cur.children[c]
		if next == nil {
			return trieFailed, cur
		}
		cur = next
	}
	if cur.end {
		return trieExists, cur
	}
	return triePrefix, cur
}
