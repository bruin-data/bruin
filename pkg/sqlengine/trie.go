package sqlengine

import "unicode/utf8"

// trie mirrors sqlglot.trie: nested maps keyed by a single "character" (rune or word),
// with end marking the presence of a complete key.
type trie struct {
	children map[string]*trie
	order    []string // insertion order of children (Python dict order)
	end      bool
	// runeKids indexes the single-rune children by rune (fast path for keyword scanning).
	runeKids map[rune]*trie
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
				if cur.runeKids == nil {
					cur.runeKids = map[rune]*trie{}
				}
				cur.runeKids[r] = next
			}
		}
		cur = next
	}
	cur.end = true
}

// getRune returns the child for the single-character key r.
func (t *trie) getRune(r rune) *trie {
	if t == nil || t.runeKids == nil {
		return nil
	}
	return t.runeKids[r]
}

func (t *trie) get(c string) *trie {
	if t == nil {
		return nil
	}
	return t.children[c]
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
