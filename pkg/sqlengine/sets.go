package sqlengine

import "sort"

// TokenSet is an immutable-by-convention bitset of token types.
type TokenSet struct {
	bits [(numTokenTypes + 63) / 64]uint64
}

func newTokenSet(types ...TokenType) TokenSet {
	var s TokenSet
	for _, t := range types {
		s.bits[t/64] |= 1 << (t % 64)
	}
	return s
}

// Has reports whether t is in the set.
func (s *TokenSet) Has(t TokenType) bool { return s.bits[t/64]&(1<<(t%64)) != 0 }

// With returns a copy of s with types added.
func (s TokenSet) With(types ...TokenType) TokenSet {
	for _, t := range types {
		s.bits[t/64] |= 1 << (t % 64)
	}
	return s
}

// Without returns a copy of s with types removed.
func (s TokenSet) Without(types ...TokenType) TokenSet {
	for _, t := range types {
		s.bits[t/64] &^= 1 << (t % 64)
	}
	return s
}

// Union returns s | o.
func (s TokenSet) Union(o TokenSet) TokenSet {
	for i := range s.bits {
		s.bits[i] |= o.bits[i]
	}
	return s
}

// Minus returns s - o.
func (s TokenSet) Minus(o TokenSet) TokenSet {
	for i := range s.bits {
		s.bits[i] &^= o.bits[i]
	}
	return s
}

// Items returns the members in ascending order.
func (s TokenSet) Items() []TokenType {
	var out []TokenType
	for t := TokenType(0); t < numTokenTypes; t++ {
		if s.Has(t) {
			out = append(out, t)
		}
	}
	return out
}

// DTypeSet is a bitset of data types.
type DTypeSet struct{ bits [(numDTypes + 63) / 64]uint64 }

func newDTypeSet(types ...DType) DTypeSet {
	var s DTypeSet
	for _, t := range types {
		s.bits[t/64] |= 1 << (t % 64)
	}
	return s
}

func (s DTypeSet) Has(t DType) bool { return s.bits[t/64]&(1<<(t%64)) != 0 }

func (s DTypeSet) With(types ...DType) DTypeSet {
	for _, t := range types {
		s.bits[t/64] |= 1 << (t % 64)
	}
	return s
}

func (s DTypeSet) Union(o DTypeSet) DTypeSet {
	for i := range s.bits {
		s.bits[i] |= o.bits[i]
	}
	return s
}

func (s DTypeSet) Items() []DType {
	var out []DType
	for t := DType(0); t < numDTypes; t++ {
		if s.Has(t) {
			out = append(out, t)
		}
	}
	return out
}

// KindSet is a bitset of expression kinds.
type KindSet struct{ bits [(numKinds + 63) / 64]uint64 }

func newKindSet(kinds ...Kind) KindSet {
	var s KindSet
	for _, k := range kinds {
		s.bits[k/64] |= 1 << (k % 64)
	}
	return s
}

func (s *KindSet) Has(k Kind) bool { return s.bits[k/64]&(1<<(k%64)) != 0 }

func (s KindSet) With(kinds ...Kind) KindSet {
	for _, k := range kinds {
		s.bits[k/64] |= 1 << (k % 64)
	}
	return s
}

func (s KindSet) Without(kinds ...Kind) KindSet {
	for _, k := range kinds {
		s.bits[k/64] &^= 1 << (k % 64)
	}
	return s
}

// StrSet is a set of strings.
type StrSet map[string]struct{}

func newStrSet(items ...string) StrSet {
	s := make(StrSet, len(items))
	for _, it := range items {
		s[it] = struct{}{}
	}
	return s
}

func (s StrSet) Has(x string) bool { _, ok := s[x]; return ok }

func (s StrSet) Clone() StrSet {
	out := make(StrSet, len(s))
	for k := range s {
		out[k] = struct{}{}
	}
	return out
}

func (s StrSet) Add(items ...string) StrSet {
	for _, it := range items {
		s[it] = struct{}{}
	}
	return s
}

func (s StrSet) Sorted() []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// formatString is an entry of a tokenizer's _FORMAT_STRINGS table.
type formatString struct {
	end       string
	tokenType TokenType
}

// Tri is a tri-state boolean (True / False / None in Python).
type Tri int8

const (
	TriNone Tri = iota
	TriFalse
	TriTrue
)

// OptionsType mirrors sqlglot.parser.OPTIONS_TYPE: keyword -> sequences of follow-up words.
type OptionsType map[string][][]string
