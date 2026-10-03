package sqlengine

import (
	"fmt"
	"sort"
	"strings"
)

// ErrorLevel mirrors sqlglot.errors.ErrorLevel.
type ErrorLevel int

const (
	ErrorLevelImmediate ErrorLevel = iota
	ErrorLevelIgnore
	ErrorLevelWarn
	ErrorLevelRaise
)

// ParseErrorDetail mirrors one entry of ParseError.errors.
type ParseErrorDetail struct {
	Description    string
	Line           int
	Col            int
	StartContext   string
	Highlight      string
	EndContext     string
	IntoExpression string
}

// ParseError mirrors sqlglot.errors.ParseError.
type ParseError struct {
	Msg    string
	Errors []ParseErrorDetail
}

func (e *ParseError) Error() string { return e.Msg }

// UnsupportedError mirrors sqlglot.errors.UnsupportedError.
type UnsupportedError struct{ Msg string }

func (e *UnsupportedError) Error() string { return e.Msg }

// ValueError mirrors a Python ValueError raised by sqlglot.
type ValueError struct{ Msg string }

func (e *ValueError) Error() string { return e.Msg }

// OptimizeError mirrors sqlglot.errors.OptimizeError.
type OptimizeError struct{ Msg string }

func (e *OptimizeError) Error() string { return e.Msg }

// SchemaError mirrors sqlglot.errors.SchemaError.
type SchemaError struct{ Msg string }

func (e *SchemaError) Error() string { return e.Msg }

const (
	ansiUnderline              = "\033[4m"
	ansiReset                  = "\033[0m"
	errorMessageContextDefault = 100
)

// highlightSQL mirrors sqlglot.errors.highlight_sql. Positions are inclusive rune offsets.
func highlightSQL(sql []rune, positions [][2]int, contextLength int) (formatted, startContext, highlight, endContext string) {
	sorted := append([][2]int{}, positions...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
	var parts []string
	firstHighlightStart := 0
	previousPartEnd := 0
	n := len(sql)
	clamp := func(a, b int) string { return pySlice(sql, a, b) }
	if sorted[0][0] > 0 {
		firstHighlightStart = sorted[0][0]
		s := firstHighlightStart - contextLength
		if s < 0 {
			s = 0
		}
		startContext = clamp(s, firstHighlightStart)
		parts = append(parts, startContext)
		previousPartEnd = firstHighlightStart
	}
	for _, p := range sorted {
		hs := p[0]
		if previousPartEnd > hs {
			hs = previousPartEnd
		}
		he := p[1] + 1
		if hs >= he {
			continue
		}
		if hs > previousPartEnd {
			parts = append(parts, clamp(previousPartEnd, hs))
		}
		parts = append(parts, ansiUnderline+clamp(hs, he)+ansiReset)
		previousPartEnd = he
	}
	if previousPartEnd < n {
		endContext = clamp(previousPartEnd, previousPartEnd+contextLength)
		parts = append(parts, endContext)
	}
	formatted = strings.Join(parts, "")
	highlight = clamp(firstHighlightStart, previousPartEnd)
	return
}

func concatMessages(errs []error, maximum int) string {
	var msgs []string
	for i, e := range errs {
		if i >= maximum {
			break
		}
		msgs = append(msgs, e.Error())
	}
	if remaining := len(errs) - maximum; remaining > 0 {
		msgs = append(msgs, fmt.Sprintf("... and %d more", remaining))
	}
	return strings.Join(msgs, "\n\n")
}

// suggestClosestMatchAndFail mirrors sqlglot.helper.suggest_closest_match_and_fail.
func suggestClosestMatchAndFail(kind, word string, possibilities []string) error {
	similar := ""
	if m := getCloseMatches(word, possibilities, 1, 0.6); len(m) > 0 {
		similar = " Did you mean " + m[0] + "?"
	}
	return &ValueError{Msg: fmt.Sprintf("Unknown %s '%s'.%s", kind, word, similar)}
}

// getCloseMatches mirrors difflib.get_close_matches.
func getCloseMatches(word string, possibilities []string, n int, cutoff float64) []string {
	type scored struct {
		score float64
		s     string
	}
	var result []scored
	b := []rune(word)
	for _, x := range possibilities {
		a := []rune(x)
		sm := newSequenceMatcher(a, b)
		if sm.realQuickRatio() >= cutoff && sm.quickRatio() >= cutoff {
			if r := sm.ratio(); r >= cutoff {
				result = append(result, scored{r, x})
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].score != result[j].score {
			return result[i].score > result[j].score
		}
		return result[i].s > result[j].s
	})
	var out []string
	for i := 0; i < len(result) && i < n; i++ {
		out = append(out, result[i].s)
	}
	return out
}

// sequenceMatcher is a port of difflib.SequenceMatcher (autojunk enabled, no isjunk).
type sequenceMatcher struct {
	a, b     []rune
	b2j      map[rune][]int
	fullbcnt map[rune]int
}

func newSequenceMatcher(a, b []rune) *sequenceMatcher {
	sm := &sequenceMatcher{a: a, b: b}
	sm.chainB()
	return sm
}

func (s *sequenceMatcher) chainB() {
	s.b2j = map[rune][]int{}
	for i, elt := range s.b {
		s.b2j[elt] = append(s.b2j[elt], i)
	}
	n := len(s.b)
	if n >= 200 {
		ntest := n/100 + 1
		for elt, idxs := range s.b2j {
			if len(idxs) > ntest {
				delete(s.b2j, elt)
			}
		}
	}
}

func (s *sequenceMatcher) findLongestMatch(alo, ahi, blo, bhi int) (int, int, int) {
	besti, bestj, bestsize := alo, blo, 0
	j2len := map[int]int{}
	for i := alo; i < ahi; i++ {
		newj2len := map[int]int{}
		for _, j := range s.b2j[s.a[i]] {
			if j < blo {
				continue
			}
			if j >= bhi {
				break
			}
			k := j2len[j-1] + 1
			newj2len[j] = k
			if k > bestsize {
				besti, bestj, bestsize = i-k+1, j-k+1, k
			}
		}
		j2len = newj2len
	}
	// No junk elements (isjunk is None), so only the non-junk extension loops apply.
	for besti > alo && bestj > blo && s.a[besti-1] == s.b[bestj-1] {
		besti, bestj, bestsize = besti-1, bestj-1, bestsize+1
	}
	for besti+bestsize < ahi && bestj+bestsize < bhi && s.a[besti+bestsize] == s.b[bestj+bestsize] {
		bestsize++
	}
	return besti, bestj, bestsize
}

func (s *sequenceMatcher) matchingBlocksSize() int {
	type q struct{ alo, ahi, blo, bhi int }
	queue := []q{{0, len(s.a), 0, len(s.b)}}
	total := 0
	for len(queue) > 0 {
		x := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		i, j, k := s.findLongestMatch(x.alo, x.ahi, x.blo, x.bhi)
		if k > 0 {
			total += k
			if x.alo < i && x.blo < j {
				queue = append(queue, q{x.alo, i, x.blo, j})
			}
			if i+k < x.ahi && j+k < x.bhi {
				queue = append(queue, q{i + k, x.ahi, j + k, x.bhi})
			}
		}
	}
	return total
}

func calcRatio(matches, length int) float64 {
	if length > 0 {
		return 2.0 * float64(matches) / float64(length)
	}
	return 1.0
}

func (s *sequenceMatcher) ratio() float64 {
	return calcRatio(s.matchingBlocksSize(), len(s.a)+len(s.b))
}

func (s *sequenceMatcher) quickRatio() float64 {
	if s.fullbcnt == nil {
		s.fullbcnt = map[rune]int{}
		for _, elt := range s.b {
			s.fullbcnt[elt]++
		}
	}
	avail := map[rune]int{}
	matches := 0
	for _, elt := range s.a {
		numb, ok := avail[elt]
		if !ok {
			numb = s.fullbcnt[elt]
		}
		avail[elt] = numb - 1
		if numb > 0 {
			matches++
		}
	}
	return calcRatio(matches, len(s.a)+len(s.b))
}

func (s *sequenceMatcher) realQuickRatio() float64 {
	la, lb := len(s.a), len(s.b)
	m := la
	if lb < m {
		m = lb
	}
	return calcRatio(m, la+lb)
}
