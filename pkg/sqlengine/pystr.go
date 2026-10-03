package sqlengine

import (
	"strconv"
	"strings"
	"unicode"
)

// Helpers that reproduce Python str semantics used by sqlglot.

func itoa(i int) string { return strconv.Itoa(i) }

// pyUpper mirrors str.upper().
// pyUpperEq reports whether pyUpper(s) == upper, without allocating for ASCII s.
func pyUpperEq(s, upper string) bool {
	if len(s) != len(upper) {
		// An ASCII s uppercases to the same length; only non-ASCII input can change length.
		for i := 0; i < len(s); i++ {
			if s[i] >= 0x80 {
				return pyUpper(s) == upper
			}
		}
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x80 {
			return pyUpper(s) == upper
		}
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != upper[i] {
			// The rest may still hold non-ASCII text whose uppercase differs in length, but the
			// prefix already differs and uppercasing never changes ASCII bytes.
			return false
		}
	}
	return true
}

// upperASCII writes pyUpper(s) into buf when s is ASCII and fits, for allocation-free map lookups
// (m[string(b)] does not allocate).
func upperASCII(buf []byte, s string) ([]byte, bool) {
	if len(s) > len(buf) {
		return nil, false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x80 {
			return nil, false
		}
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i] = c
	}
	return buf[:len(s)], true
}

func pyUpper(s string) string {
	ascii := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return strings.ToUpper(s)
	}
	var b strings.Builder
	for _, r := range s {
		if u, ok := pyUpperMap[r]; ok {
			b.WriteString(u)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// pyLower mirrors str.lower().
func pyLower(s string) string {
	ascii := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return strings.ToLower(s)
	}
	var b strings.Builder
	for _, r := range s {
		if l, ok := pyLowerMap[r]; ok {
			b.WriteString(l)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// pyIsSpaceRune mirrors str.isspace() for a single character.
func pyIsSpaceRune(r rune) bool {
	switch r {
	case '\t', '\n', '\x0b', '\x0c', '\r', '\x1c', '\x1d', '\x1e', '\x1f', ' ', '\x85', '\xa0',
		' ', ' ', ' ', ' ', ' ', '　':
		return true
	}
	return r >= ' ' && r <= ' '
}

// pyStrip mirrors str.strip() with no arguments.
func pyStrip(s string) string {
	return strings.TrimFunc(s, pyIsSpaceRune)
}

func pyIsAlnumRune(r rune) bool {
	if r < 0x80 {
		return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
	}
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

func pyIsAlphaRune(r rune) bool {
	if r < 0x80 {
		return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
	}
	return unicode.IsLetter(r)
}

// pyIsIdentifierRune mirrors str.isidentifier() for a single character.
func pyIsIdentifierRune(r rune) bool {
	if r < 0x80 {
		return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
	}
	return unicode.IsLetter(r) || unicode.Is(unicode.Nl, r) || unicode.Is(unicode.Other_ID_Start, r)
}

// pyIsDigit mirrors str.isdigit() (false for the empty string).
func pyIsDigit(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) && !unicode.Is(unicode.No, r) {
			return false
		}
	}
	return true
}

// pyRepr mirrors repr() of a Python str.
func pyRepr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString("\\n")
		case r == '\r':
			b.WriteString("\\r")
		case r == '\t':
			b.WriteString("\\t")
		case r < 0x20 || r == 0x7f:
			b.WriteString("\\x")
			b.WriteString(strconv.FormatInt(int64(r)|0x100, 16)[1:])
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}
