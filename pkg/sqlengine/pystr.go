package sqlengine

import (
	"strconv"
	"strings"
	"unicode"
)

func itoa(i int) string { return strconv.Itoa(i) }

// pyUpperEq reports whether pyUpper(s) == upper, without allocating for ASCII s.
func pyUpperEq(s, upper string) bool {
	if len(s) != len(upper) {
		// An ASCII s uppercases to the same length; only non-ASCII input can change length.
		for i := range len(s) {
			if s[i] >= 0x80 {
				return pyUpper(s) == upper
			}
		}
		return false
	}
	for i := range len(s) {
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
	for i := range len(s) {
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

// pyUpper uppercases s with full Unicode case mapping (ß becomes SS), unlike strings.ToUpper.
func pyUpper(s string) string {
	ascii := true
	for i := range len(s) {
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

// pyLower lowercases s with full Unicode case mapping (İ becomes i plus a combining dot), unlike
// strings.ToLower.
func pyLower(s string) string {
	ascii := true
	for i := range len(s) {
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

// pyIsSpaceRune reports whether r is whitespace: ASCII whitespace, the information separators
// U+001C..U+001F, NEL, and the Unicode space, line and paragraph separators (including NBSP).
func pyIsSpaceRune(r rune) bool {
	switch r {
	case '\t', '\n', '\x0b', '\x0c', '\r', '\x1c', '\x1d', '\x1e', '\x1f', ' ', '\x85', '\xa0',
		' ', ' ', ' ', ' ', ' ', '　':
		return true
	}
	return r >= ' ' && r <= ' '
}

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

// pyIsIdentifierRune reports whether r can start an identifier: an underscore, a letter, a letter
// number (Nl) or an Other_ID_Start character.
func pyIsIdentifierRune(r rune) bool {
	if r < 0x80 {
		return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
	}
	return unicode.IsLetter(r) || unicode.Is(unicode.Nl, r) || unicode.Is(unicode.Other_ID_Start, r)
}

// pyIsDigit reports whether s is non-empty and every rune is a decimal digit or another
// numeric digit (category No, e.g. superscripts).
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

// pyRepr quotes s for error messages: single quotes, or double quotes when s contains a single
// quote but no double quote; backslashes, the active quote and control characters are escaped.
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
