package objectpattern

import (
	"regexp"
	"strings"
)

// ContainsWildcard reports whether a cloud object name uses one of Bruin's
// supported wildcard forms.
func ContainsWildcard(name string) bool {
	return strings.ContainsAny(name, "*{")
}

// ExtractPrefix returns the literal prefix that can be passed to an object-list
// API before applying the full wildcard expression locally.
func ExtractPrefix(name string) string {
	minIdx := len(name)
	for _, ch := range []byte{'*', '{'} {
		if idx := strings.IndexByte(name, ch); idx >= 0 && idx < minIdx {
			minIdx = idx
		}
	}
	prefix := name[:minIdx]
	if lastSlash := strings.LastIndex(prefix, "/"); lastSlash >= 0 {
		return prefix[:lastSlash+1]
	}
	return prefix
}

// WildcardToRegex converts Bruin's object wildcard syntax to an anchored
// regular expression. An asterisk does not cross a slash, and braces represent
// comma-separated alternatives.
func WildcardToRegex(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); {
		ch := pattern[i]
		switch ch {
		case '*':
			b.WriteString("[^/]*")
			i++
		case '{':
			end := strings.IndexByte(pattern[i:], '}')
			if end < 0 {
				b.WriteString(regexp.QuoteMeta(string(ch)))
				i++
				continue
			}
			alternatives := pattern[i+1 : i+end]
			parts := strings.Split(alternatives, ",")
			b.WriteString("(")
			for j, part := range parts {
				if j > 0 {
					b.WriteString("|")
				}
				for _, alternativeChar := range strings.TrimSpace(part) {
					if alternativeChar == '*' {
						b.WriteString("[^/]*")
					} else {
						b.WriteString(regexp.QuoteMeta(string(alternativeChar)))
					}
				}
			}
			b.WriteString(")")
			i += end + 1
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
			i++
		}
	}
	b.WriteString("$")
	return b.String()
}
