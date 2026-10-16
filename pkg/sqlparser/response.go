package sqlparser

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// The command handlers return JSON-shaped results (map[string]any). run decodes them into these
// typed responses.

type tablesResponse struct {
	Tables []string
	Error  string
}

type queryResponse struct {
	Query string
	Error string
}

type queriesResponse struct {
	Queries []string
	Error   string
}

type singleSelectResponse struct {
	IsSingleSelect bool
	Error          string
}

type readOnlyResponse struct {
	ReadOnly bool
	Error    string
}

// run runs a command handler, turning its error or panic into the response's error field, and
// decodes the result into out (*Lineage or one of the response types above).
func run(out any, cmd func() (any, error)) error {
	result := runGuarded(cmd)
	m, ok := result.(map[string]any)
	if !ok {
		return fmt.Errorf("unexpected parser response %T", result)
	}
	switch o := out.(type) {
	case *Lineage:
		*o = Lineage{Columns: resultColumns(m, "columns"), NonSelectedColumns: resultColumns(m, "non_selected_columns"), Errors: resultStrings(m, "errors")}
	case *tablesResponse:
		*o = tablesResponse{Tables: resultStrings(m, "tables"), Error: resultString(m, "error")}
	case *queryResponse:
		*o = queryResponse{Query: resultString(m, "query"), Error: resultString(m, "error")}
	case *queriesResponse:
		*o = queriesResponse{Queries: resultStrings(m, "queries"), Error: resultString(m, "error")}
	case *singleSelectResponse:
		b, _ := m["is_single_select"].(bool)
		*o = singleSelectResponse{IsSingleSelect: b, Error: resultString(m, "error")}
	case *readOnlyResponse:
		b, _ := m["is_read_only"].(bool)
		*o = readOnlyResponse{ReadOnly: b, Error: resultString(m, "error")}
	default:
		return fmt.Errorf("unexpected parser response type %T", out)
	}
	return nil
}

func resultString(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return validUTF8(s)
}

// resultStrings returns nil for a missing list and a non-nil slice otherwise.
func resultStrings(m map[string]any, key string) []string {
	switch v := m[key].(type) {
	case []string:
		if v == nil {
			return nil
		}
		out := make([]string, len(v))
		for i, s := range v {
			out[i] = validUTF8(s)
		}
		return out
	case []any:
		if v == nil {
			return nil
		}
		out := make([]string, len(v))
		for i, e := range v {
			s, _ := e.(string)
			out[i] = validUTF8(s)
		}
		return out
	}
	return nil
}

// resultColumns returns nil for a missing list and a non-nil slice otherwise.
func resultColumns(m map[string]any, key string) []ColumnLineage {
	switch v := m[key].(type) {
	case []any:
		if v == nil {
			return nil
		}
		return []ColumnLineage{}
	case []map[string]any:
		if v == nil {
			return nil
		}
		out := make([]ColumnLineage, len(v))
		for i, c := range v {
			out[i] = ColumnLineage{Name: resultString(c, "name"), Upstream: resultUpstream(c["upstream"]), Type: resultString(c, "type")}
		}
		return out
	}
	return nil
}

func resultUpstream(v any) []UpstreamColumn {
	x, _ := v.([]UpstreamColumn)
	if x == nil {
		return nil
	}
	out := make([]UpstreamColumn, len(x))
	for i, u := range x {
		out[i] = UpstreamColumn{Column: validUTF8(u.Column), Table: validUTF8(u.Table)}
	}
	return out
}

// validUTF8 replaces each byte of s that is not part of a valid UTF-8 sequence with U+FFFD.
func validUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b.WriteRune(utf8.RuneError)
		} else {
			b.WriteString(s[i : i+size])
		}
		i += size
	}
	return b.String()
}

func validUTF8Slice(ss []string) []string {
	if ss == nil {
		return nil
	}
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = validUTF8(s)
	}
	return out
}

func validUTF8Strings(m map[string]string) bool {
	for k, v := range m {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return false
		}
	}
	return true
}

func validUTF8Map(m map[string]string) map[string]string {
	if validUTF8Strings(m) {
		return m
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[validUTF8(k)] = validUTF8(v)
	}
	return out
}

// validUTF8Schema returns s with invalid UTF-8 replaced, and an empty schema for nil.
func validUTF8Schema(s Schema) Schema {
	if s == nil {
		return Schema{}
	}
	valid := true
	for t, cols := range s {
		if !utf8.ValidString(t) || !validUTF8Strings(cols) {
			valid = false
			break
		}
	}
	if valid {
		return s
	}
	out := make(Schema, len(s))
	for t, cols := range s {
		out[validUTF8(t)] = validUTF8Map(cols)
	}
	return out
}
