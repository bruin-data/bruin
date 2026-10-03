package sqlparser

import (
	"encoding/json"
	"math"
	"unicode/utf8"
)

// Commands used to cross a process boundary to Python as JSON, so every request and response
// went through a JSON round trip. The in-process engine keeps the observable semantics of that
// round trip without paying for it: requests are normalized to exactly the shapes encoding/json
// decodes into (objects, arrays, float64 numbers, strings, bools, nil), and results are converted
// to the typed responses with json.Unmarshal semantics (an empty JSON array decodes to an empty,
// non-nil slice; null to nil). Values outside the handled shapes, and strings with invalid UTF-8
// (which encoding/json coerces), take the real JSON round trip instead.

// The response types are aliases of unnamed struct types, like the ones the public methods used to
// decode into, so that encoding/json error messages (which name the struct type) are unchanged.

type tablesResponse = struct {
	Tables []string `json:"tables"`
	Error  string   `json:"error"`
}

type queryResponse = struct {
	Query string `json:"query"`
	Error string `json:"error"`
}

type queriesResponse = struct {
	Queries []string `json:"queries"`
	Error   string   `json:"error"`
}

type singleSelectResponse = struct {
	IsSingleSelect bool   `json:"is_single_select"`
	Error          string `json:"error"`
}

type readOnlyResponse = struct {
	ReadOnly bool   `json:"is_read_only"`
	Error    string `json:"error"`
}

// call runs a command and decodes its response into out (one of the response types above or
// *Lineage), equivalently to `payload, sendErr := sendCommand(...); decodeErr :=
// json.Unmarshal(payload, out)`.
func (s *SQLParser) call(command string, contents map[string]any, out any) (sendErr, decodeErr error) {
	norm, ok := normalizeRequest(contents)
	if !ok {
		payload, err := s.sendCommand(&parserCommand{Command: command, Contents: contents})
		if err != nil {
			return err, nil
		}
		return nil, json.Unmarshal([]byte(payload), out)
	}
	s.mutex.Lock()
	result := runCommand(command, norm)
	s.mutex.Unlock()
	if decodeResponse(result, out) {
		return nil, nil
	}
	return nil, json.Unmarshal(encodeResponse(result), out)
}

// normalizeRequest returns what json.Unmarshal(json.Marshal(contents)) would produce.
func normalizeRequest(contents map[string]any) (map[string]any, bool) {
	if contents == nil {
		return nil, true
	}
	v, ok := normalizeValue(contents)
	if !ok {
		return nil, false
	}
	return v.(map[string]any), true
}

func normalizeValue(v any) (any, bool) {
	switch x := v.(type) {
	case nil:
		return nil, true
	case string:
		return x, utf8.ValidString(x)
	case bool:
		return x, true
	case int:
		return float64(x), true
	case float64:
		return x, !math.IsNaN(x) && !math.IsInf(x, 0)
	case map[string]any:
		if x == nil {
			return nil, true
		}
		out := make(map[string]any, len(x))
		for k, e := range x {
			ne, ok := normalizeValue(e)
			if !ok || !utf8.ValidString(k) {
				return nil, false
			}
			out[k] = ne
		}
		return out, true
	case map[string]string:
		if x == nil {
			return nil, true
		}
		out := make(map[string]any, len(x))
		for k, e := range x {
			if !utf8.ValidString(k) || !utf8.ValidString(e) {
				return nil, false
			}
			out[k] = e
		}
		return out, true
	case Schema:
		return checkSchema(x)
	case map[string]map[string]string:
		return checkSchema(x)
	case []string:
		if x == nil {
			return nil, true
		}
		out := make([]any, len(x))
		for i, e := range x {
			if !utf8.ValidString(e) {
				return nil, false
			}
			out[i] = e
		}
		return out, true
	case []CTE:
		if x == nil {
			return nil, true
		}
		out := make([]any, len(x))
		for i, c := range x {
			if !utf8.ValidString(c.Name) || !utf8.ValidString(c.Query) {
				return nil, false
			}
			out[i] = map[string]any{"name": c.Name, "query": c.Query}
		}
		return out, true
	case []any:
		if x == nil {
			return nil, true
		}
		out := make([]any, len(x))
		for i, e := range x {
			ne, ok := normalizeValue(e)
			if !ok {
				return nil, false
			}
			out[i] = ne
		}
		return out, true
	}
	return nil, false
}

// checkSchema passes a schema through as a Schema instead of its decoded map[string]any form:
// the lineage command reads both identically (sorted keys, nil column maps as None), so only the
// UTF-8 coercion of the JSON round trip needs checking.
func checkSchema(x map[string]map[string]string) (any, bool) {
	if x == nil {
		return nil, true
	}
	for t, cols := range x {
		if !utf8.ValidString(t) {
			return nil, false
		}
		for k, v := range cols {
			if !utf8.ValidString(k) || !utf8.ValidString(v) {
				return nil, false
			}
		}
	}
	return Schema(x), true
}

// decodeResponse stores result into out like json.Unmarshal(encodeResponse(result), out), for the
// result shapes the commands produce. It reports false (leaving out untouched) for anything else.
func decodeResponse(result any, out any) bool {
	m, ok := result.(map[string]any)
	if !ok {
		return false
	}
	switch o := out.(type) {
	case *Lineage:
		cols, ok1 := jsonColumns(m, "columns")
		nonSel, ok2 := jsonColumns(m, "non_selected_columns")
		errs, ok3 := jsonStrings(m, "errors")
		if !ok1 || !ok2 || !ok3 {
			return false
		}
		*o = Lineage{Columns: cols, NonSelectedColumns: nonSel, Errors: errs}
	case *tablesResponse:
		tables, ok1 := jsonStrings(m, "tables")
		e, ok2 := jsonString(m, "error")
		if !ok1 || !ok2 {
			return false
		}
		*o = tablesResponse{Tables: tables, Error: e}
	case *queryResponse:
		q, ok1 := jsonString(m, "query")
		e, ok2 := jsonString(m, "error")
		if !ok1 || !ok2 {
			return false
		}
		*o = queryResponse{Query: q, Error: e}
	case *queriesResponse:
		qs, ok1 := jsonStrings(m, "queries")
		e, ok2 := jsonString(m, "error")
		if !ok1 || !ok2 {
			return false
		}
		*o = queriesResponse{Queries: qs, Error: e}
	case *singleSelectResponse:
		b, ok1 := jsonBool(m, "is_single_select")
		e, ok2 := jsonString(m, "error")
		if !ok1 || !ok2 {
			return false
		}
		*o = singleSelectResponse{IsSingleSelect: b, Error: e}
	case *readOnlyResponse:
		b, ok1 := jsonBool(m, "is_read_only")
		e, ok2 := jsonString(m, "error")
		if !ok1 || !ok2 {
			return false
		}
		*o = readOnlyResponse{ReadOnly: b, Error: e}
	default:
		return false
	}
	return true
}

func jsonString(m map[string]any, key string) (string, bool) {
	switch v := m[key].(type) {
	case nil:
		return "", true
	case string:
		return v, utf8.ValidString(v)
	}
	return "", false
}

func jsonBool(m map[string]any, key string) (bool, bool) {
	switch v := m[key].(type) {
	case nil:
		return false, true
	case bool:
		return v, true
	}
	return false, false
}

func jsonStrings(m map[string]any, key string) ([]string, bool) {
	switch v := m[key].(type) {
	case nil:
		return nil, true
	case []string:
		if v == nil {
			return nil, true
		}
		out := make([]string, len(v))
		for i, s := range v {
			if !utf8.ValidString(s) {
				return nil, false
			}
			out[i] = s
		}
		return out, true
	case []any:
		if v == nil {
			return nil, true
		}
		out := make([]string, len(v))
		for i, e := range v {
			s, ok := e.(string)
			if !ok || !utf8.ValidString(s) {
				return nil, false
			}
			out[i] = s
		}
		return out, true
	}
	return nil, false
}

func jsonColumns(m map[string]any, key string) ([]ColumnLineage, bool) {
	switch v := m[key].(type) {
	case nil:
		return nil, true
	case []any:
		if v == nil {
			return nil, true
		}
		if len(v) == 0 {
			return []ColumnLineage{}, true
		}
		return nil, false
	case []map[string]any:
		if v == nil {
			return nil, true
		}
		out := make([]ColumnLineage, len(v))
		for i, c := range v {
			name, ok1 := jsonString(c, "name")
			typ, ok2 := jsonString(c, "type")
			up, ok3 := jsonUpstream(c["upstream"])
			if !ok1 || !ok2 || !ok3 {
				return nil, false
			}
			out[i] = ColumnLineage{Name: name, Upstream: up, Type: typ}
		}
		return out, true
	}
	return nil, false
}

func jsonUpstream(v any) ([]UpstreamColumn, bool) {
	switch x := v.(type) {
	case nil:
		return nil, true
	case []UpstreamColumn:
		if x == nil {
			return nil, true
		}
		out := make([]UpstreamColumn, len(x))
		for i, u := range x {
			if !utf8.ValidString(u.Column) || !utf8.ValidString(u.Table) {
				return nil, false
			}
			out[i] = u
		}
		return out, true
	}
	return nil, false
}
