package sqlengine

import (
	"fmt"
	"runtime"
)

// PyUpper uppercases s with full Unicode case mapping (ß becomes SS), unlike strings.ToUpper.
func PyUpper(s string) string { return pyUpper(s) }

// PyLower lowercases s with full Unicode case mapping, unlike strings.ToLower.
func PyLower(s string) string { return pyLower(s) }

// PyRepr quotes s for messages: single quotes (double quotes when s contains only single
// quotes), with backslashes, the quote character and control characters escaped.
func PyRepr(s string) string { return pyRepr(s) }

// ExprString renders e as SQL in the default dialect ("None" for nil).
func ExprString(e *Expr) string { return exprSQL(e) }

func exprSQL(e *Expr) string {
	if e == nil {
		return "None"
	}
	s, err := prototype("").Generate(e, nil)
	if err != nil {
		panic(err)
	}
	return s
}

// TokenizeScript tokenizes like the dialect's tokenizer but with an empty command set, so that
// procedural blocks stay visible as ordinary tokens. The script configuration also resets
// byteStringEscapes to stringEscapes, since it defines no byte-string escapes of its own.
func (d *Dialect) TokenizeScript(sql string) ([]*Token, error) {
	cfg := *d.tok
	cfg.commands = TokenSet{}
	cfg.byteStringEscapes = cfg.stringEscapes
	if d.Name == "athena" {
		// Athena first tokenizes with its own (command-less) settings to choose between the
		// Hive and Trino tokenizers, which keep their command sets. SQL those combined settings
		// reject is routed by each engine's own tokenizer.
		tokens, err := newTokenizerCore(&cfg).tokenize(sql)
		if err != nil {
			return d.Tokenize(sql)
		}
		if tokenizeAsHive(tokens) {
			hive := prototype("hive")
			ht, err := newTokenizerCore(hive.tok).tokenize(sql)
			if err != nil {
				return nil, err
			}
			return append([]*Token{{Type: TK_HIVE_TOKEN_STREAM, Text: "", Line: 1, Col: 1, Comments: []string{}}}, ht...), nil
		}
		return d.Tokenize(sql)
	}
	return newTokenizerCore(&cfg).tokenize(sql)
}

func recoverToError(err *error) {
	if r := recover(); r != nil {
		switch x := r.(type) {
		case parsePanic:
			*err = x.err
		case runtime.Error:
			*err = internalError(x)
		case error:
			*err = x
		default:
			*err = &ValueError{Msg: fmtAny(x)}
		}
	}
}

func fmtAny(x any) string {
	if s, ok := x.(string); ok {
		return s
	}
	return "panic"
}

// QueryLimitBuild returns a copy of q with LIMIT n, parsed in dialect d; q is left unchanged.
func QueryLimitBuild(q *Expr, n int, d *Dialect) (out *Expr, err error) {
	defer recoverToError(&err)
	limit := MaybeParse(itoa(n), KLimit, "LIMIT", d)
	return q.QueryLimit(limit, true), nil
}

// QueryLimitBuildOwned is QueryLimitBuild for a query the caller does not use afterwards.
func QueryLimitBuildOwned(q *Expr, n int, d *Dialect) (out *Expr, err error) {
	defer recoverToError(&err)
	limit := MaybeParse(itoa(n), KLimit, "LIMIT", d)
	return q.Own().QueryLimit(limit, false), nil
}

// QueryWithBuild appends a CTE named name with body sql (both parsed in dialect d) to q, in
// place.
func QueryWithBuild(q *Expr, name, sql string, d *Dialect) (out *Expr, err error) {
	defer recoverToError(&err)
	alias := MaybeParse(name, KTableAlias, "", d)
	as := MaybeParse(sql, KNone, "", d)
	return q.QueryWith(alias, as, nil, nil, true, false, nil), nil
}

// NewCTE builds a CTE node with the given body, aliased as name.
func NewCTE(body *Expr, name string) *Expr {
	return New(KCTE, "this", body, "alias", New(KTableAlias, "this", ToIdentifier(name, nil)))
}

// AliasWithQuote wraps a copy of e in an alias named name, quoting the alias when quoted is set.
func AliasWithQuote(e *Expr, name string, quoted bool) *Expr {
	return AliasExpr(e, name, &quoted, true)
}

// CastToType casts a copy of e to the type string typ, parsed in the default dialect.
func CastToType(e *Expr, typ string) *Expr {
	return CastExpr(e, typ, true, nil)
}

// OptimizeSafe runs Optimize and returns its errors instead of panicking. schema is the nested
// schema map, wrapped in a MappingSchema for the dialect; nil rules means DefaultOptimizerRules.
func OptimizeSafe(e *Expr, schema *SchemaMap, d *Dialect, rules []OptimizerRule) (out *Expr, err error) {
	defer recoverToError(&err)
	if rules == nil {
		rules = DefaultOptimizerRules
	}
	ms := NewMappingSchema(schema, nil, d, true, nil)
	return Optimize(e, ms, d, rules), nil
}

// pyTypeName names the type of a raw argument value the way Python error messages do.
func pyTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "NoneType"
	case string:
		return "str"
	case bool:
		return "bool"
	case int:
		return "int"
	case []*Expr, []any, []string:
		return "list"
	case *Expr:
		return v.(*Expr).kind.Name()
	}
	return fmt.Sprintf("%T", v)
}

// argForAttr returns the expression stored under key, for callers that use its attribute
// `attr` only when the value is set. nil, "" and false yield nil; any other non-expression value
// (e.g. a raw string set by a caller) panics with an "object has no attribute" ValueError.
func (e *Expr) argForAttr(key, attr string) *Expr {
	v := e.Arg(key)
	switch x := v.(type) {
	case nil:
		return nil
	case *Expr:
		return x
	case string:
		if x == "" {
			return nil
		}
	case bool:
		if !x {
			return nil
		}
	}
	panic(&ValueError{Msg: fmt.Sprintf("'%s' object has no attribute '%s'", pyTypeName(v), attr)})
}

// argAttr returns the expression stored under key, for callers that always use its attribute
// `attr` (e.g. the type of the "this" argument): anything that is not an expression, including
// nil, panics with an "object has no attribute" ValueError.
func (e *Expr) argAttr(key, attr string) *Expr {
	v := e.Arg(key)
	if x, ok := v.(*Expr); ok && x != nil {
		return x
	}
	panic(&ValueError{Msg: fmt.Sprintf("'%s' object has no attribute '%s'", pyTypeName(v), attr)})
}

// argKeyAttr is argAttr for a key that must be present: an absent key panics with a ValueError
// carrying the quoted key, and a non-expression value panics as in argAttr.
func (e *Expr) argKeyAttr(key, attr string) *Expr {
	if !e.HasArgKey(key) {
		panic(&ValueError{Msg: pyRepr(key)})
	}
	return e.argAttr(key, attr)
}
