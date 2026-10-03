package sqlengine

import (
	"fmt"
	"runtime"
)

// Small exported helpers used by embedding applications (e.g. Bruin) that port code written
// against the Python sqlglot API.

// PyUpper mirrors Python str.upper().
func PyUpper(s string) string { return pyUpper(s) }

// PyLower mirrors Python str.lower().
func PyLower(s string) string { return pyLower(s) }

// PyRepr mirrors Python repr() of a str.
func PyRepr(s string) string { return pyRepr(s) }

// ExprString mirrors str(expression): SQL in the default dialect.
func ExprString(e *Expr) string { return exprSQL(e) }

// exprSQL renders an expression with the default dialect.
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

// TokenizeScript tokenizes like the dialect's tokenizer but with COMMANDS emptied, so that
// procedural blocks stay visible (mirrors `type("ScriptTokenizer", (tokenizer_class,),
// {"COMMANDS": set()})`). Subclassing re-runs Tokenizer.__init_subclass__, which resets
// BYTE_STRING_ESCAPES to STRING_ESCAPES because the subclass does not define its own.
func (d *Dialect) TokenizeScript(sql string) ([]*Token, error) {
	cfg := *d.tok
	cfg.commands = TokenSet{}
	cfg.byteStringEscapes = cfg.stringEscapes
	if d.Name == "athena" {
		// Athena's tokenize() first tokenizes with its own (command-less) settings to route
		// to the Hive or Trino tokenizer, which keep their COMMANDS.
		tokens, err := newTokenizerCore(&cfg).tokenize(sql)
		if err != nil {
			return nil, err
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

// recoverToError converts builder/parser panics into errors.
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

// QueryLimitBuild mirrors `query.limit(n, dialect=d)` (copy=True) for an integer limit.
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

// QueryWithBuild mirrors `query.with_(name, as_=sql, dialect=d, copy=False)`.
func QueryWithBuild(q *Expr, name, sql string, d *Dialect) (out *Expr, err error) {
	defer recoverToError(&err)
	alias := MaybeParse(name, KTableAlias, "", d)
	as := MaybeParse(sql, KNone, "", d)
	return q.QueryWith(alias, as, nil, nil, true, false, nil), nil
}

// NewCTE mirrors exp.CTE(this=body, alias=exp.TableAlias(this=exp.to_identifier(name))).
func NewCTE(body *Expr, name string) *Expr {
	return New(KCTE, "this", body, "alias", New(KTableAlias, "this", ToIdentifier(name, nil)))
}

// AliasWithQuote mirrors exp.alias_(e, name, quoted=quoted).
func AliasWithQuote(e *Expr, name string, quoted bool) *Expr {
	return AliasExpr(e, name, &quoted, true)
}

// CastToType mirrors exp.cast(e, "<type>") with the default dialect.
func CastToType(e *Expr, typ string) *Expr {
	return CastExpr(e, typ, true, nil)
}

// OptimizeSafe mirrors `optimize(expression, schema=schema_dict, dialect=d, rules=rules)` with
// errors returned instead of raised. schema is the nested schema dict (ensure_schema wraps it in
// a MappingSchema for the dialect); nil rules means the default RULES.
func OptimizeSafe(e *Expr, schema *SchemaMap, d *Dialect, rules []OptimizerRule) (out *Expr, err error) {
	defer recoverToError(&err)
	if rules == nil {
		rules = DefaultOptimizerRules
	}
	ms := NewMappingSchema(schema, nil, d, true, nil)
	return Optimize(e, ms, d, rules), nil
}

// pyTypeName mirrors type(v).__name__ for raw argument values.
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

// argForAttr returns the expression stored under key for code that calls a method/attribute
// `attr` on it in Python when it is truthy: a non-expression truthy value (e.g. a raw string set
// by a caller) raises the same AttributeError Python would.
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

// argAttr returns the expression stored under key for code that unconditionally accesses an
// attribute `attr` on it in Python (`expression.this.type`): anything that is not an expression,
// including None, raises the same AttributeError.
func (e *Expr) argAttr(key, attr string) *Expr {
	v := e.Arg(key)
	if x, ok := v.(*Expr); ok && x != nil {
		return x
	}
	panic(&ValueError{Msg: fmt.Sprintf("'%s' object has no attribute '%s'", pyTypeName(v), attr)})
}

// argKeyAttr mirrors `expression.args[key].<attr>`: KeyError when the key is absent, AttributeError
// when the stored value is not an expression.
func (e *Expr) argKeyAttr(key, attr string) *Expr {
	if !e.HasArgKey(key) {
		panic(&ValueError{Msg: pyRepr(key)})
	}
	return e.argAttr(key, attr)
}
