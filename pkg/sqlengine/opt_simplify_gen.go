package sqlengine

// Port of simplify.gen / simplify.Gen (sqlglot/optimizer/simplify.py): a simple pseudo sql
// generator for quickly generating sortable and uniq strings.

import (
	"fmt"
	"strconv"
	"strings"
)

// smpGen mirrors simplify.gen(expression, comments).
//
// Sorting and deduping sql is a necessary step for optimization. Calling the actual
// generator is expensive so we have a bare minimum sql generator here.
func smpGen(expression *Expr, comments bool) string {
	g := &smpGenState{}
	return g.gen(expression, comments)
}

type smpGenState struct {
	stack []any
	sqls  strings.Builder
}

func (g *smpGenState) push(items ...any) { g.stack = append(g.stack, items...) }

func (g *smpGenState) pop() any {
	if len(g.stack) == 0 {
		smpRaise("IndexError", "pop from empty list")
	}
	x := g.stack[len(g.stack)-1]
	g.stack = g.stack[:len(g.stack)-1]
	return x
}

func (g *smpGenState) gen(expression *Expr, comments bool) string {
	g.stack = []any{expression}
	g.sqls.Reset()

	for len(g.stack) > 0 {
		node := g.pop()

		switch x := node.(type) {
		case *Expr:
			if x == nil {
				continue
			}
			if comments && len(x.Comments()) > 0 {
				g.push(" /*" + strings.Join(x.Comments(), ",") + "*/")
			}

			if !g.dispatch(x) {
				if x.IsA(KFunc) {
					g.function(x)
				} else {
					key := pyUpper(x.Key())
					if g.args(x, 0) {
						g.push(key + " ")
					} else {
						g.push(key)
					}
				}
			}
		case []*Expr:
			for i := len(x) - 1; i >= 0; i-- {
				if x[i] != nil {
					g.push(x[i], ",")
				}
			}
			if len(x) > 0 {
				g.pop()
			}
		case []any:
			for i := len(x) - 1; i >= 0; i-- {
				if x[i] != nil {
					g.push(x[i], ",")
				}
			}
			if len(x) > 0 {
				g.pop()
			}
		case []string:
			for i := len(x) - 1; i >= 0; i-- {
				g.push(x[i], ",")
			}
			if len(x) > 0 {
				g.pop()
			}
		default:
			if node != nil {
				g.sqls.WriteString(smpPyStr(node))
			}
		}
	}

	return g.sqls.String()
}

// smpPyStr mirrors str(value) for scalar argument values.
func smpPyStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(x)
	case DType:
		return "DType." + dtypeNames[x]
	}
	return fmt.Sprint(v)
}

// dispatch mirrors GEN_DISPATCH (handlers are looked up by the exact expression key).
func (g *smpGenState) dispatch(e *Expr) bool {
	switch e.kind {
	case KAdd:
		g.binary(e, " + ")
	case KAlias:
		g.push(e.Arg("alias"), " AS ", e.Arg("this"))
	case KAnd:
		g.binary(e, " AND ")
	case KAnonymous:
		g.anonymousSQL(e)
	case KBetween:
		g.push(e.Arg("high"), " AND ", e.Arg("low"), " BETWEEN ", e.Arg("this"))
	case KBoolean:
		if e.ArgB("this") {
			g.push("TRUE")
		} else {
			g.push("FALSE")
		}
	case KBracket:
		g.push("]", e.Expressions(), "[", e.Arg("this"))
	case KColumn:
		parts := e.Parts()
		for i := len(parts) - 1; i >= 0; i-- {
			g.push(parts[i], ".")
		}
		g.pop()
	case KDataType:
		g.args(e, 1)
		g.push(smpDTypeName(e.Arg("this")) + " ")
	case KDiv:
		g.binary(e, " / ")
	case KDot:
		g.binary(e, ".")
	case KEQ:
		g.binary(e, " = ")
	case KFrom:
		g.push(e.Arg("this"), "FROM ")
	case KGT:
		g.binary(e, " > ")
	case KGTE:
		g.binary(e, " >= ")
	case KIdentifier:
		if e.ArgB("quoted") {
			g.push(`"` + smpPyStr(e.Arg("this")) + `"`)
		} else {
			g.push(e.Arg("this"))
		}
	case KILike:
		if e.ArgB("negate") {
			g.binary(e, " NOT ILIKE ")
		} else {
			g.binary(e, " ILIKE ")
		}
	case KIn:
		g.push(")")
		g.args(e, 1)
		g.push("(", " IN ", e.Arg("this"))
	case KIntDiv:
		g.binary(e, " DIV ")
	case KIs:
		g.binary(e, " IS ")
	case KLike:
		if e.ArgB("negate") {
			g.binary(e, " NOT Like ")
		} else {
			g.binary(e, " Like ")
		}
	case KLiteral:
		if e.IsString() {
			g.push("'" + smpPyStr(e.Arg("this")) + "'")
		} else {
			g.push(e.Arg("this"))
		}
	case KLT:
		g.binary(e, " < ")
	case KLTE:
		g.binary(e, " <= ")
	case KMod:
		g.binary(e, " % ")
	case KMul:
		g.binary(e, " * ")
	case KNeg:
		g.push(e.Arg("this"), "-")
	case KNEQ:
		g.binary(e, " <> ")
	case KNot:
		g.push(e.Arg("this"), "NOT ")
	case KNull:
		g.push("NULL")
	case KOr:
		g.binary(e, " OR ")
	case KParen:
		g.push(")", e.Arg("this"), "(")
	case KSub:
		g.binary(e, " - ")
	case KSubquery:
		g.args(e, 2)
		if alias := e.Arg("alias"); truthy(alias) {
			g.push(alias)
		}
		g.push(")", e.Arg("this"), "(")
	case KTable:
		g.args(e, 4)
		if alias := e.Arg("alias"); truthy(alias) {
			g.push(alias)
		}
		parts := e.Parts()
		for i := len(parts) - 1; i >= 0; i-- {
			g.push(parts[i], ".")
		}
		g.pop()
	case KTableAlias:
		if columns := e.ArgL("columns"); len(columns) > 0 {
			g.push(")", columns, "(")
		}
		g.push(e.Arg("this"), " AS ")
	case KVar:
		g.push(e.Arg("this"))
	default:
		return false
	}
	return true
}

func smpDTypeName(v any) string {
	switch x := v.(type) {
	case DType:
		return dtypeNames[x]
	case *Expr:
		return x.Name()
	}
	smpRaise("AttributeError", "object has no attribute 'name'")
	return ""
}

func (g *smpGenState) anonymousSQL(e *Expr) {
	var name string
	switch this := e.Arg("this").(type) {
	case string:
		name = pyUpper(this)
	case *Expr:
		if !this.IsA(KIdentifier) {
			panic(&ValueError{Msg: "Anonymous.this expects a str or an Identifier, got '" + this.kind.Name() + "'."})
		}
		name = this.ThisS()
		if this.ArgB("quoted") {
			name = `"` + name + `"`
		} else {
			name = pyUpper(name)
		}
	default:
		cls := "NoneType"
		switch this.(type) {
		case int:
			cls = "int"
		case bool:
			cls = "bool"
		case DType:
			cls = "DType"
		case []*Expr:
			cls = "list"
		}
		panic(&ValueError{Msg: "Anonymous.this expects a str or an Identifier, got '" + cls + "'."})
	}

	g.push(")", e.Expressions(), "(", name)
}

func (g *smpGenState) binary(e *Expr, op string) {
	g.push(e.Arg("expression"), op, e.Arg("this"))
}

func (g *smpGenState) function(e *Expr) {
	values := make([]any, len(e.args))
	for i, a := range e.args {
		values[i] = a.val
	}
	g.push(")", values, "(", e.kind.SQLName())
}

func (g *smpGenState) args(node *Expr, argIndex int) bool {
	var kvs []any
	argTypes := node.kind.ArgTypes()
	if argIndex > 0 {
		if argIndex < len(argTypes) {
			argTypes = argTypes[argIndex:]
		} else {
			argTypes = nil
		}
	}

	for _, k := range argTypes {
		v := node.Arg(k.name)

		if v != nil {
			kvs = append(kvs, []any{":" + k.name, v})
		}
	}
	if len(kvs) > 0 {
		g.push(kvs)
		return true
	}
	return false
}
