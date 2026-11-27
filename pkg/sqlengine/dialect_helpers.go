package sqlengine

// Extra helper parameters note their conventional values in /* */; optional string parameters
// use "" for unset.

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// STRICT_TIME_FORMATS lists the strict month/day format tokens with their plain equivalents, in
// order. "Strict" dialects (e.g. modern Hive, Spark 3+) map their zero-padded MM/dd to these in
// TIME_MAPPING so they roundtrip, since a lax %m/%d renders non-padded there for parse
// expressions (see hiveFormatTime).
var STRICT_TIME_FORMATS = [][2]string{{"%mstrict", "%m"}, {"%dstrict", "%d"}}

// dhFormatTimeLiteral returns a formatTime result as a string literal; a missing result
// (ok=false) becomes the literal 'None'.
func dhFormatTimeLiteral(s string, ok bool) *Expr {
	if !ok {
		return LiteralString("None")
	}
	return LiteralString(s)
}

// formatTimeStr maps a quoted format string (quotes included) through the dialect's
// TIME_MAPPING and returns it as a string literal.
func (d *Dialect) formatTimeStr(s string) *Expr {
	r := []rune(s)
	inner := ""
	if len(r) >= 2 {
		inner = string(r[1 : len(r)-1])
	}
	return dhFormatTimeLiteral(formatTime(inner, d.S.TIME_MAPPING, d.timeTrie))
}

func (d *Dialect) formatTimeExpr(e *Expr) *Expr {
	if e != nil && e.IsString() {
		return dhFormatTimeLiteral(formatTime(e.ThisS(), d.S.TIME_MAPPING, d.timeTrie))
	}
	return e
}

func dhFlattenArgs(e *Expr) []any {
	out := make([]any, 0, len(e.args))
	for _, a := range e.args {
		switch v := a.val.(type) {
		case []*Expr:
			for _, x := range v {
				out = append(out, x)
			}
		case []string:
			for _, x := range v {
				out = append(out, x)
			}
		case int:
			// 0 generates as nothing; other ints are passed on and rejected by Generator.sql.
			if v == 0 {
				out = append(out, "")
			} else {
				out = append(out, v)
			}
		default:
			out = append(out, v)
		}
	}
	return out
}

func dhUnsupportedArgs(g *Generator, e *Expr, args ...string) {
	for _, arg := range args {
		if e.ArgB(arg) {
			g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", arg, e.Kind().Name(), g.d.ClassName))
		}
	}
}

func renameFunc(name string) GenFunc {
	return func(g *Generator, e *Expr) string {
		return g.fn(name, dhFlattenArgs(e)...)
	}
}

func bracketToElementAtSQL(g *Generator, e *Expr) string {
	// The index is shifted by 1 - offset; offset may be an int or a bool (true counts as 1).
	offset := 0
	switch v := e.Arg("offset").(type) {
	case int:
		offset = v
	case bool:
		if v {
			offset = 1
		}
	}
	index := seqGet(dhApplyIndexOffset(e.This(), e.Expressions(), 1-offset, g.d), 0)
	return g.fn("ELEMENT_AT", e.Arg("this"), index)
}

func approxCountDistinctSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "accuracy")
	return g.fn("APPROX_COUNT_DISTINCT", e.Arg("this"))
}

// ifSQL returns a GenFunc generating name(cond, true[, false]); falseValue (nil, a string or an
// *Expr) replaces a missing false branch.
func ifSQL(name string /*="IF"*/, falseValue any /*=nil*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		f := e.Arg("false")
		if !truthy(f) {
			f = falseValue
		}
		return g.fn(name, e.Arg("this"), e.Arg("true"), f)
	}
}

func arrowJSONExtractSQL(g *Generator, e *Expr) string {
	return arrowJSONExtractOpSQL(g, e, "")
}

// arrowJSONExtractOpSQL is arrowJSONExtractSQL with the operator op ("" picks -> or ->>). A path
// that is an operation is parenthesized, since it would bind looser than the arrow; CONCAT counts
// as one because it may be generated as a chain of ||. A path that is itself a JSON extraction
// stays unparenthesized, continuing the arrow chain.
func arrowJSONExtractOpSQL(g *Generator, e *Expr, op string) string {
	this := e.This()
	if g.s.JSON_TYPE_REQUIRED_FOR_EXTRACTION && this.IsA(KLiteral) && this.IsString() {
		this.Replace(CastExpr(this, DT_JSON, true, nil))
	}
	if path := e.Expression(); path.IsA(KBinary, KPredicate, KNot, KConcat) &&
		!path.IsA(KJSONExtract, KJSONExtractScalar) {
		e.Set("expression", New(KParen, "this", path))
	}
	if op == "" {
		op = "->>"
		if e.IsA(KJSONExtract) {
			op = "->"
		}
	}
	return g.binary(e, op)
}

func inlineArraySQL(g *Generator, e *Expr) string {
	return "[" + g.expressions(e, exprsOpts{dynamic: true, newLine: true, skipFirst: true, skipLast: true}) + "]"
}

func regexpCaptureGroupCount(pattern *Expr) (int, bool) {
	if pattern != nil && (pattern.IsString() || pattern.IsA(KRawString)) {
		return pyRegexGroups(pattern.Name())
	}
	return 0, false
}

// pyRegexGroups returns the number of capturing groups in pattern, or ok=false when the
// pattern is invalid: bad escapes, unbalanced parentheses, unterminated sets, unknown (?...)
// extensions, misplaced global flags, invalid group references and nothing-to-repeat or
// multiple-repeat errors.
func pyRegexGroups(pattern string) (int, bool) {
	rs := []rune(pattern)
	n := len(rs)
	groups := 0       // capturing groups opened so far
	closedGroups := 0 // capturing groups closed so far (for backreference validation)
	names := map[string]bool{}
	var stack []bool // per open paren: is it a capturing group?
	isASCIILetter := func(r rune) bool { return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }
	isDigit := func(r rune) bool { return r >= '0' && r <= '9' }
	const validEscapes = "abBdDfnrsStvwWZAxuUN"
	const validClassEscapes = "abdDfnrsStvwWxuUN"
	const flagChars = "aiLmsux-"
	// canRepeat reports whether a repeat operator is allowed at this point (i.e. there is an item).
	canRepeat := false
	lastWasRepeat := false
	for i := 0; i < n; i++ {
		c := rs[i]
		wasRepeat := lastWasRepeat
		lastWasRepeat = false
		switch c {
		case '\\':
			if i+1 >= n {
				return 0, false // bad escape (end of pattern)
			}
			i++
			e := rs[i]
			if isASCIILetter(e) && !strings.ContainsRune(validEscapes, e) {
				return 0, false // bad escape
			}
			if e >= '1' && e <= '9' {
				num := int(e - '0')
				if i+1 < n && isDigit(rs[i+1]) {
					num = num*10 + int(rs[i+1]-'0')
					i++
				}
				if num > closedGroups {
					return 0, false // invalid group reference
				}
			}
			canRepeat = !(e == 'b' || e == 'B' || e == 'A' || e == 'Z')
		case '[':
			i++
			if i < n && rs[i] == '^' {
				i++
			}
			if i < n && rs[i] == ']' {
				i++
			}
			closed := false
			for ; i < n; i++ {
				if rs[i] == '\\' {
					if i+1 >= n {
						return 0, false
					}
					i++
					if isASCIILetter(rs[i]) && !strings.ContainsRune(validClassEscapes, rs[i]) {
						return 0, false
					}
					continue
				}
				if rs[i] == ']' {
					closed = true
					break
				}
			}
			if !closed {
				return 0, false // unterminated character set
			}
			canRepeat = true
		case '(':
			capturing := true
			if i+1 < n && rs[i+1] == '?' {
				capturing = false
				if i+2 >= n {
					return 0, false // unexpected end of pattern
				}
				switch x := rs[i+2]; {
				case x == ':' || x == '=' || x == '!' || x == '>':
					i += 2
				case x == '(':
					// conditional (?(id)yes|no): skip the condition
					j := i + 3
					for j < n && rs[j] != ')' {
						j++
					}
					if j >= n {
						return 0, false
					}
					i = j
				case x == '#':
					// comment group: skip to the closing parenthesis
					j := i + 3
					for j < n && rs[j] != ')' {
						j++
					}
					if j >= n {
						return 0, false // missing ), unterminated comment
					}
					i = j
					continue
				case x == '<':
					if i+3 >= n || (rs[i+3] != '=' && rs[i+3] != '!') {
						return 0, false // unknown extension ?<x
					}
					i += 3
				case x == 'P':
					if i+3 < n && rs[i+3] == '<' {
						j := i + 4
						for j < n && rs[j] != '>' {
							j++
						}
						if j >= n || j == i+4 {
							return 0, false // missing >, unterminated name / missing group name
						}
						names[string(rs[i+4:j])] = true
						capturing = true
						i = j
					} else if i+3 < n && rs[i+3] == '=' {
						j := i + 4
						for j < n && rs[j] != ')' {
							j++
						}
						if j >= n || !names[string(rs[i+4:j])] {
							return 0, false // unknown group name
						}
						i = j
						canRepeat = true
						continue
					} else {
						return 0, false // unknown extension ?P
					}
				case strings.ContainsRune(flagChars, x):
					j := i + 2
					for j < n && strings.ContainsRune(flagChars, rs[j]) {
						j++
					}
					if j >= n {
						return 0, false // missing -, : or )
					}
					if rs[j] == ')' {
						// global flags must be at the start of the expression
						if i != 0 {
							return 0, false
						}
						i = j
						canRepeat = false
						continue
					}
					if rs[j] != ':' {
						return 0, false // missing :
					}
					i = j
				default:
					return 0, false // unknown extension
				}
			}
			if capturing {
				groups++
			}
			stack = append(stack, capturing)
			canRepeat = false
		case ')':
			if len(stack) == 0 {
				return 0, false // unbalanced parenthesis
			}
			if stack[len(stack)-1] {
				closedGroups++
			}
			stack = stack[:len(stack)-1]
			canRepeat = true
		case '|':
			canRepeat = false
		case '*', '+', '?':
			if wasRepeat {
				if c == '*' {
					return 0, false // multiple repeat
				}
				// lazy (`*?`) / possessive (`*+`) modifiers
				canRepeat = false
				continue
			}
			if !canRepeat {
				return 0, false // nothing to repeat
			}
			lastWasRepeat = true
			canRepeat = false
		case '{':
			// {m}, {m,}, {,n}, {m,n} is a repeat; anything else is a literal "{"
			j := i + 1
			for j < n && (isDigit(rs[j]) || rs[j] == ',') {
				j++
			}
			body := string(rs[i+1 : min(j, n)])
			if j < n && rs[j] == '}' && body != "" && body != "," && strings.Count(body, ",") <= 1 {
				if wasRepeat {
					return 0, false // multiple repeat
				}
				if !canRepeat {
					return 0, false // nothing to repeat
				}
				i = j
				lastWasRepeat = true
				canRepeat = false
				continue
			}
			canRepeat = true
		case '^', '$':
			canRepeat = false
		default:
			canRepeat = true
		}
	}
	if len(stack) != 0 {
		return 0, false // missing ), unterminated subpattern
	}
	return groups, true
}

func nthValueFromSQL(g *Generator, e *Expr) string {
	result := g.fn("NTH_VALUE", e.Arg("this"), e.Arg("offset"))

	fromFirst := e.Arg("from_first")
	if fromFirst == nil {
		return result
	}

	if truthy(fromFirst) {
		return result + " FROM FIRST"
	}
	return result + " FROM LAST"
}

func inlineArrayUnlessQuery(g *Generator, e *Expr) string {
	elem := seqGet(e.Expressions(), 0)
	// A value constructor's single query is a scalar element, e.g. [(SELECT ...)]
	if len(e.Expressions()) == 1 && elem.IsA(KQuery) && !e.ArgB("value_constructor") {
		return g.fn("ARRAY", elem)
	}
	return inlineArraySQL(g, e)
}

func noIlikeSQL(g *Generator, e *Expr) string {
	return g.likeSQL_(New(
		KLike,
		"this", New(KLower, "this", e.This()),
		"expression", New(KLower, "this", e.Expression()),
		"negate", e.Arg("negate"),
	))
}

func noParenCurrentDateSQL(g *Generator, e *Expr) string {
	zone := g.sqlKey(e, "this")
	if zone != "" {
		return "CURRENT_DATE AT TIME ZONE " + zone
	}
	return "CURRENT_DATE"
}

func noRecursiveCteSQL(g *Generator, e *Expr) string {
	if e.ArgB("recursive") {
		g.unsupported("Recursive CTEs are unsupported")
		e.Set("recursive", false)
	}
	return g.withSQL(e)
}

func noTablesampleSQL(g *Generator, e *Expr) string {
	g.unsupported("TABLESAMPLE unsupported")
	return g.sql(e.Arg("this"))
}

func noPivotSQL(g *Generator, e *Expr) string {
	g.unsupported("PIVOT unsupported")
	return ""
}

func noTrycastSQL(g *Generator, e *Expr) string {
	return g.castSQL(e, "")
}

func noCommentColumnConstraintSQL(g *Generator, e *Expr) string {
	g.unsupported("CommentColumnConstraint unsupported")
	return ""
}

func noMapFromEntriesSQL(g *Generator, e *Expr) string {
	g.unsupported("MAP_FROM_ENTRIES unsupported")
	return ""
}

func propertySQL(g *Generator, e *Expr) string {
	return g.propertyName(e, true) + "=" + g.sqlKey(e, "value")
}

// strpositionSQL generates a substring search as funcName(...). LOCATE and CHARINDEX take the
// substring first; POSITION becomes POSITION(substr IN str) with useAnsiPosition. A start
// position the function does not support is emulated with SUBSTRING and an offset correction;
// an unsupported occurrence is dropped with a warning.
func strpositionSQL(
	g *Generator,
	e *Expr,
	funcName string, /*="STRPOS"*/
	supportsPosition bool, /*=false*/
	supportsOccurrence bool, /*=false*/
	useAnsiPosition bool, /*=true*/
) string {
	str := e.This()
	substr := e.ArgE("substr")
	position := e.ArgE("position")
	occurrence := e.ArgE("occurrence")
	zero := LiteralInt(0)
	one := LiteralInt(1)

	if supportsOccurrence && occurrence != nil && supportsPosition && position == nil {
		position = one
	}

	transpilePosition := position != nil && !supportsPosition
	if transpilePosition {
		str = New(KSubstring, "this", str, "start", position)
	}

	var fn *Expr
	if funcName == "POSITION" && useAnsiPosition {
		fn = New(KAnonymous, "this", funcName, "expressions", []*Expr{New(KIn, "this", substr, "field", str)})
	} else {
		var args []*Expr
		if funcName == "LOCATE" || funcName == "CHARINDEX" {
			args = []*Expr{substr, str}
		} else {
			args = []*Expr{str, substr}
		}
		if supportsPosition {
			args = append(args, position)
		}
		if occurrence != nil {
			if supportsOccurrence {
				args = append(args, occurrence)
			} else {
				g.unsupported(funcName + " does not support the occurrence parameter.")
			}
		}
		fn = New(KAnonymous, "this", funcName, "expressions", args)
	}

	if transpilePosition {
		funcWithOffset := New(KSub, "this", dhBinop(KAdd, fn, position), "expression", one)
		funcWrapped := New(KIf, "this", dhBinop(KEQ, fn, zero), "true", zero, "false", funcWithOffset)
		return g.sql(funcWrapped)
	}

	return g.sql(fn)
}

func structExtractSQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + "." + g.sql(ToIdentifier(e.Expression().Name(), nil))
}

// arrayAppendSQL transpiles ARRAY_APPEND/ARRAY_PREPEND between dialects with different NULL
// propagation semantics; dialects that propagate NULLs set ARRAY_FUNCS_PROPAGATES_NULLS.
func arrayAppendSQL(name string, swapParams bool /*=false*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := e.This()
		element := e.Expression()
		args := []any{this, element}
		if swapParams {
			args = []any{element, this}
		}
		funcSQL := g.fn(name, args...)

		sourceNullPropagation := e.ArgB("null_propagation")
		targetNullPropagation := g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS

		// No transpilation needed when source and target have matching NULL semantics
		if sourceNullPropagation == targetNullPropagation {
			return funcSQL
		}

		// Source propagates NULLs, target doesn't: wrap in conditional to return NULL explicitly
		if sourceNullPropagation {
			return g.sql(New(
				KIf,
				"this", New(KIs, "this", this, "expression", New(KNull)),
				"true", New(KNull),
				"false", funcSQL,
			))
		}

		// Source doesn't propagate NULLs, target does: use COALESCE to convert NULL to empty array
		this = New(KCoalesce, "expressions", []*Expr{this, New(KArray, "expressions", []*Expr{})})
		args = []any{this, element}
		if swapParams {
			args = []any{element, this}
		}
		return g.fn(name, args...)
	}
}

// generateSeriesSQL returns a GenFunc generating funcName(start, end, step). An end-exclusive
// series uses exclusiveFuncName when set ("" means none), otherwise end - 1.
func generateSeriesSQL(funcName string, exclusiveFuncName string /*=""*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		start := e.Arg("start")
		end := e.Arg("end")
		step := e.Arg("step")

		if e.ArgB("is_end_exclusive") {
			if exclusiveFuncName != "" {
				return g.fn(exclusiveFuncName, start, end, step)
			}
			adjustedEnd := New(KSub, "this", end, "expression", LiteralInt(1))
			return g.fn(funcName, start, adjustedEnd, step)
		}

		return g.fn(funcName, start, end, step)
	}
}

// arrayConcatSQL transpiles ARRAY_CONCAT/ARRAY_CAT between dialects with different NULL
// propagation semantics; dialects that propagate NULLs set ARRAY_FUNCS_PROPAGATES_NULLS.
func arrayConcatSQL(name string) GenFunc {
	buildFuncCall := func(g *Generator, funcName string, args []*Expr) string {
		if g.s.ARRAY_CONCAT_IS_VAR_LEN {
			return g.fn(funcName, dhExprsToAny(args)...)
		} else if len(args) == 1 {
			// Single arg gets empty array to preserve semantics
			return g.fn(funcName, args[0], New(KArray, "expressions", []*Expr{}))
		}
		// Snowflake/PostgreSQL/Redshift require binary nesting: ARRAY_CAT(a, ARRAY_CAT(b, c))
		// Build right-deep tree recursively to avoid creating new ArrayConcat expressions
		result := g.fn(funcName, args[len(args)-2], args[len(args)-1])
		rest := args[:len(args)-2]
		for i := len(rest) - 1; i >= 0; i-- {
			result = funcName + "(" + g.sql(rest[i]) + ", " + result + ")"
		}
		return result
	}

	return func(g *Generator, e *Expr) string {
		this := e.This()
		if x, ok := e.Arg("expressions").(*Expr); ok && x != nil {
			// A single node instead of a list in `expressions` (ClickHouse arrayConcat) is
			// rejected; the messages keep their historical wording.
			if e.ArgB("null_propagation") == g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS || this.IsA(KArray) {
				if g.s.ARRAY_CONCAT_IS_VAR_LEN {
					panic(&ValueError{Msg: "'Add' object is not iterable"})
				}
				panic(&ValueError{Msg: "object of type 'Add' has no len()"})
			}
			panic(&ValueError{Msg: fmt.Sprintf("object of type '%s' has no len()", x.Kind().Name())})
		}
		exprs := e.Expressions()
		allArgs := append([]*Expr{this}, exprs...)

		sourceNullPropagation := e.ArgB("null_propagation")
		targetNullPropagation := g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS

		// Skip wrapper when source and target have matching NULL semantics,
		// or when the first argument is an array literal (which can never be NULL),
		// or when it's a single-argument call (empty array is added, preserving NULL semantics)
		if sourceNullPropagation == targetNullPropagation || this.IsA(KArray) || len(exprs) == 0 {
			return buildFuncCall(g, name, allArgs)
		}

		// Case 1: Source propagates NULLs, target doesn't (Snowflake → DuckDB)
		// Check if ANY argument is NULL and return NULL explicitly
		if sourceNullPropagation {
			nullChecks := make([]*Expr, 0, len(allArgs))
			for _, arg := range allArgs {
				nullChecks = append(nullChecks, New(KIs, "this", arg.Copy(), "expression", New(KNull)))
			}
			combinedCheck := nullChecks[0]
			for _, b := range nullChecks[1:] {
				combinedCheck = New(KOr, "this", combinedCheck, "expression", b)
			}

			funcSQL := buildFuncCall(g, name, allArgs)

			return g.sql(New(KIf, "this", combinedCheck, "true", New(KNull), "false", funcSQL))
		}

		// Case 2: Source doesn't propagate NULLs, target does (DuckDB → Snowflake)
		// Wrap ALL arguments in COALESCE to convert NULL → empty array
		wrappedArgs := make([]*Expr, 0, len(allArgs))
		for _, arg := range allArgs {
			wrappedArgs = append(wrappedArgs, New(KCoalesce, "expressions", []*Expr{arg.Copy(), New(KArray, "expressions", []*Expr{})}))
		}

		return buildFuncCall(g, name, wrappedArgs)
	}
}

func dhExprsToAny(list []*Expr) []any {
	out := make([]any, len(list))
	for i, x := range list {
		out[i] = x
	}
	return out
}

func varMapSQL(g *Generator, e *Expr, mapFuncName string /*="MAP"*/) string {
	keys := e.Arg("keys")
	values := e.Arg("values")
	keysE, _ := keys.(*Expr)
	valuesE, _ := values.(*Expr)

	if !keysE.IsA(KArray) || !valuesE.IsA(KArray) {
		g.unsupported("Cannot convert array columns into map.")
		return g.fn(mapFuncName, keys, values)
	}

	var args []any
	ks, vs := keysE.Expressions(), valuesE.Expressions()
	for i := 0; i < len(ks) && i < len(vs); i++ {
		args = append(args, g.sql(ks[i]))
		args = append(args, g.sql(vs[i]))
	}

	return g.fn(mapFuncName, args...)
}

// monthsBetweenSQL transpiles MONTHS_BETWEEN to dialects that don't have native support.
// Formula: DATEDIFF('month', date2, date1) + (DAY(date1) - DAY(date2)) / 31.0, with the
// fraction 0 when both dates are the last day of their month.
func monthsBetweenSQL(g *Generator, e *Expr) string {
	date1 := e.This()
	date2 := e.Expression()

	date1Cast := CastExpr(date1, DT_DATE, false, nil)
	date2Cast := CastExpr(date2, DT_DATE, false, nil)

	wholeMonths := New(KDateDiff, "this", date1Cast, "expression", date2Cast, "unit", VarChecked("month"))

	day1 := New(KDay, "this", date1Cast.Copy())
	day2 := New(KDay, "this", date2Cast.Copy())

	lastDayOfMonth1 := New(KLastDay, "this", date1Cast.Copy())
	lastDayOfMonth2 := New(KLastDay, "this", date2Cast.Copy())

	dayOfLastDay1 := New(KDay, "this", lastDayOfMonth1)
	dayOfLastDay2 := New(KDay, "this", lastDayOfMonth2)

	lastDay1 := New(KEQ, "this", day1.Copy(), "expression", dayOfLastDay1)
	lastDay2 := New(KEQ, "this", day2.Copy(), "expression", dayOfLastDay2)
	bothLastDay := New(KAnd, "this", lastDay1, "expression", lastDay2)

	fractional := New(
		KDiv,
		"this", New(KParen, "this", New(KSub, "this", day1.Copy(), "expression", day2.Copy())),
		"expression", LiteralNumber("31.0"),
	)

	fractionalWithCheck := New(KIf, "this", bothLastDay, "true", LiteralNumber("0"), "false", fractional)

	result := New(KAdd, "this", wholeMonths, "expression", fractionalWithCheck)

	return g.sql(result)
}

// buildFormattedTime returns a builder for kind(this, format), mapping the format through the
// TIME_MAPPING of the dialect (or of dialectOverride; "" means none). Without a format
// argument, def selects one: true for the dialect's TIME_FORMAT, a string for that format,
// nil or false for none.
func buildFormattedTime(kind Kind, dialectOverride string /*=""*/, def any /*=nil*/) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		targetDialect := d
		if dialectOverride != "" {
			targetDialect = MustDialect(dialectOverride)
		}

		var format *Expr
		if fmtArg := seqGet(args, 1); fmtArg != nil {
			format = targetDialect.formatTimeExpr(fmtArg)
		} else {
			if b, ok := def.(bool); ok && b {
				format = targetDialect.formatTimeStr(targetDialect.S.TIME_FORMAT)
			} else if s, ok := def.(string); ok && s != "" {
				format = targetDialect.formatTimeStr(s)
			}
		}

		return New(kind, "this", seqGet(args, 0), "format", format)
	}
}

// timeFormat returns a callable yielding an expression's time format, or nil when it equals
// the TIME_FORMAT of the given dialect ("" means the base dialect), so that the result can be
// passed straight to g.fn.
func timeFormat(dialect string /*=""*/) func(g *Generator, e *Expr) any {
	return func(g *Generator, e *Expr) any {
		tf := g.formatTime(e, nil, nil)
		if tf == "" || tf == MustDialect(dialect).S.TIME_FORMAT {
			return nil
		}
		return tf
	}
}

// buildDateDelta returns a builder for kind(this, expression, unit). With three or more
// arguments the unit comes first (and with four and supportsTimezone the last one is the
// zone); otherwise defaultUnit is used ("" means none). unitMapping renames lowercased units.
func buildDateDelta(
	kind Kind,
	unitMapping map[string]string, /*=nil*/
	defaultUnit string, /*="DAY"*/
	supportsTimezone bool, /*=false*/
) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		unitBased := len(args) >= 3
		hasTimezone := len(args) == 4
		var this *Expr
		if unitBased {
			this = args[2]
		} else {
			this = seqGet(args, 0)
		}
		var unit *Expr
		if unitBased || defaultUnit != "" {
			if unitBased {
				unit = args[0]
			} else {
				unit = LiteralString(defaultUnit)
			}
			if len(unitMapping) > 0 {
				name := unit.Name()
				if mapped, ok := unitMapping[pyLower(name)]; ok {
					name = mapped
				}
				unit = VarChecked(name)
			}
		}
		expression := New(kind, "this", this, "expression", seqGet(args, 1), "unit", unit)
		if supportsTimezone && hasTimezone {
			expression.Set("zone", args[len(args)-1])
		}
		return expression
	}
}

// buildDateDeltaWithInterval returns a builder for kind(this, n, unit) that takes n and the unit
// from an INTERVAL argument. A non-interval argument uses defaultUnit, or is a parse error when
// defaultUnit is "". Returns nil when fewer than 2 args are given.
func buildDateDeltaWithInterval(kind Kind, defaultUnit string /*=""*/) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		if len(args) < 2 {
			return nil
		}

		interval := args[1]

		if !interval.IsA(KInterval) {
			if defaultUnit == "" {
				panic(parsePanic{&ParseError{Msg: fmt.Sprintf("INTERVAL expression expected but got '%s'", dhPyStr(interval))}})
			}
			return New(kind, "this", args[0], "expression", interval, "unit", LiteralString(defaultUnit))
		}

		return New(kind, "this", args[0], "expression", interval.This(), "unit", unitToStr(interval, "DAY"))
	}
}

func dateTruncToTime(args []*Expr, d *Dialect) *Expr {
	unit := seqGet(args, 0)
	this := seqGet(args, 1)

	if this.IsA(KCast) && dhIsType(this, DT_DATE) {
		return New(KDateTrunc, "unit", unit, "this", this)
	}
	return New(KTimestampTrunc, "this", this, "unit", unit)
}

func dateAddIntervalSQL(dataType string, kind string) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := g.sqlKey(e, "this")
		interval := New(KInterval, "this", e.Expression(), "unit", unitToVar(e, "DAY"))
		return dataType + "_" + kind + "(" + this + ", " + g.sql(interval) + ")"
	}
}

func timestamptruncSQL(fn string /*="DATE_TRUNC"*/, zone bool /*=false*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		args := []any{unitToStr(e, "DAY"), e.This()}
		if zone {
			args = append(args, e.Arg("zone"))
		}
		return g.fn(fn, args...)
	}
}

func noTimestampSQL(g *Generator, e *Expr) string {
	zone := e.ArgE("zone")
	if zone == nil {
		var targetType any = DT_TIMESTAMP
		if t := annotateTypes(e, g.d).Type(); t != nil {
			targetType = t
		}
		return g.sql(CastExpr(e.This(), targetType, true, nil))
	}
	if dhTIMEZONES.Has(pyLower(zone.Name())) {
		return g.sql(New(
			KAtTimeZone,
			"this", CastExpr(e.This(), DT_TIMESTAMP, true, nil),
			"zone", zone,
		))
	}
	return g.fn("TIMESTAMP", e.Arg("this"), zone)
}

func noTimeSQL(g *Generator, e *Expr) string {
	// Transpile BQ's TIME(timestamp, zone) to CAST(TIMESTAMPTZ <timestamp> AT TIME ZONE <zone> AS TIME)
	this := CastExpr(e.This(), DT_TIMESTAMPTZ, true, nil)
	expr := CastExpr(New(KAtTimeZone, "this", this, "zone", e.Arg("zone")), DT_TIME, true, nil)
	return g.sql(expr)
}

func noDatetimeSQL(g *Generator, e *Expr) string {
	this := e.This()
	expr := e.Expression()

	if dhTIMEZONES.Has(pyLower(expr.Name())) {
		// Transpile BQ's DATETIME(timestamp, zone) to CAST(TIMESTAMPTZ <timestamp> AT TIME ZONE <zone> AS TIMESTAMP)
		this = CastExpr(this, DT_TIMESTAMPTZ, true, nil)
		this = CastExpr(New(KAtTimeZone, "this", this, "zone", expr), DT_TIMESTAMP, true, nil)
		return g.sql(this)
	}

	this = CastExpr(this, DT_DATE, true, nil)
	expr = CastExpr(expr, DT_TIME, true, nil)

	return g.sql(CastExpr(New(KAdd, "this", this, "expression", expr), DT_TIMESTAMP, true, nil))
}

func leftToSubstringSQL(g *Generator, e *Expr) string {
	return g.sql(New(KSubstring, "this", e.This(), "start", LiteralInt(1), "length", e.Expression()))
}

func rightToSubstringSQL(g *Generator, e *Expr) string {
	this := e.This()
	start := dhBinop(
		KSub,
		New(KLength, "this", this),
		ParenExpr(dhBinop(KSub, e.Expression(), 1), true),
	)
	return g.sql(New(KSubstring, "this", this, "start", start))
}

func timestrtotimeSQL(g *Generator, e *Expr, includePrecision bool /*=false*/) string {
	builder := DT_TIMESTAMP
	if e.ArgB("zone") {
		builder = DT_TIMESTAMPTZ
	}
	datatype := NewDataType(builder)

	if e.This().IsA(KLiteral) && includePrecision {
		precision := dhSubsecondPrecision(e.This().Name())
		if precision > 0 {
			datatype = NewDataType(datatype.DTypeOf())
			datatype.Set("expressions", []*Expr{New(KDataTypeParam, "this", LiteralInt(precision))})
		}
	}

	return g.sql(CastExpr(e.This(), datatype, true, g.d))
}

func datestrtodateSQL(g *Generator, e *Expr) string {
	return g.sql(CastExpr(e.This(), DT_DATE, true, nil))
}

// encodeDecodeSQL generates name(this[, replace]) for Presto and DuckDB, whose functions take no
// charset and assume UTF-8; any other charset is unsupported.
func encodeDecodeSQL(g *Generator, e *Expr, name string, replace bool /*=true*/) string {
	charset := e.ArgE("charset")
	if charset != nil {
		if n := pyLower(charset.Name()); n != "utf-8" && n != "utf8" {
			g.unsupported(fmt.Sprintf("Expected utf-8 character set, got %s.", dhPyStr(charset)))
		}
	}

	var r any
	if replace {
		r = e.Arg("replace")
	}
	return g.fn(name, e.Arg("this"), r)
}

func minOrLeast(g *Generator, e *Expr) string {
	name := "MIN"
	if len(e.Expressions()) > 0 {
		name = "LEAST"
	}
	return renameFunc(name)(g, e)
}

func maxOrGreatest(g *Generator, e *Expr) string {
	name := "MAX"
	if len(e.Expressions()) > 0 {
		name = "GREATEST"
	}
	return renameFunc(name)(g, e)
}

func countIfToSum(g *Generator, e *Expr) string {
	cond := e.This()

	if e.This().IsA(KDistinct) {
		cond = e.This().Expressions()[0]
		g.unsupported("DISTINCT is not supported when converting COUNT_IF to SUM")
	}

	return g.fn("sum", dhFunc("if", cond, 1, 0))
}

// trimSQL generates TRIM([position ][chars ]FROM target[ COLLATE c]) when characters to remove
// are given, otherwise the dialect's trim function. defaultTrimType applies when no position is
// set.
func trimSQL(g *Generator, e *Expr, defaultTrimType string /*=""*/) string {
	removeChars := g.sqlKey(e, "expression")

	if removeChars == "" {
		return g.trimSQL(e)
	}

	target := g.sqlKey(e, "this")
	trimType := g.sqlKey(e, "position")
	if trimType == "" {
		trimType = defaultTrimType
	}
	collation := g.sqlKey(e, "collation")

	if trimType != "" {
		trimType = trimType + " "
	}
	if removeChars != "" {
		removeChars = removeChars + " "
	}
	fromPart := ""
	if trimType != "" || removeChars != "" {
		fromPart = "FROM "
	}
	if collation != "" {
		collation = " COLLATE " + collation
	}
	return "TRIM(" + trimType + removeChars + fromPart + target + collation + ")"
}

func concatToDpipeSQL(g *Generator, e *Expr) string {
	exprs := g.convertConcatArgs(e)
	// At least one argument is required
	acc := exprs[0]
	for _, y := range exprs[1:] {
		acc = New(KDPipe, "this", acc, "expression", y)
	}
	return g.sql(acc)
}

func concatWsToDpipeSQL(g *Generator, e *Expr) string {
	exprs := e.Expressions()
	delim, restArgs := exprs[0], exprs[1:]
	// At least one argument after the delimiter is required
	acc := restArgs[0]
	for _, y := range restArgs[1:] {
		acc = New(KDPipe, "this", acc, "expression", New(KDPipe, "this", delim, "expression", y))
	}
	return g.sql(acc)
}

func regexpExtractSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "position", "occurrence", "parameters")
	group := e.ArgE("group")

	if group != nil && group.Name() == strconv.Itoa(g.d.S.REGEXP_EXTRACT_DEFAULT_GROUP) {
		group = nil
	}

	return g.fn(e.Kind().SQLName(), e.Arg("this"), e.Arg("expression"), group)
}

func regexpReplaceSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "position", "occurrence", "modifiers")
	if !e.HasArgKey("replacement") {
		// A missing replacement is an error (the message is the quoted key)
		panic(&ValueError{Msg: "'replacement'"})
	}
	return g.fn("REGEXP_REPLACE", e.Arg("this"), e.Arg("expression"), e.Arg("replacement"))
}

// pivotColumnNames returns the column name suffixes of pivot aggregations: the alias, or the
// aggregation's SQL (lowercase function names, identifiers unquoted). d nil means the default
// dialect.
func pivotColumnNames(aggregations []*Expr, d *Dialect) []string {
	names := []string{}
	for _, agg := range aggregations {
		if agg.IsA(KAlias) {
			names = append(names, agg.Alias())
		} else {
			// This case corresponds to aggregations without aliases being used as suffixes
			// (e.g. col_avg(foo)). We need to unquote identifiers because they're going to
			// be quoted by the parser's parsePivot, due to ToIdentifier.
			// Otherwise, we'd end up with `col_avg(`foo`)` (notice the double quotes).
			aggAllUnquoted := agg.Transform(func(node *Expr) *Expr {
				if node.IsA(KIdentifier) {
					return New(KIdentifier, "this", node.Name(), "quoted", false)
				}
				return node
			}, true)
			target := d
			if target == nil {
				target = MustDialect("")
			}
			sql, err := target.NewGenerator(&GenerateOptions{NormalizeFunctions: strp("lower")}).Generate(aggAllUnquoted, true)
			if err != nil {
				panic(err)
			}
			names = append(names, sql)
		}
	}
	return names
}

func binaryFromFunction(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		return New(kind, "this", seqGet(args, 0), "expression", seqGet(args, 1))
	}
}

func buildTimestampTrunc(args []*Expr, d *Dialect) *Expr {
	return New(KTimestampTrunc, "this", seqGet(args, 1), "unit", seqGet(args, 0), "zone", seqGet(args, 2))
}

// buildTrunc builds TRUNC for dialects with overloaded TRUNC (Oracle, Snowflake, etc).
// Uses type annotation to distinguish date vs numeric truncation.
// Returns Anonymous if type cannot be determined. defaultDateTruncUnit "" means none.
func buildTrunc(
	args []*Expr,
	d *Dialect,
	dateTruncUnabbreviate bool, /*=true*/
	defaultDateTruncUnit string, /*=""*/
	dateTruncRequiresPart bool, /*=true*/
	fractionsSupported bool, /*=false*/
) *Expr {
	this := seqGet(args, 0)
	second := seqGet(args, 1)

	if this != nil && this.Type() == nil {
		this = annotateTypes(this, d)
	}
	if second != nil && second.Type() == nil {
		second = annotateTypes(second, d)
	}

	// Date truncation
	if (this != nil && dhIsType(this, DataType_TEMPORAL_TYPES.Items()...) && (second != nil || defaultDateTruncUnit != "")) ||
		(second != nil && dhIsType(second, DataType_TEXT_TYPES.Items()...)) {
		unit := second
		if unit == nil {
			unit = LiteralString(defaultDateTruncUnit)
		}
		// New consumes `unabbreviate` for DateTrunc (see dateTruncCtorArgs); it is not stored.
		return New(KDateTrunc, "this", this, "unit", unit, "unabbreviate", dateTruncUnabbreviate)
	}

	// Numeric truncation
	if (this != nil && dhIsType(this, DataType_NUMERIC_TYPES.Items()...)) ||
		(second != nil && dhIsType(second, DataType_NUMERIC_TYPES.Items()...)) ||
		(!dateTruncRequiresPart && second == nil) {
		return New(KTrunc, "this", this, "decimals", second, "fractions_supported", fractionsSupported)
	}

	return New(KAnonymous, "this", "TRUNC", "expressions", args)
}

func anyValueToMaxSQL(g *Generator, e *Expr) string {
	return g.fn("MAX", e.Arg("this"))
}

func boolXorSQL(g *Generator, e *Expr) string {
	a := g.sql(e.Left())
	b := g.sql(e.Right())
	return "(" + a + " AND (NOT " + b + ")) OR ((NOT " + a + ") AND " + b + ")"
}

func isParseJSON(e *Expr) bool {
	return e.IsA(KParseJSON) || (e.IsA(KCast) && dhIsType(e, DT_JSON))
}

func isnullToIsNull(args []*Expr, d *Dialect) *Expr {
	this := seqGet(args, 0)
	if this.IsA(KPredicate, KConnector, KNot) {
		this = New(KParen, "this", this)
	}
	return New(KParen, "this", New(KIs, "this", this, "expression", Null()))
}

func generatedasidentitycolumnconstraintSQL(g *Generator, e *Expr) string {
	start := g.sqlKey(e, "start")
	if start == "" {
		start = "1"
	}
	increment := g.sqlKey(e, "increment")
	if increment == "" {
		increment = "1"
	}
	return "IDENTITY(" + start + ", " + increment + ")"
}

func argMaxOrMinNoCount(name string) GenFunc {
	return func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "count")
		return g.fn(name, e.Arg("this"), e.Arg("expression"))
	}
}

func dhTsOrDsReturnType(e *Expr) *Expr {
	rt := e.Arg("return_type")
	if !truthy(rt) {
		return NewDataType(DT_DATE)
	}
	return DataTypeBuild(rt, nil, false, true)
}

func tsOrDsAddCast(e *Expr) *Expr {
	this := e.This().Copy()

	returnType := dhTsOrDsReturnType(e)
	if dhIsType(returnType, DT_DATE) {
		// If we need to cast to a DATE, we cast to TIMESTAMP first to make sure we
		// can truncate timestamp strings, because some dialects can't cast them to DATE
		this = CastExpr(this, DT_TIMESTAMP, true, nil)
	}

	e.This().Replace(CastExpr(this, returnType, true, nil))
	return e
}

func dateDeltaSQL(name string, cast bool /*=false*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		if cast && e.IsA(KTsOrDsAdd) {
			e = tsOrDsAddCast(e)
		}

		return g.fn(name, unitToVar(e, "DAY"), e.Arg("expression"), e.Arg("this"))
	}
}

// dateDeltaToBinarySQL generates binary, the +/- form of the date delta e. It is parenthesized
// when e is an operand of an operator or of AT TIME ZONE, except inside parentheses or as the left
// side of + or -.
func dateDeltaToBinarySQL(g *Generator, e *Expr, binary *Expr) string {
	parent := e.Parent()
	if parent.IsA(KBinary, KUnary, KAtTimeZone) &&
		!(parent.IsA(KParen) || (parent.IsA(KAdd, KSub) && parent.This() == e)) {
		return g.sql(Paren(binary))
	}
	return g.sql(binary)
}

// dateDeltaToBinaryIntervalOp returns a GenFunc generating a date delta as this +/- INTERVAL
// (see dateDeltaToBinarySQL). With cast, a TS_OR_DS_ADD operand is cast to its return type and a
// string operand to DATE (DATETIME for DATETIME_ADD/SUB and TIMESTAMP_ADD/SUB).
func dateDeltaToBinaryIntervalOp(cast bool /*=true*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := e.This()
		unit := unitToVar(e, "DAY")
		op := KSub
		if e.IsA(KDateAdd, KTimeAdd, KDatetimeAdd, KTsOrDsAdd, KTimestampAdd) {
			op = KAdd
		}

		var toType any
		if cast {
			if e.IsA(KTsOrDsAdd) {
				toType = dhTsOrDsReturnType(e)
			} else if this.IsString() {
				if e.IsA(KDatetimeAdd, KDatetimeSub, KTimestampAdd, KTimestampSub) {
					toType = DT_DATETIME
				} else {
					toType = DT_DATE
				}
			}
		}

		if toType != nil {
			this = CastExpr(this, toType, true, nil)
		}

		expr := e.Expression()
		interval := expr
		if !expr.IsA(KInterval) {
			interval = New(KInterval, "this", expr, "unit", unit)
		}

		return dateDeltaToBinarySQL(g, e, dhBinop(op, this, interval))
	}
}

// unitToStr returns the unit of e as a string literal; placeholders and units that are neither a
// Var nor a Literal are returned as is. A missing unit becomes def ("" means nil).
func unitToStr(e *Expr, def string /*="DAY"*/) *Expr {
	unit := e.ArgE("unit")
	if unit == nil {
		if def != "" {
			return LiteralString(def)
		}
		return nil
	}

	if unit.IsA(KPlaceholder) || (!unit.Is(KVar) && !unit.Is(KLiteral)) {
		return unit
	}

	return LiteralString(unit.Name())
}

// unitToVar returns the unit of e as a Var; Var, placeholder, WEEK(day) and column units are
// returned as is. A missing unit becomes def ("" means nil).
func unitToVar(e *Expr, def string /*="DAY"*/) *Expr {
	unit := e.ArgE("unit")

	if unit.IsA(KVar, KPlaceholder, KWeekStart, KColumn) {
		return unit
	}

	value := def
	if unit != nil {
		value = unit.Name()
	}
	if value != "" {
		return New(KVar, "this", value)
	}
	return nil
}

// WEEK_START_DAY_TO_DOW maps day names to ISO 8601 day-of-week numbers (Monday=1, Sunday=7).
var WEEK_START_DAY_TO_DOW = map[string]int{
	"MONDAY":    1,
	"TUESDAY":   2,
	"WEDNESDAY": 3,
	"THURSDAY":  4,
	"FRIDAY":    5,
	"SATURDAY":  6,
	"SUNDAY":    7,
}

// weekOffsetToDow converts a dialect's WEEK_OFFSET (days relative to Monday) to the ISO day number
// of its week's first day.
func weekOffsetToDow(offset int) int {
	return ((offset%7)+7)%7 + 1
}

// weekUnitToDow computes the week start day for a week-ish diff unit, e.g BigQuery's WEEK(<day>)
// or ISOWEEK unit parts. ok=false when the unit is not week-based.
func weekUnitToDow(unit *Expr) (int, bool) {
	if unit.IsA(KVar) {
		if n := pyUpper(unit.Name()); n == "WEEK" || n == "ISOWEEK" {
			return 1, true
		}
	}

	if unit.IsA(KWeekStart) {
		v, ok := WEEK_START_DAY_TO_DOW[pyUpper(unit.Name())]
		return v, ok
	}

	return 0, false
}

// mapDatePart renames part through the dialect's DATE_PART_MAPPING (string literals stay
// strings); d nil means the base dialect. Qualified columns are returned as is.
func mapDatePart(part *Expr, d *Dialect) *Expr {
	mapped := ""
	if part != nil && !(part.IsA(KColumn) && len(part.Parts()) != 1) {
		if d == nil {
			d = MustDialect("")
		}
		mapped = d.S.DATE_PART_MAPPING[pyUpper(part.Name())]
	}
	if mapped != "" {
		if part.IsString() {
			return LiteralString(mapped)
		}
		return VarChecked(mapped)
	}

	return part
}

func noLastDaySQL(g *Generator, e *Expr) string {
	truncCurrDate := dhFunc("date_trunc", "month", e.This())
	plusOneMonth := dhFunc("date_add", truncCurrDate, 1, "month")
	minusOneDay := dhFunc("date_sub", plusOneMonth, 1, "day")

	return g.sql(CastExpr(minusOneDay, DT_DATE, true, nil))
}

// mergeWithoutTargetSQL generates MERGE for dialects that reject target-qualified columns.
func mergeWithoutTargetSQL(g *Generator, e *Expr) string {
	alias := e.This().argForAttr("alias", "this")

	normalize := func(identifier *Expr) (string, bool) {
		if identifier == nil {
			return "", false
		}
		return g.d.NormalizeIdentifier(identifier).Name(), true
	}

	targets := map[string]bool{}
	targetsHasNone := false
	add := func(s string, ok bool) {
		if ok {
			targets[s] = true
		} else {
			targetsHasNone = true
		}
	}
	in := func(s string, ok bool) bool {
		if ok {
			return targets[s]
		}
		return targetsHasNone
	}

	add(normalize(e.This().This()))

	if alias != nil {
		add(normalize(alias.This()))
	}

	for _, when := range e.ArgE("whens").Expressions() {
		// only remove the target table names from certain parts of WHEN MATCHED / WHEN NOT MATCHED
		// they are still valid in the <condition>, the right hand side of each UPDATE and the VALUES part
		// (not the column list) of the INSERT
		then := when.ArgE("then")
		if then != nil {
			if then.IsA(KUpdate) {
				for equals := range then.FindAll(KEQ) {
					equalLHS := equals.This()
					if equalLHS.IsA(KColumn) && in(normalize(equalLHS.ArgE("table"))) {
						equalLHS.Replace(ColumnExpr(equalLHS.This(), nil, nil, nil, nil, nil, true))
					}
				}
			}
			if then.IsA(KInsert) {
				columnList := then.This()
				if columnList.IsA(KTuple) {
					for _, column := range columnList.Expressions() {
						if in(normalize(column.ArgE("table"))) {
							column.Replace(ColumnExpr(column.This(), nil, nil, nil, nil, nil, true))
						}
					}
				}
			}
		}
	}

	return g.mergeSQL(e)
}

// buildJSONExtractPath returns a builder for kind(json, path...) that turns literal path
// arguments into a JSON path: integers become subscripts (shifted when !zeroBasedIndexing),
// other values keys. Non-literal path arguments fall back to FromArgList. jsonType "" means
// unset.
//
// Only the first two arguments are kept so argument-count validation passes; WithValidateArgs
// reports the truncated list to the caller.
func buildJSONExtractPath(
	kind Kind,
	zeroBasedIndexing bool, /*=true*/
	arrowReqJSONType bool, /*=false*/
	jsonType string, /*=""*/
) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		segments := []*Expr{New(KJSONPathRoot)}
		if len(args) > 1 {
			for _, arg := range args[1:] {
				if !arg.IsA(KLiteral) {
					// We use the fallback parser because we can't really transpile non-literals safely
					var rest any
					if len(args) > 2 {
						rest = args[2:]
					}
					return New(kind, "this", seqGet(args, 0), "expression", seqGet(args, 1), "expressions", rest)
				}

				text := arg.Name()
				if isPyInt(text) && (!arrowReqJSONType || !arg.IsString()) {
					index := dhPyIntValue(text)
					if !zeroBasedIndexing {
						index--
					}
					segments = append(segments, New(KJSONPathSubscript, "this", index))
				} else {
					segments = append(segments, New(KJSONPathKey, "this", text))
				}
			}
		}

		// This is done to avoid failing in the expression validator due to the arg count
		if len(args) > 2 {
			args = args[:2]
		}
		kv := []any{
			"this", seqGet(args, 0),
			"expression", New(KJSONPath, "expressions", segments),
		}

		isJSONB := kind.IsA(KJSONBExtract, KJSONBExtractScalar)
		if !isJSONB {
			kv = append(kv, "only_json_types", arrowReqJSONType)
		}

		if jsonType != "" {
			kv = append(kv, "json_type", jsonType)
		}

		return WithValidateArgs(New(kind, kv...), args)
	}
}

// jsonExtractSegments returns a GenFunc generating name(json, 'seg1', ...) from a JSON path, or
// json op 'seg1' op ... when op is set ("" means none). quotedIndex quotes subscripts too;
// other path arguments go through renameFunc.
func jsonExtractSegments(name string, quotedIndex bool /*=true*/, op string /*=""*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		path := e.Expression()
		if !path.IsA(KJSONPath) {
			return renameFunc(name)(g, e)
		}

		var segments []string
		for _, segment := range path.Expressions() {
			p := g.sql(segment)
			if p != "" {
				if segment.IsA(KJSONPathPart) && (quotedIndex || !segment.IsA(KJSONPathSubscript)) {
					p = g.d.S.QUOTE_START + g.escapeStr(p, true, "", "", false) + g.d.S.QUOTE_END
				}

				segments = append(segments, p)
			}
		}

		if op != "" {
			return strings.Join(append([]string{g.sql(e.Arg("this"))}, segments...), " "+op+" ")
		}
		args := []any{e.This()}
		for _, s := range segments {
			args = append(args, s)
		}
		return g.fn(name, args...)
	}
}

func jsonPathKeyOnlyName(g *Generator, e *Expr) string {
	if e.This().IsA(KJSONPathWildcard) {
		g.unsupported("Unsupported wildcard in JSONPathKey expression")
	}

	return e.Name()
}

// filterArrayUsingUnnest generates an array filter (or ARRAY_REMOVE) as
// ARRAY(SELECT x FROM UNNEST(array) AS (x) WHERE cond), with x the lambda parameter or _u.
func filterArrayUsingUnnest(g *Generator, e *Expr) string {
	cond := e.Expression()
	var alias any
	if cond.IsA(KLambda) && len(cond.Expressions()) == 1 {
		alias = cond.Expressions()[0]
		cond = cond.This()
	} else if cond.IsA(KPredicate) {
		alias = "_u"
	} else if e.IsA(KArrayRemove) {
		alias = "_u"
		cond = New(KNEQ, "this", alias, "expression", e.Expression())
	} else {
		g.unsupported("Unsupported filter condition")
		return ""
	}

	unnest := New(KUnnest, "expressions", []*Expr{e.This()})
	var selectAlias *Expr
	if a, ok := alias.(*Expr); ok {
		selectAlias = a
	} else {
		selectAlias = MaybeParse(alias.(string), KExpr, "", nil)
	}
	filtered := SelectExpr(selectAlias).
		SelectFrom(AliasTableExpr(unnest, nil, []any{alias}, nil, true), true).
		QueryWhere([]*Expr{cond}, true, true)
	return g.sql(New(KArray, "expressions", []*Expr{filtered}))
}

func arrayCompactSQL(g *Generator, e *Expr) string {
	lambdaID := ToIdentifier("_u", nil)
	cond := NotExpr(New(KIs, "this", lambdaID, "expression", Null()), true)
	return g.sql(New(
		KArrayFilter,
		"this", e.This(),
		"expression", New(KLambda, "this", cond, "expressions", []*Expr{lambdaID}),
	))
}

func removeFromArrayUsingFilter(g *Generator, e *Expr) string {
	lambdaID := ToIdentifier("_u", nil)
	cond := New(KNEQ, "this", lambdaID, "expression", e.Expression())

	filterSQL := g.sql(New(
		KArrayFilter,
		"this", e.This(),
		"expression", New(KLambda, "this", cond, "expressions", []*Expr{lambdaID}),
	))

	sourceNullPropagation := e.ArgB("null_propagation")
	targetNullPropagation := g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS

	// Source propagates NULLs (Snowflake), target doesn't (DuckDB):
	// When removal value is NULL, return NULL instead of applying filter
	if sourceNullPropagation && !targetNullPropagation {
		removalValue := e.Expression()

		// Optimization: skip wrapper if removal value is a non-NULL literal
		// (e.g., 5, 'a', TRUE) or an array literal (e.g., [1, 2])
		if (removalValue.IsA(KLiteral) && !removalValue.IsA(KNull)) || removalValue.IsA(KArray) {
			return filterSQL
		}

		return g.sql(New(
			KIf,
			"this", New(KIs, "this", removalValue, "expression", New(KNull)),
			"true", New(KNull),
			"false", filterSQL,
		))
	}

	return filterSQL
}

func buildDefaultDecimalType(precision int /*=-1 (unset)*/, scale int /*=-1 (unset)*/) typeConverterFn {
	return func(dtype *Expr) *Expr {
		if len(dtype.Expressions()) > 0 || precision < 0 {
			return dtype
		}

		params := strconv.Itoa(precision)
		if scale >= 0 {
			params += ", " + strconv.Itoa(scale)
		}
		return DataTypeFromStr("DECIMAL("+params+")", MustDialect(""), false)
	}
}

func sha256SQL(g *Generator, e *Expr) string {
	length := e.Text("length")
	if length == "" {
		length = "256"
	}
	return g.fn("SHA"+length, e.Arg("this"))
}

func sha2DigestSQL(g *Generator, e *Expr) string {
	length := e.Text("length")
	if length == "" {
		length = "256"
	}
	return g.fn("SHA"+length, e.Arg("this"))
}

// sequenceSQL generates SEQUENCE(start, end[, step]). A DATE/TIMESTAMP cast on one bound is applied
// to the other; an end-exclusive sequence becomes an empty array or SEQUENCE(start, end - step).
func sequenceSQL(g *Generator, e *Expr) string {
	start := e.ArgE("start")
	end := e.ArgE("end")
	step := e.ArgE("step")

	var targetType *Expr
	if start.IsA(KCast) {
		targetType = start.ArgE("to")
	} else if end.IsA(KCast) {
		targetType = end.ArgE("to")
	}

	if start != nil && end != nil {
		if targetType != nil && dhIsType(targetType, DT_DATE, DT_TIMESTAMP) {
			if start.IsA(KCast) && targetType == start.ArgE("to") {
				end = CastExpr(end, targetType, true, nil)
			} else {
				start = CastExpr(start, targetType, true, nil)
			}
		}

		if e.ArgB("is_end_exclusive") {
			stepValue := step
			if stepValue == nil {
				stepValue = LiteralInt(1)
			}
			end = ParenExpr(New(KSub, "this", end, "expression", stepValue), false)

			var seqArgs []*Expr
			for _, x := range []*Expr{start, end, step} {
				if x != nil {
					seqArgs = append(seqArgs, x)
				}
			}
			sequenceCall := New(KAnonymous, "this", "SEQUENCE", "expressions", seqArgs)
			zero := LiteralInt(0)
			shouldReturnEmpty := OrExpr(
				New(KEQ, "this", stepValue.Copy(), "expression", zero.Copy()),
				AndExpr(
					New(KGT, "this", stepValue.Copy(), "expression", zero.Copy()),
					New(KGT, "this", start.Copy(), "expression", end.Copy()),
				),
				AndExpr(
					New(KLT, "this", stepValue.Copy(), "expression", zero.Copy()),
					New(KLT, "this", start.Copy(), "expression", end.Copy()),
				),
			)
			emptyArrayOrSequence := New(
				KIf,
				"this", shouldReturnEmpty,
				"true", New(KArray, "expressions", []*Expr{}),
				"false", sequenceCall,
			)
			return g.sql(g.simplifyUnlessLiteral(emptyArrayOrSequence))
		}
	}

	return g.fn("SEQUENCE", start, end, step)
}

// dialectBuildLike has a `dialect` prefix because the parser's buildLike already exists.
func dialectBuildLike(kind Kind, notLike bool /*=false*/) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		likeExpr := New(kind, "this", seqGet(args, 0), "expression", seqGet(args, 1))

		if escape := seqGet(args, 2); escape != nil {
			likeExpr = New(KEscape, "this", likeExpr, "expression", escape)
		}

		if notLike {
			likeExpr = New(KNot, "this", likeExpr)
		}

		return likeExpr
	}
}

func buildRegexpExtract(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		// The "position" argument specifies the index of the string character to start matching from.
		// `null_if_pos_overflow` reflects the dialect's behavior when position is greater than the string
		// length. If true, returns NULL. If false, returns an empty string. `null_if_pos_overflow` is
		// only needed for KRegexpExtract - KRegexpExtractAll always returns an empty array if
		// position overflows.
		group := seqGet(args, 2)
		if group == nil {
			group = LiteralInt(d.S.REGEXP_EXTRACT_DEFAULT_GROUP)
		}
		kv := []any{
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"group", group,
			"parameters", seqGet(args, 3),
		}
		if kind == KRegexpExtract {
			kv = append(kv, "null_if_pos_overflow", d.S.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL)
		}
		return New(kind, kv...)
	}
}

// explodeToUnnestSQL generates LATERAL VIEW EXPLODE as CROSS JOIN UNNEST, and POSEXPLODE as a
// CROSS JOIN of a LATERAL subquery over UNNEST WITH ORDINALITY; other laterals use lateralSQL.
func explodeToUnnestSQL(g *Generator, e *Expr) string {
	this := e.This()
	alias := e.ArgE("alias")

	var crossJoinExpr *Expr
	if this.IsA(KPosexplode) && alias != nil {
		// Spark's `FROM x LATERAL VIEW POSEXPLODE(y) t AS pos, col` has the following semantics:
		// - The first column is the position and the rest (1 for array, 2 for maps) are the exploded values
		// - The position is 0-based whereas WITH ORDINALITY is 1-based
		// For that matter, we must (1) subtract 1 from the ORDINALITY position and (2) rearrange the columns accordingly, returning:
		// `FROM x CROSS JOIN LATERAL (SELECT pos - 1 AS pos, col FROM UNNEST(y) WITH ORDINALITY AS t(col, pos))
		columns := alias.ArgL("columns")
		pos, cols := columns[0], append([]*Expr{}, columns[1:]...)

		selects := append([]*Expr{AliasExpr(dhBinop(KSub, pos, 1), pos, nil, true)}, cols...)
		lateralSubquery := SelectExpr(selects...).SelectFrom(New(
			KUnnest,
			"expressions", []*Expr{this.This()},
			"offset", true,
			"alias", New(KTableAlias, "this", alias.This(), "columns", append(append([]*Expr{}, cols...), pos)),
		), true)

		crossJoinExpr = New(KLateral, "this", lateralSubquery.QuerySubquery(nil, true))
	} else if this.IsA(KExplode) {
		crossJoinExpr = New(KUnnest, "expressions", []*Expr{this.This()}, "alias", alias)
	}

	if crossJoinExpr != nil {
		return g.sql(New(KJoin, "this", crossJoinExpr, "kind", "cross"))
	}

	return g.lateralSQL(e)
}

func timestampdiffSQL(g *Generator, e *Expr) string {
	return g.fn("TIMESTAMPDIFF", e.Arg("unit"), e.Arg("expression"), e.Arg("this"))
}

func noMakeIntervalSQL(g *Generator, e *Expr, sep string /*=", "*/) string {
	var args []any
	for _, a := range e.args {
		value := a.val
		if v, ok := value.(*Expr); ok && v.IsA(KKwarg) {
			value = v.Expression()
		}

		if chunkAIsNone(value) {
			continue
		}

		var valueSQL string
		if v, ok := value.(*Expr); ok && v.IsString() {
			valueSQL = v.Name()
		} else {
			valueSQL = g.sql(value)
		}
		args = append(args, valueSQL+" "+a.key)
	}

	return "INTERVAL " + g.sql(LiteralString(g.formatArgs(sep, args...)))
}

func lengthOrCharLengthSQL(g *Generator, e *Expr) string {
	lengthFunc := "CHAR_LENGTH"
	if e.ArgB("binary") {
		lengthFunc = "LENGTH"
	}
	return g.fn(lengthFunc, e.Arg("this"))
}

// groupconcatSQL generates GROUP_CONCAT as funcName(this, sep[ ON OVERFLOW ...]), with the
// ordering as WITHIN GROUP (ORDER BY ...) or inside the call, followed by any LIMIT. sep is
// the default separator ("" means none).
func groupconcatSQL(
	g *Generator,
	e *Expr,
	funcName string, /*="LISTAGG"*/
	sep string, /*=","*/
	withinGroup bool, /*=true*/
	onOverflow bool, /*=false*/
) string {
	this := e.This()
	var sepExpr any = e.Arg("separator")
	if !truthy(sepExpr) {
		sepExpr = nil
		if sep != "" {
			sepExpr = LiteralString(sep)
		}
	}
	separator := g.sql(sepExpr)

	onOverflowSQL := g.sqlKey(e, "on_overflow")
	if onOverflow && onOverflowSQL != "" {
		onOverflowSQL = " ON OVERFLOW " + onOverflowSQL
	} else {
		onOverflowSQL = ""
	}

	var limit *Expr
	if this.IsA(KLimit) && this.This() != nil {
		limit = this
		this = limit.This().Pop()
	}

	order := this.Find(KOrder)

	if order != nil && order.This() != nil {
		this = order.This().Pop()
	}

	var sepArg any
	if separator != "" || onOverflowSQL != "" {
		sepArg = separator + onOverflowSQL
	}
	args := g.formatArgs(", ", this, sepArg)

	// The arguments are kept as raw SQL; a Var renders its text verbatim.
	listagg := New(KAnonymous, "this", funcName, "expressions", []*Expr{dhRawSQL(args)})

	modifiers := g.sql(limit)

	if order != nil {
		if withinGroup {
			listagg = New(KWithinGroup, "this", listagg, "expression", order)
		} else {
			modifiers = g.sql(order) + modifiers
		}
	}

	if modifiers != "" {
		listagg.Set("expressions", []*Expr{dhRawSQL(args + modifiers)})
	}

	return g.sql(listagg)
}

// dhRawSQL wraps a raw SQL string so it can be stored in an expression list; Var renders its
// `this` verbatim.
func dhRawSQL(s string) *Expr { return New(KVar, "this", s) }

func buildTimetostrOrTochar(args []*Expr, d *Dialect) *Expr {
	if len(args) == 2 {
		this := args[0]
		if this.Type() == nil {
			annotateTypes(this, d)
		}

		if dhIsType(this, DataType_TEMPORAL_TYPES.Items()...) {
			return buildFormattedTime(KTimeToStr, "", true)(args, d)
		}
	}

	return FromArgList(KToChar, args)
}

func buildReplaceWithOptionalReplacement(args []*Expr, d *Dialect) *Expr {
	replacement := seqGet(args, 2)
	if replacement == nil {
		replacement = LiteralString("")
	}
	return New(KReplace, "this", seqGet(args, 0), "expression", seqGet(args, 1), "replacement", replacement)
}

func regexpReplaceGlobalModifier(e *Expr) *Expr {
	modifiers := e.ArgE("modifiers")
	singleReplace := e.ArgB("single_replace")
	occurrence := e.ArgE("occurrence")

	if !singleReplace && (occurrence == nil || (occurrence.IsInt() && dhIsZero(occurrence))) {
		if modifiers == nil || modifiers.IsString() {
			// Append 'g' to the modifiers if they are not provided since
			// the semantics of REGEXP_REPLACE from the input dialect
			// is to replace all occurrences of the pattern.
			value := ""
			if modifiers != nil {
				value = modifiers.Name()
			}
			modifiers = LiteralString(value + "g")
		}
	}

	return modifiers
}

func dhIsZero(e *Expr) bool {
	v, _ := e.toPyNumber()
	return v != nil && v.Sign() == 0
}

// getbitSQL generates SQL for Getbit according to DuckDB and Postgres, transpiling it if either:
// 1. The zero index corresponds to the least-significant bit
// 2. The input type is an integer value.
func getbitSQL(g *Generator, e *Expr) string {
	value := e.This()
	position := e.Expression()

	intTypes := append(DataType_SIGNED_INTEGER_TYPES.Items(), DataType_UNSIGNED_INTEGER_TYPES.Items()...)
	if !e.ArgB("zero_is_msb") && dhIsType(e, intTypes...) {
		shifted := New(KBitwiseRightShift, "this", value, "expression", position)
		masked := New(KBitwiseAnd, "this", shifted, "expression", LiteralInt(1))
		return g.sql(masked)
	}

	return g.fn("GET_BIT", value, position)
}

// jarowinklerSimilarity returns a GenFunc generating fn(a, b), uppercasing both sides for a
// case-insensitive comparison.
func jarowinklerSimilarity(fn string) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := e.This()
		expr := e.Expression()
		if e.ArgB("case_insensitive") {
			this = New(KUpper, "this", this)
			expr = New(KUpper, "this", expr)
		}

		return g.fn(fn, this, expr)
	}
}

// dhPyStr formats an argument value for messages and SQL text: nil as None, booleans as
// True/False, expressions as default-dialect SQL, DTypes as DType.NAME.
func dhPyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case *Expr:
		if x == nil {
			return "None"
		}
		return exprSQL(x)
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

// dhPyIntValue parses a string accepted by isPyInt (surrounding whitespace and underscores
// allowed). Anything else panics with a ValueError whose message keeps its historical wording.
func dhPyIntValue(text string) int {
	s := strings.ReplaceAll(pyStrip(text), "_", "")
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(&ValueError{Msg: "invalid literal for int() with base 10: " + pyRepr(text)})
	}
	return n
}

func dhIsType(e *Expr, dtypes ...DType) bool {
	args := make([]any, len(dtypes))
	for i, d := range dtypes {
		args[i] = d
	}
	return dhIsTypeAny(e, args...)
}

func dhIsTypeAny(e *Expr, dtypes ...any) bool {
	if e == nil {
		return false
	}
	switch propOwner_is_type[e.kind] {
	case KDataType:
		return DataTypeIsType(e, dtypes, false)
	case KCast:
		to := e.ArgE("to")
		return to != nil && DataTypeIsType(to, dtypes, false)
	}
	t := e.RawType()
	return t != nil && DataTypeIsType(t, dtypes, false)
}

// dhConvert converts a value (expression, string, bool, int or nil) into an expression; other
// types panic with a ValueError.
func dhConvert(v any, copy bool) *Expr {
	switch x := v.(type) {
	case *Expr:
		if x == nil {
			return Null()
		}
		if copy {
			return x.Copy()
		}
		return x
	case string:
		return LiteralString(x)
	case bool:
		return Boolean(x)
	case nil:
		return Null()
	case int:
		return LiteralInt(x)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Cannot convert %v", v)})
}

// dhBinop builds a binary node of klass over a copy of self and the converted other,
// parenthesizing binary operands unless one of them already is of klass.
func dhBinop(klass Kind, self *Expr, other any) *Expr {
	this := self.Copy()
	o := dhConvert(other, true)
	if !this.IsA(klass) && !o.IsA(klass) {
		this = wrapIfKind(this, KBinary)
		o = wrapIfKind(o, KBinary)
	}
	return New(klass, "this", this, "expression", o)
}

// dhFunc builds a function call in the default dialect (args: *Expr, string or int; strings and
// ints are parsed as SQL): a known function through its builder, otherwise an anonymous one. A
// known function needs at least one argument; validation errors panic with a ValueError.
func dhFunc(name string, args ...any) *Expr {
	d := MustDialect("")

	converted := make([]*Expr, 0, len(args))
	for _, a := range args {
		switch x := a.(type) {
		case *Expr:
			converted = append(converted, maybeParseExpr(x, true))
		case string:
			converted = append(converted, MaybeParse(x, KNone, "", d))
		default:
			converted = append(converted, MaybeParse(dhPyStr(x), KNone, "", d))
		}
	}

	var function *Expr
	var constructor FuncBuilder
	if d.P != nil {
		constructor = d.P.FUNCTIONS[pyUpper(name)]
	}
	if constructor != nil && len(converted) > 0 {
		function, converted = callFuncBuilder(constructor, converted, d)
	} else if constructor != nil {
		panic(&ValueError{Msg: fmt.Sprintf("Unable to convert '%s' into a Func. Either manually construct the Func expression of interest or parse the function call.", name)})
	} else {
		function = New(KAnonymous, "this", name, "expressions", converted)
	}

	for _, msg := range function.ErrorMessages(converted) {
		panic(&ValueError{Msg: msg})
	}

	return function
}

// dhApplyIndexOffset adds offset to a single integer subscript of an array (or untyped)
// expression and simplifies it. Other subscripts are returned unchanged.
func dhApplyIndexOffset(this *Expr, expressions []*Expr, offset int, d *Dialect) []*Expr {
	if offset == 0 || len(expressions) != 1 {
		return expressions
	}

	expression := expressions[0]

	if this.Type() == nil {
		annotateTypes(this, d)
	}

	if t := this.Type().DTypeOf(); t != DT_UNKNOWN && t != DT_ARRAY {
		return expressions
	}

	if expression.Type() == nil {
		annotateTypes(expression, d)
	}

	if DataType_INTEGER_TYPES.Has(expression.Type().DTypeOf()) {
		expression = simplifyExpr(dhBinop(KAdd, expression, offset), MustDialect(""))
		return []*Expr{expression}
	}

	return expressions
}

// dhSubsecondPrecision returns the subsecond precision of a timestamp literal: 6 for
// microseconds, 3 for milliseconds, 0 for none or an invalid literal.
func dhSubsecondPrecision(timestampLiteral string) int {
	microsecond, ok := dhFromISOFormatMicrosecond(timestampLiteral)
	if !ok {
		return 0
	}
	subsecondDigitCount := len(strings.TrimRight(fmt.Sprintf("%06d", microsecond), "0"))
	precision := 0
	if subsecondDigitCount > 3 {
		precision = 6
	} else if subsecondDigitCount > 0 {
		precision = 3
	}
	return precision
}

// dhFromISOFormatMicrosecond strictly parses an ISO-8601 date or date-time (calendar or week
// dates, optional time with fraction and UTC offset) and returns its microsecond; ok=false
// for invalid input.
func dhFromISOFormatMicrosecond(s string) (int, bool) {
	r := []rune(s)
	n := len(r)
	if n < 7 {
		return 0, false
	}
	at := func(i int) rune {
		if i >= 0 && i < n {
			return r[i]
		}
		return 0
	}
	isDigit := func(c rune) bool { return c >= '0' && c <= '9' }
	parseDigits := func(p, num int) (int, int, bool) {
		v := 0
		for i := range num {
			c := at(p + i)
			if !isDigit(c) {
				return 0, p, false
			}
			v = v*10 + int(c-'0')
		}
		return v, p + num, true
	}

	var sep int
	if n == 7 {
		sep = 7
	} else if at(4) == '-' {
		if at(5) == 'W' {
			if n < 8 {
				return 0, false
			}
			if n > 8 && at(8) == '-' {
				if n == 9 {
					return 0, false
				}
				if n > 10 && isDigit(at(10)) {
					sep = 8
				} else {
					sep = 10
				}
			} else {
				sep = 8
			}
		} else {
			sep = 10
		}
	} else if at(4) == 'W' {
		idx := 7
		for ; idx < n; idx++ {
			if !isDigit(at(idx)) {
				break
			}
		}
		if idx < 9 {
			sep = idx
		} else if idx%2 == 0 {
			sep = 7
		} else {
			sep = 8
		}
	} else {
		sep = 8
	}

	year, p, ok := parseDigits(0, 4)
	if !ok {
		return 0, false
	}
	usesSeparator := at(p) == '-'
	if usesSeparator {
		p++
	}
	var month, day int
	if at(p) == 'W' {
		p++
		isoWeek, np, ok := parseDigits(p, 2)
		if !ok {
			return 0, false
		}
		p = np
		isoDay := 1
		if p < sep {
			if usesSeparator {
				if at(p) != '-' {
					return 0, false
				}
				p++
			}
			isoDay, _, ok = parseDigits(p, 1)
			if !ok {
				return 0, false
			}
		}
		if !dhISOToYMD(year, isoWeek, isoDay) {
			return 0, false
		}
	} else {
		month, p, ok = parseDigits(p, 2)
		if !ok {
			return 0, false
		}
		if usesSeparator {
			if at(p) != '-' {
				return 0, false
			}
			p++
		}
		day, _, ok = parseDigits(p, 2)
		if !ok {
			return 0, false
		}
		if year < 1 || month < 1 || month > 12 || day < 1 || day > dhDaysInMonth(year, month) {
			return 0, false
		}
	}

	if n <= sep {
		return 0, true
	}

	start := sep + 1
	end := n
	tzPos := start
	for {
		c := at(tzPos)
		if c == 'Z' || c == '+' || c == '-' {
			break
		}
		tzPos++
		if tzPos >= end {
			break
		}
	}

	hour, minute, second, microsecond, rv := dhParseHHMMSSFF(at, start, tzPos)
	if rv < 0 {
		return 0, false
	}
	if tzPos == end {
		if rv == 1 {
			return 0, false
		}
	} else if at(tzPos) == 'Z' {
		if at(tzPos+1) != 0 {
			return 0, false
		}
	} else {
		tzh, tzm, tzs, tzus, rv := dhParseHHMMSSFF(at, tzPos+1, end)
		if rv != 0 {
			return 0, false
		}
		// timezone offsets must be strictly between -24h and 24h
		total := ((tzh*3600+tzm*60+tzs)*1000000 + tzus)
		if total >= 24*3600*1000000 {
			return 0, false
		}
	}

	if hour > 23 || minute > 59 || second > 59 || microsecond > 999999 {
		return 0, false
	}
	return microsecond, true
}

// dhParseHHMMSSFF parses hh[:mm[:ss[.ffffff]]] (separators optional) over runes [p, pEnd).
// Returns rv < 0 on error, 1 when trailing characters remain and 0 on a clean parse.
func dhParseHHMMSSFF(at func(int) rune, p, pEnd int) (hour, minute, second, microsecond, rv int) {
	isDigit := func(c rune) bool { return c >= '0' && c <= '9' }
	vals := [3]int{}
	hasSeparator := true
	for i := range 3 {
		v := 0
		for j := range 2 {
			c := at(p + j)
			if !isDigit(c) {
				return 0, 0, 0, 0, -3
			}
			v = v*10 + int(c-'0')
		}
		vals[i] = v
		p += 2

		c := at(p)
		p++
		if i == 0 {
			hasSeparator = c == ':'
		}

		if p >= pEnd {
			if c != 0 {
				return vals[0], vals[1], vals[2], 0, 1
			}
			return vals[0], vals[1], vals[2], 0, 0
		} else if hasSeparator && c == ':' {
			if i == 2 {
				return 0, 0, 0, 0, -4
			}
			continue
		} else if c == '.' || c == ',' {
			break
		} else if !hasSeparator {
			p--
		} else {
			return 0, 0, 0, 0, -4
		}
	}

	lenRemains := pEnd - p
	toParse := lenRemains
	if lenRemains >= 6 {
		toParse = 6
	}
	us := 0
	for j := range toParse {
		c := at(p + j)
		if !isDigit(c) {
			return 0, 0, 0, 0, -3
		}
		us = us*10 + int(c-'0')
	}
	p += toParse
	correction := []int{100000, 10000, 1000, 100, 10}
	if toParse < 6 && toParse > 0 {
		us *= correction[toParse-1]
	}
	for isDigit(at(p)) {
		p++ // skip truncated digits
	}
	if at(p) != 0 {
		return vals[0], vals[1], vals[2], us, 1
	}
	return vals[0], vals[1], vals[2], us, 0
}

func dhDaysInMonth(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// dhISOToYMD validates an ISO calendar date (year 1-9999, week 1-52 or 53 in long years, day
// 1-7) and checks that the resulting date is in range.
func dhISOToYMD(isoYear, isoWeek, isoDay int) bool {
	if isoYear < 1 || isoYear > 9999 {
		return false
	}
	jan1 := time.Date(isoYear, 1, 1, 0, 0, 0, 0, time.UTC)
	if isoWeek <= 0 || isoWeek >= 53 {
		outOfRange := true
		if isoWeek == 53 {
			// ISO years have 53 weeks in them on years starting with a Thursday
			// and leap years starting on a Wednesday
			leap := isoYear%4 == 0 && (isoYear%100 != 0 || isoYear%400 == 0)
			if jan1.Weekday() == time.Thursday || (jan1.Weekday() == time.Wednesday && leap) {
				outOfRange = false
			}
		}
		if outOfRange {
			return false
		}
	}
	if isoDay <= 0 || isoDay >= 8 {
		return false
	}
	firstWeekday := (int(jan1.Weekday()) + 6) % 7 // Monday == 0
	week1Monday := jan1.AddDate(0, 0, -firstWeekday)
	if firstWeekday > 3 {
		week1Monday = week1Monday.AddDate(0, 0, 7)
	}
	result := week1Monday.AddDate(0, 0, (isoWeek-1)*7+(isoDay-1))
	return result.Year() >= 1 && result.Year() <= 9999
}

// dhTIMEZONES is the set of known time zone names (lowercased IANA names).
var dhTIMEZONES = newStrSet(
	"africa/abidjan", "africa/accra", "africa/addis_ababa", "africa/algiers", "africa/asmara", "africa/asmera",
	"africa/bamako", "africa/bangui", "africa/banjul", "africa/bissau", "africa/blantyre", "africa/brazzaville",
	"africa/bujumbura", "africa/cairo", "africa/casablanca", "africa/ceuta", "africa/conakry", "africa/dakar",
	"africa/dar_es_salaam", "africa/djibouti", "africa/douala", "africa/el_aaiun", "africa/freetown",
	"africa/gaborone", "africa/harare", "africa/johannesburg", "africa/juba", "africa/kampala",
	"africa/khartoum", "africa/kigali", "africa/kinshasa", "africa/lagos", "africa/libreville", "africa/lome",
	"africa/luanda", "africa/lubumbashi", "africa/lusaka", "africa/malabo", "africa/maputo", "africa/maseru",
	"africa/mbabane", "africa/mogadishu", "africa/monrovia", "africa/nairobi", "africa/ndjamena",
	"africa/niamey", "africa/nouakchott", "africa/ouagadougou", "africa/porto-novo", "africa/sao_tome",
	"africa/timbuktu", "africa/tripoli", "africa/tunis", "africa/windhoek", "america/adak", "america/anchorage",
	"america/anguilla", "america/antigua", "america/araguaina", "america/argentina/buenos_aires",
	"america/argentina/catamarca", "america/argentina/comodrivadavia", "america/argentina/cordoba",
	"america/argentina/jujuy", "america/argentina/la_rioja", "america/argentina/mendoza",
	"america/argentina/rio_gallegos", "america/argentina/salta", "america/argentina/san_juan",
	"america/argentina/san_luis", "america/argentina/tucuman", "america/argentina/ushuaia", "america/aruba",
	"america/asuncion", "america/atikokan", "america/atka", "america/bahia", "america/bahia_banderas",
	"america/barbados", "america/belem", "america/belize", "america/blanc-sablon", "america/boa_vista",
	"america/bogota", "america/boise", "america/buenos_aires", "america/cambridge_bay", "america/campo_grande",
	"america/cancun", "america/caracas", "america/catamarca", "america/cayenne", "america/cayman",
	"america/chicago", "america/chihuahua", "america/ciudad_juarez", "america/coral_harbour", "america/cordoba",
	"america/costa_rica", "america/creston", "america/cuiaba", "america/curacao", "america/danmarkshavn",
	"america/dawson", "america/dawson_creek", "america/denver", "america/detroit", "america/dominica",
	"america/edmonton", "america/eirunepe", "america/el_salvador", "america/ensenada", "america/fort_nelson",
	"america/fort_wayne", "america/fortaleza", "america/glace_bay", "america/godthab", "america/goose_bay",
	"america/grand_turk", "america/grenada", "america/guadeloupe", "america/guatemala", "america/guayaquil",
	"america/guyana", "america/halifax", "america/havana", "america/hermosillo", "america/indiana/indianapolis",
	"america/indiana/knox", "america/indiana/marengo", "america/indiana/petersburg",
	"america/indiana/tell_city", "america/indiana/vevay", "america/indiana/vincennes",
	"america/indiana/winamac", "america/indianapolis", "america/inuvik", "america/iqaluit", "america/jamaica",
	"america/jujuy", "america/juneau", "america/kentucky/louisville", "america/kentucky/monticello",
	"america/knox_in", "america/kralendijk", "america/la_paz", "america/lima", "america/los_angeles",
	"america/louisville", "america/lower_princes", "america/maceio", "america/managua", "america/manaus",
	"america/marigot", "america/martinique", "america/matamoros", "america/mazatlan", "america/mendoza",
	"america/menominee", "america/merida", "america/metlakatla", "america/mexico_city", "america/miquelon",
	"america/moncton", "america/monterrey", "america/montevideo", "america/montreal", "america/montserrat",
	"america/nassau", "america/new_york", "america/nipigon", "america/nome", "america/noronha",
	"america/north_dakota/beulah", "america/north_dakota/center", "america/north_dakota/new_salem",
	"america/nuuk", "america/ojinaga", "america/panama", "america/pangnirtung", "america/paramaribo",
	"america/phoenix", "america/port-au-prince", "america/port_of_spain", "america/porto_acre",
	"america/porto_velho", "america/puerto_rico", "america/punta_arenas", "america/rainy_river",
	"america/rankin_inlet", "america/recife", "america/regina", "america/resolute", "america/rio_branco",
	"america/rosario", "america/santa_isabel", "america/santarem", "america/santiago", "america/santo_domingo",
	"america/sao_paulo", "america/scoresbysund", "america/shiprock", "america/sitka", "america/st_barthelemy",
	"america/st_johns", "america/st_kitts", "america/st_lucia", "america/st_thomas", "america/st_vincent",
	"america/swift_current", "america/tegucigalpa", "america/thule", "america/thunder_bay", "america/tijuana",
	"america/toronto", "america/tortola", "america/vancouver", "america/virgin", "america/whitehorse",
	"america/winnipeg", "america/yakutat", "america/yellowknife", "antarctica/casey", "antarctica/davis",
	"antarctica/dumontdurville", "antarctica/macquarie", "antarctica/mawson", "antarctica/mcmurdo",
	"antarctica/palmer", "antarctica/rothera", "antarctica/south_pole", "antarctica/syowa", "antarctica/troll",
	"antarctica/vostok", "arctic/longyearbyen", "asia/aden", "asia/almaty", "asia/amman", "asia/anadyr",
	"asia/aqtau", "asia/aqtobe", "asia/ashgabat", "asia/ashkhabad", "asia/atyrau", "asia/baghdad",
	"asia/bahrain", "asia/baku", "asia/bangkok", "asia/barnaul", "asia/beirut", "asia/bishkek", "asia/brunei",
	"asia/calcutta", "asia/chita", "asia/choibalsan", "asia/chongqing", "asia/chungking", "asia/colombo",
	"asia/dacca", "asia/damascus", "asia/dhaka", "asia/dili", "asia/dubai", "asia/dushanbe", "asia/famagusta",
	"asia/gaza", "asia/harbin", "asia/hebron", "asia/ho_chi_minh", "asia/hong_kong", "asia/hovd",
	"asia/irkutsk", "asia/istanbul", "asia/jakarta", "asia/jayapura", "asia/jerusalem", "asia/kabul",
	"asia/kamchatka", "asia/karachi", "asia/kashgar", "asia/kathmandu", "asia/katmandu", "asia/khandyga",
	"asia/kolkata", "asia/krasnoyarsk", "asia/kuala_lumpur", "asia/kuching", "asia/kuwait", "asia/macao",
	"asia/macau", "asia/magadan", "asia/makassar", "asia/manila", "asia/muscat", "asia/nicosia",
	"asia/novokuznetsk", "asia/novosibirsk", "asia/omsk", "asia/oral", "asia/phnom_penh", "asia/pontianak",
	"asia/pyongyang", "asia/qatar", "asia/qostanay", "asia/qyzylorda", "asia/rangoon", "asia/riyadh",
	"asia/saigon", "asia/sakhalin", "asia/samarkand", "asia/seoul", "asia/shanghai", "asia/singapore",
	"asia/srednekolymsk", "asia/taipei", "asia/tashkent", "asia/tbilisi", "asia/tehran", "asia/tel_aviv",
	"asia/thimbu", "asia/thimphu", "asia/tokyo", "asia/tomsk", "asia/ujung_pandang", "asia/ulaanbaatar",
	"asia/ulan_bator", "asia/urumqi", "asia/ust-nera", "asia/vientiane", "asia/vladivostok", "asia/yakutsk",
	"asia/yangon", "asia/yekaterinburg", "asia/yerevan", "atlantic/azores", "atlantic/bermuda",
	"atlantic/canary", "atlantic/cape_verde", "atlantic/faeroe", "atlantic/faroe", "atlantic/jan_mayen",
	"atlantic/madeira", "atlantic/reykjavik", "atlantic/south_georgia", "atlantic/st_helena",
	"atlantic/stanley", "australia/act", "australia/adelaide", "australia/brisbane", "australia/broken_hill",
	"australia/canberra", "australia/currie", "australia/darwin", "australia/eucla", "australia/hobart",
	"australia/lhi", "australia/lindeman", "australia/lord_howe", "australia/melbourne", "australia/north",
	"australia/nsw", "australia/perth", "australia/queensland", "australia/south", "australia/sydney",
	"australia/tasmania", "australia/victoria", "australia/west", "australia/yancowinna", "brazil/acre",
	"brazil/denoronha", "brazil/east", "brazil/west", "canada/atlantic", "canada/central", "canada/eastern",
	"canada/mountain", "canada/newfoundland", "canada/pacific", "canada/saskatchewan", "canada/yukon", "cet",
	"chile/continental", "chile/easterisland", "cst6cdt", "cuba", "eet", "egypt", "eire", "est", "est5edt",
	"etc/gmt", "etc/gmt+0", "etc/gmt+1", "etc/gmt+10", "etc/gmt+11", "etc/gmt+12", "etc/gmt+2", "etc/gmt+3",
	"etc/gmt+4", "etc/gmt+5", "etc/gmt+6", "etc/gmt+7", "etc/gmt+8", "etc/gmt+9", "etc/gmt-0", "etc/gmt-1",
	"etc/gmt-10", "etc/gmt-11", "etc/gmt-12", "etc/gmt-13", "etc/gmt-14", "etc/gmt-2", "etc/gmt-3", "etc/gmt-4",
	"etc/gmt-5", "etc/gmt-6", "etc/gmt-7", "etc/gmt-8", "etc/gmt-9", "etc/gmt0", "etc/greenwich", "etc/uct",
	"etc/universal", "etc/utc", "etc/zulu", "europe/amsterdam", "europe/andorra", "europe/astrakhan",
	"europe/athens", "europe/belfast", "europe/belgrade", "europe/berlin", "europe/bratislava",
	"europe/brussels", "europe/bucharest", "europe/budapest", "europe/busingen", "europe/chisinau",
	"europe/copenhagen", "europe/dublin", "europe/gibraltar", "europe/guernsey", "europe/helsinki",
	"europe/isle_of_man", "europe/istanbul", "europe/jersey", "europe/kaliningrad", "europe/kiev",
	"europe/kirov", "europe/kyiv", "europe/lisbon", "europe/ljubljana", "europe/london", "europe/luxembourg",
	"europe/madrid", "europe/malta", "europe/mariehamn", "europe/minsk", "europe/monaco", "europe/moscow",
	"europe/nicosia", "europe/oslo", "europe/paris", "europe/podgorica", "europe/prague", "europe/riga",
	"europe/rome", "europe/samara", "europe/san_marino", "europe/sarajevo", "europe/saratov",
	"europe/simferopol", "europe/skopje", "europe/sofia", "europe/stockholm", "europe/tallinn", "europe/tirane",
	"europe/tiraspol", "europe/ulyanovsk", "europe/uzhgorod", "europe/vaduz", "europe/vatican", "europe/vienna",
	"europe/vilnius", "europe/volgograd", "europe/warsaw", "europe/zagreb", "europe/zaporozhye",
	"europe/zurich", "gb", "gb-eire", "gmt", "gmt+0", "gmt-0", "gmt0", "greenwich", "hongkong", "hst",
	"iceland", "indian/antananarivo", "indian/chagos", "indian/christmas", "indian/cocos", "indian/comoro",
	"indian/kerguelen", "indian/mahe", "indian/maldives", "indian/mauritius", "indian/mayotte",
	"indian/reunion", "iran", "israel", "jamaica", "japan", "kwajalein", "libya", "met", "mexico/bajanorte",
	"mexico/bajasur", "mexico/general", "mst", "mst7mdt", "navajo", "nz", "nz-chat", "pacific/apia",
	"pacific/auckland", "pacific/bougainville", "pacific/chatham", "pacific/chuuk", "pacific/easter",
	"pacific/efate", "pacific/enderbury", "pacific/fakaofo", "pacific/fiji", "pacific/funafuti",
	"pacific/galapagos", "pacific/gambier", "pacific/guadalcanal", "pacific/guam", "pacific/honolulu",
	"pacific/johnston", "pacific/kanton", "pacific/kiritimati", "pacific/kosrae", "pacific/kwajalein",
	"pacific/majuro", "pacific/marquesas", "pacific/midway", "pacific/nauru", "pacific/niue", "pacific/norfolk",
	"pacific/noumea", "pacific/pago_pago", "pacific/palau", "pacific/pitcairn", "pacific/pohnpei",
	"pacific/ponape", "pacific/port_moresby", "pacific/rarotonga", "pacific/saipan", "pacific/samoa",
	"pacific/tahiti", "pacific/tarawa", "pacific/tongatapu", "pacific/truk", "pacific/wake", "pacific/wallis",
	"pacific/yap", "poland", "portugal", "prc", "pst8pdt", "roc", "rok", "singapore", "turkey", "uct",
	"universal", "us/alaska", "us/aleutian", "us/arizona", "us/central", "us/east-indiana", "us/eastern",
	"us/hawaii", "us/indiana-starke", "us/michigan", "us/mountain", "us/pacific", "us/samoa", "utc", "w-su",
	"wet", "zulu",
)
