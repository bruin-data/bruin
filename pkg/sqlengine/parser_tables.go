package sqlengine

import (
	"maps"
	"slices"
	"strings"
)

// Port of the module-level builder functions of sqlglot/parser.py (build_var_map, build_like, ...)
// and of the class-level callable tables of sqlglot.parser.Parser (FUNCTIONS, LAMBDAS, ...).

// ---------------------------------------------------------------------------------------------
// Func.from_arg_list
// ---------------------------------------------------------------------------------------------

// FromArgList mirrors Func.from_arg_list: positional args are mapped onto the class arg_types
// in order. If the class has is_var_len_args, the last arg type receives the remaining args as
// a list (possibly empty). Extra args of non var-len functions are dropped (zip semantics).
func FromArgList(kind Kind, args []*Expr) *Expr {
	argTypes := kind.ArgTypes()
	kv := make([]any, 0, 2*len(argTypes))
	if kind.isVarLenArgs() {
		// If this function supports variable length argument treat the last argument as such.
		nonVarLen := argTypes[:len(argTypes)-1]
		numNonVar := len(nonVarLen)
		for i, a := range nonVarLen {
			if i >= len(args) {
				break
			}
			kv = append(kv, a.name, args[i])
		}
		rest := []*Expr{}
		if len(args) > numNonVar {
			rest = append(rest, args[numNonVar:]...)
		}
		kv = append(kv, argTypes[len(argTypes)-1].name, rest)
	} else {
		for i, a := range argTypes {
			if i >= len(args) {
				break
			}
			kv = append(kv, a.name, args[i])
		}
	}
	return New(kind, kv...)
}

// fromArgList returns the FuncBuilder for `Func.from_arg_list` of the given class.
func fromArgList(kind Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr { return FromArgList(kind, args) }
}

// ---------------------------------------------------------------------------------------------
// Module-level builders of sqlglot/parser.py
// ---------------------------------------------------------------------------------------------

// argsFrom mirrors the Python slice args[i:] (always a fresh, non-nil list).
func argsFrom(args []*Expr, i int) []*Expr {
	out := []*Expr{}
	if i < len(args) {
		out = append(out, args[i:]...)
	}
	return out
}

// buildVarMap mirrors build_var_map.
func buildVarMap(args []*Expr) *Expr {
	if len(args) == 1 && args[0].IsStar() {
		return New(KStarMap, "this", args[0])
	}

	keys := []*Expr{}
	values := []*Expr{}
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, args[i])
		if i+1 >= len(args) {
			// Python: args[i + 1] raises IndexError
			panic(&ValueError{Msg: "list index out of range"})
		}
		values = append(values, args[i+1])
	}

	return New(KVarMap, "keys", New(KArray, "expressions", keys), "values", New(KArray, "expressions", values))
}

// buildLike mirrors build_like.
func buildLike(args []*Expr) *Expr {
	like := New(KLike, "this", seqGet(args, 1), "expression", seqGet(args, 0))
	if len(args) > 2 {
		return New(KEscape, "this", like, "expression", seqGet(args, 2))
	}
	return like
}

// binaryRangeParser mirrors binary_range_parser.
func binaryRangeParser(exprType Kind, reverseArgs bool) rangeParseFn {
	return func(p *Parser, this *Expr) *Expr {
		expression := p.parseBitwise()
		if reverseArgs {
			this, expression = expression, this
		}
		return p.parseEscape(p.expression(New(exprType, "this", this, "expression", expression)))
	}
}

// buildLogarithm mirrors build_logarithm.
func buildLogarithm(args []*Expr, d *Dialect) *Expr {
	// Default argument order is base, expression
	this := seqGet(args, 0)
	expression := seqGet(args, 1)

	if expression != nil {
		if d.S.LOG_BASE_FIRST != TriTrue {
			this, expression = expression, this
		}
		return New(KLog, "this", this, "expression", expression)
	}

	if d.P.LOG_DEFAULTS_TO_LN {
		return New(KLn, "this", this)
	}
	return New(KLog, "this", this)
}

// buildHex mirrors build_hex.
func buildHex(args []*Expr, d *Dialect) *Expr {
	arg := seqGet(args, 0)
	if d.S.HEX_LOWERCASE {
		return New(KLowerHex, "this", arg)
	}
	return New(KHex, "this", arg)
}

// buildLower mirrors build_lower.
func buildLower(args []*Expr) *Expr {
	// LOWER(HEX(..)) can be simplified to LowerHex to simplify its transpilation
	arg := seqGet(args, 0)
	if arg.IsA(KHex) {
		return New(KLowerHex, "this", arg.This())
	}
	return New(KLower, "this", arg)
}

// buildUpper mirrors build_upper.
func buildUpper(args []*Expr) *Expr {
	// UPPER(HEX(..)) can be simplified to Hex to simplify its transpilation
	arg := seqGet(args, 0)
	if arg.IsA(KHex) {
		return New(KHex, "this", arg.This())
	}
	return New(KUpper, "this", arg)
}

// buildExtractJSONWithPath mirrors build_extract_json_with_path.
func buildExtractJSONWithPath(exprType Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		expression := New(exprType, "this", seqGet(args, 0), "expression", d.toJSONPath(seqGet(args, 1)))
		if len(args) > 2 && exprType == KJSONExtract {
			expression.Set("expressions", argsFrom(args, 2))
		}
		if exprType == KJSONExtractScalar {
			expression.Set("scalar_only", d.S.JSON_EXTRACT_SCALAR_SCALAR_ONLY)
		}
		return expression
	}
}

// buildMod mirrors build_mod.
func buildMod(args []*Expr) *Expr {
	this := seqGet(args, 0)
	expression := seqGet(args, 1)

	// Wrap the operands if they are binary nodes, e.g. MOD(a + 1, 7) -> (a + 1) % 7
	if this.IsA(KBinary) {
		this = New(KParen, "this", this)
	}
	if expression.IsA(KBinary) {
		expression = New(KParen, "this", expression)
	}

	return New(KMod, "this", this, "expression", expression)
}

// buildPad mirrors build_pad (is_left defaults to True in Python).
func buildPad(args []*Expr, isLeft bool) *Expr {
	return New(
		KPad,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"fill_pattern", seqGet(args, 2),
		"is_left", isLeft,
	)
}

// buildConvertTimezone mirrors build_convert_timezone. defaultSourceTz "" means None.
func buildConvertTimezone(args []*Expr, defaultSourceTz string) *Expr {
	if len(args) == 2 {
		var sourceTz *Expr
		if defaultSourceTz != "" {
			sourceTz = LiteralString(defaultSourceTz)
		}
		return New(
			KConvertTimezone,
			"source_tz", sourceTz,
			"target_tz", seqGet(args, 0),
			"timestamp", seqGet(args, 1),
		)
	}

	return FromArgList(KConvertTimezone, args)
}

// buildTrim mirrors build_trim (Python defaults: is_left=True, reverse_args=False).
func buildTrim(args []*Expr, isLeft bool, reverseArgs bool) *Expr {
	this, expression := seqGet(args, 0), seqGet(args, 1)

	if expression != nil && reverseArgs {
		this, expression = expression, this
	}

	position := "TRAILING"
	if isLeft {
		position = "LEADING"
	}
	return New(KTrim, "this", this, "expression", expression, "position", position)
}

// buildCoalesce mirrors build_coalesce. isNvl / isNull are the Python `bool | None` values
// (pass nil for None).
func buildCoalesce(args []*Expr, isNvl, isNull any) *Expr {
	return New(
		KCoalesce,
		"this", seqGet(args, 0),
		"expressions", argsFrom(args, 1),
		"is_nvl", isNvl,
		"is_null", isNull,
	)
}

// buildLocateStrposition mirrors build_locate_strposition.
func buildLocateStrposition(args []*Expr) *Expr {
	return New(
		KStrPosition,
		"this", seqGet(args, 1),
		"substr", seqGet(args, 0),
		"position", seqGet(args, 2),
	)
}

// buildArrayAppend mirrors build_array_append.
func buildArrayAppend(args []*Expr, d *Dialect) *Expr {
	return New(
		KArrayAppend,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"null_propagation", d.S.ARRAY_FUNCS_PROPAGATES_NULLS,
	)
}

// buildArrayPrepend mirrors build_array_prepend.
func buildArrayPrepend(args []*Expr, d *Dialect) *Expr {
	return New(
		KArrayPrepend,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"null_propagation", d.S.ARRAY_FUNCS_PROPAGATES_NULLS,
	)
}

// buildArrayConcat mirrors build_array_concat.
func buildArrayConcat(args []*Expr, d *Dialect) *Expr {
	return New(
		KArrayConcat,
		"this", seqGet(args, 0),
		"expressions", argsFrom(args, 1),
		"null_propagation", d.S.ARRAY_FUNCS_PROPAGATES_NULLS,
	)
}

// buildArrayRemove mirrors build_array_remove.
func buildArrayRemove(args []*Expr, d *Dialect) *Expr {
	return New(
		KArrayRemove,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"null_propagation", d.S.ARRAY_FUNCS_PROPAGATES_NULLS,
	)
}

// ---------------------------------------------------------------------------------------------
// Helpers for the tables below
// ---------------------------------------------------------------------------------------------

// anyExpr converts an expression into an `any`, mapping a nil pointer to an untyped nil.
func anyExpr(e *Expr) any {
	if e == nil {
		return nil
	}
	return e
}

// chunkGVar mirrors exp.var(name) for a string name.
func chunkGVar(name string) *Expr {
	if name == "" {
		panic(&ValueError{Msg: "Cannot convert empty name into var."})
	}
	return VarExpr(name)
}

// chunkGWrapConnector mirrors exp._wrap(expression, exp.Connector).
func chunkGWrapConnector(e *Expr) *Expr {
	if e.IsA(KConnector) {
		return New(KParen, "this", e)
	}
	return e
}

// chunkGAnd mirrors exp.and_(*expressions, copy=False) for expression operands.
func chunkGAnd(expressions []*Expr) *Expr {
	conditions := []*Expr{}
	for _, e := range expressions {
		if e != nil {
			conditions = append(conditions, e)
		}
	}
	if len(conditions) == 0 {
		// Python: `this, *rest = conditions` on an empty list
		panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
	}

	this, rest := conditions[0], conditions[1:]
	if len(rest) > 0 {
		this = chunkGWrapConnector(this)
	}
	for _, expression := range rest {
		this = New(KAnd, "this", this, "expression", chunkGWrapConnector(expression))
	}
	return this
}

// chunkGQueryWhere mirrors Query.where(*expressions, copy=False) (append=True).
func chunkGQueryWhere(instance *Expr, expressions ...*Expr) *Expr {
	filtered := []*Expr{}
	for _, e := range expressions {
		if e.IsA(KWhere) {
			e = e.This()
		}
		if e != nil {
			filtered = append(filtered, e)
		}
	}
	if len(filtered) == 0 {
		return instance
	}

	if existing := instance.ArgE("where"); existing != nil {
		filtered = append([]*Expr{existing.This()}, filtered...)
	}

	node := chunkGAnd(filtered)
	instance.Set("where", New(KWhere, "this", node))
	return instance
}

// chunkGQueryOrderBy mirrors Query.order_by(*expressions, append=append_, copy=False).
func chunkGQueryOrderBy(instance *Expr, append_ bool, expressions ...*Expr) *Expr {
	parsed := []*Expr{}
	var propKeys []string
	propVals := map[string]any{}

	for _, expression := range expressions {
		if expression == nil {
			continue
		}
		if !expression.IsA(KOrder) {
			expression = New(KOrder, "expressions", []*Expr{expression})
		}
		for _, k := range expression.ArgKeys() {
			v := expression.Arg(k)
			if k == "expressions" {
				if l, ok := v.([]*Expr); ok {
					parsed = append(parsed, l...)
				}
			} else {
				if _, seen := propVals[k]; !seen {
					propKeys = append(propKeys, k)
				}
				propVals[k] = v
			}
		}
	}

	if existing := instance.ArgE("order"); append_ && existing != nil {
		parsed = append(append([]*Expr{}, existing.Expressions()...), parsed...)
	}
	child := New(KOrder, "expressions", parsed)
	for _, k := range propKeys {
		child.Set(k, propVals[k])
	}
	instance.Set("order", child)
	return instance
}

// chunkGSelectDistinct mirrors Select.distinct(copy=False) with no ON expressions.
func chunkGSelectDistinct(instance *Expr) *Expr {
	if !instance.IsA(KSelect) {
		// Python: only exp.Select defines .distinct()
		name := "NoneType"
		if instance != nil {
			name = instance.Kind().Name()
		}
		panic(&ValueError{Msg: "'" + name + "' object has no attribute 'distinct'"})
	}
	instance.Set("distinct", New(KDistinct, "on", nil))
	return instance
}

// propKwargsTypeError mirrors the Python TypeError raised when a PROPERTY_PARSERS entry is
// called with keyword arguments it does not accept. Parser._parse_property_before catches it
// (and calls raise_error); elsewhere it propagates like any non-ParseError exception.
type propKwargsTypeError struct{ Msg string }

func (e *propKwargsTypeError) Error() string { return e.Msg }

// pyKwargs returns the kwargs that Python would pass (only truthy ones, in the insertion order
// of the kwargs dict built in Parser._parse_property_before) as alternating key/value pairs.
func (kw propKwargs) pyKwargs() []any {
	var kv []any
	if kw.no {
		kv = append(kv, "no", true)
	}
	if kw.dual {
		kv = append(kv, "dual", true)
	}
	if kw.before {
		kv = append(kv, "before", true)
	}
	if kw.default_ {
		kv = append(kv, "default", true)
	}
	if kw.local != "" {
		kv = append(kv, "local", kw.local)
	}
	if kw.after {
		kv = append(kv, "after", true)
	}
	if kw.minimum {
		kv = append(kv, "minimum", true)
	}
	if kw.maximum {
		kv = append(kv, "maximum", true)
	}
	return kv
}

// checkPropKwargs panics with a propKwargsTypeError if kw carries a kwarg not in allowed
// (mirrors calling a Python function with an unexpected keyword argument).
func checkPropKwargs(kw propKwargs, fn string, allowed ...string) {
	kv := kw.pyKwargs()
	for i := 0; i < len(kv); i += 2 {
		k := kv[i].(string)
		if !slices.Contains(allowed, k) {
			panic(&propKwargsTypeError{Msg: fn + "() got an unexpected keyword argument '" + k + "'"})
		}
	}
}

// noKwargs wraps a PROPERTY_PARSERS entry defined as `lambda self: ...` (accepts no kwargs).
func noKwargs(fn func(p *Parser) any) propertyParseFn {
	return func(p *Parser, kw propKwargs) any {
		checkPropKwargs(kw, "Parser.<lambda>")
		return fn(p)
	}
}

// noKwargsE is noKwargs for expression-returning entries.
func noKwargsE(fn func(p *Parser) *Expr) propertyParseFn {
	return func(p *Parser, kw propKwargs) any {
		checkPropKwargs(kw, "Parser.<lambda>")
		return anyExpr(fn(p))
	}
}

// parserKeysTrie mirrors new_trie(key.split(" ") for key in parsers).
func parserKeysTrie[V any](parsers map[string]V) *trie {
	keys := make([][]string, 0, len(parsers))
	for k := range parsers {
		keys = append(keys, strings.Split(k, " "))
	}
	return newTrieFromWords(keys...)
}

// ---------------------------------------------------------------------------------------------
// Parser class-level tables
// ---------------------------------------------------------------------------------------------

func newBaseParserSettings() *ParserSettings {
	s := &ParserSettings{
		ParserData: parserSettings_base(),
		h:          defaultParserHooks(),
	}

	// FUNCTIONS
	functions := make(map[string]FuncBuilder, len(FUNCTION_BY_NAME)+64)
	for name, kind := range FUNCTION_BY_NAME {
		functions[name] = fromArgList(kind)
	}
	coalesce := func(args []*Expr, _ *Dialect) *Expr { return buildCoalesce(args, nil, nil) }
	for _, name := range []string{"COALESCE", "IFNULL", "NVL"} {
		functions[name] = coalesce
	}
	arrayAgg := func(args []*Expr, d *Dialect) *Expr {
		var nullsExcluded any
		if d.S.ARRAY_AGG_INCLUDES_NULLS == TriNone {
			nullsExcluded = true
		}
		return New(KArrayAgg, "this", seqGet(args, 0), "nulls_excluded", nullsExcluded)
	}
	uuidIsString := func(d *Dialect) any {
		if d.S.UUID_IS_STRING_TYPE {
			return true
		}
		return nil
	}
	maps.Copy(functions, map[string]FuncBuilder{
		"ARRAY": func(args []*Expr, _ *Dialect) *Expr {
			return New(KArray, "expressions", args)
		},
		"ARRAYAGG":     arrayAgg,
		"ARRAY_AGG":    arrayAgg,
		"ARRAY_APPEND": buildArrayAppend,
		"ARRAY_CAT":    buildArrayConcat,
		"ARRAY_CONCAT": buildArrayConcat,
		"ARRAY_INTERSECT": func(args []*Expr, _ *Dialect) *Expr {
			return New(KArrayIntersect, "expressions", args)
		},
		"ARRAY_INTERSECTION": func(args []*Expr, _ *Dialect) *Expr {
			return New(KArrayIntersect, "expressions", args)
		},
		"ARRAY_PREPEND": buildArrayPrepend,
		"ARRAY_REMOVE":  buildArrayRemove,
		"COUNT": func(args []*Expr, _ *Dialect) *Expr {
			return New(KCount, "this", seqGet(args, 0), "expressions", argsFrom(args, 1), "big_int", true)
		},
		"CONCAT": func(args []*Expr, d *Dialect) *Expr {
			return New(
				KConcat,
				"expressions", args,
				"safe", !d.S.STRICT_STRING_CONCAT,
				"coalesce", d.S.CONCAT_COALESCE,
			)
		},
		"CONCAT_WS": func(args []*Expr, d *Dialect) *Expr {
			return New(
				KConcatWs,
				"expressions", args,
				"safe", !d.S.STRICT_STRING_CONCAT,
				"coalesce", d.S.CONCAT_WS_COALESCE,
			)
		},
		"CONVERT_TIMEZONE": func(args []*Expr, _ *Dialect) *Expr {
			return buildConvertTimezone(args, "")
		},
		"DATE_TO_DATE_STR": func(args []*Expr, _ *Dialect) *Expr {
			return New(KCast, "this", seqGet(args, 0), "to", NewDataType(DT_TEXT))
		},
		"GENERATE_DATE_ARRAY": func(args []*Expr, _ *Dialect) *Expr {
			start := seqGet(args, 0)
			end := seqGet(args, 1)
			step := seqGet(args, 2)
			if step == nil {
				step = New(KInterval, "this", LiteralString("1"), "unit", chunkGVar("DAY"))
			}
			return New(KGenerateDateArray, "start", start, "end", end, "step", step)
		},
		"GENERATE_UUID": func(args []*Expr, d *Dialect) *Expr {
			return New(KUuid, "is_string", uuidIsString(d))
		},
		"GLOB": func(args []*Expr, _ *Dialect) *Expr {
			return New(KGlob, "this", seqGet(args, 1), "expression", seqGet(args, 0))
		},
		"GREATEST": func(args []*Expr, d *Dialect) *Expr {
			return New(
				KGreatest,
				"this", seqGet(args, 0),
				"expressions", argsFrom(args, 1),
				"ignore_nulls", d.S.LEAST_GREATEST_IGNORES_NULLS,
			)
		},
		"LEAST": func(args []*Expr, d *Dialect) *Expr {
			return New(
				KLeast,
				"this", seqGet(args, 0),
				"expressions", argsFrom(args, 1),
				"ignore_nulls", d.S.LEAST_GREATEST_IGNORES_NULLS,
			)
		},
		"HEX":                    buildHex,
		"JSON_EXTRACT":           buildExtractJSONWithPath(KJSONExtract),
		"JSON_EXTRACT_SCALAR":    buildExtractJSONWithPath(KJSONExtractScalar),
		"JSON_EXTRACT_PATH_TEXT": buildExtractJSONWithPath(KJSONExtractScalar),
		"JSON_KEYS": func(args []*Expr, d *Dialect) *Expr {
			return New(KJSONKeys, "this", seqGet(args, 0), "expression", d.toJSONPath(seqGet(args, 1)))
		},
		"LIKE": func(args []*Expr, _ *Dialect) *Expr { return buildLike(args) },
		"LOG":  buildLogarithm,
		"LOG2": func(args []*Expr, _ *Dialect) *Expr {
			return New(KLog, "this", LiteralInt(2), "expression", seqGet(args, 0))
		},
		"LOG10": func(args []*Expr, _ *Dialect) *Expr {
			return New(KLog, "this", LiteralInt(10), "expression", seqGet(args, 0))
		},
		"LOWER":    func(args []*Expr, _ *Dialect) *Expr { return buildLower(args) },
		"LPAD":     func(args []*Expr, _ *Dialect) *Expr { return buildPad(args, true) },
		"LEFTPAD":  func(args []*Expr, _ *Dialect) *Expr { return buildPad(args, true) },
		"LTRIM":    func(args []*Expr, _ *Dialect) *Expr { return buildTrim(args, true, false) },
		"MOD":      func(args []*Expr, _ *Dialect) *Expr { return buildMod(args) },
		"RIGHTPAD": func(args []*Expr, _ *Dialect) *Expr { return buildPad(args, false) },
		"RPAD":     func(args []*Expr, _ *Dialect) *Expr { return buildPad(args, false) },
		"RTRIM":    func(args []*Expr, _ *Dialect) *Expr { return buildTrim(args, false, false) },
		"SCOPE_RESOLUTION": func(args []*Expr, _ *Dialect) *Expr {
			if len(args) != 2 {
				return New(KScopeResolution, "expression", seqGet(args, 0))
			}
			return New(KScopeResolution, "this", seqGet(args, 0), "expression", seqGet(args, 1))
		},
		"STRPOS":    fromArgList(KStrPosition),
		"CHARINDEX": func(args []*Expr, _ *Dialect) *Expr { return buildLocateStrposition(args) },
		"INSTR":     fromArgList(KStrPosition),
		"LOCATE":    func(args []*Expr, _ *Dialect) *Expr { return buildLocateStrposition(args) },
		"TIME_TO_TIME_STR": func(args []*Expr, _ *Dialect) *Expr {
			return New(KCast, "this", seqGet(args, 0), "to", NewDataType(DT_TEXT))
		},
		"TO_HEX": buildHex,
		"TS_OR_DS_TO_DATE_STR": func(args []*Expr, _ *Dialect) *Expr {
			return New(
				KSubstring,
				"this", New(KCast, "this", seqGet(args, 0), "to", NewDataType(DT_TEXT)),
				"start", LiteralInt(1),
				"length", LiteralInt(10),
			)
		},
		"UNNEST": func(args []*Expr, _ *Dialect) *Expr {
			return New(KUnnest, "expressions", ensureList(seqGet(args, 0)))
		},
		"UPPER": func(args []*Expr, _ *Dialect) *Expr { return buildUpper(args) },
		"UUID": func(args []*Expr, d *Dialect) *Expr {
			return New(KUuid, "is_string", uuidIsString(d))
		},
		"UUID_STRING": func(args []*Expr, d *Dialect) *Expr {
			return New(
				KUuid,
				"this", seqGet(args, 0),
				"name", seqGet(args, 1),
				"is_string", uuidIsString(d),
			)
		},
		"VAR_MAP": func(args []*Expr, _ *Dialect) *Expr { return buildVarMap(args) },
	})
	s.FUNCTIONS = functions

	// LAMBDAS
	s.LAMBDAS = map[TokenType]lambdaParseFn{
		TK_ARROW: func(p *Parser, expressions []*Expr) *Expr {
			this := p.replaceLambda(p.parseDisjunction(), expressions)
			return p.expression(New(KLambda, "this", this, "expressions", expressions))
		},
		TK_FARROW: func(p *Parser, expressions []*Expr) *Expr {
			this := chunkGVar(expressions[0].Name())
			return p.expression(New(KKwarg, "this", this, "expression", p.parseDisjunction()))
		},
	}

	// COLUMN_OPERATORS
	s.COLUMN_OPERATORS = map[TokenType]columnOperatorFn{
		TK_DOT: nil,
		TK_DOTCOLON: func(p *Parser, this, to *Expr) *Expr {
			return p.expression(New(KJSONCast, "this", this, "to", to))
		},
		TK_DCOLON: func(p *Parser, this, to *Expr) *Expr {
			return p.buildCast(p.s.STRICT_CAST, "this", this, "to", to)
		},
		TK_ARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.expression(New(
				KJSONExtract,
				"this", this,
				"expression", p.d.toJSONPath(path),
				"only_json_types", p.s.JSON_ARROWS_REQUIRE_JSON_TYPE,
			))
		},
		TK_DARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.expression(New(
				KJSONExtractScalar,
				"this", this,
				"expression", p.d.toJSONPath(path),
				"only_json_types", p.s.JSON_ARROWS_REQUIRE_JSON_TYPE,
				"scalar_only", p.d.S.JSON_EXTRACT_SCALAR_SCALAR_ONLY,
			))
		},
		TK_HASH_ARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.expression(New(KJSONBExtract, "this", this, "expression", path))
		},
		TK_DHASH_ARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.expression(New(KJSONBExtractScalar, "this", this, "expression", path))
		},
		TK_PLACEHOLDER: func(p *Parser, this, key *Expr) *Expr {
			return p.expression(New(KJSONBContains, "this", this, "expression", key))
		},
	}

	// EXPRESSION_PARSERS
	s.EXPRESSION_PARSERS = map[Kind]parseFn{
		KCluster:   func(p *Parser) *Expr { return p.parseSort(KCluster, TK_CLUSTER_BY) },
		KColumn:    func(p *Parser) *Expr { return p.parseColumn() },
		KColumnDef: func(p *Parser) *Expr { return p.parseColumnDef(p.parseColumn(), true) },
		KCondition: func(p *Parser) *Expr { return p.parseDisjunction() },
		KDataType: func(p *Parser) *Expr {
			return p.parseTypes(false, true, false, false)
		},
		KExpr:                  func(p *Parser) *Expr { return p.parseExpression() },
		KFrom:                  func(p *Parser) *Expr { return p.parseFrom(true, false, false) },
		KGrantPrincipal:        func(p *Parser) *Expr { return p.parseGrantPrincipal() },
		KGrantPrivilege:        func(p *Parser) *Expr { return p.parseGrantPrivilege() },
		KGroup:                 func(p *Parser) *Expr { return p.parseGroup(false) },
		KHaving:                func(p *Parser) *Expr { return p.parseHaving(false) },
		KHint:                  func(p *Parser) *Expr { return p.parseHintBody() },
		KIdentifier:            func(p *Parser) *Expr { return p.parseIdVar(true, nil) },
		KJoin:                  func(p *Parser) *Expr { return p.parseJoin(false, false, nil) },
		KLambda:                func(p *Parser) *Expr { return p.parseLambda(false) },
		KLateral:               func(p *Parser) *Expr { return p.parseLateral() },
		KLimit:                 func(p *Parser) *Expr { return p.parseLimit(nil, false, false) },
		KOffset:                func(p *Parser) *Expr { return p.parseOffset(nil) },
		KOrder:                 func(p *Parser) *Expr { return p.parseOrder(nil, false) },
		KOrdered:               func(p *Parser) *Expr { return p.parseOrdered(nil) },
		KProperties:            func(p *Parser) *Expr { return p.parseProperties(false) },
		KPartitionedByProperty: func(p *Parser) *Expr { return p.parsePartitionedBy() },
		KQualify:               func(p *Parser) *Expr { return p.parseQualify() },
		KReturning:             func(p *Parser) *Expr { return p.parseReturning() },
		KSelect: func(p *Parser) *Expr {
			return p.parseSelect(false, false, true, true, true, nil)
		},
		KSort:       func(p *Parser) *Expr { return p.parseSort(KSort, TK_SORT_BY) },
		KTable:      func(p *Parser) *Expr { return p.parseTableParts(false, false, false, false) },
		KTableAlias: func(p *Parser) *Expr { return p.parseTableAlias(nil) },
		KTuple:      func(p *Parser) *Expr { return p.parseValue(false) },
		KWhens:      func(p *Parser) *Expr { return p.parseWhenMatched() },
		KWhere:      func(p *Parser) *Expr { return p.parseWhere(false) },
		KWindow:     func(p *Parser) *Expr { return p.parseNamedWindow() },
		KWith:       func(p *Parser) *Expr { return p.parseWith(false) },
	}

	// STATEMENT_PARSERS
	s.STATEMENT_PARSERS = map[TokenType]parseFn{
		TK_ALTER:    func(p *Parser) *Expr { return p.parseAlter() },
		TK_ANALYZE:  func(p *Parser) *Expr { return p.parseAnalyze() },
		TK_BEGIN:    func(p *Parser) *Expr { return p.parseTransaction() },
		TK_CACHE:    func(p *Parser) *Expr { return p.parseCache() },
		TK_COMMENT:  func(p *Parser) *Expr { return p.parseComment(true) },
		TK_COMMIT:   func(p *Parser) *Expr { return p.parseCommitOrRollback() },
		TK_COPY:     func(p *Parser) *Expr { return p.parseCopy() },
		TK_CREATE:   func(p *Parser) *Expr { return p.parseCreate() },
		TK_DELETE:   func(p *Parser) *Expr { return p.parseDelete() },
		TK_DESC:     func(p *Parser) *Expr { return p.parseDescribe() },
		TK_DESCRIBE: func(p *Parser) *Expr { return p.parseDescribe() },
		TK_DROP:     func(p *Parser) *Expr { return p.parseDrop(false) },
		TK_GRANT:    func(p *Parser) *Expr { return p.parseGrant() },
		TK_REVOKE:   func(p *Parser) *Expr { return p.parseRevoke() },
		TK_INSERT:   func(p *Parser) *Expr { return p.parseInsert() },
		TK_KILL:     func(p *Parser) *Expr { return p.parseKill() },
		TK_LOAD:     func(p *Parser) *Expr { return p.parseLoad() },
		TK_MERGE:    func(p *Parser) *Expr { return p.parseMerge() },
		// Python passes is_unpivot=None here
		TK_PIVOT: func(p *Parser) *Expr { return p.parseSimplifiedPivot(nil) },
		TK_PRAGMA: func(p *Parser) *Expr {
			return p.expression(New(KPragma, "this", p.parseExpression()))
		},
		TK_REFRESH:   func(p *Parser) *Expr { return p.parseRefresh() },
		TK_ROLLBACK:  func(p *Parser) *Expr { return p.parseCommitOrRollback() },
		TK_SET:       func(p *Parser) *Expr { return p.parseSet(false, false) },
		TK_TRUNCATE:  func(p *Parser) *Expr { return p.parseTruncateTable() },
		TK_UNCACHE:   func(p *Parser) *Expr { return p.parseUncache() },
		TK_UNPIVOT:   func(p *Parser) *Expr { return p.parseSimplifiedPivot(true) },
		TK_UPDATE:    func(p *Parser) *Expr { return p.parseUpdate() },
		TK_USE:       func(p *Parser) *Expr { return p.parseUse() },
		TK_SEMICOLON: func(p *Parser) *Expr { return New(KSemicolon) },
	}

	// UNARY_PARSERS
	s.UNARY_PARSERS = map[TokenType]parseFn{
		TK_PLUS: func(p *Parser) *Expr { return p.parseUnary() }, // Unary + is handled as a no-op
		TK_NOT: func(p *Parser) *Expr {
			return p.expression(New(KNot, "this", p.parseEquality()))
		},
		TK_TILDE: func(p *Parser) *Expr {
			return p.expression(New(KBitwiseNot, "this", p.parseUnary()))
		},
		TK_DASH: func(p *Parser) *Expr {
			return p.expression(New(KNeg, "this", p.parseUnary()))
		},
		TK_PIPE_SLASH: func(p *Parser) *Expr {
			return p.expression(New(KSqrt, "this", p.parseUnary()))
		},
		TK_DPIPE_SLASH: func(p *Parser) *Expr {
			return p.expression(New(KCbrt, "this", p.parseUnary()))
		},
	}

	// STRING_PARSERS
	stringParsers := func() map[TokenType]tokenParseFn {
		return map[TokenType]tokenParseFn{
			TK_HEREDOC_STRING: func(p *Parser, token *Token) *Expr {
				return p.expressionTok(New(KRawString, "this", token.Text), token)
			},
			TK_NATIONAL_STRING: func(p *Parser, token *Token) *Expr {
				return p.expressionTok(New(KNational, "this", token.Text), token)
			},
			TK_RAW_STRING: func(p *Parser, token *Token) *Expr {
				return p.expressionTok(New(KRawString, "this", token.Text), token)
			},
			TK_STRING: func(p *Parser, token *Token) *Expr {
				return p.expressionTok(New(KLiteral, "this", token.Text, "is_string", true), token)
			},
			TK_UNICODE_STRING: func(p *Parser, token *Token) *Expr {
				this := token.Text
				// self._match_text_seq("UESCAPE") and self._parse_string() -> False when unmatched
				var escape any = false
				if p.matchTextSeq("UESCAPE") {
					escape = p.parseString()
				}
				return p.expressionTok(New(KUnicodeString, "this", this, "escape", escape), token)
			},
		}
	}
	s.STRING_PARSERS = stringParsers()

	// NUMERIC_PARSERS
	numericParsers := func() map[TokenType]tokenParseFn {
		return map[TokenType]tokenParseFn{
			TK_BIT_STRING: func(p *Parser, token *Token) *Expr {
				return p.expressionTok(New(KBitString, "this", token.Text), token)
			},
			TK_BYTE_STRING: func(p *Parser, token *Token) *Expr {
				var isBytes any
				if p.d.S.BYTE_STRING_IS_BYTES_TYPE {
					isBytes = true
				}
				return p.expressionTok(New(KByteString, "this", token.Text, "is_bytes", isBytes), token)
			},
			TK_HEX_STRING: func(p *Parser, token *Token) *Expr {
				var isInteger any
				if p.d.S.HEX_STRING_IS_INTEGER_TYPE {
					isInteger = true
				}
				return p.expressionTok(New(KHexString, "this", token.Text, "is_integer", isInteger), token)
			},
			TK_NUMBER: func(p *Parser, token *Token) *Expr {
				return p.expressionTok(New(KLiteral, "this", token.Text, "is_string", false), token)
			},
		}
	}
	s.NUMERIC_PARSERS = numericParsers()

	// PRIMARY_PARSERS = {**STRING_PARSERS, **NUMERIC_PARSERS, ...}
	s.PRIMARY_PARSERS = map[TokenType]tokenParseFn{}
	maps.Copy(s.PRIMARY_PARSERS, stringParsers())
	maps.Copy(s.PRIMARY_PARSERS, numericParsers())
	maps.Copy(s.PRIMARY_PARSERS, map[TokenType]tokenParseFn{
		TK_INTRODUCER: func(p *Parser, token *Token) *Expr { return p.parseIntroducer(token) },
		TK_NULL:       func(p *Parser, _ *Token) *Expr { return p.expression(Null()) },
		TK_TRUE:       func(p *Parser, _ *Token) *Expr { return p.expression(New(KBoolean, "this", true)) },
		TK_FALSE:      func(p *Parser, _ *Token) *Expr { return p.expression(New(KBoolean, "this", false)) },
		TK_SESSION_PARAMETER: func(p *Parser, _ *Token) *Expr {
			return p.parseSessionParameter()
		},
		TK_STAR: func(p *Parser, _ *Token) *Expr { return p.parseStarOps() },
	})

	// PLACEHOLDER_PARSERS
	s.PLACEHOLDER_PARSERS = map[TokenType]parseFn{
		TK_PLACEHOLDER: func(p *Parser) *Expr { return p.expression(New(KPlaceholder)) },
		TK_PARAMETER:   func(p *Parser) *Expr { return p.parseParameter() },
		TK_COLON: func(p *Parser) *Expr {
			if p.matchSet(p.s.COLON_PLACEHOLDER_TOKENS) {
				return p.expression(New(KPlaceholder, "this", p.prev.Text))
			}
			return nil
		},
	}

	// RANGE_PARSERS
	s.RANGE_PARSERS = map[TokenType]rangeParseFn{
		TK_AT_GT:      binaryRangeParser(KArrayContainsAll, false),
		TK_BETWEEN:    func(p *Parser, this *Expr) *Expr { return p.parseBetween(this) },
		TK_GLOB:       binaryRangeParser(KGlob, false),
		TK_ILIKE:      binaryRangeParser(KILike, false),
		TK_IN:         func(p *Parser, this *Expr) *Expr { return p.parseIn(this, false) },
		TK_IRLIKE:     binaryRangeParser(KRegexpILike, false),
		TK_IS:         func(p *Parser, this *Expr) *Expr { return p.parseIs(this) },
		TK_LIKE:       binaryRangeParser(KLike, false),
		TK_LT_AT:      binaryRangeParser(KArrayContainedBy, false),
		TK_OVERLAPS:   binaryRangeParser(KOverlaps, false),
		TK_RLIKE:      binaryRangeParser(KRegexpLike, false),
		TK_SIMILAR_TO: binaryRangeParser(KSimilarTo, false),
		TK_FOR:        func(p *Parser, this *Expr) *Expr { return p.parseComprehension(this) },
		TK_QMARK_AMP:  binaryRangeParser(KJSONBContainsAllTopKeys, false),
		TK_QMARK_PIPE: binaryRangeParser(KJSONBContainsAnyTopKeys, false),
		TK_HASH_DASH:  binaryRangeParser(KJSONBDeleteAtPath, false),
		TK_AT_QMARK:   binaryRangeParser(KJSONBPathExists, false),
		TK_ADJACENT:   binaryRangeParser(KAdjacent, false),
		TK_OPERATOR:   func(p *Parser, this *Expr) *Expr { return p.parseOperator(this) },
		TK_AMP_LT:     binaryRangeParser(KExtendsLeft, false),
		TK_AMP_GT:     binaryRangeParser(KExtendsRight, false),
	}

	// PIPE_SYNTAX_TRANSFORM_PARSERS
	s.PIPE_SYNTAX_TRANSFORM_PARSERS = map[string]pipeTransformFn{
		"AGGREGATE": func(p *Parser, query *Expr) *Expr { return p.parsePipeSyntaxAggregate(query) },
		"AS": func(p *Parser, query *Expr) *Expr {
			expressions := []*Expr{Star()}
			return p.buildPipeCte(query, expressions, p.parseTableAlias(nil))
		},
		"DISTINCT": func(p *Parser, query *Expr) *Expr {
			// self._advance() or query.distinct(copy=False)
			p.advance(1)
			return chunkGSelectDistinct(query)
		},
		"EXTEND": func(p *Parser, query *Expr) *Expr { return p.parsePipeSyntaxExtend(query) },
		"LIMIT":  func(p *Parser, query *Expr) *Expr { return p.parsePipeSyntaxLimit(query) },
		"ORDER BY": func(p *Parser, query *Expr) *Expr {
			return chunkGQueryOrderBy(query, false, p.parseOrder(nil, false))
		},
		"PIVOT":       func(p *Parser, query *Expr) *Expr { return p.parsePipeSyntaxPivot(query) },
		"SELECT":      func(p *Parser, query *Expr) *Expr { return p.parsePipeSyntaxSelect(query) },
		"TABLESAMPLE": func(p *Parser, query *Expr) *Expr { return p.parsePipeSyntaxTablesample(query) },
		"UNPIVOT":     func(p *Parser, query *Expr) *Expr { return p.parsePipeSyntaxPivot(query) },
		"WHERE": func(p *Parser, query *Expr) *Expr {
			return chunkGQueryWhere(query, p.parseWhere(false))
		},
	}

	// PROPERTY_PARSERS
	stability := func(v string) propertyParseFn {
		return noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(KStabilityProperty, "this", LiteralString(v)))
		})
	}
	assignment := func(k Kind) propertyParseFn {
		return noKwargsE(func(p *Parser) *Expr { return p.parsePropertyAssignment(k) })
	}
	simple := func(k Kind) propertyParseFn {
		return noKwargsE(func(p *Parser) *Expr { return p.expression(New(k)) })
	}
	characterSet := func(p *Parser, kw propKwargs) any {
		checkPropKwargs(kw, "Parser._parse_character_set", "default")
		return anyExpr(p.parseCharacterSet(kw.default_))
	}
	partitionedBy := noKwargsE(func(p *Parser) *Expr { return p.parsePartitionedBy() })
	locking := noKwargsE(func(p *Parser) *Expr { return p.parseLocking() })
	sqlSecurity := noKwargsE(func(p *Parser) *Expr { return p.parseSqlSecurity() })
	s.PROPERTY_PARSERS = map[string]propertyParseFn{
		"ALLOWED_VALUES": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(
				KAllowedValuesProperty,
				"expressions", p.parseCSV(func() *Expr { return p.parsePrimary() }, TK_COMMA),
			))
		}),
		"ALGORITHM":      assignment(KAlgorithmProperty),
		"AUTO":           noKwargsE(func(p *Parser) *Expr { return p.parseAutoProperty() }),
		"AUTO_INCREMENT": assignment(KAutoIncrementProperty),
		"BACKUP": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(KBackupProperty, "this", p.parseVar(true, nil, false)))
		}),
		"BLOCKCOMPRESSION": noKwargsE(func(p *Parser) *Expr { return p.parseBlockcompression() }),
		"CALLED":           noKwargsE(func(p *Parser) *Expr { return p.parseCalledOnNullInputProperty() }),
		"CHARSET":          characterSet,
		"CHARACTER SET":    characterSet,
		"CHECKSUM":         noKwargsE(func(p *Parser) *Expr { return p.parseChecksum() }),
		"CLUSTER BY":       noKwargsE(func(p *Parser) *Expr { return p.parseClusterProperty() }),
		"CLUSTERED":        noKwargsE(func(p *Parser) *Expr { return p.parseClusteredBy() }),
		"COLLATE": func(p *Parser, kw propKwargs) any {
			return anyExpr(p.parsePropertyAssignment(KCollateProperty, kw.pyKwargs()...))
		},
		"COMMENT":  assignment(KSchemaCommentProperty),
		"CONTAINS": noKwargsE(func(p *Parser) *Expr { return p.parseContainsProperty() }),
		"COPY":     noKwargsE(func(p *Parser) *Expr { return p.parseCopyProperty() }),
		"DATABLOCKSIZE": func(p *Parser, kw propKwargs) any {
			checkPropKwargs(kw, "Parser._parse_datablocksize", "default", "minimum", "maximum")
			return anyExpr(p.parseDatablocksize(kw.default_, kw.minimum, kw.maximum))
		},
		"DATA_DELETION": noKwargsE(func(p *Parser) *Expr { return p.parseDataDeletionProperty() }),
		"DEFINER":       noKwargsE(func(p *Parser) *Expr { return p.parseDefiner() }),
		"DETERMINISTIC": stability("IMMUTABLE"),
		"DISTRIBUTED":   noKwargsE(func(p *Parser) *Expr { return p.parseDistributedProperty() }),
		"DUPLICATE": noKwargsE(func(p *Parser) *Expr {
			return p.parseCompositeKeyProperty(KDuplicateKeyProperty)
		}),
		"DYNAMIC":   simple(KDynamicProperty),
		"DISTKEY":   noKwargsE(func(p *Parser) *Expr { return p.parseDistkey() }),
		"DISTSTYLE": assignment(KDistStyleProperty),
		"EMPTY":     simple(KEmptyProperty),
		"ENGINE":    assignment(KEngineProperty),
		"ENVIRONMENT": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(
				KEnviromentProperty,
				"expressions", p.parseWrappedCSV(func() *Expr { return p.parseAssignment() }, TK_COMMA, false),
			))
		}),
		"HANDLER":  assignment(KHandlerProperty),
		"EXECUTE":  assignment(KExecuteAsProperty),
		"EXTERNAL": simple(KExternalProperty),
		"FALLBACK": func(p *Parser, kw propKwargs) any {
			checkPropKwargs(kw, "Parser._parse_fallback", "no")
			return anyExpr(p.parseFallback(kw.no))
		},
		"FORMAT":    assignment(KFileFormatProperty),
		"FREESPACE": noKwargsE(func(p *Parser) *Expr { return p.parseFreespace() }),
		"GLOBAL":    simple(KGlobalProperty),
		"HEAP":      simple(KHeapProperty),
		"ICEBERG":   simple(KIcebergProperty),
		"IMMUTABLE": stability("IMMUTABLE"),
		"INHERITS": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(
				KInheritsProperty,
				"expressions", p.parseWrappedCSV(func() *Expr {
					return p.parseTable(false, false, nil, false, false, false, false)
				}, TK_COMMA, false),
			))
		}),
		"INPUT": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(KInputModelProperty, "this", p.parseSchema(nil)))
		}),
		"JOURNAL": func(p *Parser, kw propKwargs) any {
			return anyExpr(p.parseJournal(kw))
		},
		"LANGUAGE": assignment(KLanguageProperty),
		"LAYOUT":   noKwargsE(func(p *Parser) *Expr { return p.parseDictProperty("LAYOUT") }),
		"LIFETIME": noKwargsE(func(p *Parser) *Expr { return p.parseDictRange("LIFETIME") }),
		"LIKE":     noKwargsE(func(p *Parser) *Expr { return p.parseCreateLike() }),
		"LOCATION": assignment(KLocationProperty),
		"LOCK":     locking,
		"LOCKING":  locking,
		"LOG": func(p *Parser, kw propKwargs) any {
			checkPropKwargs(kw, "Parser._parse_log", "no")
			return anyExpr(p.parseLog(kw.no))
		},
		"MATERIALIZED": simple(KMaterializedProperty),
		"MERGEBLOCKRATIO": func(p *Parser, kw propKwargs) any {
			checkPropKwargs(kw, "Parser._parse_mergeblockratio", "no", "default")
			return anyExpr(p.parseMergeblockratio(kw.no, kw.default_))
		},
		"MODIFIES": noKwargsE(func(p *Parser) *Expr { return p.parseModifiesProperty() }),
		"MULTISET": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(KSetProperty, "multi", true))
		}),
		"NO":       noKwargsE(func(p *Parser) *Expr { return p.parseNoProperty() }),
		"ON":       noKwargsE(func(p *Parser) *Expr { return p.parseOnProperty() }),
		"ORDER BY": noKwargsE(func(p *Parser) *Expr { return p.parseOrder(nil, true) }),
		"OUTPUT": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(KOutputModelProperty, "this", p.parseSchema(nil)))
		}),
		"PARTITION":      noKwargsE(func(p *Parser) *Expr { return p.parsePartitionedOf() }),
		"PARTITION BY":   partitionedBy,
		"PARTITIONED BY": partitionedBy,
		"PARTITIONED_BY": partitionedBy,
		"PRIMARY KEY": noKwargsE(func(p *Parser) *Expr {
			return p.parsePrimaryKey(false, true, false)
		}),
		"RANGE":      noKwargsE(func(p *Parser) *Expr { return p.parseDictRange("RANGE") }),
		"READS":      noKwargsE(func(p *Parser) *Expr { return p.parseReadsProperty() }),
		"REMOTE":     noKwargsE(func(p *Parser) *Expr { return p.parseRemoteWithConnection() }),
		"RETURNS":    noKwargsE(func(p *Parser) *Expr { return p.parseReturns() }),
		"STRICT":     simple(KStrictProperty),
		"STREAMING":  simple(KStreamingTableProperty),
		"ROW":        noKwargsE(func(p *Parser) *Expr { return p.parseRow() }),
		"ROW_FORMAT": assignment(KRowFormatProperty),
		"SAMPLE": noKwargsE(func(p *Parser) *Expr {
			// self._match_text_seq("BY") and self._parse_bitwise() -> False when unmatched
			var this any = false
			if p.matchTextSeq("BY") {
				this = p.parseBitwise()
			}
			return p.expression(New(KSampleProperty, "this", this))
		}),
		"SECURE":       simple(KSecureProperty),
		"SECURITY":     sqlSecurity,
		"SQL SECURITY": sqlSecurity,
		"SET": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(KSetProperty, "multi", false))
		}),
		"SETTINGS":          noKwargsE(func(p *Parser) *Expr { return p.parseSettingsProperty() }),
		"SHARING":           assignment(KSharingProperty),
		"SORTKEY":           noKwargsE(func(p *Parser) *Expr { return p.parseSortkey(false) }),
		"SOURCE":            noKwargsE(func(p *Parser) *Expr { return p.parseDictProperty("SOURCE") }),
		"STABLE":            stability("STABLE"),
		"STORED":            noKwargsE(func(p *Parser) *Expr { return p.parseStored() }),
		"SYSTEM_VERSIONING": noKwargsE(func(p *Parser) *Expr { return p.parseSystemVersioningProperty(false) }),
		"TBLPROPERTIES": noKwargs(func(p *Parser) any {
			return p.parseWrappedProperties()
		}),
		"TEMP":      simple(KTemporaryProperty),
		"TEMPORARY": simple(KTemporaryProperty),
		"TO":        noKwargsE(func(p *Parser) *Expr { return p.parseToTable() }),
		"TRANSIENT": simple(KTransientProperty),
		"TRANSFORM": noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(
				KTransformModelProperty,
				"expressions", p.parseWrappedCSV(func() *Expr { return p.parseExpression() }, TK_COMMA, false),
			))
		}),
		"TTL":      noKwargsE(func(p *Parser) *Expr { return p.parseTtl() }),
		"USING":    assignment(KFileFormatProperty),
		"UNLOGGED": simple(KUnloggedProperty),
		"VOLATILE": noKwargsE(func(p *Parser) *Expr { return p.parseVolatileProperty() }),
		"WITH": noKwargs(func(p *Parser) any {
			return p.parseWithProperty()
		}),
	}

	// CONSTRAINT_PARSERS
	autoIncrement := func(p *Parser) *Expr { return p.parseAutoIncrement() }
	bucketOrTruncate := func(p *Parser) *Expr { return p.parsePartitionedByBucketOrTruncate() }
	s.CONSTRAINT_PARSERS = map[string]parseFn{
		"AUTOINCREMENT":  autoIncrement,
		"AUTO_INCREMENT": autoIncrement,
		"CASESPECIFIC": func(p *Parser) *Expr {
			return p.expression(New(KCaseSpecificColumnConstraint, "not_", false))
		},
		"CHARACTER SET": func(p *Parser) *Expr {
			return p.expression(New(KCharacterSetColumnConstraint, "this", p.parseVarOrString(false)))
		},
		"CHECK": func(p *Parser) *Expr { return p.parseCheckConstraint() },
		"COLLATE": func(p *Parser) *Expr {
			this := p.parseIdentifier()
			if this == nil {
				this = p.parseColumn()
			}
			return p.expression(New(KCollateColumnConstraint, "this", this))
		},
		"COMMENT": func(p *Parser) *Expr {
			return p.expression(New(KCommentColumnConstraint, "this", p.parseString()))
		},
		"COMPRESS": func(p *Parser) *Expr { return p.parseCompress() },
		"CLUSTERED": func(p *Parser) *Expr {
			return p.expression(New(
				KClusteredColumnConstraint,
				"this", p.parseWrappedCSV(func() *Expr { return p.parseOrdered(nil) }, TK_COMMA, false),
			))
		},
		"NONCLUSTERED": func(p *Parser) *Expr {
			return p.expression(New(
				KNonClusteredColumnConstraint,
				"this", p.parseWrappedCSV(func() *Expr { return p.parseOrdered(nil) }, TK_COMMA, false),
			))
		},
		"DEFAULT": func(p *Parser) *Expr {
			return p.expression(New(KDefaultColumnConstraint, "this", p.parseBitwise()))
		},
		"ENCODE": func(p *Parser) *Expr {
			return p.expression(New(KEncodeColumnConstraint, "this", p.parseVar(false, nil, false)))
		},
		"EPHEMERAL": func(p *Parser) *Expr {
			return p.expression(New(KEphemeralColumnConstraint, "this", p.parseBitwise()))
		},
		"EXCLUDE": func(p *Parser) *Expr {
			return p.expression(New(KExcludeColumnConstraint, "this", p.parseIndexParams()))
		},
		"FOREIGN KEY": func(p *Parser) *Expr { return p.parseForeignKey() },
		"FORMAT": func(p *Parser) *Expr {
			return p.expression(New(KDateFormatColumnConstraint, "this", p.parseVarOrString(false)))
		},
		"GENERATED": func(p *Parser) *Expr { return p.parseGeneratedAsIdentity() },
		"IDENTITY":  autoIncrement,
		"INLINE":    func(p *Parser) *Expr { return p.parseInline() },
		"LIKE":      func(p *Parser) *Expr { return p.parseCreateLike() },
		"NOT":       func(p *Parser) *Expr { return p.parseNotConstraint() },
		"NULL": func(p *Parser) *Expr {
			return p.expression(New(KNotNullColumnConstraint, "allow_null", true))
		},
		"ON": func(p *Parser) *Expr {
			if p.match(TK_UPDATE) {
				if e := p.expression(New(
					KOnUpdateColumnConstraint,
					"this", p.parseFunction(nil, false, true, false),
				)); e != nil {
					return e
				}
			}
			return p.expression(New(KOnProperty, "this", p.parseIdVar(true, nil)))
		},
		"PATH": func(p *Parser) *Expr {
			return p.expression(New(KPathColumnConstraint, "this", p.parseString()))
		},
		"PERIOD":      func(p *Parser) *Expr { return p.parsePeriodForSystemTime() },
		"PRIMARY KEY": func(p *Parser) *Expr { return p.parsePrimaryKey(false, false, false) },
		"REFERENCES":  func(p *Parser) *Expr { return p.parseReferences(false) },
		"TITLE": func(p *Parser) *Expr {
			return p.expression(New(KTitleColumnConstraint, "this", p.parseVarOrString(false)))
		},
		"TTL": func(p *Parser) *Expr {
			// [self._parse_bitwise()] may hold a None element, like in Python
			return p.expression(New(KMergeTreeTTL, "expressions", []*Expr{p.parseBitwise()}))
		},
		"UNIQUE": func(p *Parser) *Expr { return p.parseUnique() },
		"UPPERCASE": func(p *Parser) *Expr {
			return p.expression(New(KUppercaseColumnConstraint))
		},
		"WITH": func(p *Parser) *Expr {
			return p.expression(New(KProperties, "expressions", p.parseWrappedProperties()))
		},
		"BUCKET":   bucketOrTruncate,
		"TRUNCATE": bucketOrTruncate,
	}

	// ALTER_PARSERS
	s.ALTER_PARSERS = map[string]parseAnyFn{
		"ADD": func(p *Parser) any { return p.parseAlterTableAdd() },
		"AS": func(p *Parser) any {
			return anyExpr(p.parseSelect(false, false, true, true, true, nil))
		},
		"ALTER":      func(p *Parser) any { return anyExpr(p.parseAlterTableAlter()) },
		"CLUSTER BY": func(p *Parser) any { return anyExpr(p.parseClusterProperty()) },
		"DELETE": func(p *Parser) any {
			return anyExpr(p.expression(New(KDelete, "where", p.parseWhere(false))))
		},
		"DROP":   func(p *Parser) any { return p.parseAlterTableDrop() },
		"RENAME": func(p *Parser) any { return anyExpr(p.parseAlterTableRename()) },
		"SET":    func(p *Parser) any { return anyExpr(p.parseAlterTableSet()) },
		"SWAP": func(p *Parser) any {
			// self._match(TokenType.WITH) and self._parse_table(schema=True) -> False when unmatched
			var this any = false
			if p.match(TK_WITH) {
				this = p.parseTable(true, false, nil, false, false, false, false)
			}
			return anyExpr(p.expression(New(KSwapTable, "this", this)))
		},
	}

	// ALTER_ALTER_PARSERS
	s.ALTER_ALTER_PARSERS = map[string]parseFn{
		"DISTKEY":   func(p *Parser) *Expr { return p.parseAlterDiststyle() },
		"DISTSTYLE": func(p *Parser) *Expr { return p.parseAlterDiststyle() },
		// Python passes compound=None here
		"SORTKEY":  func(p *Parser) *Expr { return p.parseAlterSortkey(false) },
		"COMPOUND": func(p *Parser) *Expr { return p.parseAlterSortkey(true) },
	}

	// NO_PAREN_FUNCTION_PARSERS
	s.NO_PAREN_FUNCTION_PARSERS = map[string]parseFn{
		"ANY": func(p *Parser) *Expr {
			return p.expression(New(KAny, "this", p.parseBitwise()))
		},
		"CASE": func(p *Parser) *Expr { return p.parseCase() },
		"CONNECT_BY_ROOT": func(p *Parser) *Expr {
			return p.expression(New(KConnectByRoot, "this", p.parseColumn()))
		},
		"IF": func(p *Parser) *Expr { return p.parseIf() },
	}

	// FUNCTION_PARSERS
	s.FUNCTION_PARSERS = map[string]parseFn{}
	for _, name := range KArgMax.SQLNames() {
		s.FUNCTION_PARSERS[name] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KArgMax, 0) }
	}
	for _, name := range KArgMin.SQLNames() {
		s.FUNCTION_PARSERS[name] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KArgMin, 0) }
	}
	maps.Copy(s.FUNCTION_PARSERS, map[string]parseFn{
		// Python passes safe=None for CAST / CONVERT
		"CAST":           func(p *Parser) *Expr { return p.parseCast(p.s.STRICT_CAST, false) },
		"CEIL":           func(p *Parser) *Expr { return p.parseCeilFloor(KCeil) },
		"CONVERT":        func(p *Parser) *Expr { return p.parseConvert(p.s.STRICT_CAST, false) },
		"CHAR":           func(p *Parser) *Expr { return p.parseChar() },
		"CHR":            func(p *Parser) *Expr { return p.parseChar() },
		"DECODE":         func(p *Parser) *Expr { return p.parseDecode() },
		"EXTRACT":        func(p *Parser) *Expr { return p.parseExtract() },
		"FLOOR":          func(p *Parser) *Expr { return p.parseCeilFloor(KFloor) },
		"GAP_FILL":       func(p *Parser) *Expr { return p.parseGapFill() },
		"INITCAP":        func(p *Parser) *Expr { return p.parseInitcap() },
		"JSON_OBJECT":    func(p *Parser) *Expr { return p.parseJsonObject(false) },
		"JSON_OBJECTAGG": func(p *Parser) *Expr { return p.parseJsonObject(true) },
		"JSON_TABLE":     func(p *Parser) *Expr { return p.parseJsonTable() },
		"MATCH":          func(p *Parser) *Expr { return p.parseMatchAgainst() },
		"NORMALIZE":      func(p *Parser) *Expr { return p.parseNormalize() },
		"OPENJSON":       func(p *Parser) *Expr { return p.parseOpenJson() },
		"OVERLAY":        func(p *Parser) *Expr { return p.parseOverlay() },
		"POSITION":       func(p *Parser) *Expr { return p.parsePosition(false) },
		"SAFE_CAST":      func(p *Parser) *Expr { return p.parseCast(false, true) },
		"STRING_AGG":     func(p *Parser) *Expr { return p.parseStringAgg() },
		"SUBSTRING":      func(p *Parser) *Expr { return p.parseSubstring() },
		"TRIM":           func(p *Parser) *Expr { return p.parseTrim() },
		"TRY_CAST":       func(p *Parser) *Expr { return p.parseCast(false, true) },
		"TRY_CONVERT":    func(p *Parser) *Expr { return p.parseConvert(false, true) },
		"XMLELEMENT":     func(p *Parser) *Expr { return p.parseXmlElement() },
		"XMLTABLE":       func(p *Parser) *Expr { return p.parseXmlTable() },
	})

	// QUERY_MODIFIER_PARSERS
	limit := func(p *Parser) (string, any) { return "limit", anyExpr(p.parseLimit(nil, false, false)) }
	locks := func(p *Parser) (string, any) { return "locks", p.parseLocks() }
	sample := func(p *Parser) (string, any) { return "sample", anyExpr(p.parseTableSample(true)) }
	s.QUERY_MODIFIER_PARSERS = map[TokenType]queryModifierFn{
		TK_MATCH_RECOGNIZE: func(p *Parser) (string, any) { return "match", anyExpr(p.parseMatchRecognize()) },
		TK_PREWHERE:        func(p *Parser) (string, any) { return "prewhere", anyExpr(p.parsePrewhere(false)) },
		TK_WHERE:           func(p *Parser) (string, any) { return "where", anyExpr(p.parseWhere(false)) },
		TK_GROUP_BY:        func(p *Parser) (string, any) { return "group", anyExpr(p.parseGroup(false)) },
		TK_HAVING:          func(p *Parser) (string, any) { return "having", anyExpr(p.parseHaving(false)) },
		TK_QUALIFY:         func(p *Parser) (string, any) { return "qualify", anyExpr(p.parseQualify()) },
		TK_WINDOW:          func(p *Parser) (string, any) { return "windows", p.parseWindowClause() },
		TK_ORDER_BY:        func(p *Parser) (string, any) { return "order", anyExpr(p.parseOrder(nil, false)) },
		TK_LIMIT:           limit,
		TK_FETCH:           limit,
		TK_OFFSET:          func(p *Parser) (string, any) { return "offset", anyExpr(p.parseOffset(nil)) },
		TK_FOR:             locks,
		TK_LOCK:            locks,
		TK_TABLE_SAMPLE:    sample,
		TK_USING:           sample,
		TK_CLUSTER_BY: func(p *Parser) (string, any) {
			return "cluster", anyExpr(p.parseCluster())
		},
		TK_DISTRIBUTE_BY: func(p *Parser) (string, any) {
			return "distribute", anyExpr(p.parseSort(KDistribute, TK_DISTRIBUTE_BY))
		},
		TK_SORT_BY: func(p *Parser) (string, any) {
			return "sort", anyExpr(p.parseSort(KSort, TK_SORT_BY))
		},
		TK_CONNECT_BY: func(p *Parser) (string, any) { return "connect", anyExpr(p.parseConnect(true)) },
		TK_START_WITH: func(p *Parser) (string, any) { return "connect", anyExpr(p.parseConnect(false)) },
	}

	// SET_PARSERS
	s.SET_PARSERS = map[string]parseFn{
		"GLOBAL":      func(p *Parser) *Expr { return p.parseSetItemAssignment("GLOBAL") },
		"LOCAL":       func(p *Parser) *Expr { return p.parseSetItemAssignment("LOCAL") },
		"SESSION":     func(p *Parser) *Expr { return p.parseSetItemAssignment("SESSION") },
		"TRANSACTION": func(p *Parser) *Expr { return p.parseSetTransaction(false) },
	}

	// SHOW_PARSERS
	s.SHOW_PARSERS = map[string]parseFn{}

	// TYPE_LITERAL_PARSERS
	s.TYPE_LITERAL_PARSERS = map[DType]typeLiteralParseFn{
		DT_JSON: func(p *Parser, this, _ *Expr) *Expr {
			return p.expression(New(KParseJSON, "this", this))
		},
	}

	// TYPE_CONVERTERS
	s.TYPE_CONVERTERS = map[DType]typeConverterFn{}

	// ANALYZE_EXPRESSION_PARSERS
	s.ANALYZE_EXPRESSION_PARSERS = map[string]parseFn{
		"ALL":       func(p *Parser) *Expr { return p.parseAnalyzeColumns() },
		"COMPUTE":   func(p *Parser) *Expr { return p.parseAnalyzeStatistics() },
		"DELETE":    func(p *Parser) *Expr { return p.parseAnalyzeDelete() },
		"DROP":      func(p *Parser) *Expr { return p.parseAnalyzeHistogram() },
		"ESTIMATE":  func(p *Parser) *Expr { return p.parseAnalyzeStatistics() },
		"LIST":      func(p *Parser) *Expr { return p.parseAnalyzeList() },
		"PREDICATE": func(p *Parser) *Expr { return p.parseAnalyzeColumns() },
		"UPDATE":    func(p *Parser) *Expr { return p.parseAnalyzeHistogram() },
		"VALIDATE":  func(p *Parser) *Expr { return p.parseAnalyzeValidate() },
	}

	// DESCRIBE_QUALIFIER_PARSERS does not exist on the base Parser (Snowflake only); keep an
	// empty table so dialects can fill it.
	s.DESCRIBE_QUALIFIER_PARSERS = map[string]parseFn{}

	s.SHOW_TRIE = parserKeysTrie(s.SHOW_PARSERS)
	s.SET_TRIE = parserKeysTrie(s.SET_PARSERS)

	return s
}

// cloneParserSettings returns a copy of s whose callable tables can be modified without
// affecting s (mirrors subclassing a Parser and overriding table entries). The ParserData struct
// is copied shallowly (its inner sets/maps are shared), tries are shared like inherited
// class attributes (dialects that change SHOW_PARSERS / SET_PARSERS must rebuild them, as in
// Python), and the hooks are copied by value.
func cloneParserSettings(s *ParserSettings) *ParserSettings {
	c := *s
	if s.ParserData != nil {
		data := *s.ParserData
		c.ParserData = &data
	}
	c.FUNCTIONS = maps.Clone(s.FUNCTIONS)
	c.LAMBDAS = maps.Clone(s.LAMBDAS)
	c.COLUMN_OPERATORS = maps.Clone(s.COLUMN_OPERATORS)
	c.EXPRESSION_PARSERS = maps.Clone(s.EXPRESSION_PARSERS)
	c.STATEMENT_PARSERS = maps.Clone(s.STATEMENT_PARSERS)
	c.UNARY_PARSERS = maps.Clone(s.UNARY_PARSERS)
	c.STRING_PARSERS = maps.Clone(s.STRING_PARSERS)
	c.NUMERIC_PARSERS = maps.Clone(s.NUMERIC_PARSERS)
	c.PRIMARY_PARSERS = maps.Clone(s.PRIMARY_PARSERS)
	c.PLACEHOLDER_PARSERS = maps.Clone(s.PLACEHOLDER_PARSERS)
	c.RANGE_PARSERS = maps.Clone(s.RANGE_PARSERS)
	c.PIPE_SYNTAX_TRANSFORM_PARSERS = maps.Clone(s.PIPE_SYNTAX_TRANSFORM_PARSERS)
	c.PROPERTY_PARSERS = maps.Clone(s.PROPERTY_PARSERS)
	c.CONSTRAINT_PARSERS = maps.Clone(s.CONSTRAINT_PARSERS)
	c.ALTER_PARSERS = maps.Clone(s.ALTER_PARSERS)
	c.ALTER_ALTER_PARSERS = maps.Clone(s.ALTER_ALTER_PARSERS)
	c.NO_PAREN_FUNCTION_PARSERS = maps.Clone(s.NO_PAREN_FUNCTION_PARSERS)
	c.FUNCTION_PARSERS = maps.Clone(s.FUNCTION_PARSERS)
	c.QUERY_MODIFIER_PARSERS = maps.Clone(s.QUERY_MODIFIER_PARSERS)
	c.SET_PARSERS = maps.Clone(s.SET_PARSERS)
	c.SHOW_PARSERS = maps.Clone(s.SHOW_PARSERS)
	c.TYPE_LITERAL_PARSERS = maps.Clone(s.TYPE_LITERAL_PARSERS)
	c.TYPE_CONVERTERS = maps.Clone(s.TYPE_CONVERTERS)
	c.ANALYZE_EXPRESSION_PARSERS = maps.Clone(s.ANALYZE_EXPRESSION_PARSERS)
	c.DESCRIBE_QUALIFIER_PARSERS = maps.Clone(s.DESCRIBE_QUALIFIER_PARSERS)
	return &c
}
