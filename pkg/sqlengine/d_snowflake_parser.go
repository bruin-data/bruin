package sqlengine

// Port of sqlglot/parsers/snowflake.py (sqlglot v30.13.0): module-level builders, the callable
// tables of SnowflakeParser and its method overrides. Data-only class attributes (token sets,
// flags, FLATTEN_COLUMNS, UNDROP_OBJECTS, ...) live in the generated zz_parser_settings.go.

// ---------------------------------------------------------------------------------------------
// Module-level helpers
// ---------------------------------------------------------------------------------------------

// snowflakeKwargsKV flattens an insertion-ordered kwargs dict into alternating key/value pairs.
func snowflakeKwargsKV(b *builderKwargs) []any {
	kv := make([]any, 0, 2*len(b.keys))
	for _, k := range b.keys {
		kv = append(kv, k, b.vals[k])
	}
	return kv
}

// snowflakeIsDateUnit mirrors sqlglot.helper.is_date_unit.
func snowflakeIsDateUnit(e *Expr) bool {
	if e == nil {
		return false
	}
	switch pyLower(e.Name()) {
	case "day", "week", "month", "quarter", "year", "year_month":
		return true
	}
	return false
}

// _build_approx_top_k
//
// Normalizes APPROX_TOP_K arguments to match Snowflake semantics.
// Snowflake APPROX_TOP_K signature: APPROX_TOP_K(column [, k] [, counters])
// - k defaults to 1 if omitted (per Snowflake documentation)
// - counters is optional precision parameter
func snowflakeBuildApproxTopK(args []*Expr, _ *Dialect) *Expr {
	// Add default k=1 if only column is provided
	if len(args) == 1 {
		args = append(args, LiteralInt(1))
	}
	// args.append mutates the caller's list: validate arity against the mutated list.
	return WithValidateArgs(FromArgList(KApproxTopK, args), args)
}

// _build_to_number
func snowflakeBuildToNumber(args []*Expr, safe bool) *Expr {
	secondArg := seqGet(args, 1)
	var format, precision, scale *Expr
	if secondArg != nil && secondArg.IsNumber() {
		format = nil
		precision = secondArg
		scale = seqGet(args, 2)
		if scale == nil {
			scale = LiteralInt(0)
		}
	} else {
		format = secondArg
		precision = seqGet(args, 2)
		if precision == nil {
			precision = LiteralInt(38)
		}
		scale = seqGet(args, 3)
		if scale == nil {
			scale = LiteralInt(0)
		}
	}

	return New(
		KToNumber,
		"this", seqGet(args, 0),
		"format", format,
		"precision", precision,
		"scale", scale,
		"safe", safe,
	)
}

// _build_date_from_parts
func snowflakeBuildDateFromParts(args []*Expr, _ *Dialect) *Expr {
	return New(
		KDateFromParts,
		"year", seqGet(args, 0),
		"month", seqGet(args, 1),
		"day", seqGet(args, 2),
		"allow_overflow", true,
	)
}

// snowflakeTimestampTypes mirrors TIMESTAMP_TYPES (timestamp types used in _build_datetime).
var snowflakeTimestampTypes = map[DType]string{
	DT_TIMESTAMP:    "TO_TIMESTAMP",
	DT_TIMESTAMPLTZ: "TO_TIMESTAMP_LTZ",
	DT_TIMESTAMPNTZ: "TO_TIMESTAMP_NTZ",
	DT_TIMESTAMPTZ:  "TO_TIMESTAMP_TZ",
}

// _build_datetime
func snowflakeBuildDatetime(name string, kind DType, safe bool) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		value := seqGet(args, 0)
		scaleOrFmt := seqGet(args, 1)

		intValue := value != nil && isPyInt(value.Name())
		intScaleOrFmt := scaleOrFmt != nil && scaleOrFmt.IsInt()

		_, isTimestampType := snowflakeTimestampTypes[kind]

		if value.IsA(KLiteral, KNeg) || (value != nil && scaleOrFmt != nil) {
			// Converts calls like `TO_TIME('01:02:03')` into casts
			if len(args) == 1 && value.IsString() && !intValue {
				var cast *Expr
				if safe {
					cast = New(KTryCast, "this", value, "to", NewDataType(kind), "requires_string", true)
				} else {
					cast = CastExpr(value, kind, true, nil)
				}
				if safe && kind == DT_DATE {
					cast.Set("probe_date_format", true)
				}
				return cast
			}

			// Handles `TO_TIMESTAMP(str, fmt)` and `TO_TIMESTAMP(num, scale)` as special
			// cases so we can transpile them, since they're relatively common
			if isTimestampType {
				if !safe && (intScaleOrFmt || (intValue && scaleOrFmt == nil)) {
					// TRY_TO_TIMESTAMP('integer') is not parsed into exp.UnixToTime as
					// it's not easily transpilable. Also, numeric-looking strings with
					// format strings (e.g., TO_TIMESTAMP('20240115', 'YYYYMMDD')) should
					// use StrToTime, not UnixToTime.
					unixExpr := New(KUnixToTime, "this", value, "scale", scaleOrFmt)
					unixExpr.Set("target_type", NewDataType(kind))
					return unixExpr
				}
				if scaleOrFmt != nil && !intScaleOrFmt {
					// Format string provided (e.g., 'YYYY-MM-DD'), use StrToTime
					strtotimeExpr := buildFormattedTime(KStrToTime, "", nil)(args, d)
					strtotimeExpr.Set("safe", safe)
					strtotimeExpr.Set("target_type", NewDataType(kind))
					return strtotimeExpr
				}
			}
		}

		// Handle DATE/TIME with format strings - allow int_value if a format string is provided
		hasFormatString := scaleOrFmt != nil && !intScaleOrFmt
		if (kind == DT_DATE || kind == DT_TIME) && (!intValue || hasFormatString) {
			klass := KTsOrDsToTime
			if kind == DT_DATE {
				klass = KTsOrDsToDate
			}
			formattedExp := buildFormattedTime(klass, "", nil)(args, d)
			formattedExp.Set("safe", safe)
			return formattedExp
		}

		return New(KAnonymous, "this", name, "expressions", args)
	}
}

// _build_bitwise
func snowflakeBuildBitwise(kind Kind, name string) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		if len(args) == 3 {
			// Special handling for bitwise operations with padside argument
			if kind == KBitwiseAnd || kind == KBitwiseOr || kind == KBitwiseXor {
				return New(kind, "this", seqGet(args, 0), "expression", seqGet(args, 1), "padside", seqGet(args, 2))
			}
			return New(KAnonymous, "this", name, "expressions", args)
		}

		result := binaryFromFunction(kind)(args, d)

		// Snowflake specifies INT128 for bitwise shifts
		if kind == KBitwiseLeftShift || kind == KBitwiseRightShift {
			result.Set("requires_int128", true)
		}

		return result
	}
}

// _build_if_from_div0
// https://docs.snowflake.com/en/sql-reference/functions/div0
func snowflakeBuildIfFromDiv0(args []*Expr, _ *Dialect) *Expr {
	lhs := wrapIfKind(seqGet(args, 0), KBinary)
	rhs := wrapIfKind(seqGet(args, 1), KBinary)

	cond := New(KEQ, "this", rhs, "expression", LiteralInt(0)).ExprAnd(
		[]*Expr{NotExpr(New(KIs, "this", lhs, "expression", Null()), true)}, true, true,
	)
	trueV := LiteralInt(0)
	falseV := New(KDiv, "this", lhs, "expression", rhs)
	return New(KIf, "this", cond, "true", trueV, "false", falseV)
}

// _build_if_from_div0null
// https://docs.snowflake.com/en/sql-reference/functions/div0null
func snowflakeBuildIfFromDiv0Null(args []*Expr, _ *Dialect) *Expr {
	lhs := wrapIfKind(seqGet(args, 0), KBinary)
	rhs := wrapIfKind(seqGet(args, 1), KBinary)

	// Returns 0 when divisor is 0 OR NULL
	cond := New(KEQ, "this", rhs, "expression", LiteralInt(0)).ExprOr(
		[]*Expr{New(KIs, "this", rhs, "expression", Null())}, true, true,
	)
	trueV := LiteralInt(0)
	falseV := New(KDiv, "this", lhs, "expression", rhs)
	return New(KIf, "this", cond, "true", trueV, "false", falseV)
}

// _build_if_from_zeroifnull
// https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
func snowflakeBuildIfFromZeroIfNull(args []*Expr, _ *Dialect) *Expr {
	cond := New(KIs, "this", seqGet(args, 0), "expression", New(KNull))
	return New(KIf, "this", cond, "true", LiteralInt(0), "false", seqGet(args, 0))
}

// _build_search
func snowflakeBuildSearch(args []*Expr, _ *Dialect) *Expr {
	kw := newBuilderKwargs([]any{"this", seqGet(args, 0), "expression", seqGet(args, 1)})
	for _, arg := range argsFrom(args, 2) {
		if arg.IsA(KKwarg) {
			kw.set(pyLower(arg.Name()), arg)
		}
	}
	return New(KSearch, snowflakeKwargsKV(kw)...)
}

// _build_if_from_nullifzero
// https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
func snowflakeBuildIfFromNullIfZero(args []*Expr, _ *Dialect) *Expr {
	cond := New(KEQ, "this", seqGet(args, 0), "expression", LiteralInt(0))
	return New(KIf, "this", cond, "true", New(KNull), "false", seqGet(args, 0))
}

// _build_regexp_replace
func snowflakeBuildRegexpReplace(args []*Expr, _ *Dialect) *Expr {
	regexpReplace := FromArgList(KRegexpReplace, args)

	if !regexpReplace.ArgB("replacement") {
		regexpReplace.Set("replacement", LiteralString(""))
	}

	return regexpReplace
}

// _build_regexp_like
func snowflakeBuildRegexpLike(args []*Expr, _ *Dialect) *Expr {
	return New(
		KRegexpLike,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"flag", seqGet(args, 2),
		"full_match", true,
	)
}

// _date_trunc_to_time
func snowflakeDateTruncToTime(args []*Expr, d *Dialect) *Expr {
	trunc := dateTruncToTime(args, d)
	unit := mapDatePart(trunc.ArgE("unit"), nil)
	trunc.Set("unit", unit)
	if trunc.This() == nil {
		// trunc.this.is_type(...) raises AttributeError when `this` is missing
		panic(&ValueError{Msg: "'NoneType' object has no attribute 'is_type'"})
	}
	isTimeInput := dhIsType(trunc.This(), DT_TIME, DT_TIMETZ)
	if (trunc.IsA(KTimestampTrunc) && snowflakeIsDateUnit(unit) || isTimeInput) ||
		(trunc.IsA(KDateTrunc) && !snowflakeIsDateUnit(unit)) {
		trunc.Set("input_type_preserved", true)
	}
	return trunc
}

// _build_regexp_extract
func snowflakeBuildRegexpExtract(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		group := seqGet(args, 5)
		if group == nil {
			group = LiteralInt(0)
		}
		kv := []any{
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"position", seqGet(args, 2),
			"occurrence", seqGet(args, 3),
			"parameters", seqGet(args, 4),
			"group", group,
		}
		if kind == KRegexpExtract {
			kv = append(kv, "null_if_pos_overflow", d.S.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL)
		}
		return New(kind, kv...)
	}
}

// _build_timestamp_from_parts
//
// Build TimestampFromParts with support for both syntaxes:
// 1. TIMESTAMP_FROM_PARTS(year, month, day, hour, minute, second [, nanosecond] [, time_zone])
// 2. TIMESTAMP_FROM_PARTS(date_expr, time_expr) - Snowflake specific
func snowflakeBuildTimestampFromParts(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 2 {
		return New(KTimestampFromParts, "this", seqGet(args, 0), "expression", seqGet(args, 1))
	}
	return FromArgList(KTimestampFromParts, args)
}

// _build_round
//
// Build Round expression, unwrapping Snowflake's named parameters.
// Maps EXPR => this, SCALE => decimals, ROUNDING_MODE => truncate.
//
// Note: Snowflake does not support mixing named and positional arguments.
// Arguments are either all named or all positional.
func snowflakeBuildRound(args []*Expr, _ *Dialect) *Expr {
	kwargMap := map[string]string{"EXPR": "this", "SCALE": "decimals", "ROUNDING_MODE": "truncate"}
	roundArgs := newBuilderKwargs(nil)
	positionalKeys := []string{"this", "decimals", "truncate"}
	positionalIdx := 0

	for _, arg := range args {
		if arg.IsA(KKwarg) {
			key := pyUpper(arg.This().Name())
			if roundKey, ok := kwargMap[key]; ok && roundKey != "" {
				roundArgs.set(roundKey, arg.Expression())
			}
		} else if positionalIdx < len(positionalKeys) {
			roundArgs.set(positionalKeys[positionalIdx], arg)
			positionalIdx++
		}
	}

	expression := New(KRound, snowflakeKwargsKV(roundArgs)...)
	expression.Set("casts_non_integer_decimals", true)
	return expression
}

// _build_array_sort
func snowflakeBuildArraySort(args []*Expr, _ *Dialect) *Expr {
	asc := seqGet(args, 1)
	nullsFirst := seqGet(args, 2)
	if nullsFirst == nil && asc.IsA(KBoolean) {
		nullsFirst = Boolean(!asc.ArgB("this"))
	}
	return New(KSortArray, "this", seqGet(args, 0), "asc", asc, "nulls_first", nullsFirst)
}

// _build_generator
//
// Build Generator expression, unwrapping Snowflake's named parameters.
// Maps ROWCOUNT => rowcount, TIMELIMIT => timelimit.
func snowflakeBuildGenerator(args []*Expr, _ *Dialect) *Expr {
	kwargMap := map[string]string{"ROWCOUNT": "rowcount", "TIMELIMIT": "timelimit"}
	genArgs := newBuilderKwargs(nil)

	positionalKeys := []string{"rowcount", "timelimit"}

	for i, arg := range args {
		if arg.IsA(KKwarg) {
			key := pyUpper(arg.This().Name())
			if genKey, ok := kwargMap[key]; ok && genKey != "" {
				genArgs.set(genKey, arg.Expression())
			}
		} else if i < len(positionalKeys) {
			genArgs.set(positionalKeys[i], arg)
		}
	}

	return New(KGenerator, snowflakeKwargsKV(genArgs)...)
}

// _show_parser
func snowflakeShowParser(this string, terse, iceberg bool) parseFn {
	return func(p *Parser) *Expr {
		return snowflakeParseShowSnowflake(p, this, terse, iceberg)
	}
}

// snowflakeRankingWindowFunctionsWithFrame mirrors RANKING_WINDOW_FUNCTIONS_WITH_FRAME.
var snowflakeRankingWindowFunctionsWithFrame = []Kind{KFirstValue, KLastValue, KNthValue}

// build_object_construct
func snowflakeBuildObjectConstruct(args []*Expr) *Expr {
	expression := buildVarMap(args)

	if expression.IsA(KStarMap) {
		return expression
	}

	var keys, values []*Expr
	if k := expression.ArgE("keys"); k != nil {
		keys = k.Expressions()
	}
	if v := expression.ArgE("values"); v != nil {
		values = v.Expressions()
	}
	props := []*Expr{}
	for i := 0; i < len(keys) && i < len(values); i++ {
		props = append(props, New(KPropertyEQ, "this", keys[i], "expression", values[i]))
	}
	return New(KStruct, "expressions", props)
}

// ---------------------------------------------------------------------------------------------
// SnowflakeParser class body
// ---------------------------------------------------------------------------------------------

func customizeSnowflakeParser(d *Dialect) {
	P := d.P

	P.RANGE_PARSERS[TK_RLIKE] = func(p *Parser, this *Expr) *Expr {
		return p.expression(New(KRegexpLike, "this", this, "expression", p.parseBitwise(), "full_match", true))
	}

	// FUNCTIONS
	F := P.FUNCTIONS
	F["CHARINDEX"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KStrPosition,
			"this", seqGet(args, 1),
			"substr", seqGet(args, 0),
			"position", seqGet(args, 2),
			"clamp_position", true,
		)
	}
	F["ADD_MONTHS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KAddMonths,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"preserve_end_of_month", true,
		)
	}
	F["APPROX_PERCENTILE"] = fromArgList(KApproxQuantile)
	F["CURRENT_TIME"] = func(args []*Expr, _ *Dialect) *Expr { return New(KLocaltime, "this", seqGet(args, 0)) }
	F["APPROX_TOP_K"] = snowflakeBuildApproxTopK
	F["ARRAY_CONSTRUCT"] = func(args []*Expr, _ *Dialect) *Expr { return New(KArray, "expressions", args) }
	F["ARRAY_CONTAINS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KArrayContains,
			"this", seqGet(args, 1),
			"expression", seqGet(args, 0),
			"ensure_variant", false,
			"check_null", true,
		)
	}
	F["ARRAY_DISTINCT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArrayDistinct, "this", seqGet(args, 0), "check_null", true)
	}
	F["ARRAY_GENERATE_RANGE"] = snowflakeArrayGenerateRange
	F["ARRAY_EXCEPT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArrayExcept, "this", seqGet(args, 0), "expression", seqGet(args, 1), "is_multiset", true)
	}
	F["ARRAY_INTERSECTION"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArrayIntersect, "expressions", args, "is_multiset", true)
	}
	F["ARRAY_POSITION"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArrayPosition, "this", seqGet(args, 1), "expression", seqGet(args, 0), "zero_based", true)
	}
	F["ARRAY_SLICE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KArraySlice,
			"this", seqGet(args, 0),
			"start", seqGet(args, 1),
			"end", seqGet(args, 2),
			"zero_based", true,
		)
	}
	F["ARRAY_SORT"] = snowflakeBuildArraySort
	F["ARRAY_FLATTEN"] = fromArgList(KFlatten)
	F["ARRAY_TO_STRING"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KArrayToString,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"null_is_empty", true,
			"null_delim_is_null", true,
		)
	}
	F["ARRAYS_OVERLAP"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArrayOverlaps, "this", seqGet(args, 0), "expression", seqGet(args, 1), "null_safe", true)
	}
	F["BITAND"] = snowflakeBuildBitwise(KBitwiseAnd, "BITAND")
	F["BIT_AND"] = snowflakeBuildBitwise(KBitwiseAnd, "BITAND")
	F["BITNOT"] = func(args []*Expr, _ *Dialect) *Expr { return New(KBitwiseNot, "this", seqGet(args, 0)) }
	F["BIT_NOT"] = func(args []*Expr, _ *Dialect) *Expr { return New(KBitwiseNot, "this", seqGet(args, 0)) }
	F["BITXOR"] = snowflakeBuildBitwise(KBitwiseXor, "BITXOR")
	F["BIT_XOR"] = snowflakeBuildBitwise(KBitwiseXor, "BITXOR")
	F["BITOR"] = snowflakeBuildBitwise(KBitwiseOr, "BITOR")
	F["BIT_OR"] = snowflakeBuildBitwise(KBitwiseOr, "BITOR")
	F["BITSHIFTLEFT"] = snowflakeBuildBitwise(KBitwiseLeftShift, "BITSHIFTLEFT")
	F["BIT_SHIFTLEFT"] = snowflakeBuildBitwise(KBitwiseLeftShift, "BIT_SHIFTLEFT")
	F["BITSHIFTRIGHT"] = snowflakeBuildBitwise(KBitwiseRightShift, "BITSHIFTRIGHT")
	F["BIT_SHIFTRIGHT"] = snowflakeBuildBitwise(KBitwiseRightShift, "BIT_SHIFTRIGHT")
	for _, name := range []string{"BITANDAGG", "BITAND_AGG", "BIT_AND_AGG", "BIT_ANDAGG"} {
		F[name] = fromArgList(KBitwiseAndAgg)
	}
	for _, name := range []string{"BITORAGG", "BITOR_AGG", "BIT_OR_AGG", "BIT_ORAGG"} {
		F[name] = fromArgList(KBitwiseOrAgg)
	}
	for _, name := range []string{"BITXORAGG", "BITXOR_AGG", "BIT_XOR_AGG", "BIT_XORAGG"} {
		F[name] = fromArgList(KBitwiseXorAgg)
	}
	F["BITMAP_OR_AGG"] = fromArgList(KBitmapOrAgg)
	F["BOOLAND"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KBooland, "this", seqGet(args, 0), "expression", seqGet(args, 1), "round_input", true)
	}
	F["BOOLOR"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KBoolor, "this", seqGet(args, 0), "expression", seqGet(args, 1), "round_input", true)
	}
	F["BOOLNOT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KBoolnot, "this", seqGet(args, 0), "round_input", true)
	}
	F["BOOLXOR"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KXor, "this", seqGet(args, 0), "expression", seqGet(args, 1), "round_input", true)
	}
	F["CORR"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KCorr, "this", seqGet(args, 0), "expression", seqGet(args, 1), "null_on_zero_variance", true)
	}
	F["COUNT_IF"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KCountIf, "this", seqGet(args, 0), "zero_on_all_null", true)
	}
	F["DATE"] = snowflakeBuildDatetime("DATE", DT_DATE, false)
	F["DATEFROMPARTS"] = snowflakeBuildDateFromParts
	F["DATE_FROM_PARTS"] = snowflakeBuildDateFromParts
	F["DATE_TRUNC"] = snowflakeDateTruncToTime
	F["DATEADD"] = snowflakeBuildDateTimeAdd(KDateAdd)
	F["DATEDIFF"] = snowflakeBuildDatediff
	F["DAYNAME"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KDayname, "this", seqGet(args, 0), "abbreviated", true)
	}
	F["DAYOFWEEKISO"] = fromArgList(KDayOfWeekIso)
	F["DIV0"] = snowflakeBuildIfFromDiv0
	F["DIV0NULL"] = snowflakeBuildIfFromDiv0Null
	F["EDITDISTANCE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KLevenshtein, "this", seqGet(args, 0), "expression", seqGet(args, 1), "max_dist", seqGet(args, 2))
	}
	F["FLATTEN"] = fromArgList(KExplode)
	F["GENERATOR"] = snowflakeBuildGenerator
	F["GET"] = fromArgList(KGetExtract)
	F["GETDATE"] = fromArgList(KCurrentTimestamp)
	F["GET_PATH"] = func(args []*Expr, d *Dialect) *Expr {
		return New(
			KJSONExtract,
			"this", seqGet(args, 0),
			"expression", d.toJSONPath(seqGet(args, 1)),
			"requires_json", true,
		)
	}
	F["GREATEST_IGNORE_NULLS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KGreatest, "this", seqGet(args, 0), "expressions", argsFrom(args, 1), "ignore_nulls", true)
	}
	F["LEAST_IGNORE_NULLS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KLeast, "this", seqGet(args, 0), "expressions", argsFrom(args, 1), "ignore_nulls", true)
	}
	F["LEFT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KLeft, "this", seqGet(args, 0), "expression", seqGet(args, 1), "negative_length_returns_empty", true)
	}
	F["HEX_DECODE_BINARY"] = fromArgList(KUnhex)
	F["HEX_ENCODE"] = fromArgList(KHex)
	F["IFF"] = fromArgList(KIf)
	F["JAROWINKLER_SIMILARITY"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KJarowinklerSimilarity,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"case_insensitive", true,
			"integer_scale", true,
		)
	}
	F["MD5_HEX"] = fromArgList(KMD5)
	F["MD5_BINARY"] = fromArgList(KMD5Digest)
	F["MD5_NUMBER_LOWER64"] = fromArgList(KMD5NumberLower64)
	F["MD5_NUMBER_UPPER64"] = fromArgList(KMD5NumberUpper64)
	F["MONTHNAME"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KMonthname, "this", seqGet(args, 0), "abbreviated", true)
	}
	F["LAST_DAY"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KLastDay, "this", seqGet(args, 0), "unit", mapDatePart(seqGet(args, 1), nil))
	}
	F["LEN"] = func(args []*Expr, _ *Dialect) *Expr { return New(KLength, "this", seqGet(args, 0), "binary", true) }
	F["LENGTH"] = func(args []*Expr, _ *Dialect) *Expr { return New(KLength, "this", seqGet(args, 0), "binary", true) }
	F["LOCALTIMESTAMP"] = fromArgList(KCurrentTimestamp)
	F["NULLIFZERO"] = snowflakeBuildIfFromNullIfZero
	F["OBJECT_CONSTRUCT"] = func(args []*Expr, _ *Dialect) *Expr { return snowflakeBuildObjectConstruct(args) }
	F["OBJECT_KEYS"] = fromArgList(KJSONKeys)
	F["OCTET_LENGTH"] = fromArgList(KByteLength)
	F["PARSE_URL"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KParseUrl, "this", seqGet(args, 0), "permissive", seqGet(args, 1))
	}
	F["REGEXP_EXTRACT_ALL"] = snowflakeBuildRegexpExtract(KRegexpExtractAll)
	F["REGEXP_LIKE"] = snowflakeBuildRegexpLike
	F["REGEXP_REPLACE"] = snowflakeBuildRegexpReplace
	F["REGEXP_SUBSTR"] = snowflakeBuildRegexpExtract(KRegexpExtract)
	F["REGEXP_SUBSTR_ALL"] = snowflakeBuildRegexpExtract(KRegexpExtractAll)
	F["RANDOM"] = func(args []*Expr, _ *Dialect) *Expr {
		// lower=exp.Literal.number(-9223372036854775808.0), upper=exp.Literal.number(9223372036854775807.0)
		// (-2^63 / 2^63-1 as floats to avoid overflow). str(float) gives '-9.223372036854776e+18' and
		// Literal.number stores str(abs(Decimal(...))) for negatives, hence the different casing.
		return New(
			KRand,
			"this", seqGet(args, 0),
			"lower", New(KNeg, "this", New(KLiteral, "this", "9.223372036854776E+18", "is_string", false)),
			"upper", New(KLiteral, "this", "9.223372036854776e+18", "is_string", false),
		)
	}
	F["REPLACE"] = buildReplaceWithOptionalReplacement
	F["RIGHT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KRight, "this", seqGet(args, 0), "expression", seqGet(args, 1), "negative_length_returns_empty", true)
	}
	F["RLIKE"] = snowflakeBuildRegexpLike
	F["ROUND"] = snowflakeBuildRound
	F["SHA1_BINARY"] = fromArgList(KSHA1Digest)
	F["SHA1_HEX"] = fromArgList(KSHA)
	F["SHA2_BINARY"] = fromArgList(KSHA2Digest)
	F["SHA2_HEX"] = fromArgList(KSHA2)
	F["SPLIT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KSplit,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"null_returns_null", true,
			"empty_delimiter_returns_whole", true,
		)
	}
	F["SQUARE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KPow, "this", seqGet(args, 0), "expression", LiteralInt(2))
	}
	F["STDDEV_SAMP"] = fromArgList(KStddev)
	F["SYSDATE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KCurrentTimestamp, "this", seqGet(args, 0), "sysdate", true)
	}
	F["TABLE"] = func(args []*Expr, _ *Dialect) *Expr { return New(KTableFromRows, "this", seqGet(args, 0)) }
	F["TIMEADD"] = snowflakeBuildDateTimeAdd(KTimeAdd)
	F["TIMEDIFF"] = snowflakeBuildDatediff
	F["TIME_FROM_PARTS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KTimeFromParts,
			"hour", seqGet(args, 0),
			"min", seqGet(args, 1),
			"sec", seqGet(args, 2),
			"nano", seqGet(args, 3),
			"overflow", true,
		)
	}
	F["TIMESTAMPADD"] = snowflakeBuildDateTimeAdd(KDateAdd)
	F["TIMESTAMPDIFF"] = snowflakeBuildDatediff
	F["TIMESTAMPFROMPARTS"] = snowflakeBuildTimestampFromParts
	F["TIMESTAMP_FROM_PARTS"] = snowflakeBuildTimestampFromParts
	F["TIMESTAMPNTZFROMPARTS"] = snowflakeBuildTimestampFromParts
	F["TIMESTAMP_NTZ_FROM_PARTS"] = snowflakeBuildTimestampFromParts
	F["TRUNC"] = func(args []*Expr, d *Dialect) *Expr { return buildTrunc(args, d, true, "", false, true) }
	F["TRUNCATE"] = func(args []*Expr, d *Dialect) *Expr { return buildTrunc(args, d, true, "", false, true) }
	F["TRY_DECRYPT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KDecrypt,
			"this", seqGet(args, 0),
			"passphrase", seqGet(args, 1),
			"aad", seqGet(args, 2),
			"encryption_method", seqGet(args, 3),
			"safe", true,
		)
	}
	F["TRY_DECRYPT_RAW"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KDecryptRaw,
			"this", seqGet(args, 0),
			"key", seqGet(args, 1),
			"iv", seqGet(args, 2),
			"aad", seqGet(args, 3),
			"encryption_method", seqGet(args, 4),
			"aead", seqGet(args, 5),
			"safe", true,
		)
	}
	F["TRY_PARSE_JSON"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KParseJSON, "this", seqGet(args, 0), "safe", true)
	}
	F["TRY_TO_BINARY"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KToBinary, "this", seqGet(args, 0), "format", seqGet(args, 1), "safe", true)
	}
	F["TRY_TO_BOOLEAN"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KToBoolean, "this", seqGet(args, 0), "safe", true)
	}
	F["TRY_TO_DATE"] = snowflakeBuildDatetime("TRY_TO_DATE", DT_DATE, true)
	for _, name := range []string{"TRY_TO_DECIMAL", "TRY_TO_NUMBER", "TRY_TO_NUMERIC"} {
		F[name] = func(args []*Expr, _ *Dialect) *Expr { return snowflakeBuildToNumber(args, true) }
	}
	F["TRY_TO_DOUBLE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KToDouble, "this", seqGet(args, 0), "format", seqGet(args, 1), "safe", true)
	}
	F["TRY_TO_FILE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KToFile, "this", seqGet(args, 0), "path", seqGet(args, 1), "safe", true)
	}
	F["TRY_TO_TIME"] = snowflakeBuildDatetime("TRY_TO_TIME", DT_TIME, true)
	F["TRY_TO_TIMESTAMP"] = snowflakeBuildDatetime("TRY_TO_TIMESTAMP", DT_TIMESTAMP, true)
	F["TRY_TO_TIMESTAMP_LTZ"] = snowflakeBuildDatetime("TRY_TO_TIMESTAMP_LTZ", DT_TIMESTAMPLTZ, true)
	F["TRY_TO_TIMESTAMP_NTZ"] = snowflakeBuildDatetime("TRY_TO_TIMESTAMP_NTZ", DT_TIMESTAMPNTZ, true)
	F["TRY_TO_TIMESTAMP_TZ"] = snowflakeBuildDatetime("TRY_TO_TIMESTAMP_TZ", DT_TIMESTAMPTZ, true)
	F["TO_CHAR"] = buildTimetostrOrTochar
	F["TO_DATE"] = snowflakeBuildDatetime("TO_DATE", DT_DATE, false)
	for _, name := range []string{"TO_DECIMAL", "TO_NUMBER", "TO_NUMERIC"} {
		F[name] = func(args []*Expr, _ *Dialect) *Expr { return snowflakeBuildToNumber(args, false) }
	}
	F["TO_TIME"] = snowflakeBuildDatetime("TO_TIME", DT_TIME, false)
	F["TO_TIMESTAMP"] = snowflakeBuildDatetime("TO_TIMESTAMP", DT_TIMESTAMP, false)
	F["TO_TIMESTAMP_LTZ"] = snowflakeBuildDatetime("TO_TIMESTAMP_LTZ", DT_TIMESTAMPLTZ, false)
	F["TO_TIMESTAMP_NTZ"] = snowflakeBuildDatetime("TO_TIMESTAMP_NTZ", DT_TIMESTAMPNTZ, false)
	F["TO_TIMESTAMP_TZ"] = snowflakeBuildDatetime("TO_TIMESTAMP_TZ", DT_TIMESTAMPTZ, false)
	F["TO_GEOGRAPHY"] = func(args []*Expr, _ *Dialect) *Expr {
		if len(args) == 1 {
			return CastExpr(args[0], DT_GEOGRAPHY, true, nil)
		}
		return New(KAnonymous, "this", "TO_GEOGRAPHY", "expressions", args)
	}
	F["TO_GEOMETRY"] = func(args []*Expr, _ *Dialect) *Expr {
		if len(args) == 1 {
			return CastExpr(args[0], DT_GEOMETRY, true, nil)
		}
		return New(KAnonymous, "this", "TO_GEOMETRY", "expressions", args)
	}
	F["TO_VARCHAR"] = buildTimetostrOrTochar
	F["TO_JSON"] = fromArgList(KJSONFormat)
	F["VECTOR_COSINE_SIMILARITY"] = fromArgList(KCosineDistance)
	F["VECTOR_INNER_PRODUCT"] = fromArgList(KDotProduct)
	F["VECTOR_L1_DISTANCE"] = fromArgList(KManhattanDistance)
	F["VECTOR_L2_DISTANCE"] = fromArgList(KEuclideanDistance)
	F["ZEROIFNULL"] = snowflakeBuildIfFromZeroIfNull
	F["LIKE"] = dialectBuildLike(KLike, false)
	F["ILIKE"] = dialectBuildLike(KILike, false)
	F["SEARCH"] = snowflakeBuildSearch
	F["SKEW"] = fromArgList(KSkewness)
	F["SPLIT_PART"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KSplitPart,
			"this", seqGet(args, 0),
			"delimiter", seqGet(args, 1),
			"part_index", seqGet(args, 2),
			"part_index_zero_as_one", true,
			"empty_delimiter_returns_whole", true,
		)
	}
	F["STRTOK"] = func(args []*Expr, _ *Dialect) *Expr {
		delimiter := seqGet(args, 1)
		if delimiter == nil {
			delimiter = LiteralString(" ")
		}
		partIndex := seqGet(args, 2)
		if partIndex == nil {
			partIndex = LiteralNumber("1")
		}
		return New(KStrtok, "this", seqGet(args, 0), "delimiter", delimiter, "part_index", partIndex)
	}
	F["STRTOK_TO_ARRAY"] = func(args []*Expr, _ *Dialect) *Expr {
		expression := seqGet(args, 1)
		if expression == nil {
			expression = LiteralString(" ")
		}
		return New(KStrtokToArray, "this", seqGet(args, 0), "expression", expression)
	}
	F["SYSTIMESTAMP"] = fromArgList(KCurrentTimestamp)
	F["IDENTIFIER"] = fromArgList(KDynamicIdentifier)
	F["UNICODE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnicode, "this", seqGet(args, 0), "empty_is_zero", true)
	}
	F["WEEKISO"] = fromArgList(KWeekOfYear)
	F["WEEKOFYEAR"] = fromArgList(KWeek)
	delete(F, "PREDICT")

	// FUNCTION_PARSERS
	FP := P.FUNCTION_PARSERS
	FP["DATE_PART"] = snowflakeParseDatePart
	FP["DIRECTORY"] = snowflakeParseDirectory
	FP["OBJECT_CONSTRUCT_KEEP_NULL"] = func(p *Parser) *Expr { return p.parseJsonObject(false) }
	FP["LISTAGG"] = func(p *Parser) *Expr { return p.parseStringAgg() }
	FP["SEMANTIC_VIEW"] = snowflakeParseSemanticView
	FP["SUBSTR"] = func(p *Parser) *Expr { return p.parseSubstring() }
	delete(FP, "TRIM")

	// ALTER_PARSERS
	P.ALTER_PARSERS["MODIFY"] = func(p *Parser) any { return anyExpr(p.parseAlterTableAlter()) }
	P.ALTER_PARSERS["SESSION"] = func(p *Parser) any { return anyExpr(p.parseAlterSession()) }
	P.ALTER_PARSERS["UNSET"] = func(p *Parser) any {
		tag := p.matchTextSeq("TAG")
		expressions := p.parseCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA)
		return anyExpr(p.expression(New(KSet, "tag", tag, "expressions", expressions, "unset", true)))
	}

	// STATEMENT_PARSERS
	P.STATEMENT_PARSERS[TK_GET] = snowflakeParseGet
	P.STATEMENT_PARSERS[TK_PUT] = snowflakeParsePut
	P.STATEMENT_PARSERS[TK_SHOW] = func(p *Parser) *Expr { return p.parseShow() }
	P.STATEMENT_PARSERS[TK_UNDROP] = snowflakeParseUndrop

	// PROPERTY_PARSERS
	P.PROPERTY_PARSERS["CREDENTIALS"] = noKwargsE(snowflakeParseCredentialsProperty)
	P.PROPERTY_PARSERS["FILE_FORMAT"] = noKwargsE(snowflakeParseFileFormatProperty)
	P.PROPERTY_PARSERS["LOCATION"] = noKwargsE(snowflakeParseLocationProperty)
	P.PROPERTY_PARSERS["ROW"] = noKwargsE(func(p *Parser) *Expr {
		if p.matchTextSeq("ACCESS", "POLICY") {
			return snowflakeParseRowAccessPolicy(p)
		}
		return p.parseRow()
	})
	P.PROPERTY_PARSERS["TAG"] = noKwargsE(snowflakeParseTag)
	P.PROPERTY_PARSERS["USING"] = noKwargsE(func(p *Parser) *Expr {
		if !p.matchTextSeq("TEMPLATE") {
			return nil
		}
		return p.expression(New(KUsingTemplateProperty, "this", p.parseStatement()))
	})

	// DESCRIBE_QUALIFIER_PARSERS
	P.DESCRIBE_QUALIFIER_PARSERS = map[string]parseFn{
		"API":         func(p *Parser) *Expr { return p.expression(New(KApiProperty)) },
		"APPLICATION": func(p *Parser) *Expr { return p.expression(New(KApplicationProperty)) },
		"CATALOG":     func(p *Parser) *Expr { return p.expression(New(KCatalogProperty)) },
		"COMPUTE":     func(p *Parser) *Expr { return p.expression(New(KComputeProperty)) },
		"DATABASE": func(p *Parser) *Expr {
			if p.curr.ok() && upperText(p.curr) == "ROLE" {
				return p.expression(New(KDatabaseProperty))
			}
			return nil
		},
		"DYNAMIC":      func(p *Parser) *Expr { return p.expression(New(KDynamicProperty)) },
		"EXTERNAL":     func(p *Parser) *Expr { return p.expression(New(KExternalProperty)) },
		"HYBRID":       func(p *Parser) *Expr { return p.expression(New(KHybridProperty)) },
		"ICEBERG":      func(p *Parser) *Expr { return p.expression(New(KIcebergProperty)) },
		"MASKING":      func(p *Parser) *Expr { return p.expression(New(KMaskingProperty)) },
		"MATERIALIZED": func(p *Parser) *Expr { return p.expression(New(KMaterializedProperty)) },
		"NETWORK":      func(p *Parser) *Expr { return p.expression(New(KNetworkProperty)) },
		"ROW": func(p *Parser) *Expr {
			if p.matchTextSeq("ACCESS") {
				return p.expression(New(KRowAccessProperty))
			}
			return nil
		},
		"SECURITY": func(p *Parser) *Expr {
			if p.curr.ok() && upperText(p.curr) == "INTEGRATION" {
				return p.expression(New(KSecurityIntegrationProperty))
			}
			return nil
		},
	}

	// TYPE_CONVERTERS (redefined, not merged)
	P.TYPE_CONVERTERS = map[DType]typeConverterFn{
		// https://docs.snowflake.com/en/sql-reference/data-types-numeric#number
		DT_DECIMAL: buildDefaultDecimalType(38, 0),
	}

	// SHOW_PARSERS (redefined, not merged); SHOW_TRIE is rebuilt by setupDialectSettings.
	P.SHOW_PARSERS = map[string]parseFn{
		"DATABASES":            snowflakeShowParser("DATABASES", false, false),
		"SCHEMAS":              snowflakeShowParser("SCHEMAS", false, false),
		"OBJECTS":              snowflakeShowParser("OBJECTS", false, false),
		"TABLES":               snowflakeShowParser("TABLES", false, false),
		"VIEWS":                snowflakeShowParser("VIEWS", false, false),
		"PRIMARY KEYS":         snowflakeShowParser("PRIMARY KEYS", false, false),
		"IMPORTED KEYS":        snowflakeShowParser("IMPORTED KEYS", false, false),
		"UNIQUE KEYS":          snowflakeShowParser("UNIQUE KEYS", false, false),
		"SEQUENCES":            snowflakeShowParser("SEQUENCES", false, false),
		"STAGES":               snowflakeShowParser("STAGES", false, false),
		"COLUMNS":              snowflakeShowParser("COLUMNS", false, false),
		"USERS":                snowflakeShowParser("USERS", false, false),
		"FILE FORMATS":         snowflakeShowParser("FILE FORMATS", false, false),
		"FUNCTIONS":            snowflakeShowParser("FUNCTIONS", false, false),
		"PROCEDURES":           snowflakeShowParser("PROCEDURES", false, false),
		"WAREHOUSES":           snowflakeShowParser("WAREHOUSES", false, false),
		"ICEBERG TABLES":       snowflakeShowParser("TABLES", false, true),
		"TERSE ICEBERG TABLES": snowflakeShowParser("TABLES", true, true),
		"TERSE DATABASES":      snowflakeShowParser("DATABASES", true, false),
		"TERSE SCHEMAS":        snowflakeShowParser("SCHEMAS", true, false),
		"TERSE OBJECTS":        snowflakeShowParser("OBJECTS", true, false),
		"TERSE TABLES":         snowflakeShowParser("TABLES", true, false),
		"TERSE VIEWS":          snowflakeShowParser("VIEWS", true, false),
		"TERSE SEQUENCES":      snowflakeShowParser("SEQUENCES", true, false),
		"TERSE USERS":          snowflakeShowParser("USERS", true, false),
		// TERSE has no semantic effect for KEYS, so we do not set the terse AST arg
		"TERSE PRIMARY KEYS":  snowflakeShowParser("PRIMARY KEYS", false, false),
		"TERSE IMPORTED KEYS": snowflakeShowParser("IMPORTED KEYS", false, false),
		"TERSE UNIQUE KEYS":   snowflakeShowParser("UNIQUE KEYS", false, false),
	}

	// CONSTRAINT_PARSERS
	for _, k := range []string{"WITH", "MASKING", "PROJECTION", "TAG"} {
		P.CONSTRAINT_PARSERS[k] = snowflakeParseWithConstraint
	}

	// LAMBDAS
	P.LAMBDAS[TK_ARROW] = func(p *Parser, expressions []*Expr) *Expr {
		this := p.replaceLambda(p.parseAssignment(), expressions)
		args := make([]*Expr, 0, len(expressions))
		for _, e := range expressions {
			if e.IsA(KCast) {
				args = append(args, e.This())
			} else {
				args = append(args, e)
			}
		}
		return p.expression(New(KLambda, "this", this, "expressions", args))
	}

	// COLUMN_OPERATORS
	P.COLUMN_OPERATORS[TK_EXCLAMATION] = func(p *Parser, this, attr *Expr) *Expr {
		return p.expression(New(KModelAttribute, "this", this, "expression", attr))
	}

	// Method overrides
	P.h.parseDescribe = snowflakeParseDescribe
	P.h.parseUse = snowflakeParseUse
	P.h.negateRange = snowflakeNegateRange
	P.h.parsePropertyBefore = snowflakeParsePropertyBefore
	P.h.parseWithProperty = snowflakeParseWithProperty
	P.h.parseCreate = snowflakeParseCreate
	P.h.parseBracketKeyValue = snowflakeParseBracketKeyValue
	P.h.parseLateral = snowflakeParseLateral
	P.h.parseTableParts = snowflakeParseTableParts
	P.h.parseTable = snowflakeParseTable
	P.h.parseFunctionCall = snowflakeParseFunctionCall
	P.h.parseIdVar = snowflakeParseIdVar
	P.h.parseFileLocation = snowflakeParseFileLocation
	P.h.parseLambdaArg = snowflakeParseLambdaArg
	P.h.parseForeignKey = snowflakeParseForeignKey
	P.h.parseSet = snowflakeParseSet
	P.h.parsePosition = snowflakeParsePosition
	P.h.parseSubstring = snowflakeParseSubstring
	P.h.buildCast = snowflakeBuildCast
	P.h.parseWindow = snowflakeParseWindow
}

// snowflakeArrayGenerateRange mirrors SnowflakeParser.FUNCTIONS["ARRAY_GENERATE_RANGE"].
func snowflakeArrayGenerateRange(args []*Expr, _ *Dialect) *Expr {
	return New(
		KGenerateSeries,
		// Snowflake has exclusive end semantics
		"start", seqGet(args, 0),
		"end", seqGet(args, 1),
		"step", seqGet(args, 2),
		"is_end_exclusive", true,
	)
}

// snowflakeBuildDatediff mirrors the DATEDIFF/TIMEDIFF/TIMESTAMPDIFF lambdas (and the
// generator module's _build_datediff).
func snowflakeBuildDatediff(args []*Expr, _ *Dialect) *Expr {
	return New(
		KDateDiff,
		"this", seqGet(args, 2),
		"expression", seqGet(args, 1),
		"unit", mapDatePart(seqGet(args, 0), nil),
		"date_part_boundary", true,
	)
}

// snowflakeBuildDateTimeAdd mirrors the DATEADD/TIMEADD/TIMESTAMPADD lambdas (and the generator
// module's _build_date_time_add).
func snowflakeBuildDateTimeAdd(kind Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		return New(
			kind,
			"this", seqGet(args, 2),
			"expression", seqGet(args, 1),
			"unit", mapDatePart(seqGet(args, 0), nil),
		)
	}
}

// ---------------------------------------------------------------------------------------------
// SnowflakeParser methods
// ---------------------------------------------------------------------------------------------

// _parse_directory
func snowflakeParseDirectory(p *Parser) *Expr {
	table := p.parseTableParts(false, false, false, false)
	this := table
	if table.IsA(KTable) {
		this = table.This()
	}
	return p.expression(New(KDirectoryStage, "this", this))
}

// _parse_describe
func snowflakeParseDescribe(p *Parser) *Expr {
	index := p.index

	if matchTextKeys(p, p.s.DESCRIBE_QUALIFIER_PARSERS) {
		qualifier := p.s.DESCRIBE_QUALIFIER_PARSERS[upperText(p.prev)](p)

		if qualifier != nil {
			kind := ""
			if p.matchSet(p.s.CREATABLES) {
				kind = upperText(p.prev)
			}

			if kind != "" {
				this := p.parseTable(true, false, nil, false, false, false, false)
				properties := p.expression(New(KProperties, "expressions", []*Expr{qualifier}))
				postProps := p.parseProperties(false)
				var expressions any
				if postProps != nil {
					expressions = postProps.Expressions()
				}
				return p.expression(New(
					KDescribe,
					"this", this,
					"kind", kind,
					"properties", properties,
					"expressions", expressions,
				))
			}
		}
	}

	p.retreat(index)
	return p.baseParseDescribe()
}

// _parse_use
func snowflakeParseUse(p *Parser) *Expr {
	if p.matchTextSeq("SECONDARY", "ROLES") {
		// self._match_texts(("ALL", "NONE")) and exp.var(...) -> False when unmatched
		var this any = false
		if p.matchTexts("ALL", "NONE") {
			this = VarChecked(upperText(p.prev))
		}
		var roles any
		if this == false {
			roles = p.parseCSV(func() *Expr { return p.parseTable(false, false, nil, false, false, false, false) }, TK_COMMA)
		}
		return p.expression(New(KUse, "kind", "SECONDARY ROLES", "this", this, "expressions", roles))
	}

	return p.baseParseUse()
}

// _negate_range
func snowflakeNegateRange(p *Parser, this *Expr) *Expr {
	if this == nil {
		return this
	}

	query := this.ArgE("query")
	if this.IsA(KIn) && query.IsA(KQuery) {
		// Snowflake treats `value NOT IN (subquery)` as `VALUE <> ALL (subquery)`, so
		// we do this conversion here to avoid parsing it into `NOT value IN (subquery)`
		// which can produce different results (most likely a SnowFlake bug).
		//
		// https://docs.snowflake.com/en/sql-reference/functions/in
		// Context: https://github.com/tobymao/sqlglot/issues/3890
		return p.expression(New(KNEQ, "this", this.This(), "expression", New(KAll, "this", query.UnnestSubqueryOrParen())))
	}

	return p.expression(New(KNot, "this", this))
}

// _parse_tag
func snowflakeParseTag(p *Parser) *Expr {
	items := parseWrappedAny(p, func() []any {
		return parseCSVAny(p, p.parseProperty, TK_COMMA, chunkAIsNone)
	}, false)
	exprs := []*Expr{}
	for _, item := range items {
		switch v := item.(type) {
		case []*Expr:
			exprs = append(exprs, v...)
		case *Expr:
			exprs = append(exprs, v)
		}
	}
	return p.expression(New(KTags, "expressions", exprs))
}

// _parse_property_before
func snowflakeParsePropertyBefore(p *Parser) any {
	prop := p.baseParsePropertyBefore()
	if truthy(prop) {
		return prop
	}

	if !p.next.ok() || p.next.Type != TK_EQ {
		return nil
	}

	if e := p.parseSequenceProperties(); e != nil {
		return e
	}
	return anyExpr(p.parseKeyValueProperty(func() *Expr { return p.parsePrimaryOrVar() }))
}

// _parse_with_constraint
func snowflakeParseWithConstraint(p *Parser) *Expr {
	if p.prev.Type != TK_WITH {
		p.retreat(p.index - 1)
	}

	if p.matchTextSeq("MASKING", "POLICY") {
		policy := p.parseColumn()
		this := policy
		if policy.IsA(KColumn) {
			this = policy.ToDot(true)
		}
		// self._match(TokenType.USING) and self._parse_wrapped_csv(...) -> False when unmatched
		var expressions any = false
		if p.match(TK_USING) {
			expressions = p.parseWrappedCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA, false)
		}
		return p.expression(New(KMaskingPolicyColumnConstraint, "this", this, "expressions", expressions))
	}
	if p.matchTextSeq("PROJECTION", "POLICY") {
		policy := p.parseColumn()
		this := policy
		if policy.IsA(KColumn) {
			this = policy.ToDot(true)
		}
		return p.expression(New(KProjectionPolicyColumnConstraint, "this", this))
	}
	if p.match(TK_TAG) {
		return snowflakeParseTag(p)
	}

	return nil
}

// _parse_with_property
func snowflakeParseWithProperty(p *Parser) any {
	if p.match(TK_TAG) {
		return anyExpr(snowflakeParseTag(p))
	}

	if p.matchTextSeq("ROW", "ACCESS", "POLICY") {
		return anyExpr(snowflakeParseRowAccessPolicy(p))
	}

	return p.baseParseWithProperty()
}

// _parse_row_access_policy
func snowflakeParseRowAccessPolicy(p *Parser) *Expr {
	var policy *Expr
	var expressions any
	// GET_DDL outputs #unknown_policy when the user lacks privileges to see the policy name
	if p.match(TK_HASH) {
		policy = p.parseVar(true, nil, false)
		if policy != nil {
			policy = New(KVar, "this", "#"+policy.Name())
		}
		expressions = nil
	} else {
		policy = p.parseColumn()
		if policy.IsA(KColumn) {
			policy = policy.ToDot(true)
		}
		if !p.match(TK_ON) {
			p.raiseError("Expected ON after ROW ACCESS POLICY name", nil)
		}
		expressions = p.parseWrappedCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA, false)
	}

	return p.expression(New(KRowAccessProperty, "this", policy, "expressions", expressions))
}

// _parse_create
func snowflakeParseCreate(p *Parser) *Expr {
	expression := p.baseParseCreate()
	if expression.IsA(KCreate) && p.s.NON_TABLE_CREATABLES.Has(expression.KindText()) {
		// Replace the Table node with the enclosed Identifier
		expression.This().Replace(expression.This().This())
	}

	return expression
}

// _parse_date_part
// https://docs.snowflake.com/en/sql-reference/functions/date_part.html
// https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts
func snowflakeParseDatePart(p *Parser) *Expr {
	this := p.parseVar(false, nil, false)
	if this == nil {
		this = p.parseType(true, false)
	}

	if this == nil {
		return nil
	}

	// Handle both syntaxes: DATE_PART(part, expr) and DATE_PART(part FROM expr)
	// self._match_set((FROM, COMMA)) and self._parse_bitwise() -> False when unmatched
	var expression any = false
	if p.matchAny(TK_FROM, TK_COMMA) {
		expression = p.parseBitwise()
	}
	return p.expression(New(KExtract, "this", mapDatePart(this, p.d), "expression", expression))
}

// _parse_bracket_key_value
func snowflakeParseBracketKeyValue(p *Parser, isMap bool) *Expr {
	if isMap {
		// Keys are strings in Snowflake's objects, see also:
		// - https://docs.snowflake.com/en/sql-reference/data-types-semistructured
		// - https://docs.snowflake.com/en/sql-reference/functions/object_construct
		if e := p.parseSlice(p.parseString()); e != nil {
			return e
		}
		return p.parseAssignment()
	}

	return p.parseSlice(p.parseAlias(p.parseAssignment(), true))
}

// _parse_lateral
func snowflakeParseLateral(p *Parser) *Expr {
	lateral := p.baseParseLateral()
	if lateral == nil {
		return lateral
	}

	if lateral.This().IsA(KExplode) {
		tableAlias := lateral.ArgE("alias")
		columns := make([]*Expr, 0, len(p.s.FLATTEN_COLUMNS))
		for _, col := range p.s.FLATTEN_COLUMNS {
			columns = append(columns, ToIdentifier(col, nil))
		}
		if tableAlias != nil && !tableAlias.ArgB("columns") {
			tableAlias.Set("columns", columns)
		} else if tableAlias == nil {
			cols := make([]any, len(columns))
			for i, c := range columns {
				cols[i] = c
			}
			AliasTableExpr(lateral, "_flattened", cols, nil, false)
		}
	}

	return lateral
}

// _parse_table_parts
func snowflakeParseTableParts(p *Parser, schema bool, isDbReference bool, wildcard bool, fast bool) *Expr {
	// https://docs.snowflake.com/en/user-guide/querying-stage
	var table *Expr
	if p.matchNoAdvance(TK_STRING) {
		table = p.parseString()
	} else if p.matchTextSeqNoAdvance("@") {
		table = snowflakeParseLocationPath(p)
	}

	if table != nil {
		var fileFormat, pattern *Expr

		wrapped := p.match(TK_L_PAREN)
		for p.curr.ok() && wrapped && !p.match(TK_R_PAREN) {
			if p.matchTextSeq("FILE_FORMAT", "=>") {
				fileFormat = p.parseString()
				if fileFormat == nil {
					fileFormat = p.baseParseTableParts(false, isDbReference, false, false)
				}
			} else if p.matchTextSeq("PATTERN", "=>") {
				pattern = p.parseString()
			} else {
				break
			}

			p.match(TK_COMMA)
		}

		table = p.expression(New(KTable, "this", table, "format", fileFormat, "pattern", pattern))
	} else {
		table = p.baseParseTableParts(schema, isDbReference, false, fast)
	}

	return table
}

// _parse_table
func snowflakeParseTable(p *Parser, schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr {
	// consume_pipe is not forwarded to super() (it falls back to its default, False)
	table := p.baseParseTable(schema, joins, aliasTokens, parseBracket, isDbReference, parsePartition, false)
	if table.IsA(KTable) && table.This().IsA(KTableFromRows) {
		tableFromRows := table.This()
		for _, at := range KTableFromRows.ArgTypes() {
			if at.name != "this" {
				tableFromRows.Set(at.name, table.Arg(at.name))
			}
		}

		table = tableFromRows
	}

	return table
}

// _parse_function_call
func snowflakeParseFunctionCall(p *Parser, functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr {
	this := p.baseParseFunctionCall(functions, anonymous, optionalParens, anyToken)

	// Snowflake can invoke a function whose name is dynamically resolved, e.g.
	// `IDENTIFIER('my_func')(1, 2)`. The trailing argument list is the call's arguments.
	//
	// https://docs.snowflake.com/en/sql-reference/identifier-literal
	if this.IsA(KDynamicIdentifier) && p.matchNoAdvance(TK_L_PAREN) {
		this.Set("expressions", p.parseWrappedCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA, false))
	}

	return this
}

// _parse_id_var
func snowflakeParseIdVar(p *Parser, anyToken bool, tokens *TokenSet) *Expr {
	if p.matchTextSeq("IDENTIFIER", "(") {
		identifier := p.baseParseIdVar(anyToken, tokens)
		if identifier == nil {
			identifier = p.parseString()
		}
		p.matchRParen(nil)
		return p.expression(New(KDynamicIdentifier, "this", identifier))
	}

	return p.baseParseIdVar(anyToken, tokens)
}

// _parse_show_snowflake
func snowflakeParseShowSnowflake(p *Parser, this string, terse bool, iceberg bool) *Expr {
	var scope *Expr
	var scopeKind any

	history := p.matchTextSeq("HISTORY")

	var like *Expr
	if p.match(TK_LIKE) {
		like = p.parseString()
	}

	if p.match(TK_IN) {
		if p.matchTextSeq("ACCOUNT") {
			scopeKind = "ACCOUNT"
		} else if p.matchTextSeq("CLASS") {
			scopeKind = "CLASS"
			scope = p.parseTableParts(false, false, false, false)
		} else if p.matchTextSeq("APPLICATION") {
			sk := "APPLICATION"
			if p.matchTextSeq("PACKAGE") {
				sk += " PACKAGE"
			}
			scopeKind = sk
			scope = p.parseTableParts(false, false, false, false)
		} else if p.matchSet(p.s.DB_CREATABLES) {
			scopeKind = upperText(p.prev)
			if p.curr.ok() {
				scope = p.parseTableParts(false, false, false, false)
			}
		} else if p.curr.ok() {
			if p.s.SCHEMA_KINDS.Has(this) {
				scopeKind = "SCHEMA"
			} else {
				scopeKind = "TABLE"
			}
			scope = p.parseTableParts(false, false, false, false)
		}
	}

	// self._match_text_seq(...) and self._parse_...() -> False when unmatched
	var startsWith any = false
	if p.matchTextSeq("STARTS", "WITH") {
		startsWith = p.parseString()
	}
	limit := p.parseLimit(nil, false, false)
	var from *Expr
	if p.match(TK_FROM) {
		from = p.parseString()
	}
	var privileges any = false
	if p.matchTextSeq("WITH", "PRIVILEGES") {
		privileges = p.parseCSV(func() *Expr { return p.parseVar(true, nil, true) }, TK_COMMA)
	}

	return p.expression(New(
		KShow,
		"terse", terse,
		"iceberg", iceberg,
		"this", this,
		"history", history,
		"like", like,
		"scope", scope,
		"scope_kind", scopeKind,
		"starts_with", startsWith,
		"limit", limit,
		"from_", from,
		"privileges", privileges,
	))
}

// _parse_undrop
func snowflakeParseUndrop(p *Parser) *Expr {
	start := p.prev
	kind := p.parseVarFromOptions(p.s.UNDROP_OBJECTS, false)
	if kind == nil {
		return p.parseAsCommand(start)
	}

	name := kind.Name()
	this := p.parseTableParts(false, name == "ACCOUNT" || name == "DATABASE" || name == "SCHEMA", false, false)
	var rename *Expr
	if p.matchTextSeq("RENAME", "TO") {
		rename = p.parseTableParts(false, false, false, false)
	}
	return p.expression(New(KUndrop, "this", this, "kind", name, "rename", rename))
}

// _parse_put
func snowflakeParsePut(p *Parser) *Expr {
	if p.curr.Type != TK_STRING {
		return p.parseAsCommand(p.prev)
	}

	this := p.parseString()
	target := snowflakeParseLocationPath(p)
	properties := p.parseProperties(false)
	return p.expression(New(KPut, "this", this, "target", target, "properties", properties))
}

// _parse_get
func snowflakeParseGet(p *Parser) *Expr {
	start := p.prev

	// If we detect GET( then we need to parse a function, not a statement
	if p.match(TK_L_PAREN) {
		p.retreat(p.index - 2)
		return p.parseExpression()
	}

	target := snowflakeParseLocationPath(p)

	// Parse as command if unquoted file path
	if p.curr.Type == TK_URI_START {
		return p.parseAsCommand(start)
	}

	this := p.parseString()
	properties := p.parseProperties(false)
	return p.expression(New(KGet, "this", this, "target", target, "properties", properties))
}

// _parse_location_property
func snowflakeParseLocationProperty(p *Parser) *Expr {
	p.match(TK_EQ)
	return p.expression(New(KLocationProperty, "this", snowflakeParseLocationPath(p)))
}

// _parse_file_location
func snowflakeParseFileLocation(p *Parser) *Expr {
	// Parse either a subquery or a staged file
	if p.matchNoAdvance(TK_L_PAREN) {
		return p.parseSelect(false, true, false, true, true, nil)
	}
	return p.parseTableParts(false, false, false, false)
}

// _parse_location_path
func snowflakeParseLocationPath(p *Parser) *Expr {
	start := p.curr
	p.advanceAny(true)

	// We avoid consuming a comma token because external tables like @foo and @bar
	// can be joined in a query with a comma separator, as well as closing paren
	// in case of subqueries
	for p.isConnected() && !p.matchAnyNoAdvance(TK_COMMA, TK_L_PAREN, TK_R_PAREN) {
		p.advanceAny(true)
	}

	return VarChecked(p.findSQL(start, p.prev))
}

// _parse_lambda_arg
func snowflakeParseLambdaArg(p *Parser) *Expr {
	this := p.baseParseLambdaArg()

	if this == nil {
		return this
	}

	typ := p.parseTypes(false, false, true, false)

	if typ != nil {
		return p.expression(New(KCast, "this", this, "to", typ))
	}

	return this
}

// _parse_foreign_key
func snowflakeParseForeignKey(p *Parser) *Expr {
	// inlineFK, the REFERENCES columns are implied
	if p.matchNoAdvance(TK_REFERENCES) {
		return p.expression(New(KForeignKey))
	}

	// outoflineFK, explicitly names the columns
	return p.baseParseForeignKey()
}

// _parse_file_format_property
func snowflakeParseFileFormatProperty(p *Parser) *Expr {
	p.match(TK_EQ)
	var expressions []*Expr
	if p.matchNoAdvance(TK_L_PAREN) {
		expressions = p.parseWrappedOptions()
	} else {
		expressions = []*Expr{p.parseFormatName()}
	}

	return p.expression(New(KFileFormatProperty, "expressions", expressions))
}

// _parse_credentials_property
func snowflakeParseCredentialsProperty(p *Parser) *Expr {
	return p.expression(New(KCredentialsProperty, "expressions", p.parseWrappedOptions()))
}

// _parse_semantic_view
func snowflakeParseSemanticView(p *Parser) *Expr {
	kwargs := newBuilderKwargs([]any{"this", p.parseTableParts(false, false, false, false)})

	for p.curr.ok() && !p.matchNoAdvance(TK_R_PAREN) {
		if p.matchTexts("DIMENSIONS", "METRICS", "FACTS") {
			keyword := pyLower(p.prev.Text)
			kwargs.set(keyword, p.parseCSV(func() *Expr { return p.parseAlias(p.parseDisjunction(), true) }, TK_COMMA))
		} else if p.matchTextSeq("WHERE") {
			kwargs.set("where", p.parseExpression())
		} else {
			p.raiseError("Expecting ) or encountered unexpected keyword", nil)
			break
		}
	}

	return p.expression(New(KSemanticView, snowflakeKwargsKV(kwargs)...))
}

// _parse_set
func snowflakeParseSet(p *Parser, unset bool, tag bool) *Expr {
	set := p.baseParseSet(unset, tag)

	if set.IsA(KSet) {
		for _, expr := range set.Expressions() {
			if expr.IsA(KSetItem) {
				expr.Set("kind", "VARIABLE")
			}
		}
	}
	return set
}

// _parse_position
func snowflakeParsePosition(p *Parser, haystackFirst bool) *Expr {
	result := p.baseParsePosition(haystackFirst)
	result.Set("clamp_position", true)
	return result
}

// _parse_substring
func snowflakeParseSubstring(p *Parser) *Expr {
	result := p.baseParseSubstring()
	result.Set("zero_start", true)
	return result
}

// build_cast
func snowflakeBuildCast(p *Parser, strict bool, kv ...any) *Expr {
	var to, this *Expr
	for i := 0; i+1 < len(kv); i += 2 {
		switch kv[i] {
		case "to":
			to, _ = kv[i+1].(*Expr)
		case "this":
			this, _ = kv[i+1].(*Expr)
		}
	}
	if !strict && to != nil && to.Arg("this") == any(DT_BOOLEAN) {
		return p.expression(New(KToBoolean, "this", this, "safe", true))
	}
	cast := p.baseBuildCast(strict, kv...)
	if cast.IsA(KTryCast) && to != nil {
		if dt, ok := to.Arg("this").(DType); ok && DataType_TEXT_TYPES.Has(dt) && len(to.Expressions()) > 0 {
			cast.Set("null_on_text_overflow", true)
		} else if to.Arg("this") == any(DT_DATE) && cast.This().IsString() {
			cast.Set("probe_date_format", true)
		}
	}
	return cast
}

// _parse_window
func snowflakeParseWindow(p *Parser, this *Expr, alias bool) *Expr {
	if this.IsA(KNthValue) {
		if p.matchTextSeq("FROM", "FIRST") {
			this.Set("from_first", true)
		} else if p.matchTextSeq("FROM", "LAST") {
			this.Set("from_first", false)
		}
	}

	result := p.baseParseWindow(this, alias)

	// Set default window frame for ranking functions if not present
	if result.IsA(KWindow) && this.IsA(snowflakeRankingWindowFunctionsWithFrame...) && !result.ArgB("spec") {
		frame := New(
			KWindowSpec,
			"kind", "ROWS",
			"start", "UNBOUNDED",
			"start_side", "PRECEDING",
			"end", "UNBOUNDED",
			"end_side", "FOLLOWING",
		)
		result.Set("spec", frame)
	}
	return result
}
