package sqlengine

// Port of the DuckDBGenerator class of sqlglot/generators/duckdb.py (continued).

import (
	"fmt"
	"strconv"
	"strings"
)

// duckdbRegexpcountSQL mirrors regexpcount_sql.
func duckdbRegexpcountSQL(g *Generator, e *Expr) string {
	this := e.This()
	pattern := e.Expression()
	position := e.ArgE("position")
	parameters := e.ArgE("parameters")

	// Validate flags - only "ims" flags are supported for embedded patterns
	validatedFlags := duckdbValidateRegexpFlags(g, parameters, "ims")

	if position != nil {
		this = New(KSubstring, "this", this, "start", position)
	}

	// Embed flags in pattern (REGEXP_EXTRACT_ALL doesn't support flags argument)
	if validatedFlags != "" {
		pattern = New(KConcat, "expressions", []*Expr{LiteralString("(?" + validatedFlags + ")"), pattern})
	}

	// Handle empty pattern: Snowflake returns 0, DuckDB would match between every character
	result := duckdbWhen(duckdbCase(),
		New(KEQ, "this", pattern, "expression", LiteralString("")),
		LiteralInt(0), true)
	result = duckdbElse(result,
		New(KLength, "this", New(KAnonymous, "this", "REGEXP_EXTRACT_ALL", "expressions", []*Expr{this, pattern})),
		true)

	return g.sql(result)
}

// duckdbRegexpreplaceSQL mirrors regexpreplace_sql.
func duckdbRegexpreplaceSQL(g *Generator, e *Expr) string {
	subject := e.This()
	pattern := e.Expression()
	replacement := e.ArgE("replacement")
	if replacement == nil {
		replacement = LiteralString("")
	}
	position := e.ArgE("position")
	occurrence := e.ArgE("occurrence")
	modifiers := e.ArgE("modifiers")

	validatedFlags := duckdbValidateRegexpFlags(g, modifiers, "cimsg")

	// Handle occurrence (only literals supported)
	if occurrence != nil && !occurrence.IsInt() {
		g.unsupported("REGEXP_REPLACE with non-literal occurrence")
	} else {
		occ := 0
		if occurrence != nil && occurrence.IsInt() {
			occ, _ = duckdbToPyInt(occurrence)
		}
		if occ > 1 {
			g.unsupported(fmt.Sprintf("REGEXP_REPLACE occurrence=%d not supported", occ))
		} else if occ == 0 && !strings.Contains(validatedFlags, "g") && !e.ArgB("single_replace") {
			// flag duckdb to do either all or none, single_replace check is for duckdb round trip
			validatedFlags += "g"
		}
	}

	// Handle position (only literals supported)
	var prefix *Expr
	if position != nil && !position.IsInt() {
		g.unsupported("REGEXP_REPLACE with non-literal position")
	} else if position != nil && position.IsInt() {
		if pos, _ := duckdbToPyInt(position); pos > 1 {
			prefix = New(KSubstring, "this", subject, "start", LiteralInt(1), "length", LiteralInt(pos-1))
			subject = New(KSubstring, "this", subject, "start", LiteralInt(pos))
		}
	}

	var flags *Expr
	if validatedFlags != "" {
		flags = LiteralString(validatedFlags)
	}
	result := New(KAnonymous, "this", "REGEXP_REPLACE", "expressions", duckdbNonNil(subject, pattern, replacement, flags))

	if prefix != nil {
		result = New(KConcat, "expressions", []*Expr{prefix, result})
	}

	return g.sql(result)
}

// duckdbRegexplikeSQL mirrors regexplike_sql.
func duckdbRegexplikeSQL(g *Generator, e *Expr) string {
	this := e.This()
	pattern := e.Expression()
	flag := e.ArgE("flag")

	if e.ArgB("full_match") {
		validatedFlags := duckdbValidateRegexpFlags(g, flag, "cims")
		flag = nil
		if validatedFlags != "" {
			flag = LiteralString(validatedFlags)
		}
		return g.fn("REGEXP_FULL_MATCH", this, pattern, flag)
	}

	return g.fn("REGEXP_MATCHES", this, pattern, flag)
}

// duckdbLevenshteinSQL mirrors levenshtein_sql.
func duckdbLevenshteinSQL(g *Generator, e *Expr) string {
	duckdbUnsupportedArgs(g, e, "ins_cost", "del_cost", "sub_cost")
	this := e.This()
	expr := e.Expression()
	maxDist := e.ArgE("max_dist")

	if maxDist == nil {
		return g.fn("LEVENSHTEIN", this, expr)
	}

	// Emulate Snowflake semantics: if distance > max_dist, return max_dist
	levenshtein := New(KLevenshtein, "this", this, "expression", expr)
	return g.sql(New(KLeast, "this", levenshtein, "expressions", []*Expr{maxDist}))
}

// duckdbPadSQL mirrors pad_sql.
//
// Handle RPAD/LPAD for VARCHAR and BINARY types.
func duckdbPadSQL(g *Generator, e *Expr) string {
	stringArg := e.This()
	fillArg := e.ArgE("fill_pattern")
	if fillArg == nil {
		fillArg = LiteralString(" ")
	}

	if duckdbIsBinary(stringArg) || duckdbIsBinary(fillArg) {
		lengthArg := e.Expression()
		isLeft := e.ArgB("is_left")

		inputLen := New(KByteLength, "this", stringArg)
		charsNeeded := duckdbSub(lengthArg, inputLen)
		padCount := New(KGreatest, "this", LiteralInt(0), "expressions", []*Expr{charsNeeded}, "ignore_nulls", true)
		repeatExpr := New(KRepeat, "this", fillArg, "times", padCount)

		left, right := stringArg, repeatExpr
		if isLeft {
			left, right = right, left
		}

		result := New(KDPipe, "this", left, "expression", right)
		return g.sql(result)
	}

	// For VARCHAR: Delegate to parent class (handles PAD_FILL_PATTERN_IS_REQUIRED)
	return g.basePadSQL(e)
}

// duckdbMinhashSQL mirrors minhash_sql.
func duckdbMinhashSQL(g *Generator, e *Expr) string {
	k := e.This()
	exprs := e.Expressions()

	if len(exprs) != 1 || exprs[0].IsA(KStar) {
		g.unsupported("MINHASH with multiple expressions or * requires manual query restructuring")
		args := []any{k}
		for _, x := range exprs {
			args = append(args, x)
		}
		return g.fn("MINHASH", args...)
	}

	expr := exprs[0]
	result := duckdbReplacePlaceholders(duckdbMinhashTemplate.get().Copy(), map[string]*Expr{"expr": expr, "k": k})
	return "(" + g.sql(result) + ")"
}

// duckdbMinhashcombineSQL mirrors minhashcombine_sql.
func duckdbMinhashcombineSQL(g *Generator, e *Expr) string {
	expr := e.This()
	result := duckdbReplacePlaceholders(duckdbMinhashCombineTemplate.get().Copy(), map[string]*Expr{"expr": expr})
	return "(" + g.sql(result) + ")"
}

// duckdbApproximatesimilaritySQL mirrors approximatesimilarity_sql.
func duckdbApproximatesimilaritySQL(g *Generator, e *Expr) string {
	expr := e.This()
	result := duckdbReplacePlaceholders(duckdbApproximateSimilarityTemplate.get().Copy(), map[string]*Expr{"expr": expr})
	return "(" + g.sql(result) + ")"
}

// duckdbArrayuniqueaggSQL mirrors arrayuniqueagg_sql.
func duckdbArrayuniqueaggSQL(g *Generator, e *Expr) string {
	return g.sql(New(
		KFilter,
		"this", duckdbFunc("LIST", New(KDistinct, "expressions", []*Expr{e.This()})),
		"expression", New(KWhere, "this", duckdbIs(e.This().Copy(), Null()).ExprNot(true)),
	))
}

// duckdbArraydistinctSQL mirrors arraydistinct_sql.
func duckdbArraydistinctSQL(g *Generator, e *Expr) string {
	arr := e.This()
	fn := g.fn("LIST_DISTINCT", arr)

	if e.ArgB("check_null") {
		addNullToArray := duckdbFunc(
			"LIST_APPEND",
			duckdbFunc("LIST_DISTINCT", New(KArrayCompact, "this", arr)),
			Null(),
		)
		return g.sql(New(
			KIf,
			"this", New(KNEQ, "this", New(KArraySize, "this", arr), "expression", duckdbFunc("LIST_COUNT", arr)),
			"true", addNullToArray,
			"false", fn,
		))
	}

	return fn
}

// duckdbArrayintersectSQL mirrors arrayintersect_sql.
func duckdbArrayintersectSQL(g *Generator, e *Expr) string {
	if e.ArgB("is_multiset") && len(e.Expressions()) == 2 {
		return duckdbArrayBagSQL(g, duckdbArrayIntersectionCondition.get(), e.Expressions()[0], e.Expressions()[1])
	}
	return g.functionFallbackSQL(e)
}

// duckdbArrayexceptSQL mirrors arrayexcept_sql.
func duckdbArrayexceptSQL(g *Generator, e *Expr) string {
	arr1, arr2 := e.This(), e.Expression()
	if e.ArgB("is_multiset") {
		return duckdbArrayBagSQL(g, duckdbArrayExceptCondition.get(), arr1, arr2)
	}
	return g.sql(duckdbReplacePlaceholders(duckdbArrayExceptSetTemplate.get(), map[string]*Expr{"arr1": arr1, "arr2": arr2}))
}

// duckdbArraysliceSQL mirrors arrayslice_sql.
//
// Transpiles Snowflake's ARRAY_SLICE (0-indexed, exclusive end) to DuckDB's
// ARRAY_SLICE (1-indexed, inclusive end).
func duckdbArraysliceSQL(g *Generator, e *Expr) string {
	start, end := e.ArgE("start"), e.ArgE("end")

	if e.ArgB("zero_based") {
		if start != nil {
			c := duckdbWhen(duckdbCase(),
				New(KGTE, "this", start.Copy(), "expression", LiteralInt(0)),
				New(KAdd, "this", start.Copy(), "expression", LiteralInt(1)),
				true)
			start = duckdbElse(c, start, true)
		}
		if end != nil {
			c := duckdbWhen(duckdbCase(),
				New(KLT, "this", end.Copy(), "expression", LiteralInt(0)),
				New(KSub, "this", end.Copy(), "expression", LiteralInt(1)),
				true)
			end = duckdbElse(c, end, true)
		}
	}

	return g.fn("ARRAY_SLICE", e.Arg("this"), start, end, e.ArgE("step"))
}

// duckdbArrayszipSQL mirrors arrayszip_sql.
func duckdbArrayszipSQL(g *Generator, e *Expr) string {
	args := e.Expressions()

	if len(args) == 0 {
		// Return [{}] - using MAP([], []) since DuckDB can't represent empty structs
		return g.sql(ArrayExpr([]*Expr{New(KMap, "keys", ArrayExpr(nil, true), "values", ArrayExpr(nil, true))}, true))
	}

	// Build placeholder values for template
	lengths := make([]*Expr, len(args))
	for i, arg := range args {
		lengths[i] = New(KLength, "this", arg)
	}
	maxLen := lengths[0]
	if len(lengths) != 1 {
		maxLen = New(KGreatest, "this", lengths[0], "expressions", lengths[1:])
	}

	// Empty struct with same schema: {'$1': NULL, '$2': NULL, ...}
	emptyArgs := make([]any, len(args))
	for i := range args {
		emptyArgs[i] = New(KPropertyEQ, "this", LiteralString(fmt.Sprintf("$%d", i+1)), "expression", Null())
	}
	emptyStruct := duckdbFunc("STRUCT", emptyArgs...)

	// Struct for transform: {'$1': COALESCE(arr1, [])[__i + 1], ...}
	// COALESCE wrapping handles NULL arrays - prevents invalid NULL[i] syntax
	index := duckdbAdd(ColumnExpr("__i", nil, nil, nil, nil, nil, true), 1)
	transformArgs := make([]any, len(args))
	for i, arg := range args {
		transformArgs[i] = New(
			KPropertyEQ,
			"this", LiteralString(fmt.Sprintf("$%d", i+1)),
			"expression", duckdbGetItem(duckdbFunc("COALESCE", arg, ArrayExpr(nil, true)), index),
		)
	}
	transformStruct := duckdbFunc("STRUCT", transformArgs...)

	nullChecks := make([]*Expr, len(args))
	emptyChecks := make([]*Expr, len(args))
	for i, arg := range args {
		nullChecks[i] = duckdbIs(arg, Null())
		emptyChecks[i] = New(KEQ, "this", New(KLength, "this", arg), "expression", LiteralInt(0))
	}

	result := duckdbReplacePlaceholders(duckdbArraysZipTemplate.get().Copy(), map[string]*Expr{
		"null_check":       OrExpr(nullChecks...),
		"all_empty_check":  AndExpr(emptyChecks...),
		"empty_struct":     emptyStruct,
		"max_len":          maxLen,
		"transform_struct": transformStruct,
	})
	return g.sql(result)
}

// duckdbLeftRightSQL mirrors _left_right_sql.
func duckdbLeftRightSQL(g *Generator, e *Expr, funcName string) string {
	arg := e.This()
	length := e.Expression()
	isBinary := duckdbIsBinary(arg)

	var result *Expr
	if isBinary {
		// LEFT/RIGHT(blob, n) becomes UNHEX(LEFT/RIGHT(HEX(blob), n * 2))
		// Each byte becomes 2 hex chars, so multiply length by 2
		hexArg := New(KHex, "this", arg)
		hexLength := New(KMul, "this", length, "expression", LiteralInt(2))
		result = New(KUnhex, "this", New(KAnonymous, "this", funcName, "expressions", []*Expr{hexArg, hexLength}))
	} else {
		result = New(KAnonymous, "this", funcName, "expressions", []*Expr{arg, length})
	}

	if e.ArgB("negative_length_returns_empty") {
		empty := LiteralString("")
		if isBinary {
			empty = New(KUnhex, "this", empty)
		}
		c := duckdbWhen(duckdbCase(), duckdbLT(length, LiteralInt(0)), empty, true)
		result = duckdbElse(c, result, true)
	}

	return g.sql(result)
}

// duckdbStuffSQL mirrors stuff_sql.
func duckdbStuffSQL(g *Generator, e *Expr) string {
	base := e.This()
	start := e.ArgE("start")
	length := e.ArgE("length")
	insertion := e.Expression()
	isBinary := duckdbIsBinary(base)

	var left, right *Expr
	if isBinary {
		// DuckDB's SUBSTRING doesn't accept BLOB; operate on the HEX string instead
		// (each byte = 2 hex chars), then UNHEX back to BLOB
		base = New(KHex, "this", base)
		insertion = New(KHex, "this", insertion)
		left = New(
			KSubstring,
			"this", base.Copy(),
			"start", LiteralInt(1),
			"length", duckdbMul(duckdbSub(start.Copy(), LiteralInt(1)), LiteralInt(2)),
		)
		right = New(
			KSubstring,
			"this", base.Copy(),
			"start", duckdbAdd(duckdbMul(duckdbSub(duckdbAdd(start, length), LiteralInt(1)), LiteralInt(2)), LiteralInt(1)),
		)
	} else {
		left = New(
			KSubstring,
			"this", base.Copy(),
			"start", LiteralInt(1),
			"length", duckdbSub(start.Copy(), LiteralInt(1)),
		)
		right = New(KSubstring, "this", base.Copy(), "start", duckdbAdd(start, length))
	}
	result := New(
		KDPipe,
		"this", New(KDPipe, "this", left, "expression", insertion),
		"expression", right,
	)

	if isBinary {
		result = New(KUnhex, "this", result)
	}

	return g.sql(result)
}

// duckdbRandSQL mirrors rand_sql.
func duckdbRandSQL(g *Generator, e *Expr) string {
	if e.This() != nil {
		g.unsupported("RANDOM with seed is not supported in DuckDB")
	}

	lower := e.ArgE("lower")
	upper := e.ArgE("upper")

	if lower != nil && upper != nil {
		// scale DuckDB's [0,1) to the specified range
		rangeSize := ParenExpr(duckdbSub(upper, lower), true)
		scaled := New(KAdd, "this", lower, "expression", duckdbMul(duckdbFunc("random"), rangeSize))

		// For now we assume that if bounds are set, return type is BIGINT. Snowflake/Teradata
		result := CastExpr(scaled, DT_BIGINT, true, nil)
		return g.sql(result)
	}

	// Default DuckDB behavior - just return RANDOM() as float
	return "RANDOM()"
}

// duckdbBytelengthSQL mirrors bytelength_sql.
func duckdbBytelengthSQL(g *Generator, e *Expr) string {
	arg := e.This()

	// Check if it's a text type (handles both literals and annotated expressions)
	if duckdbIsType(arg, DataType_TEXT_TYPES.Items()...) {
		return g.fn("OCTET_LENGTH", New(KEncode, "this", arg))
	}

	// Default: pass through as-is (conservative for DuckDB, handles binary and unannotated)
	return g.fn("OCTET_LENGTH", arg)
}

// duckdbBase64encodeSQL mirrors base64encode_sql.
func duckdbBase64encodeSQL(g *Generator, e *Expr) string {
	// DuckDB TO_BASE64 requires BLOB input
	// Snowflake BASE64_ENCODE accepts both VARCHAR and BINARY - for VARCHAR it implicitly
	// encodes UTF-8 bytes. We add ENCODE unless the input is a binary type.
	result := e.This()

	// Check if input is a string type - ENCODE only accepts VARCHAR
	if duckdbIsType(result, DataType_TEXT_TYPES.Items()...) {
		result = New(KEncode, "this", result)
	}

	result = New(KToBase64, "this", result)

	maxLineLength := e.ArgE("max_line_length")
	alphabet := e.ArgE("alphabet")

	// Handle custom alphabet by replacing standard chars with custom ones
	result = duckdbApplyBase64AlphabetReplacements(result, alphabet, false)

	// Handle max_line_length by inserting newlines every N characters
	lineLength := "0"
	positive := false
	if maxLineLength.IsA(KLiteral) && maxLineLength.IsNumber() {
		if v, isInt := maxLineLength.toPyNumber(); v != nil {
			positive = v.Sign() > 0
			if isInt {
				i, _ := duckdbToPyInt(maxLineLength)
				lineLength = strconv.Itoa(i)
			} else {
				lineLength = maxLineLength.ThisS()
			}
		}
	}
	if positive {
		newline := New(KChr, "expressions", []*Expr{LiteralInt(10)})
		result = New(
			KTrim,
			"this", New(
				KRegexpReplace,
				"this", result,
				"expression", LiteralString("(.{"+lineLength+"})"),
				"replacement", New(KConcat, "expressions", []*Expr{LiteralString("\\1"), newline.Copy()}),
			),
			"expression", newline,
			"position", "TRAILING",
		)
	}

	return g.sql(result)
}

// duckdbHexSQL mirrors hex_sql.
func duckdbHexSQL(g *Generator, e *Expr) string {
	c := e.ArgE("case")

	if c == nil {
		return g.fn("HEX", e.Arg("this"))
	}

	hexExpr := New(KHex, "this", e.This())
	r := duckdbWhen(duckdbCase(), duckdbIs(c, Null()), Null(), true)
	r = duckdbWhen(r, duckdbEQ(c.Copy(), 0), New(KLower, "this", hexExpr.Copy()), true)
	r = duckdbElse(r, hexExpr, true)
	return g.sql(r)
}

// duckdbReplaceSQL mirrors replace_sql.
func duckdbReplaceSQL(g *Generator, e *Expr) string {
	resultSQL := g.fn(
		"REPLACE",
		duckdbCastToVarchar(e.This()),
		duckdbCastToVarchar(e.Expression()),
		duckdbCastToVarchar(e.ArgE("replacement")),
	)
	return duckdbGenWithCastToBlob(g, e, resultSQL)
}

// duckdbBitwiseOp mirrors DuckDBGenerator._bitwise_op.
func duckdbBitwiseOp(g *Generator, e *Expr, op string) string {
	duckdbPrepareBinaryBitwiseArgs(e)
	resultSQL := g.binary(e, op)
	return duckdbGenWithCastToBlob(g, e, resultSQL)
}

// duckdbBitwisexorSQL mirrors bitwisexor_sql.
func duckdbBitwisexorSQL(g *Generator, e *Expr) string {
	duckdbPrepareBinaryBitwiseArgs(e)
	resultSQL := g.fn("XOR", e.Arg("this"), e.Arg("expression"))
	return duckdbGenWithCastToBlob(g, e, resultSQL)
}

// duckdbObjectinsertSQL mirrors objectinsert_sql.
func duckdbObjectinsertSQL(g *Generator, e *Expr) string {
	this := e.This()
	keySQL := ""
	if key := e.ArgE("key"); key != nil {
		keySQL = key.Name()
	}
	valueSQL := g.sqlKey(e, "value")

	kvSQL := keySQL + " := " + valueSQL

	// If the input struct is empty e.g. transpiling OBJECT_INSERT(OBJECT_CONSTRUCT(), key, value) from Snowflake
	// then we can generate STRUCT_PACK which will build it since STRUCT_INSERT({}, key := value) is not valid DuckDB
	if this.IsA(KStruct) && len(this.Expressions()) == 0 {
		return g.fn("STRUCT_PACK", kvSQL)
	}

	return g.fn("STRUCT_INSERT", this, kvSQL)
}

// duckdbMapdeleteSQL mirrors mapdelete_sql.
func duckdbMapdeleteSQL(g *Generator, e *Expr) string {
	mapArg := e.This()
	keysToDelete := e.Expressions()

	xDotKey := New(KDot, "this", ToIdentifier("x", nil), "expression", ToIdentifier("key", nil))

	lambdaExpr := New(
		KLambda,
		"this", New(KIn, "this", xDotKey, "expressions", keysToDelete).ExprNot(true),
		"expressions", []*Expr{ToIdentifier("x", nil)},
	)
	result := duckdbFunc(
		"MAP_FROM_ENTRIES",
		New(KArrayFilter, "this", duckdbFunc("MAP_ENTRIES", mapArg), "expression", lambdaExpr),
	)
	return g.sql(result)
}

// duckdbMappickSQL mirrors mappick_sql.
func duckdbMappickSQL(g *Generator, e *Expr) string {
	mapArg := e.This()
	keysToPick := e.Expressions()

	xDotKey := New(KDot, "this", ToIdentifier("x", nil), "expression", ToIdentifier("key", nil))

	var lambdaExpr *Expr
	if len(keysToPick) == 1 && duckdbIsType(keysToPick[0], DT_ARRAY) {
		lambdaExpr = New(
			KLambda,
			"this", duckdbFunc("ARRAY_CONTAINS", keysToPick[0], xDotKey),
			"expressions", []*Expr{ToIdentifier("x", nil)},
		)
	} else {
		lambdaExpr = New(
			KLambda,
			"this", New(KIn, "this", xDotKey, "expressions", keysToPick),
			"expressions", []*Expr{ToIdentifier("x", nil)},
		)
	}

	result := duckdbFunc(
		"MAP_FROM_ENTRIES",
		duckdbFunc("LIST_FILTER", duckdbFunc("MAP_ENTRIES", mapArg), lambdaExpr),
	)
	return g.sql(result)
}

// duckdbMapinsertSQL mirrors mapinsert_sql.
func duckdbMapinsertSQL(g *Generator, e *Expr) string {
	duckdbUnsupportedArgs(g, e, "update_flag")
	mapArg := e.This()
	key := e.ArgE("key")
	value := e.ArgE("value")

	mapType := mapArg.Type()

	if value != nil {
		if mapType != nil && len(mapType.Expressions()) > 1 {
			// Extract the value type from MAP(key_type, value_type)
			valueType := mapType.Expressions()[1]
			// Cast value to match the map's value type to avoid type conflicts
			value = CastExpr(value, valueType, true, nil)
		}
		// else: polymorphic MAP case - no type parameters available, use value as-is
	}

	// Create a single-entry map for the new key-value pair
	newEntryStruct := New(KStruct, "expressions", []*Expr{New(KPropertyEQ, "this", key, "expression", value)})
	newEntry := New(KToMap, "this", newEntryStruct)

	// Use MAP_CONCAT to merge the original map with the new entry
	// This automatically handles both insert and update cases
	result := duckdbFunc("MAP_CONCAT", mapArg, newEntry)

	return g.sql(result)
}

// duckdbSpaceSQL mirrors space_sql.
func duckdbSpaceSQL(g *Generator, e *Expr) string {
	// DuckDB's REPEAT requires BIGINT for the count parameter
	return g.sql(New(
		KRepeat,
		"this", LiteralString(" "),
		"times", CastExpr(e.This(), DT_BIGINT, true, nil),
	))
}

// duckdbTablefromrowsSQL mirrors tablefromrows_sql.
func duckdbTablefromrowsSQL(g *Generator, e *Expr) string {
	// For GENERATOR, unwrap TABLE() - just emit the Generator (becomes RANGE)
	if e.This().IsA(KGenerator) {
		// Preserve alias, joins, and other table-level args
		table := New(
			KTable,
			"this", e.This(),
			"alias", e.ArgE("alias"),
			"joins", e.Arg("joins"),
		)
		return g.sql(table)
	}

	return g.baseTablefromrowsSQL(e)
}

// duckdbUnnestSQL mirrors unnest_sql.
func duckdbUnnestSQL(g *Generator, e *Expr) string {
	if e.ArgB("explode_array") {
		// In BigQuery, UNNESTing a nested array leads to explosion of the top-level array & struct
		// This is transpiled to DDB by transforming "FROM UNNEST(...)" to "FROM (SELECT UNNEST(..., max_depth => 2))"
		e.Append("expressions", New(KKwarg, "this", VarExpr("max_depth"), "expression", LiteralInt(2)))

		// If BQ's UNNEST is aliased, we transform it from a column alias to a table alias in DDB
		var alias *Expr = e.ArgE("alias")
		if alias.IsA(KTableAlias) {
			e.Set("alias", nil)
			if cols := alias.ArgL("columns"); len(cols) > 0 {
				alias = New(KTableAlias, "this", seqGet(cols, 0))
			}
		}

		unnestSQL := g.baseUnnestSQL(e)
		sel := New(KSelect, "expressions", []*Expr{duckdbRaw(unnestSQL)}).QuerySubquery(alias, true)
		return g.sql(sel)
	}

	return g.baseUnnestSQL(e)
}

// duckdbIgnorenullsSQL mirrors ignorenulls_sql.
func duckdbIgnorenullsSQL(g *Generator, e *Expr) string {
	this := e.This()

	if this.IsA(g.s.IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS...) {
		// DuckDB should render IGNORE NULLS only for the general-purpose
		// window functions that accept it e.g. FIRST_VALUE(... IGNORE NULLS) OVER (...)
		return g.baseIgnorenullsSQL(e)
	}

	if this.IsA(KFirst) {
		this = New(KAnyValue, "this", this.This())
	}

	if !this.IsA(KAnyValue, KApproxQuantiles) {
		g.unsupported("IGNORE NULLS is not supported for non-window functions.")
	}

	return g.sql(this)
}

// duckdbSplitSQL mirrors split_sql.
func duckdbSplitSQL(g *Generator, e *Expr) string {
	baseFunc := duckdbFunc("STR_SPLIT", e.This(), e.Expression())

	caseExpr := duckdbElse(duckdbCase(), baseFunc, true)
	needsCase := false

	if e.ArgB("null_returns_null") {
		caseExpr = duckdbWhen(caseExpr, duckdbIs(e.Expression(), Null()), Null(), true)
		needsCase = true
	}

	if e.ArgB("empty_delimiter_returns_whole") {
		// When delimiter is empty string, return input string as single array element
		arrayWithInput := ArrayExpr([]*Expr{e.This()}, true)
		caseExpr = duckdbWhen(caseExpr, duckdbEQ(e.Expression(), LiteralString("")), arrayWithInput, true)
		needsCase = true
	}

	if needsCase {
		return g.sql(caseExpr)
	}
	return g.sql(baseFunc)
}

// duckdbSplitpartSQL mirrors splitpart_sql.
func duckdbSplitpartSQL(g *Generator, e *Expr) string {
	stringArg := e.This()
	delimiterArg := e.ArgE("delimiter")
	partIndexArg := e.ArgE("part_index")

	if delimiterArg != nil && partIndexArg != nil {
		// Handle Snowflake's "index 0 and 1 both return first element" behavior
		if e.ArgB("part_index_zero_as_one") {
			// Convert 0 to 1 for compatibility
			c := duckdbWhen(duckdbCase(), duckdbEQ(partIndexArg, LiteralNumber("0")), LiteralNumber("1"), true)
			partIndexArg = New(KParen, "this", duckdbElse(c, partIndexArg, true))
		}

		// Use Anonymous to avoid recursion
		baseFuncExpr := New(KAnonymous, "this", "SPLIT_PART", "expressions", []*Expr{stringArg, delimiterArg, partIndexArg})
		needsCaseTransform := false
		caseExpr := duckdbElse(duckdbCase(), baseFuncExpr, true)

		if e.ArgB("empty_delimiter_returns_whole") {
			// When delimiter is empty string:
			// - Return whole string if part_index is 1 or -1
			// - Return empty string otherwise
			c := duckdbWhen(duckdbCase(),
				OrExpr(
					duckdbEQ(partIndexArg, LiteralNumber("1")),
					duckdbEQ(partIndexArg, LiteralNumber("-1")),
				),
				stringArg, true)
			emptyCase := New(KParen, "this", duckdbElse(c, LiteralString(""), true))

			caseExpr = duckdbWhen(caseExpr, duckdbEQ(delimiterArg, LiteralString("")), emptyCase, true)
			needsCaseTransform = true
		}

		if needsCaseTransform {
			return g.sql(caseExpr)
		}
		return g.sql(baseFuncExpr)
	}

	return g.functionFallbackSQL(e)
}

// duckdbRespectnullsSQL mirrors respectnulls_sql.
func duckdbRespectnullsSQL(g *Generator, e *Expr) string {
	if e.This().IsA(g.s.IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS...) {
		// DuckDB should render RESPECT NULLS only for the general-purpose
		// window functions that accept it e.g. FIRST_VALUE(... RESPECT NULLS) OVER (...)
		return g.baseRespectnullsSQL(e)
	}

	g.unsupported("RESPECT NULLS is not supported for non-window functions.")
	return g.sqlKey(e, "this")
}

// duckdbArraytostringSQL mirrors arraytostring_sql.
func duckdbArraytostringSQL(g *Generator, e *Expr) string {
	null := e.ArgE("null")

	if e.ArgB("null_is_empty") {
		x := ToIdentifier("x", nil)
		listTransform := New(
			KTransform,
			"this", e.This().Copy(),
			"expression", New(
				KLambda,
				"this", New(
					KCoalesce,
					"this", CastExpr(x, "TEXT", true, nil),
					"expressions", []*Expr{LiteralString("")},
				),
				"expressions", []*Expr{x},
			),
		)
		arrayToString := New(KArrayToString, "this", listTransform, "expression", e.Expression())
		if e.ArgB("null_delim_is_null") {
			c := duckdbWhen(duckdbCase(), duckdbIs(e.Expression().Copy(), Null()), Null(), true)
			c = duckdbElse(c, arrayToString, true)
			return g.sql(c)
		}
		return g.sql(arrayToString)
	}

	if null != nil {
		x := ToIdentifier("x", nil)
		return g.sql(New(
			KArrayToString,
			"this", New(
				KTransform,
				"this", e.This(),
				"expression", New(
					KLambda,
					"this", New(KCoalesce, "this", x, "expressions", []*Expr{null}),
					"expressions", []*Expr{x},
				),
			),
			"expression", e.Expression(),
		))
	}

	return g.fn("ARRAY_TO_STRING", e.Arg("this"), e.Arg("expression"))
}

// duckdbConcatwsSQL mirrors concatws_sql.
func duckdbConcatwsSQL(g *Generator, e *Expr) string {
	// DuckDB-specific: handle binary types using DPipe (||) operator
	exprs := e.Expressions()
	separator := seqGet(exprs, 0)
	var args []*Expr
	if len(exprs) > 1 {
		args = exprs[1:]
	}

	anyBinary := duckdbIsBinary(separator)
	for _, arg := range args {
		if duckdbIsBinary(arg) {
			anyBinary = true
		}
	}
	if anyBinary {
		result := args[0]
		for _, arg := range args[1:] {
			result = New(
				KDPipe,
				"this", New(KDPipe, "this", result, "expression", separator),
				"expression", arg,
			)
		}
		return g.sql(result)
	}

	return g.baseConcatwsSQL(e)
}

// duckdbRegexpExtractSQL mirrors _regexp_extract_sql (regexpextract_sql / regexpextractall_sql).
func duckdbRegexpExtractSQL(g *Generator, e *Expr) string {
	this := e.This()
	group := e.ArgE("group")
	params := e.ArgE("parameters")
	position := e.ArgE("position")
	occurrence := e.ArgE("occurrence")
	nullIfPosOverflow := e.ArgB("null_if_pos_overflow")

	// Handle Snowflake's 'e' flag: it enables capture group extraction
	// In DuckDB, this is controlled by the group parameter directly
	if params != nil && params.IsString() && strings.Contains(params.Name(), "e") {
		params = LiteralString(strings.ReplaceAll(params.Name(), "e", ""))
	}

	validatedFlags := duckdbValidateRegexpFlags(g, params, "cims")

	// Strip default group when no following params (DuckDB default is same as group=0)
	if validatedFlags == "" && group != nil && group.Name() == strconv.Itoa(g.d.S.REGEXP_EXTRACT_DEFAULT_GROUP) {
		group = nil
	}

	var flagsExpr *Expr
	if validatedFlags != "" {
		flagsExpr = LiteralString(validatedFlags)
	}

	// use substring to handle position argument
	if position != nil {
		if pos, ok := duckdbToPyInt(position); !position.IsInt() || (ok && pos > 1) {
			this = New(KSubstring, "this", this, "start", position)

			if nullIfPosOverflow {
				this = New(KNullif, "this", this, "expression", LiteralString(""))
			}
		}
	}

	isExtractAll := e.IsA(KRegexpExtractAll)
	nonSingleOccurrence := false
	if occurrence != nil {
		occ, ok := duckdbToPyInt(occurrence)
		nonSingleOccurrence = !occurrence.IsInt() || (ok && occ > 1)
	}

	name := "REGEXP_EXTRACT"
	if isExtractAll || nonSingleOccurrence {
		name = "REGEXP_EXTRACT_ALL"
	}

	result := New(KAnonymous, "this", name, "expressions", duckdbNonNil(this, e.Expression(), group, flagsExpr))

	// Array slicing for REGEXP_EXTRACT_ALL with occurrence
	if isExtractAll && nonSingleOccurrence {
		result = New(KBracket, "this", result, "expressions", []*Expr{New(KSlice, "this", occurrence)})
	} else if nonSingleOccurrence {
		// ARRAY_EXTRACT for REGEXP_EXTRACT with occurrence > 1
		result = New(KAnonymous, "this", "ARRAY_EXTRACT", "expressions", []*Expr{result, occurrence})
	}

	return g.sql(result)
}

// duckdbRegexpinstrSQL mirrors regexpinstr_sql.
func duckdbRegexpinstrSQL(g *Generator, e *Expr) string {
	this := e.This()
	pattern := e.Expression()
	position := e.ArgE("position")
	origOcc := e.ArgE("occurrence")
	occurrence := origOcc
	if occurrence == nil {
		occurrence = LiteralInt(1)
	}
	option := e.ArgE("option")
	parameters := e.ArgE("parameters")

	validatedFlags := duckdbValidateRegexpFlags(g, parameters, "ims")
	if validatedFlags != "" {
		pattern = New(KConcat, "expressions", []*Expr{LiteralString("(?" + validatedFlags + ")"), pattern})
	}

	// Handle starting position offset
	posOffset := LiteralInt(0)
	if position != nil {
		if pos, ok := duckdbToPyInt(position); !position.IsInt() || (ok && pos > 1) {
			this = New(KSubstring, "this", this, "start", position)
			posOffset = duckdbSub(position, LiteralInt(1))
		}
	}

	// Helper: LIST_SUM(LIST_TRANSFORM(list[1:end], x -> LENGTH(x)))
	sumLengths := func(funcName string, end *Expr) *Expr {
		lst := New(
			KBracket,
			"this", New(KAnonymous, "this", funcName, "expressions", []*Expr{this, pattern}),
			"expressions", []*Expr{New(KSlice, "this", LiteralInt(1), "expression", end)},
			"offset", 1,
		)
		transform := New(
			KAnonymous,
			"this", "LIST_TRANSFORM",
			"expressions", []*Expr{
				lst,
				New(
					KLambda,
					"this", New(KLength, "this", ToIdentifier("x", nil)),
					"expressions", []*Expr{ToIdentifier("x", nil)},
				),
			},
		)
		return New(
			KCoalesce,
			"this", New(KAnonymous, "this", "LIST_SUM", "expressions", []*Expr{transform}),
			"expressions", []*Expr{LiteralInt(0)},
		)
	}

	// Position = 1 + sum(split_lengths[1:occ]) + sum(match_lengths[1:occ-1]) + offset
	basePos := duckdbAdd(
		duckdbAdd(
			duckdbAdd(LiteralInt(1), sumLengths("STRING_SPLIT_REGEX", occurrence)),
			sumLengths("REGEXP_EXTRACT_ALL", duckdbSub(occurrence, LiteralInt(1))),
		),
		posOffset,
	)

	// option=1: add match length for end position
	if option != nil && option.IsInt() {
		if v, _ := duckdbToPyInt(option); v == 1 {
			matchAtOcc := New(
				KBracket,
				"this", New(KAnonymous, "this", "REGEXP_EXTRACT_ALL", "expressions", []*Expr{this, pattern}),
				"expressions", []*Expr{occurrence},
				"offset", 1,
			)
			basePos = duckdbAdd(basePos, New(
				KCoalesce,
				"this", New(KLength, "this", matchAtOcc),
				"expressions", []*Expr{LiteralInt(0)},
			))
		}
	}

	// NULL checks for all provided arguments
	// .copy() is used strictly because .is_() alters the node's parent pointer, mutating the parsed AST
	nullArgs := []*Expr{e.This(), e.Expression(), position, origOcc, option, parameters}
	var nullChecks []*Expr
	for _, arg := range nullArgs {
		if arg != nil {
			nullChecks = append(nullChecks, duckdbIs(arg.Copy(), New(KNull)))
		}
	}

	matches := New(KAnonymous, "this", "REGEXP_EXTRACT_ALL", "expressions", []*Expr{this, pattern})

	c := duckdbWhen(duckdbCase(), OrExpr(nullChecks...), New(KNull), true)
	c = duckdbWhen(c, duckdbEQ(pattern.Copy(), LiteralString("")), LiteralInt(0), true)
	c = duckdbWhen(c, duckdbLT(New(KLength, "this", matches), occurrence), LiteralInt(0), true)
	c = duckdbElse(c, basePos, true)
	return g.sql(c)
}

// duckdbNumbertostrSQL mirrors numbertostr_sql.
func duckdbNumbertostrSQL(g *Generator, e *Expr) string {
	duckdbUnsupportedArgs(g, e, "culture")
	format := e.ArgE("format")
	if format != nil && format.IsInt() {
		return g.fn("FORMAT", "'{:,."+format.Name()+"f}'", e.Arg("this"))
	}

	g.unsupported("Only integer formats are supported by NumberToStr")
	return g.functionFallbackSQL(e)
}

// duckdbAliasesSQL mirrors aliases_sql.
func duckdbAliasesSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KPosexplode) {
		return duckdbPosexplodeSQL(g, this)
	}

	return g.baseAliasesSQL(e)
}

// duckdbPosexplodeSQL mirrors posexplode_sql.
func duckdbPosexplodeSQL(g *Generator, e *Expr) string {
	this := e.This()
	parent := e.Parent()

	// The default Spark aliases are "pos" and "col", unless specified otherwise
	pos, col := ToIdentifier("pos", nil), ToIdentifier("col", nil)

	if parent.IsA(KAliases) {
		// Column case: SELECT POSEXPLODE(col) [AS (a, b)]
		exprs := parent.Expressions()
		if len(exprs) != 2 {
			panic(&ValueError{Msg: fmt.Sprintf("not enough values to unpack (expected 2, got %d)", len(exprs))})
		}
		pos, col = exprs[0], exprs[1]
	} else if parent.IsA(KTable) {
		// Table case: SELECT * FROM POSEXPLODE(col) [AS (a, b)]
		alias := parent.ArgE("alias")
		if alias != nil {
			if cols := alias.ArgL("columns"); len(cols) > 0 {
				if len(cols) != 2 {
					panic(&ValueError{Msg: fmt.Sprintf("too many values to unpack (expected 2, got %d)", len(cols))})
				}
				pos, col = cols[0], cols[1]
			}
			alias.Pop()
		}
	}

	// Translate POSEXPLODE to UNNEST + GENERATE_SUBSCRIPTS
	// Note: In Spark pos is 0-indexed, but in DuckDB it's 1-indexed, so we subtract 1 from GENERATE_SUBSCRIPTS
	unnestSQL := g.sql(New(KUnnest, "expressions", []*Expr{this}, "alias", col))
	genSubscripts := g.sql(New(
		KAlias,
		"this", duckdbSub(
			New(KAnonymous, "this", "GENERATE_SUBSCRIPTS", "expressions", []*Expr{this, LiteralInt(1)}),
			LiteralInt(1),
		),
		"alias", pos,
	))

	posexplodeSQL := g.formatArgs(", ", genSubscripts, unnestSQL)

	if parent.IsA(KFrom) || (parent != nil && parent.Parent().IsA(KFrom)) {
		// SELECT * FROM POSEXPLODE(col) -> SELECT * FROM (SELECT GENERATE_SUBSCRIPTS(...), UNNEST(...))
		return g.sql(New(KSubquery, "this", New(KSelect, "expressions", []*Expr{duckdbRaw(posexplodeSQL)})))
	}

	return posexplodeSQL
}

// duckdbAddmonthsSQL mirrors addmonths_sql.
//
// Handles three key issues:
// 1. Float/decimal months: e.g., Snowflake rounds, whereas DuckDB INTERVAL requires integers
// 2. End-of-month preservation: If input is last day of month, result is last day of result month
// 3. Type preservation: Maintains DATE/TIMESTAMPTZ types (DuckDB defaults to TIMESTAMP).
func duckdbAddmonthsSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.Type() == nil {
		this = annotateTypes(this, g.d)
	}

	if duckdbIsType(this, DataType_TEXT_TYPES.Items()...) {
		this = New(KCast, "this", this, "to", duckdbNewTypeExpr(DT_TIMESTAMP))
	}

	// Detect float/decimal months to apply rounding (Snowflake behavior)
	// DuckDB INTERVAL syntax doesn't support non-integer expressions, so use TO_MONTHS
	monthsExpr := e.Expression()
	if monthsExpr.Type() == nil {
		monthsExpr = annotateTypes(monthsExpr, g.d)
	}

	// Build interval or to_months expression based on type
	var intervalOrToMonths *Expr
	if duckdbIsType(monthsExpr, DT_FLOAT, DT_DOUBLE, DT_DECIMAL) {
		// Float/decimal case: Round and use TO_MONTHS(CAST(ROUND(value) AS INT))
		intervalOrToMonths = duckdbFunc("TO_MONTHS", CastExpr(duckdbFunc("ROUND", monthsExpr), "INT", true, nil))
	} else {
		// Integer case: standard INTERVAL N MONTH syntax
		intervalOrToMonths = New(KInterval, "this", monthsExpr, "unit", VarExpr("MONTH"))
	}

	dateAddExpr := New(KAdd, "this", this, "expression", intervalOrToMonths)

	// Apply end-of-month preservation if Snowflake flag is set
	// CASE WHEN LAST_DAY(date) = date THEN LAST_DAY(result) ELSE result END
	resultExpr := dateAddExpr
	if e.ArgB("preserve_end_of_month") {
		c := duckdbWhen(duckdbCase(),
			New(KEQ, "this", duckdbFunc("LAST_DAY", this), "expression", this),
			duckdbFunc("LAST_DAY", dateAddExpr), true)
		resultExpr = duckdbElse(c, dateAddExpr, true)
	}

	// DuckDB's DATE_ADD function returns TIMESTAMP/DATETIME by default, even when the input is DATE
	// To match for example Snowflake's ADD_MONTHS behavior (which preserves the input type)
	// We need to cast the result back to the original type when the input is DATE or TIMESTAMPTZ
	// Example: ADD_MONTHS('2023-01-31'::date, 1) should return DATE, not TIMESTAMP
	if duckdbIsType(this, DT_DATE, DT_TIMESTAMPTZ) {
		return g.sql(New(KCast, "this", resultExpr, "to", this.Type()))
	}
	return g.sql(resultExpr)
}

// duckdbIsDateUnit mirrors helper.is_date_unit.
func duckdbIsDateUnit(e *Expr) bool {
	if e == nil {
		return false
	}
	switch pyLower(e.Name()) {
	case "day", "week", "month", "quarter", "year", "year_month":
		return true
	}
	return false
}

// duckdbDatetruncSQL mirrors datetrunc_sql.
func duckdbDatetruncSQL(g *Generator, e *Expr) string {
	unitArg := e.ArgE("unit")
	date := e.This()

	weekStart, ok := weekUnitToDow(unitArg)
	unit := unitToStr(e, "DAY")

	var result string
	if ok && weekStart != 0 {
		result = g.sql(duckdbBuildWeekTruncExpression(date, weekStart, true))
	} else {
		result = g.fn("DATE_TRUNC", unit, date)
	}

	if e.ArgB("input_type_preserved") &&
		duckdbIsType(date, DataType_TEMPORAL_TYPES.Items()...) &&
		!(duckdbIsDateUnit(unit) && duckdbIsType(date, DT_DATE)) {
		return g.sql(New(KCast, "this", result, "to", date.Type()))
	}

	return result
}

// duckdbTimestamptruncSQL mirrors timestamptrunc_sql.
func duckdbTimestamptruncSQL(g *Generator, e *Expr) string {
	unit := unitToStr(e, "DAY")
	zone := e.ArgE("zone")
	timestamp := e.This()
	dateUnit := duckdbIsDateUnit(unit)

	if dateUnit && zone != nil {
		// BigQuery's TIMESTAMP_TRUNC with timezone truncates in the target timezone and returns as UTC.
		// Double AT TIME ZONE needed for BigQuery compatibility:
		// 1. First AT TIME ZONE: ensures truncation happens in the target timezone
		// 2. Second AT TIME ZONE: converts the DATE result back to TIMESTAMPTZ (preserving time component)
		timestamp = New(KAtTimeZone, "this", timestamp, "zone", zone)
		resultSQL := g.fn("DATE_TRUNC", unit, timestamp)
		return g.sql(New(KAtTimeZone, "this", resultSQL, "zone", zone))
	}

	result := g.fn("DATE_TRUNC", unit, timestamp)
	if e.ArgB("input_type_preserved") {
		if timestamp.Type() != nil && duckdbIsType(timestamp, DT_TIME, DT_TIMETZ) {
			dummyDate := New(
				KCast,
				"this", LiteralString("1970-01-01"),
				"to", duckdbNewTypeExpr(DT_DATE),
			)
			dateTime := New(KAdd, "this", dummyDate, "expression", timestamp)
			result = g.fn("DATE_TRUNC", unit, dateTime)
			return g.sql(New(KCast, "this", result, "to", timestamp.Type()))
		}

		if duckdbIsType(timestamp, DataType_TEMPORAL_TYPES.Items()...) && !(dateUnit && duckdbIsType(timestamp, DT_DATE)) {
			return g.sql(New(KCast, "this", result, "to", timestamp.Type()))
		}
	}

	return result
}

// duckdbTrimSQL mirrors trim_sql.
func duckdbTrimSQL(g *Generator, e *Expr) string {
	e.This().Replace(duckdbCastToVarchar(e.This()))
	if e.Expression() != nil {
		e.Expression().Replace(duckdbCastToVarchar(e.Expression()))
	}

	resultSQL := g.baseTrimSQL(e)
	return duckdbGenWithCastToBlob(g, e, resultSQL)
}

// duckdbRoundSQL mirrors round_sql.
func duckdbRoundSQL(g *Generator, e *Expr) string {
	this := e.This()
	decimals := e.ArgE("decimals")
	truncate := e.ArgE("truncate")

	// DuckDB requires the scale (decimals) argument to be an INT
	// Some dialects (e.g., Snowflake) allow non-integer scales and cast to an integer internally
	if decimals != nil && e.ArgB("casts_non_integer_decimals") {
		if !(decimals.IsInt() || duckdbIsType(decimals, DataType_INTEGER_TYPES.Items()...)) {
			decimals = CastExpr(decimals, DT_INT, true, nil)
		}
	}

	fn := "ROUND"
	if truncate != nil {
		switch truncate.ThisS() {
		// BigQuery uses ROUND_HALF_EVEN; Snowflake uses HALF_TO_EVEN
		case "ROUND_HALF_EVEN", "HALF_TO_EVEN":
			fn = "ROUND_EVEN"
			truncate = nil
		// BigQuery uses ROUND_HALF_AWAY_FROM_ZERO; Snowflake uses HALF_AWAY_FROM_ZERO
		case "ROUND_HALF_AWAY_FROM_ZERO", "HALF_AWAY_FROM_ZERO":
			truncate = nil
		}
	}

	return g.fn(fn, this, decimals, truncate)
}

// duckdbTrycastSQL mirrors trycast_sql.
func duckdbTrycastSQL(g *Generator, e *Expr) string {
	to := e.ArgE("to")
	toType := to.Arg("this")
	src := e.This()

	toDType, isDType := toType.(DType)
	if e.ArgB("null_on_text_overflow") && isDType && DataType_TEXT_TYPES.Has(toDType) && len(to.Expressions()) > 0 {
		c := duckdbWhen(duckdbCase(),
			New(KLTE, "this", duckdbFunc("LENGTH", src), "expression", to.Expressions()[0].This()),
			CastExpr(src, "TEXT", true, nil), true)
		c = duckdbElse(c, Null(), true)
		return g.sql(c)
	} else if isDType && toDType == DT_DATE && e.ArgB("probe_date_format") {
		slashStrptime := CastExpr(
			duckdbFunc("TRY_STRPTIME", src, LiteralString(g.s._TRYCAST_DATE_SLASH_FMT)),
			"DATE", true, nil,
		)
		monStrptime := CastExpr(
			duckdbFunc("TRY_STRPTIME", src, LiteralString(g.s._TRYCAST_DATE_MON_FMT)),
			"DATE", true, nil,
		)
		c := duckdbWhen(duckdbCase(), duckdbFunc("CONTAINS", src, LiteralString("/")), slashStrptime, true)
		c = duckdbWhen(c, New(KRegexpLike, "this", src, "expression", LiteralString("[A-Za-z]")), monStrptime, true)
		c = duckdbElse(c, New(KTryCast, "this", src, "to", to), true)
		return g.sql(c)
	} else if toExpr, ok := toType.(*Expr); ok && toExpr.IsA(KInterval) && toExpr.ArgB("unit") && e.ArgB("requires_string") {
		unit := toExpr.ArgE("unit")
		intervalType := DataTypeBuild("INTERVAL", nil, false, true)
		if unit.IsA(KIntervalSpan) {
			g.unsupported("TRY_CAST to INTERVAL with span (e.g. HOUR TO MINUTE) is not supported in DuckDB")
			return g.sql(New(KTryCast, "this", src, "to", intervalType))
		}
		return g.sql(New(
			KTryCast,
			"this", New(KDPipe, "this", src, "expression", LiteralString(" "+unit.Name())),
			"to", intervalType,
		))
	}

	return g.baseTrycastSQL(e)
}

// duckdbStrtokSQL mirrors strtok_sql.
func duckdbStrtokSQL(g *Generator, e *Expr) string {
	stringArg := e.This()
	delimiterArg := e.ArgE("delimiter")
	partIndexArg := e.ArgE("part_index")

	if delimiterArg != nil && partIndexArg != nil {
		// Escape regex chars and build character class at runtime using REGEXP_REPLACE
		escapedDelimiter := New(
			KAnonymous,
			"this", "REGEXP_REPLACE",
			"expressions", []*Expr{
				delimiterArg,
				LiteralString(`([\[\]^.\-*+?(){}|$\\])`), // Escape problematic regex chars
				LiteralString(`\\\1`),                    // Replace with escaped version using $1 backreference
				LiteralString("g"),                       // Global flag
			},
		)
		// CASE WHEN delimiter = '' THEN '' ELSE CONCAT('[', escaped_delimiter, ']') END
		regexPattern := duckdbWhen(duckdbCase(), duckdbEQ(delimiterArg, LiteralString("")), LiteralString(""), true)
		regexPattern = duckdbElse(regexPattern,
			duckdbFunc("CONCAT", LiteralString("["), escapedDelimiter, LiteralString("]")),
			true)

		// STRTOK skips empty strings, so we need to filter them out
		// LIST_FILTER(REGEXP_SPLIT_TO_ARRAY(string, pattern), x -> x != '')[index]
		splitArray := duckdbFunc("REGEXP_SPLIT_TO_ARRAY", stringArg, regexPattern)
		x := ToIdentifier("x", nil)
		isEmpty := duckdbEQ(x, LiteralString(""))
		filteredArray := duckdbFunc(
			"LIST_FILTER",
			splitArray,
			New(KLambda, "this", NotExpr(isEmpty.Copy(), true), "expressions", []*Expr{x.Copy()}),
		)
		baseFunc := New(
			KBracket,
			"this", filteredArray,
			"expressions", []*Expr{partIndexArg},
			"offset", 1,
		)

		// Use template with the built regex pattern
		result := duckdbReplacePlaceholders(duckdbStrtokTemplate.get().Copy(), map[string]*Expr{
			"string":     stringArg,
			"delimiter":  delimiterArg,
			"part_index": partIndexArg,
			"base_func":  baseFunc,
		})

		return g.sql(result)
	}

	return g.functionFallbackSQL(e)
}

// duckdbStrtoktoarraySQL mirrors strtoktoarray_sql.
func duckdbStrtoktoarraySQL(g *Generator, e *Expr) string {
	stringArg := e.This()
	delimiterArg := e.ArgE("expression")
	if delimiterArg == nil {
		delimiterArg = LiteralString(" ")
	}

	escaped := New(
		KRegexpReplace,
		"this", delimiterArg.Copy(),
		"expression", LiteralString(`([\[\]^.\-*+?(){}|$\\])`),
		"replacement", LiteralString(`\\\1`),
		"modifiers", LiteralString("g"),
	)
	return g.sql(duckdbReplacePlaceholders(duckdbStrtokToArrayTemplate.get().Copy(), map[string]*Expr{
		"string":    stringArg,
		"delimiter": delimiterArg,
		"escaped":   escaped,
	}))
}

// duckdbApproxquantileSQL mirrors approxquantile_sql.
func duckdbApproxquantileSQL(g *Generator, e *Expr) string {
	result := g.fn("APPROX_QUANTILE", e.Arg("this"), e.ArgE("quantile"))

	// DuckDB returns integers for APPROX_QUANTILE, cast to DOUBLE if the expected type is a real type
	if duckdbIsType(e, DataType_REAL_TYPES.Items()...) {
		result = "CAST(" + result + " AS DOUBLE)"
	}

	return result
}

// duckdbApproxquantilesSQL mirrors approxquantiles_sql.
//
// BigQuery's APPROX_QUANTILES(expr, n) returns an array of n+1 approximate quantile values
// dividing the input distribution into n equal-sized buckets.
func duckdbApproxquantilesSQL(g *Generator, e *Expr) string {
	this := e.This()
	var numQuantilesExpr *Expr
	if this.IsA(KDistinct) {
		// APPROX_QUANTILES requires 2 args and DISTINCT node grabs both
		if len(this.Expressions()) < 2 {
			g.unsupported("APPROX_QUANTILES requires a bucket count argument")
			return g.functionFallbackSQL(e)
		}
		numQuantilesExpr = this.Expressions()[1].Pop()
	} else {
		numQuantilesExpr = e.Expression()
	}

	if !numQuantilesExpr.IsA(KLiteral) || !numQuantilesExpr.IsInt() {
		g.unsupported("APPROX_QUANTILES bucket count must be a positive integer")
		return g.functionFallbackSQL(e)
	}

	numQuantiles, _ := duckdbToPyInt(numQuantilesExpr)
	if numQuantiles <= 0 {
		g.unsupported("APPROX_QUANTILES bucket count must be a positive integer")
		return g.functionFallbackSQL(e)
	}

	quantiles := make([]*Expr, 0, numQuantiles+1)
	for i := 0; i <= numQuantiles; i++ {
		quantiles = append(quantiles, LiteralNumber(duckdbDecimalDivStr(i, numQuantiles)))
	}

	return g.sql(New(KApproxQuantile, "this", this, "quantile", New(KArray, "expressions", quantiles)))
}

// duckdbJsonextractscalarSQL mirrors jsonextractscalar_sql.
func duckdbJsonextractscalarSQL(g *Generator, e *Expr) string {
	if e.ArgB("scalar_only") {
		e = New(
			KJSONExtractScalar,
			"this", renameFunc("JSON_VALUE")(g, e),
			"expression", "'$'",
		)
	}
	return duckdbArrowJSONExtractSQL(g, e)
}

// duckdbBitwisenotSQL mirrors bitwisenot_sql.
func duckdbBitwisenotSQL(g *Generator, e *Expr) string {
	this := e.This()

	if duckdbIsBinary(this) {
		e.SetType(duckdbNewTypeExpr(DT_BINARY))
	}

	arg := duckdbCastToBit(this)

	if this.IsA(KNeg) {
		arg = New(KParen, "this", arg)
	}

	e.Set("this", arg)

	resultSQL := "~" + g.sqlKey(e, "this")

	return duckdbGenWithCastToBlob(g, e, resultSQL)
}

// duckdbWindowSQL mirrors window_sql.
func duckdbWindowSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KCorr) || (this.IsA(KFilter) && this.This().IsA(KCorr)) {
		return duckdbCorrSQL(g, e)
	}

	return g.baseWindowSQL(e)
}

// duckdbFilterSQL mirrors filter_sql.
func duckdbFilterSQL(g *Generator, e *Expr) string {
	if e.This().IsA(KCorr) {
		return duckdbCorrSQL(g, e)
	}

	return g.baseFilterSQL(e)
}

// duckdbCorrSQL mirrors DuckDBGenerator._corr_sql.
func duckdbCorrSQL(g *Generator, e *Expr) string {
	if e.IsA(KCorr) && !e.ArgB("null_on_zero_variance") {
		return g.fn("CORR", e.Arg("this"), e.Arg("expression"))
	}

	corrExpr := duckdbMaybeCorrNullToFalse(e)
	if corrExpr == nil {
		if e.IsA(KWindow) {
			return g.baseWindowSQL(e)
		}
		if e.IsA(KFilter) {
			return g.baseFilterSQL(e)
		}
		corrExpr = e // make mypy happy
	}

	c := duckdbWhen(duckdbCase(), New(KIsNan, "this", corrExpr), Null(), true)
	c = duckdbElse(c, corrExpr, true)
	return g.sql(c)
}

// duckdbUuidSQL mirrors uuid_sql.
func duckdbUuidSQL(g *Generator, e *Expr) string {
	namespace := e.This()
	name := e.ArgE("name")

	// UUID v5 (namespace + name) - Emulate using SHA1
	if namespace != nil && name != nil {
		result := duckdbReplacePlaceholders(duckdbUUIDV5Template.get().Copy(), map[string]*Expr{
			"namespace": namespace,
			"name":      name,
		})
		return g.sql(result)
	}

	return g.baseUuidSQL(e)
}
