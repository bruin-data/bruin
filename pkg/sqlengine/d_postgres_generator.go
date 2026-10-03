package sqlengine

import (
	"math/big"
	"strings"
	"sync"
)

// Port of sqlglot/generators/postgres.py (PostgresGenerator).

// postgresDateDiffFactor mirrors generators.postgres.DATE_DIFF_FACTOR.
var postgresDateDiffFactor = map[string]string{
	"MICROSECOND": " * 1000000",
	"MILLISECOND": " * 1000",
	"SECOND":      "",
	"MINUTE":      " / 60",
	"HOUR":        " / 3600",
	"DAY":         " / 86400",
}

// postgresOptStr maps an Optional[str] result ("" = None) to a g.fn argument.
func postgresOptStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// postgresDateAddSQL mirrors generators.postgres._date_add_sql(kind).
func postgresDateAddSQL(kind string) GenFunc {
	return func(g *Generator, expression *Expr) string {
		if expression.IsA(KTsOrDsAdd) {
			expression = tsOrDsAddCast(expression)
		}

		this := g.sqlKey(expression, "this")
		unit := expression.Arg("unit")

		e := g.simplifyUnlessLiteral(expression.Expression())
		if e.IsA(KInterval) {
			return this + " " + kind + " " + g.sql(e)
		} else if e.IsA(KLiteral) {
			e.Set("is_string", true)
		} else if e.IsNumber() {
			e = LiteralString(chunkDPyNumberStr(e))
		} else {
			one := LiteralInt(1)
			intervalTimesValue := dhBinop(KMul, New(KInterval, "this", one, "unit", unit), e)
			return this + " " + kind + " " + g.sql(intervalTimesValue)
		}

		return this + " " + kind + " " + g.sql(New(KInterval, "this", e, "unit", unit))
	}
}

// postgresDateDiffSQL mirrors generators.postgres._date_diff_sql.
func postgresDateDiffSQL(g *Generator, expression *Expr) string {
	unit := pyUpper(expression.Text("unit"))
	if unit == "" {
		unit = "DAY"
	}

	// Dialects like MySQL count crossed day boundaries, which maps to DATE subtraction
	if unit == "DAY" && expression.ArgB("date_part_boundary") {
		this := CastExpr(expression.This(), DT_DATE, true, nil)
		expr := CastExpr(expression.Expression(), DT_DATE, true, nil)
		return g.sql(ParenExpr(dhBinop(KSub, this, expr), true))
	}

	factor, hasFactor := postgresDateDiffFactor[unit]

	end := "CAST(" + g.sqlKey(expression, "this") + " AS TIMESTAMP)"
	start := "CAST(" + g.sqlKey(expression, "expression") + " AS TIMESTAMP)"

	if hasFactor {
		return "CAST(EXTRACT(epoch FROM " + end + " - " + start + ")" + factor + " AS BIGINT)"
	}

	age := "AGE(" + end + ", " + start + ")"

	switch unit {
	case "WEEK":
		unit = "EXTRACT(days FROM (" + end + " - " + start + ")) / 7"
	case "MONTH":
		unit = "EXTRACT(year FROM " + age + ") * 12 + EXTRACT(month FROM " + age + ")"
	case "QUARTER":
		unit = "EXTRACT(year FROM " + age + ") * 4 + EXTRACT(month FROM " + age + ") / 3"
	case "YEAR":
		unit = "EXTRACT(year FROM " + age + ")"
	default:
		unit = age
	}

	return "CAST(" + unit + " AS BIGINT)"
}

// postgresSubstringSQL mirrors generators.postgres._substring_sql.
func postgresSubstringSQL(g *Generator, expression *Expr) string {
	this := g.sqlKey(expression, "this")
	start := g.sqlKey(expression, "start")
	length := g.sqlKey(expression, "length")

	fromPart := ""
	if start != "" {
		fromPart = " FROM " + start
	}
	forPart := ""
	if length != "" {
		forPart = " FOR " + length
	}

	return "SUBSTRING(" + this + fromPart + forPart + ")"
}

// postgresRemoveFirstEqual mirrors list.remove(x) (structural equality) on an args list.
func postgresRemoveFirstEqual(list []*Expr, x *Expr) []*Expr {
	for i, y := range list {
		if y.Equal(x) {
			out := make([]*Expr, 0, len(list)-1)
			out = append(out, list[:i]...)
			return append(out, list[i+1:]...)
		}
	}
	panic(&ValueError{Msg: "list.remove(x): x not in list"})
}

// postgresAutoIncrementToSerial mirrors generators.postgres._auto_increment_to_serial.
func postgresAutoIncrementToSerial(expression *Expr) *Expr {
	auto := expression.Find(KAutoIncrementColumnConstraint)

	if auto != nil {
		expression.SetArgRaw("constraints", postgresRemoveFirstEqual(expression.ArgL("constraints"), auto.Parent()))
		kind := expression.ArgE("kind")

		switch kind.Arg("this") {
		case DT_INT:
			kind.Replace(New(KDataType, "this", DT_SERIAL))
		case DT_SMALLINT:
			kind.Replace(New(KDataType, "this", DT_SMALLSERIAL))
		case DT_BIGINT:
			kind.Replace(New(KDataType, "this", DT_BIGSERIAL))
		}
	}

	return expression
}

// postgresSerialToGenerated mirrors generators.postgres._serial_to_generated.
func postgresSerialToGenerated(expression *Expr) *Expr {
	if !expression.IsA(KColumnDef) {
		return expression
	}
	kind := expression.ArgE("kind")
	if kind == nil {
		return expression
	}

	var dataType *Expr
	switch kind.Arg("this") {
	case DT_SERIAL:
		dataType = New(KDataType, "this", DT_INT)
	case DT_SMALLSERIAL:
		dataType = New(KDataType, "this", DT_SMALLINT)
	case DT_BIGSERIAL:
		dataType = New(KDataType, "this", DT_BIGINT)
	}

	if dataType != nil {
		expression.ArgE("kind").Replace(dataType)
		if !expression.HasArgKey("constraints") {
			// expression.args["constraints"] raises KeyError in Python
			panic(&ValueError{Msg: "'constraints'"})
		}
		constraints := expression.ArgL("constraints")
		generated := New(KColumnConstraint, "kind", New(KGeneratedAsIdentityColumnConstraint, "this", false))
		notnull := New(KColumnConstraint, "kind", New(KNotNullColumnConstraint))

		contains := func(list []*Expr, x *Expr) bool {
			for _, y := range list {
				if y.Equal(x) {
					return true
				}
			}
			return false
		}

		// Python inserts into the args list in place (no parent bookkeeping).
		if !contains(constraints, notnull) {
			constraints = append([]*Expr{notnull}, constraints...)
		}
		if !contains(constraints, generated) {
			constraints = append([]*Expr{generated}, constraints...)
		}
		expression.SetArgRaw("constraints", constraints)
	}

	return expression
}

// postgresJSONExtractSQL mirrors generators.postgres._json_extract_sql(name, op).
func postgresJSONExtractSQL(name, op string) GenFunc {
	return func(g *Generator, expression *Expr) string {
		if expression.ArgB("only_json_types") {
			return jsonExtractSegments(name, false, op)(g, expression)
		}
		return jsonExtractSegments(name, true, "")(g, expression)
	}
}

// postgresUnixToTimeSQL mirrors generators.postgres._unix_to_time_sql.
func postgresUnixToTimeSQL(g *Generator, expression *Expr) string {
	scale := expression.ArgE("scale")
	timestamp := expression.This()

	if scale == nil || scale.Equal(LiteralInt(0)) {
		return g.fn("TO_TIMESTAMP", timestamp, postgresOptStr(g.formatTime(expression, nil, nil)))
	}

	return g.fn(
		"TO_TIMESTAMP",
		New(KDiv, "this", timestamp, "expression", dhFunc("POW", 10, scale)),
		postgresOptStr(g.formatTime(expression, nil, nil)),
	)
}

// postgresLevenshteinSQL mirrors generators.postgres._levenshtein_sql.
func postgresLevenshteinSQL(g *Generator, expression *Expr) string {
	name := "LEVENSHTEIN"
	if expression.ArgB("max_dist") {
		name = "LEVENSHTEIN_LESS_EQUAL"
	}

	return renameFunc(name)(g, expression)
}

// postgresVersionedAnyvalueSQL mirrors generators.postgres._versioned_anyvalue_sql.
func postgresVersionedAnyvalueSQL(g *Generator, expression *Expr) string {
	// https://www.postgresql.org/docs/16/functions-aggregate.html
	// https://www.postgresql.org/about/featurematrix/
	if g.d.Version[0] < 16 {
		return anyValueToMaxSQL(g, expression)
	}

	return renameFunc("ANY_VALUE")(g, expression)
}

// postgresRoundSQL mirrors generators.postgres._round_sql.
func postgresRoundSQL(g *Generator, expression *Expr) string {
	this := g.sqlKey(expression, "this")
	decimals := g.sqlKey(expression, "decimals")

	if decimals == "" {
		return g.fn("ROUND", this)
	}

	if expression.Type() == nil {
		expression = annotateTypes(expression, g.d)
	}

	// ROUND(double precision, integer) is not permitted in Postgres
	// so it's necessary to cast to decimal before rounding.
	if dhIsType(expression.This(), DT_DOUBLE) {
		decimalType := New(KDataType, "this", DT_DECIMAL)
		decimalType.Set("expressions", expression.Expressions())
		this = g.sql(New(KCast, "this", this, "to", decimalType))
	}

	return g.fn("ROUND", this, decimals)
}

func customizePostgresGenerator(d *Dialect) {
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (module-level dict)
	for _, k := range []string{"cluster", "distribute", "sort"} {
		delete(G.AFTER_HAVING_MODIFIER_TRANSFORMS, k)
	}
	keys := make([]string, 0, len(G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS))
	for _, k := range G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if _, ok := G.AFTER_HAVING_MODIFIER_TRANSFORMS[k]; ok {
			keys = append(keys, k)
		}
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	T := G.TRANSFORMS
	delete(T, KCommentColumnConstraint)

	T[KAnyValue] = postgresVersionedAnyvalueSQL
	T[KArrayConcat] = arrayConcatSQL("ARRAY_CAT")
	T[KArrayFilter] = filterArrayUsingUnnest
	T[KArrayAppend] = arrayAppendSQL("ARRAY_APPEND", false)
	T[KArrayPrepend] = arrayAppendSQL("ARRAY_PREPEND", true)
	T[KBitwiseAndAgg] = renameFunc("BIT_AND")
	T[KBitwiseOrAgg] = renameFunc("BIT_OR")
	T[KBitwiseXor] = func(g *Generator, e *Expr) string { return g.binary(e, "#") }
	T[KBitwiseXorAgg] = renameFunc("BIT_XOR")
	T[KColumnDef] = transformPreprocess([]func(*Expr) *Expr{postgresAutoIncrementToSerial, postgresSerialToGenerated}, nil)
	T[KCurrentDate] = noParenCurrentDateSQL
	T[KCurrentTimestamp] = func(g *Generator, e *Expr) string { return "CURRENT_TIMESTAMP" }
	T[KCurrentUser] = func(g *Generator, e *Expr) string { return "CURRENT_USER" }
	T[KCurrentVersion] = renameFunc("VERSION")
	T[KDateAdd] = postgresDateAddSQL("+")
	T[KDateDiff] = postgresDateDiffSQL
	T[KDateStrToDate] = datestrtodateSQL
	T[KDateSub] = postgresDateAddSQL("-")
	T[KExplode] = renameFunc("UNNEST")
	T[KExplodingGenerateSeries] = renameFunc("GENERATE_SERIES")
	T[KGenerateSeries] = generateSeriesSQL("GENERATE_SERIES", "")
	T[KGetbit] = getbitSQL
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		return groupconcatSQL(g, e, "STRING_AGG", ",", false, false)
	}
	T[KIntDiv] = renameFunc("DIV")
	T[KJSONArrayAgg] = func(g *Generator, e *Expr) string {
		return g.funcFull("JSON_AGG", "(", g.sqlKey(e, "order")+")", true, g.sqlKey(e, "this"))
	}
	T[KJSONExtract] = postgresJSONExtractSQL("JSON_EXTRACT_PATH", "->")
	T[KJSONExtractScalar] = postgresJSONExtractSQL("JSON_EXTRACT_PATH_TEXT", "->>")
	T[KJSONBExtract] = func(g *Generator, e *Expr) string { return g.binary(e, "#>") }
	T[KJSONBExtractScalar] = func(g *Generator, e *Expr) string { return g.binary(e, "#>>") }
	T[KJSONBContains] = func(g *Generator, e *Expr) string { return g.binary(e, "?") }
	T[KParseJSON] = func(g *Generator, e *Expr) string {
		return g.sql(CastExpr(e.This(), DT_JSON, true, nil))
	}
	T[KJSONPathKey] = jsonPathKeyOnlyName
	T[KJSONPathRoot] = func(g *Generator, e *Expr) string { return "" }
	T[KJSONPathSubscript] = func(g *Generator, e *Expr) string { return g.jsonPathPart(e.Arg("this")) }
	T[KLastDay] = noLastDaySQL
	T[KLogicalOr] = renameFunc("BOOL_OR")
	T[KLogicalAnd] = renameFunc("BOOL_AND")
	T[KMax] = maxOrGreatest
	T[KMapFromEntries] = noMapFromEntriesSQL
	T[KMin] = minOrLeast
	T[KMerge] = mergeWithoutTargetSQL
	T[KPartitionedByProperty] = func(g *Generator, e *Expr) string {
		return "PARTITION BY " + g.sqlKey(e, "this")
	}
	T[KPercentileCont] = transformPreprocess([]func(*Expr) *Expr{transformAddWithinGroupForPercentiles}, nil)
	T[KPercentileDisc] = transformPreprocess([]func(*Expr) *Expr{transformAddWithinGroupForPercentiles}, nil)
	T[KPivot] = noPivotSQL
	T[KRand] = renameFunc("RANDOM")
	T[KRegexpLike] = func(g *Generator, e *Expr) string { return g.binary(e, "~") }
	T[KRegexpILike] = func(g *Generator, e *Expr) string { return g.binary(e, "~*") }
	T[KRegexpReplace] = func(g *Generator, e *Expr) string {
		return g.fn(
			"REGEXP_REPLACE",
			e.Arg("this"),
			e.Arg("expression"),
			e.ArgE("replacement"),
			e.ArgE("position"),
			e.ArgE("occurrence"),
			regexpReplaceGlobalModifier(e),
		)
	}
	T[KRound] = postgresRoundSQL
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateSemiAndAntiJoins,
		transformEliminateQualify,
	}, nil)
	T[KSHA2] = sha256SQL
	T[KSHA2Digest] = sha2DigestSQL
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "POSITION", false, false, true)
	}
	T[KStrToDate] = func(g *Generator, e *Expr) string {
		return g.fn("TO_DATE", e.Arg("this"), postgresOptStr(g.formatTime(e, nil, nil)))
	}
	T[KStrToTime] = func(g *Generator, e *Expr) string {
		return g.fn("TO_TIMESTAMP", e.Arg("this"), postgresOptStr(g.formatTime(e, nil, nil)))
	}
	T[KStructExtract] = structExtractSQL
	T[KSubstring] = postgresSubstringSQL
	T[KTimeFromParts] = renameFunc("MAKE_TIME")
	T[KTimestampFromParts] = renameFunc("MAKE_TIMESTAMP")
	T[KTimestampTrunc] = timestamptruncSQL("DATE_TRUNC", true)
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, false) }
	T[KTimeToStr] = func(g *Generator, e *Expr) string {
		return g.fn("TO_CHAR", e.Arg("this"), postgresOptStr(g.formatTime(e, nil, nil)))
	}
	T[KToChar] = func(g *Generator, e *Expr) string {
		if e.ArgB("format") {
			return g.functionFallbackSQL(e)
		}
		return g.tocharSQL(e)
	}
	T[KTrim] = func(g *Generator, e *Expr) string { return trimSQL(g, e, "") }
	T[KTryCast] = noTrycastSQL
	T[KTsOrDsAdd] = postgresDateAddSQL("+")
	T[KTsOrDsDiff] = postgresDateDiffSQL
	T[KUuid] = func(g *Generator, e *Expr) string { return "GEN_RANDOM_UUID()" }
	T[KTimeToUnix] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_PART", LiteralString("epoch"), e.Arg("this"))
	}
	T[KVariancePop] = renameFunc("VAR_POP")
	T[KVariance] = renameFunc("VAR_SAMP")
	T[KXor] = boolXorSQL
	T[KUnicode] = renameFunc("ASCII")
	// exp.UnixToTime appears twice in the Python dict literal; the later entry wins.
	T[KUnixToTime] = postgresUnixToTimeSQL
	T[KLevenshtein] = postgresLevenshteinSQL
	T[KJSONObjectAgg] = renameFunc("JSON_OBJECT_AGG")
	T[KJSONBObjectAgg] = renameFunc("JSONB_OBJECT_AGG")
	T[KCountIf] = countIfToSum

	// <key>_sql method overrides
	G.h.lateralSQL = postgresLateralSQL
	G.h.columndefSQL = postgresColumndefSQL
	G.h.unnestSQL = postgresUnnestSQL
	G.h.bracketSQL = postgresBracketSQL
	G.h.matchagainstSQL = postgresMatchagainstSQL
	G.h.altersetSQL = postgresAltersetSQL
	G.h.datatypeSQL = postgresDatatypeSQL
	G.h.castSQL = postgresCastSQL
	G.h.computedcolumnconstraintSQL = postgresComputedcolumnconstraintSQL
	G.h.ignorenullsSQL = postgresIgnorenullsSQL
	G.h.respectnullsSQL = postgresRespectnullsSQL
	G.h.intervalSQL = postgresIntervalSQL
	G.h.placeholderSQL = postgresPlaceholderSQL

	// dialect-only <key>_sql methods
	G.methods[KSchemaCommentProperty] = postgresSchemacommentpropertySQL
	G.methods[KCommentColumnConstraint] = postgresCommentcolumnconstraintSQL
	G.methods[KArray] = postgresArraySQL
	G.methods[KIsAscii] = postgresIsasciiSQL
	G.methods[KCurrentSchema] = postgresCurrentschemaSQL
	G.methods[KArrayContains] = postgresArraycontainsSQL
}

func postgresLateralSQL(g *Generator, expression *Expr) string {
	sql := g.baseLateralSQL(expression)

	if expression.Arg("cross_apply") != nil {
		sql = sql + " ON TRUE"
	}

	return sql
}

func postgresSchemacommentpropertySQL(g *Generator, expression *Expr) string {
	g.unsupported("Table comments are not supported in the CREATE statement")
	return ""
}

func postgresCommentcolumnconstraintSQL(g *Generator, expression *Expr) string {
	g.unsupported("Column comments are not supported in the CREATE statement")
	return ""
}

func postgresColumndefSQL(g *Generator, expression *Expr, sep string) string {
	// PostgreSQL places parameter modes BEFORE parameter name
	paramConstraint := expression.Find(KInOutColumnConstraint)

	if paramConstraint != nil {
		modeSQL := g.sql(paramConstraint)
		paramConstraint.Pop() // Remove to prevent double-rendering
		baseSQL := g.baseColumndefSQL(expression, sep)
		return modeSQL + " " + baseSQL
	}

	return g.baseColumndefSQL(expression, sep)
}

var (
	postgresArrayJSONType     *Expr
	postgresArrayJSONTypeOnce sync.Once
	postgresBigInt3           = big.NewInt(3)
)

func postgresUnnestSQL(g *Generator, expression *Expr) string {
	if len(expression.Expressions()) == 1 {
		arg := expression.Expressions()[0]
		if arg.IsA(KGenerateDateArray) {
			kv := []any{}
			for _, k := range arg.ArgKeys() {
				kv = append(kv, k, arg.Arg(k))
			}
			generateSeries := New(KGenerateSeries, kv...)
			if expression.Parent().IsA(KFrom, KJoin) {
				var alias any = "_unnested_generate_series"
				if a := expression.ArgE("alias"); a != nil {
					alias = a
				}
				table := AliasTableExpr(New(KTable, "this", generateSeries), "_t", []any{"value"}, nil, true)
				generateSeries = SelectExpr(MaybeParse("value::date", KNone, "", nil)).
					SelectFrom(table, true).
					QuerySubquery(alias, true)
			}
			return g.sql(generateSeries)
		}

		this := annotateTypes(arg, g.d)
		postgresArrayJSONTypeOnce.Do(func() {
			postgresArrayJSONType = DataTypeBuild("array<json>", nil, true, true)
		})
		if dhIsTypeAny(this, postgresArrayJSONType) {
			for this.IsA(KCast) {
				this = this.This()
			}

			argAsJSON := g.sql(CastExpr(this, DT_JSON, true, nil))
			alias := g.sqlKey(expression, "alias")
			if alias != "" {
				alias = " AS " + alias
			}

			if expression.ArgB("offset") {
				g.unsupported("Unsupported JSON_ARRAY_ELEMENTS with offset")
			}

			return "JSON_ARRAY_ELEMENTS(" + argAsJSON + ")" + alias
		}
	}

	return g.baseUnnestSQL(expression)
}

// Forms like ARRAY[1, 2, 3][3] aren't allowed; we need to wrap the ARRAY.
func postgresBracketSQL(g *Generator, expression *Expr) string {
	if expression.This().IsA(KArray) {
		expression.Set("this", ParenExpr(expression.This(), false))
	}

	return g.baseBracketSQL(expression)
}

func postgresMatchagainstSQL(g *Generator, expression *Expr) string {
	this := g.sqlKey(expression, "this")
	var expressions []string
	for _, e := range expression.Expressions() {
		expressions = append(expressions, g.sql(e)+" @@ "+this)
	}
	sql := strings.Join(expressions, " OR ")
	if len(expressions) > 1 {
		return "(" + sql + ")"
	}
	return sql
}

func postgresAltersetSQL(g *Generator, expression *Expr) string {
	exprs := g.expressions(expression, exprsOpts{flat: true})
	if exprs != "" {
		exprs = "(" + exprs + ")"
	}

	accessMethod := g.sqlKey(expression, "access_method")
	if accessMethod != "" {
		accessMethod = "ACCESS METHOD " + accessMethod
	}
	tablespace := g.sqlKey(expression, "tablespace")
	if tablespace != "" {
		tablespace = "TABLESPACE " + tablespace
	}
	option := g.sqlKey(expression, "option")

	return "SET " + exprs + accessMethod + tablespace + option
}

func postgresDatatypeSQL(g *Generator, expression *Expr) string {
	if DataTypeIsType(expression, []any{DT_ARRAY}, false) {
		if len(expression.Expressions()) > 0 {
			values := g.expressions(expression, exprsOpts{key: "values", flat: true})
			return g.expressions(expression, exprsOpts{flat: true}) + "[" + values + "]"
		}
		return "ARRAY"
	}

	if DataTypeIsType(expression, []any{DT_ENUM}, false) {
		return "ENUM (" + g.expressions(expression, exprsOpts{flat: true}) + ")"
	}

	if DataTypeIsType(expression, []any{DT_DOUBLE, DT_FLOAT}, false) && len(expression.Expressions()) > 0 {
		// Postgres doesn't support precision for REAL and DOUBLE PRECISION types
		return "FLOAT(" + g.expressions(expression, exprsOpts{flat: true}) + ")"
	}

	return g.baseDatatypeSQL(expression)
}

func postgresCastSQL(g *Generator, expression *Expr, safePrefix string) string {
	this := expression.This()

	// Postgres casts DIV() to decimal for transpilation but when roundtripping it's superfluous
	if this.IsA(KIntDiv) && expression.ArgE("to").Equal(New(KDataType, "this", DT_DECIMAL)) {
		return g.sql(this)
	}

	return g.baseCastSQL(expression, safePrefix)
}

func postgresArraySQL(g *Generator, expression *Expr) string {
	exprs := expression.Expressions()
	funcName := g.normalizeFunc("ARRAY")

	if seqGet(exprs, 0).IsA(KQuery) {
		return funcName + "(" + g.sql(exprs[0]) + ")"
	}

	return funcName + inlineArraySQL(g, expression)
}

func postgresComputedcolumnconstraintSQL(g *Generator, expression *Expr) string {
	return "GENERATED ALWAYS AS (" + g.sqlKey(expression, "this") + ") STORED"
}

func postgresIsasciiSQL(g *Generator, expression *Expr) string {
	return "(" + g.sql(expression.Arg("this")) + " ~ '^[[:ascii:]]*$')"
}

func postgresIgnorenullsSQL(g *Generator, expression *Expr) string {
	// https://www.postgresql.org/docs/current/functions-window.html
	g.unsupported("PostgreSQL does not support IGNORE NULLS.")
	return g.sql(expression.Arg("this"))
}

func postgresRespectnullsSQL(g *Generator, expression *Expr) string {
	// https://www.postgresql.org/docs/current/functions-window.html
	g.unsupported("PostgreSQL does not support RESPECT NULLS.")
	return g.sql(expression.Arg("this"))
}

func postgresCurrentschemaSQL(g *Generator, expression *Expr) string {
	dhUnsupportedArgs(g, expression, "this")
	return "CURRENT_SCHEMA"
}

func postgresIntervalSQL(g *Generator, expression *Expr) string {
	unit := pyLower(expression.Text("unit"))

	this := expression.This()
	if strings.HasPrefix(unit, "quarter") && this.IsA(KLiteral) {
		n := postgresPyInt(this)
		n = new(big.Int).Mul(n, postgresBigInt3)
		this.Replace(LiteralString(n.String()))
		expression.ArgE("unit").Replace(VarChecked("MONTH"))
	}

	return g.baseIntervalSQL(expression)
}

func postgresPlaceholderSQL(g *Generator, expression *Expr) string {
	if expression.ArgB("jdbc") {
		return "?"
	}

	this := ""
	if expression.ArgB("this") {
		this = "(" + expression.Name() + ")"
	}
	return g.s.NAMED_PLACEHOLDER_TOKEN + this + "s"
}

func postgresArraycontainsSQL(g *Generator, expression *Expr) string {
	// Convert DuckDB's LIST_CONTAINS(array, value) to PostgreSQL
	// DuckDB behavior:
	//   - LIST_CONTAINS([1,2,3], 2) -> true
	//   - LIST_CONTAINS([1,2,3], 4) -> false
	//   - LIST_CONTAINS([1,2,NULL], 4) -> false (not NULL)
	//   - LIST_CONTAINS([1,2,3], NULL) -> NULL
	//
	// PostgreSQL equivalent: CASE WHEN value IS NULL THEN NULL
	//                            ELSE COALESCE(value = ANY(array), FALSE) END
	value := expression.Expression()
	array := expression.This()

	coalesceExpr := New(
		KCoalesce,
		"this", dhBinop(KEQ, value, New(KAny, "this", New(KParen, "this", array))),
		"expressions", []*Expr{Boolean(false)},
	)

	caseExpr := New(KCase)
	caseExpr.Append("ifs", New(KIf, "this", New(KIs, "this", value, "expression", Null()), "true", Null()))
	caseExpr.Set("default", coalesceExpr)

	return g.sql(caseExpr)
}
