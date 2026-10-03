package sqlengine

import (
	"fmt"
	"strings"
)

// Port of sqlglot/generators/presto.py (sqlglot v30.13.0).

// prestoFmtArg converts a Generator.format_time result ("" means None) into a Generator.func argument.
func prestoFmtArg(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// prestoPyFmt mirrors f"{value}" of a Generator.format_time result ("" means None).
func prestoPyFmt(s string) string {
	if s == "" {
		return "None"
	}
	return s
}

// prestoSha2DigestSQL mirrors generators.presto._sha2_digest_sql.
func prestoSha2DigestSQL(g *Generator, e *Expr) string {
	length := e.Text("length")
	if length == "" {
		length = "256"
	}
	if length != "256" && length != "512" {
		g.unsupported("SHA" + length + " is not supported in Presto")
	}

	this := e.This()
	if dhIsType(this, DataType_TEXT_TYPES.Items()...) {
		// the native digest takes VARBINARY, so a text-typed argument needs encoding
		this = New(KEncode, "this", this, "charset", LiteralString("utf-8"))
	}

	return g.fn("SHA"+length, this)
}

// prestoInitcapSQL mirrors generators.presto._initcap_sql.
func prestoInitcapSQL(g *Generator, e *Expr) string {
	delimiters := e.Expression()
	if delimiters != nil && !(delimiters.IsString() && delimiters.ThisS() == g.d.S.INITCAP_DEFAULT_DELIMITER_CHARS) {
		g.unsupported("INITCAP does not support custom delimiters")
	}

	regex := `(\w)(\w*)`
	return "REGEXP_REPLACE(" + g.sqlKey(e, "this") + ", '" + regex + "', x -> UPPER(x[1]) || LOWER(x[2]))"
}

// prestoNoSortArray mirrors generators.presto._no_sort_array.
func prestoNoSortArray(g *Generator, e *Expr) string {
	var comparator any
	if asc := e.ArgE("asc"); asc != nil && asc.Equal(Boolean(false)) {
		comparator = "(a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END"
	}
	return g.fn("ARRAY_SORT", e.Arg("this"), comparator)
}

// prestoSchemaSQL mirrors generators.presto._schema_sql.
func prestoSchemaSQL(g *Generator, e *Expr) string {
	if e.Parent().IsA(KPartitionedByProperty) {
		// Any columns in the ARRAY[] string literals should not be quoted
		// (Python replaces each Identifier by its name, a plain str; a Var renders the same text.)
		e.Transform(func(n *Expr) *Expr {
			if n.IsA(KIdentifier) {
				return dhRawSQL(n.Name())
			}
			return n
		}, false)

		var partitionExprs []*Expr
		for _, c := range e.Expressions() {
			var s string
			if c.IsA(KFunc, KProperty) {
				s = g.sql(c)
			} else {
				s = g.sqlKey(c, "this")
			}
			partitionExprs = append(partitionExprs, LiteralString(s))
		}
		if partitionExprs == nil {
			partitionExprs = []*Expr{}
		}
		return g.sql(New(KArray, "expressions", partitionExprs))
	}

	if parent := e.Parent(); parent != nil {
		for schema := range parent.FindAll(KSchema) {
			if schema == e {
				continue
			}

			// `column_defs` is a generator (always truthy); `expression.expressions` is a fresh
			// empty list (so extending it is a no-op) unless the arg holds a non-empty list.
			if schema.Parent().IsA(KProperty) {
				if exprs := e.Expressions(); len(exprs) > 0 {
					for cd := range schema.FindAll(KColumnDef) {
						exprs = append(exprs, cd)
					}
					// list.extend does not re-parent the column defs
					e.SetArgRaw("expressions", exprs)
				}
			}
		}
	}

	return g.schemaSQL(e)
}

// prestoQuantileSQL mirrors generators.presto._quantile_sql.
func prestoQuantileSQL(g *Generator, e *Expr) string {
	g.unsupported("Presto does not support exact quantiles")
	return g.fn("APPROX_PERCENTILE", e.Arg("this"), e.Arg("quantile"))
}

// prestoStrToTimeSQL mirrors generators.presto._str_to_time_sql.
func prestoStrToTimeSQL(g *Generator, e *Expr) string {
	return g.fn("DATE_PARSE", e.Arg("this"), prestoFmtArg(g.formatTime(e, nil, nil)))
}

// prestoTsOrDsToDateSQL mirrors generators.presto._ts_or_ds_to_date_sql.
func prestoTsOrDsToDateSQL(g *Generator, e *Expr) string {
	timeFormat := g.formatTime(e, nil, nil)
	if timeFormat != "" && timeFormat != g.d.S.TIME_FORMAT && timeFormat != g.d.S.DATE_FORMAT {
		// exp.cast parses the generated SQL string with the default dialect
		return g.sql(CastExpr(MaybeParse(prestoStrToTimeSQL(g, e), KNone, "", nil), DT_DATE, false, nil))
	}
	return g.sql(CastExpr(CastExpr(e.This(), DT_TIMESTAMP, true, nil), DT_DATE, true, nil))
}

// prestoTsOrDsAddSQL mirrors generators.presto._ts_or_ds_add_sql.
func prestoTsOrDsAddSQL(g *Generator, e *Expr) string {
	e = tsOrDsAddCast(e)
	unit := unitToStr(e, "DAY")
	return g.fn("DATE_ADD", unit, e.Arg("expression"), e.Arg("this"))
}

// prestoTsOrDsDiffSQL mirrors generators.presto._ts_or_ds_diff_sql.
func prestoTsOrDsDiffSQL(g *Generator, e *Expr) string {
	this := CastExpr(e.This(), DT_TIMESTAMP, true, nil)
	expr := CastExpr(e.Expression(), DT_TIMESTAMP, true, nil)
	unit := unitToStr(e, "DAY")
	return g.fn("DATE_DIFF", unit, expr, this)
}

// prestoFirstLastSQL mirrors generators.presto._first_last_sql.
//
// Trino doesn't support FIRST / LAST as functions, but they're valid in the context
// of MATCH_RECOGNIZE, so we need to preserve them in that case. In all other cases
// they're converted into an ARBITRARY call.
//
// Reference: https://trino.io/docs/current/sql/match-recognize.html#logical-navigation-functions
func prestoFirstLastSQL(g *Generator, e *Expr) string {
	if e.FindAncestor(KMatchRecognize, KSelect).IsA(KMatchRecognize) {
		return g.functionFallbackSQL(e)
	}

	return renameFunc("ARBITRARY")(g, e)
}

// prestoUnixToTimeSQL mirrors generators.presto._unix_to_time_sql.
func prestoUnixToTimeSQL(g *Generator, e *Expr) string {
	scale := e.ArgE("scale")
	timestamp := g.sqlKey(e, "this")
	if scale == nil || scale.Equal(LiteralInt(0)) {
		return renameFunc("FROM_UNIXTIME")(g, e)
	}

	return "FROM_UNIXTIME(CAST(" + timestamp + " AS DOUBLE) / POW(10, " + dhPyStr(scale) + "))"
}

// prestoToInt mirrors generators.presto._to_int.
func prestoToInt(g *Generator, e *Expr) *Expr {
	if e.Type() == nil {
		annotateTypes(e, g.d)
	}
	if t := e.Type(); t != nil && !DataType_INTEGER_TYPES.Has(t.DTypeOf()) {
		return CastExpr(e, DT_BIGINT, true, nil)
	}
	return e
}

// prestoDateDeltaSQL mirrors generators.presto._date_delta_sql(name, negate_interval=False).
func prestoDateDeltaSQL(name string, negateInterval bool) GenFunc {
	return func(g *Generator, e *Expr) string {
		interval := prestoToInt(g, e.Expression())
		if negateInterval {
			interval = dhBinop(KMul, interval, -1)
		}
		return g.fn(name, unitToStr(e, "DAY"), interval, e.Arg("this"))
	}
}

// prestoDateDiffSQL mirrors generators.presto._date_diff_sql.
func prestoDateDiffSQL(g *Generator, e *Expr) string {
	// Presto/Trino only expose date_diff(unit, ts1, ts2); it returns ts2 - ts1, so the
	// operands are emitted as (expression, this) to preserve `this - expression` semantics.
	this := e.This()
	expr := e.Expression()
	unit := unitToStr(e, "DAY")

	// DATE_DIFF counts complete units between its operands, whereas dialects that set
	// date_part_boundary count unit boundary crossings, so the operands are truncated
	// down to the unit to make the two coincide
	if unit != nil && e.ArgB("date_part_boundary") {
		rawUnit := e.ArgE("unit")
		dow, hasDow := weekUnitToDow(rawUnit)

		if hasDow {
			unit = LiteralString("WEEK")

			// DATE_TRUNC('WEEK', ...) is Monday-based; shifting both operands by the same
			// delta realigns it to the requested week start without changing the diff
			shiftDays := 1 - dow
			if dow == 7 {
				shiftDays = 1
			}
			if shiftDays != 0 {
				delta := New(KInterval, "this", LiteralString(fmt.Sprint(shiftDays)), "unit", VarChecked("DAY"))
				this = New(KAdd, "this", this, "expression", delta)
				expr = New(KAdd, "this", expr, "expression", delta.Copy())
			}
		}

		if !rawUnit.IsA(KWeekStart) || hasDow {
			this = New(KDateTrunc, "unit", unit.Copy(), "this", this)
			expr = New(KDateTrunc, "unit", unit.Copy(), "this", expr)
		}
	}

	return g.fn("DATE_DIFF", unit, expr, this)
}

// prestoExplodeToUnnestSQL mirrors generators.presto._explode_to_unnest_sql.
func prestoExplodeToUnnestSQL(g *Generator, e *Expr) string {
	explode := e.This()
	if explode.IsA(KExplode) {
		explodedType := explode.This().Type()
		alias := e.ArgE("alias")

		// This attempts a best-effort transpilation of LATERAL VIEW EXPLODE on a struct array
		if alias.IsA(KTableAlias) &&
			explodedType.IsA(KDataType) &&
			dhIsType(explodedType, DT_ARRAY) &&
			len(explodedType.Expressions()) > 0 &&
			dhIsType(explodedType.Expressions()[0], DT_STRUCT) {
			// When unnesting a ROW in Presto, it produces N columns, so we need to fix the alias
			var cols []*Expr
			for _, c := range explodedType.Expressions()[0].Expressions() {
				cols = append(cols, c.This().Copy())
			}
			if cols == nil {
				cols = []*Expr{}
			}
			alias.Set("columns", cols)
		}
	} else if explode.IsA(KInline) {
		explode.Replace(New(KExplode, "this", explode.This().Copy()))
	}

	return explodeToUnnestSQL(g, e)
}

// prestoAmendExplodedColumnTable mirrors generators.presto.amend_exploded_column_table.
func prestoAmendExplodedColumnTable(expression *Expr) *Expr {
	// We check for expression.type because the columns can be amended only if types were inferred
	if expression.IsA(KSelect) && expression.Type() != nil {
		for _, lateral := range expression.ArgL("laterals") {
			alias := lateral.ArgE("alias")
			if !lateral.This().IsA(KExplode) || !alias.IsA(KTableAlias) || len(alias.ArgL("columns")) != 1 {
				continue
			}

			newTable := alias.This()
			oldTable := pyLower(alias.ArgL("columns")[0].Name())

			// When transpiling a LATERAL VIEW EXPLODE Spark query, the exploded fields may be qualified
			// with the struct column, resulting in invalid Presto references that need to be amended
			for column := range FindAllInScope(expression, KColumn) {
				if pyLower(column.Text("db")) == oldTable {
					column.Set("table", column.ArgE("db").Pop())
				} else if pyLower(column.Text("table")) == oldTable {
					column.Set("table", newTable.Copy())
				} else if pyLower(column.Name()) == oldTable && column.Parent().IsA(KDot) {
					column.Parent().Replace(ColumnExpr(column.Parent().Expression(), newTable, nil, nil, nil, nil, true))
				}
			}
		}
	}

	return expression
}

// ---------------------------------------------------------------------------------------------
// PrestoGenerator
// ---------------------------------------------------------------------------------------------

func customizePrestoGenerator(d *Dialect) {
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (module level)
	for _, k := range []string{"cluster", "distribute", "sort"} {
		delete(G.AFTER_HAVING_MODIFIER_TRANSFORMS, k)
	}
	keys := make([]string, 0, len(G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS))
	for _, k := range G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if k != "cluster" && k != "distribute" && k != "sort" {
			keys = append(keys, k)
		}
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	T := G.TRANSFORMS
	T[KAnyValue] = renameFunc("ARBITRARY")
	T[KApproxQuantile] = func(g *Generator, e *Expr) string {
		return g.fn("APPROX_PERCENTILE", e.Arg("this"), e.Arg("weight"), e.Arg("quantile"), e.Arg("accuracy"))
	}
	T[KArgMax] = renameFunc("MAX_BY")
	T[KArgMin] = renameFunc("MIN_BY")
	T[KArray] = transformPreprocess(
		[]func(*Expr) *Expr{transformInheritStructFieldNames},
		func(g *Generator, e *Expr) string {
			return "ARRAY[" + g.expressions(e, exprsOpts{flat: true}) + "]"
		},
	)
	T[KArrayAny] = renameFunc("ANY_MATCH")
	T[KArrayConcat] = renameFunc("CONCAT")
	T[KArrayContains] = renameFunc("CONTAINS")
	T[KArrayToString] = renameFunc("ARRAY_JOIN")
	T[KArrayUniqueAgg] = renameFunc("SET_AGG")
	T[KArraySlice] = renameFunc("SLICE")
	T[KAtTimeZone] = renameFunc("AT_TIMEZONE")
	T[KBitwiseAnd] = func(g *Generator, e *Expr) string { return g.fn("BITWISE_AND", e.Arg("this"), e.Arg("expression")) }
	T[KBitwiseLeftShift] = renameFunc("BITWISE_LEFT_SHIFT")
	T[KBitwiseNot] = func(g *Generator, e *Expr) string { return g.fn("BITWISE_NOT", e.Arg("this")) }
	T[KBitwiseOr] = func(g *Generator, e *Expr) string { return g.fn("BITWISE_OR", e.Arg("this"), e.Arg("expression")) }
	T[KBitwiseRightShift] = renameFunc("BITWISE_RIGHT_SHIFT")
	T[KBitwiseXor] = func(g *Generator, e *Expr) string { return g.fn("BITWISE_XOR", e.Arg("this"), e.Arg("expression")) }
	T[KCast] = transformPreprocess([]func(*Expr) *Expr{transformEpochCastToTs}, nil)
	T[KCurrentTime] = func(*Generator, *Expr) string { return "CURRENT_TIME" }
	T[KCurrentTimestamp] = func(*Generator, *Expr) string { return "CURRENT_TIMESTAMP" }
	T[KCurrentUser] = func(*Generator, *Expr) string { return "CURRENT_USER" }
	T[KDateAdd] = prestoDateDeltaSQL("DATE_ADD", false)
	T[KDateDiff] = prestoDateDiffSQL
	T[KDatetimeDiff] = prestoDateDiffSQL
	T[KTimestampDiff] = prestoDateDiffSQL
	T[KDateStrToDate] = datestrtodateSQL
	T[KDateToDi] = func(g *Generator, e *Expr) string {
		return "CAST(DATE_FORMAT(" + g.sqlKey(e, "this") + ", " + g.d.S.DATEINT_FORMAT + ") AS INT)"
	}
	T[KDateSub] = prestoDateDeltaSQL("DATE_ADD", true)
	T[KDayOfWeek] = func(g *Generator, e *Expr) string {
		return "((" + g.fn("DAY_OF_WEEK", e.Arg("this")) + " % 7) + 1)"
	}
	T[KDayOfWeekIso] = renameFunc("DAY_OF_WEEK")
	T[KDecode] = func(g *Generator, e *Expr) string { return encodeDecodeSQL(g, e, "FROM_UTF8", true) }
	T[KDiToDate] = func(g *Generator, e *Expr) string {
		return "CAST(DATE_PARSE(CAST(" + g.sqlKey(e, "this") + " AS VARCHAR), " + g.d.S.DATEINT_FORMAT + ") AS DATE)"
	}
	T[KEncode] = func(g *Generator, e *Expr) string { return encodeDecodeSQL(g, e, "TO_UTF8", true) }
	T[KFileFormatProperty] = func(g *Generator, e *Expr) string {
		return "format=" + g.sql(LiteralString(e.Name()))
	}
	T[KFirst] = prestoFirstLastSQL
	T[KFromISO8601Date] = renameFunc("FROM_ISO8601_DATE")
	T[KFromISO8601Timestamp] = renameFunc("FROM_ISO8601_TIMESTAMP")
	T[KFromTimeZone] = func(g *Generator, e *Expr) string {
		return "WITH_TIMEZONE(" + g.sqlKey(e, "this") + ", " + g.sqlKey(e, "zone") + ") AT TIME ZONE 'UTC'"
	}
	T[KGenerateSeries] = sequenceSQL
	T[KGenerateDateArray] = sequenceSQL
	T[KIf] = ifSQL("IF", nil)
	T[KILike] = noIlikeSQL
	T[KInitcap] = prestoInitcapSQL
	T[KLast] = prestoFirstLastSQL
	T[KLastDay] = func(g *Generator, e *Expr) string { return g.fn("LAST_DAY_OF_MONTH", e.Arg("this")) }
	T[KLateral] = prestoExplodeToUnnestSQL
	T[KLeft] = leftToSubstringSQL
	T[KLevenshtein] = func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "ins_cost", "del_cost", "sub_cost", "max_dist")
		return renameFunc("LEVENSHTEIN_DISTANCE")(g, e)
	}
	T[KLogicalAnd] = renameFunc("BOOL_AND")
	T[KLogicalOr] = renameFunc("BOOL_OR")
	T[KPivot] = noPivotSQL
	T[KQuantile] = prestoQuantileSQL
	T[KRegexpExtract] = regexpExtractSQL
	T[KRegexpExtractAll] = regexpExtractSQL
	T[KRight] = rightToSubstringSQL
	T[KSchema] = prestoSchemaSQL
	T[KSchemaCommentProperty] = func(g *Generator, e *Expr) string { return g.nakedProperty(e) }
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateWindowClause,
		transformEliminateQualify,
		transformEliminateDistinctOn,
		transformExplodeProjectionToUnnest(1),
		transformEliminateSemiAndAntiJoins,
		prestoAmendExplodedColumnTable,
	}, nil)
	T[KSortArray] = prestoNoSortArray
	T[KSqlSecurityProperty] = func(g *Generator, e *Expr) string { return "SECURITY " + g.sql(e.Arg("this")) }
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "STRPOS", false, true, true)
	}
	T[KStrToDate] = func(g *Generator, e *Expr) string { return "CAST(" + prestoStrToTimeSQL(g, e) + " AS DATE)" }
	T[KStrToMap] = renameFunc("SPLIT_TO_MAP")
	T[KStrToTime] = prestoStrToTimeSQL
	T[KStructExtract] = structExtractSQL
	T[KTable] = transformPreprocess([]func(*Expr) *Expr{transformUnnestGenerateSeries}, nil)
	T[KTimestamp] = noTimestampSQL
	T[KTimestampAdd] = prestoDateDeltaSQL("DATE_ADD", false)
	T[KTimestampTrunc] = timestamptruncSQL("DATE_TRUNC", false)
	T[KTimeStrToDate] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, false) }
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, false) }
	T[KTimeStrToUnix] = func(g *Generator, e *Expr) string {
		return g.fn("TO_UNIXTIME", g.fn("DATE_PARSE", e.This(), g.d.S.TIME_FORMAT))
	}
	T[KTimeToStr] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_FORMAT", e.Arg("this"), prestoFmtArg(g.formatTime(e, nil, nil)))
	}
	T[KTimeToUnix] = renameFunc("TO_UNIXTIME")
	T[KToChar] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_FORMAT", e.Arg("this"), prestoFmtArg(g.formatTime(e, nil, nil)))
	}
	T[KTryCast] = transformPreprocess([]func(*Expr) *Expr{transformEpochCastToTs}, nil)
	T[KTsOrDiToDi] = func(g *Generator, e *Expr) string {
		return "CAST(SUBSTR(REPLACE(CAST(" + g.sqlKey(e, "this") + " AS VARCHAR), '-', ''), 1, 8) AS INT)"
	}
	T[KTsOrDsAdd] = prestoTsOrDsAddSQL
	T[KTsOrDsDiff] = prestoTsOrDsDiffSQL
	T[KTsOrDsToDate] = prestoTsOrDsToDateSQL
	T[KUnhex] = renameFunc("FROM_HEX")
	T[KUnixToStr] = func(g *Generator, e *Expr) string {
		return "DATE_FORMAT(FROM_UNIXTIME(" + g.sqlKey(e, "this") + "), " + prestoPyFmt(g.formatTime(e, nil, nil)) + ")"
	}
	T[KUnixToTime] = prestoUnixToTimeSQL
	T[KUnixToTimeStr] = func(g *Generator, e *Expr) string {
		return "CAST(FROM_UNIXTIME(" + g.sqlKey(e, "this") + ") AS VARCHAR)"
	}
	T[KVariancePop] = renameFunc("VAR_POP")
	T[KWith] = transformPreprocess([]func(*Expr) *Expr{transformAddRecursiveCteColumnNames}, nil)
	T[KWithinGroup] = transformPreprocess([]func(*Expr) *Expr{transformRemoveWithinGroupForPercentiles}, nil)
	// Note: Presto's TRUNCATE always returns DOUBLE, even with decimals=0, whereas
	// most dialects return INT (SQLite also returns REAL, see sqlite.py). This creates
	// a bidirectional transpilation gap: Presto→Other may change float division to int
	// division, and vice versa. Modeling precisely would require exp.FloatTrunc or
	// similar, deemed overengineering for this subtle semantic difference.
	T[KTrunc] = renameFunc("TRUNCATE")
	T[KXor] = boolXorSQL
	T[KMD5Digest] = renameFunc("MD5")
	T[KSHA] = renameFunc("SHA1")
	T[KSHA1Digest] = renameFunc("SHA1")
	T[KSHA2Digest] = prestoSha2DigestSQL
	T[KSubstring] = renameFunc("SUBSTR")

	// methods
	G.h.extractSQL = prestoExtractSQL
	G.methods[KJSONFormat] = prestoJSONFormatSQL
	G.methods[KMD5] = prestoMD5SQL
	G.methods[KSHA2] = prestoSHA2SQL
	G.methods[KStrToUnix] = prestoStrToUnixSQL
	G.h.bracketSQL = prestoBracketSQL
	G.h.structSQL = prestoStructSQL
	G.h.intervalSQL = prestoIntervalSQL
	G.h.transactionSQL = prestoTransactionSQL
	G.h.offsetLimitModifiers = prestoOffsetLimitModifiers
	G.h.createSQL = prestoCreateSQL
	G.h.deleteSQL = prestoDeleteSQL
	G.methods[KJSONExtract] = prestoJSONExtractSQL
	G.methods[KGroupConcat] = prestoGroupConcatSQL
}

// prestoExtractSQL mirrors PrestoGenerator.extract_sql.
func prestoExtractSQL(g *Generator, e *Expr) string {
	datePart := e.Name()

	if !strings.HasPrefix(datePart, "EPOCH") {
		return g.baseExtractSQL(e)
	}

	scale := 0
	switch datePart {
	case "EPOCH_MILLISECOND":
		scale = 1000
	case "EPOCH_MICROSECOND":
		scale = 1000000
	case "EPOCH_NANOSECOND":
		scale = 1000000000
	}

	value := e.Expression()

	ts := CastExpr(value, NewDataType(DT_TIMESTAMP), true, nil)
	toUnix := New(KTimeToUnix, "this", ts)

	if scale != 0 {
		toUnix = New(KMul, "this", toUnix, "expression", LiteralInt(scale))
	}

	return g.sql(toUnix)
}

// prestoJSONFormatSQL mirrors PrestoGenerator.jsonformat_sql.
func prestoJSONFormatSQL(g *Generator, e *Expr) string {
	this := e.This()
	isJSON := e.ArgB("is_json")

	if this != nil && !(isJSON || this.Type() != nil) {
		this = annotateTypes(this, g.d)
	}

	if !(isJSON || dhIsType(this, DT_JSON)) {
		this.Replace(CastExpr(this, DT_JSON, true, nil))
	}

	return g.functionFallbackSQL(e)
}

// prestoMD5SQL mirrors PrestoGenerator.md5_sql.
func prestoMD5SQL(g *Generator, e *Expr) string {
	this := e.This()

	if this.Type() == nil {
		this = annotateTypes(this, g.d)
	}

	if dhIsType(this, DataType_TEXT_TYPES.Items()...) {
		this = New(KEncode, "this", this, "charset", LiteralString("utf-8"))
	}

	return g.fn("LOWER", g.fn("TO_HEX", g.fn("MD5", g.sql(this))))
}

// prestoSHA2SQL mirrors PrestoGenerator.sha2_sql.
func prestoSHA2SQL(g *Generator, e *Expr) string {
	length := e.Text("length")
	if length == "" {
		length = "256"
	}
	if length != "256" && length != "512" {
		g.unsupported("SHA" + length + " is not supported in Presto")
	}

	this := e.This()

	if dhIsType(this, DataType_TEXT_TYPES.Items()...) {
		this = New(KEncode, "this", this, "charset", LiteralString("utf-8"))
	}

	return g.fn("LOWER", g.fn("TO_HEX", g.fn("SHA"+length, g.sql(this))))
}

// prestoStrToUnixSQL mirrors PrestoGenerator.strtounix_sql.
func prestoStrToUnixSQL(g *Generator, e *Expr) string {
	// Since `TO_UNIXTIME` requires a `TIMESTAMP`, we need to parse the argument into one.
	// To do this, we first try to `DATE_PARSE` it, but since this can fail when there's a
	// timezone involved, we wrap it in a `TRY` call and use `PARSE_DATETIME` as a fallback,
	// which seems to be using the same time mapping as Hive, as per:
	// https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html
	this := e.This()
	valueAsText := CastExpr(this, DT_TEXT, true, nil)
	valueAsTimestamp := this
	if this.IsString() {
		valueAsTimestamp = CastExpr(this, DT_TIMESTAMP, true, nil)
	}

	parseWithoutTz := g.fn("DATE_PARSE", valueAsText, prestoFmtArg(g.formatTime(e, nil, nil)))

	formattedValue := g.fn("DATE_FORMAT", valueAsTimestamp, prestoFmtArg(g.formatTime(e, nil, nil)))
	hive := prototype("hive")
	parseWithTz := g.fn(
		"PARSE_DATETIME",
		formattedValue,
		prestoFmtArg(g.formatTime(e, hive.S.INVERSE_TIME_MAPPING, hive.inverseTimeTrie)),
	)
	coalesced := g.fn("COALESCE", g.fn("TRY", parseWithoutTz), parseWithTz)
	return g.fn("TO_UNIXTIME", coalesced)
}

// prestoBracketSQL mirrors PrestoGenerator.bracket_sql.
func prestoBracketSQL(g *Generator, e *Expr) string {
	if e.ArgB("safe") {
		return bracketToElementAtSQL(g, e)
	}
	return g.baseBracketSQL(e)
}

// prestoStructSQL mirrors PrestoGenerator.struct_sql.
func prestoStructSQL(g *Generator, e *Expr) string {
	if e.Type() == nil {
		annotateTypes(e, g.d)
	}

	var values, schema []any
	unknownType := false

	for _, x := range e.Expressions() {
		if x.IsA(KPropertyEQ) {
			if t := x.Type(); t != nil && dhIsType(t, DT_UNKNOWN) {
				unknownType = true
			} else {
				schema = append(schema, g.sqlKey(x, "this")+" "+g.sql(x.Type()))
			}
			values = append(values, g.sqlKey(x, "expression"))
		} else {
			values = append(values, g.sql(x))
		}
	}

	size := len(e.Expressions())

	if size == 0 || len(schema) != size {
		if unknownType {
			g.unsupported("Cannot convert untyped key-value definitions (try annotate_types).")
		}
		return g.fn("ROW", values...)
	}
	vs := make([]string, len(values))
	for i, v := range values {
		vs[i] = v.(string)
	}
	ss := make([]string, len(schema))
	for i, v := range schema {
		ss[i] = v.(string)
	}
	return "CAST(ROW(" + strings.Join(vs, ", ") + ") AS ROW(" + strings.Join(ss, ", ") + "))"
}

// prestoIntervalSQL mirrors PrestoGenerator.interval_sql.
func prestoIntervalSQL(g *Generator, e *Expr) string {
	if e.This() != nil && strings.HasPrefix(pyUpper(e.Text("unit")), "WEEK") {
		return "(" + e.This().Name() + " * INTERVAL '7' DAY)"
	}
	return g.baseIntervalSQL(e)
}

// prestoTransactionSQL mirrors PrestoGenerator.transaction_sql.
func prestoTransactionSQL(g *Generator, e *Expr) string {
	modes := ""
	switch m := e.Arg("modes").(type) {
	case []string:
		if len(m) > 0 {
			modes = " " + strings.Join(m, ", ")
		}
	case []any:
		if len(m) > 0 {
			parts := make([]string, len(m))
			for i, x := range m {
				parts[i] = dhPyStr(x)
			}
			modes = " " + strings.Join(parts, ", ")
		}
	}
	return "START TRANSACTION" + modes
}

// prestoOffsetLimitModifiers mirrors PrestoGenerator.offset_limit_modifiers.
func prestoOffsetLimitModifiers(g *Generator, e *Expr, fetch bool, limit *Expr) []string {
	return []string{
		g.sqlKey(e, "offset"),
		g.sql(limit),
	}
}

// prestoCreateSQL mirrors PrestoGenerator.create_sql.
//
// Presto doesn't support CREATE VIEW with expressions (ex: `CREATE VIEW x (cola)` then `(cola)` is the expression),
// so we need to remove them.
func prestoCreateSQL(g *Generator, e *Expr) string {
	kind := e.Arg("kind")
	schema := e.This()
	if kind == "VIEW" && len(schema.Expressions()) > 0 {
		e.This().Set("expressions", nil)
	}
	return g.baseCreateSQL(e)
}

// prestoDeleteSQL mirrors PrestoGenerator.delete_sql.
//
// Presto only supports DELETE FROM for a single table without an alias, so we need
// to remove the unnecessary parts. If the original DELETE statement contains more
// than one table to be deleted, we can't safely map it 1-1 to a Presto statement.
func prestoDeleteSQL(g *Generator, e *Expr) string {
	tables := e.ArgL("tables")
	if len(tables) == 0 {
		tables = []*Expr{e.This()}
	}
	if len(tables) > 1 {
		return g.baseDeleteSQL(e)
	}

	table := tables[0]
	e.Set("this", table)
	e.Set("tables", nil)

	if table.IsA(KTable) {
		if tableAlias := table.argForAttr("alias", "pop"); tableAlias != nil {
			tableAlias.Pop()
			e = e.Transform(transformUnqualifyColumns, true)
		}
	}

	return g.baseDeleteSQL(e)
}

// prestoJSONExtractSQL mirrors PrestoGenerator.jsonextract_sql.
func prestoJSONExtractSQL(g *Generator, e *Expr) string {
	isJSONExtract := true
	if v, ok := g.d.Settings["variant_extract_is_json_extract"]; ok {
		isJSONExtract = truthy(v)
	}

	// Generate JSON_EXTRACT unless the user has configured that a Snowflake / Databricks
	// VARIANT extract (e.g. col:x.y) should map to dot notation (i.e ROW access) in Presto/Trino
	if !e.ArgB("variant_extract") || isJSONExtract {
		args := []any{e.This(), e.Expression()}
		for _, x := range e.Expressions() {
			args = append(args, x)
		}
		return g.fn("JSON_EXTRACT", args...)
	}

	this := g.sqlKey(e, "this")

	// Convert the JSONPath extraction `JSON_EXTRACT(col, '$.x.y) to a ROW access col.x.y
	var segments []string
	pathKeys := e.Expression().Expressions()
	if len(pathKeys) > 0 {
		pathKeys = pathKeys[1:]
	}
	for _, pathKey := range pathKeys {
		if !pathKey.IsA(KJSONPathKey) {
			// Cannot transpile subscripts, wildcards etc to dot notation
			g.unsupported("Cannot transpile JSONPath segment '" + dhPyStr(pathKey) + "' to ROW access")
			continue
		}
		key := pathKey.ThisS()
		if !isSafeIdentifier(key) {
			key = `"` + key + `"`
		}
		segments = append(segments, "."+key)
	}

	expr := strings.Join(segments, "")

	return this + expr
}

// prestoGroupConcatSQL mirrors PrestoGenerator.groupconcat_sql.
func prestoGroupConcatSQL(g *Generator, e *Expr) string {
	return g.fn(
		"ARRAY_JOIN",
		g.fn("ARRAY_AGG", e.This()),
		e.Arg("separator"),
	)
}
