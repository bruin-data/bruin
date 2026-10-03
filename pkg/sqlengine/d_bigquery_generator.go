package sqlengine

import (
	"fmt"
)

// Port of sqlglot/generators/bigquery.py (module-level helpers and BigQueryGenerator).

var bigqueryDquotesEscapingJSONFunctions = []string{"JSON_QUERY", "JSON_VALUE", "JSON_QUERY_ARRAY"}

// _derived_table_values_to_unnest.
func bigqueryDerivedTableValuesToUnnest(g *Generator, expression *Expr) string {
	if expression.FindAncestor(KFrom, KJoin) == nil {
		return g.valuesSQL(expression, true)
	}

	var structs []*Expr
	alias := expression.ArgE("alias")
	for tup := range expression.FindAll(KTuple) {
		var fieldAliases []any
		if alias != nil && len(alias.TableAliasColumns()) > 0 {
			for _, c := range alias.TableAliasColumns() {
				fieldAliases = append(fieldAliases, c)
			}
		} else {
			for i := range tup.Expressions() {
				fieldAliases = append(fieldAliases, fmt.Sprintf("_c%d", i))
			}
		}
		fields := tup.Expressions()
		n := min(len(fieldAliases), len(fields))
		expressions := make([]*Expr, 0, n)
		for i := range n {
			expressions = append(expressions, New(
				KPropertyEQ,
				"this", ToIdentifierAny(fieldAliases[i], nil, true),
				"expression", fields[i],
			))
		}
		structs = append(structs, New(KStruct, "expressions", expressions))
	}

	// Due to `UNNEST_COLUMN_ONLY`, it is expected that the table alias be contained in the columns expression
	var aliasNameOnly *Expr
	if alias != nil {
		aliasNameOnly = New(KTableAlias, "columns", []*Expr{alias.This()})
	}
	return g.unnestSQL(
		New(KUnnest, "expressions", []*Expr{ArrayExpr(structs, false)}, "alias", aliasNameOnly),
	)
}

// _returnsproperty_sql.
func bigqueryReturnspropertySQL(g *Generator, expression *Expr) string {
	this := expression.This()
	var thisSQL string
	if this.IsA(KSchema) {
		thisSQL = g.sqlKey(this, "this") + " <" + g.expressions(this, exprsOpts{}) + ">"
	} else {
		thisSQL = g.sql(this)
	}
	return "RETURNS " + thisSQL
}

// _create_sql.
func bigqueryCreateSQL(g *Generator, expression *Expr) string {
	returns := expression.Find(KReturnsProperty)
	if expression.KindText() == "FUNCTION" && returns != nil && returns.ArgB("is_table") {
		expression.Set("kind", "TABLE FUNCTION")

		if expression.Expression().IsA(KSubquery, KLiteral) {
			expression.Set("expression", expression.Expression().Arg("this"))
		}
	}

	return g.createSQL(expression)
}

// _alias_ordered_group
//
// https://issuetracker.google.com/issues/162294746
// workaround for bigquery bug when grouping by an expression and then ordering
// WITH x AS (SELECT 1 y)
// SELECT y + 1 z
// FROM x
// GROUP BY x + 1
// ORDER by z.
func bigqueryAliasOrderedGroup(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		group := expression.ArgE("group")
		order := expression.ArgE("order")

		if group != nil && order != nil {
			type aliasEntry struct{ key, alias *Expr }
			var aliases []aliasEntry
			for _, sel := range expression.Selects() {
				if sel.IsA(KAlias) {
					// dict semantics: a later equal key overwrites the value in place
					replaced := false
					for i := range aliases {
						if aliases[i].key.Equal(sel.This()) {
							aliases[i].alias = sel.ArgE("alias")
							replaced = true
							break
						}
					}
					if !replaced {
						aliases = append(aliases, aliasEntry{sel.This(), sel.ArgE("alias")})
					}
				}
			}

			for _, grouped := range group.Expressions() {
				if grouped.IsInt() {
					continue
				}
				var alias *Expr
				for _, a := range aliases {
					if a.key.Equal(grouped) {
						alias = a.alias
						break
					}
				}
				if alias != nil {
					grouped.Replace(ColumnExpr(alias, nil, nil, nil, nil, nil, true))
				}
			}
		}
	}

	return expression
}

// _pushdown_cte_column_names: BigQuery doesn't allow column names when defining a CTE, so we try
// to push them down.
func bigqueryPushdownCTEColumnNames(expression *Expr) *Expr {
	if expression.IsA(KCTE) && len(expression.AliasColumnNames()) > 0 {
		cteQuery := expression.This()

		if cteQuery.IsStar() {
			// logger.warning("Can't push down CTE column names for star queries. ...")
			return expression
		}

		columnNames := expression.AliasColumnNames()
		expression.ArgE("alias").Set("columns", nil)

		selects := cteQuery.Selects()
		n := min(len(columnNames), len(selects))
		for i := range n {
			name := columnNames[i]
			sel := selects[i]
			toReplace := sel

			if sel.IsA(KAlias) {
				sel = sel.This()
			}

			// Inner aliases are shadowed by the CTE column names
			toReplace.Replace(AliasExpr(sel, name, nil, true))
		}
	}

	return expression
}

// _unnest_explode_generate_series
//
// Rewrites exploding GENERATE_SERIES projections into table references, e.g.
//
//	SELECT GENERATE_SERIES(1, 2) AS x           -> SELECT x FROM GENERATE_SERIES(1, 2) AS x
//	SELECT y, GENERATE_SERIES(1, 2) AS x FROM t -> SELECT y, x FROM t CROSS JOIN GENERATE_SERIES(1, 2) AS x
//
// since BigQuery can't explode in the projection and must unnest it in the FROM clause instead.
// The resulting table reference is unnested downstream by `transforms.unnest_generate_series`.
func bigqueryUnnestExplodeGenerateSeries(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		for _, projection := range expression.Selects() {
			series := projection.Unalias()
			if series.IsA(KExplodingGenerateSeries) {
				columnName := projection.OutputName()
				if columnName == "" {
					columnName = "_gen_series_value"
				}

				projection.Replace(ColumnExpr(columnName, nil, nil, nil, nil, nil, true))
				table := New(
					KTable,
					"this", series,
					"alias", New(KTableAlias, "this", ToIdentifier(columnName, nil)),
				)

				if expression.ArgB("from_") {
					expression.SelectJoin(table, nil, nil, true, "CROSS", nil, false)
				} else {
					expression.Set("from_", New(KFrom, "this", table))
				}
			}
		}
	}

	return expression
}

// _array_contains_sql.
func bigqueryArrayContainsSQL(g *Generator, expression *Expr) string {
	unnest := New(KUnnest, "expressions", []*Expr{expression.Left()})
	unnest = AliasTableExpr(unnest, "_unnest", []any{"_col"}, nil, true)
	query := SelectExpr(LiteralNumber("1"))
	query = query.SelectFrom(unnest, true)
	query = query.QueryWhere([]*Expr{dhBinop(KEQ, ColumnExpr("_col", nil, nil, nil, nil, nil, true), expression.Right())}, true, true)
	return g.sql(New(KExists, "this", query))
}

// _ts_or_ds_add_sql.
func bigqueryTsOrDsAddSQL(g *Generator, expression *Expr) string {
	return dateAddIntervalSQL("DATE", "ADD")(g, tsOrDsAddCast(expression))
}

// _ts_or_ds_diff_sql.
func bigqueryTsOrDsDiffSQL(g *Generator, expression *Expr) string {
	expression.This().Replace(CastExpr(expression.This(), DT_TIMESTAMP, true, nil))
	expression.Expression().Replace(CastExpr(expression.Expression(), DT_TIMESTAMP, true, nil))
	unit := unitToVar(expression, "DAY")
	return g.fn("DATE_DIFF", expression.Arg("this"), expression.Arg("expression"), unit)
}

// _unix_to_time_sql.
func bigqueryUnixToTimeSQL(g *Generator, expression *Expr) string {
	scale := expression.ArgE("scale")
	timestamp := expression.This()

	if scale == nil || scale.Equal(LiteralInt(0)) { // UnixToTime.SECONDS
		return g.fn("TIMESTAMP_SECONDS", timestamp)
	}
	if scale.Equal(LiteralInt(3)) { // UnixToTime.MILLIS
		return g.fn("TIMESTAMP_MILLIS", timestamp)
	}
	if scale.Equal(LiteralInt(6)) { // UnixToTime.MICROS
		return g.fn("TIMESTAMP_MICROS", timestamp)
	}

	unixSeconds := CastExpr(
		New(KDiv, "this", timestamp, "expression", dhFunc("POW", 10, scale)), DT_BIGINT, true, nil,
	)
	return g.fn("TIMESTAMP_SECONDS", unixSeconds)
}

// bigqueryFmtArg turns the result of Generator.format_time into a func argument ("" is None).
func bigqueryFmtArg(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// _str_to_datetime_sql.
func bigqueryStrToDatetimeSQL(g *Generator, expression *Expr) string {
	this := g.sqlKey(expression, "this")
	dtype := "TIMESTAMP"
	if expression.IsA(KStrToDate) {
		dtype = "DATE"
	}

	if expression.ArgB("safe") {
		fmtStr := g.formatTime(expression, g.d.S.INVERSE_FORMAT_MAPPING, g.d.inverseFormatTrie)
		if fmtStr == "" {
			fmtStr = "None" // f-string of None
		}
		return "SAFE_CAST(" + this + " AS " + dtype + " FORMAT " + fmtStr + ")"
	}

	fmtStr := g.formatTime(expression, nil, nil)
	return g.fn("PARSE_"+dtype, bigqueryFmtArg(fmtStr), this, expression.Arg("zone"))
}

// _levenshtein_sql.
func bigqueryLevenshteinSQL(g *Generator, expression *Expr) string {
	dhUnsupportedArgs(g, expression, "ins_cost", "del_cost", "sub_cost")
	var maxDist any = expression.Arg("max_dist")
	if truthy(maxDist) {
		maxDist = New(KKwarg, "this", VarExpr("max_distance"), "expression", maxDist)
	}

	return g.fn("EDIT_DISTANCE", expression.Arg("this"), expression.Arg("expression"), maxDist)
}

// _json_extract_sql.
func bigqueryJSONExtractSQL(g *Generator, expression *Expr) string {
	name, _ := expression.MetaGet("name").(string)
	if name == "" {
		name = expression.Kind().SQLName()
	}
	upper := pyUpper(name)

	dquoteEscaping := false
	for _, f := range bigqueryDquotesEscapingJSONFunctions {
		if upper == f {
			dquoteEscaping = true
			break
		}
	}

	if dquoteEscaping {
		g.quoteJSONPathKeyUsingBrackets = false
	}

	sql := renameFunc(upper)(g, expression)

	if dquoteEscaping {
		g.quoteJSONPathKeyUsingBrackets = true
	}

	return sql
}

func customizeBigQueryGenerator(d *Dialect) {
	G := d.G
	T := G.TRANSFORMS

	T[KAIEmbed] = renameFunc("EMBED")
	T[KAIGenerate] = renameFunc("GENERATE")
	T[KAISimilarity] = renameFunc("SIMILARITY")
	T[KApproxTopK] = renameFunc("APPROX_TOP_COUNT")
	T[KApproxDistinct] = renameFunc("APPROX_COUNT_DISTINCT")
	T[KArgMax] = argMaxOrMinNoCount("MAX_BY")
	T[KArgMin] = argMaxOrMinNoCount("MIN_BY")
	T[KArray] = inlineArrayUnlessQuery
	T[KArrayContains] = bigqueryArrayContainsSQL
	T[KArrayFilter] = filterArrayUsingUnnest
	T[KArrayRemove] = filterArrayUsingUnnest
	T[KBitwiseAndAgg] = renameFunc("BIT_AND")
	T[KBitwiseOrAgg] = renameFunc("BIT_OR")
	T[KBitwiseXorAgg] = renameFunc("BIT_XOR")
	T[KBitwiseCount] = renameFunc("BIT_COUNT")
	T[KByteLength] = renameFunc("BYTE_LENGTH")
	T[KCast] = transformPreprocess([]func(*Expr) *Expr{transformRemovePrecisionParameterizedTypes}, nil)
	T[KCollateProperty] = func(g *Generator, e *Expr) string {
		if e.ArgB("default") {
			return "DEFAULT COLLATE " + g.sqlKey(e, "this")
		}
		return "COLLATE " + g.sqlKey(e, "this")
	}
	T[KCommit] = func(g *Generator, e *Expr) string { return "COMMIT TRANSACTION" }
	T[KCountIf] = renameFunc("COUNTIF")
	T[KCreate] = bigqueryCreateSQL
	T[KCTE] = transformPreprocess([]func(*Expr) *Expr{bigqueryPushdownCTEColumnNames}, nil)
	T[KDateAdd] = dateAddIntervalSQL("DATE", "ADD")
	T[KDateDiff] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_DIFF", e.Arg("this"), e.Arg("expression"), unitToVar(e, "DAY"))
	}
	T[KDateFromParts] = renameFunc("DATE")
	T[KDateStrToDate] = datestrtodateSQL
	T[KDateSub] = dateAddIntervalSQL("DATE", "SUB")
	T[KDatetimeAdd] = dateAddIntervalSQL("DATETIME", "ADD")
	T[KDatetimeSub] = dateAddIntervalSQL("DATETIME", "SUB")
	T[KDateFromUnixDate] = renameFunc("DATE_FROM_UNIX_DATE")
	T[KFromTimeZone] = func(g *Generator, e *Expr) string {
		return g.fn("DATETIME", g.fn("TIMESTAMP", e.This(), e.Arg("zone")), "'UTC'")
	}
	T[KGenerateSeries] = generateSeriesSQL("GENERATE_ARRAY", "")
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		return groupconcatSQL(g, e, "STRING_AGG", "", false, false)
	}
	T[KHex] = func(g *Generator, e *Expr) string {
		return g.fn("UPPER", g.fn("TO_HEX", g.sqlKey(e, "this")))
	}
	T[KHexString] = func(g *Generator, e *Expr) string { return g.hexstringSQL(e, "FROM_HEX") }
	T[KIf] = ifSQL("IF", "NULL")
	T[KILike] = noIlikeSQL
	T[KIntDiv] = renameFunc("DIV")
	T[KInt64] = renameFunc("INT64")
	T[KJSONBool] = renameFunc("BOOL")
	T[KJSONExtract] = bigqueryJSONExtractSQL
	T[KJSONExtractArray] = bigqueryJSONExtractSQL
	T[KJSONExtractScalar] = bigqueryJSONExtractSQL
	T[KJSONFormat] = func(g *Generator, e *Expr) string {
		name := "TO_JSON_STRING"
		if e.ArgB("to_json") {
			name = "TO_JSON"
		}
		return g.fn(name, e.Arg("this"), e.Arg("options"))
	}
	T[KJSONKeysAtDepth] = renameFunc("JSON_KEYS")
	T[KJSONValueArray] = renameFunc("JSON_VALUE_ARRAY")
	T[KLevenshtein] = bigqueryLevenshteinSQL
	T[KMax] = maxOrGreatest
	T[KMD5] = func(g *Generator, e *Expr) string { return g.fn("TO_HEX", g.fn("MD5", e.This())) }
	T[KMD5Digest] = renameFunc("MD5")
	T[KMin] = minOrLeast
	T[KNormalize] = func(g *Generator, e *Expr) string {
		name := "NORMALIZE"
		if e.ArgB("is_casefold") {
			name = "NORMALIZE_AND_CASEFOLD"
		}
		return g.fn(name, e.Arg("this"), e.Arg("form"))
	}
	T[KPartitionedByProperty] = func(g *Generator, e *Expr) string { return "PARTITION BY " + g.sqlKey(e, "this") }
	T[KRegexpExtract] = func(g *Generator, e *Expr) string {
		return g.fn("REGEXP_EXTRACT", e.Arg("this"), e.Arg("expression"), e.Arg("position"), e.Arg("occurrence"))
	}
	T[KRegexpExtractAll] = func(g *Generator, e *Expr) string {
		return g.fn("REGEXP_EXTRACT_ALL", e.Arg("this"), e.Arg("expression"))
	}
	T[KRegexpReplace] = regexpReplaceSQL
	T[KRegexpLike] = renameFunc("REGEXP_CONTAINS")
	T[KReturnsProperty] = bigqueryReturnspropertySQL
	T[KRollback] = func(g *Generator, e *Expr) string { return "ROLLBACK TRANSACTION" }
	T[KParseTime] = func(g *Generator, e *Expr) string {
		return g.fn("PARSE_TIME", bigqueryFmtArg(g.formatTime(e, nil, nil)), e.Arg("this"))
	}
	T[KParseDatetime] = func(g *Generator, e *Expr) string {
		return g.fn("PARSE_DATETIME", bigqueryFmtArg(g.formatTime(e, nil, nil)), e.Arg("this"))
	}
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		bigqueryUnnestExplodeGenerateSeries,
		transformExplodeProjectionToUnnest(0),
		transformUnqualifyUnnest,
		transformEliminateDistinctOn,
		bigqueryAliasOrderedGroup,
		transformEliminateSemiAndAntiJoins,
	}, nil)
	T[KSHA] = renameFunc("SHA1")
	T[KSHA2] = sha256SQL
	T[KSHA1Digest] = renameFunc("SHA1")
	T[KSHA2Digest] = sha2DigestSQL
	T[KStabilityProperty] = func(g *Generator, e *Expr) string {
		if e.Name() == "IMMUTABLE" {
			return "DETERMINISTIC"
		}
		return "NOT DETERMINISTIC"
	}
	T[KString] = renameFunc("STRING")
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "INSTR", true, true, true)
	}
	T[KStrToDate] = bigqueryStrToDatetimeSQL
	T[KStrToTime] = bigqueryStrToDatetimeSQL
	T[KSessionUser] = func(g *Generator, e *Expr) string { return "SESSION_USER()" }
	T[KTable] = transformPreprocess([]func(*Expr) *Expr{transformUnnestGenerateSeries}, nil)
	T[KTimeAdd] = dateAddIntervalSQL("TIME", "ADD")
	T[KTimeFromParts] = renameFunc("TIME")
	T[KTimestampFromParts] = renameFunc("DATETIME")
	T[KTimeSub] = dateAddIntervalSQL("TIME", "SUB")
	T[KTimestampAdd] = dateAddIntervalSQL("TIMESTAMP", "ADD")
	T[KTimestampDiff] = renameFunc("TIMESTAMP_DIFF")
	T[KTimestampSub] = dateAddIntervalSQL("TIMESTAMP", "SUB")
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, false) }
	T[KTransaction] = func(g *Generator, e *Expr) string { return "BEGIN TRANSACTION" }
	T[KTsOrDsAdd] = bigqueryTsOrDsAddSQL
	T[KTsOrDsDiff] = bigqueryTsOrDsDiffSQL
	T[KTsOrDsToTime] = renameFunc("TIME")
	T[KTsOrDsToDatetime] = renameFunc("DATETIME")
	T[KTsOrDsToTimestamp] = renameFunc("TIMESTAMP")
	T[KUnhex] = renameFunc("FROM_HEX")
	T[KUnixDate] = renameFunc("UNIX_DATE")
	T[KUnixToTime] = bigqueryUnixToTimeSQL
	T[KUuid] = func(g *Generator, e *Expr) string { return "GENERATE_UUID()" }
	T[KValues] = bigqueryDerivedTableValuesToUnnest
	T[KVariancePop] = renameFunc("VAR_POP")
	T[KSafeDivide] = renameFunc("SAFE_DIVIDE")

	// WINDOW comes after QUALIFY
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#window_clause
	// BigQuery requires QUALIFY before WINDOW
	ahm := map[string]GenFunc{
		"qualify": G.AFTER_HAVING_MODIFIER_TRANSFORMS["qualify"],
		"windows": G.AFTER_HAVING_MODIFIER_TRANSFORMS["windows"],
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS = ahm
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = []string{"qualify", "windows"}

	// Dialect-only <key>_sql methods
	G.methods[KDateTrunc] = bigqueryDatetruncSQL
	G.methods[KTimeToStr] = bigqueryTimetostrSQL
	G.methods[KContains] = bigqueryContainsSQL

	// Method overrides
	G.h.modSQL = bigqueryModSQL
	G.h.columnParts = bigqueryColumnParts
	G.h.tableParts = bigqueryTableParts
	G.h.eqSQL = bigqueryEqSQL
	G.h.attimezoneSQL = bigqueryAttimezoneSQL
	G.h.trycastSQL = bigqueryTrycastSQL
	G.h.bracketSQL = bigqueryBracketSQL
	G.h.inUnnestOp = bigqueryInUnnestOp
	G.h.versionSQL = bigqueryVersionSQL
	G.h.castSQL = bigqueryCastSQL
	G.h.clusterpropertySQL = bigqueryClusterpropertySQL
}

// datetrunc_sql.
func bigqueryDatetruncSQL(g *Generator, expression *Expr) string {
	unit := expression.ArgE("unit")
	var unitSQL string
	if unit.IsString() {
		unitSQL = unit.Name()
	} else {
		unitSQL = g.sql(unit)
	}
	return g.fn("DATE_TRUNC", expression.Arg("this"), unitSQL, expression.Arg("zone"))
}

// mod_sql.
func bigqueryModSQL(g *Generator, expression *Expr) string {
	this := expression.This()
	expr := expression.Expression()
	if this.IsA(KParen) {
		this = this.Unnest()
	}
	if expr.IsA(KParen) {
		expr = expr.Unnest()
	}
	return g.fn("MOD", this, expr)
}

// column_parts.
func bigqueryColumnParts(g *Generator, expression *Expr) string {
	if truthy(expression.MetaGet("quoted_column")) {
		// If a column reference is of the form `dataset.table`.name, we need
		// to preserve the quoted table path, otherwise the reference breaks
		parts := expression.Parts()
		if len(parts) > 0 {
			parts = parts[:len(parts)-1]
		}
		tableParts := bigqueryJoinPartNames(parts)
		tablePath := g.sql(New(KIdentifier, "this", tableParts, "quoted", true))
		return tablePath + "." + g.sqlKey(expression, "this")
	}

	return g.baseColumnParts(expression)
}

// table_parts.
func bigqueryTableParts(g *Generator, expression *Expr) string {
	// Depending on the context, `x.y` may not resolve to the same data source as `x`.`y`, so
	// we need to make sure the correct quoting is used in each case.
	//
	// For example, if there is a CTE x that clashes with a schema name, then the former will
	// return the table y in that schema, whereas the latter will return the CTE's y column:
	//
	// - WITH x AS (SELECT [1, 2] AS y) SELECT * FROM x, `x.y`   -> cross join
	// - WITH x AS (SELECT [1, 2] AS y) SELECT * FROM x, `x`.`y` -> implicit unnest
	if truthy(expression.MetaGet("quoted_table")) {
		tableParts := bigqueryJoinPartNames(expression.Parts())
		return g.sql(New(KIdentifier, "this", tableParts, "quoted", true))
	}

	return g.baseTableParts(expression)
}

// timetostr_sql.
func bigqueryTimetostrSQL(g *Generator, expression *Expr) string {
	this := expression.This()
	var funcName string
	if this.IsA(KTsOrDsToDatetime) {
		funcName = "FORMAT_DATETIME"
	} else if this.IsA(KTsOrDsToTimestamp) {
		funcName = "FORMAT_TIMESTAMP"
	} else if this.IsA(KTsOrDsToTime) {
		funcName = "FORMAT_TIME"
	} else {
		funcName = "FORMAT_DATE"
	}

	timeExpr := expression
	if this.IsA(g.s.TS_OR_DS_TYPES...) {
		timeExpr = this
	}
	return g.fn(
		funcName, bigqueryFmtArg(g.formatTime(expression, nil, nil)), timeExpr.Arg("this"), expression.Arg("zone"),
	)
}

// eq_sql.
func bigqueryEqSQL(g *Generator, expression *Expr) string {
	// Operands of = cannot be NULL in BigQuery
	if expression.Left().IsA(KNull) || expression.Right().IsA(KNull) {
		if !expression.Parent().IsA(KUpdate) {
			return "NULL"
		}
	}

	return g.binary(expression, "=")
}

// attimezone_sql.
func bigqueryAttimezoneSQL(g *Generator, expression *Expr) string {
	parent := expression.Parent()

	// BigQuery allows CAST(.. AS {STRING|TIMESTAMP} [FORMAT <fmt> [AT TIME ZONE <tz>]]).
	// Only the TIMESTAMP one should use the below conversion, when AT TIME ZONE is included.
	if !parent.IsA(KCast) || !DataTypeIsType(parent.ArgE("to"), []any{"text"}, false) {
		return g.fn(
			"TIMESTAMP", g.fn("DATETIME", expression.This(), expression.Arg("zone")),
		)
	}

	return g.baseAttimezoneSQL(expression)
}

// trycast_sql.
func bigqueryTrycastSQL(g *Generator, expression *Expr) string {
	return g.castSQL(expression, "SAFE_")
}

// bigqueryIntArg extracts a Python int argument value.
func bigqueryIntArg(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// bracket_sql.
func bigqueryBracketSQL(g *Generator, expression *Expr) string {
	this := expression.This()
	expressions := expression.Expressions()

	if len(expressions) == 1 && this != nil && dhIsType(this, DT_STRUCT) {
		a := expressions[0]
		if a.Type() == nil {
			a = annotateTypes(a, g.d)
		}

		if t := a.Type(); t != nil && DataType_TEXT_TYPES.Has(t.DTypeOf()) {
			// BQ doesn't support bracket syntax with string values for structs
			return g.sql(this) + "." + a.Name()
		}
	}

	expressionsSQL := g.expressions(expression, exprsOpts{flat: true})
	offset := expression.Arg("offset")

	if n, ok := bigqueryIntArg(offset); ok && n == 0 {
		expressionsSQL = "OFFSET(" + expressionsSQL + ")"
	} else if ok && n == 1 {
		expressionsSQL = "ORDINAL(" + expressionsSQL + ")"
	} else if offset != nil {
		g.unsupported(fmt.Sprintf("Unsupported array offset: %v", offset))
	}

	if expression.ArgB("safe") {
		expressionsSQL = "SAFE_" + expressionsSQL
	}

	return g.sql(this) + "[" + expressionsSQL + "]"
}

// in_unnest_op.
func bigqueryInUnnestOp(g *Generator, expression *Expr) string {
	return g.sql(expression)
}

// version_sql.
func bigqueryVersionSQL(g *Generator, expression *Expr) string {
	if expression.Name() == "TIMESTAMP" {
		expression.Set("this", "SYSTEM_TIME")
	}
	return g.baseVersionSQL(expression)
}

// contains_sql.
func bigqueryContainsSQL(g *Generator, expression *Expr) string {
	this := expression.This()
	expr := expression.Expression()

	if this.IsA(KLower) && expr.IsA(KLower) {
		this = this.This()
		expr = expr.This()
	}

	return g.fn("CONTAINS_SUBSTR", this, expr, expression.Arg("json_scope"))
}

// cast_sql.
func bigqueryCastSQL(g *Generator, expression *Expr, safePrefix string) string {
	this := expression.This()

	// This ensures that inline type-annotated ARRAY literals like ARRAY<INT64>[1, 2, 3]
	// are roundtripped unaffected. The inner check excludes ARRAY(SELECT ...) expressions,
	// because they aren't literals and so the above syntax is invalid BigQuery.
	if this.IsA(KArray) {
		elem := seqGet(this.Expressions(), 0)
		if !(elem != nil && elem.Find(KQuery) != nil) {
			return g.sqlKey(expression, "to") + g.sql(this)
		}
	}

	return g.baseCastSQL(expression, safePrefix)
}

// clusterproperty_sql.
func bigqueryClusterpropertySQL(g *Generator, expression *Expr) string {
	if expression.ArgB("this") {
		g.unsupported("Unsupported CLUSTER BY " + g.sqlKey(expression, "this"))
		return ""
	}
	return g.opExpressions("CLUSTER BY", expression, false)
}
