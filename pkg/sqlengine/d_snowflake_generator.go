package sqlengine

import (
	"fmt"
	"strconv"
	"strings"
)

// Port of sqlglot/generators/snowflake.py (sqlglot v30.13.0): module-level helpers/transforms,
// the TRANSFORMS table edits and the SnowflakeGenerator method overrides. Data-only class
// attributes (TYPE_MAPPING, PROPERTIES_LOCATION, flags, ...) live in zz_generator_settings.go.

// ---------------------------------------------------------------------------------------------
// Module-level helpers
// ---------------------------------------------------------------------------------------------

// snowflakeStrOrNone maps a Python `str | None` result encoded as "" (None) to a g.fn argument.
func snowflakeStrOrNone(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// snowflakeKindSetItems lists the kinds of a KindSet (for isinstance checks against all of them).
func snowflakeKindSetItems(s KindSet) []Kind {
	var out []Kind
	for k := Kind(0); k < numKinds; k++ {
		if s.Has(k) {
			out = append(out, k)
		}
	}
	return out
}

// _regexpilike_sql.
func snowflakeRegexpILikeSQL(g *Generator, e *Expr) string {
	flag := e.Text("flag")

	if !strings.Contains(flag, "i") {
		flag += "i"
	}

	return g.fn("REGEXP_LIKE", e.Arg("this"), e.Arg("expression"), LiteralString(flag))
}

// _unqualify_pivot_columns
//
// Snowflake doesn't allow columns referenced in UNPIVOT to be qualified,
// so we need to unqualify them. Same goes for ANY ORDER BY <column>.
func snowflakeUnqualifyPivotColumns(expression *Expr) *Expr {
	if expression.IsA(KPivot) {
		if expression.ArgB("unpivot") {
			expression = transformUnqualifyColumns(expression)
		} else {
			for _, field := range expression.ArgL("fields") {
				var fieldExprs []*Expr
				if field != nil {
					fieldExprs = field.Expressions()
				}
				fieldExpr := seqGet(fieldExprs, 0)

				if fieldExpr.IsA(KPivotAny) {
					unqualifiedFieldExpr := transformUnqualifyColumns(fieldExpr)
					field.SetIndex("expressions", unqualifiedFieldExpr, 0, true)
				}
			}
		}
	}

	return expression
}

// _flatten_structured_types_unless_iceberg.
func snowflakeFlattenStructuredTypesUnlessIceberg(expression *Expr) *Expr {
	if !expression.IsA(KCreate) {
		panic(&ValueError{Msg: "AssertionError"})
	}

	flattenStructuredType := func(expression *Expr) *Expr {
		if expression.IsA(KDataType) {
			if dt, ok := expression.Arg("this").(DType); ok && DataType_NESTED_TYPES.Has(dt) {
				expression.Set("expressions", nil)
			}
		}
		return expression
	}

	props := expression.ArgE("properties")
	if expression.This().IsA(KSchema) && !(props != nil && props.Find(KIcebergProperty) != nil) {
		for _, schemaExpression := range expression.This().Expressions() {
			if schemaExpression.IsA(KColumnDef) {
				columnType := schemaExpression.ArgE("kind")
				if columnType.IsA(KDataType) {
					columnType.Transform(flattenStructuredType, false)
				}
			}
		}
	}

	return expression
}

// _unnest_generate_date_array.
func snowflakeUnnestGenerateDateArray(unnest *Expr) {
	generateDateArray := unnest.Expressions()[0]
	start := generateDateArray.ArgE("start")
	end := generateDateArray.ArgE("end")
	step := generateDateArray.ArgE("step")

	if start == nil || end == nil || !step.IsA(KInterval) || step.Name() != "1" {
		return
	}

	unit := step.ArgE("unit")

	unnestAlias := unnest.ArgE("alias")
	// sequence_value_name is either a str or an Identifier
	var sequenceValueName any = "value"
	if unnestAlias != nil {
		unnestAlias = unnestAlias.Copy()
		if c := seqGet(unnestAlias.ArgL("columns"), 0); c != nil {
			sequenceValueName = c
		}
	}

	var castTarget *Expr
	switch v := sequenceValueName.(type) {
	case string:
		castTarget = MaybeParse(v, KNone, "", nil)
	case *Expr:
		castTarget = v
	}

	// We'll add the next sequence value to the starting date and project the result
	dateAdd := snowflakeBuildDateTimeAdd(KDateAdd)(
		[]*Expr{unit, CastExpr(castTarget, "int", true, nil), CastExpr(start, "date", true, nil)}, nil,
	)

	// We use DATEDIFF to compute the number of sequence values needed
	numberSequence := snowflakeArrayGenerateRange(
		[]*Expr{LiteralInt(0), dhBinop(KAdd, snowflakeBuildDatediff([]*Expr{unit, start, end}, nil), 1)}, nil,
	)

	unnest.Set("expressions", []*Expr{numberSequence})

	unnestParent := unnest.Parent()
	if unnestParent.IsA(KJoin) {
		sel := unnestParent.Parent()
		if sel.IsA(KSelect) {
			var replaceColumnName string
			switch v := sequenceValueName.(type) {
			case string:
				replaceColumnName = v
			case *Expr:
				replaceColumnName = v.Name()
			}

			scope := BuildScope(sel)
			if scope != nil {
				for _, column := range scope.Columns() {
					if pyLower(column.Name()) == pyLower(replaceColumnName) {
						if column.Parent().IsA(KSelect) {
							column.Replace(dateAdd.ExprAs(replaceColumnName, nil, true))
						} else {
							column.Replace(dateAdd)
						}
					}
				}
			}

			lateral := New(KLateral, "this", unnestParent.This().Pop())
			unnestParent.Replace(New(KJoin, "this", lateral))
		}
	} else {
		unnest.Replace(
			SelectExpr(dateAdd.ExprAs(sequenceValueName, nil, true)).
				SelectFrom(unnest.Copy(), true).
				QuerySubquery(unnestAlias, true),
		)
	}
}

// _transform_generate_date_array.
func snowflakeTransformGenerateDateArray(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		for generateDateArray := range expression.FindAll(KGenerateDateArray) {
			parent := generateDateArray.Parent()

			// If GENERATE_DATE_ARRAY is used directly as an array (e.g passed into ARRAY_LENGTH), the transformed Snowflake
			// query is the following (it'll be unnested properly on the next iteration due to copy):
			// SELECT ref(GENERATE_DATE_ARRAY(...)) -> SELECT ref((SELECT ARRAY_AGG(*) FROM UNNEST(GENERATE_DATE_ARRAY(...))))
			if !parent.IsA(KUnnest) {
				unnest := New(KUnnest, "expressions", []*Expr{generateDateArray.Copy()})
				generateDateArray.Replace(
					SelectExpr(New(KArrayAgg, "this", Star())).SelectFrom(unnest, true).QuerySubquery(nil, true),
				)
			}

			if parent.IsA(KUnnest) && parent.Parent().IsA(KFrom, KJoin) && len(parent.Expressions()) == 1 {
				snowflakeUnnestGenerateDateArray(parent)
			}
		}
	}

	return expression
}

// _regexpextract_sql.
func snowflakeRegexpExtractSQL(g *Generator, e *Expr) string {
	// Other dialects don't support all of the following parameters, so we need to
	// generate default values as necessary to ensure the transpilation is correct
	group := e.ArgE("group")

	// To avoid generating all these default values, we set group to None if
	// it's 0 (also default value) which doesn't trigger the following chain
	if group != nil && group.Name() == "0" {
		group = nil
	}

	parameters := e.ArgE("parameters")
	if parameters == nil && group != nil {
		parameters = LiteralString("c")
	}
	occurrence := e.ArgE("occurrence")
	if occurrence == nil && parameters != nil {
		occurrence = LiteralInt(1)
	}
	position := e.ArgE("position")
	if position == nil && occurrence != nil {
		position = LiteralInt(1)
	}

	name := "REGEXP_SUBSTR_ALL"
	if e.IsA(KRegexpExtract) {
		name = "REGEXP_SUBSTR"
	}
	return g.fn(name, e.Arg("this"), e.Arg("expression"), position, occurrence, parameters, group)
}

// _json_extract_value_array_sql.
func snowflakeJSONExtractValueArraySQL(g *Generator, e *Expr) string {
	jsonExtract := New(KJSONExtract, "this", e.This(), "expression", e.Expression())
	ident := ToIdentifier("x", nil)

	var this *Expr
	if e.IsA(KJSONValueArray) {
		this = CastExpr(ident, DT_VARCHAR, true, nil)
	} else {
		this = New(KParseJSON, "this", "TO_JSON("+exprSQL(ident)+")")
	}

	transformLambda := New(KLambda, "expressions", []*Expr{ident}, "this", this)

	return g.fn("TRANSFORM", jsonExtract, transformLambda)
}

// _qualify_unnested_columns.
func snowflakeQualifyUnnestedColumns(expression *Expr) *Expr {
	if !expression.IsA(KSelect) {
		return expression
	}

	scope := BuildScope(expression)
	if scope == nil {
		return expression
	}

	var unnests []*Expr
	for u := range scope.FindAll(KUnnest) {
		unnests = append(unnests, u)
	}

	if len(unnests) == 0 {
		return expression
	}

	takenSourceNames := newStrSet(scope.Sources.Keys()...)
	columnSource := map[string]*Expr{}
	// unnest_to_identifier is keyed by Unnest expressions (structural equality).
	type unnestIdent struct{ unnest, ident *Expr }
	var unnestToIdentifier []unnestIdent
	lookupUnnest := func(u *Expr) *Expr {
		for _, ui := range unnestToIdentifier {
			if ui.unnest.Equal(u) {
				return ui.ident
			}
		}
		return nil
	}

	var unnestIdentifier *Expr
	origExpression := expression.Copy()

	for _, unnest := range unnests {
		if !unnest.Parent().IsA(KFrom, KJoin) {
			continue
		}

		// Try to infer column names produced by an unnest operator. This is only possible
		// when we can peek into the (statically known) contents of the unnested value.
		unnestColumns := newStrSet()
		var unnestColumnsOrder []string
		for _, unnestExpr := range unnest.Expressions() {
			if !unnestExpr.IsA(KArray) {
				continue
			}

			for _, arrayExpr := range unnestExpr.Expressions() {
				allPropEQ := true
				for _, structExpr := range arrayExpr.Expressions() {
					if !structExpr.IsA(KPropertyEQ) {
						allPropEQ = false
						break
					}
				}
				if !(arrayExpr.IsA(KStruct) && len(arrayExpr.Expressions()) > 0 && allPropEQ) {
					continue
				}

				for _, structExpr := range arrayExpr.Expressions() {
					name := pyLower(structExpr.This().Name())
					if !unnestColumns.Has(name) {
						unnestColumns[name] = struct{}{}
						unnestColumnsOrder = append(unnestColumnsOrder, name)
					}
				}
				break
			}

			if len(unnestColumns) > 0 {
				break
			}
		}

		unnestAlias := unnest.ArgE("alias")
		if unnestAlias == nil {
			aliasName := findNewName(takenSourceNames, "value")
			takenSourceNames[aliasName] = struct{}{}

			// Produce a `TableAlias` AST similar to what is produced for BigQuery. This
			// will be corrected later, when we generate SQL for the `Unnest` AST node.
			aliasedUnnest := AliasTableExpr(unnest, nil, []any{aliasName}, nil, true)
			scope.Replace(unnest, aliasedUnnest)

			unnestIdentifier = aliasedUnnest.ArgE("alias").ArgL("columns")[0]
		} else {
			var aliasColumns []*Expr
			if unnestAlias.IsA(KTableAlias) {
				aliasColumns = unnestAlias.ArgL("columns")
			}
			unnestIdentifier = unnestAlias.This()
			if unnestIdentifier == nil {
				unnestIdentifier = seqGet(aliasColumns, 0)
			}
		}

		if !unnestIdentifier.IsA(KIdentifier) {
			return origExpression
		}

		unnestToIdentifier = append(unnestToIdentifier, unnestIdent{unnest, unnestIdentifier})
		for _, c := range unnestColumnsOrder {
			columnSource[pyLower(c)] = unnestIdentifier
		}
	}

	for _, column := range scope.Columns() {
		if column.TableName() != "" {
			continue
		}

		table := columnSource[pyLower(column.Name())]
		if unnestIdentifier != nil && table == nil && scope.Sources.Len() == 1 &&
			pyLower(column.Name()) != pyLower(unnestIdentifier.Name()) {
			unnestAncestor := column.FindAncestor(KUnnest, KSelect)
			if unnestAncestor.IsA(KUnnest) {
				ancestorIdentifier := lookupUnnest(unnestAncestor)
				if ancestorIdentifier != nil && pyLower(ancestorIdentifier.Name()) == pyLower(unnestIdentifier.Name()) {
					continue
				}
			}

			table = unnestIdentifier
		}

		if table != nil {
			column.Set("table", table.Copy())
		} else {
			column.Set("table", nil)
		}
	}

	return expression
}

// _eliminate_dot_variant_lookup.
func snowflakeEliminateDotVariantLookup(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		// This transformation is used to facilitate transpilation of BigQuery `UNNEST` operations
		// to Snowflake. It should not affect roundtrip because `Unnest` nodes cannot be produced
		// by Snowflake's parser.
		//
		// Additionally, at the time of writing this, BigQuery is the only dialect that produces a
		// `TableAlias` node that only fills `columns` and not `this`, due to `UNNEST_COLUMN_ONLY`.
		unnestAliases := newStrSet()
		for unnest := range FindAllInScope(expression, KUnnest) {
			unnestAlias := unnest.ArgE("alias")
			if unnestAlias.IsA(KTableAlias) && unnestAlias.This() == nil && len(unnestAlias.ArgL("columns")) == 1 {
				unnestAliases[unnestAlias.ArgL("columns")[0].Name()] = struct{}{}
			}
		}

		if len(unnestAliases) > 0 {
			for c := range FindAllInScope(expression, KColumn) {
				if unnestAliases.Has(c.TableName()) {
					bracketLHS := c.ArgE("table")
					bracketRHS := LiteralString(c.Name())
					bracket := New(KBracket, "this", bracketLHS, "expressions", []*Expr{bracketRHS})

					if c.Parent() == expression {
						// Retain column projection names by using aliases
						c.Replace(AliasExpr(bracket, c.This().Copy(), nil, true))
					} else {
						c.Replace(bracket)
					}
				}
			}
		}
	}

	return expression
}

// ---------------------------------------------------------------------------------------------
// SnowflakeGenerator class body
// ---------------------------------------------------------------------------------------------

func customizeSnowflakeGenerator(d *Dialect) {
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

	safePrefix := func(e *Expr) string {
		if e.ArgB("safe") {
			return "TRY_"
		}
		return ""
	}

	T := G.TRANSFORMS
	T[KApproxDistinct] = renameFunc("APPROX_COUNT_DISTINCT")
	T[KArgMax] = renameFunc("MAX_BY")
	T[KArgMin] = renameFunc("MIN_BY")
	T[KArray] = transformPreprocess([]func(*Expr) *Expr{transformInheritStructFieldNames}, nil)
	T[KArrayConcat] = arrayConcatSQL("ARRAY_CAT")
	T[KArrayAppend] = arrayAppendSQL("ARRAY_APPEND", false)
	T[KArrayPrepend] = arrayAppendSQL("ARRAY_PREPEND", false)
	T[KArrayContains] = func(g *Generator, e *Expr) string {
		var first *Expr
		if b, ok := e.Arg("ensure_variant").(bool); ok && !b {
			first = e.Expression()
		} else {
			first = CastExpr(e.Expression(), DT_VARIANT, false, nil)
		}
		return g.fn("ARRAY_CONTAINS", first, e.Arg("this"))
	}
	T[KArrayPosition] = func(g *Generator, e *Expr) string {
		return g.fn("ARRAY_POSITION", e.Arg("expression"), e.Arg("this"))
	}
	T[KArrayIntersect] = renameFunc("ARRAY_INTERSECTION")
	T[KArrayOverlaps] = renameFunc("ARRAYS_OVERLAP")
	T[KAtTimeZone] = func(g *Generator, e *Expr) string {
		return g.fn("CONVERT_TIMEZONE", e.Arg("zone"), e.Arg("this"))
	}
	T[KBitwiseOr] = renameFunc("BITOR")
	T[KBitwiseXor] = renameFunc("BITXOR")
	T[KBitwiseAnd] = renameFunc("BITAND")
	T[KBitwiseAndAgg] = renameFunc("BITANDAGG")
	T[KBitwiseOrAgg] = renameFunc("BITORAGG")
	T[KBitwiseXorAgg] = renameFunc("BITXORAGG")
	T[KBitwiseNot] = renameFunc("BITNOT")
	T[KBitwiseLeftShift] = renameFunc("BITSHIFTLEFT")
	T[KBitwiseRightShift] = renameFunc("BITSHIFTRIGHT")
	T[KCreate] = transformPreprocess([]func(*Expr) *Expr{snowflakeFlattenStructuredTypesUnlessIceberg}, nil)
	T[KCurrentTimestamp] = func(g *Generator, e *Expr) string {
		if e.ArgB("sysdate") {
			return g.fn("SYSDATE")
		}
		return g.functionFallbackSQL(e)
	}
	T[KCurrentSchemas] = func(g *Generator, e *Expr) string { return g.fn("CURRENT_SCHEMAS") }
	T[KLocaltime] = func(g *Generator, e *Expr) string {
		if e.This() != nil {
			return g.fn("CURRENT_TIME", e.Arg("this"))
		}
		return "CURRENT_TIME"
	}
	T[KLocaltimestamp] = func(g *Generator, e *Expr) string {
		if e.This() != nil {
			return g.fn("CURRENT_TIMESTAMP", e.Arg("this"))
		}
		return "CURRENT_TIMESTAMP"
	}
	T[KDateAdd] = dateDeltaSQL("DATEADD", false)
	T[KDateDiff] = dateDeltaSQL("DATEDIFF", false)
	T[KDatetimeAdd] = dateDeltaSQL("TIMESTAMPADD", false)
	T[KDatetimeDiff] = timestampdiffSQL
	T[KDateStrToDate] = datestrtodateSQL
	T[KDecrypt] = func(g *Generator, e *Expr) string {
		return g.fn(
			safePrefix(e)+"DECRYPT",
			e.Arg("this"),
			e.Arg("passphrase"),
			e.Arg("aad"),
			e.Arg("encryption_method"),
		)
	}
	T[KDecryptRaw] = func(g *Generator, e *Expr) string {
		return g.fn(
			safePrefix(e)+"DECRYPT_RAW",
			e.Arg("this"),
			e.Arg("key"),
			e.Arg("iv"),
			e.Arg("aad"),
			e.Arg("encryption_method"),
			e.Arg("aead"),
		)
	}
	T[KDayOfMonth] = renameFunc("DAYOFMONTH")
	T[KDayOfWeek] = renameFunc("DAYOFWEEK")
	T[KDayOfWeekIso] = renameFunc("DAYOFWEEKISO")
	T[KDayOfYear] = renameFunc("DAYOFYEAR")
	T[KDotProduct] = renameFunc("VECTOR_INNER_PRODUCT")
	T[KExplode] = renameFunc("FLATTEN")
	T[KExtract] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_PART", mapDatePart(e.This(), g.d), e.Arg("expression"))
	}
	T[KCosineDistance] = renameFunc("VECTOR_COSINE_SIMILARITY")
	T[KEuclideanDistance] = renameFunc("VECTOR_L2_DISTANCE")
	T[KHandlerProperty] = func(g *Generator, e *Expr) string { return "HANDLER = " + g.sqlKey(e, "this") }
	T[KFileFormatProperty] = func(g *Generator, e *Expr) string {
		return "FILE_FORMAT=(" + g.expressions(e, exprsOpts{key: "expressions", sep: strp2(" ")}) + ")"
	}
	T[KFromTimeZone] = func(g *Generator, e *Expr) string {
		return g.fn("CONVERT_TIMEZONE", e.Arg("zone"), "'UTC'", e.Arg("this"))
	}
	T[KGenerateSeries] = func(g *Generator, e *Expr) string {
		var end any = e.Arg("end")
		if !e.ArgB("is_end_exclusive") {
			end = dhBinop(KAdd, e.ArgE("end"), 1)
		}
		return g.fn("ARRAY_GENERATE_RANGE", e.Arg("start"), end, e.Arg("step"))
	}
	T[KGetExtract] = renameFunc("GET")
	T[KGroupConcat] = func(g *Generator, e *Expr) string { return groupconcatSQL(g, e, "LISTAGG", "", true, false) }
	T[KIf] = ifSQL("IFF", "NULL")
	T[KJSONArray] = func(g *Generator, e *Expr) string {
		args := make([]any, 0, len(e.Expressions()))
		for _, x := range e.Expressions() {
			args = append(args, x)
		}
		return g.fn("TO_VARIANT", g.fn("ARRAY_CONSTRUCT", args...))
	}
	T[KJSONExtractArray] = snowflakeJSONExtractValueArraySQL
	T[KJSONExtractScalar] = func(g *Generator, e *Expr) string {
		return g.fn("JSON_EXTRACT_PATH_TEXT", e.Arg("this"), e.Arg("expression"))
	}
	T[KJSONKeys] = renameFunc("OBJECT_KEYS")
	T[KJSONObject] = func(g *Generator, e *Expr) string {
		args := make([]any, 0, len(e.Expressions()))
		for _, x := range e.Expressions() {
			args = append(args, x)
		}
		return g.fn("OBJECT_CONSTRUCT_KEEP_NULL", args...)
	}
	T[KJSONPathRoot] = func(g *Generator, e *Expr) string { return "" }
	T[KJSONValueArray] = snowflakeJSONExtractValueArraySQL
	levenshtein := renameFunc("EDITDISTANCE")
	T[KLevenshtein] = func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "ins_cost", "del_cost", "sub_cost")
		return levenshtein(g, e)
	}
	T[KLocationProperty] = func(g *Generator, e *Expr) string { return "LOCATION=" + g.sqlKey(e, "this") }
	T[KLogicalAnd] = renameFunc("BOOLAND_AGG")
	T[KLogicalOr] = renameFunc("BOOLOR_AGG")
	T[KMap] = func(g *Generator, e *Expr) string { return varMapSQL(g, e, "OBJECT_CONSTRUCT") }
	T[KManhattanDistance] = renameFunc("VECTOR_L1_DISTANCE")
	T[KMakeInterval] = func(g *Generator, e *Expr) string { return noMakeIntervalSQL(g, e, ", ") }
	T[KMax] = maxOrGreatest
	T[KMin] = minOrLeast
	T[KParseJSON] = func(g *Generator, e *Expr) string { return g.fn(safePrefix(e)+"PARSE_JSON", e.Arg("this")) }
	T[KToBinary] = func(g *Generator, e *Expr) string {
		return g.fn(safePrefix(e)+"TO_BINARY", e.Arg("this"), e.Arg("format"))
	}
	T[KToBoolean] = func(g *Generator, e *Expr) string { return g.fn(safePrefix(e)+"TO_BOOLEAN", e.Arg("this")) }
	T[KToDouble] = func(g *Generator, e *Expr) string {
		return g.fn(safePrefix(e)+"TO_DOUBLE", e.Arg("this"), e.Arg("format"))
	}
	T[KToFile] = func(g *Generator, e *Expr) string {
		return g.fn(safePrefix(e)+"TO_FILE", e.Arg("this"), e.Arg("path"))
	}
	T[KJSONFormat] = renameFunc("TO_JSON")
	T[KPartitionedByProperty] = func(g *Generator, e *Expr) string { return "PARTITION BY " + g.sqlKey(e, "this") }
	T[KPercentileCont] = transformPreprocess([]func(*Expr) *Expr{transformAddWithinGroupForPercentiles}, nil)
	T[KPercentileDisc] = transformPreprocess([]func(*Expr) *Expr{transformAddWithinGroupForPercentiles}, nil)
	T[KPivot] = transformPreprocess([]func(*Expr) *Expr{snowflakeUnqualifyPivotColumns}, nil)
	T[KRegexpExtract] = snowflakeRegexpExtractSQL
	T[KRegexpExtractAll] = snowflakeRegexpExtractSQL
	T[KRegexpILike] = snowflakeRegexpILikeSQL
	T[KRowAccessProperty] = func(g *Generator, e *Expr) string { return snowflakeRowaccesspropertySQL(g, e) }
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateWindowClause,
		transformEliminateDistinctOn,
		transformExplodeProjectionToUnnest(0),
		transformEliminateSemiAndAntiJoins,
		snowflakeTransformGenerateDateArray,
		snowflakeQualifyUnnestedColumns,
		snowflakeEliminateDotVariantLookup,
	}, nil)
	T[KSHA] = renameFunc("SHA1")
	T[KSHA1Digest] = renameFunc("SHA1_BINARY")
	T[KMD5Digest] = renameFunc("MD5_BINARY")
	T[KMD5NumberLower64] = renameFunc("MD5_NUMBER_LOWER64")
	T[KMD5NumberUpper64] = renameFunc("MD5_NUMBER_UPPER64")
	T[KHex] = renameFunc("HEX_ENCODE")
	T[KLowerHex] = renameFunc("TO_CHAR")
	T[KSkewness] = renameFunc("SKEW")
	T[KStarMap] = renameFunc("OBJECT_CONSTRUCT")
	T[KStartsWith] = renameFunc("STARTSWITH")
	T[KEndsWith] = renameFunc("ENDSWITH")
	T[KRand] = func(g *Generator, e *Expr) string { return g.fn("RANDOM", e.Arg("this")) }
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "CHARINDEX", true, false, true)
	}
	T[KStrToDate] = func(g *Generator, e *Expr) string {
		return g.fn("DATE", e.Arg("this"), snowflakeStrOrNone(g.formatTime(e, nil, nil)))
	}
	T[KStringToArray] = renameFunc("STRTOK_TO_ARRAY")
	T[KStrtokToArray] = renameFunc("STRTOK_TO_ARRAY")
	T[KStuff] = renameFunc("INSERT")
	T[KStPoint] = renameFunc("ST_MAKEPOINT")
	T[KTimeAdd] = dateDeltaSQL("TIMEADD", false)
	T[KTimeSlice] = func(g *Generator, e *Expr) string {
		return g.fn("TIME_SLICE", e.Arg("this"), e.Arg("expression"), unitToStr(e, "DAY"), e.Arg("kind"))
	}
	T[KTimestamp] = noTimestampSQL
	T[KTimestampAdd] = dateDeltaSQL("TIMESTAMPADD", false)
	T[KTimestampDiff] = func(g *Generator, e *Expr) string {
		return g.fn("TIMESTAMPDIFF", e.Arg("unit"), e.Arg("expression"), e.Arg("this"))
	}
	T[KTimestampTrunc] = timestamptruncSQL("DATE_TRUNC", false)
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, false) }
	T[KTimeToUnix] = func(g *Generator, e *Expr) string {
		return "EXTRACT(epoch_second FROM " + g.sqlKey(e, "this") + ")"
	}
	T[KToArray] = renameFunc("TO_ARRAY")
	T[KToChar] = func(g *Generator, e *Expr) string { return g.functionFallbackSQL(e) }
	T[KTsOrDsAdd] = dateDeltaSQL("DATEADD", true)
	T[KTsOrDsDiff] = dateDeltaSQL("DATEDIFF", false)
	T[KTsOrDsToDate] = func(g *Generator, e *Expr) string {
		return g.fn(safePrefix(e)+"TO_DATE", e.Arg("this"), snowflakeStrOrNone(g.formatTime(e, nil, nil)))
	}
	T[KTsOrDsToTime] = func(g *Generator, e *Expr) string {
		return g.fn(safePrefix(e)+"TO_TIME", e.Arg("this"), snowflakeStrOrNone(g.formatTime(e, nil, nil)))
	}
	T[KUnhex] = renameFunc("HEX_DECODE_BINARY")
	T[KUnixToTime] = func(g *Generator, e *Expr) string {
		return g.fn("TO_TIMESTAMP", e.Arg("this"), e.Arg("scale"))
	}
	T[KUuid] = renameFunc("UUID_STRING")
	T[KVarMap] = func(g *Generator, e *Expr) string { return varMapSQL(g, e, "OBJECT_CONSTRUCT") }
	T[KBooland] = renameFunc("BOOLAND")
	T[KBoolor] = renameFunc("BOOLOR")
	T[KWeekOfYear] = renameFunc("WEEKISO")
	T[KYearOfWeek] = renameFunc("YEAROFWEEK")
	T[KYearOfWeekIso] = renameFunc("YEAROFWEEKISO")
	T[KXor] = renameFunc("BOOLXOR")
	T[KByteLength] = renameFunc("OCTET_LENGTH")
	T[KFlatten] = renameFunc("ARRAY_FLATTEN")
	T[KArrayConcatAgg] = func(g *Generator, e *Expr) string {
		return g.fn("ARRAY_FLATTEN", New(KArrayAgg, "this", e.Arg("this")))
	}
	T[KSHA2Digest] = func(g *Generator, e *Expr) string {
		length := e.ArgE("length")
		if length == nil {
			length = LiteralInt(256)
		}
		return g.fn("SHA2_BINARY", e.Arg("this"), length)
	}
	for _, k := range []Kind{
		KJSONPathFilter, KJSONPathRecursive, KJSONPathScript, KJSONPathSelector,
		KJSONPathSlice, KJSONPathUnion, KJSONPathWildcard,
	} {
		delete(T, k)
	}

	// Method overrides (hooks) and dialect-only <key>_sql methods.
	G.h.dynamicidentifierSQL = snowflakeDynamicidentifierSQL
	G.methods[KSortArray] = snowflakeSortarraySQL
	G.methods[KNthValue] = snowflakeNthvalueSQL
	G.h.withProperties = snowflakeWithProperties
	G.h.valuesSQL = snowflakeValuesSQL
	G.h.datatypeSQL = snowflakeDatatypeSQL
	G.h.tonumberSQL = snowflakeTonumberSQL
	G.methods[KTimestampFromParts] = snowflakeTimestampfrompartsSQL
	G.h.castSQL = snowflakeCastSQL
	G.h.trycastSQL = snowflakeTrycastSQL
	G.h.logSQL = snowflakeLogSQL
	G.methods[KGreatest] = snowflakeGreatestSQL
	G.methods[KLeast] = snowflakeLeastSQL
	G.methods[KGenerator] = snowflakeGeneratorSQL
	G.h.unnestSQL = snowflakeUnnestSQL
	G.methods[KUndrop] = snowflakeUndropSQL
	G.h.showSQL = snowflakeShowSQL
	G.methods[KRowAccessProperty] = snowflakeRowaccesspropertySQL
	G.h.describeSQL = snowflakeDescribeSQL
	G.h.generatedasidentitycolumnconstraintSQL = snowflakeGeneratedasidentitycolumnconstraintSQL
	G.h.structSQL = snowflakeStructSQL
	G.methods[KApproxQuantile] = snowflakeApproxquantileSQL
	G.h.altersetSQL = snowflakeAltersetSQL
	G.h.strtotimeSQL = snowflakeStrtotimeSQL
	G.methods[KTimestampSub] = snowflakeTimestampsubSQL
	G.methods[KJSONExtract] = snowflakeJsonextractSQL
	G.methods[KTimeToStr] = snowflakeTimetostrSQL
	G.methods[KDateSub] = snowflakeDatesubSQL
	G.h.selectSQL = snowflakeSelectSQL
	G.h.createableSQL = snowflakeCreateableSQL
	G.h.arrayaggSQL = snowflakeArrayaggSQL
	G.methods[KArrayDistinct] = snowflakeArraydistinctSQL
	G.methods[KArrayToString] = snowflakeArraytostringSQL
	G.methods[KArray] = snowflakeArraySQL
	G.h.currentdateSQL = snowflakeCurrentdateSQL
	G.h.dotSQL = snowflakeDotSQL
	G.h.modelattributeSQL = snowflakeModelattributeSQL
	G.methods[KFormat] = snowflakeFormatSQL
	G.methods[KSplitPart] = snowflakeSplitpartSQL
	G.methods[KUniform] = snowflakeUniformSQL
	G.h.windowSQL = snowflakeWindowSQL
	G.h.filterSQL = snowflakeFilterSQL
	G.h.withingroupSQL = snowflakeWithingroupSQL
}

// ---------------------------------------------------------------------------------------------
// SnowflakeGenerator methods
// ---------------------------------------------------------------------------------------------

// dynamicidentifier_sql.
func snowflakeDynamicidentifierSQL(g *Generator, e *Expr) string {
	this := g.fn("IDENTIFIER", e.Arg("this"))
	if e.HasArgKey("expressions") {
		// `IDENTIFIER(...)` invoked as a function, e.g. `IDENTIFIER('my_func')(1, 2)`
		args := make([]any, 0, len(e.Expressions()))
		for _, x := range e.Expressions() {
			args = append(args, x)
		}
		return g.funcFull(this, "(", ")", false, args...)
	}
	return this
}

// sortarray_sql.
func snowflakeSortarraySQL(g *Generator, e *Expr) string {
	asc := e.ArgE("asc")
	nullsFirst := e.ArgE("nulls_first")
	if asc.Equal(Boolean(false)) && nullsFirst.Equal(Boolean(true)) {
		nullsFirst = nil
	}
	return g.fn("ARRAY_SORT", e.Arg("this"), asc, nullsFirst)
}

// nthvalue_sql.
func snowflakeNthvalueSQL(g *Generator, e *Expr) string {
	result := g.fn("NTH_VALUE", e.Arg("this"), e.Arg("offset"))

	fromFirst := e.Arg("from_first")

	if fromFirst != nil {
		if truthy(fromFirst) {
			result = result + " FROM FIRST"
		} else {
			result = result + " FROM LAST"
		}
	}

	return result
}

// with_properties.
func snowflakeWithProperties(g *Generator, properties *Expr) string {
	return g.properties(properties, g.sep(""), " ", "", false)
}

// values_sql.
func snowflakeValuesSQL(g *Generator, e *Expr, valuesAsTable bool) string {
	if e.Find(snowflakeKindSetItems(g.s.UNSUPPORTED_VALUES_EXPRESSIONS)...) != nil {
		valuesAsTable = false
	}

	return g.baseValuesSQL(e, valuesAsTable)
}

// datatype_sql.
func snowflakeDatatypeSQL(g *Generator, e *Expr) string {
	// Check if this is a FLOAT type nested inside a VECTOR type
	// VECTOR only accepts FLOAT (not DOUBLE), INT, and STRING as element types
	// https://docs.snowflake.com/en/sql-reference/data-types-vector
	if dhIsType(e, DT_DOUBLE) {
		parent := e.Parent()
		if parent.IsA(KDataType) && dhIsType(parent, DT_VECTOR) {
			// Preserve FLOAT for VECTOR types instead of mapping to synonym DOUBLE
			return "FLOAT"
		}
	}

	expressions := e.Expressions()
	if len(expressions) > 0 && dhIsType(e, DataType_STRUCT_TYPES.Items()...) {
		for _, fieldType := range expressions {
			// The correct syntax is OBJECT [ (<key> <value_type [NOT NULL] [, ...]) ]
			if fieldType.IsA(KDataType) {
				return "OBJECT"
			}
			if fieldType.IsA(KColumnDef) && fieldType.This() != nil && fieldType.This().IsString() {
				// Doing OBJECT('foo' VARCHAR) is invalid snowflake Syntax. Moreover, besides
				// converting 'foo' into an identifier, we also need to quote it because these
				// keys are case-sensitive. For example:
				//
				// WITH t AS (SELECT OBJECT_CONSTRUCT('x', 'y') AS c) SELECT c:x FROM t -- correct
				// WITH t AS (SELECT OBJECT_CONSTRUCT('x', 'y') AS c) SELECT c:X FROM t -- incorrect, returns NULL
				fieldType.This().Replace(ToIdentifier(fieldType.Name(), boolp(true)))
			}
		}
	}

	return g.baseDatatypeSQL(e)
}

// tonumber_sql.
func snowflakeTonumberSQL(g *Generator, e *Expr) string {
	precision := e.ArgE("precision")
	scale := e.ArgE("scale")

	defaultPrecision := precision.IsA(KLiteral) && precision.Name() == "38"
	defaultScale := scale.IsA(KLiteral) && scale.Name() == "0"

	if defaultPrecision && defaultScale {
		precision = nil
		scale = nil
	} else if defaultScale {
		scale = nil
	}

	funcName := "TO_NUMBER"
	if e.ArgB("safe") {
		funcName = "TRY_TO_NUMBER"
	}

	return g.fn(funcName, e.Arg("this"), e.Arg("format"), precision, scale)
}

// timestampfromparts_sql.
func snowflakeTimestampfrompartsSQL(g *Generator, e *Expr) string {
	milli := e.ArgE("milli")
	if milli != nil {
		milliToNano := dhBinop(KMul, milli.Pop(), LiteralInt(1000000))
		e.Set("nano", milliToNano)
	}

	return renameFunc("TIMESTAMP_FROM_PARTS")(g, e)
}

// cast_sql.
func snowflakeCastSQL(g *Generator, e *Expr, safePrefix string) string {
	if dhIsType(e, DT_GEOGRAPHY) {
		return g.fn("TO_GEOGRAPHY", e.Arg("this"))
	}
	if dhIsType(e, DT_GEOMETRY) {
		return g.fn("TO_GEOMETRY", e.Arg("this"))
	}

	return g.baseCastSQL(e, safePrefix)
}

// trycast_sql.
func snowflakeTrycastSQL(g *Generator, e *Expr) string {
	value := e.This()

	if value.Type() == nil {
		value = annotateTypes(value, g.d)
	}

	// Snowflake requires that TRY_CAST's value be a string
	// If TRY_CAST is being roundtripped (since Snowflake is the only dialect that sets "requires_string") or
	// if we can deduce that the value is a string, then we can generate TRY_CAST
	if e.ArgB("requires_string") || dhIsType(value, DataType_TEXT_TYPES.Items()...) {
		return g.baseTrycastSQL(e)
	}

	return g.castSQL(e, "")
}

// log_sql.
func snowflakeLogSQL(g *Generator, e *Expr) string {
	if e.Expression() == nil {
		return g.fn("LN", e.Arg("this"))
	}

	return g.baseLogSQL(e)
}

// greatest_sql.
func snowflakeGreatestSQL(g *Generator, e *Expr) string {
	name := "GREATEST"
	if e.ArgB("ignore_nulls") {
		name = "GREATEST_IGNORE_NULLS"
	}
	args := []any{e.This()}
	for _, x := range e.Expressions() {
		args = append(args, x)
	}
	return g.fn(name, args...)
}

// least_sql.
func snowflakeLeastSQL(g *Generator, e *Expr) string {
	name := "LEAST"
	if e.ArgB("ignore_nulls") {
		name = "LEAST_IGNORE_NULLS"
	}
	args := []any{e.This()}
	for _, x := range e.Expressions() {
		args = append(args, x)
	}
	return g.fn(name, args...)
}

// generator_sql.
func snowflakeGeneratorSQL(g *Generator, e *Expr) string {
	var args []any
	rowcount := e.ArgE("rowcount")
	timelimit := e.ArgE("timelimit")

	if rowcount != nil {
		args = append(args, New(KKwarg, "this", VarChecked("ROWCOUNT"), "expression", rowcount))
	}
	if timelimit != nil {
		args = append(args, New(KKwarg, "this", VarChecked("TIMELIMIT"), "expression", timelimit))
	}

	return g.fn("GENERATOR", args...)
}

// unnest_sql.
func snowflakeUnnestSQL(g *Generator, e *Expr) string {
	unnestAlias := e.ArgE("alias")
	offset := e.Arg("offset")

	var unnestAliasColumns []*Expr
	if unnestAlias != nil {
		unnestAliasColumns = unnestAlias.ArgL("columns")
	}
	value := seqGet(unnestAliasColumns, 0)
	if value == nil {
		value = ToIdentifier("value", nil)
	}

	var offsetCol *Expr
	if o, ok := offset.(*Expr); ok && o != nil {
		offsetCol = o.Pop()
	} else {
		offsetCol = ToIdentifier("index", nil)
	}

	columns := []*Expr{
		ToIdentifier("seq", nil),
		ToIdentifier("key", nil),
		ToIdentifier("path", nil),
		offsetCol,
		value,
		ToIdentifier("this", nil),
	}

	if unnestAlias != nil {
		unnestAlias.Set("columns", columns)
	} else {
		unnestAlias = New(KTableAlias, "this", "_u", "columns", columns)
	}

	tableInput := g.sql(e.Expressions()[0])
	if !strings.HasPrefix(tableInput, "INPUT =>") {
		tableInput = "INPUT => " + tableInput
	}

	expressionParent := e.Parent()

	var explode string
	if expressionParent.IsA(KLateral) {
		explode = "FLATTEN(" + tableInput + ")"
	} else {
		explode = "TABLE(FLATTEN(" + tableInput + "))"
	}
	alias := g.sql(unnestAlias)
	if alias != "" {
		alias = " AS " + alias
	}
	valueSQL := ""
	if !expressionParent.IsA(KFrom, KJoin, KLateral) {
		valueSQL = exprSQL(value) + " FROM "
	}

	return valueSQL + explode + alias
}

// undrop_sql.
func snowflakeUndropSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	kind := e.ArgS("kind")
	rename := g.sqlKey(e, "rename")
	if rename != "" {
		rename = " RENAME TO " + rename
	}
	return "UNDROP " + kind + " " + this + rename
}

// show_sql.
func snowflakeShowSQL(g *Generator, e *Expr) string {
	terse := ""
	if e.ArgB("terse") {
		terse = "TERSE "
	}
	iceberg := ""
	if e.ArgB("iceberg") {
		iceberg = "ICEBERG "
	}
	history := ""
	if e.ArgB("history") {
		history = " HISTORY"
	}
	like := g.sqlKey(e, "like")
	if like != "" {
		like = " LIKE " + like
	}

	scope := g.sqlKey(e, "scope")
	if scope != "" {
		scope = " " + scope
	}

	scopeKind := g.sqlKey(e, "scope_kind")
	if scopeKind != "" {
		scopeKind = " IN " + scopeKind
	}

	startsWith := g.sqlKey(e, "starts_with")
	if startsWith != "" {
		startsWith = " STARTS WITH " + startsWith
	}

	limit := g.sqlKey(e, "limit")

	from := g.sqlKey(e, "from_")
	if from != "" {
		from = " FROM " + from
	}

	privileges := g.expressions(e, exprsOpts{key: "privileges", flat: true})
	if privileges != "" {
		privileges = " WITH PRIVILEGES " + privileges
	}

	return "SHOW " + terse + iceberg + e.Name() + history + like + scopeKind + scope + startsWith + limit + from + privileges
}

// rowaccessproperty_sql.
func snowflakeRowaccesspropertySQL(g *Generator, e *Expr) string {
	if e.This() == nil {
		return "ROW ACCESS"
	}
	on := ""
	if len(e.Expressions()) > 0 {
		on = " ON (" + g.expressions(e, exprsOpts{flat: true}) + ")"
	}
	return "WITH ROW ACCESS POLICY " + g.sqlKey(e, "this") + on
}

// describe_sql.
func snowflakeDescribeSQL(g *Generator, e *Expr) string {
	kindValue := e.ArgS("kind")
	if kindValue == "" {
		kindValue = "TABLE"
	}

	properties := e.ArgE("properties")
	var kind string
	if properties != nil {
		qualifier := g.expressions(properties, exprsOpts{sep: strp2(" ")})
		kind = " " + qualifier + " " + kindValue
	} else {
		kind = " " + kindValue
	}

	this := " " + g.sqlKey(e, "this")
	expressions := g.expressions(e, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " " + expressions
	}
	return "DESCRIBE" + kind + this + expressions
}

// generatedasidentitycolumnconstraint_sql.
func snowflakeGeneratedasidentitycolumnconstraintSQL(g *Generator, e *Expr) string {
	start := ""
	if s := e.ArgE("start"); s != nil {
		start = " START " + exprSQL(s)
	}
	increment := ""
	if i := e.ArgE("increment"); i != nil {
		increment = " INCREMENT " + exprSQL(i)
	}

	orderClause := ""
	if order := e.Arg("order"); order != nil {
		if truthy(order) {
			orderClause = " ORDER"
		} else {
			orderClause = " NOORDER"
		}
	}

	return "AUTOINCREMENT" + start + increment + orderClause
}

// struct_sql.
func snowflakeStructSQL(g *Generator, e *Expr) string {
	if len(e.Expressions()) == 1 {
		arg := e.Expressions()[0]
		if arg.IsStar() || (arg.IsA(KILike) && arg.Left().IsStar()) {
			// Wildcard syntax: https://docs.snowflake.com/en/sql-reference/data-types-semistructured#object
			return "{" + g.sql(e.Expressions()[0]) + "}"
		}
	}

	var args []any

	for i, x := range e.Expressions() {
		if x.IsA(KPropertyEQ) {
			if x.This().IsA(KIdentifier) {
				args = append(args, LiteralString(x.Name()))
			} else {
				args = append(args, x.This())
			}
			args = append(args, x.Expression())
		} else {
			args = append(args, LiteralString("_"+strconv.Itoa(i)), x)
		}
	}

	return g.fn("OBJECT_CONSTRUCT", args...)
}

// approxquantile_sql.
func snowflakeApproxquantileSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "weight", "accuracy")
	return g.fn("APPROX_PERCENTILE", e.Arg("this"), e.Arg("quantile"))
}

// alterset_sql.
func snowflakeAltersetSQL(g *Generator, e *Expr) string {
	exprs := g.expressions(e, exprsOpts{flat: true})
	if exprs != "" {
		exprs = " " + exprs
	}
	fileFormat := g.expressions(e, exprsOpts{key: "file_format", flat: true, sep: strp2(" ")})
	if fileFormat != "" {
		fileFormat = " STAGE_FILE_FORMAT = (" + fileFormat + ")"
	}
	copyOptions := g.expressions(e, exprsOpts{key: "copy_options", flat: true, sep: strp2(" ")})
	if copyOptions != "" {
		copyOptions = " STAGE_COPY_OPTIONS = (" + copyOptions + ")"
	}
	tag := g.expressions(e, exprsOpts{key: "tag", flat: true})
	if tag != "" {
		tag = " TAG " + tag
	}

	return "SET" + exprs + fileFormat + copyOptions + tag
}

// strtotime_sql.
func snowflakeStrtotimeSQL(g *Generator, e *Expr) string {
	// target_type is stored as a DataType instance
	targetType := e.ArgE("target_type")

	// Get the type enum from DataType instance or from type annotation
	var typeEnum any
	if targetType.IsA(KDataType) {
		typeEnum = targetType.Arg("this")
	} else if t := e.Type(); t != nil {
		typeEnum = t.Arg("this")
	} else {
		typeEnum = DT_TIMESTAMP
	}

	funcName := "TO_TIMESTAMP"
	if dt, ok := typeEnum.(DType); ok {
		if n, ok := snowflakeTimestampTypes[dt]; ok {
			funcName = n
		}
	}

	prefix := ""
	if e.ArgB("safe") {
		prefix = "TRY_"
	}
	return g.fn(prefix+funcName, e.Arg("this"), snowflakeStrOrNone(g.formatTime(e, nil, nil)))
}

// timestampsub_sql.
func snowflakeTimestampsubSQL(g *Generator, e *Expr) string {
	return g.sql(New(
		KTimestampAdd,
		"this", e.This(),
		"expression", dhBinop(KMul, e.Expression(), -1),
		"unit", e.Arg("unit"),
	))
}

// jsonextract_sql.
func snowflakeJsonextractSQL(g *Generator, e *Expr) string {
	this := e.This()

	// JSON strings are valid coming from other dialects such as BQ so
	// for these cases we PARSE_JSON preemptively
	if !this.IsA(KParseJSON, KJSONExtract) && !e.ArgB("requires_json") {
		this = New(KParseJSON, "this", this)
	}

	return g.fn("GET_PATH", this, e.Arg("expression"))
}

// timetostr_sql.
func snowflakeTimetostrSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsString() {
		this = CastExpr(this, DT_TIMESTAMP, true, nil)
	}

	return g.fn("TO_CHAR", this, snowflakeStrOrNone(g.formatTime(e, nil, nil)))
}

// datesub_sql.
func snowflakeDatesubSQL(g *Generator, e *Expr) string {
	value := e.Expression()
	if value != nil {
		value.Replace(dhBinop(KMul, value, -1))
	} else {
		g.unsupported("DateSub cannot be transpiled if the subtracted count is unknown")
	}

	return dateDeltaSQL("DATEADD", false)(g, e)
}

// select_sql.
func snowflakeSelectSQL(g *Generator, e *Expr) string {
	limit := e.Arg("limit")
	offset := e.Arg("offset")
	if truthy(offset) && !truthy(limit) {
		e.QueryLimit(Null(), false)
	}
	return g.baseSelectSQL(e)
}

// createable_sql.
func snowflakeCreateableSQL(g *Generator, e *Expr, locations propLocations) string {
	isMaterialized := e.Find(KMaterializedProperty)
	copyGrantsProperty := e.Find(KCopyGrantsProperty)

	if e.KindText() == "VIEW" && isMaterialized != nil && copyGrantsProperty != nil {
		// For materialized views, COPY GRANTS is located *before* the columns list
		// This is in contrast to normal views where COPY GRANTS is located *after* the columns list
		// We default CopyGrantsProperty to POST_SCHEMA which means we need to output it POST_NAME if a materialized view is detected
		// ref: https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#syntax
		// ref: https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax
		postSchemaProperties := locations[Loc_POST_SCHEMA]
		idx := -1
		for i, p := range postSchemaProperties {
			if p.Equal(copyGrantsProperty) {
				idx = i
				break
			}
		}
		if idx < 0 {
			panic(&ValueError{Msg: "list.index(x): x not in list"})
		}
		postSchemaProperties = append(postSchemaProperties[:idx:idx], postSchemaProperties[idx+1:]...)
		locations[Loc_POST_SCHEMA] = postSchemaProperties

		thisName := g.sqlKey(e.This(), "this")
		copyGrants := g.sql(copyGrantsProperty)
		thisSchema := g.schemaColumnsSQL(e.This())
		if thisSchema != "" {
			thisSchema = g.sep1() + thisSchema
		}

		return thisName + g.sep1() + copyGrants + thisSchema
	}

	return g.baseCreateableSQL(e, locations)
}

// arrayagg_sql.
func snowflakeArrayaggSQL(g *Generator, e *Expr) string {
	this := e.This()

	// If an ORDER BY clause is present, we need to remove it from ARRAY_AGG
	// and add it later as part of the WITHIN GROUP clause
	var order *Expr
	if this.IsA(KOrder) {
		order = this
	}
	if order != nil {
		e.Set("this", order.This().Pop())
	}

	exprSQLStr := g.baseArrayaggSQL(e)

	if order != nil {
		exprSQLStr = g.sql(New(KWithinGroup, "this", exprSQLStr, "expression", order))
	}

	return exprSQLStr
}

// arraydistinct_sql.
func snowflakeArraydistinctSQL(g *Generator, e *Expr) string {
	if e.ArgB("check_null") {
		return g.fn("ARRAY_DISTINCT", e.Arg("this"))
	}
	return g.fn("ARRAY_DISTINCT", New(KArrayCompact, "this", e.Arg("this")))
}

// arraytostring_sql.
func snowflakeArraytostringSQL(g *Generator, e *Expr) string {
	return g.fn("ARRAY_TO_STRING", e.Arg("this"), e.Arg("expression"))
}

// array_sql.
func snowflakeArraySQL(g *Generator, e *Expr) string {
	expressions := e.Expressions()

	firstExpr := seqGet(expressions, 0)
	if firstExpr.IsA(KSelect) {
		// SELECT AS STRUCT foo AS alias_foo -> ARRAY_AGG(OBJECT_CONSTRUCT('alias_foo', foo))
		if pyUpper(firstExpr.Text("kind")) == "STRUCT" {
			var objectConstructArgs []*Expr
			for _, expr := range firstExpr.Expressions() {
				// Alias case: SELECT AS STRUCT foo AS alias_foo -> OBJECT_CONSTRUCT('alias_foo', foo)
				// Column case: SELECT AS STRUCT foo -> OBJECT_CONSTRUCT('foo', foo)
				name := expr
				if expr.IsA(KAlias) {
					name = expr.This()
				}

				objectConstructArgs = append(objectConstructArgs, LiteralString(expr.AliasOrName()), name)
			}

			arrayAgg := New(KArrayAgg, "this", snowflakeBuildObjectConstruct(objectConstructArgs))

			firstExpr.Set("kind", nil)
			firstExpr.Set("expressions", []*Expr{arrayAgg})

			return g.sql(firstExpr.QuerySubquery(nil, true))
		}
	}

	return inlineArraySQL(g, e)
}

// currentdate_sql.
func snowflakeCurrentdateSQL(g *Generator, e *Expr) string {
	zone := g.sqlKey(e, "this")
	if zone == "" {
		return g.baseCurrentdateSQL(e)
	}

	expr := New(
		KCast,
		"this", New(KConvertTimezone, "target_tz", zone, "timestamp", New(KCurrentTimestamp)),
		"to", NewDataType(DT_DATE),
	)
	return g.sql(expr)
}

// dot_sql.
func snowflakeDotSQL(g *Generator, e *Expr) string {
	this := e.argAttr("this", "type")

	if this.Type() == nil {
		this = annotateTypes(this, g.d)
	}

	if !this.IsA(KDot) && dhIsType(this, DT_STRUCT) {
		// Generate colon notation for the top level STRUCT
		return g.sql(this) + ":" + g.sqlKey(e, "expression")
	}

	return g.baseDotSQL(e)
}

// modelattribute_sql.
func snowflakeModelattributeSQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + "!" + g.sqlKey(e, "expression")
}

// format_sql.
func snowflakeFormatSQL(g *Generator, e *Expr) string {
	if pyLower(e.Name()) == "%s" && len(e.Expressions()) == 1 {
		return g.fn("TO_CHAR", e.Expressions()[0])
	}

	return g.functionFallbackSQL(e)
}

// splitpart_sql.
func snowflakeSplitpartSQL(g *Generator, e *Expr) string {
	// Set part_index to 1 if missing
	if !e.ArgB("delimiter") {
		e.Set("delimiter", LiteralString(" "))
	}

	if !e.ArgB("part_index") {
		e.Set("part_index", LiteralInt(1))
	}

	return renameFunc("SPLIT_PART")(g, e)
}

// uniform_sql.
func snowflakeUniformSQL(g *Generator, e *Expr) string {
	gen := e.ArgE("gen")
	seed := e.ArgE("seed")

	// From Databricks UNIFORM(min, max, seed) -> Wrap gen in RANDOM(seed)
	if seed != nil {
		gen = New(KRand, "this", seed)
	}

	// No gen argument (from Databricks 2-arg UNIFORM(min, max)) -> Add RANDOM()
	if gen == nil {
		gen = New(KRand)
	}

	return g.fn("UNIFORM", e.Arg("this"), e.Arg("expression"), gen)
}

// window_sql.
func snowflakeWindowSQL(g *Generator, e *Expr) string {
	spec := e.ArgE("spec")
	this := e.This()

	if (this.IsA(snowflakeRankingWindowFunctionsWithFrame...) ||
		(this.IsA(KRespectNulls, KIgnoreNulls) && this.This().IsA(snowflakeRankingWindowFunctionsWithFrame...))) &&
		spec != nil &&
		(pyUpper(spec.Text("kind")) == "ROWS" &&
			pyUpper(spec.Text("start")) == "UNBOUNDED" &&
			pyUpper(spec.Text("start_side")) == "PRECEDING" &&
			pyUpper(spec.Text("end")) == "UNBOUNDED" &&
			pyUpper(spec.Text("end_side")) == "FOLLOWING") {
		// omit the default window from window ranking functions
		e.Set("spec", nil)
	}
	return g.baseWindowSQL(e)
}

// filter_sql.
func snowflakeFilterSQL(g *Generator, e *Expr) string {
	// Snowflake doesn't support FILTER (WHERE cond), so we rewrite it into an
	// equivalent conditional aggregation, i.e. wrap the input values in an IFF
	agg := e.This()
	aggArg := agg.This()
	cond := e.Expression().This()

	if agg.IsA(KWithinGroup) {
		// Ordered-set aggregates take their input from the ORDER BY key, so the
		// condition has to wrap that instead of the aggregate's own argument
		if aggArg.IsA(KMode, KPercentileCont, KPercentileDisc) {
			for _, ordered := range agg.Expression().Expressions() {
				key := ordered.This()
				key.Replace(New(KIf, "this", cond.Copy(), "true", key.Copy()))
			}

			return g.sql(agg)
		}

		// Besides the percentile functions, these are the only functions Snowflake
		// accepts WITHIN GROUP for, so anything else can't be rewritten correctly
		if aggArg.IsA(KArrayAgg, KGroupConcat) {
			aggArg = aggArg.This()
		} else {
			g.unsupported("Unable to rewrite FILTER into the aggregate's arguments")
			return g.sql(agg)
		}
	}

	// `COUNT(*/t.*) FILTER (WHERE cond)` counts qualifying rows, but a star can't be an IFF
	// argument: `IFF(cond, *, NULL)` expands to multiple columns once the table has 2+ of
	// them, which Snowflake rejects. Use its native COUNT_IF instead.
	if agg.IsA(KCount) && aggArg.IsStar() {
		return g.fn("COUNT_IF", cond)
	}

	// `DISTINCT` and `ORDER BY` are part of the aggregate's own argument list, so the
	// condition has to wrap the values underneath them rather than the whole clause --
	// `IFF(cond, DISTINCT x, NULL)` is not a call any dialect accepts.
	if aggArg.IsA(KOrder) {
		aggArg = aggArg.This()
	}

	var targets []*Expr
	if aggArg.IsA(KDistinct) {
		targets = aggArg.Expressions()
	} else {
		targets = []*Expr{aggArg}
	}

	for _, target := range targets {
		// cond.copy() / target.copy() raise AttributeError on non-expressions (e.g. the raw
		// function-name string of an Anonymous aggregate).
		if cond == nil {
			panic(&ValueError{Msg: fmt.Sprintf("'%s' object has no attribute 'copy'", pyTypeName(e.Expression().Arg("this")))})
		}
		if target == nil {
			raw := any(nil)
			if aggArg == nil {
				raw = agg.Arg("this")
			}
			panic(&ValueError{Msg: fmt.Sprintf("'%s' object has no attribute 'copy'", pyTypeName(raw))})
		}
		target.Replace(New(KIf, "this", cond.Copy(), "true", target.Copy()))
	}

	return g.sql(agg)
}

// withingroup_sql.
func snowflakeWithingroupSQL(g *Generator, e *Expr) string {
	// Snowflake's MODE doesn't support the ordered-set syntax, i.e. it only
	// accepts the value to aggregate as an argument: MODE(<expr>)
	if e.This().IsA(KMode) && e.This().This() == nil {
		order := e.Expression()
		if order.IsA(KOrder) && len(order.Expressions()) == 1 {
			return g.sql(New(KMode, "this", order.Expressions()[0].This()))
		}
	}

	return g.baseWithingroupSQL(e)
}
