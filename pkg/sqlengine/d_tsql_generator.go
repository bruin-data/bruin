package sqlengine

// Port of sqlglot/generators/tsql.py (TSQLGenerator and its module-level helpers).

import (
	"strings"
)

// tsqlDATE_PART_UNMAPPING mirrors generators/tsql.py DATE_PART_UNMAPPING.
var tsqlDATE_PART_UNMAPPING = map[string]string{
	"WEEKISO":         "ISO_WEEK",
	"DAYOFWEEK":       "WEEKDAY",
	"TIMEZONE_MINUTE": "TZOFFSET",
}

// tsqlBIT_TYPES mirrors generators/tsql.py BIT_TYPES (exact classes).
var tsqlBIT_TYPES = map[Kind]bool{KEQ: true, KNEQ: true, KIs: true, KIn: true, KSelect: true, KAlias: true}

// tsqlFormatSQL mirrors generators/tsql.py _format_sql.
func tsqlFormatSQL(g *Generator, e *Expr) string {
	fmtE := e.ArgE("format")

	var fmtSQL string
	if !e.IsA(KNumberToStr) {
		if fmtE.IsString() {
			mapped, ok := formatTime(fmtE.Name(), tsqlDialect().S.INVERSE_TIME_MAPPING, nil)
			fmtSQL = g.sql(tsqlLiteralStringOpt(mapped, ok))
		} else {
			fmtSQL = g.formatTime(e, nil, nil)
			if fmtSQL == "" {
				fmtSQL = g.sql(fmtE)
			}
		}
	} else {
		fmtSQL = g.sql(fmtE)
	}

	return g.fn("FORMAT", e.Arg("this"), fmtSQL, e.Arg("culture"))
}

// tsqlStringAggSQL mirrors generators/tsql.py _string_agg_sql.
func tsqlStringAggSQL(g *Generator, e *Expr) string {
	this := e.This()
	distinct := e.Find(KDistinct)
	if distinct != nil {
		// exp.Distinct can appear below an exp.Order or an exp.GroupConcat expression
		g.unsupported("T-SQL STRING_AGG doesn't support DISTINCT.")
		this = distinct.Pop().Expressions()[0]
	}

	order := ""
	if e.This().IsA(KOrder) {
		if e.This().This() != nil {
			this = e.This().This().Pop()
		}
		// Order has a leading space
		order = " WITHIN GROUP (" + string([]rune(g.sql(e.Arg("this")))[1:]) + ")"
	}

	separator := e.ArgE("separator")
	if separator == nil {
		separator = LiteralString(",")
	}
	return "STRING_AGG(" + g.formatArgs(", ", this, separator) + ")" + order
}

// QualifyDerivedTableOutputsTSQL mirrors generators/tsql.py qualify_derived_table_outputs:
// ensures all (unnamed) output columns are aliased for CTEs and Subqueries.
func QualifyDerivedTableOutputsTSQL(expression *Expr) *Expr {
	alias := expression.ArgE("alias")

	if expression.IsA(KCTE, KSubquery) && alias.IsA(KTableAlias) && len(alias.ArgL("columns")) == 0 {
		// We keep track of the unaliased column projection indexes instead of the expressions
		// themselves, because the latter are going to be replaced by new nodes when the aliases
		// are added and hence we won't be able to reach these newly added Alias parents
		query := expression.This()
		tsqlCheckSelectsAttr(query)
		var unaliasedColumnIndexes []int
		for i, c := range query.Selects() {
			if c.IsA(KColumn) && c.Alias() == "" {
				unaliasedColumnIndexes = append(unaliasedColumnIndexes, i)
			}
		}

		QualifyOutputs(query, tsqlDialect())

		// Preserve the quoting information of columns for newly added Alias nodes
		querySelects := query.Selects()
		for _, selectIndex := range unaliasedColumnIndexes {
			aliasNode := querySelects[selectIndex]
			column := aliasNode.This()
			if column.This().IsA(KIdentifier) {
				aliasNode.ArgE("alias").Set("quoted", column.This().ArgB("quoted"))
			}
		}
	}

	return expression
}

// tsqlCheckSelectsAttr mirrors the Python errors raised by `query.selects` when the node has no
// (implemented) `selects` property (e.g. a ClickHouse `WITH <expr> AS x` CTE or a DuckDB PIVOT).
func tsqlCheckSelectsAttr(query *Expr) {
	if query == nil {
		panic(&ValueError{Msg: "'NoneType' object has no attribute 'selects'"})
	}
	switch propOwner_selects[query.Kind()] {
	case KNone:
		panic(&ValueError{Msg: "'" + query.Kind().Name() + "' object has no attribute 'selects'"})
	case KSelectable:
		panic(&ValueError{Msg: "Subclasses must implement selects"})
	}
}

// tsqlJSONExtractSQL mirrors generators/tsql.py _json_extract_sql.
func tsqlJSONExtractSQL(g *Generator, e *Expr) string {
	jsonQuery := g.fn("JSON_QUERY", e.Arg("this"), e.Arg("expression"))
	jsonValue := g.fn("JSON_VALUE", e.Arg("this"), e.Arg("expression"))
	return g.fn("ISNULL", jsonQuery, jsonValue)
}

// tsqlTimestrtotimeSQL mirrors generators/tsql.py _timestrtotime_sql.
func tsqlTimestrtotimeSQL(g *Generator, e *Expr) string {
	sql := timestrtotimeSQL(g, e, false)
	if e.ArgB("zone") {
		// If there is a timezone, produce an expression like:
		// CAST('2020-01-01 12:13:14-08:00' AS DATETIMEOFFSET) AT TIME ZONE 'UTC'
		// If you dont have AT TIME ZONE 'UTC', wrapping that expression in another cast back to DATETIME2 just drops the timezone information
		return g.sql(New(KAtTimeZone, "this", sql, "zone", LiteralString("UTC")))
	}
	return sql
}

// tsqlTableName mirrors exp.table_name(table) (default dialect, identify=False).
func tsqlTableName(table *Expr) string {
	parts := table.Parts()
	if len(parts) == 0 {
		panic(&ValueError{Msg: "Cannot parse " + exprSQL(table)})
	}
	out := make([]string, len(parts))
	for i, part := range parts {
		if !SAFE_IDENTIFIER_RE.MatchString(part.Name()) {
			sql, err := MustDialect("").NewGenerator(&GenerateOptions{Identify: "true", NoComments: true}).Generate(part, false)
			if err != nil {
				panic(err)
			}
			out[i] = sql
		} else {
			out[i] = part.Name()
		}
	}
	return strings.Join(out, ".")
}

// customizeTSQLGenerator applies the TSQLGenerator class body on top of the base Generator.
func customizeTSQLGenerator(d *Dialect) {
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (no cluster/distribute/sort)
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
	delete(T, KReturnsProperty)
	T[KAnyValue] = anyValueToMaxSQL
	T[KAtan2] = renameFunc("ATN2")
	T[KArrayToString] = renameFunc("STRING_AGG")
	T[KAutoIncrementColumnConstraint] = func(g *Generator, e *Expr) string { return "IDENTITY" }
	T[KCeil] = renameFunc("CEILING")
	T[KChr] = renameFunc("CHAR")
	T[KDateAdd] = dateDeltaSQL("DATEADD", false)
	T[KCTE] = transformPreprocess([]func(*Expr) *Expr{QualifyDerivedTableOutputsTSQL}, nil)
	T[KCurrentDate] = renameFunc("GETDATE")
	T[KCurrentTimestamp] = renameFunc("GETDATE")
	T[KCurrentTimestampLTZ] = renameFunc("SYSDATETIMEOFFSET")
	T[KDateStrToDate] = datestrtodateSQL
	T[KGeneratedAsIdentityColumnConstraint] = generatedasidentitycolumnconstraintSQL
	T[KGroupConcat] = tsqlStringAggSQL
	T[KIf] = renameFunc("IIF")
	T[KJSONExtract] = tsqlJSONExtractSQL
	T[KJSONExtractScalar] = tsqlJSONExtractSQL
	T[KLastDay] = func(g *Generator, e *Expr) string { return g.fn("EOMONTH", e.Arg("this")) }
	T[KLn] = renameFunc("LOG")
	T[KMax] = maxOrGreatest
	T[KMD5] = func(g *Generator, e *Expr) string { return g.fn("HASHBYTES", LiteralString("MD5"), e.Arg("this")) }
	T[KMin] = minOrLeast
	T[KNumberToStr] = tsqlFormatSQL
	T[KRepeat] = renameFunc("REPLICATE")
	T[KCurrentSchema] = renameFunc("SCHEMA_NAME")
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateDistinctOn,
		transformEliminateSemiAndAntiJoins,
		transformEliminateQualify,
		transformUnnestGenerateDateArrayUsingRecursiveCte,
	}, nil)
	T[KStddev] = renameFunc("STDEV")
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "CHARINDEX", true, false, true)
	}
	T[KSubquery] = transformPreprocess([]func(*Expr) *Expr{QualifyDerivedTableOutputsTSQL}, nil)
	T[KSHA] = func(g *Generator, e *Expr) string { return g.fn("HASHBYTES", LiteralString("SHA1"), e.Arg("this")) }
	T[KSHA1Digest] = func(g *Generator, e *Expr) string { return g.fn("HASHBYTES", LiteralString("SHA1"), e.Arg("this")) }
	T[KSHA2] = func(g *Generator, e *Expr) string {
		length := "256"
		if e.HasArgKey("length") {
			length = dhPyStr(e.Arg("length"))
		}
		return g.fn("HASHBYTES", LiteralString("SHA2_"+length), e.Arg("this"))
	}
	T[KTemporaryProperty] = func(g *Generator, e *Expr) string { return "" }
	T[KTimeStrToTime] = tsqlTimestrtotimeSQL
	T[KTimeToStr] = tsqlFormatSQL
	T[KTimestampAdd] = dateDeltaSQL("DATEADD", false)
	T[KTrim] = func(g *Generator, e *Expr) string { return trimSQL(g, e, "") }
	T[KTsOrDsAdd] = dateDeltaSQL("DATEADD", true)
	T[KTsOrDsDiff] = dateDeltaSQL("DATEDIFF", false)
	T[KTimestampTrunc] = func(g *Generator, e *Expr) string { return g.fn("DATETRUNC", e.Arg("unit"), e.Arg("this")) }
	T[KTrunc] = func(g *Generator, e *Expr) string {
		decimals := e.ArgE("decimals")
		if decimals == nil {
			decimals = LiteralInt(0)
		}
		return g.fn("ROUND", e.Arg("this"), decimals, LiteralInt(1))
	}
	T[KUuid] = func(g *Generator, e *Expr) string { return "NEWID()" }
	T[KDateFromParts] = renameFunc("DATEFROMPARTS")

	G.h.scopeResolution = tsqlScopeResolution
	G.h.selectSQL = tsqlSelectSQL
	G.h.convertSQL = tsqlConvertSQL
	G.h.queryoptionSQL = tsqlQueryoptionSQL
	G.h.lateralOp = tsqlLateralOp
	G.h.extractSQL = tsqlExtractSQL
	G.h.setitemSQL = tsqlSetitemSQL
	G.h.booleanSQL = tsqlBooleanSQL
	G.h.isSQL = tsqlIsSQL
	G.h.createableSQL = tsqlCreateableSQL
	G.h.createSQL = tsqlCreateSQL
	G.h.intoSQL = tsqlIntoSQL
	G.h.offsetSQL = tsqlOffsetSQL
	G.h.versionSQL = tsqlVersionSQL
	G.h.returningSQL = tsqlReturningSQL
	G.h.transactionSQL = tsqlTransactionSQL
	G.h.commitSQL = tsqlCommitSQL
	G.h.rollbackSQL = tsqlRollbackSQL
	G.h.identifierSQL = tsqlIdentifierSQL
	G.h.constraintSQL = tsqlConstraintSQL
	G.h.partitionSQL = tsqlPartitionSQL
	G.h.alterSQL = tsqlAlterSQL
	G.h.dropSQL = tsqlDropSQL
	G.h.optionsModifier = tsqlOptionsModifier
	G.h.dpipeSQL = tsqlDpipeSQL
	G.h.columndefSQL = tsqlColumndefSQL
	G.h.storedprocedureSQL = tsqlStoredprocedureSQL
	G.h.ifblockSQL = tsqlIfblockSQL
	G.h.whileblockSQL = tsqlWhileblockSQL
	G.h.executeSQL = tsqlExecuteSQL
	G.h.executesqlSQL = tsqlExecutesqlSQL

	M := G.methods
	M[KSplitPart] = tsqlSplitpartSQL
	M[KTimeFromParts] = tsqlTimefrompartsSQL
	M[KTimestampFromParts] = tsqlTimestampfrompartsSQL
	M[KCount] = tsqlCountSQL
	M[KDateDiff] = tsqlDatediffSQL
	M[KReturnsProperty] = tsqlReturnspropertySQL
	M[KLength] = tsqlLengthSQL
	M[KRight] = tsqlRightSQL
	M[KLeft] = tsqlLeftSQL
	M[KIsAscii] = tsqlIsasciiSQL
	M[KCoalesce] = tsqlCoalesceSQL
}

// tsqlScopeResolution mirrors TSQLGenerator.scope_resolution.
func tsqlScopeResolution(g *Generator, rhs string, scopeName string) string {
	return scopeName + "::" + rhs
}

// tsqlSelectSQL mirrors TSQLGenerator.select_sql.
func tsqlSelectSQL(g *Generator, e *Expr) string {
	limit := e.ArgE("limit")
	offset := e.ArgE("offset")

	if limit.IsA(KFetch) && offset == nil {
		// Dialects like Oracle can FETCH directly from a row set but
		// T-SQL requires an ORDER BY + OFFSET clause in order to FETCH
		offset = New(KOffset, "expression", LiteralInt(0))
		e.Set("offset", offset)
	}

	if offset != nil {
		if !e.ArgB("order") {
			// ORDER BY is required in order to use OFFSET in a query, so we use
			// a noop order by, since we don't really care about the order.
			// See: https://www.microsoftpressstore.com/articles/article.aspx?p=2314819
			e.QueryOrderBy([]*Expr{SelectExpr(Null()).QuerySubquery(nil, true)}, true, false)
		}

		if limit.IsA(KLimit) {
			// TOP and OFFSET can't be combined, we need use FETCH instead of TOP
			// we replace here because otherwise TOP would be generated in select_sql
			limit.Replace(New(KFetch, "direction", "FIRST", "count", limit.Expression()))
		}
	}

	return g.baseSelectSQL(e)
}

// tsqlConvertSQL mirrors TSQLGenerator.convert_sql.
func tsqlConvertSQL(g *Generator, e *Expr) string {
	name := "CONVERT"
	if e.ArgB("safe") {
		name = "TRY_CONVERT"
	}
	return g.fn(name, e.Arg("this"), e.Arg("expression"), e.Arg("style"))
}

// tsqlQueryoptionSQL mirrors TSQLGenerator.queryoption_sql.
func tsqlQueryoptionSQL(g *Generator, e *Expr) string {
	option := g.sqlKey(e, "this")
	value := g.sqlKey(e, "expression")
	if value != "" {
		optionalEqualSign := ""
		if tsqlOPTIONS_THAT_REQUIRE_EQUAL.Has(option) {
			optionalEqualSign = "= "
		}
		return option + " " + optionalEqualSign + value
	}
	return option
}

// tsqlLateralOp mirrors TSQLGenerator.lateral_op.
func tsqlLateralOp(g *Generator, e *Expr) string {
	crossApply := e.Arg("cross_apply")
	if b, ok := crossApply.(bool); ok {
		if b {
			return "CROSS APPLY"
		}
		return "OUTER APPLY"
	}

	// TODO: perhaps we can check if the parent is a Join and transpile it appropriately
	g.unsupported("LATERAL clause is not supported.")
	return "LATERAL"
}

// tsqlSplitpartSQL mirrors TSQLGenerator.splitpart_sql.
func tsqlSplitpartSQL(g *Generator, e *Expr) string {
	this := e.This()
	splitCount := len(strings.Split(this.Name(), "."))
	delimiter := e.ArgE("delimiter")
	partIndex := e.ArgE("part_index")

	if !(this.IsA(KLiteral) && delimiter.IsA(KLiteral) && partIndex.IsA(KLiteral)) ||
		(delimiter != nil && delimiter.Name() != ".") ||
		partIndex == nil ||
		splitCount > 4 {
		g.unsupported("SPLIT_PART can be transpiled to PARSENAME only for '.' delimiter and literal values")
		return ""
	}

	return g.fn("PARSENAME", this, LiteralNumber(tsqlPyNumSub(splitCount+1, partIndex)))
}

// tsqlExtractSQL mirrors TSQLGenerator.extract_sql.
func tsqlExtractSQL(g *Generator, e *Expr) string {
	part := e.This()
	var name any = part
	if v, ok := tsqlDATE_PART_UNMAPPING[pyUpper(part.Name())]; ok && v != "" {
		name = v
	}

	return g.fn("DATEPART", name, e.Arg("expression"))
}

// tsqlTimefrompartsSQL mirrors TSQLGenerator.timefromparts_sql.
func tsqlTimefrompartsSQL(g *Generator, e *Expr) string {
	if nano := e.ArgE("nano"); nano != nil {
		nano.Pop()
		g.unsupported("Specifying nanoseconds is not supported in TIMEFROMPARTS.")
	}

	if e.Arg("fractions") == nil {
		e.Set("fractions", LiteralInt(0))
	}
	if e.Arg("precision") == nil {
		e.Set("precision", LiteralInt(0))
	}

	return renameFunc("TIMEFROMPARTS")(g, e)
}

// tsqlTimestampfrompartsSQL mirrors TSQLGenerator.timestampfromparts_sql.
func tsqlTimestampfrompartsSQL(g *Generator, e *Expr) string {
	if zone := e.ArgE("zone"); zone != nil {
		zone.Pop()
		g.unsupported("Time zone is not supported in DATETIMEFROMPARTS.")
	}

	if nano := e.ArgE("nano"); nano != nil {
		nano.Pop()
		g.unsupported("Specifying nanoseconds is not supported in DATETIMEFROMPARTS.")
	}

	if e.Arg("milli") == nil {
		e.Set("milli", LiteralInt(0))
	}

	return renameFunc("DATETIMEFROMPARTS")(g, e)
}

// tsqlSetitemSQL mirrors TSQLGenerator.setitem_sql.
func tsqlSetitemSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KEQ) && !this.Left().IsA(KParameter) {
		// T-SQL does not use '=' in SET command, except when the LHS is a variable.
		return g.sql(this.Left()) + " " + g.sql(this.Right())
	}

	return g.baseSetitemSQL(e)
}

// tsqlBooleanSQL mirrors TSQLGenerator.boolean_sql.
func tsqlBooleanSQL(g *Generator, e *Expr) string {
	parent := e.Parent()
	if (parent != nil && tsqlBIT_TYPES[parent.Kind()]) || e.FindAncestor(KValues, KSelect).IsA(KValues) {
		if e.ArgB("this") {
			return "1"
		}
		return "0"
	}

	if e.ArgB("this") {
		return "(1 = 1)"
	}
	return "(1 = 0)"
}

// tsqlIsSQL mirrors TSQLGenerator.is_sql.
func tsqlIsSQL(g *Generator, e *Expr) string {
	if e.Expression().IsA(KBoolean) {
		return g.binary(e, "=")
	}
	return g.binary(e, "IS")
}

// tsqlCreateableSQL mirrors TSQLGenerator.createable_sql.
func tsqlCreateableSQL(g *Generator, e *Expr, locations propLocations) string {
	sql := g.sqlKey(e, "this")
	properties := e.ArgE("properties")

	start := g.identifierStart
	hasTemp := false
	if properties != nil {
		for _, prop := range properties.Expressions() {
			if prop.IsA(KTemporaryProperty) {
				hasTemp = true
				break
			}
		}
	}
	if !strings.HasPrefix(sql, "#") && !strings.HasPrefix(sql, start+"#") && hasTemp {
		if strings.HasPrefix(sql, start) {
			sql = start + "#" + sql[len(start):]
		} else {
			sql = "#" + sql
		}
	}

	return sql
}

// tsqlCreateSQL mirrors TSQLGenerator.create_sql.
func tsqlCreateSQL(g *Generator, e *Expr) string {
	kind := e.KindText()
	exists := e.ArgB("exists")
	e.Set("exists", nil)

	likeProperty := e.Find(KLikeProperty)
	var ctasExpression *Expr
	if likeProperty != nil {
		ctasExpression = likeProperty.This()
	} else {
		ctasExpression = e.Expression()
	}

	if kind == "VIEW" {
		e.This().Set("catalog", nil)
		with := e.ArgE("with_")
		if ctasExpression != nil && with != nil {
			// We've already preprocessed the Create expression to bubble up any nested CTEs,
			// but CREATE VIEW actually requires the WITH clause to come after it so we need
			// to amend the AST by moving the CTEs to the CREATE VIEW statement's query.
			ctasExpression.Set("with_", with.Pop())
		}
	} else if kind == "FUNCTION" && ctasExpression.IsA(KReturn) {
		if body := ctasExpression.This().Unnest(); body.IsA(KQuery) {
			if with := e.ArgE("with_"); with != nil {
				// Similar to the VIEW branch, the table-valued functions require the WITH clause
				// to stay inside the RETURN body, so we move back any CTEs that were bubbled up.
				body.Set("with_", with.Pop())
			}
		}
	}

	table := e.Find(KTable)

	var sql string
	// Convert CTAS statement to SELECT .. INTO ..
	if kind == "TABLE" && ctasExpression != nil {
		if ctasExpression.IsA(KSelect, KSetOperation) {
			ctasExpression = ctasExpression.QuerySubquery(nil, true)
		}

		properties := e.ArgE("properties")
		if properties == nil {
			properties = New(KProperties)
		}
		isTemp := false
		for _, p := range properties.Expressions() {
			if p.IsA(KTemporaryProperty) {
				isTemp = true
				break
			}
		}

		selectInto := SelectExpr(Star()).SelectFrom(AliasTableExpr(ctasExpression, "temp", nil, nil, true), true)
		selectInto.Set("into", New(KInto, "this", table, "temporary", isTemp))

		if likeProperty != nil {
			selectInto.QueryLimit(LiteralInt(0), false)
		}

		sql = g.sql(selectInto)
	} else {
		sql = g.baseCreateSQL(e)
	}

	if exists {
		tableName := ""
		if table != nil {
			tableName = tsqlTableName(table)
		}
		identifier := g.sql(LiteralString(tableName))
		sqlWithCtes := g.prependCtes(e, sql)
		sqlLiteral := g.sql(LiteralString(sqlWithCtes))
		if kind == "SCHEMA" {
			return "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = " + identifier + ") EXEC(" + sqlLiteral + ")"
		} else if kind == "TABLE" {
			var schemaCond, catalogCond *Expr
			if table.DbName() != "" {
				schemaCond = New(KEQ, "this", ColumnExpr("TABLE_SCHEMA", nil, nil, nil, nil, nil, true), "expression", LiteralString(table.DbName()))
			}
			if table.CatalogName() != "" {
				catalogCond = New(KEQ, "this", ColumnExpr("TABLE_CATALOG", nil, nil, nil, nil, nil, true), "expression", LiteralString(table.CatalogName()))
			}
			where := AndExpr(
				New(KEQ, "this", ColumnExpr("TABLE_NAME", nil, nil, nil, nil, nil, true), "expression", LiteralString(table.Name())),
				schemaCond,
				catalogCond,
			)
			return "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE " + exprSQL(where) + ") EXEC(" + sqlLiteral + ")"
		} else if kind == "INDEX" {
			index := g.sql(LiteralString(e.This().Text("this")))
			return "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id(" + identifier + ") AND name = " + index + ") EXEC(" + sqlLiteral + ")"
		}
	} else if e.ArgB("replace") {
		sql = strings.Replace(sql, "CREATE OR REPLACE ", "CREATE OR ALTER ", 1)
	}

	return g.prependCtes(e, sql)
}

// tsqlIntoSQL mirrors TSQLGenerator.into_sql.
func tsqlIntoSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "unlogged", "expressions")
	if e.ArgB("temporary") {
		// If the Into expression has a temporary property, push this down to the Identifier
		table := e.Find(KTable)
		if table != nil && table.This().IsA(KIdentifier) {
			table.This().Set("temporary", true)
		}
	}

	return g.seg("INTO") + " " + g.sqlKey(e, "this")
}

// tsqlCountSQL mirrors TSQLGenerator.count_sql.
func tsqlCountSQL(g *Generator, e *Expr) string {
	funcName := "COUNT"
	if e.ArgB("big_int") {
		funcName = "COUNT_BIG"
	}
	return renameFunc(funcName)(g, e)
}

// tsqlDatediffSQL mirrors TSQLGenerator.datediff_sql.
func tsqlDatediffSQL(g *Generator, e *Expr) string {
	funcName := "DATEDIFF"
	if e.ArgB("big_int") {
		funcName = "DATEDIFF_BIG"
	}
	return dateDeltaSQL(funcName, false)(g, e)
}

// tsqlOffsetSQL mirrors TSQLGenerator.offset_sql.
func tsqlOffsetSQL(g *Generator, e *Expr) string {
	return g.baseOffsetSQL(e) + " ROWS"
}

// tsqlVersionSQL mirrors TSQLGenerator.version_sql.
func tsqlVersionSQL(g *Generator, e *Expr) string {
	name := e.Name()
	if name == "TIMESTAMP" {
		name = "SYSTEM_TIME"
	}
	this := "FOR " + name
	expr := e.Expression()
	kind := e.Text("kind")
	var exprSQLs string
	if kind == "FROM" || kind == "BETWEEN" {
		args := expr.Expressions()
		sep := "AND"
		if kind == "FROM" {
			sep = "TO"
		}
		exprSQLs = g.sql(seqGet(args, 0)) + " " + sep + " " + g.sql(seqGet(args, 1))
	} else {
		exprSQLs = g.sql(expr)
	}

	if exprSQLs != "" {
		exprSQLs = " " + exprSQLs
	}
	return this + " " + kind + exprSQLs
}

// tsqlReturnspropertySQL mirrors TSQLGenerator.returnsproperty_sql.
func tsqlReturnspropertySQL(g *Generator, e *Expr) string {
	table := ""
	if t := e.Arg("table"); truthy(t) {
		table = dhPyStr(t) + " "
	}
	return "RETURNS " + table + g.sqlKey(e, "this")
}

// tsqlReturningSQL mirrors TSQLGenerator.returning_sql.
func tsqlReturningSQL(g *Generator, e *Expr) string {
	into := g.sqlKey(e, "into")
	if into != "" {
		into = g.seg("INTO " + into)
	}
	return g.seg("OUTPUT") + " " + g.expressions(e, exprsOpts{flat: true}) + into
}

// tsqlTransactionSQL mirrors TSQLGenerator.transaction_sql.
func tsqlTransactionSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	if this != "" {
		this = " " + this
	}
	mark := g.sqlKey(e, "mark")
	if mark != "" {
		mark = " WITH MARK " + mark
	}
	return "BEGIN TRANSACTION" + this + mark
}

// tsqlCommitSQL mirrors TSQLGenerator.commit_sql.
func tsqlCommitSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	if this != "" {
		this = " " + this
	}
	durabilityV := e.Arg("durability")
	durability := ""
	if durabilityV != nil {
		if truthy(durabilityV) {
			durability = " WITH (DELAYED_DURABILITY = ON)"
		} else {
			durability = " WITH (DELAYED_DURABILITY = OFF)"
		}
	}
	return "COMMIT TRANSACTION" + this + durability
}

// tsqlRollbackSQL mirrors TSQLGenerator.rollback_sql.
func tsqlRollbackSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	if this != "" {
		this = " " + this
	}
	return "ROLLBACK TRANSACTION" + this
}

// tsqlIdentifierSQL mirrors TSQLGenerator.identifier_sql.
func tsqlIdentifierSQL(g *Generator, e *Expr) string {
	identifier := g.baseIdentifierSQL(e)

	var prefix string
	if e.ArgB("global_") {
		prefix = "##"
	} else if e.ArgB("temporary") {
		prefix = "#"
	} else {
		return identifier
	}

	start := g.identifierStart
	if e.ArgB("quoted") && strings.HasPrefix(identifier, start) {
		return start + prefix + identifier[len(start):]
	}

	return prefix + identifier
}

// tsqlConstraintSQL mirrors TSQLGenerator.constraint_sql.
func tsqlConstraintSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	expressions := g.expressions(e, exprsOpts{flat: true, sep: strp2(" ")})
	return "CONSTRAINT " + this + " " + expressions
}

// tsqlLengthSQL mirrors TSQLGenerator.length_sql.
func tsqlLengthSQL(g *Generator, e *Expr) string { return tsqlUncastText(g, e, "LEN") }

// tsqlRightSQL mirrors TSQLGenerator.right_sql.
func tsqlRightSQL(g *Generator, e *Expr) string { return tsqlUncastText(g, e, "RIGHT") }

// tsqlLeftSQL mirrors TSQLGenerator.left_sql.
func tsqlLeftSQL(g *Generator, e *Expr) string { return tsqlUncastText(g, e, "LEFT") }

// tsqlUncastText mirrors TSQLGenerator._uncast_text.
func tsqlUncastText(g *Generator, e *Expr, name string) string {
	this := e.This()
	var thisSQL string
	if this.IsA(KCast) && DataTypeIsType(this.ArgE("to"), []any{DT_TEXT}, false) {
		thisSQL = g.sqlKey(this, "this")
	} else {
		thisSQL = g.sql(this)
	}
	expressionSQL := g.sqlKey(e, "expression")
	var expressionArg any
	if expressionSQL != "" {
		expressionArg = expressionSQL
	}
	return g.fn(name, thisSQL, expressionArg)
}

// tsqlPartitionSQL mirrors TSQLGenerator.partition_sql.
func tsqlPartitionSQL(g *Generator, e *Expr) string {
	return "WITH (PARTITIONS(" + g.expressions(e, exprsOpts{flat: true}) + "))"
}

// tsqlAlterSQL mirrors TSQLGenerator.alter_sql.
func tsqlAlterSQL(g *Generator, e *Expr) string {
	action := seqGet(e.ArgL("actions"), 0)
	if action.IsA(KAlterRename) {
		return "EXEC sp_rename '" + g.sql(e.Arg("this")) + "', '" + action.This().Name() + "'"
	}
	return g.baseAlterSQL(e)
}

// tsqlDropSQL mirrors TSQLGenerator.drop_sql.
func tsqlDropSQL(g *Generator, e *Expr) string {
	if s, ok := e.Arg("kind").(string); ok && s == "VIEW" {
		e.This().Set("catalog", nil)
	}
	return g.baseDropSQL(e)
}

// tsqlOptionsModifier mirrors TSQLGenerator.options_modifier.
func tsqlOptionsModifier(g *Generator, e *Expr) string {
	options := g.expressions(e, exprsOpts{key: "options"})
	if options != "" {
		return " OPTION" + g.wrap(options)
	}
	return ""
}

// tsqlDpipeSQL mirrors TSQLGenerator.dpipe_sql.
func tsqlDpipeSQL(g *Generator, e *Expr) string {
	flat := e.Flatten(true)
	var acc *Expr
	for i, y := range flat {
		if i == 0 {
			acc = y
			continue
		}
		acc = New(KAdd, "this", acc, "expression", y)
	}
	return g.sql(acc)
}

// tsqlIsasciiSQL mirrors TSQLGenerator.isascii_sql.
func tsqlIsasciiSQL(g *Generator, e *Expr) string {
	return "(PATINDEX(CONVERT(VARCHAR(MAX), 0x255b5e002d7f5d25) COLLATE Latin1_General_BIN, " + g.sql(e.Arg("this")) + ") = 0)"
}

// tsqlColumndefSQL mirrors TSQLGenerator.columndef_sql.
func tsqlColumndefSQL(g *Generator, e *Expr, sep string) string {
	this := g.baseColumndefSQL(e, sep)
	def := g.sqlKey(e, "default")
	if def != "" {
		def = " = " + def
	}
	output := g.sqlKey(e, "output")
	if output != "" {
		output = " " + output
	}
	return this + def + output
}

// tsqlCoalesceSQL mirrors TSQLGenerator.coalesce_sql.
func tsqlCoalesceSQL(g *Generator, e *Expr) string {
	funcName := "COALESCE"
	if e.ArgB("is_null") {
		funcName = "ISNULL"
	}
	return renameFunc(funcName)(g, e)
}

// tsqlStoredprocedureSQL mirrors TSQLGenerator.storedprocedure_sql.
func tsqlStoredprocedureSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	expressions := g.expressions(e, exprsOpts{})
	if e.ArgB("wrapped") {
		expressions = g.wrap(expressions)
	} else {
		expressions = " " + expressions
	}
	if pyStrip(expressions) != "" {
		return this + expressions
	}
	return this
}

// tsqlIfblockSQL mirrors TSQLGenerator.ifblock_sql.
func tsqlIfblockSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	trueS := g.sqlKey(e, "true")
	if trueS != "" {
		trueS = " " + trueS
	} else {
		trueS = " "
	}
	falseS := g.sqlKey(e, "false")
	if falseS != "" {
		falseS = "; ELSE BEGIN " + falseS
	}
	return "IF " + this + " BEGIN" + trueS + falseS
}

// tsqlWhileblockSQL mirrors TSQLGenerator.whileblock_sql.
func tsqlWhileblockSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	body := g.sqlKey(e, "body")
	if body != "" {
		body = " " + body
	} else {
		body = " "
	}
	return "WHILE " + this + " BEGIN" + body
}

// tsqlExecuteSQL mirrors TSQLGenerator.execute_sql.
func tsqlExecuteSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	expressions := g.expressions(e, exprsOpts{})
	if expressions != "" {
		expressions = " " + expressions
	}
	returnStatus := g.sqlKey(e, "return_status")
	if returnStatus != "" {
		returnStatus = returnStatus + " = "
	}
	return "EXECUTE " + returnStatus + this + expressions
}

// tsqlExecutesqlSQL mirrors TSQLGenerator.executesql_sql.
func tsqlExecutesqlSQL(g *Generator, e *Expr) string {
	return g.executeSQL(e)
}
