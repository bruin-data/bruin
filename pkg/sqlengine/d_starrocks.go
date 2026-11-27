package sqlengine

// StarRocks builds on the MySQL dialect, so only its differences from MySQL are applied here.

import "strings"

func init() {
	registerCustomizer("starrocks", customizeStarRocks)
	mysqlPartitionPropertyImpls["starrocks"] = starrocksParsePartitionProperty
}

func customizeStarRocks(d *Dialect) {
	P := d.P

	P.FUNCTIONS["ADDDATE"] = buildDateDeltaWithInterval(KDateAdd, "DAY")
	P.FUNCTIONS["DATE_ADD"] = buildDateDeltaWithInterval(KDateAdd, "DAY")
	P.FUNCTIONS["DATE_SUB"] = buildDateDeltaWithInterval(KDateSub, "DAY")
	P.FUNCTIONS["SUBDATE"] = buildDateDeltaWithInterval(KDateSub, "DAY")
	P.FUNCTIONS["DATE_TRUNC"] = buildTimestampTrunc
	P.FUNCTIONS["DATE_DIFF"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDateDiff, "this", seqGet(args, 1), "expression", seqGet(args, 2), "unit", seqGet(args, 0))
	}
	P.FUNCTIONS["ARRAY_FLATTEN"] = fromArgList(KFlatten)
	P.FUNCTIONS["LIKE"] = dialectBuildLike(KLike, false)
	P.FUNCTIONS["TO_DAYS"] = fromArgList(KToDays)
	P.FUNCTIONS["REGEXP"] = fromArgList(KRegexpLike)
	// StarRocks' MAP() is a variadic constructor: MAP(k1, v1, k2, v2, ...)
	// https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map/
	P.FUNCTIONS["MAP"] = func(args []*Expr, d *Dialect) *Expr { return buildVarMap(args) }
	P.FUNCTIONS["TIME_SLICE"] = starrocksBuildTimeSlice
	// TABLE(<tvf>) wraps a table function invocation whose arguments are constants
	// https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/generate_series/
	P.FUNCTIONS["TABLE"] = func(args []*Expr, _ *Dialect) *Expr { return New(KTableFromRows, "this", seqGet(args, 0)) }

	P.PROPERTY_PARSERS["PROPERTIES"] = noKwargs(func(p *Parser) any {
		return p.parseWrappedProperties()
	})
	P.PROPERTY_PARSERS["AGGREGATE"] = noKwargs(func(p *Parser) any {
		return anyExpr(p.parseCompositeKeyProperty(KAggregateKeyProperty))
	})
	P.PROPERTY_PARSERS["UNIQUE"] = noKwargs(func(p *Parser) any {
		return anyExpr(p.parseCompositeKeyProperty(KUniqueKeyProperty))
	})
	P.PROPERTY_PARSERS["ROLLUP"] = noKwargs(func(p *Parser) any {
		return anyExpr(starrocksParseRollupProperty(p))
	})
	P.PROPERTY_PARSERS["REFRESH"] = noKwargs(func(p *Parser) any {
		return anyExpr(starrocksParseRefreshProperty(p))
	})

	P.STATEMENT_PARSERS[TK_REFRESH] = starrocksParseRefresh

	// The aggregation type of a value column in an AGGREGATE KEY table, e.g. pv BIGINT SUM
	// https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/
	for _, name := range []string{"SUM", "MAX", "MIN", "REPLACE", "REPLACE_IF_NOT_NULL", "HLL_UNION"} {
		P.CONSTRAINT_PARSERS[name] = func(p *Parser) *Expr { return VarExpr(upperText(p.prev)) }
	}

	P.h.parseCreate = starrocksParseCreate
	P.h.parseUnnest = starrocksParseUnnest
	P.h.parsePartitionedBy = starrocksParsePartitionedBy
	P.h.parseTable = starrocksParseTable
	P.h.parseBracket = starrocksParseBracket

	customizeStarRocksGenerator(d)
}

// starrocksParseBracket parses a map{key: value, ...} literal as a map rather than a struct; map
// stays an identifier elsewhere.
// https://docs.starrocks.io/docs/sql-reference/data-types/semi_structured/Map/
func starrocksParseBracket(p *Parser, this *Expr) *Expr {
	isMapLiteral := this.IsA(KColumn) && this.Text("table") == "" && pyUpper(this.Name()) == "MAP" &&
		p.matchNoAdvance(TK_L_BRACE)

	bracket := p.baseParseBracket(this)
	if !isMapLiteral || bracket == nil {
		return bracket
	}

	// Wrap the literal itself, not any subscripts that follow it, e.g. map{...}[1]
	literal := bracket
	for literal.IsA(KBracket) {
		literal = literal.This()
	}

	toMap := p.expression(New(KToMap, "this", literal.Copy()))
	if literal == bracket {
		return toMap
	}

	literal.Replace(toMap)
	return bracket
}

// starrocksParseRefresh reads the materialized view name as bare table parts, since a table would
// take PARTITION and FORCE as a partition selection and an index hint.
// https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW/
func starrocksParseRefresh(p *Parser) *Expr {
	if !p.matchTextSeq("MATERIALIZED", "VIEW") {
		return p.parseRefresh()
	}

	this := p.parseTableParts(false, false, false, false)

	var partitionStart, partitionEnd *Expr
	if p.matchTextSeq("PARTITION", "START") {
		partitionStart = p.parseWrapped(p.parseString, false)
		p.matchTextSeq("END")
		partitionEnd = p.parseWrapped(p.parseString, false)
	}

	force := p.matchTextSeq("FORCE")
	var mode any
	if p.matchTextSeq("WITH", "SYNC", "MODE") {
		mode = "SYNC"
	} else if p.matchTextSeq("WITH", "ASYNC", "MODE") {
		mode = "ASYNC"
	}

	return p.expression(New(
		KRefresh,
		"this", this,
		"kind", "MATERIALIZED VIEW",
		"partition_start", partitionStart,
		"partition_end", partitionEnd,
		"force", force,
		"mode", mode,
	))
}

// starrocksBuildTimeSlice builds TIME_SLICE(dt, INTERVAL n unit [, FLOOR | CEIL]).
// https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/time_slice/
func starrocksBuildTimeSlice(args []*Expr, d *Dialect) *Expr {
	if len(args) < 2 {
		return nil
	}

	var timeSlice, boundary *Expr
	if args[1].IsA(KInterval) {
		timeSlice = buildDateDeltaWithInterval(KTimeSlice, "")(args, d)
		boundary = seqGet(args, 2)
	} else {
		timeSlice = FromArgList(KTimeSlice, args)
		boundary = seqGet(args, 3)
	}
	if timeSlice == nil {
		return nil
	}

	// StarRocks rejects a quoted slice length
	if amount := timeSlice.Expression(); amount.IsString() && pyIntParse(amount.Name(), 10) {
		timeSlice.Set("expression", LiteralNumber(amount.Name()))
	}

	if boundary != nil {
		kind := "START"
		if pyUpper(boundary.Name()) == "CEIL" {
			kind = "END"
		}
		timeSlice.Set("kind", LiteralString(kind))
	}

	// StarRocks anchors slices at 0001-01-01, unlike Snowflake's 1970-01-01 epoch
	timeSlice.Set("origin", LiteralString("0001-01-01"))
	return timeSlice
}

func starrocksTimesliceSQL(g *Generator, e *Expr) string {
	interval := New(KInterval, "this", e.Expression(), "unit", e.ArgE("unit"))
	var boundary *Expr
	if kind := pyUpper(e.Text("kind")); kind != "" {
		if kind == "END" {
			boundary = VarExpr("CEIL")
		} else {
			boundary = VarExpr("FLOOR")
		}
	}
	return g.fn("TIME_SLICE", e.This(), interval, boundary)
}

func starrocksParseRollupProperty(p *Parser) *Expr {
	parseRollupIndex := func() *Expr {
		this := p.parseIdVar(true, nil)
		expressions := p.parseWrappedIdVars(false)
		var fromIndex *Expr
		if p.matchTextSeq("FROM") {
			fromIndex = p.parseIdVar(true, nil)
		}
		var properties *Expr
		if p.matchTextSeq("PROPERTIES") {
			properties = p.expression(New(KProperties, "expressions", p.parseWrappedProperties()))
		}
		return p.expression(New(
			KRollupIndex,
			"this", this,
			"expressions", expressions,
			"from_index", fromIndex,
			"properties", properties,
		))
	}

	return p.expression(New(KRollupProperty, "expressions", p.parseWrappedCSV(parseRollupIndex, TK_COMMA, false)))
}

func starrocksParseCreate(p *Parser) *Expr {
	create := p.baseParseCreate()

	// Starrocks' primary key is defined outside of the schema, so we need to move it there
	// https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/#usage
	if create.IsA(KCreate) && create.This().IsA(KSchema) {
		props := create.ArgE("properties")
		if props != nil {
			primaryKey := props.Find(KPrimaryKey)
			if primaryKey != nil {
				create.This().Append("expressions", primaryKey.Pop())
			}
		}
	}

	return create
}

func starrocksParseTable(p *Parser, schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr {
	table := p.baseParseTable(schema, joins, aliasTokens, parseBracket, isDbReference, parsePartition, consumePipe)
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

func starrocksParseUnnest(p *Parser, withAlias bool) *Expr {
	unnest := p.baseParseUnnest(withAlias)

	if unnest != nil {
		alias := unnest.ArgE("alias")

		if alias == nil {
			// Starrocks defaults to naming the table alias as "unnest"
			alias = New(KTableAlias, "this", ToIdentifier("unnest", nil))
			unnest.Set("alias", alias)
		}

		// Starrocks defaults to naming the UNNEST column as "unnest" if it's not otherwise
		// specified. With several arrays every column is named "unnest", so none is added.
		if !alias.ArgB("columns") && len(unnest.Expressions()) == 1 {
			alias.Set("columns", []*Expr{ToIdentifier("unnest", nil)})
		}
	}

	return unnest
}

func starrocksParsePartitionedBy(p *Parser) *Expr {
	return p.expression(New(
		KPartitionedByProperty,
		"this", New(
			KSchema,
			"expressions", p.parseWrappedCSV(func() *Expr { return p.parseAssignment() }, TK_COMMA, true),
		),
	))
}

func starrocksParsePartitionProperty(p *Parser) any {
	expr := mysqlParsePartitionProperty(p)

	if !truthy(expr) {
		return anyExpr(p.parsePartitionedBy())
	}

	if e, ok := expr.(*Expr); ok && e.IsA(KProperty) {
		return e
	}

	p.matchLParen(nil)

	var createExpressions any
	if p.matchTextSeqNoAdvance("START") {
		createExpressions = p.parseCSV(func() *Expr { return starrocksParsePartitioningGranularityDynamic(p) }, TK_COMMA)
	}

	p.matchRParen(nil)

	return p.expression(New(
		KPartitionByRangeProperty,
		"partition_expressions", expr,
		"create_expressions", createExpressions,
	))
}

func starrocksParsePartitioningGranularityDynamic(p *Parser) *Expr {
	p.matchTextSeq("START")
	start := p.parseWrapped(func() *Expr { return p.parseString() }, false)
	p.matchTextSeq("END")
	end := p.parseWrapped(func() *Expr { return p.parseString() }, false)
	p.matchTextSeq("EVERY")
	every := p.parseWrapped(func() *Expr {
		if e := p.parseInterval(true); e != nil {
			return e
		}
		return p.parseNumber()
	}, false)
	return p.expression(New(KPartitionByRangePropertyDynamic, "start", start, "end", end, "every", every))
}

func starrocksParseRefreshProperty(p *Parser) *Expr {
	var method any = false
	if p.matchTexts("DEFERRED", "IMMEDIATE") {
		method = upperText(p.prev)
	}
	var kind any = false
	if p.matchTexts("ASYNC", "MANUAL") {
		kind = upperText(p.prev)
	}
	var start any = false
	if p.matchTextSeq("START") {
		start = anyExpr(p.parseWrapped(func() *Expr { return p.parseString() }, false))
	}

	var every, unit *Expr
	if p.matchTextSeq("EVERY") {
		p.matchLParen(nil)
		p.matchTextSeq("INTERVAL")
		every = p.parseNumber()
		unit = p.parseVar(true, nil, false)
		p.matchRParen(nil)
	}

	return p.expression(New(
		KRefreshTriggerProperty,
		"method", method,
		"kind", kind,
		"starts", start,
		"every", every,
		"unit", unit,
	))
}

// starrocksEliminateBetweenInDelete rewrites BETWEEN in the WHERE clause of a DELETE as explicit
// comparisons, because StarRocks doesn't support BETWEEN in DELETE statements.
func starrocksEliminateBetweenInDelete(expression *Expr) *Expr {
	if where := expression.ArgE("where"); where != nil {
		for between := range where.FindAll(KBetween) {
			between.Replace(AndExprOpts([]*Expr{
				New(KGTE, "this", between.This().Copy(), "expression", between.Arg("low")),
				New(KLTE, "this", between.This().Copy(), "expression", between.Arg("high")),
			}, false, true))
		}
	}
	return expression
}

// starrocksStDistanceSphere renders StDistance as ST_Distance_Sphere(x1, y1, x2, y2).
// https://docs.starrocks.io/docs/sql-reference/sql-functions/spatial-functions/st_distance_sphere/
func starrocksStDistanceSphere(g *Generator, e *Expr) string {
	point1 := e.Arg("this")
	point2 := e.Arg("expression")

	point1X := g.fn("ST_X", point1)
	point1Y := g.fn("ST_Y", point1)
	point2X := g.fn("ST_X", point2)
	point2Y := g.fn("ST_Y", point2)

	return g.fn("ST_Distance_Sphere", point1X, point1Y, point2X, point2Y)
}

func starrocksTomapSQL(g *Generator, e *Expr) string {
	literal := e.This()
	if !literal.IsA(KStruct) {
		return "MAP " + g.sql(literal)
	}

	entries := make([]string, 0, len(literal.Expressions()))
	for _, entry := range literal.Expressions() {
		entries = append(entries, g.sqlKey(entry, "this")+":"+g.sqlKey(entry, "expression"))
	}
	return "MAP{" + strings.Join(entries, ", ") + "}"
}

func customizeStarRocksGenerator(d *Dialect) {
	G := d.G
	T := G.TRANSFORMS

	// StarRocks uses the native TRIM(str, chars)/LTRIM/RTRIM function form, not
	// MySQL's TRIM(chars FROM str) syntax, so it falls back to the base generator.
	// DATEDIFF and PIVOT have StarRocks render methods instead of MySQL's transforms.
	delete(T, KDateDiff)
	delete(T, KDateTrunc)
	delete(T, KPivot)
	delete(T, KTrim)

	T[KArgMax] = renameFunc("MAX_BY")
	T[KTimeSlice] = starrocksTimesliceSQL
	T[KArgMin] = renameFunc("MIN_BY")
	T[KArray] = inlineArraySQL
	T[KArrayAgg] = renameFunc("ARRAY_AGG")
	// a <@ b (ArrayContainedBy) is equivalent to ARRAY_CONTAINS_ALL(b, a)
	T[KArrayContainedBy] = func(g *Generator, e *Expr) string {
		return g.fn("ARRAY_CONTAINS_ALL", e.Arg("expression"), e.Arg("this"))
	}
	T[KArrayContainsAll] = renameFunc("ARRAY_CONTAINS_ALL")
	T[KArrayFilter] = renameFunc("ARRAY_FILTER")
	T[KArrayToString] = renameFunc("ARRAY_JOIN")
	T[KApproxDistinct] = approxCountDistinctSQL
	T[KCurrentVersion] = func(g *Generator, e *Expr) string { return "CURRENT_VERSION()" }
	T[KDelete] = transformPreprocess([]func(*Expr) *Expr{starrocksEliminateBetweenInDelete}, nil)
	T[KFlatten] = renameFunc("ARRAY_FLATTEN")
	T[KJSONExtractScalar] = arrowJSONExtractSQL
	T[KJSONExtract] = arrowJSONExtractSQL
	// Both MAP forms (two-array MAP([keys], [values]) and variadic MAP(k1, v1, ...))
	// generate StarRocks' variadic MAP(k1, v1, k2, v2, ...) constructor
	T[KMap] = func(g *Generator, e *Expr) string { return varMapSQL(g, e, "MAP") }
	T[KToMap] = starrocksTomapSQL
	T[KProperty] = propertySQL
	T[KRegexpLike] = renameFunc("REGEXP")
	// Inherited from MySQL, minus operations StarRocks supports natively
	// (QUALIFY, FULL OUTER JOIN, SEMI/ANTI JOIN)
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateDistinctOn,
		transformUnnestGenerateDateArrayUsingRecursiveCte,
	}, nil)
	T[KSchemaCommentProperty] = func(g *Generator, e *Expr) string { return g.nakedProperty(e) }
	T[KSqlSecurityProperty] = func(g *Generator, e *Expr) string { return "SECURITY " + g.sql(e.Arg("this")) }
	T[KStDistance] = starrocksStDistanceSphere
	T[KStrToUnix] = func(g *Generator, e *Expr) string {
		return g.fn("UNIX_TIMESTAMP", e.Arg("this"), mysqlFormatTimeArg(g, e))
	}
	T[KTimestampTrunc] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_TRUNC", unitToStr(e, "DAY"), e.Arg("this"))
	}
	T[KTimeStrToDate] = renameFunc("TO_DATE")
	T[KUnixToStr] = func(g *Generator, e *Expr) string {
		return g.fn("FROM_UNIXTIME", e.Arg("this"), mysqlFormatTimeArg(g, e))
	}
	T[KUnixToTime] = renameFunc("FROM_UNIXTIME")
	T[KVarMap] = func(g *Generator, e *Expr) string { return varMapSQL(g, e, "MAP") }

	G.h.createSQL = starrocksCreateSQL
	G.h.ignorenullsSQL = starrocksIgnorenullsSQL
	// RESPECT NULLS is the default behavior, and StarRocks rejects the keyword
	G.h.respectnullsSQL = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }
	G.h.clusterpropertySQL = starrocksClusterpropertySQL
	G.h.refreshtriggerpropertySQL = starrocksRefreshtriggerpropertySQL
	G.methods[KPartitionedByProperty] = starrocksPartitionedbypropertySQL
	G.methods[KRefresh] = starrocksRefreshSQL
	G.methods[KDateDiff] = starrocksDatediffSQL
	G.methods[KPivot] = starrocksPivotSQL
}

// starrocksDatediffSQL renders a day difference over date parts as DATEDIFF(a, b) and any other
// difference as DATE_DIFF(unit, a, b), which counts elapsed units.
// https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/datediff/
func starrocksDatediffSQL(g *Generator, e *Expr) string {
	unit := e.ArgE("unit")
	if e.ArgB("date_part_boundary") && (unit == nil || pyUpper(unit.Name()) == "DAY") {
		return g.fn("DATEDIFF", e.This(), e.Expression())
	}
	return g.fn("DATE_DIFF", unitToStr(e, "DAY"), e.This(), e.Expression())
}

// starrocksPivotSQL renders PIVOT (...); UNPIVOT and the PIVOT statement are unsupported.
// https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT_PIVOT/
func starrocksPivotSQL(g *Generator, e *Expr) string {
	if e.ArgB("unpivot") || e.ArgB("this") {
		return noPivotSQL(g, e)
	}
	return g.pivotSQL(e)
}

// starrocksIgnorenullsSQL renders IGNORE NULLS right after the first argument of FIRST_VALUE,
// LAST_VALUE, LAG and LEAD, e.g. LAG(x IGNORE NULLS, 1, 0); elsewhere it is unsupported.
// https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/
func starrocksIgnorenullsSQL(g *Generator, e *Expr) string {
	fn := e.This()
	if fn.IsA(KFirstValue, KLastValue, KLag, KLead) {
		arg := g.baseIgnorenullsSQL(New(KIgnoreNulls, "this", fn.This().Copy()))
		return g.fn(fn.Kind().SQLName(), arg, fn.Arg("offset"), fn.Arg("default"))
	}
	return mysqlIgnorenullsSQL(g, e)
}

func starrocksRefreshSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	kind := ""
	if !e.This().IsA(KLiteral) {
		kind = e.Text("kind") + " "
	}
	partition := ""
	if partitionStart := g.sqlKey(e, "partition_start"); partitionStart != "" {
		partition = " PARTITION START (" + partitionStart + ") END (" + g.sqlKey(e, "partition_end") + ")"
	}
	force := ""
	if e.ArgB("force") {
		force = " FORCE"
	}
	mode := g.sqlKey(e, "mode")
	if mode != "" {
		mode = " WITH " + mode + " MODE"
	}
	return "REFRESH " + kind + this + partition + force + mode
}

func starrocksCreateSQL(g *Generator, e *Expr) string {
	// Starrocks' primary key is defined outside of the schema, so we need to move it there
	schema := e.This()
	if schema.IsA(KSchema) {
		primaryKey := schema.Find(KPrimaryKey)

		if primaryKey != nil {
			props := e.ArgE("properties")

			if props == nil {
				props = New(KProperties, "expressions", []*Expr{})
				e.Set("properties", props)
			}

			// Insert after the ENGINE property if there is one, otherwise at the beginning.
			engine := props.Find(KEngineProperty)
			engineIndex := -1
			if engine != nil {
				engineIndex = engine.Index()
				if engineIndex < 0 {
					engineIndex = 0
				}
			}
			props.SetIndex("expressions", primaryKey.Pop(), engineIndex+1, false)
		}
	}

	return g.baseCreateSQL(e)
}

func starrocksPartitionedbypropertySQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KSchema) {
		// For MVs, StarRocks needs outer parentheses.
		create := e.FindAncestor(KCreate)

		sql := g.expressions(this, exprsOpts{flat: true})
		allCols := true
		for _, col := range this.Expressions() {
			if !col.IsA(KColumn, KIdentifier) {
				allCols = false
				break
			}
		}
		if (create != nil && pyUpper(create.ArgS("kind")) == "VIEW") || allCols {
			sql = "(" + sql + ")"
		}

		return "PARTITION BY " + sql
	}

	return "PARTITION BY " + g.sql(this)
}

func starrocksClusterpropertySQL(g *Generator, e *Expr) string {
	if e.ArgB("this") {
		g.unsupported("Unsupported CLUSTER BY " + g.sqlKey(e, "this"))
		return ""
	}
	expressions := g.expressions(e, exprsOpts{flat: true})
	return "ORDER BY (" + expressions + ")"
}

// starrocksRefreshtriggerpropertySQL renders StarRocks' REFRESH clause, which differs slightly from Doris'.
func starrocksRefreshtriggerpropertySQL(g *Generator, e *Expr) string {
	method := g.sqlKey(e, "method")
	if method != "" {
		method = " " + method
	}
	kind := g.sqlKey(e, "kind")
	if kind != "" {
		kind = " " + kind
	}
	starts := g.sqlKey(e, "starts")
	if starts != "" {
		starts = " START (" + starts + ")"
	}
	every := g.sqlKey(e, "every")
	unit := g.sqlKey(e, "unit")
	if every != "" && unit != "" {
		every = " EVERY (INTERVAL " + every + " " + unit + ")"
	} else {
		every = ""
	}

	return "REFRESH" + method + kind + starts + every
}
