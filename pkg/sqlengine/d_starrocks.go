package sqlengine

// Port of sqlglot/dialects/starrocks.py (class StarRocks), sqlglot/parsers/starrocks.py
// (StarRocksParser) and sqlglot/generators/starrocks.py (StarRocksGenerator). StarRocks inherits
// from MySQL: only the differences from the MySQL classes are ported here.

func init() {
	registerCustomizer("starrocks", customizeStarRocks)
	mysqlPartitionPropertyImpls["starrocks"] = starrocksParsePartitionProperty
}

// ---------------------------------------------------------------------------------------------
// Customizer
// ---------------------------------------------------------------------------------------------

func customizeStarRocks(d *Dialect) {
	P := d.P

	// FUNCTIONS
	P.FUNCTIONS["ADDDATE"] = buildDateDeltaWithInterval(KDateAdd, "DAY")
	P.FUNCTIONS["DATE_ADD"] = buildDateDeltaWithInterval(KDateAdd, "DAY")
	P.FUNCTIONS["DATE_SUB"] = buildDateDeltaWithInterval(KDateSub, "DAY")
	P.FUNCTIONS["SUBDATE"] = buildDateDeltaWithInterval(KDateSub, "DAY")
	P.FUNCTIONS["DATE_TRUNC"] = buildTimestampTrunc
	P.FUNCTIONS["DATEDIFF"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDateDiff, "this", seqGet(args, 0), "expression", seqGet(args, 1), "unit", LiteralString("DAY"))
	}
	P.FUNCTIONS["DATE_DIFF"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDateDiff, "this", seqGet(args, 1), "expression", seqGet(args, 2), "unit", seqGet(args, 0))
	}
	P.FUNCTIONS["ARRAY_FLATTEN"] = fromArgList(KFlatten)
	P.FUNCTIONS["REGEXP"] = fromArgList(KRegexpLike)
	// StarRocks' MAP() is a variadic constructor: MAP(k1, v1, k2, v2, ...)
	// https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map/
	P.FUNCTIONS["MAP"] = func(args []*Expr, d *Dialect) *Expr { return buildVarMap(args) }

	// PROPERTY_PARSERS
	P.PROPERTY_PARSERS["PROPERTIES"] = mysqlNoKwargs("StarRocksParser.<lambda>", func(p *Parser) any {
		return p.parseWrappedProperties()
	})
	P.PROPERTY_PARSERS["UNIQUE"] = mysqlNoKwargs("StarRocksParser.<lambda>", func(p *Parser) any {
		return anyExpr(p.parseCompositeKeyProperty(KUniqueKeyProperty))
	})
	P.PROPERTY_PARSERS["ROLLUP"] = mysqlNoKwargs("StarRocksParser.<lambda>", func(p *Parser) any {
		return anyExpr(starrocksParseRollupProperty(p))
	})
	P.PROPERTY_PARSERS["REFRESH"] = mysqlNoKwargs("StarRocksParser.<lambda>", func(p *Parser) any {
		return anyExpr(starrocksParseRefreshProperty(p))
	})

	// Method overrides
	P.h.parseCreate = starrocksParseCreate
	P.h.parseUnnest = starrocksParseUnnest
	P.h.parsePartitionedBy = starrocksParsePartitionedBy

	customizeStarRocksGenerator(d)
}

// ---------------------------------------------------------------------------------------------
// StarRocksParser methods
// ---------------------------------------------------------------------------------------------

// starrocksParseRollupProperty mirrors StarRocksParser._parse_rollup_property.
func starrocksParseRollupProperty(p *Parser) *Expr {
	// ROLLUP (rollup_name (col1, col2) [FROM from_index] [PROPERTIES (...)], ...)
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

// starrocksParseCreate mirrors StarRocksParser._parse_create.
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

// starrocksParseUnnest mirrors StarRocksParser._parse_unnest.
func starrocksParseUnnest(p *Parser, withAlias bool) *Expr {
	unnest := p.baseParseUnnest(withAlias)

	if unnest != nil {
		alias := unnest.ArgE("alias")

		if alias == nil {
			// Starrocks defaults to naming the table alias as "unnest"
			alias = New(
				KTableAlias,
				"this", ToIdentifier("unnest", nil),
				"columns", []*Expr{ToIdentifier("unnest", nil)},
			)
			unnest.Set("alias", alias)
		} else if !alias.ArgB("columns") {
			// Starrocks defaults to naming the UNNEST column as "unnest"
			// if it's not otherwise specified
			alias.Set("columns", []*Expr{ToIdentifier("unnest", nil)})
		}
	}

	return unnest
}

// starrocksParsePartitionedBy mirrors StarRocksParser._parse_partitioned_by.
func starrocksParsePartitionedBy(p *Parser) *Expr {
	return p.expression(New(
		KPartitionedByProperty,
		"this", New(
			KSchema,
			"expressions", p.parseWrappedCSV(func() *Expr { return p.parseAssignment() }, TK_COMMA, true),
		),
	))
}

// starrocksParsePartitionProperty mirrors StarRocksParser._parse_partition_property.
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

// starrocksParsePartitioningGranularityDynamic mirrors
// StarRocksParser._parse_partitioning_granularity_dynamic.
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

// starrocksParseRefreshProperty mirrors StarRocksParser._parse_refresh_property.
//
// REFRESH [DEFERRED | IMMEDIATE]
//
//	[ASYNC | ASYNC [START (<start_time>)] EVERY (INTERVAL <refresh_interval>) | MANUAL]
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

// ---------------------------------------------------------------------------------------------
// StarRocksGenerator
// ---------------------------------------------------------------------------------------------

// starrocksEliminateBetweenInDelete mirrors _eliminate_between_in_delete.
//
// StarRocks doesn't support BETWEEN in DELETE statements, so we convert
// BETWEEN expressions to explicit comparisons.
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

// starrocksStDistanceSphere mirrors st_distance_sphere.
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

func customizeStarRocksGenerator(d *Dialect) {
	G := d.G
	T := G.TRANSFORMS

	// StarRocks uses the native TRIM(str, chars)/LTRIM/RTRIM function form, not
	// MySQL's TRIM(chars FROM str) syntax, so it falls back to the base generator.
	delete(T, KDateTrunc)
	delete(T, KTrim)

	T[KArgMax] = renameFunc("MAX_BY")
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
	T[KDateDiff] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_DIFF", unitToStr(e, "DAY"), e.Arg("this"), e.Arg("expression"))
	}
	T[KDelete] = transformPreprocess([]func(*Expr) *Expr{starrocksEliminateBetweenInDelete}, nil)
	T[KFlatten] = renameFunc("ARRAY_FLATTEN")
	T[KJSONExtractScalar] = arrowJSONExtractSQL
	T[KJSONExtract] = arrowJSONExtractSQL
	// Both MAP forms (two-array MAP([keys], [values]) and variadic MAP(k1, v1, ...))
	// generate StarRocks' variadic MAP(k1, v1, k2, v2, ...) constructor
	T[KMap] = func(g *Generator, e *Expr) string { return varMapSQL(g, e, "MAP") }
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
	G.h.clusterpropertySQL = starrocksClusterpropertySQL
	G.h.refreshtriggerpropertySQL = starrocksRefreshtriggerpropertySQL
	G.methods[KPartitionedByProperty] = starrocksPartitionedbypropertySQL
}

// starrocksCreateSQL mirrors StarRocksGenerator.create_sql.
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

			// Verify if the first one is an engine property. Is true then insert it after the engine,
			// otherwise insert it at the beginning
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

// starrocksPartitionedbypropertySQL mirrors StarRocksGenerator.partitionedbyproperty_sql.
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

// starrocksClusterpropertySQL mirrors StarRocksGenerator.clusterproperty_sql.
// Generate StarRocks ORDER BY clause for clustering.
func starrocksClusterpropertySQL(g *Generator, e *Expr) string {
	if e.ArgB("this") {
		g.unsupported("Unsupported CLUSTER BY " + g.sqlKey(e, "this"))
		return ""
	}
	expressions := g.expressions(e, exprsOpts{flat: true})
	return "ORDER BY (" + expressions + ")"
}

// starrocksRefreshtriggerpropertySQL mirrors StarRocksGenerator.refreshtriggerproperty_sql.
// Generate StarRocks REFRESH clause for materialized views.
// There is a little difference of the syntax between StarRocks and Doris.
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
