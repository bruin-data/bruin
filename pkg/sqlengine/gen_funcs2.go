package sqlengine

// Port of sqlglot/generator.py (generator chunk E, part 2): slice_sql .. renameindex_sql.

import "fmt"

// slice_sql (generator.py L5679).
func (g *Generator) sliceSQL(expression *Expr) string {
	step := g.sqlKey(expression, "step")
	end := g.sql(expression.Arg("expression"))
	begin := g.sql(expression.Arg("this"))

	sql := end
	if step != "" {
		sql = end + ":" + step
	}
	if sql != "" {
		return begin + ":" + sql
	}
	return begin + ":"
}

// apply_sql (generator.py L5687).
func (g *Generator) applySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expr := g.sqlKey(expression, "expression")

	return this + " APPLY(" + expr + ")"
}

// _grant_or_revoke_sql (generator.py L5693).
func (g *Generator) grantOrRevokeSQL(expression *Expr, keyword string, preposition string, grantOptionPrefix string, grantOptionSuffix string) string {
	privilegesSQL := g.expressions(expression, exprsOpts{key: "privileges", flat: true})

	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = " " + kind
	}

	securable := g.sqlKey(expression, "securable")
	if securable != "" {
		securable = " " + securable
	}

	principals := g.expressions(expression, exprsOpts{key: "principals", flat: true})

	if !expression.ArgB("grant_option") {
		grantOptionPrefix, grantOptionSuffix = "", ""
	}

	// cascade for revoke only
	cascade := g.sqlKey(expression, "cascade")
	if cascade != "" {
		cascade = " " + cascade
	}

	return keyword + " " + grantOptionPrefix + privilegesSQL + " ON" + kind + securable + " " + preposition + " " + principals + grantOptionSuffix + cascade
}

// grant_sql (generator.py L5720).
func (g *Generator) grantSQL(expression *Expr) string {
	return g.grantOrRevokeSQL(expression, "GRANT", "TO", "", " WITH GRANT OPTION")
}

// revoke_sql (generator.py L5728).
func (g *Generator) revokeSQL(expression *Expr) string {
	return g.grantOrRevokeSQL(expression, "REVOKE", "FROM", "GRANT OPTION FOR ", "")
}

// grantprivilege_sql (generator.py L5736).
func (g *Generator) grantprivilegeSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	columns := g.expressions(expression, exprsOpts{flat: true})
	if columns != "" {
		columns = "(" + columns + ")"
	}

	return this + columns
}

// grantprincipal_sql (generator.py L5743).
func (g *Generator) grantprincipalSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")

	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = kind + " "
	}

	return kind + this
}

// columns_sql (generator.py L5751).
func (g *Generator) columnsSQL(expression *Expr) string {
	fn := g.functionFallbackSQL(expression)
	if expression.ArgB("unpack") {
		fn = "*" + fn
	}

	return fn
}

// overlay_sql (generator.py L5758).
func (g *Generator) overlaySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expr := g.sqlKey(expression, "expression")
	fromSQL := g.sqlKey(expression, "from_")
	forSQL := g.sqlKey(expression, "for_")
	if forSQL != "" {
		forSQL = " FOR " + forSQL
	}

	return "OVERLAY(" + this + " PLACING " + expr + " FROM " + fromSQL + forSQL + ")"
}

// todouble_sql (generator.py L5767)
// @unsupported_args("format").
func (g *Generator) todoubleSQL(expression *Expr) string {
	if expression.ArgB("format") {
		g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", "format", expression.Kind().Name(), g.d.ClassName))
	}

	castKind := KCast
	if expression.ArgB("safe") {
		castKind = KTryCast
	}
	return g.sql(New(castKind, "this", expression.This(), "to", NewDataType(DT_DOUBLE)))
}

// string_sql (generator.py L5772).
func (g *Generator) stringSQL(expression *Expr) string {
	this := expression.This()
	zone := expression.ArgE("zone")

	if zone != nil {
		// This is a BigQuery specific argument for STRING(<timestamp_expr>, <time_zone>)
		// BigQuery stores timestamps internally as UTC, so ConvertTimezone is used with UTC
		// set for source_tz to transpile the time conversion before the STRING cast
		this = New(KConvertTimezone, "source_tz", LiteralString("UTC"), "target_tz", zone, "timestamp", this)
	}

	return g.sql(genECast(this, DT_VARCHAR, nil))
}

// median_sql (generator.py L5786).
func (g *Generator) medianSQL(expression *Expr) string {
	if !g.s.SUPPORTS_MEDIAN {
		return g.sql(New(KPercentileCont, "this", expression.This(), "expression", LiteralNumber("0.5")))
	}

	return g.functionFallbackSQL(expression)
}

// overflowtruncatebehavior_sql (generator.py L5794).
func (g *Generator) overflowtruncatebehaviorSQL(expression *Expr) string {
	filler := g.sqlKey(expression, "this")
	if filler != "" {
		filler = " " + filler
	}
	withCount := "WITHOUT COUNT"
	if expression.ArgB("with_count") {
		withCount = "WITH COUNT"
	}
	return "TRUNCATE" + filler + " " + withCount
}

// unixseconds_sql (generator.py L5800).
func (g *Generator) unixsecondsSQL(expression *Expr) string {
	if g.s.SUPPORTS_UNIX_SECONDS {
		return g.functionFallbackSQL(expression)
	}

	startTs := genECast(LiteralString("1970-01-01 00:00:00+00"), DT_TIMESTAMPTZ, nil)

	return g.sql(
		New(KTimestampDiff, "this", expression.This(), "expression", startTs, "unit", VarExpr("SECONDS")),
	)
}

// arraysize_sql (generator.py L5810).
func (g *Generator) arraysizeSQL(expression *Expr) string {
	dim := expression.Expression()

	// For dialects that don't support the dimension arg, we can safely transpile it's default value (1st dimension)
	if dim != nil && g.s.ARRAY_SIZE_DIM_REQUIRED == TriNone {
		if !(dim.IsInt() && dim.Name() == "1") {
			g.unsupported("Cannot transpile dimension argument for ARRAY_LENGTH")
		}
		dim = nil
	}

	// If dimension is required but not specified, default initialize it
	if g.s.ARRAY_SIZE_DIM_REQUIRED == TriTrue && dim == nil {
		dim = LiteralInt(1)
	}

	return g.fn(g.s.ARRAY_SIZE_NAME, expression.Arg("this"), dim)
}

// attach_sql (generator.py L5825).
func (g *Generator) attachSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	existsSQL := ""
	if expression.ArgB("exists") {
		existsSQL = " IF NOT EXISTS"
	}
	expressions := g.expressions(expression, exprsOpts{})
	if expressions != "" {
		expressions = " (" + expressions + ")"
	}

	return "ATTACH" + existsSQL + " " + this + expressions
}

// detach_sql (generator.py L5833).
func (g *Generator) detachSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = " " + kind
	}
	// the DATABASE keyword is required if IF EXISTS is set for DuckDB
	// ref: https://duckdb.org/docs/stable/sql/statements/attach.html#detach-syntax
	exists := ""
	if expression.ArgB("exists") {
		exists = " IF EXISTS"
	}
	if exists != "" {
		if kind == "" {
			kind = " DATABASE"
		}
	}

	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	cluster := g.sqlKey(expression, "cluster")
	if cluster != "" {
		cluster = " " + cluster
	}
	permanent := ""
	if expression.ArgB("permanent") {
		permanent = " PERMANENTLY"
	}
	sync := ""
	if expression.ArgB("sync") {
		sync = " SYNC"
	}
	return "DETACH" + kind + exists + this + cluster + permanent + sync
}

// attachoption_sql (generator.py L5850).
func (g *Generator) attachoptionSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	value := g.sqlKey(expression, "expression")
	if value != "" {
		value = " " + value
	}
	return this + value
}

// watermarkcolumnconstraint_sql (generator.py L5856).
func (g *Generator) watermarkcolumnconstraintSQL(expression *Expr) string {
	return "WATERMARK FOR " + g.sqlKey(expression, "this") + " AS " + g.sqlKey(expression, "expression")
}

// encodeproperty_sql (generator.py L5861).
func (g *Generator) encodepropertySQL(expression *Expr) string {
	encode := "ENCODE"
	if expression.ArgB("key") {
		encode = "KEY ENCODE"
	}
	encode = encode + " " + g.sqlKey(expression, "this")

	properties := expression.ArgE("properties")
	if properties != nil {
		encode = encode + " " + g.properties(properties, "", ", ", "", true)
	}

	return encode
}

// includeproperty_sql (generator.py L5871).
func (g *Generator) includepropertySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	include := "INCLUDE " + this

	columnDef := g.sqlKey(expression, "column_def")
	if columnDef != "" {
		include = include + " " + columnDef
	}

	alias := g.sqlKey(expression, "alias")
	if alias != "" {
		include = include + " AS " + alias
	}

	return include
}

// xmlelement_sql (generator.py L5885).
func (g *Generator) xmlelementSQL(expression *Expr) string {
	prefix := "NAME"
	if expression.ArgB("evalname") {
		prefix = "EVALNAME"
	}
	name := prefix + " " + g.sqlKey(expression, "this")
	args := []any{name}
	for _, e := range expression.Expressions() {
		args = append(args, e)
	}
	return g.fn("XMLELEMENT", args...)
}

// xmlkeyvalueoption_sql (generator.py L5890).
func (g *Generator) xmlkeyvalueoptionSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expr := g.sqlKey(expression, "expression")
	if expr != "" {
		expr = "(" + expr + ")"
	}
	return this + expr
}

// partitionbyrangeproperty_sql (generator.py L5896).
func (g *Generator) basePartitionbyrangepropertySQL(expression *Expr) string {
	partitions := g.expressions(expression, exprsOpts{key: "partition_expressions"})
	create := g.expressions(expression, exprsOpts{key: "create_expressions"})
	return "PARTITION BY RANGE " + g.wrap(partitions) + " " + g.wrap(create)
}

// partitionbyrangepropertydynamic_sql (generator.py L5901).
func (g *Generator) basePartitionbyrangepropertydynamicSQL(expression *Expr) string {
	start := g.sqlKey(expression, "start")
	end := g.sqlKey(expression, "end")

	if !expression.HasArgKey("every") {
		// expression.args["every"] raises KeyError
		panic(&ValueError{Msg: "'every'"})
	}
	every := expression.ArgE("every")
	if every.IsA(KInterval) && every.This().IsString() {
		every.This().Replace(LiteralNumber(every.Name()))
	}

	return "START " + g.wrap(start) + " END " + g.wrap(end) + " EVERY " + g.wrap(g.sql(every))
}

// unpivotcolumns_sql (generator.py L5913).
func (g *Generator) unpivotcolumnsSQL(expression *Expr) string {
	name := g.sqlKey(expression, "this")
	values := g.expressions(expression, exprsOpts{flat: true})

	return "NAME " + name + " VALUE " + values
}

// analyzesample_sql (generator.py L5919).
func (g *Generator) analyzesampleSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	sample := g.sqlKey(expression, "sample")
	return "SAMPLE " + sample + " " + kind
}

// analyzestatistics_sql (generator.py L5924).
func (g *Generator) analyzestatisticsSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	option := g.sqlKey(expression, "option")
	if option != "" {
		option = " " + option
	}
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	columns := g.expressions(expression, exprsOpts{})
	if columns != "" {
		columns = " " + columns
	}
	return kind + option + " STATISTICS" + this + columns
}

// analyzehistogram_sql (generator.py L5934).
func (g *Generator) analyzehistogramSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	columns := g.expressions(expression, exprsOpts{})
	innerExpression := g.sqlKey(expression, "expression")
	if innerExpression != "" {
		innerExpression = " " + innerExpression
	}
	updateOptions := g.sqlKey(expression, "update_options")
	if updateOptions != "" {
		updateOptions = " " + updateOptions + " UPDATE"
	}
	return this + " HISTOGRAM ON " + columns + innerExpression + updateOptions
}

// analyzedelete_sql (generator.py L5943).
func (g *Generator) analyzedeleteSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = " " + kind
	}
	return "DELETE" + kind + " STATISTICS"
}

// analyzelistchainedrows_sql (generator.py L5948).
func (g *Generator) analyzelistchainedrowsSQL(expression *Expr) string {
	innerExpression := g.sqlKey(expression, "expression")
	return "LIST CHAINED ROWS" + innerExpression
}

// analyzevalidate_sql (generator.py L5952).
func (g *Generator) analyzevalidateSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	innerExpression := g.sqlKey(expression, "expression")
	return "VALIDATE " + kind + this + innerExpression
}

// analyze_sql (generator.py L5959).
func (g *Generator) analyzeSQL(expression *Expr) string {
	options := g.expressions(expression, exprsOpts{key: "options", sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}
	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = " " + kind
	}
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	mode := g.sqlKey(expression, "mode")
	if mode != "" {
		mode = " " + mode
	}
	properties := g.sqlKey(expression, "properties")
	if properties != "" {
		properties = " " + properties
	}
	partition := g.sqlKey(expression, "partition")
	if partition != "" {
		partition = " " + partition
	}
	innerExpression := g.sqlKey(expression, "expression")
	if innerExpression != "" {
		innerExpression = " " + innerExpression
	}
	return "ANALYZE" + options + kind + this + partition + mode + innerExpression + properties
}

// xmltable_sql (generator.py L5976).
func (g *Generator) xmltableSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	namespaces := g.expressions(expression, exprsOpts{key: "namespaces"})
	if namespaces != "" {
		namespaces = "XMLNAMESPACES(" + namespaces + "), "
	}
	passing := g.expressions(expression, exprsOpts{key: "passing"})
	if passing != "" {
		passing = g.sep1() + "PASSING" + g.seg(passing)
	}
	columns := g.expressions(expression, exprsOpts{key: "columns"})
	if columns != "" {
		columns = g.sep1() + "COLUMNS" + g.seg(columns)
	}
	byRef := ""
	if expression.ArgB("by_ref") {
		byRef = g.sep1() + "RETURNING SEQUENCE BY REF"
	}
	return "XMLTABLE(" + g.sep("") + g.indentDefault(namespaces+this+passing+byRef+columns) + g.segSep(")", "")
}

// xmlnamespace_sql (generator.py L5987).
func (g *Generator) xmlnamespaceSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if expression.This().IsA(KAlias) {
		return this
	}
	return "DEFAULT " + this
}

// export_sql (generator.py L5991).
func (g *Generator) exportSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	connection := g.sqlKey(expression, "connection")
	if connection != "" {
		connection = "WITH CONNECTION " + connection + " "
	}
	options := g.sqlKey(expression, "options")
	return "EXPORT DATA " + connection + options + " AS " + this
}

// declare_sql (generator.py L5998).
func (g *Generator) declareSQL(expression *Expr) string {
	replace := ""
	if expression.ArgB("replace") {
		replace = "OR REPLACE "
	}
	return "DECLARE " + replace + g.expressions(expression, exprsOpts{flat: true})
}

// declareitem_sql (generator.py L6002).
func (g *Generator) declareitemSQL(expression *Expr) string {
	variables := g.expressions(expression, exprsOpts{key: "this"})
	def := g.sqlKey(expression, "default")
	if def != "" {
		def = " " + g.s.DECLARE_DEFAULT_ASSIGNMENT + " " + def
	}

	kind := g.sqlKey(expression, "kind")
	if expression.ArgE("kind").IsA(KSchema) {
		kind = "TABLE " + kind
	}

	if kind != "" {
		kind = " " + kind
	}

	return variables + kind + def
}

// recursivewithsearch_sql (generator.py L6015).
func (g *Generator) recursivewithsearchSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	this := g.sqlKey(expression, "this")
	set := g.sqlKey(expression, "expression")
	using := g.sqlKey(expression, "using")
	if using != "" {
		using = " USING " + using
	}

	kindSQL := "SEARCH " + kind + " FIRST BY"
	if kind == "CYCLE" {
		kindSQL = kind
	}

	return kindSQL + " " + this + " SET " + set + using
}

// parameterizedagg_sql (generator.py L6026).
func (g *Generator) parameterizedaggSQL(expression *Expr) string {
	params := g.expressions(expression, exprsOpts{key: "params", flat: true})
	return g.fn(expression.Name(), genEExprArgs(expression.Expressions())...) + "(" + params + ")"
}

// anonymousaggfunc_sql (generator.py L6030).
func (g *Generator) anonymousaggfuncSQL(expression *Expr) string {
	return g.fn(expression.Name(), genEExprArgs(expression.Expressions())...)
}

// combinedaggfunc_sql (generator.py L6033).
func (g *Generator) combinedaggfuncSQL(expression *Expr) string {
	return g.anonymousaggfuncSQL(expression)
}

// combinedparameterizedagg_sql (generator.py L6036).
func (g *Generator) combinedparameterizedaggSQL(expression *Expr) string {
	return g.parameterizedaggSQL(expression)
}

// show_sql (generator.py L6039).
func (g *Generator) baseShowSQL(expression *Expr) string {
	g.unsupported("Unsupported SHOW statement")
	return ""
}

// install_sql (generator.py L6043).
func (g *Generator) baseInstallSQL(expression *Expr) string {
	g.unsupported("Unsupported INSTALL statement")
	return ""
}

// get_put_sql (generator.py L6047).
func (g *Generator) getPutSQL(expression *Expr) string {
	// Snowflake GET/PUT statements:
	//   PUT <file> <internalStage> <properties>
	//   GET <internalStage> <file> <properties>
	props := expression.ArgE("properties")
	propsSQL := ""
	if props != nil {
		propsSQL = g.properties(props, " ", " ", "", false)
	}
	this := g.sqlKey(expression, "this")
	target := g.sqlKey(expression, "target")

	if expression.IsA(KPut) {
		return "PUT " + this + " " + target + propsSQL
	}
	return "GET " + target + " " + this + propsSQL
}

// translatecharacters_sql (generator.py L6061).
func (g *Generator) translatecharactersSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expr := g.sqlKey(expression, "expression")
	withError := ""
	if expression.ArgB("with_error") {
		withError = " WITH ERROR"
	}
	return "TRANSLATE(" + this + " USING " + expr + withError + ")"
}

// decodecase_sql (generator.py L6067).
func (g *Generator) decodecaseSQL(expression *Expr) string {
	if g.s.SUPPORTS_DECODE_CASE {
		return g.fn("DECODE", genEExprArgs(expression.Expressions())...)
	}

	all := expression.Expressions()
	if len(all) == 0 {
		panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
	}
	decodeExpr, expressions := all[0], all[1:]

	var ifs []*Expr
	for i := 0; i+1 < len(expressions); i += 2 {
		search, result := expressions[i], expressions[i+1]
		if search.IsA(KLiteral) {
			ifs = append(ifs, New(KIf, "this", genEBinop(KEQ, decodeExpr, search), "true", result))
		} else if search.IsA(KNull) {
			ifs = append(ifs, New(KIf, "this", genEBinop(KIs, decodeExpr, New(KNull)), "true", result))
		} else {
			if search.IsA(KBinary) {
				search = Paren(search.Copy())
			}

			cond := genECombine(
				[]*Expr{
					genEBinop(KEQ, decodeExpr, search),
					genECombine(
						[]*Expr{genEBinop(KIs, decodeExpr, New(KNull)), genEBinop(KIs, search, New(KNull))},
						KAnd, false, true,
					),
				},
				KOr, false, true,
			)
			ifs = append(ifs, New(KIf, "this", cond, "true", result))
		}
	}

	var def *Expr
	if len(expressions)%2 == 1 {
		def = expressions[len(expressions)-1]
	}
	caseExpr := New(KCase, "ifs", ifs, "default", def)
	return g.sql(caseExpr)
}

// semanticview_sql (generator.py L6093).
func (g *Generator) semanticviewSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	this = g.segSep(this, "")
	dimensions := g.expressions(expression, exprsOpts{key: "dimensions", dynamic: true, skipFirst: true, skipLast: true})
	if dimensions != "" {
		dimensions = g.seg("DIMENSIONS " + dimensions)
	}
	metrics := g.expressions(expression, exprsOpts{key: "metrics", dynamic: true, skipFirst: true, skipLast: true})
	if metrics != "" {
		metrics = g.seg("METRICS " + metrics)
	}
	facts := g.expressions(expression, exprsOpts{key: "facts", dynamic: true, skipFirst: true, skipLast: true})
	if facts != "" {
		facts = g.seg("FACTS " + facts)
	}
	where := g.sqlKey(expression, "where")
	if where != "" {
		where = g.seg("WHERE " + where)
	}
	body := g.indent(this+metrics+dimensions+facts+where, 0, -1, true, false)
	return "SEMANTIC_VIEW(" + body + g.segSep(")", "")
}

// getextract_sql (generator.py L6111).
func (g *Generator) getextractSQL(expression *Expr) string {
	this := expression.This()
	expr := expression.Expression()

	if this.Type() == nil || expression.Type() == nil {
		this = annotateTypes(this, g.d)
	}

	if genEIsType(this, DT_ARRAY, DT_MAP) {
		return g.sql(New(KBracket, "this", this, "expressions", []*Expr{expr}))
	}

	return g.sql(New(KJSONExtract, "this", this, "expression", g.d.toJSONPath(expr)))
}

// datefromunixdate_sql (generator.py L6125).
func (g *Generator) datefromunixdateSQL(expression *Expr) string {
	return g.sql(
		New(
			KDateAdd,
			"this", genECast(LiteralString("1970-01-01"), DT_DATE, nil),
			"expression", expression.This(),
			"unit", VarExpr("DAY"),
		),
	)
}

// space_sql (generator.py L6134).
func (g *Generator) baseSpaceSQL(expression *Expr) string {
	return g.sql(New(KRepeat, "this", LiteralString(" "), "times", expression.This()))
}

// buildproperty_sql (generator.py L6137).
func (g *Generator) buildpropertySQL(expression *Expr) string {
	return "BUILD " + g.sqlKey(expression, "this")
}

// refreshtriggerproperty_sql (generator.py L6140).
func (g *Generator) baseRefreshtriggerpropertySQL(expression *Expr) string {
	method := g.sqlKey(expression, "method")
	kind := expression.Arg("kind")
	if !truthy(kind) {
		return "REFRESH " + method
	}

	every := g.sqlKey(expression, "every")
	unit := g.sqlKey(expression, "unit")
	if every != "" {
		every = " EVERY " + every + " " + unit
	}
	starts := g.sqlKey(expression, "starts")
	if starts != "" {
		starts = " STARTS " + starts
	}

	return "REFRESH " + method + " ON " + genEPyStr(kind) + every + starts
}

// modelattribute_sql (generator.py L6154).
func (g *Generator) baseModelattributeSQL(expression *Expr) string {
	g.unsupported("The model!attribute syntax is not supported")
	return ""
}

// directorystage_sql (generator.py L6158).
func (g *Generator) directorystageSQL(expression *Expr) string {
	return g.fn("DIRECTORY", expression.Arg("this"))
}

// uuid_sql (generator.py L6161).
func (g *Generator) baseUuidSQL(expression *Expr) string {
	isString := expression.ArgB("is_string")
	uuidFuncSQL := g.fn("UUID")

	if isString && !g.d.S.UUID_IS_STRING_TYPE {
		// exp.cast(<str>, ...) parses the string with the given dialect first
		parsed, err := g.d.ParseOne(uuidFuncSQL, nil)
		if err != nil {
			panic(genPanic{err})
		}
		return g.sql(genECast(parsed, DT_VARCHAR, g.d))
	}

	return uuidFuncSQL
}

// initcap_sql (generator.py L6170).
func (g *Generator) initcapSQL(expression *Expr) string {
	delimiters := expression.Expression()

	if delimiters != nil {
		// do not generate delimiters arg if we are round-tripping from default delimiters
		if delimiters.IsString() && delimiters.ThisS() == g.d.S.INITCAP_DEFAULT_DELIMITER_CHARS {
			delimiters = nil
		} else if !g.d.S.INITCAP_SUPPORTS_CUSTOM_DELIMITERS {
			g.unsupported("INITCAP does not support custom delimiters")
			delimiters = nil
		}
	}

	return g.fn("INITCAP", expression.Arg("this"), delimiters)
}

// localtime_sql (generator.py L6186).
func (g *Generator) localtimeSQL(expression *Expr) string {
	this := expression.This()
	if this != nil {
		return g.fn("LOCALTIME", this)
	}
	return "LOCALTIME"
}

// localtimestamp_sql (generator.py L6190).
func (g *Generator) localtimestampSQL(expression *Expr) string {
	this := expression.This()
	if this != nil {
		return g.fn("LOCALTIMESTAMP", this)
	}
	return "LOCALTIMESTAMP"
}

// weekstart_sql (generator.py L6194).
func (g *Generator) weekstartSQL(expression *Expr) string {
	this := pyUpper(expression.This().Name())
	if g.d.S.WEEK_OFFSET == -1 && this == "SUNDAY" {
		// BigQuery specific optimization since WEEK(SUNDAY) == WEEK
		return "WEEK"
	}

	return g.fn("WEEK", expression.Arg("this"))
}

// chr_sql (generator.py L6202).
func (g *Generator) baseChrSQL(expression *Expr, name string) string {
	this := g.expressions(expression, exprsOpts{})
	charset := g.sqlKey(expression, "charset")
	using := ""
	if charset != "" {
		using = " USING " + charset
	}
	return g.fn(name, this+using)
}

// block_sql (generator.py L6208).
func (g *Generator) blockSQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{sep: strp2("; "), flat: true})
	if expressions != "" {
		return expressions
	}
	return ""
}

// storedprocedure_sql (generator.py L6212).
func (g *Generator) baseStoredprocedureSQL(expression *Expr) string {
	g.unsupported("Unsupported Stored Procedure syntax")
	return ""
}

// ifblock_sql (generator.py L6216).
func (g *Generator) baseIfblockSQL(expression *Expr) string {
	g.unsupported("Unsupported If block syntax")
	return ""
}

// whileblock_sql (generator.py L6220).
func (g *Generator) baseWhileblockSQL(expression *Expr) string {
	g.unsupported("Unsupported While block syntax")
	return ""
}

// execute_sql (generator.py L6224).
func (g *Generator) baseExecuteSQL(expression *Expr) string {
	g.unsupported("Unsupported Execute syntax")
	return ""
}

// executesql_sql (generator.py L6228).
func (g *Generator) baseExecutesqlSQL(expression *Expr) string {
	g.unsupported("Unsupported Execute syntax")
	return ""
}

// altermodifysqlsecurity_sql (generator.py L6232).
func (g *Generator) altermodifysqlsecuritySQL(expression *Expr) string {
	props := g.expressions(expression, exprsOpts{sep: strp2(" ")})
	return "MODIFY " + props
}

// usingproperty_sql (generator.py L6236).
func (g *Generator) baseUsingpropertySQL(expression *Expr) string {
	kind := expression.Arg("kind")
	return "USING " + genEPyStr(kind) + " " + g.sqlKey(expression, "this")
}

// renameindex_sql (generator.py L6240).
func (g *Generator) renameindexSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	to := g.sqlKey(expression, "to")
	return "RENAME INDEX " + this + " TO " + to
}

// genEExprArgs converts an expression list into variadic g.fn arguments (Python *expressions).
func genEExprArgs(list []*Expr) []any {
	args := make([]any, 0, len(list))
	for _, e := range list {
		args = append(args, e)
	}
	return args
}
