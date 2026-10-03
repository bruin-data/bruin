package sqlengine

// Port of sqlglot/generator.py (generator chunk E, part 1): dateadd_sql .. arrayagg_sql.

import (
	"fmt"
	"regexp"
)

// dateadd_sql (generator.py L5170).
func (g *Generator) dateaddSQL(expression *Expr) string {
	return g.fn(
		"DATE_ADD",
		expression.Arg("this"),
		expression.Arg("expression"),
		genEUnitToStr(expression, "DAY"),
	)
}

// arrayany_sql (generator.py L5180).
func (g *Generator) arrayanySQL(expression *Expr) string {
	if g.s.CAN_IMPLEMENT_ARRAY_ANY {
		filtered := New(KArrayFilter, "this", expression.This(), "expression", expression.Expression())
		filteredNotEmpty := genEBinop(KNEQ, New(KArraySize, "this", filtered), LiteralInt(0))
		originalIsEmpty := genEBinop(KEQ, New(KArraySize, "this", expression.This()), LiteralInt(0))
		return g.sql(Paren(genECombine([]*Expr{originalIsEmpty, filteredNotEmpty}, KOr, true, true).Copy()))
	}

	// SQLGlot's executor supports ARRAY_ANY, so we don't wanna warn for the SQLGlot dialect
	if g.d.ClassName != "Dialect" {
		g.unsupported("ARRAY_ANY is unsupported")
	}

	return g.functionFallbackSQL(expression)
}

// struct_sql (generator.py L5195).
func (g *Generator) baseStructSQL(expression *Expr) string {
	var exprs []*Expr
	for _, e := range expression.Expressions() {
		if e.IsA(KPropertyEQ) {
			var alias any
			if e.This().IsString() {
				alias = e.Name()
			} else {
				alias = e.This()
			}
			exprs = append(exprs, genEAlias(e.Expression(), alias))
		} else {
			exprs = append(exprs, e)
		}
	}
	expression.Set("expressions", exprs)

	return g.functionFallbackSQL(expression)
}

// partitionrange_sql (generator.py L5208).
func (g *Generator) basePartitionrangeSQL(expression *Expr) string {
	low := g.sqlKey(expression, "this")
	high := g.sqlKey(expression, "expression")

	return low + " TO " + high
}

// truncatetable_sql (generator.py L5214).
func (g *Generator) truncatetableSQL(expression *Expr) string {
	target := "TABLE"
	if expression.ArgB("is_database") {
		target = "DATABASE"
	}
	tables := " " + g.expressions(expression, exprsOpts{})

	exists := ""
	if expression.ArgB("exists") {
		exists = " IF EXISTS"
	}

	onCluster := g.sqlKey(expression, "cluster")
	if onCluster != "" {
		onCluster = " " + onCluster
	}

	identity := g.sqlKey(expression, "identity")
	if identity != "" {
		identity = " " + identity + " IDENTITY"
	}

	option := g.sqlKey(expression, "option")
	if option != "" {
		option = " " + option
	}

	partition := g.sqlKey(expression, "partition")
	if partition != "" {
		partition = " " + partition
	}

	return "TRUNCATE " + target + exists + tables + onCluster + identity + option + partition
}

// convert_sql (generator.py L5236)
// This transpiles T-SQL's CONVERT function
// https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16
func (g *Generator) baseConvertSQL(expression *Expr) string {
	to := expression.This()
	value := expression.Expression()
	style := expression.ArgE("style")
	safe := expression.Arg("safe")
	strict := expression.ArgB("strict")

	if to == nil || value == nil {
		return ""
	}

	// Retrieve length of datatype and override to default if not specified
	if seqGet(to.Expressions(), 0) == nil && g.s.PARAMETERIZABLE_TEXT_TYPES.Has(to.DTypeOf()) {
		to = New(KDataType, "this", to.DTypeOf(), "expressions", []*Expr{LiteralInt(30)}, "nested", false)
	}

	var transformed *Expr
	castKind := KTryCast
	if strict {
		castKind = KCast
	}

	// Check whether a conversion with format (T-SQL calls this 'style') is applicable
	if style.IsA(KLiteral) && style.IsInt() {
		styleValue := style.Name()
		convertedStyle, ok := MustDialect("tsql").S.CONVERT_FORMAT_MAPPING[styleValue]
		if !ok || convertedStyle == "" {
			g.unsupported("Unsupported T-SQL 'style' value: " + styleValue)
		}
		if !ok {
			// exp.Literal.string(None)
			convertedStyle = "None"
		}

		format := LiteralString(convertedStyle)

		toThis := to.DTypeOf()
		if toThis == DT_DATE {
			transformed = New(KStrToDate, "this", value, "format", format)
		} else if toThis == DT_DATETIME || toThis == DT_DATETIME2 {
			transformed = New(KStrToTime, "this", value, "format", format)
		} else if g.s.PARAMETERIZABLE_TEXT_TYPES.Has(toThis) {
			transformed = New(castKind, "this", New(KTimeToStr, "this", value, "format", format), "to", to, "safe", safe)
		} else if toThis == DT_TEXT {
			transformed = New(KTimeToStr, "this", value, "format", format)
		}
	}

	if transformed == nil {
		transformed = New(castKind, "this", value, "to", to, "safe", safe)
	}

	return g.sql(transformed)
}

// _jsonpathkey_sql (generator.py L5278).
func (g *Generator) baseJsonpathkeySQL(expression *Expr) string {
	if thisE := expression.This(); thisE.IsA(KJSONPathWildcard) {
		this := g.jsonPathPart(thisE)
		if this != "" {
			return "." + this
		}
		return ""
	}
	this := expression.ThisS()

	quoted := expression.ArgB("quoted")
	if !(quoted && g.s.JSON_PATH_KEY_QUOTED_FORCES_BRACKETS) && genESafeJSONPathKeyRE(g).MatchString(this) {
		return "." + this
	}

	this = g.jsonPathPart(this)

	if quoted && g.s.QUOTE_JSON_PATH {
		// The whole path is rendered as a single quoted string literal, so the bracketed key
		// (which may itself contain backslash-escaped quotes, e.g. ["x \"y\"z"]) must be
		// escaped again for the outer string literal (-> ["x \\"y\\"z"]).
		this = g.escapeStr(this, true, "", "", false)
	}

	if g.quoteJSONPathKeyUsingBrackets && g.s.JSON_PATH_BRACKETED_KEY_SUPPORTED {
		return "[" + this + "]"
	}
	return "." + this
}

// _jsonpathsubscript_sql (generator.py L5304).
func (g *Generator) baseJsonpathsubscriptSQL(expression *Expr) string {
	this := g.jsonPathPart(expression.Arg("this"))
	if this != "" {
		return "[" + this + "]"
	}
	return ""
}

// _simplify_unless_literal (generator.py L5308).
func (g *Generator) simplifyUnlessLiteral(expression *Expr) *Expr {
	if !expression.IsA(KLiteral) {
		expression = simplifyExpr(expression, g.d)
	}

	return expression
}

// _embed_ignore_nulls (generator.py L5316).
func (g *Generator) embedIgnoreNulls(expression *Expr, text string) string {
	this := expression.This()
	if len(g.s.RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS) > 0 && this.IsA(g.s.RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS...) {
		g.unsupported(fmt.Sprintf("RESPECT/IGNORE NULLS is not supported for %s in %s", this.Key(), g.d.ClassName))
		return g.sql(this)
	}

	if g.s.IGNORE_NULLS_IN_FUNC && !truthy(expression.MetaGet("inline")) {
		if g.s.IGNORE_NULLS_BEFORE_ORDER {
			// The first modifier here will be the one closest to the AggFunc's arg
			// (stable sort by: HavingMax < Order < Limit)
			var mod *Expr
			modRank := 3
			for x := range expression.FindAll(KHavingMax, KOrder, KLimit) {
				rank := 2
				if x.IsA(KHavingMax) {
					rank = 0
				} else if x.IsA(KOrder) {
					rank = 1
				}
				if rank < modRank {
					mod, modRank = x, rank
				}
			}

			if mod != nil {
				inner := New(expression.Kind(), "this", mod.This().Copy())
				inner.Meta()["inline"] = true
				mod.This().Replace(inner)
				return g.sql(expression.Arg("this"))
			}
		}

		aggFunc := expression.Find(KAggFunc)

		if aggFunc != nil {
			aggFuncSQL := genEDropLastRune(g.sqlNoComment(aggFunc)) + " " + text + ")"
			return g.maybeCommentC(aggFuncSQL, nil, aggFunc.Comments())
		}
	}

	return g.sqlKey(expression, "this") + " " + text
}

// copyparameter_sql (generator.py L5357).
func (g *Generator) copyparameterSQL(expression *Expr) string {
	option := g.sqlKey(expression, "this")

	if len(expression.Expressions()) > 0 {
		upper := pyUpper(option)

		// Snowflake FILE_FORMAT options are separated by whitespace
		sep := ", "
		if upper == "FILE_FORMAT" {
			sep = " "
		}

		// Databricks copy/format options do not set their list of values with EQ
		op := " = "
		if upper == "COPY_OPTIONS" || upper == "FORMAT_OPTIONS" {
			op = " "
		}
		values := g.expressions(expression, exprsOpts{flat: true, sep: strp2(sep)})
		return option + op + "(" + values + ")"
	}

	value := g.sqlKey(expression, "expression")

	if value == "" {
		return option
	}

	op := " "
	if g.s.COPY_PARAMS_EQ_REQUIRED {
		op = " = "
	}

	return option + op + value
}

// credentials_sql (generator.py L5380).
func (g *Generator) credentialsSQL(expression *Expr) string {
	credExpr := expression.Arg("credentials")
	var credentials string
	if ce, ok := credExpr.(*Expr); ok && ce.IsA(KLiteral) {
		// Redshift case: CREDENTIALS <string>
		credentials = g.sqlKey(expression, "credentials")
		if credentials != "" {
			credentials = "CREDENTIALS " + credentials
		}
	} else {
		// Snowflake case: CREDENTIALS = (...)
		credentials = g.expressions(expression, exprsOpts{key: "credentials", flat: true, sep: strp2(" ")})
		if credExpr != nil {
			credentials = "CREDENTIALS = (" + credentials + ")"
		} else {
			credentials = ""
		}
	}

	storage := g.sqlKey(expression, "storage")
	if storage != "" {
		storage = "STORAGE_INTEGRATION = " + storage
	}

	encryption := g.expressions(expression, exprsOpts{key: "encryption", flat: true, sep: strp2(" ")})
	if encryption != "" {
		encryption = " ENCRYPTION = (" + encryption + ")"
	}

	iamRole := g.sqlKey(expression, "iam_role")
	if iamRole != "" {
		iamRole = "IAM_ROLE " + iamRole
	}

	region := g.sqlKey(expression, "region")
	if region != "" {
		region = " REGION " + region
	}

	return credentials + storage + encryption + iamRole + region
}

// copy_sql (generator.py L5405).
func (g *Generator) copySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if g.s.COPY_HAS_INTO_KEYWORD {
		this = " INTO " + this
	} else {
		this = " " + this
	}

	credentials := g.sqlKey(expression, "credentials")
	if credentials != "" {
		credentials = g.seg(credentials)
	}
	files := g.expressions(expression, exprsOpts{key: "files", flat: true})
	kind := ""
	if files != "" {
		if expression.ArgB("kind") {
			kind = g.seg("FROM")
		} else {
			kind = g.seg("TO")
		}
	}

	sep := " "
	if g.d.S.COPY_PARAMS_ARE_CSV {
		sep = ", "
	}
	params := g.expressions(expression, exprsOpts{
		key:       "params",
		sep:       strp2(sep),
		newLine:   true,
		skipLast:  true,
		skipFirst: true,
		noIndent:  !g.s.COPY_PARAMS_ARE_WRAPPED,
	})

	if params != "" {
		if g.s.COPY_PARAMS_ARE_WRAPPED {
			params = " WITH (" + params + ")"
		} else if !g.pretty && (files != "" || credentials != "") {
			params = " " + params
		}
	}

	return "COPY" + this + kind + " " + files + credentials + params
}

// semicolon_sql (generator.py L5433).
func (g *Generator) semicolonSQL(expression *Expr) string {
	return ""
}

// datadeletionproperty_sql (generator.py L5436).
func (g *Generator) datadeletionpropertySQL(expression *Expr) string {
	onSQL := "OFF"
	if expression.ArgB("on") {
		onSQL = "ON"
	}
	var filterCol any
	if s := g.sqlKey(expression, "filter_column"); s != "" {
		filterCol = "FILTER_COLUMN=" + s
	}
	var retentionPeriod any
	if s := g.sqlKey(expression, "retention_period"); s != "" {
		retentionPeriod = "RETENTION_PERIOD=" + s
	}

	if filterCol != nil || retentionPeriod != nil {
		onSQL = g.fn("ON", filterCol, retentionPeriod)
	}

	return "DATA_DELETION=" + onSQL
}

// maskingpolicycolumnconstraint_sql (generator.py L5448).
func (g *Generator) maskingpolicycolumnconstraintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " USING (" + expressions + ")"
	}
	return "MASKING POLICY " + this + expressions
}

// gapfill_sql (generator.py L5456).
func (g *Generator) gapfillSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	this = "TABLE " + this
	args := []any{this}
	for _, k := range expression.ArgKeys() {
		if k != "this" {
			args = append(args, expression.Arg(k))
		}
	}
	return g.fn("GAP_FILL", args...)
}

// scope_resolution (generator.py L5461).
func (g *Generator) baseScopeResolution(rhs string, scopeName string) string {
	var scope any
	if scopeName != "" {
		scope = scopeName
	}
	return g.fn("SCOPE_RESOLUTION", scope, rhs)
}

// scoperesolution_sql (generator.py L5464).
func (g *Generator) scoperesolutionSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	exprE := expression.Expression()

	var expr string
	if exprE.IsA(KFunc) {
		// T-SQL's CLR functions are case sensitive
		args := make([]any, 0, len(exprE.Expressions()))
		for _, a := range exprE.Expressions() {
			args = append(args, a)
		}
		expr = g.sqlKey(exprE, "this") + "(" + g.formatArgs(", ", args...) + ")"
	} else {
		expr = g.sqlKey(expression, "expression")
	}

	return g.scopeResolution(expr, this)
}

// parsejson_sql (generator.py L5476).
func (g *Generator) baseParsejsonSQL(expression *Expr) string {
	// PARSE_JSON_NAME = None is represented by ""
	if g.s.PARSE_JSON_NAME == "" {
		return g.sql(expression.Arg("this"))
	}

	return g.fn(g.s.PARSE_JSON_NAME, expression.Arg("this"), expression.Arg("expression"))
}

// rand_sql (generator.py L5482).
func (g *Generator) baseRandSQL(expression *Expr) string {
	lower := g.sqlKey(expression, "lower")
	upper := g.sqlKey(expression, "upper")

	if lower != "" && upper != "" {
		return "(" + upper + " - " + lower + ") * " + g.fn("RAND", expression.Arg("this")) + " + " + lower
	}
	return g.fn("RAND", expression.Arg("this"))
}

// changes_sql (generator.py L5490).
func (g *Generator) changesSQL(expression *Expr) string {
	information := g.sqlKey(expression, "information")
	information = "INFORMATION => " + information
	atBefore := g.sqlKey(expression, "at_before")
	if atBefore != "" {
		atBefore = g.seg("") + atBefore
	}
	end := g.sqlKey(expression, "end")
	if end != "" {
		end = g.seg("") + end
	}

	return "CHANGES (" + information + ")" + atBefore + end
}

// pad_sql (generator.py L5500).
func (g *Generator) basePadSQL(expression *Expr) string {
	prefix := "R"
	if expression.ArgB("is_left") {
		prefix = "L"
	}

	var fillPattern any
	if s := g.sqlKey(expression, "fill_pattern"); s != "" {
		fillPattern = s
	}
	if fillPattern == nil && g.s.PAD_FILL_PATTERN_IS_REQUIRED {
		fillPattern = "' '"
	}

	return g.fn(prefix+"PAD", expression.Arg("this"), expression.Arg("expression"), fillPattern)
}

// summarize_sql (generator.py L5509).
func (g *Generator) summarizeSQL(expression *Expr) string {
	table := ""
	if expression.ArgB("table") {
		table = " TABLE"
	}
	return "SUMMARIZE" + table + " " + g.sql(expression.Arg("this"))
}

// explodinggenerateseries_sql (generator.py L5513).
func (g *Generator) explodinggenerateseriesSQL(expression *Expr) string {
	kv := make([]any, 0, 2*len(expression.ArgKeys()))
	for _, k := range expression.ArgKeys() {
		kv = append(kv, k, expression.Arg(k))
	}
	generateSeries := New(KGenerateSeries, kv...)

	parent := expression.Parent()
	if parent.IsA(KAlias, KTableAlias) {
		parent = parent.Parent()
	}

	if g.s.SUPPORTS_EXPLODING_PROJECTIONS && !parent.IsA(KTable, KUnnest) {
		return g.sql(New(KUnnest, "expressions", []*Expr{generateSeries}))
	}

	if parent.IsA(KSelect) {
		g.unsupported("GenerateSeries projection unnesting is not supported.")
	}

	return g.sql(generateSeries)
}

// converttimezone_sql (generator.py L5528).
func (g *Generator) baseConverttimezoneSQL(expression *Expr) string {
	if g.s.SUPPORTS_CONVERT_TIMEZONE {
		return g.functionFallbackSQL(expression)
	}

	sourceTz := expression.ArgE("source_tz")
	targetTz := expression.ArgE("target_tz")
	timestamp := expression.ArgE("timestamp")

	if sourceTz != nil && timestamp != nil {
		timestamp = New(KAtTimeZone, "this", genECast(timestamp, DT_TIMESTAMPNTZ, nil), "zone", sourceTz)
	}

	expr := New(KAtTimeZone, "this", timestamp, "zone", targetTz)

	return g.sql(expr)
}

// json_sql (generator.py L5545).
func (g *Generator) jsonSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}

	with := expression.Arg("with_")

	var withSQL string
	if with == nil {
		withSQL = ""
	} else if !truthy(with) {
		withSQL = " WITHOUT"
	} else {
		withSQL = " WITH"
	}

	uniqueSQL := ""
	if expression.ArgB("unique") {
		uniqueSQL = " UNIQUE KEYS"
	}

	return "JSON" + this + withSQL + uniqueSQL
}

// jsonvalue_sql (generator.py L5562).
func (g *Generator) jsonvalueSQL(expression *Expr) string {
	path := g.sqlKey(expression, "path")
	returning := g.sqlKey(expression, "returning")
	if returning != "" {
		returning = " RETURNING " + returning
	}

	onCondition := g.sqlKey(expression, "on_condition")
	if onCondition != "" {
		onCondition = " " + onCondition
	}

	return g.fn("JSON_VALUE", expression.Arg("this"), path+returning+onCondition)
}

// skipjsoncolumn_sql (generator.py L5572).
func (g *Generator) skipjsoncolumnSQL(expression *Expr) string {
	regexpSQL := ""
	if expression.ArgB("regexp") {
		regexpSQL = " REGEXP"
	}
	return "SKIP" + regexpSQL + " " + g.sql(expression.Arg("expression"))
}

// conditionalinsert_sql (generator.py L5576).
func (g *Generator) conditionalinsertSQL(expression *Expr) string {
	else_ := ""
	if expression.ArgB("else_") {
		else_ = "ELSE "
	}
	condition := g.sqlKey(expression, "expression")
	if condition != "" {
		condition = "WHEN " + condition + " THEN "
	} else {
		condition = else_
	}
	insertRunes := []rune(g.sqlKey(expression, "this"))
	insert := ""
	if n := len([]rune("INSERT")); len(insertRunes) > n {
		insert = string(insertRunes[n:])
	}
	insert = pyStrip(insert)
	return condition + insert
}

// multitableinserts_sql (generator.py L5583).
func (g *Generator) multitableinsertsSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	expressions := g.seg(g.expressions(expression, exprsOpts{sep: strp2(" ")}))
	res := "INSERT " + kind + expressions + g.seg(g.sqlKey(expression, "source"))
	return res
}

// oncondition_sql (generator.py L5589).
func (g *Generator) onconditionSQL(expression *Expr) string {
	// Static options like "NULL ON ERROR" are stored as strings, in contrast to "DEFAULT <expr> ON ERROR"
	var empty string
	if e, ok := expression.Arg("empty").(*Expr); ok && e != nil {
		empty = "DEFAULT " + exprSQL(e) + " ON EMPTY"
	} else {
		empty = g.sqlKey(expression, "empty")
	}

	var errorSQL string
	if e, ok := expression.Arg("error").(*Expr); ok && e != nil {
		errorSQL = "DEFAULT " + exprSQL(e) + " ON ERROR"
	} else {
		errorSQL = g.sqlKey(expression, "error")
	}

	if errorSQL != "" && empty != "" {
		if g.d.S.ON_CONDITION_EMPTY_BEFORE_ERROR {
			errorSQL = empty + " " + errorSQL
		} else {
			errorSQL = errorSQL + " " + empty
		}
		empty = ""
	}

	null := g.sqlKey(expression, "null")

	return empty + errorSQL + null
}

// jsonextractquote_sql (generator.py L5617).
func (g *Generator) jsonextractquoteSQL(expression *Expr) string {
	scalar := ""
	if expression.ArgB("scalar") {
		scalar = " ON SCALAR STRING"
	}
	return g.sqlKey(expression, "option") + " QUOTES" + scalar
}

// jsonexists_sql (generator.py L5621).
func (g *Generator) jsonexistsSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	path := g.sqlKey(expression, "path")

	passing := g.expressions(expression, exprsOpts{key: "passing"})
	if passing != "" {
		passing = " PASSING " + passing
	}

	onCondition := g.sqlKey(expression, "on_condition")
	if onCondition != "" {
		onCondition = " " + onCondition
	}

	path = path + passing + onCondition

	return g.fn("JSON_EXISTS", this, path)
}

// _add_arrayagg_null_filter (generator.py L5635)
// Add NULL filter to ARRAY_AGG if dialect requires it.
func (g *Generator) addArrayaggNullFilter(arrayAggSQL string, arrayAggExpr *Expr, columnExpr *Expr) string {
	// Add a NULL FILTER on the column to mimic the results going from a dialect that excludes nulls
	// on ARRAY_AGG (e.g Spark) to one that doesn't (e.g. DuckDB)
	if !(g.d.S.ARRAY_AGG_INCLUDES_NULLS == TriTrue && arrayAggExpr.ArgB("nulls_excluded")) {
		return arrayAggSQL
	}

	parent := arrayAggExpr.Parent()
	if parent.IsA(KFilter) {
		parentCond := parent.Expression().This()
		notNull := genENot(genEBinop(KIs, columnExpr, Null()), true)
		parentCond.Replace(genECombine([]*Expr{parentCond, notNull}, KAnd, true, true))
	} else if columnExpr.Find(KColumn) != nil {
		// Do not add the filter if the input is not a column (e.g. literal, struct etc)
		// DISTINCT is already present in the agg function, do not propagate it to FILTER as well
		var thisSQL string
		if columnExpr.IsA(KDistinct) {
			thisSQL = g.expressions(columnExpr, exprsOpts{})
		} else {
			thisSQL = g.sql(columnExpr)
		}
		arrayAggSQL = arrayAggSQL + " FILTER(WHERE " + thisSQL + " IS NOT NULL)"
	}

	return arrayAggSQL
}

// arrayagg_sql (generator.py L5675).
func (g *Generator) baseArrayaggSQL(expression *Expr) string {
	arrayAgg := g.functionFallbackSQL(expression)
	return g.addArrayaggNullFilter(arrayAgg, expression, expression.This())
}

// ---------------------------------------------------------------------------
// Chunk E helpers (ports of sqlglot builders / dialect helpers used above).
// ---------------------------------------------------------------------------

// genEUnitToStr mirrors sqlglot.dialects.dialect.unit_to_str(expression, default).
func genEUnitToStr(expression *Expr, def string) *Expr {
	unit := expression.ArgE("unit")
	if unit == nil {
		if def != "" {
			return LiteralString(def)
		}
		return nil
	}

	if unit.IsA(KPlaceholder) || !(unit.Is(KVar) || unit.Is(KLiteral)) {
		return unit
	}

	return LiteralString(unit.Name())
}

// genEWrap mirrors expressions.core._wrap(expression, kind).
func genEWrap(e *Expr, kind Kind) *Expr {
	if e.IsA(kind) {
		return Paren(e)
	}
	return e
}

// genEBinop mirrors Expr._binop(klass, other) where other is already an expression
// (convert(other, copy=True) copies it).
func genEBinop(kind Kind, self *Expr, other *Expr) *Expr {
	this := self.Copy()
	o := other.Copy()
	if !this.IsA(kind) && !o.IsA(kind) {
		this = genEWrap(this, KBinary)
		o = genEWrap(o, KBinary)
	}
	return New(kind, "this", this, "expression", o)
}

// genECombine mirrors expressions.core._combine (used by and_ / or_ / Expr.and_ / Expr.or_).
// op is KAnd or KOr. nil expressions are skipped.
func genECombine(expressions []*Expr, op Kind, copy bool, wrap bool) *Expr {
	var conditions []*Expr
	for _, x := range expressions {
		if x == nil {
			continue
		}
		if copy {
			x = x.Copy()
		}
		conditions = append(conditions, x)
	}

	this, rest := conditions[0], conditions[1:]
	if len(rest) > 0 && wrap {
		this = genEWrap(this, KConnector)
	}
	for _, x := range rest {
		if wrap {
			x = genEWrap(x, KConnector)
		}
		this = New(op, "this", this, "expression", x)
	}

	return this
}

// genENot mirrors exp.not_(expression, copy) / Expr.not_(copy).
func genENot(expression *Expr, copy bool) *Expr {
	this := expression
	if copy {
		this = expression.Copy()
	}
	return New(KNot, "this", genEWrap(this, KConnector))
}

// genEAlias mirrors exp.alias_(expression, alias) with table=False, quoted=None, copy=True.
// alias is a string, an Identifier expression or nil.
func genEAlias(expression *Expr, alias any) *Expr {
	e := expression.Copy()

	var id *Expr
	switch a := alias.(type) {
	case string:
		id = ToIdentifier(a, nil)
	case *Expr:
		if a != nil {
			if !a.IsA(KIdentifier) {
				panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + kindClassRepr(a.Kind())})
			}
			id = a.Copy()
		}
	}

	// We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
	// the AST, representing a "named_window" construct (eg. bigquery).
	if e.Kind().hasArgType("alias") && !e.Is(KWindow) {
		e.Set("alias", id)
		return e
	}
	return New(KAlias, "this", e, "alias", id)
}

// genECast mirrors exp.cast(expression, to, copy=True, dialect=d) for a DType target.
// d == nil means the base dialect.
func genECast(expression *Expr, to DType, d *Dialect) *Expr {
	expr := expression.Copy()
	dataType := NewDataType(to)

	// dont re-cast if the expression is already a cast to the correct type
	if expr.IsA(KCast) {
		targetDialect := d
		if targetDialect == nil {
			targetDialect = MustDialect("")
		}
		var typeMapping map[DType]string
		if targetDialect.G != nil {
			typeMapping = targetDialect.G.TYPE_MAPPING
		}

		existingCastType := expr.ArgE("to").DTypeOf()
		newCastType := dataType.DTypeOf()
		existing, ok := typeMapping[existingCastType]
		if !ok {
			existing = dtypeNames[existingCastType]
		}
		newT, ok := typeMapping[newCastType]
		if !ok {
			newT = dtypeNames[newCastType]
		}
		typesAreEquivalent := existing == newT

		if genEIsType(expr, newCastType) || typesAreEquivalent {
			return expr
		}
	}

	cast := New(KCast, "this", expr, "to", dataType)
	cast.SetType(dataType)

	return cast
}

// genEIsType mirrors Expr.is_type(*dtypes) for plain DType arguments
// (DataType.is_type on data types, Cast.is_type -> to.is_type, otherwise the annotated type).
func genEIsType(e *Expr, dtypes ...DType) bool {
	if e == nil {
		return false
	}
	var t *Expr
	switch {
	case e.kind.isDataType():
		t = e
	case e.IsA(KCast):
		t = e.ArgE("to")
	default:
		t = e.typ
	}
	if t == nil {
		return false
	}
	selfType := t.DTypeOf()
	for _, dt := range dtypes {
		var matches bool
		if selfType == DT_USERDEFINED || dt == DT_USERDEFINED {
			matches = t.Equal(NewDataType(dt))
		} else {
			matches = selfType == dt
		}
		if matches {
			return true
		}
	}
	return false
}

// genEDropLastRune mirrors Python's s[:-1].
func genEDropLastRune(s string) string {
	r := []rune(s)
	if len(r) == 0 {
		return ""
	}
	return string(r[:len(r)-1])
}

// genEPyStr mirrors Python's str(value) inside f-strings.
func genEPyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int:
		return itoa(x)
	case *Expr:
		if x == nil {
			return "None"
		}
		return exprSQL(x)
	}
	return fmt.Sprint(v)
}

// Generator.SAFE_JSON_PATH_KEY_RE overrides (Python `$` also matches before a trailing newline,
// and `\w` is Unicode-aware).
var (
	genEBigQuerySafeJSONPathKeyRE = regexp.MustCompile(`^[\-\p{L}\p{N}_]*\n?$`)
	genEHiveSafeJSONPathKeyRE     = regexp.MustCompile(`^[_\-a-zA-Z][\-\p{L}\p{N}_]*\n?$`)
)

// genESafeJSONPathKeyRE mirrors self.SAFE_JSON_PATH_KEY_RE (not part of the generated settings):
// BigQueryGenerator and HiveGenerator override it; DatabricksGenerator resets it to SAFE_IDENTIFIER_RE.
func genESafeJSONPathKeyRE(g *Generator) *regexp.Regexp {
	switch {
	case g.d.Is("databricks"):
		return SAFE_IDENTIFIER_RE
	case g.d.Is("hive"):
		return genEHiveSafeJSONPathKeyRE
	case g.d.Is("bigquery"):
		return genEBigQuerySafeJSONPathKeyRE
	}
	return SAFE_IDENTIFIER_RE
}
