package sqlengine

import (
	"fmt"
	"regexp"
	"strings"
)

func (g *Generator) dateaddSQL(expression *Expr) string {
	return g.fn(
		"DATE_ADD",
		expression.Arg("this"),
		expression.Arg("expression"),
		genEUnitToStr(expression, "DAY"),
	)
}

func (g *Generator) arrayanySQL(expression *Expr) string {
	if g.s.CAN_IMPLEMENT_ARRAY_ANY {
		filtered := New(KArrayFilter, "this", expression.This(), "expression", expression.Expression())
		filteredNotEmpty := genEBinop(KNEQ, New(KArraySize, "this", filtered), LiteralInt(0))
		originalIsEmpty := genEBinop(KEQ, New(KArraySize, "this", expression.This()), LiteralInt(0))
		return g.sql(Paren(genECombine([]*Expr{originalIsEmpty, filteredNotEmpty}, KOr, true, true).Copy()))
	}

	// The base dialect keeps ARRAY_ANY as is, so it is not reported as unsupported there.
	if g.d.ClassName != "Dialect" {
		g.unsupported("ARRAY_ANY is unsupported")
	}

	return g.functionFallbackSQL(expression)
}

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

func (g *Generator) basePartitionrangeSQL(expression *Expr) string {
	low := g.sqlKey(expression, "this")
	high := g.sqlKey(expression, "expression")

	return low + " TO " + high
}

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

// baseConvertSQL transpiles T-SQL's CONVERT function into a cast, or into a string/time
// conversion when a known style is given.
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

	// T-SQL's CONVERT defaults an unsized text type to length 30.
	if seqGet(to.Expressions(), 0) == nil && g.s.PARAMETERIZABLE_TEXT_TYPES.Has(to.DTypeOf()) {
		to = New(KDataType, "this", to.DTypeOf(), "expressions", []*Expr{LiteralInt(30)}, "nested", false)
	}

	var transformed *Expr
	castKind := KTryCast
	if strict {
		castKind = KCast
	}

	if style.IsA(KLiteral) && style.IsInt() {
		styleValue := style.Name()
		convertedStyle, ok := MustDialect("tsql").S.CONVERT_FORMAT_MAPPING[styleValue]
		if !ok || convertedStyle == "" {
			g.unsupported("Unsupported T-SQL 'style' value: " + styleValue)
		}
		if !ok {
			// An unknown style is rendered as the string literal 'None'.
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

	if g.quoteJSONPathKeyUsingBrackets && g.s.JSON_PATH_BRACKETED_KEY_SUPPORTED {
		return "[" + this + "]"
	}
	return "." + this
}

func (g *Generator) baseJsonpathsubscriptSQL(expression *Expr) string {
	this := g.jsonPathPart(expression.Arg("this"))
	if this != "" {
		return "[" + this + "]"
	}
	return ""
}

func (g *Generator) simplifyUnlessLiteral(expression *Expr) *Expr {
	if !expression.IsA(KLiteral) {
		expression = simplifyExpr(expression, g.d)
	}

	return expression
}

// embedIgnoreNulls renders IGNORE/RESPECT NULLS (text) after its operand, or inside the aggregate
// call when the dialect requires that.
func (g *Generator) embedIgnoreNulls(expression *Expr, text string) string {
	this := expression.This()
	if len(g.s.RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS) > 0 && this.IsA(g.s.RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS...) {
		g.unsupported(fmt.Sprintf("RESPECT/IGNORE NULLS is not supported for %s in %s", this.Key(), g.d.ClassName))
		return g.sql(this)
	}

	if g.s.IGNORE_NULLS_IN_FUNC && !truthy(expression.MetaGet("inline")) {
		if g.s.IGNORE_NULLS_BEFORE_ORDER {
			// The first modifier here will be the one closest to the AggFunc's arg
			// (stable sort by: HavingMax < Order < Limit); modifiers of subqueries don't count
			var mod *Expr
			modRank := 3
			for x := range FindAllInScope(expression, KHavingMax, KOrder, KLimit) {
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

func (g *Generator) semicolonSQL(expression *Expr) string {
	return ""
}

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

func (g *Generator) maskingpolicycolumnconstraintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " USING (" + expressions + ")"
	}
	return "MASKING POLICY " + this + expressions
}

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

func (g *Generator) baseScopeResolution(rhs string, scopeName string) string {
	var scope any
	if scopeName != "" {
		scope = scopeName
	}
	return g.fn("SCOPE_RESOLUTION", scope, rhs)
}

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

func (g *Generator) baseParsejsonSQL(expression *Expr) string {
	// An empty PARSE_JSON_NAME means the dialect has no such function.
	if g.s.PARSE_JSON_NAME == "" {
		return g.sql(expression.Arg("this"))
	}

	return g.fn(g.s.PARSE_JSON_NAME, expression.Arg("this"), expression.Arg("expression"))
}

func (g *Generator) baseRandSQL(expression *Expr) string {
	lower := g.sqlKey(expression, "lower")
	upper := g.sqlKey(expression, "upper")

	if lower != "" && upper != "" {
		return "(" + upper + " - " + lower + ") * " + g.fn("RAND", expression.Arg("this")) + " + " + lower
	}
	return g.fn("RAND", expression.Arg("this"))
}

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

func (g *Generator) summarizeSQL(expression *Expr) string {
	table := ""
	if expression.ArgB("table") {
		table = " TABLE"
	}
	return "SUMMARIZE" + table + " " + g.sql(expression.Arg("this"))
}

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

func (g *Generator) skipjsoncolumnSQL(expression *Expr) string {
	regexpSQL := ""
	if expression.ArgB("regexp") {
		regexpSQL = " REGEXP"
	}
	return "SKIP" + regexpSQL + " " + g.sql(expression.Arg("expression"))
}

// conditionalinsertSQL renders one branch of a multi-table insert: WHEN cond THEN INTO ... or
// ELSE INTO ... (the branch insert without its INSERT keyword).
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

func (g *Generator) multitableinsertsSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	source := g.sqlKey(expression, "source")

	var sql string
	if kind != "" {
		expressions := g.seg(g.expressions(expression, exprsOpts{sep: strp2(" ")}))
		sql = "INSERT " + kind + expressions + g.seg(source)
	} else {
		// without a kind it is the FROM-first form: FROM x INSERT ... [INSERT ...]
		inserts := make([]string, 0, len(expression.Expressions()))
		for _, e := range expression.Expressions() {
			inserts = append(inserts, g.sql(e))
		}
		sql = "FROM " + source + g.seg(strings.Join(inserts, g.sep1()))
	}

	return g.prependCtes(expression, sql)
}

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

func (g *Generator) jsonextractquoteSQL(expression *Expr) string {
	scalar := ""
	if expression.ArgB("scalar") {
		scalar = " ON SCALAR STRING"
	}
	return g.sqlKey(expression, "option") + " QUOTES" + scalar
}

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

func (g *Generator) baseArrayaggSQL(expression *Expr) string {
	arrayAgg := g.functionFallbackSQL(expression)
	return g.addArrayaggNullFilter(arrayAgg, expression, expression.This())
}

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

func genEWrap(e *Expr, kind Kind) *Expr {
	if e.IsA(kind) {
		return Paren(e)
	}
	return e
}

func genEBinop(kind Kind, self *Expr, other *Expr) *Expr {
	this := self.Copy()
	o := other.Copy()
	if !this.IsA(kind) && !o.IsA(kind) {
		this = genEWrap(this, KBinary)
		o = genEWrap(o, KBinary)
	}
	return New(kind, "this", this, "expression", o)
}

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

func genENot(expression *Expr, copy bool) *Expr {
	this := expression
	if copy {
		this = expression.Copy()
	}
	return New(KNot, "this", genEWrap(this, KConnector))
}

// genEAlias aliases a copy of expression; alias is a string, an Identifier expression or nil.
func genEAlias(expression *Expr, alias any) *Expr {
	e := expression.Copy()

	var id *Expr
	switch a := alias.(type) {
	case string:
		id = ToIdentifier(a, nil)
	case *Expr:
		if a != nil {
			if !a.IsA(KIdentifier) {
				panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + a.Kind().Name()})
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

// genECast casts a copy of expression to the type to. A cast whose target type already is, or
// renders like, to in dialect d is returned unchanged. d == nil means the base dialect.
func genECast(expression *Expr, to DType, d *Dialect) *Expr {
	expr := expression.Copy()
	dataType := NewDataType(to)

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

func genEDropLastRune(s string) string {
	r := []rune(s)
	if len(r) == 0 {
		return ""
	}
	return string(r[:len(r)-1])
}

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

// Dialect-specific patterns for JSON path keys that can be rendered unbracketed. They accept one
// trailing newline before the end, and their word class is Unicode-aware (\p{L}, \p{N}, _).
var (
	genEBigQuerySafeJSONPathKeyRE = regexp.MustCompile(`^[\-\p{L}\p{N}_]*\n?$`)
	genEHiveSafeJSONPathKeyRE     = regexp.MustCompile(`^[_\-a-zA-Z][\-\p{L}\p{N}_]*\n?$`)
)

// genESafeJSONPathKeyRE returns the dialect's pattern for unbracketed JSON path keys (it is not
// part of the generator settings): BigQuery and Hive have their own, while Databricks, although
// derived from Hive, uses SAFE_IDENTIFIER_RE like the base dialect.
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
