package sqlengine

import (
	"fmt"
)

// Generator chunk D (part 2): sqlglot/generator.py tag_sql .. lastday_sql, plus helpers.

// tag_sql (generator.py L4721)
func (g *Generator) tagSQL(expression *Expr) string {
	return genDPyStr(expression.Arg("prefix")) + g.sql(expression.Arg("this")) + genDPyStr(expression.Arg("postfix"))
}

// token_sql (generator.py L4724)
func (g *Generator) tokenSQL(tokenType TokenType) string {
	if s, ok := g.s.TOKEN_MAPPING[tokenType]; ok {
		return s
	}
	return tokenType.Name()
}

// userdefinedfunction_sql (generator.py L4727)
func (g *Generator) userdefinedfunctionSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressions := g.noIdentify(func() string { return g.expressions(expression, exprsOpts{}) })
	if expression.ArgB("wrapped") {
		expressions = g.wrap(expressions)
	} else {
		expressions = " " + expressions
	}
	if pyStrip(expressions) != "" {
		return this + expressions
	}
	return this
}

// macrooverloads_sql (generator.py L4735)
func (g *Generator) macrooverloadsSQL(expression *Expr) string {
	return g.expressions(expression, exprsOpts{flat: true})
}

// macrooverload_sql (generator.py L4738)
func (g *Generator) macrooverloadSQL(expression *Expr) string {
	params := g.noIdentify(func() string { return g.expressions(expression, exprsOpts{flat: true}) })
	body := g.sqlKey(expression, "this")
	prefix := ""
	if expression.ArgB("is_table") {
		prefix = "TABLE "
	}
	return "(" + params + ") AS " + prefix + body
}

// joinhint_sql (generator.py L4744)
func (g *Generator) joinhintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressions := g.expressions(expression, exprsOpts{flat: true})
	return this + "(" + expressions + ")"
}

// kwarg_sql (generator.py L4749)
func (g *Generator) kwargSQL(expression *Expr) string {
	return g.binary(expression, "=>")
}

// when_sql (generator.py L4752)
func (g *Generator) whenSQL(expression *Expr) string {
	matched := "NOT MATCHED"
	if expression.ArgB("matched") {
		matched = "MATCHED"
	}
	source := ""
	if g.s.MATCHED_BY_SOURCE && expression.ArgB("source") {
		source = " BY SOURCE"
	}
	condition := g.sqlKey(expression, "condition")
	if condition != "" {
		condition = " AND " + condition
	}

	thenValue := expression.Arg("then")
	thenExpression, _ := thenValue.(*Expr)
	var then string
	if thenExpression.IsA(KInsert) {
		this := g.sqlKey(thenExpression, "this")
		if this != "" {
			this = "INSERT " + this
		} else {
			this = "INSERT"
		}
		then = g.sqlKey(thenExpression, "expression")
		if then != "" {
			then = this + " VALUES " + then
		} else {
			then = this
		}
	} else if thenExpression.IsA(KUpdate) {
		if thenExpression.ArgE("expressions").IsA(KStar) {
			then = "UPDATE " + g.sqlKey(thenExpression, "expressions")
		} else {
			expressionsSQL := g.expressions(thenExpression, exprsOpts{})
			if expressionsSQL != "" {
				then = "UPDATE SET" + g.sep(" ") + expressionsSQL
			} else {
				then = "UPDATE"
			}
		}
	} else {
		then = g.sql(thenValue)
	}

	if thenExpression.IsA(KInsert, KUpdate) {
		where := g.sqlKey(thenExpression, "where")
		if where != "" && !g.s.SUPPORTS_MERGE_WHERE {
			kind := "UPDATE"
			if thenExpression.IsA(KInsert) {
				kind = "INSERT"
			}
			g.unsupported("WHERE clause in MERGE " + kind + " is not supported")
			where = ""
		}
		then = then + where
	}
	return "WHEN " + matched + source + condition + " THEN " + then
}

// whens_sql (generator.py L4782)
func (g *Generator) whensSQL(expression *Expr) string {
	return g.expressions(expression, exprsOpts{sep: strp2(" "), noIndent: true})
}

// merge_sql (generator.py L4785)
func (g *Generator) mergeSQL(expression *Expr) string {
	table := expression.This()
	tableAlias := ""

	hints := table.ArgL("hints")
	if len(hints) > 0 && table.Alias() != "" && hints[0].IsA(KWithTableHint) {
		// T-SQL syntax is MERGE ... <target_table> [WITH (<merge_hint>)] [[AS] table_alias]
		tableAlias = " AS " + g.sql(table.ArgE("alias").Pop())
	}

	this := g.sql(table)
	using := "USING " + g.sqlKey(expression, "using")
	whens := g.sqlKey(expression, "whens")

	on := g.sqlKey(expression, "on")
	if on != "" {
		on = "ON " + on
	}

	if on == "" {
		on = g.expressions(expression, exprsOpts{key: "using_cond"})
		if on != "" {
			on = "USING (" + on + ")"
		}
	}

	returning := g.sqlKey(expression, "returning")
	if returning != "" {
		whens = whens + returning
	}

	sep := g.sep(" ")

	return g.prependCtes(
		expression,
		"MERGE INTO "+this+tableAlias+sep+using+sep+on+sep+whens,
	)
}

// tochar_sql (generator.py L4816)
func (g *Generator) tocharSQL(expression *Expr) string {
	g.genDUnsupportedArgs(expression, "format")
	return g.sql(genDCast(expression.This(), DT_TEXT, nil))
}

// tonumber_sql (generator.py L4820)
func (g *Generator) baseTonumberSQL(expression *Expr) string {
	g.genDUnsupportedArgs(expression, "default")
	if !g.s.SUPPORTS_TO_NUMBER {
		g.unsupported("Unsupported TO_NUMBER function")
		return g.sql(genDCast(expression.This(), DT_DOUBLE, nil))
	}

	fmtArg := expression.Arg("format")
	if !truthy(fmtArg) {
		g.unsupported("Conversion format is required for TO_NUMBER")
		return g.sql(genDCast(expression.This(), DT_DOUBLE, nil))
	}

	return g.fn("TO_NUMBER", expression.Arg("this"), fmtArg)
}

// dictproperty_sql (generator.py L4833)
func (g *Generator) dictpropertySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	kind := g.sqlKey(expression, "kind")
	settingsSQL := g.expressions(expression, exprsOpts{key: "settings", sep: strp2(" ")})
	args := "()"
	if settingsSQL != "" {
		args = "(" + g.sep("") + settingsSQL + g.segSep(")", "")
	}
	return this + "(" + kind + args + ")"
}

// dictrange_sql (generator.py L4840)
func (g *Generator) dictrangeSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	max := g.sqlKey(expression, "max")
	min := g.sqlKey(expression, "min")
	return this + "(MIN " + min + " MAX " + max + ")"
}

// dictsubproperty_sql (generator.py L4846)
func (g *Generator) dictsubpropertySQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + " " + g.sqlKey(expression, "value")
}

// duplicatekeyproperty_sql (generator.py L4849)
func (g *Generator) duplicatekeypropertySQL(expression *Expr) string {
	return "DUPLICATE KEY (" + g.expressions(expression, exprsOpts{flat: true}) + ")"
}

// uniquekeyproperty_sql (generator.py L4853)
// https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/
func (g *Generator) baseUniquekeypropertySQL(expression *Expr, prefix string) string {
	return prefix + " (" + g.expressions(expression, exprsOpts{flat: true}) + ")"
}

// distributedbyproperty_sql (generator.py L4859)
// https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/#distribution_desc
func (g *Generator) distributedbypropertySQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " " + g.wrap(expressions)
	}
	buckets := g.sqlKey(expression, "buckets")
	kind := g.sqlKey(expression, "kind")
	if buckets != "" {
		buckets = " BUCKETS " + buckets
	}
	order := g.sqlKey(expression, "order")
	return "DISTRIBUTED BY " + kind + expressions + buckets + order
}

// oncluster_sql (generator.py L4868)
func (g *Generator) baseOnclusterSQL(expression *Expr) string {
	return ""
}

// clusteredbyproperty_sql (generator.py L4871)
func (g *Generator) clusteredbypropertySQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{key: "expressions", flat: true})
	sortedBy := g.expressions(expression, exprsOpts{key: "sorted_by", flat: true})
	if sortedBy != "" {
		sortedBy = " SORTED BY (" + sortedBy + ")"
	}
	buckets := g.sqlKey(expression, "buckets")
	return "CLUSTERED BY (" + expressions + ")" + sortedBy + " INTO " + buckets + " BUCKETS"
}

// anyvalue_sql (generator.py L4878)
func (g *Generator) baseAnyvalueSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	having := g.sqlKey(expression, "having")

	if having != "" {
		kind := "MIN"
		if expression.ArgB("max") {
			kind = "MAX"
		}
		this = this + " HAVING " + kind + " " + having
	}

	return g.fn("ANY_VALUE", this)
}

// querytransform_sql (generator.py L4887)
func (g *Generator) querytransformSQL(expression *Expr) string {
	transform := g.fn("TRANSFORM", genDExprsToAny(expression.Expressions())...)
	rowFormatBefore := g.sqlKey(expression, "row_format_before")
	if rowFormatBefore != "" {
		rowFormatBefore = " " + rowFormatBefore
	}
	recordWriter := g.sqlKey(expression, "record_writer")
	if recordWriter != "" {
		recordWriter = " RECORDWRITER " + recordWriter
	}
	using := " USING " + g.sqlKey(expression, "command_script")
	schema := g.sqlKey(expression, "schema")
	if schema != "" {
		schema = " AS " + schema
	}
	rowFormatAfter := g.sqlKey(expression, "row_format_after")
	if rowFormatAfter != "" {
		rowFormatAfter = " " + rowFormatAfter
	}
	recordReader := g.sqlKey(expression, "record_reader")
	if recordReader != "" {
		recordReader = " RECORDREADER " + recordReader
	}
	return transform + rowFormatBefore + recordWriter + using + schema + rowFormatAfter + recordReader
}

// indexconstraintoption_sql (generator.py L4902)
func (g *Generator) indexconstraintoptionSQL(expression *Expr) string {
	keyBlockSize := g.sqlKey(expression, "key_block_size")
	if keyBlockSize != "" {
		return "KEY_BLOCK_SIZE = " + keyBlockSize
	}

	using := g.sqlKey(expression, "using")
	if using != "" {
		return "USING " + using
	}

	parser := g.sqlKey(expression, "parser")
	if parser != "" {
		return "WITH PARSER " + parser
	}

	comment := g.sqlKey(expression, "comment")
	if comment != "" {
		return "COMMENT " + comment
	}

	visible := expression.Arg("visible")
	if visible != nil {
		if truthy(visible) {
			return "VISIBLE"
		}
		return "INVISIBLE"
	}

	engineAttr := g.sqlKey(expression, "engine_attr")
	if engineAttr != "" {
		return "ENGINE_ATTRIBUTE = " + engineAttr
	}

	secondaryEngineAttr := g.sqlKey(expression, "secondary_engine_attr")
	if secondaryEngineAttr != "" {
		return "SECONDARY_ENGINE_ATTRIBUTE = " + secondaryEngineAttr
	}

	g.unsupported("Unsupported index constraint option.")
	return ""
}

// checkcolumnconstraint_sql (generator.py L4934)
func (g *Generator) checkcolumnconstraintSQL(expression *Expr) string {
	enforced := ""
	if expression.ArgB("enforced") {
		enforced = " ENFORCED"
	}
	return "CHECK (" + g.sqlKey(expression, "this") + ")" + enforced
}

// indexcolumnconstraint_sql (generator.py L4938)
func (g *Generator) baseIndexcolumnconstraintSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = kind + " INDEX"
	} else {
		kind = "INDEX"
	}
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	indexType := g.sqlKey(expression, "index_type")
	if indexType != "" {
		indexType = " USING " + indexType
	}
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " (" + expressions + ")"
	}
	options := g.expressions(expression, exprsOpts{key: "options", sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}
	return kind + this + indexType + expressions + options
}

// nvl2_sql (generator.py L4951)
func (g *Generator) nvl2SQL(expression *Expr) string {
	if g.s.NVL2_SUPPORTED {
		return g.functionFallbackSQL(expression)
	}

	caseExpr := genDCaseWhen(
		New(KCase),
		genDNot(genDBinop(KIs, expression.This(), Null()), false),
		expression.ArgE("true"),
		false,
	)
	elseCond := expression.ArgE("false")
	if elseCond != nil {
		genDCaseElse(caseExpr, elseCond, false)
	}

	return g.sql(caseExpr)
}

// comprehension_sql (generator.py L4966)
func (g *Generator) comprehensionSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expr := g.sqlKey(expression, "expression")
	position := g.sqlKey(expression, "position")
	if position != "" {
		position = ", " + position
	}
	iterator := g.sqlKey(expression, "iterator")
	condition := g.sqlKey(expression, "condition")
	if condition != "" {
		condition = " IF " + condition
	}
	return this + " FOR " + expr + position + " IN " + iterator + condition
}

// columnprefix_sql (generator.py L4976)
func (g *Generator) columnprefixSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + "(" + g.sqlKey(expression, "expression") + ")"
}

// opclass_sql (generator.py L4979)
func (g *Generator) opclassSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + " " + g.sqlKey(expression, "expression")
}

// _ml_sql (generator.py L4982)
func (g *Generator) mlSQL(expression *Expr, name string) string {
	model := g.sqlKey(expression, "this")
	model = "MODEL " + model
	expr := expression.Arg("expression")
	var exprSQL any // None
	if truthy(expr) {
		s := g.sqlKey(expression, "expression")
		if x, ok := expr.(*Expr); ok && x.IsA(KTable) {
			s = "TABLE " + s
		}
		exprSQL = s
	}

	var parameters any // None
	if p := g.sqlKey(expression, "params_struct"); p != "" {
		parameters = p
	}

	return g.fn(name, model, exprSQL, parameters)
}

// predict_sql (generator.py L4996)
func (g *Generator) predictSQL(expression *Expr) string {
	return g.mlSQL(expression, "PREDICT")
}

// generateembedding_sql (generator.py L4999)
func (g *Generator) generateembeddingSQL(expression *Expr) string {
	name := "GENERATE_EMBEDDING"
	if expression.ArgB("is_text") {
		name = "GENERATE_TEXT_EMBEDDING"
	}
	return g.mlSQL(expression, name)
}

// generatetext_sql (generator.py L5003)
func (g *Generator) generatetextSQL(expression *Expr) string {
	return g.mlSQL(expression, "GENERATE_TEXT")
}

// generatetable_sql (generator.py L5006)
func (g *Generator) generatetableSQL(expression *Expr) string {
	return g.mlSQL(expression, "GENERATE_TABLE")
}

// generatebool_sql (generator.py L5009)
func (g *Generator) generateboolSQL(expression *Expr) string {
	return g.mlSQL(expression, "GENERATE_BOOL")
}

// generateint_sql (generator.py L5012)
func (g *Generator) generateintSQL(expression *Expr) string {
	return g.mlSQL(expression, "GENERATE_INT")
}

// generatedouble_sql (generator.py L5015)
func (g *Generator) generatedoubleSQL(expression *Expr) string {
	return g.mlSQL(expression, "GENERATE_DOUBLE")
}

// mltranslate_sql (generator.py L5018)
func (g *Generator) mltranslateSQL(expression *Expr) string {
	return g.mlSQL(expression, "TRANSLATE")
}

// mlforecast_sql (generator.py L5021)
func (g *Generator) mlforecastSQL(expression *Expr) string {
	return g.mlSQL(expression, "FORECAST")
}

// aiforecast_sql (generator.py L5024)
func (g *Generator) aiforecastSQL(expression *Expr) string {
	thisSQL := g.sqlKey(expression, "this")
	if expression.This().IsA(KTable) {
		thisSQL = "TABLE " + thisSQL
	}

	return g.fn(
		"FORECAST",
		thisSQL,
		expression.Arg("data_col"),
		expression.Arg("timestamp_col"),
		expression.Arg("model"),
		expression.Arg("id_cols"),
		expression.Arg("horizon"),
		expression.Arg("forecast_end_timestamp"),
		expression.Arg("confidence_level"),
		expression.Arg("output_historical_time_series"),
		expression.Arg("context_window"),
	)
}

// featuresattime_sql (generator.py L5043)
func (g *Generator) featuresattimeSQL(expression *Expr) string {
	thisSQL := g.sqlKey(expression, "this")
	if expression.This().IsA(KTable) {
		thisSQL = "TABLE " + thisSQL
	}

	return g.fn(
		"FEATURES_AT_TIME",
		thisSQL,
		expression.Arg("time"),
		expression.Arg("num_rows"),
		expression.Arg("ignore_feature_nulls"),
	)
}

// vectorsearch_sql (generator.py L5056)
func (g *Generator) vectorsearchSQL(expression *Expr) string {
	thisSQL := g.sqlKey(expression, "this")
	if expression.This().IsA(KTable) {
		thisSQL = "TABLE " + thisSQL
	}

	queryTable := g.sqlKey(expression, "query_table")
	if expression.ArgE("query_table").IsA(KTable) {
		queryTable = "TABLE " + queryTable
	}

	return g.fn(
		"VECTOR_SEARCH",
		thisSQL,
		expression.Arg("column_to_search"),
		queryTable,
		expression.Arg("query_column_to_search"),
		expression.Arg("top_k"),
		expression.Arg("distance_type"),
		expression.Arg("options"),
	)
}

// forin_sql (generator.py L5076)
func (g *Generator) forinSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressionSQL := g.sqlKey(expression, "expression")
	return "FOR " + this + " DO " + expressionSQL
}

// refresh_sql (generator.py L5081)
func (g *Generator) refreshSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	kind := ""
	if !expression.This().IsA(KLiteral) {
		kind = expression.Text("kind") + " "
	}
	return "REFRESH " + kind + this
}

// toarray_sql (generator.py L5086)
func (g *Generator) toarraySQL(expression *Expr) string {
	arg := expression.This()
	if arg.Type() == nil {
		arg = genDAnnotateTypes(arg, g.d)
	}

	if genDIsType(arg, DT_ARRAY) {
		return g.sql(arg)
	}

	condForNull := genDBinop(KIs, arg, Null())
	return g.sql(genDFunc("IF", condForNull, Null(), New(KArray, "expressions", []*Expr{arg})))
}

// tsordstotime_sql (generator.py L5099)
func (g *Generator) baseTsordstotimeSQL(expression *Expr) string {
	this := expression.This()
	timeFormat := g.formatTime(expression, nil, nil)

	if timeFormat != "" {
		return g.sql(
			genDCast(
				New(KStrToTime, "this", this, "format", expression.Arg("format")),
				DT_TIME,
				nil,
			),
		)
	}

	if this.IsA(KTsOrDsToTime) || genDIsType(this, DT_TIME) {
		return g.sql(this)
	}

	return g.sql(genDCast(this, DT_TIME, nil))
}

// tsordstotimestamp_sql (generator.py L5116)
func (g *Generator) tsordstotimestampSQL(expression *Expr) string {
	this := expression.This()
	if this.IsA(KTsOrDsToTimestamp) || genDIsType(this, DT_TIMESTAMP) {
		return g.sql(this)
	}

	return g.sql(genDCast(this, DT_TIMESTAMP, g.d))
}

// tsordstodatetime_sql (generator.py L5123)
func (g *Generator) tsordstodatetimeSQL(expression *Expr) string {
	this := expression.This()
	if this.IsA(KTsOrDsToDatetime) || genDIsType(this, DT_DATETIME) {
		return g.sql(this)
	}

	return g.sql(genDCast(this, DT_DATETIME, g.d))
}

// tsordstodate_sql (generator.py L5130)
func (g *Generator) tsordstodateSQL(expression *Expr) string {
	this := expression.This()
	timeFormat := g.formatTime(expression, nil, nil)
	safe := expression.Arg("safe")
	if timeFormat != "" && timeFormat != g.d.S.TIME_FORMAT && timeFormat != g.d.S.DATE_FORMAT {
		return g.sql(
			genDCast(
				New(KStrToTime, "this", this, "format", expression.Arg("format"), "safe", safe),
				DT_DATE,
				nil,
			),
		)
	}

	if this.IsA(KTsOrDsToDate) || genDIsType(this, DT_DATE) {
		return g.sql(this)
	}

	if truthy(safe) {
		return g.sql(New(KTryCast, "this", this, "to", New(KDataType, "this", DT_DATE)))
	}

	return g.sql(genDCast(this, DT_DATE, nil))
}

// unixdate_sql (generator.py L5150)
func (g *Generator) unixdateSQL(expression *Expr) string {
	return g.sql(
		genDFunc(
			"DATEDIFF",
			expression.This(),
			genDCast(LiteralString("1970-01-01"), DT_DATE, nil),
			"day",
		),
	)
}

// lastday_sql (generator.py L5160)
func (g *Generator) lastdaySQL(expression *Expr) string {
	if g.s.LAST_DAY_SUPPORTS_DATE_PART {
		return g.functionFallbackSQL(expression)
	}

	unit := expression.Text("unit")
	if unit != "" && unit != "MONTH" {
		g.unsupported("Date parts are not supported in LAST_DAY.")
	}

	return g.fn("LAST_DAY", expression.Arg("this"))
}

// ---------------------------------------------------------------------------------------------
// Chunk D helpers (ports of sqlglot.expressions builders / Expr methods not yet in the package).
// ---------------------------------------------------------------------------------------------

// genDPyStr mirrors Python's str(value) as used inside f-strings.
func genDPyStr(v any) string {
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
		// Expr.__str__ -> Expr.sql() (default dialect)
		return exprSQL(x)
	}
	return fmt.Sprint(v)
}

// genDArgSqls converts a list argument (of expressions or strings) into the `sqls` input of
// Generator.expressions, mirroring iteration over `expression.args.get(key)`.
func genDArgSqls(v any) []any {
	switch x := v.(type) {
	case []*Expr:
		out := make([]any, 0, len(x))
		for _, e := range x {
			out = append(out, e)
		}
		return out
	case []string:
		out := make([]any, 0, len(x))
		for _, s := range x {
			out = append(out, s)
		}
		return out
	case []any:
		return x
	}
	return nil
}

// genDExprsToAny converts []*Expr to []any for *args splats.
func genDExprsToAny(xs []*Expr) []any {
	out := make([]any, 0, len(xs))
	for _, x := range xs {
		out = append(out, x)
	}
	return out
}

// genDBaseDialect mirrors Dialect.get_or_raise(None).
func genDBaseDialect() *Dialect {
	d, err := GetDialect("")
	if err != nil {
		panic(genPanic{err})
	}
	return d
}

// genDMaybeParse mirrors exp.maybe_parse(value, dialect=dialect, copy=copy).
func genDMaybeParse(v any, dialect *Dialect, copy bool) *Expr {
	switch x := v.(type) {
	case *Expr:
		if x == nil {
			panic(genPanic{&ParseError{Msg: "SQL cannot be None"}})
		}
		if copy {
			return x.Copy()
		}
		return x
	case nil:
		panic(genPanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	sql := genDPyStr(v)
	if dialect == nil {
		dialect = genDBaseDialect()
	}
	e, err := dialect.ParseOne(sql, nil)
	if err != nil {
		panic(genPanic{err})
	}
	return e
}

// genDWrap mirrors exp._wrap(expression, kind).
func genDWrap(e *Expr, kind Kind) *Expr {
	if e.IsA(kind) {
		return New(KParen, "this", e)
	}
	return e
}

// genDBinop mirrors Expr._binop(klass, other): both operands are copied (other via convert).
func genDBinop(kind Kind, this, other *Expr) *Expr {
	this = this.Copy()
	other = other.Copy()
	if !this.IsA(kind) && !other.IsA(kind) {
		this = genDWrap(this, KBinary)
		other = genDWrap(other, KBinary)
	}
	return New(kind, "this", this, "expression", other)
}

// genDNot mirrors exp.not_(expression, copy=copy) (and Expr.not_(copy=copy)).
func genDNot(e *Expr, copy bool) *Expr {
	this := genDMaybeParse(e, nil, copy)
	return New(KNot, "this", genDWrap(this, KConnector))
}

// genDCombine mirrors exp._combine(expressions, operator, copy=copy, wrap=wrap)
// for expression inputs (used by exp.and_ / exp.or_).
func genDCombine(exprs []*Expr, operator Kind, copy, wrap bool) *Expr {
	var conditions []*Expr
	for _, x := range exprs {
		if x == nil {
			continue
		}
		conditions = append(conditions, genDMaybeParse(x, nil, copy))
	}

	this, rest := conditions[0], conditions[1:]
	if len(rest) > 0 && wrap {
		this = genDWrap(this, KConnector)
	}
	for _, x := range rest {
		if wrap {
			x = genDWrap(x, KConnector)
		}
		this = New(operator, "this", this, "expression", x)
	}
	return this
}

// genDCaseWhen mirrors Case.when(condition, then, copy=copy).
func genDCaseWhen(c *Expr, condition, then any, copy bool) *Expr {
	instance := c
	if copy && c != nil {
		instance = c.Copy()
	}
	instance.Append("ifs", New(
		KIf,
		"this", genDMaybeParse(condition, nil, copy),
		"true", genDMaybeParse(then, nil, copy),
	))
	return instance
}

// genDCaseElse mirrors Case.else_(condition, copy=copy).
func genDCaseElse(c *Expr, condition any, copy bool) *Expr {
	instance := c
	if copy && c != nil {
		instance = c.Copy()
	}
	instance.Set("default", genDMaybeParse(condition, nil, copy))
	return instance
}

// genDDataTypeIsType mirrors DataType.is_type(*dtypes) for DType arguments (check_nullable=False).
func genDDataTypeIsType(dt *Expr, dtypes ...DType) bool {
	if dt == nil {
		return false
	}
	selfThis := dt.Arg("this")
	for _, d := range dtypes {
		other := NewDataType(d)
		var matches bool
		if selfThis == any(DT_USERDEFINED) || d == DT_USERDEFINED {
			matches = dt.Equal(other)
		} else {
			matches = selfThis == any(d)
		}
		if matches {
			return true
		}
	}
	return false
}

// genDIsType mirrors Expr.is_type(*dtypes) (with the Cast and DataType overrides).
func genDIsType(e *Expr, dtypes ...DType) bool {
	if e == nil {
		return false
	}
	if e.IsA(KCast) {
		return genDDataTypeIsType(e.ArgE("to"), dtypes...)
	}
	if e.kind.isDataType() {
		return genDDataTypeIsType(e, dtypes...)
	}
	t := e.RawType()
	return t != nil && genDDataTypeIsType(t, dtypes...)
}

// genDIsTypeSet mirrors Expr.is_type(*SOME_TYPES).
func genDIsTypeSet(e *Expr, set DTypeSet) bool {
	return genDIsType(e, set.Items()...)
}

// genDCast mirrors exp.cast(expression, to, copy=True, dialect=dialect) for a DType target.
// dialect nil means None (the base dialect).
func genDCast(expression *Expr, to DType, dialect *Dialect) *Expr {
	expr := genDMaybeParse(expression, dialect, true)
	dataType := NewDataType(to)

	// dont re-cast if the expression is already a cast to the correct type
	if expr.IsA(KCast) {
		targetDialect := dialect
		if targetDialect == nil {
			targetDialect = genDBaseDialect()
		}
		typeMapping := targetDialect.G.TYPE_MAPPING

		existingCastType := expr.ArgE("to").DTypeOf()
		newCastType := dataType.DTypeOf()
		existingMapped, ok := typeMapping[existingCastType]
		if !ok {
			existingMapped = dtypeValues[existingCastType]
		}
		newMapped, ok := typeMapping[newCastType]
		if !ok {
			newMapped = dtypeValues[newCastType]
		}
		typesAreEquivalent := existingMapped == newMapped

		if genDIsType(expr, to) || typesAreEquivalent {
			return expr
		}
	}

	result := New(KCast, "this", expr, "to", dataType)
	result.SetType(dataType)
	return result
}

// genDTable mirrors exp.table_(table) where table is an Identifier (or None).
func genDTable(table *Expr) *Expr {
	var this *Expr
	if table != nil {
		if !table.IsA(KIdentifier) {
			panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + table.classRepr()})
		}
		this = table.Copy()
	}
	return New(KTable, "this", this, "db", nil, "catalog", nil, "alias", nil)
}

// genDFunc mirrors exp.func(name, *args) with the default dialect (copy=True).
func genDFunc(name string, args ...any) *Expr {
	dialect := genDBaseDialect()

	converted := make([]*Expr, 0, len(args))
	for _, a := range args {
		converted = append(converted, genDMaybeParse(a, dialect, true))
	}

	var function *Expr
	if constructor := dialect.P.FUNCTIONS[pyUpper(name)]; constructor != nil {
		function, converted = callFuncBuilder(constructor, converted, dialect)
	} else {
		function = New(KAnonymous, "this", name, "expressions", converted)
	}

	for _, msg := range function.ErrorMessages(converted) {
		panic(&ValueError{Msg: msg})
	}

	return function
}

// genDAnnotateTypes mirrors sqlglot.optimizer.annotate_types.annotate_types(expression, dialect=...).
func genDAnnotateTypes(e *Expr, dialect *Dialect) *Expr { return annotateTypes(e, dialect) }

// genDTrieEmpty mirrors Python falsiness of a trie dict.
func genDTrieEmpty(t *trie) bool {
	return t == nil || (len(t.children) == 0 && !t.end)
}
