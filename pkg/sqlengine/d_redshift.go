package sqlengine

import (
	"fmt"
	"strings"
	"sync"
)

// Redshift settings are cloned from Postgres's resolved settings, so only Redshift's own
// differences are applied here.

func init() { registerCustomizer("redshift", customizeRedshift) }

func customizeRedshift(d *Dialect) {
	customizeRedshiftParser(d)
	customizeRedshiftGenerator(d)
}

func redshiftBuildDateDelta(exprType Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		expr := New(
			exprType,
			"this", seqGet(args, 2),
			"expression", seqGet(args, 1),
			"unit", mapDatePart(seqGet(args, 0), nil),
		)
		if exprType == KTsOrDsAdd {
			expr.Set("return_type", New(KDataType, "this", DT_TIMESTAMP))
		}

		return expr
	}
}

// redshiftDateDeltaSQL switches to the underscored function (DATE_ADD, DATE_DIFF) when the unit
// is an expression rather than a date-part keyword, since only that form accepts one.
func redshiftDateDeltaSQL(name string) GenFunc {
	underscored := strings.Replace(name, "DATE", "DATE_", 1)
	return func(g *Generator, e *Expr) string {
		unit := e.ArgE("unit")
		if unit == nil || unit.IsA(KLiteral) {
			unit = unitToVar(e, "DAY")
		}
		fn := name
		if unit != nil && !unit.IsA(KVar) {
			fn = underscored
		}
		return g.fn(fn, unit, e.Arg("expression"), e.Arg("this"))
	}
}

func redshiftBytestringSQL(g *Generator, e *Expr) string {
	if !e.ArgB("is_bytes") {
		return g.sql(LiteralString(e.ThisS()))
	}
	return g.fn("FROM_HEX", LiteralString(postgresBytesHex(e.ThisS())))
}

func customizeRedshiftParser(d *Dialect) {
	P := d.P

	delete(P.FUNCTIONS, "GET_BIT")
	P.FUNCTIONS["ADD_MONTHS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KTsOrDsAdd,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"unit", VarChecked("month"),
			"return_type", New(KDataType, "this", DT_TIMESTAMP),
		)
	}
	P.FUNCTIONS["CONVERT_TIMEZONE"] = func(args []*Expr, _ *Dialect) *Expr {
		return buildConvertTimezone(args, "UTC")
	}
	P.FUNCTIONS["DATEADD"] = redshiftBuildDateDelta(KTsOrDsAdd)
	P.FUNCTIONS["DATE_ADD"] = redshiftBuildDateDelta(KTsOrDsAdd)
	P.FUNCTIONS["DATEDIFF"] = redshiftBuildDateDelta(KTsOrDsDiff)
	P.FUNCTIONS["DATE_DIFF"] = redshiftBuildDateDelta(KTsOrDsDiff)
	P.FUNCTIONS["GETDATE"] = fromArgList(KCurrentTimestamp)
	P.FUNCTIONS["LISTAGG"] = fromArgList(KGroupConcat)
	P.FUNCTIONS["REGEXP_SUBSTR"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KRegexpExtract,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"position", seqGet(args, 2),
			"occurrence", seqGet(args, 3),
			"parameters", seqGet(args, 4),
		)
	}
	P.FUNCTIONS["SPLIT_TO_ARRAY"] = func(args []*Expr, _ *Dialect) *Expr {
		expression := seqGet(args, 1)
		if expression == nil {
			expression = LiteralString(",")
		}
		return New(KStringToArray, "this", seqGet(args, 0), "expression", expression)
	}
	P.FUNCTIONS["ARRAY_CONTAINS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KArrayContains,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"check_null", seqGet(args, 2),
		)
	}
	P.FUNCTIONS["STRTOL"] = fromArgList(KFromBase)
	P.FUNCTIONS["TEXTLEN"] = fromArgList(KLength)

	P.NO_PAREN_FUNCTION_PARSERS["APPROXIMATE"] = func(p *Parser) *Expr { return redshiftParseApproximateCount(p) }
	P.NO_PAREN_FUNCTION_PARSERS["SYSDATE"] = func(p *Parser) *Expr {
		return p.expression(New(KCurrentTimestamp, "sysdate", true))
	}

	P.FUNCTION_PARSERS["OBJECT_TRANSFORM"] = func(p *Parser) *Expr { return redshiftParseObjectTransform(p) }

	P.h.parseTable = redshiftParseTable
	P.h.parseConvert = redshiftParseConvert
	P.h.parseProjections = redshiftParseProjections
}

func redshiftParseTable(p *Parser, schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr {
	// Redshift supports UNPIVOTing SUPER objects, e.g. `UNPIVOT foo.obj[0] AS val AT attr`
	unpivot := p.match(TK_UNPIVOT)
	table := p.baseParseTable(schema, joins, aliasTokens, parseBracket, isDbReference, false, false)

	if unpivot {
		return p.expression(New(KPivot, "this", table, "unpivot", true))
	}
	return table
}

func redshiftParseConvert(p *Parser, strict bool, safe bool) *Expr {
	var safeV any
	if safe {
		safeV = true
	}
	to := p.parseTypes(false, false, true, false)
	p.match(TK_COMMA)
	this := p.parseBitwise()
	return p.expression(New(KCast, "this", this, "to", to, "safe", safeV))
}

func redshiftParseObjectTransform(p *Parser) *Expr {
	this := p.parseColumn()
	keep := []*Expr{}
	set := []*Expr{}
	if p.match(TK_KEEP) {
		keep = p.parseCSV(p.parsePrimary, TK_COMMA)
	}
	if p.match(TK_SET) {
		set = p.parseCSV(p.parseExpression, TK_COMMA)
	}
	return p.expression(New(KObjectTransform, "this", this, "keep", keep, "set_", set))
}

// redshiftParseApproximateCount backs up to APPROXIMATE and returns nil for any function other
// than COUNT(DISTINCT x) and PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY x).
func redshiftParseApproximateCount(p *Parser) *Expr {
	index := p.index - 1
	fn := p.parseFunction(nil, false, true, false)

	if fn.IsA(KCount) && fn.This().IsA(KDistinct) {
		return p.expression(New(KApproxDistinct, "this", seqGet(fn.This().Expressions(), 0)))
	}
	if fn.IsA(KWithinGroup) && fn.This().IsA(KPercentileDisc) {
		ordered := seqGet(fn.Expression().Expressions(), 0)
		var this *Expr
		if ordered != nil {
			this = ordered.This()
		}
		return p.expression(New(KApproxQuantile, "this", this, "quantile", fn.This().This()))
	}
	p.retreat(index)
	return nil
}

func redshiftParseProjections(p *Parser) ([]*Expr, []*Expr) {
	projections, _ := p.baseParseProjections()
	if upperText(p.prev) == "EXCLUDE" && p.curr.ok() {
		p.retreat(p.index - 1)
	}

	// EXCLUDE clause always comes at the end of the projection list and applies to it as a whole
	exclude := []*Expr{}
	if p.matchTextSeq("EXCLUDE") {
		exclude = p.parseWrappedCSV(p.parseExpression, TK_COMMA, true)
	}

	if len(exclude) > 0 {
		expr := projections[len(projections)-1]
		if expr.IsA(KAlias) && pyUpper(expr.Alias()) == "EXCLUDE" {
			projections[len(projections)-1] = expr.This().Pop()
		}
	}

	return projections, exclude
}

func customizeRedshiftGenerator(d *Dialect) {
	G := d.G
	G.methods[KByteString] = redshiftBytestringSQL

	T := G.TRANSFORMS

	for _, k := range []Kind{KPivot, KParseJSON, KAnyValue, KLastDay, KSHA2, KGetbit, KRound, KTryCast} {
		delete(T, k)
	}

	T[KArrayConcat] = arrayConcatSQL("ARRAY_CONCAT")
	T[KConcat] = concatToDpipeSQL
	T[KConcatWs] = concatWsToDpipeSQL
	T[KApproxDistinct] = func(g *Generator, e *Expr) string {
		return "APPROXIMATE COUNT(DISTINCT " + g.sqlKey(e, "this") + ")"
	}
	T[KCurrentTimestamp] = func(g *Generator, e *Expr) string {
		if e.ArgB("sysdate") {
			return "SYSDATE"
		}
		return "GETDATE()"
	}
	T[KCurrentUserId] = func(g *Generator, e *Expr) string { return "CURRENT_USER_ID" }
	T[KDateAdd] = redshiftDateDeltaSQL("DATEADD")
	T[KDateDiff] = redshiftDateDeltaSQL("DATEDIFF")
	T[KDistKeyProperty] = func(g *Generator, e *Expr) string { return g.fn("DISTKEY", e.Arg("this")) }
	T[KDistStyleProperty] = func(g *Generator, e *Expr) string { return g.nakedProperty(e) }
	T[KExplode] = func(g *Generator, e *Expr) string { return redshiftExplodeSQL(g, e) }
	T[KFarmFingerprint] = renameFunc("FARMFINGERPRINT64")
	T[KFromBase] = renameFunc("STRTOL")
	T[KGeneratedAsIdentityColumnConstraint] = generatedasidentitycolumnconstraintSQL
	T[KJSONExtract] = jsonExtractSegments("JSON_EXTRACT_PATH_TEXT", true, "")
	T[KJSONExtractScalar] = jsonExtractSegments("JSON_EXTRACT_PATH_TEXT", true, "")
	T[KGroupConcat] = renameFunc("LISTAGG")
	T[KHex] = func(g *Generator, e *Expr) string {
		return g.fn("UPPER", g.fn("TO_HEX", g.sqlKey(e, "this")))
	}
	T[KRegexpExtract] = renameFunc("REGEXP_SUBSTR")
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateWindowClause,
		transformEliminateDistinctOn,
		transformEliminateSemiAndAntiJoins,
		transformUnqualifyUnnest,
		transformUnnestGenerateDateArrayUsingRecursiveCte,
	}, nil)
	T[KSortKeyProperty] = func(g *Generator, e *Expr) string {
		compound := ""
		if e.ArgB("compound") {
			compound = "COMPOUND "
		}
		var args []any
		for _, x := range e.ArgL("this") {
			args = append(args, x)
		}
		return compound + "SORTKEY(" + g.formatArgs(", ", args...) + ")"
	}
	T[KStartsWith] = func(g *Generator, e *Expr) string {
		return g.sql(e.Arg("this")) + " LIKE " + g.sql(e.Arg("expression")) + " || '%'"
	}
	T[KStringToArray] = renameFunc("SPLIT_TO_ARRAY")
	T[KTableSample] = noTablesampleSQL
	T[KTsOrDsAdd] = redshiftDateDeltaSQL("DATEADD")
	T[KTsOrDsDiff] = redshiftDateDeltaSQL("DATEDIFF")
	T[KUnixToTime] = func(g *Generator, e *Expr) string { return redshiftUnixToTimeSQL(g, e) }
	T[KSHA2Digest] = func(g *Generator, e *Expr) string {
		length := e.ArgE("length")
		if length == nil {
			length = LiteralInt(256)
		}
		return g.fn("SHA2", e.Arg("this"), length)
	}

	G.h.unnestSQL = redshiftUnnestSQL
	G.h.castSQL = redshiftCastSQL
	G.h.datatypeSQL = redshiftDatatypeSQL
	G.h.altersetSQL = redshiftAltersetSQL
	G.h.ignorenullsSQL = func(g *Generator, e *Expr) string { return g.baseIgnorenullsSQL(e) }
	G.h.respectnullsSQL = func(g *Generator, e *Expr) string { return g.baseRespectnullsSQL(e) }

	G.methods[KStPoint] = redshiftStpointSQL
	G.methods[KArrayContains] = redshiftArraycontainsSQL
	G.methods[KObjectTransform] = redshiftObjecttransformSQL
	G.methods[KApproxQuantile] = redshiftApproxquantileSQL
	G.methods[KArray] = redshiftArraySQL
	G.methods[KExplode] = redshiftExplodeSQL
}

func redshiftStpointSQL(g *Generator, expression *Expr) string {
	// ST_POINT only accepts 2 args in Redshift; use ST_MAKEPOINT for 3 or 4 args
	if expression.ArgB("z") || expression.ArgB("m") {
		return g.fn(
			"ST_MAKEPOINT",
			expression.Arg("this"),
			expression.Arg("expression"),
			expression.ArgE("z"),
			expression.ArgE("m"),
		)
	}
	return g.fn("ST_POINT", expression.Arg("this"), expression.Arg("expression"))
}

func redshiftArraycontainsSQL(g *Generator, expression *Expr) string {
	return g.fn(
		"ARRAY_CONTAINS",
		expression.Arg("this"),
		expression.Arg("expression"),
		expression.ArgE("check_null"),
	)
}

func redshiftObjecttransformSQL(g *Generator, expression *Expr) string {
	this := g.sqlKey(expression, "this")
	keep := g.expressions(expression, exprsOpts{key: "keep", flat: true})
	set := g.expressions(expression, exprsOpts{key: "set_", flat: true})
	keepSQL := ""
	if keep != "" {
		keepSQL = " KEEP " + keep
	}
	setSQL := ""
	if set != "" {
		setSQL = " SET " + set
	}
	return "OBJECT_TRANSFORM(" + this + keepSQL + setSQL + ")"
}

func redshiftApproxquantileSQL(g *Generator, expression *Expr) string {
	return "APPROXIMATE " + g.sql(
		New(
			KWithinGroup,
			"this", New(KPercentileDisc, "this", expression.Arg("quantile")),
			"expression", New(KOrder, "expressions", []*Expr{New(KOrdered, "this", expression.This())}),
		),
	)
}

func redshiftUnnestSQL(g *Generator, expression *Expr) string {
	args := expression.Expressions()
	numArgs := len(args)

	if numArgs != 1 {
		g.unsupported(fmt.Sprintf("Unsupported number of arguments in UNNEST: %d", numArgs))
		return ""
	}

	if expression.FindAncestor(KFrom, KJoin, KSelect).IsA(KSelect) {
		g.unsupported("Unsupported UNNEST when not used in FROM/JOIN clauses")
		return ""
	}

	arg := g.sql(seqGet(args, 0))

	alias := g.expressions(expression.ArgE("alias"), exprsOpts{key: "columns", flat: true})
	if alias != "" {
		return arg + " AS " + alias
	}
	return arg
}

func redshiftCastSQL(g *Generator, expression *Expr, safePrefix string) string {
	if dhIsType(expression, DT_JSON) {
		// Redshift doesn't support a JSON type, so casting to it is treated as a noop
		return g.sqlKey(expression, "this")
	}

	return postgresCastSQL(g, expression, safePrefix)
}

var (
	redshiftTextType     *Expr
	redshiftTextTypeOnce sync.Once
)

// redshiftDatatypeSQL maps TEXT without a precision to VARCHAR(max) and TEXT(n) to VARCHAR(n):
// Redshift turns TEXT into VARCHAR(255), while TEXT generally means VARCHAR of max length.
func redshiftDatatypeSQL(g *Generator, expression *Expr) string {
	redshiftTextTypeOnce.Do(func() { redshiftTextType = DataTypeBuild("text", nil, true, true) })
	if DataTypeIsType(expression, []any{redshiftTextType}, false) {
		expression.Set("this", DT_VARCHAR)
		precision := expression.Arg("expressions")

		if !truthy(precision) {
			expression.Append("expressions", VarChecked("MAX"))
		}
	}

	return postgresDatatypeSQL(g, expression)
}

func redshiftAltersetSQL(g *Generator, expression *Expr) string {
	exprs := g.expressions(expression, exprsOpts{flat: true})
	if exprs != "" {
		exprs = " TABLE PROPERTIES (" + exprs + ")"
	}
	location := g.sqlKey(expression, "location")
	if location != "" {
		location = " LOCATION " + location
	}
	fileFormat := g.expressions(expression, exprsOpts{key: "file_format", flat: true, sep: strp2(" ")})
	if fileFormat != "" {
		fileFormat = " FILE FORMAT " + fileFormat
	}

	return "SET" + exprs + location + fileFormat
}

func redshiftArraySQL(g *Generator, expression *Expr) string {
	if expression.ArgB("bracket_notation") {
		return postgresArraySQL(g, expression)
	}

	return renameFunc("ARRAY")(g, expression)
}

func redshiftExplodeSQL(g *Generator, expression *Expr) string {
	g.unsupported("Unsupported EXPLODE() function")
	return ""
}

func redshiftUnixToTimeSQL(g *Generator, expression *Expr) string {
	scale := expression.ArgE("scale")
	this := g.sql(expression.Arg("this"))

	if scale != nil && !scale.Equal(LiteralInt(0)) && scale.IsInt() {
		this = "(" + this + " / POWER(10, " + chunkDPyNumberStr(scale) + "))"
	}

	return "(TIMESTAMP 'epoch' + " + this + " * INTERVAL '1 SECOND')"
}
