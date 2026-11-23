package sqlengine

import (
	"regexp"
	"strings"
)

func init() { registerCustomizer("trino", customizeTrino) }

func customizeTrino(d *Dialect) {
	P := d.P

	P.FUNCTIONS["VERSION"] = fromArgList(KCurrentVersion)

	P.FUNCTION_PARSERS["TRIM"] = func(p *Parser) *Expr { return p.parseTrim() }
	P.FUNCTION_PARSERS["JSON_QUERY"] = trinoParseJSONQuery
	P.FUNCTION_PARSERS["JSON_VALUE"] = func(p *Parser) *Expr { return p.parseJsonValue(true) }
	P.FUNCTION_PARSERS["LISTAGG"] = func(p *Parser) *Expr { return p.parseStringAgg() }

	P.h.parseCte = trinoParseCte

	for _, dt := range []DType{DT_TIMESTAMP, DT_TIMESTAMPTZ, DT_TIME, DT_TIMETZ} {
		P.TYPE_LITERAL_PARSERS[dt] = trinoParseTemporalLiteral
	}

	G := d.G

	T := G.TRANSFORMS
	T[KArraySum] = func(g *Generator, e *Expr) string {
		return "REDUCE(" + g.sqlKey(e, "this") + ", 0, (acc, x) -> acc + x, acc -> acc)"
	}
	T[KArrayUniqueAgg] = func(g *Generator, e *Expr) string {
		return "ARRAY_AGG(DISTINCT " + g.sqlKey(e, "this") + ")"
	}
	T[KCurrentVersion] = renameFunc("VERSION")
	T[KFromISO8601TimestampNanos] = renameFunc("FROM_ISO8601_TIMESTAMP_NANOS")
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		return groupconcatSQL(g, e, "LISTAGG", ",", true, true)
	}
	T[KLocationProperty] = func(g *Generator, e *Expr) string { return g.propertySQL(e) }
	T[KMerge] = mergeWithoutTargetSQL
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateQualify,
		transformEliminateDistinctOn,
		transformExplodeProjectionToUnnest(1),
		transformEliminateSemiAndAntiJoins,
		prestoAmendExplodedColumnTable,
	}, nil)
	T[KStabilityProperty] = func(g *Generator, e *Expr) string {
		if e.Name() == "IMMUTABLE" {
			return "DETERMINISTIC"
		}
		return "NOT DETERMINISTIC"
	}
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, true) }
	T[KTrim] = func(g *Generator, e *Expr) string { return trimSQL(g, e, "") }

	G.methods[KJSONExtract] = trinoJSONExtractSQL
	G.methods[KFunctionSpecification] = trinoFunctionspecificationSQL
	G.methods[KIfBlock] = trinoIfblockSQL
	G.methods[KCaseStatement] = trinoCasestatementSQL
	G.methods[KWhileBlock] = trinoWhileblockSQL
	G.methods[KLoopBlock] = trinoLoopblockSQL
	G.methods[KRepeatBlock] = trinoRepeatblockSQL
	G.methods[KLeave] = func(g *Generator, e *Expr) string { return "LEAVE " + g.sqlKey(e, "this") }
	G.methods[KIterate] = func(g *Generator, e *Expr) string { return "ITERATE " + g.sqlKey(e, "this") }
}

// trinoParseCte parses a WITH clause entry: FUNCTION <name>(...) starts an inline SQL function
// specification, as opposed to a CTE that happens to be named function (WITH function AS (...)).
// https://trino.io/docs/current/udf/sql.html
func trinoParseCte(p *Parser) *Expr {
	if p.matchNoAdvance(TK_FUNCTION) && p.s.ID_VAR_TOKENS.Has(p.next.Type) {
		p.advance(1)
		return trinoParseFunctionSpecification(p)
	}

	return p.baseParseCte()
}

func trinoParseFunctionSpecification(p *Parser) *Expr {
	this := p.parseUserDefinedFunction(TK_FUNCTION)

	// The WITH (...) properties are collected apart from the characteristics so that they are
	// generated as their own clause at the end
	var characteristics, properties []*Expr
	for {
		if p.match(TK_WITH) {
			parseProperty := func() *Expr { return p.parseKeyValueProperty(nil) }
			properties = append(properties, p.parseWrappedCSV(parseProperty, TK_COMMA, false)...)
			continue
		}

		if p.matchTextSeq("NOT", "DETERMINISTIC") {
			characteristics = append(characteristics, p.expression(New(KStabilityProperty, "this", LiteralString("VOLATILE"))))
			continue
		}

		characteristic := p.parseProperty()
		if !truthy(characteristic) {
			break
		}
		characteristics = append(characteristics, chunkAEnsureList(characteristic)...)
	}

	var characteristicsExpr, propertiesExpr *Expr
	if len(characteristics) > 0 {
		characteristicsExpr = p.expression(New(KProperties, "expressions", characteristics))
	}
	if len(properties) > 0 {
		propertiesExpr = p.expression(New(KProperties, "expressions", properties))
	}

	return p.expression(New(
		KFunctionSpecification,
		"this", this,
		"characteristics", characteristicsExpr,
		"properties", propertiesExpr,
		"expression", trinoParseRoutineStatement(p),
	))
}

// trinoParseRoutineStatements parses routine statements, across semicolon-separated chunks, up
// to and including one of the terminators. Unlike a top-level block it stops at a terminator
// even when tokens follow it: the enclosing query does (... END SELECT f(1)).
func trinoParseRoutineStatements(p *Parser, terminators ...string) []*Expr {
	statements := []*Expr{}

	for !p.matchTexts(terminators...) {
		if !p.curr.ok() {
			if p.chunkIndex >= len(p.chunks) {
				p.raiseError("Unexpected end of routine body", nil)
				break
			}

			p.advanceChunk()
		} else if !p.match(TK_SEMICOLON) {
			statement := trinoParseRoutineStatement(p)
			if statement == nil {
				break
			}

			statements = append(statements, statement)
		}
	}

	return statements
}

func trinoParseRoutineBlock(p *Parser) *Expr {
	p.match(TK_BEGIN)
	statements := trinoParseRoutineStatements(p, "END")
	statements = append(statements, New(KEndStatement))

	return p.expression(New(KBlock, "expressions", statements, "begin", true))
}

// trinoParseRoutineIf parses the rest of an IF routine statement, nesting each ELSEIF branch into
// the "false" slot of the previous branch.
func trinoParseRoutineIf(p *Parser) *Expr {
	parseBranch := func() *Expr {
		condition := p.parseDisjunction()
		p.matchTextSeq("THEN")
		trueBlock := p.expression(New(KBlock, "expressions", trinoParseRoutineStatements(p, "ELSEIF", "ELSE", "END")))
		return p.expression(New(KIfBlock, "this", condition, "true", trueBlock))
	}

	this := parseBranch()
	tail := this
	for pyUpperEq(p.prev.Text, "ELSEIF") {
		chunk, index := p.chunkIndex, p.index
		node := parseBranch()
		tail.Set("false", node)
		tail = node
		if p.chunkIndex == chunk && p.index == index {
			break
		}
	}

	if pyUpperEq(p.prev.Text, "ELSE") {
		tail.Set("false", p.expression(New(KBlock, "expressions", trinoParseRoutineStatements(p, "END"))))
	}

	p.matchTextSeq("IF")
	return this
}

func trinoParseRoutineCase(p *Parser) *Expr {
	this := p.parseDisjunction()

	parseBranch := func() *Expr {
		condition := p.parseDisjunction()
		p.matchTextSeq("THEN")
		trueBlock := p.expression(New(KBlock, "expressions", trinoParseRoutineStatements(p, "WHEN", "ELSE", "END")))
		return p.expression(New(KIf, "this", condition, "true", trueBlock))
	}

	ifs := []*Expr{}
	p.matchTextSeq("WHEN")
	for pyUpperEq(p.prev.Text, "WHEN") {
		chunk, index := p.chunkIndex, p.index
		ifs = append(ifs, parseBranch())
		if p.chunkIndex == chunk && p.index == index {
			break
		}
	}

	var defaultBlock *Expr
	if pyUpperEq(p.prev.Text, "ELSE") {
		defaultBlock = p.expression(New(KBlock, "expressions", trinoParseRoutineStatements(p, "END")))
	}

	p.matchTextSeq("CASE")
	return p.expression(New(KCaseStatement, "this", this, "ifs", ifs, "default", defaultBlock))
}

func trinoParseRoutineWhile(p *Parser, label *Expr) *Expr {
	condition := p.parseDisjunction()
	p.matchTextSeq("DO")
	body := p.expression(New(KBlock, "expressions", trinoParseRoutineStatements(p, "END")))
	p.matchTextSeq("WHILE")
	return p.expression(New(KWhileBlock, "this", condition, "body", body, "label", label))
}

func trinoParseRoutineLoop(p *Parser, label *Expr) *Expr {
	body := p.expression(New(KBlock, "expressions", trinoParseRoutineStatements(p, "END")))
	p.matchTextSeq("LOOP")
	return p.expression(New(KLoopBlock, "body", body, "label", label))
}

func trinoParseRoutineRepeat(p *Parser, label *Expr) *Expr {
	body := p.expression(New(KBlock, "expressions", trinoParseRoutineStatements(p, "UNTIL")))
	until := p.parseDisjunction()
	p.matchTextSeq("END", "REPEAT")
	return p.expression(New(KRepeatBlock, "body", body, "until", until, "label", label))
}

func trinoParseRoutineStatement(p *Parser) *Expr {
	// A "label:" may name a WHILE, LOOP or REPEAT block for ITERATE and LEAVE. Any non-reserved
	// keyword (SET, IF, ITERATE, ...) can be a label, so the colon is looked for first.
	var label *Expr
	if p.next.Type == TK_COLON {
		label = p.parseIdVar(true, nil)
		p.match(TK_COLON)
	}

	if p.matchNoAdvance(TK_BEGIN) {
		return trinoParseRoutineBlock(p)
	}

	if p.matchTextSeq("RETURN") {
		return p.expression(New(KReturn, "this", p.parseDisjunction()))
	}

	if p.matchTextSeq("IF") {
		return trinoParseRoutineIf(p)
	}

	if p.matchTextSeq("CASE") {
		return trinoParseRoutineCase(p)
	}

	if p.match(TK_DECLARE) {
		return p.parseDeclare()
	}

	if p.match(TK_SET) {
		return p.parseSet(false, false)
	}

	if p.matchTextSeq("ITERATE") {
		return p.expression(New(KIterate, "this", p.parseIdVar(true, nil)))
	}

	if p.matchTextSeq("LEAVE") {
		return p.expression(New(KLeave, "this", p.parseIdVar(true, nil)))
	}

	if p.matchTextSeq("WHILE") {
		return trinoParseRoutineWhile(p, label)
	}

	if p.matchTextSeq("LOOP") {
		return trinoParseRoutineLoop(p, label)
	}

	if p.matchTextSeq("REPEAT") {
		return trinoParseRoutineRepeat(p, label)
	}

	p.raiseError("Expected routine statement", nil)
	return nil
}

func trinoFunctionspecificationSQL(g *Generator, e *Expr) string {
	characteristicsSQL := ""
	if characteristics := e.ArgE("characteristics"); characteristics != nil {
		characteristicsSQL = g.properties(characteristics, " ", " ", "", false)
	}
	withSQL := ""
	if properties := e.ArgE("properties"); properties != nil {
		withSQL = " " + g.withProperties(properties)
	}
	body := g.sqlKey(e, "expression")
	return "FUNCTION " + g.sqlKey(e, "this") + characteristicsSQL + withSQL + " " + body
}

// trinoIfblockSQL renders an IF routine statement, flattening the ELSEIF branches nested in the
// "false" slots back into IF ... THEN ...; ELSEIF ... THEN ...; [ELSE ...;] END IF.
func trinoIfblockSQL(g *Generator, e *Expr) string {
	var branches []string
	node := e
	for node != nil && node.IsA(KIfBlock) {
		keyword := "IF"
		if len(branches) > 0 {
			keyword = "ELSEIF"
		}
		branches = append(branches, keyword+" "+g.sqlKey(node, "this")+" THEN "+g.sqlKey(node, "true")+";")
		node = node.ArgE("false")
	}

	if node != nil {
		branches = append(branches, "ELSE "+g.sql(node)+";")
	}

	return strings.Join(branches, " ") + " END IF"
}

func trinoCasestatementSQL(g *Generator, e *Expr) string {
	branches := []string{"CASE"}
	if this := g.sqlKey(e, "this"); this != "" {
		branches[0] = "CASE " + this
	}

	for _, node := range e.ArgL("ifs") {
		branches = append(branches, "WHEN "+g.sqlKey(node, "this")+" THEN "+g.sqlKey(node, "true")+";")
	}

	if defaultBlock := e.ArgE("default"); defaultBlock != nil {
		branches = append(branches, "ELSE "+g.sql(defaultBlock)+";")
	}

	branches = append(branches, "END CASE")
	return strings.Join(branches, " ")
}

func trinoRoutineLabelSQL(g *Generator, e *Expr) string {
	if label := e.ArgE("label"); label != nil {
		return g.sql(label) + ": "
	}
	return ""
}

func trinoWhileblockSQL(g *Generator, e *Expr) string {
	return trinoRoutineLabelSQL(g, e) + "WHILE " + g.sqlKey(e, "this") + " DO " + g.sqlKey(e, "body") + "; END WHILE"
}

func trinoLoopblockSQL(g *Generator, e *Expr) string {
	return trinoRoutineLabelSQL(g, e) + "LOOP " + g.sqlKey(e, "body") + "; END LOOP"
}

func trinoRepeatblockSQL(g *Generator, e *Expr) string {
	return trinoRoutineLabelSQL(g, e) + "REPEAT " + g.sqlKey(e, "body") + "; UNTIL " + g.sqlKey(e, "until") + " END REPEAT"
}

var trinoFractionalSecondsRE = regexp.MustCompile(`:\d+\.(\d+)`)

// trinoParseTemporalLiteral parses a time or timestamp literal such as TIMESTAMP '... 00:00:00.12'
// into a cast whose type has the literal's precision (here 2), unless the type sets one.
func trinoParseTemporalLiteral(p *Parser, this, dataType *Expr) *Expr {
	if len(dataType.Expressions()) == 0 {
		precision := 0
		if m := trinoFractionalSecondsRE.FindStringSubmatch(this.Name()); m != nil {
			precision = len([]rune(m[1]))
		}
		dataType.Set("expressions", []*Expr{New(KDataTypeParam, "this", LiteralInt(precision))})
	}
	return p.expression(New(KCast, "this", this, "to", dataType))
}

func trinoParseJSONQueryQuote(p *Parser) *Expr {
	if !(p.matchTextSeq("KEEP", "QUOTES") || p.matchTextSeq("OMIT", "QUOTES")) {
		return nil
	}

	option := pyUpper(p.tokens[p.index-2].Text)
	return p.expression(New(
		KJSONExtractQuote,
		"option", option,
		"scalar", p.matchTextSeq("ON", "SCALAR", "STRING"),
	))
}

// trinoParseJSONQuery parses the arguments of JSON_QUERY(json, path [wrapper option]
// [quote option] [ON condition]) into a JSONExtract with json_query set. The path is false, not
// nil, when there is no comma.
func trinoParseJSONQuery(p *Parser) *Expr {
	this := p.parseBitwise()
	var expression any = false
	if p.match(TK_COMMA) {
		expression = p.parseBitwise()
	}
	option := p.parseVarFromOptions(p.s.JSON_QUERY_OPTIONS, false)
	quote := trinoParseJSONQueryQuote(p)
	onCondition := p.parseOnCondition()
	return p.expression(New(
		KJSONExtract,
		"this", this,
		"expression", expression,
		"option", option,
		"json_query", true,
		"quote", quote,
		"on_condition", onCondition,
	))
}

func trinoJSONExtractSQL(g *Generator, e *Expr) string {
	if !e.ArgB("json_query") {
		return prestoJSONExtractSQL(g, e)
	}

	jsonPath := g.sqlKey(e, "expression")
	option := g.sqlKey(e, "option")
	if option != "" {
		option = " " + option
	}

	quote := g.sqlKey(e, "quote")
	if quote != "" {
		quote = " " + quote
	}

	onCondition := g.sqlKey(e, "on_condition")
	if onCondition != "" {
		onCondition = " " + onCondition
	}

	return g.fn(
		"JSON_QUERY",
		e.Arg("this"),
		jsonPath+option+quote+onCondition,
	)
}
