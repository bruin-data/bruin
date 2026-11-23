package sqlengine

import (
	"math/big"
	"strings"
)

func init() { registerCustomizer("sqlite", customizeSQLite) }

func customizeSQLite(d *Dialect) {
	P := d.P

	P.FUNCTION_PARSERS["GROUP_CONCAT"] = func(p *Parser) *Expr { return p.parseStringAgg() }

	P.FUNCTIONS["DATETIME"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KAnonymous, "this", "DATETIME", "expressions", args)
	}
	P.FUNCTIONS["EDITDIST3"] = fromArgList(KLevenshtein)
	P.FUNCTIONS["JSON_EXTRACT"] = sqliteBuildJSONExtract
	P.FUNCTIONS["JSON_GROUP_ARRAY"] = fromArgList(KJSONArrayAgg)
	P.FUNCTIONS["JSON_GROUP_OBJECT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KJSONObjectAgg, "expressions", args)
	}
	P.FUNCTIONS["STRFTIME"] = sqliteBuildStrftime
	P.FUNCTIONS["SQLITE_VERSION"] = fromArgList(KCurrentVersion)
	P.FUNCTIONS["TIME"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KAnonymous, "this", "TIME", "expressions", args)
	}

	// USING accepts (and ignores) any property keyword arguments.
	P.PROPERTY_PARSERS["USING"] = func(p *Parser, _ propKwargs) any {
		return anyExpr(sqliteParseModuleProperty(p))
	}
	P.PROPERTY_PARSERS["VIRTUAL"] = noKwargsE(func(p *Parser) *Expr {
		return p.expression(New(KVirtualProperty))
	})
	P.PROPERTY_PARSERS["WITHOUT"] = noKwargsE(func(p *Parser) *Expr {
		if p.matchTextSeq("ROWID") {
			return p.expression(New(KWithoutRowidProperty))
		}
		return nil
	})

	P.STATEMENT_PARSERS[TK_ATTACH] = func(p *Parser) *Expr { return sqliteParseAttachDetach(p, true) }
	P.STATEMENT_PARSERS[TK_DETACH] = func(p *Parser) *Expr { return sqliteParseAttachDetach(p, false) }
	P.STATEMENT_PARSERS[TK_PRAGMA] = sqliteParsePragma

	// https://www.sqlite.org/lang_expr.html
	P.RANGE_PARSERS[TK_MATCH] = binaryRangeParser(KMatch, false)
	P.RANGE_PARSERS[TK_IS] = sqliteParseIs

	P.PRIMARY_PARSERS[TK_HEX_STRING] = sqliteParseHexString

	// Unary + removes column affinity, so it is kept rather than dropped
	// https://www.sqlite.org/datatype3.html#affinity_of_expressions
	P.UNARY_PARSERS[TK_PLUS] = func(p *Parser) *Expr { return p.expression(New(KPos, "this", p.parseUnary())) }

	P.h.parseUnique = sqliteParseUnique
	P.h.parseGeneratedAsIdentity = sqliteParseGeneratedAsIdentity
	P.h.parseIsRhs = sqliteParseIsRhs

	// ||, -> and ->> form a single left-associative precedence tier that binds tighter than
	// multiplication, so they are consumed by parseFactorOperand rather than as column operators
	// or in parseBitwise: https://sqlite.org/lang_expr.html
	delete(P.COLUMN_OPERATORS, TK_ARROW)
	delete(P.COLUMN_OPERATORS, TK_DARROW)
	arithmetic := map[TokenType]bool{}
	for _, ops := range []map[TokenType]Kind{P.BITWISE, P.TERM, P.FACTOR} {
		for tok := range ops {
			arithmetic[tok] = tok != TK_COLLATE
		}
	}
	P.h.parseFactorOperand = func(p *Parser) *Expr { return sqliteParseFactorOperand(p, arithmetic) }

	G := d.G

	// table options are separated by commas, e.g. STRICT, WITHOUT ROWID
	G.h.rootProperties = func(g *Generator, properties *Expr) string {
		return g.expressions(properties, exprsOpts{noIndent: true, sep: strp2(", ")})
	}

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
	T[KAnyValue] = anyValueToMaxSQL
	T[KChr] = renameFunc("CHAR")
	T[KCountIf] = countIfToSum
	T[KCreate] = transformPreprocess([]func(*Expr) *Expr{sqliteTransformCreate}, nil)
	T[KCurrentDate] = func(_ *Generator, _ *Expr) string { return "CURRENT_DATE" }
	T[KCurrentTime] = func(_ *Generator, _ *Expr) string { return "CURRENT_TIME" }
	T[KCurrentTimestamp] = func(_ *Generator, _ *Expr) string { return "CURRENT_TIMESTAMP" }
	T[KCurrentVersion] = func(_ *Generator, _ *Expr) string { return "SQLITE_VERSION()" }
	T[KColumnDef] = transformPreprocess([]func(*Expr) *Expr{sqliteGeneratedToAutoIncrement}, nil)
	T[KDateStrToDate] = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }
	T[KIf] = renameFunc("IIF")
	T[KILike] = noIlikeSQL
	jsonGroupArray := renameFunc("JSON_GROUP_ARRAY")
	T[KJSONArrayAgg] = func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "order", "null_handling", "return_type", "strict")
		return jsonGroupArray(g, e)
	}
	T[KJSONObjectAgg] = func(g *Generator, e *Expr) string { return g.jsonobjectSQL(e, "JSON_GROUP_OBJECT") }
	editdist3 := renameFunc("EDITDIST3")
	T[KLevenshtein] = func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "ins_cost", "del_cost", "sub_cost", "max_dist")
		return editdist3(g, e)
	}
	T[KLogicalOr] = renameFunc("MAX")
	T[KLogicalAnd] = renameFunc("MIN")
	T[KPivot] = noPivotSQL
	T[KRand] = renameFunc("RANDOM")
	T[KRegexpLike] = func(g *Generator, e *Expr) string { return g.binary(e, "REGEXP") }
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		sqliteOffsetToLimit,
		transformEliminateDistinctOn,
		transformEliminateQualify,
		transformEliminateSemiAndAntiJoins,
	}, nil)
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "INSTR", false, false, true)
	}
	T[KTableSample] = noTablesampleSQL
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }
	T[KTimeToStr] = func(g *Generator, e *Expr) string {
		return g.fn("STRFTIME", e.Arg("format"), e.Arg("this"))
	}
	T[KTryCast] = noTrycastSQL
	T[KTsOrDsToTimestamp] = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }

	// Per-kind overrides of base methods that are not hooks (only reached via dispatch)
	G.methods[KInsert] = sqliteInsertSQL
	G.methods[KDateAdd] = sqliteDateaddSQL
	G.methods[KDateSub] = sqliteDatesubSQL
	G.methods[KWindowSpec] = sqliteWindowspecSQL
	// IS and IS NOT are SQLite's null-safe comparisons
	G.methods[KNullSafeEQ] = func(g *Generator, e *Expr) string { return g.binary(e, "IS") }
	G.methods[KNullSafeNEQ] = func(g *Generator, e *Expr) string { return g.binary(e, "IS NOT") }

	// Overrides of hooked base methods
	G.h.castSQL = sqliteCastSQL
	G.h.transactionSQL = sqliteTransactionSQL
	G.h.ignorenullsSQL = sqliteIgnorenullsSQL
	G.h.respectnullsSQL = sqliteRespectnullsSQL
	G.h.computedcolumnconstraintSQL = sqliteComputedcolumnconstraintSQL

	// Per-kind methods that only SQLite defines
	G.methods[KBitwiseAndAgg] = sqliteBitwiseandaggSQL
	G.methods[KBitwiseOrAgg] = sqliteBitwiseoraggSQL
	G.methods[KBitwiseXorAgg] = sqliteBitwisexoraggSQL
	G.methods[KJSONExtract] = sqliteJsonextractSQL
	G.methods[KJSONExtractScalar] = sqliteJsonextractscalarSQL
	G.methods[KTrunc] = sqliteTruncSQL
	G.methods[KGenerateSeries] = sqliteGenerateseriesSQL
	G.methods[KDateDiff] = sqliteDatediffSQL
	G.methods[KGroupConcat] = sqliteGroupconcatSQL
	G.methods[KLeast] = sqliteLeastSQL
	G.methods[KGreatest] = sqliteGreatestSQL
	G.methods[KIsAscii] = sqliteIsasciiSQL
	G.methods[KCurrentSchema] = sqliteCurrentschemaSQL
}

// sqliteBuildStrftime builds STRFTIME(format[, value]) as TimeToStr, defaulting the value to the
// current timestamp (arity is then validated against the extended argument list).
func sqliteBuildStrftime(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		args = append(append(make([]*Expr, 0, 2), args...), New(KCurrentTimestamp))
		return WithValidateArgs(New(KTimeToStr, "this", New(KTsOrDsToTimestamp, "this", args[1]), "format", args[0]), args)
	}
	if len(args) == 2 {
		return New(KTimeToStr, "this", New(KTsOrDsToTimestamp, "this", args[1]), "format", args[0])
	}
	return New(KAnonymous, "this", "STRFTIME", "expressions", args)
}

// sqliteBuildJSONExtract builds JSON_EXTRACT: with a single path it returns an SQL value like
// ->> (a JSONExtractScalar marked json_subtype, since object/array results keep the JSON
// subtype), with several paths a JSON array (a JSONExtract).
// https://www.sqlite.org/json1.html#jex
func sqliteBuildJSONExtract(args []*Expr, d *Dialect) *Expr {
	if len(args) == 2 {
		extract := buildExtractJSONWithPath(KJSONExtractScalar)(args, d)
		extract.Set("json_subtype", true)
		return extract
	}
	return buildExtractJSONWithPath(KJSONExtract)(args, d)
}

// sqliteParseHexString parses a hex token: 0x<digits> is an integer, read as a signed 64-bit
// two's-complement value like SQLite does, and x'<digits>' stays a blob literal.
func sqliteParseHexString(p *Parser, token *Token) *Expr {
	runes := p.runes()
	if start := int(token.Start); start+1 < len(runes) && runes[start] == '0' && (runes[start+1] == 'x' || runes[start+1] == 'X') {
		value, ok := new(big.Int).SetString(strings.ReplaceAll(token.Text, "_", ""), 16)
		if !ok || !sqliteValidHexDigits(token.Text) {
			panic(&ValueError{Msg: "invalid literal for int() with base 16: " + pyRepr(token.Text)})
		}
		if value.Cmp(new(big.Int).Lsh(big.NewInt(1), 63)) >= 0 {
			value.Sub(value, new(big.Int).Lsh(big.NewInt(1), 64))
		}
		return p.expressionTok(LiteralNumber(value.String()), token)
	}

	return p.expressionTok(New(KHexString, "this", token.Text), token)
}

// sqliteValidHexDigits reports whether s is hex digits with single underscores only between
// digits.
func sqliteValidHexDigits(s string) bool {
	if s == "" || s[0] == '_' || s[len(s)-1] == '_' {
		return false
	}
	return !strings.Contains(s, "__")
}

// sqliteParseGeneratedAsIdentity parses GENERATED ... AS; the GENERATED ALWAYS AS (expr)
// [STORED | VIRTUAL] form becomes a computed column.
func sqliteParseGeneratedAsIdentity(p *Parser) *Expr {
	this := p.baseParseGeneratedAsIdentity()

	// Only an expression-bearing form is a computed column, so a true identity (AS IDENTITY) does
	// not consume a following storage keyword
	if this.IsA(KGeneratedAsIdentityColumnConstraint) && this.ArgE("expression") != nil {
		persisted := p.matchTexts("STORED", "VIRTUAL") && upperText(p.prev) == "STORED"
		return p.expression(New(KComputedColumnConstraint, "this", this.ArgE("expression"), "persisted", persisted))
	}

	return this
}

// sqliteParseIs parses IS / IS NOT. They compare any two values, treating NULLs as equal, so with
// a right side other than NULL, TRUE or FALSE they are the null-safe comparisons IS [NOT]
// DISTINCT FROM. A parenthesized NULL, TRUE or FALSE right side is unwrapped.
func sqliteParseIs(p *Parser, this *Expr) *Expr {
	this = p.parseIs(this)

	isNode, wrappedInNot := this, false
	if this.IsA(KNot) && this.This().IsA(KIs) {
		isNode, wrappedInNot = this.This(), true
	}

	if isNode.IsA(KIs) {
		if rhs := isNode.Expression().Unnest(); rhs.IsA(KNull, KBoolean) {
			isNode.Set("expression", rhs)
		} else {
			kind := KNullSafeEQ
			if wrappedInNot {
				kind = KNullSafeNEQ
			}
			this = New(kind, "this", isNode.This(), "expression", isNode.Expression())
		}
	}

	return this
}

// sqliteParseIsRhs parses the right operand of IS and IS [NOT] DISTINCT FROM: ordering
// comparisons bind above IS, but range predicates bind below it.
func sqliteParseIsRhs(p *Parser, distinct bool) *Expr {
	this := p.parseBitwise()
	for chunkDMatchKey(p, p.s.COMPARISON) {
		comments := p.prevComments
		kind := p.s.COMPARISON[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseBitwise()), comments)
	}
	return this
}

// sqliteParseFactorOperand parses an operand of a FACTOR operator: COLLATE-bound unary operands
// joined by ||, -> and ->>, left to right. A || chain followed by an arrow is parenthesized so
// dialects where the arrows bind tighter keep the grouping, and so is the whole tier when it is
// an operand of an arithmetic operator (in arithmetic), which binds looser elsewhere.
func sqliteParseFactorOperand(p *Parser, arithmetic map[TokenType]bool) *Expr {
	inArithmeticOperand := p.prev != nil && arithmetic[p.prev.Type]
	this := sqliteParseConcatOperand(p)
	parsedOp := false

	for p.curr.ok() {
		tok := p.curr.Type
		if tok != TK_DPIPE && tok != TK_ARROW && tok != TK_DARROW {
			break
		}
		if tok != TK_DPIPE && this.IsA(KDPipe) {
			this = p.expression(New(KParen, "this", this))
		}

		p.advance(1)
		expression := sqliteParseConcatOperand(p)
		switch tok {
		case TK_DPIPE:
			this = p.expression(New(KDPipe, "this", this, "expression", expression, "safe", !p.d.S.STRICT_STRING_CONCAT))
		case TK_ARROW:
			this = p.expression(New(
				KJSONExtract,
				"this", this,
				"expression", p.d.toJSONPath(expression),
				"only_json_types", p.s.JSON_ARROWS_REQUIRE_JSON_TYPE,
			))
		default:
			this = p.expression(New(
				KJSONExtractScalar,
				"this", this,
				"expression", p.d.toJSONPath(expression),
				"only_json_types", p.s.JSON_ARROWS_REQUIRE_JSON_TYPE,
				"scalar_only", p.d.S.JSON_EXTRACT_SCALAR_SCALAR_ONLY,
			))
		}
		parsedOp = true
	}

	if parsedOp && (inArithmeticOperand || (p.curr.ok() && arithmetic[p.curr.Type])) {
		this = p.expression(New(KParen, "this", this))
	}

	return this
}

// sqliteParseConcatOperand parses a unary expression and the COLLATE clauses after it, since
// COLLATE binds tighter than any binary operator.
func sqliteParseConcatOperand(p *Parser) *Expr {
	this := p.parseUnary()
	for p.match(TK_COLLATE) {
		collate := p.expression(New(KCollate, "this", this, "expression", p.parseUnary()))
		normalizeCollate(collate)
		this = collate
	}
	return this
}

func sqliteParseUnique(p *Parser) *Expr {
	// Do not consume more tokens if UNIQUE is used as a standalone constraint, e.g:
	// CREATE TABLE foo (bar TEXT UNIQUE REFERENCES baz ...)
	if _, ok := p.s.CONSTRAINT_PARSERS[upperText(p.curr)]; ok {
		return p.expression(New(KUniqueColumnConstraint))
	}

	return p.baseParseUnique()
}

func sqliteParseModuleProperty(p *Parser) *Expr {
	name := p.parseIdVar(true, nil)
	var expressions any
	if p.matchNoAdvance(TK_L_PAREN) {
		expressions = p.parseWrappedCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA, false)
	}
	return p.expression(New(KModuleProperty, "this", name, "expressions", expressions))
}

func sqliteParsePragma(p *Parser) *Expr {
	name := p.parseIdVar(true, nil)

	if p.match(TK_DOT) {
		name = New(KDot, "this", name, "expression", p.parseIdVar(true, nil))
	}

	p.match(TK_EQ)

	value := p.parseWrapped(func() *Expr {
		if v := p.parseUnary(); v != nil {
			return v
		}
		return p.parseVar(true, nil, false)
	}, true)

	if value != nil {
		return p.expression(New(KPragma, "this", New(KEQ, "this", name, "expression", value)))
	}

	return p.expression(New(KPragma, "this", name))
}

func sqliteParseAttachDetach(p *Parser, isAttach bool /* true for ATTACH */) *Expr {
	p.match(TK_DATABASE)
	this := p.parseExpression()

	if isAttach {
		return p.expression(New(KAttach, "this", this))
	}
	return p.expression(New(KDetach, "this", this))
}

// sqliteListRemove returns a copy of list without its first element structurally equal to x. It
// panics with a ValueError when there is none; the message keeps its historical wording.
func sqliteListRemove(list []*Expr, x *Expr) []*Expr {
	for i, y := range list {
		if y.Equal(x) {
			out := make([]*Expr, 0, len(list)-1)
			out = append(out, list[:i]...)
			return append(out, list[i+1:]...)
		}
	}
	panic(&ValueError{Msg: "list.remove(x): x not in list"})
}

// sqliteTransformCreate moves a single-column primary key to its column and enforces
// auto_increment on primary keys.
func sqliteTransformCreate(expression *Expr) *Expr {
	schema := expression.This()

	if expression.IsA(KCreate) && schema.IsA(KSchema) {
		defs := map[string]*Expr{}
		var defsOrder []string
		var primaryKey *Expr

		for _, e := range schema.Expressions() {
			if e.IsA(KColumnDef) {
				name := e.Name()
				if _, ok := defs[name]; !ok {
					defsOrder = append(defsOrder, name)
				}
				defs[name] = e
			} else if e.IsA(KPrimaryKey) {
				primaryKey = e
			}
		}

		if primaryKey != nil && len(primaryKey.Expressions()) == 1 {
			key := primaryKey.Expressions()[0].Name()
			column, ok := defs[key]
			if !ok {
				panic(&ValueError{Msg: "'" + key + "'"})
			}
			column.Append("constraints", New(KColumnConstraint, "kind", New(KPrimaryKeyColumnConstraint)))
			// Drop the table-level primary key without parent bookkeeping
			schema.SetArgRaw("expressions", sqliteListRemove(schema.Expressions(), primaryKey))
		}

		for _, name := range defsOrder {
			column := defs[name]
			primaryKeyIndex := -1
			var autoIncrement *Expr
			autoIncrementIndex := -1

			constraints := column.ArgL("constraints")
			for i, constraint := range constraints {
				kind := constraint.ArgE("kind")
				if kind.IsA(KPrimaryKeyColumnConstraint) {
					primaryKeyIndex = i
				} else if kind.IsA(KAutoIncrementColumnConstraint) {
					autoIncrement = constraint
					autoIncrementIndex = i
				}
			}

			if autoIncrement != nil && (primaryKeyIndex == -1 || autoIncrementIndex < primaryKeyIndex) {
				constraints = sqliteListRemove(constraints, autoIncrement)
				if primaryKeyIndex != -1 {
					// Insert before the primary key constraint (append when past the end)
					idx := min(primaryKeyIndex, len(constraints))
					out := make([]*Expr, 0, len(constraints)+1)
					out = append(out, constraints[:idx]...)
					out = append(out, autoIncrement)
					constraints = append(out, constraints[idx:]...)
				}
				column.SetArgRaw("constraints", constraints)
			}
		}
	}

	return expression
}

// sqliteGeneratedToAutoIncrement replaces a column's GENERATED ... AS IDENTITY constraint (and
// its NOT NULL) with AUTOINCREMENT. Expression-bearing forms are computed columns and are kept.
func sqliteGeneratedToAutoIncrement(expression *Expr) *Expr {
	if !expression.IsA(KColumnDef) {
		return expression
	}

	generated := expression.Find(KGeneratedAsIdentityColumnConstraint)

	if generated != nil && generated.ArgE("expression") == nil {
		generated.Parent().Pop()

		notNull := expression.Find(KNotNullColumnConstraint)
		if notNull != nil {
			notNull.Parent().Pop()
		}

		expression.Append("constraints", New(KColumnConstraint, "kind", New(KAutoIncrementColumnConstraint)))
	}

	return expression
}

// sqliteOffsetToLimit adds LIMIT -1 to a SELECT that has an OFFSET but no LIMIT, since SQLite
// only accepts OFFSET after LIMIT.
func sqliteOffsetToLimit(expression *Expr) *Expr {
	if !expression.IsA(KSelect) {
		return expression
	}

	offset := expression.Arg("offset")

	if limit := expression.ArgE("limit"); truthy(offset) && (limit == nil || limit.isLimitAll()) {
		expression.QueryLimit(MaybeParse("-1", KLimit, "LIMIT", nil), false)
	}

	return expression
}

// sqliteInsertSQL renders INSERT, turning INSERT IGNORE into INSERT OR IGNORE.
func sqliteInsertSQL(g *Generator, expression *Expr) string {
	if expression.ArgB("ignore") {
		expression.Set("ignore", false)
		expression.Set("alternative", "IGNORE")
	}

	return g.insertSQL(expression)
}

func sqliteBitwiseandaggSQL(g *Generator, expression *Expr) string {
	g.unsupported("BITWISE_AND aggregation is not supported in SQLite")
	return g.functionFallbackSQL(expression)
}

func sqliteBitwiseoraggSQL(g *Generator, expression *Expr) string {
	g.unsupported("BITWISE_OR aggregation is not supported in SQLite")
	return g.functionFallbackSQL(expression)
}

func sqliteBitwisexoraggSQL(g *Generator, expression *Expr) string {
	g.unsupported("BITWISE_XOR aggregation is not supported in SQLite")
	return g.functionFallbackSQL(expression)
}

// sqliteJsonextractscalarSQL renders JSONExtractScalar with the ->> operator, or as
// JSON_EXTRACT(x, path) when it came from a single-path JSON_EXTRACT: unlike ->>, JSON_EXTRACT
// keeps the JSON subtype of object/array results, observable when they feed another JSON
// function.
func sqliteJsonextractscalarSQL(g *Generator, expression *Expr) string {
	if expression.ArgB("json_subtype") {
		return g.fn("JSON_EXTRACT", expression.This(), expression.Expression())
	}
	return arrowJSONExtractSQL(g, expression)
}

func sqliteJsonextractSQL(g *Generator, expression *Expr) string {
	if len(expression.Expressions()) > 0 {
		return g.functionFallbackSQL(expression)
	}
	return arrowJSONExtractSQL(g, expression)
}

// sqliteDateaddSQL renders DATE_ADD as DATE(x, '<amount> <unit>'), or DATETIME for units below a
// day, which DATE would drop. Weeks become days, and an amount that is not a literal is
// concatenated with its unit: DATE(x, (amount) || ' <unit>').
func sqliteDateaddSQL(g *Generator, expression *Expr) string {
	modifier := expression.Expression()
	unit := expression.ArgE("unit")
	// An INTERVAL amount carries its own unit, e.g. DATE_ADD(d, INTERVAL 1 DAY);
	// unwrap it so the unit is not left inside the quoted modifier string.
	if modifier.IsA(KInterval) {
		if unit == nil {
			unit = modifier.ArgE("unit")
		}
		modifier = modifier.This()
	}

	if unit != nil && pyUpper(unit.Name()) == "WEEK" {
		modifier = dhBinop(KMul, modifier, 7)
		unit = VarExpr("DAY")
	}

	fn := "DATE"
	if unit != nil {
		switch pyUpper(unit.Name()) {
		case "HOUR", "MINUTE", "SECOND":
			fn = "DATETIME"
		}
	}

	modifier = modifier.Unnest()
	if unit != nil && !modifier.IsA(KLiteral) && !(modifier.IsA(KNeg) && modifier.This().IsNumber()) {
		return g.fn(fn, expression.Arg("this"), New(
			KDPipe,
			"this", ParenExpr(modifier, true),
			"expression", LiteralString(" "+unit.Name()),
		))
	}

	var modifierSQL string
	if modifier.IsString() {
		modifierSQL = modifier.Name()
	} else {
		modifierSQL = g.sql(modifier)
	}
	if unit != nil {
		modifierSQL = modifierSQL + " " + unit.Name()
	}
	return g.fn(fn, expression.Arg("this"), LiteralString(modifierSQL))
}

// sqliteDatesubSQL renders DATE_SUB, which SQLite lacks, as DATE_ADD of the negated amount.
func sqliteDatesubSQL(g *Generator, expression *Expr) string {
	amount := expression.Expression()
	unit := expression.ArgE("unit")
	if amount.IsA(KInterval) {
		if unit == nil {
			unit = amount.ArgE("unit")
		}
		amount = amount.This()
	}

	amount = amount.Unnest()
	var negated *Expr
	switch {
	case amount.IsA(KNeg):
		negated = amount.This()
	case amount.IsA(KLiteral):
		text := strings.TrimPrefix(amount.Name(), "+")
		if rest, ok := strings.CutPrefix(text, "-"); ok {
			text = rest
		} else {
			text = "-" + text
		}
		negated = LiteralString(text)
	case amount.IsA(KColumn, KFunc):
		negated = New(KNeg, "this", amount)
	default:
		negated = New(KNeg, "this", ParenExpr(amount, false))
	}

	return sqliteDateaddSQL(g, New(KDateAdd, "this", expression.This(), "expression", negated, "unit", unit))
}

// sqliteCastSQL renders CAST, using the DATE, DATETIME, TIME and JSON functions for those
// targets.
func sqliteCastSQL(g *Generator, expression *Expr, safePrefix string) string {
	switch {
	case expression.IsTypeOf(DT_DATE):
		return g.fn("DATE", expression.Arg("this"))
	case expression.IsTypeOf(DT_DATETIME, DT_TIMESTAMP, DT_TIMESTAMPNTZ):
		return g.fn("DATETIME", expression.Arg("this"))
	case expression.IsTypeOf(DT_TIME):
		return g.fn("TIME", expression.Arg("this"))
	case expression.IsTypeOf(DT_JSON, DT_JSONB):
		return g.fn("JSON", expression.Arg("this"))
	}

	return g.baseCastSQL(expression, "")
}

// sqliteTruncSQL renders TRUNC; a decimals argument is reported as unsupported.
//
// Note: SQLite's TRUNC always returns REAL (e.g., trunc(10.99) -> 10.0), not INTEGER.
// This creates a transpilation gap affecting division semantics, similar to Presto.
func sqliteTruncSQL(g *Generator, expression *Expr) string {
	dhUnsupportedArgs(g, expression, "decimals")
	return g.fn("TRUNC", expression.Arg("this"))
}

// sqliteGenerateseriesSQL renders GENERATE_SERIES; a column alias moves into a subquery that
// selects value under that alias.
func sqliteGenerateseriesSQL(g *Generator, expression *Expr) string {
	parent := expression.Parent()
	var alias *Expr
	if parent != nil {
		alias, _ = parent.Arg("alias").(*Expr)
	}

	var sql string
	if alias.IsA(KTableAlias) && len(alias.ArgL("columns")) > 0 {
		columnAlias := alias.ArgL("columns")[0]
		alias.Set("columns", nil)
		value := AliasExpr(MaybeParse("value", KNone, "", nil), columnAlias, nil, true)
		sql = g.sql(SelectExpr(value).SelectFrom(expression, true).QuerySubquery(nil, true))
	} else {
		sql = g.functionFallbackSQL(expression)
	}

	return sql
}

// sqliteDatediffSQL renders DATE_DIFF as the JULIANDAY difference scaled to the unit and cast to
// INTEGER.
func sqliteDatediffSQL(g *Generator, expression *Expr) string {
	unitE := expression.ArgE("unit")
	unit := "DAY"
	if unitE != nil {
		unit = pyUpper(unitE.Name())
	}

	sql := "(JULIANDAY(" + g.sqlKey(expression, "this") + ") - JULIANDAY(" + g.sqlKey(expression, "expression") + "))"

	switch unit {
	case "MONTH":
		sql = sql + " / 30.0"
	case "YEAR":
		sql = sql + " / 365.0"
	case "HOUR":
		sql = sql + " * 24.0"
	case "MINUTE":
		sql = sql + " * 1440.0"
	case "SECOND":
		sql = sql + " * 86400.0"
	case "MILLISECOND":
		sql = sql + " * 86400000.0"
	case "MICROSECOND":
		sql = sql + " * 86400000000.0"
	case "NANOSECOND":
		sql = sql + " * 86400000000000.0"
	case "DAY":
		// JULIANDAY already counts days
	default:
		g.unsupported("DATEDIFF unsupported for '" + unit + "'.")
	}

	return "CAST(" + sql + " AS INTEGER)"
}

// sqliteGroupconcatSQL renders GROUP_CONCAT([DISTINCT] x[, separator] [ORDER BY ...]). A window
// function cannot order its argument, so that ORDER BY is reported as unsupported and dropped, and
// a DISTINCT aggregate takes a single argument, so a comma separator (the default) is dropped.
// https://www.sqlite.org/lang_aggfunc.html#group_concat
func sqliteGroupconcatSQL(g *Generator, expression *Expr) string {
	node := expression
	if expression.Parent().IsA(KFilter) {
		node = expression.Parent()
	}
	window := node.Parent()

	if expression.This().IsA(KOrder) && window.IsA(KWindow) && window.This() == node {
		g.unsupported("SQLite GROUP_CONCAT window functions do not support argument ORDER BY")
		expression.Set("this", expression.This().This())
	}

	this := expression.This()
	if this.IsA(KOrder) {
		this = this.This()
	}
	if separator := expression.ArgE("separator"); this.IsA(KDistinct) && separator != nil && separator.Equal(LiteralString(",")) {
		expression.Set("separator", nil)
	}

	return groupconcatSQL(g, expression, "GROUP_CONCAT", "", false, false)
}

// sqliteLeastSQL renders LEAST as the multi-argument MIN(...), or as its only argument.
func sqliteLeastSQL(g *Generator, expression *Expr) string {
	if len(expression.Expressions()) > 0 {
		return renameFunc("MIN")(g, expression)
	}

	return g.sqlKey(expression, "this")
}

// sqliteGreatestSQL renders GREATEST as the multi-argument MAX(...), or as its only argument.
func sqliteGreatestSQL(g *Generator, expression *Expr) string {
	if len(expression.Expressions()) > 0 {
		return renameFunc("MAX")(g, expression)
	}

	return g.sqlKey(expression, "this")
}

func sqliteTransactionSQL(g *Generator, expression *Expr) string {
	this := ""
	if v := expression.Arg("this"); truthy(v) {
		this = " " + gchunkCPyStr(v)
	}
	return "BEGIN" + this + " TRANSACTION"
}

// sqliteIsasciiSQL renders IS_ASCII as a GLOB test for characters outside 0x01-0x7F.
func sqliteIsasciiSQL(g *Generator, expression *Expr) string {
	return "(NOT " + g.sql(expression.Arg("this")) + " GLOB CAST(x'2a5b5e012d7f5d2a' AS TEXT))"
}

func sqliteCurrentschemaSQL(g *Generator, expression *Expr) string {
	dhUnsupportedArgs(g, expression, "this")
	return "'main'"
}

func sqliteIgnorenullsSQL(g *Generator, expression *Expr) string {
	g.unsupported("SQLite does not support IGNORE NULLS.")
	return g.sql(expression.Arg("this"))
}

func sqliteRespectnullsSQL(g *Generator, expression *Expr) string {
	return g.sql(expression.Arg("this"))
}

// sqliteWindowspecSQL renders a window frame; a RANGE frame starting at CURRENT ROW without an
// end bound is rendered as RANGE CURRENT ROW.
func sqliteWindowspecSQL(g *Generator, expression *Expr) string {
	if pyUpper(expression.Text("kind")) == "RANGE" && pyUpper(expression.Text("start")) == "CURRENT ROW" &&
		expression.Arg("end") == nil {
		return "RANGE CURRENT ROW"
	}

	return g.windowspecSQL(expression)
}

func sqliteComputedcolumnconstraintSQL(g *Generator, e *Expr) string {
	if e.ArgB("data_type") {
		g.unsupported("SQLite generated columns do not support a data type")
	}

	this := e.This()
	thisSQL := g.sql(this)
	if !this.IsA(KParen) {
		thisSQL = "(" + thisSQL + ")"
	}
	storage := ""
	if e.ArgB("persisted") {
		storage = " STORED"
	}
	notNull := ""
	if e.ArgB("not_null") {
		notNull = " NOT NULL"
	}
	return "AS " + thisSQL + storage + notNull
}
