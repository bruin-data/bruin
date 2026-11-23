package sqlengine

import (
	"iter"
	"strings"
)

// chunkCRowTokens lets baseParseTableSample accept ROW as a sampling method name.
var chunkCRowTokens = newTokenSet(TK_ROW)

// chunkCOptList converts an optional list into an argument value: a nil slice becomes an untyped
// nil (so that Set removes the key and New leaves it unset), anything else stays the list.
func chunkCOptList(l []*Expr) any {
	if l == nil {
		return nil
	}
	return l
}

// chunkCAnyExpr converts a possibly-nil expression into an untyped value (a nil *Expr becomes an
// untyped nil).
func chunkCAnyExpr(e *Expr) any {
	if e == nil {
		return nil
	}
	return e
}

// chunkCKwargs is an insertion-ordered set of key / value arguments for New, with helpers for
// keys that collect lists of expressions.
type chunkCKwargs struct {
	keys []string
	vals []any
}

func (k *chunkCKwargs) set(key string, v any) {
	for i, kk := range k.keys {
		if kk == key {
			k.vals[i] = v
			return
		}
	}
	k.keys = append(k.keys, key)
	k.vals = append(k.vals, v)
}

// joinQualifiers are the join arguments that qualify a join beyond its table.
var joinQualifiers = []string{"on", "using", "side", "kind", "method"}

func hasJoinQualifier(kwargs *chunkCKwargs) bool {
	for _, k := range joinQualifiers {
		if truthy(kwargs.get(k)) {
			return true
		}
	}
	return false
}

func (k *chunkCKwargs) get(key string) any {
	for i, kk := range k.keys {
		if kk == key {
			return k.vals[i]
		}
	}
	return nil
}

func (k *chunkCKwargs) list(key string) []*Expr {
	for i, kk := range k.keys {
		if kk == key {
			l, _ := k.vals[i].([]*Expr)
			return l
		}
	}
	l := []*Expr{}
	k.set(key, l)
	return l
}

func (k *chunkCKwargs) extendList(key string, items []*Expr) {
	l := k.list(key)
	k.set(key, append(l, items...))
}

func (k *chunkCKwargs) appendList(key string, item *Expr) {
	l := k.list(key)
	k.set(key, append(l, item))
}

func (k *chunkCKwargs) kv() []any {
	out := make([]any, 0, 2*len(k.keys))
	for i, kk := range k.keys {
		out = append(out, kk, k.vals[i])
	}
	return out
}

func (p *Parser) chunkCAliasTokensOr(aliasTokens *TokenSet) *TokenSet {
	if aliasTokens != nil && *aliasTokens != (TokenSet{}) {
		return aliasTokens
	}
	return &p.s.TABLE_ALIAS_TOKENS
}

func chunkCToIdentifier(name *Expr, copy bool) *Expr {
	if name == nil {
		return nil
	}
	if name.IsA(KIdentifier) {
		if copy {
			return name.Copy()
		}
		return name
	}
	panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + name.Kind().Name()})
}

// chunkCColumn builds a Column from identifier parts (col may also be a Star), wrapped in Dots
// for any extra fields.
func chunkCColumn(col, table, db, catalog *Expr, fields []*Expr, copy bool) *Expr {
	if !col.IsA(KStar) {
		col = chunkCToIdentifier(col, copy)
	}
	this := New(
		KColumn,
		"this", col,
		"table", chunkCToIdentifier(table, copy),
		"db", chunkCToIdentifier(db, copy),
		"catalog", chunkCToIdentifier(catalog, copy),
	)
	if len(fields) > 0 {
		exprs := []*Expr{this}
		for _, f := range fields {
			exprs = append(exprs, chunkCToIdentifier(f, copy))
		}
		this = DotBuild(exprs)
	}
	return this
}

// chunkCAlias aliases e with an Identifier (or nil): it sets e's own alias arg when its kind has
// one (except Window), and wraps e in an Alias otherwise.
func chunkCAlias(e *Expr, alias *Expr, copy bool) *Expr {
	if copy {
		e = e.Copy()
	}
	// the alias is always copied, whatever copy says
	alias = chunkCToIdentifier(alias, true)
	if e.Kind().hasArgType("alias") && !e.Is(KWindow) {
		e.Set("alias", alias)
		return e
	}
	return New(KAlias, "this", e, "alias", alias)
}

// chunkCTableToColumn converts a Table to a Column (catalog.db.table.name plus any further
// fields), keeping its alias. A Table wrapping a function or array yields that expression.
func chunkCTableToColumn(t *Expr, copy bool) *Expr {
	parts := t.Parts()
	lastPart := parts[len(parts)-1]

	var col *Expr
	if lastPart.IsA(KIdentifier) {
		n := len(parts)
		if n > 4 {
			n = 4
		}
		rev := make([]*Expr, 4)
		for i := range n {
			rev[i] = parts[n-1-i]
		}
		var fields []*Expr
		if len(parts) > 4 {
			fields = parts[4:]
		}
		col = chunkCColumn(rev[0], rev[1], rev[2], rev[3], fields, copy)
	} else {
		// This branch will be reached if a function or array is wrapped in a `Table`
		col = lastPart
	}

	if alias := t.ArgE("alias"); alias != nil {
		col = chunkCAlias(col, alias.This(), copy)
	}
	return col
}

func chunkCUnpivotTarget(e *Expr) *Expr {
	// UNPIVOT's pre-FOR values and FOR field are new output names, not column references.
	if e.IsA(KColumn) && e.TableName() == "" {
		return e.This()
	}
	if e.IsA(KTuple) {
		exprs := e.Expressions()
		out := make([]*Expr, len(exprs))
		for i, x := range exprs {
			out[i] = chunkCUnpivotTarget(x)
		}
		e.Set("expressions", out)
	}
	return e
}

// chunkCProduct returns the Cartesian product of lists, in order (the last list varies
// fastest).
func chunkCProduct(lists [][]string) [][]string {
	result := [][]string{{}}
	for _, l := range lists {
		var next [][]string
		for _, prefix := range result {
			for _, item := range l {
				combo := make([]string, 0, len(prefix)+1)
				combo = append(combo, prefix...)
				combo = append(combo, item)
				next = append(next, combo)
			}
		}
		result = next
	}
	return result
}

// parseMatchRecognizeMeasure parses one MEASURES item of MATCH_RECOGNIZE: [FINAL | RUNNING] <expr>.
// window_frame is false (not nil) when neither keyword is present.
func (p *Parser) parseMatchRecognizeMeasure() *Expr {
	var windowFrame any = false
	if p.matchTexts("FINAL", "RUNNING") {
		windowFrame = upperText(p.prev)
	}
	return p.expression(New(
		KMatchRecognizeMeasure,
		"window_frame", windowFrame,
		"this", p.parseExpression(),
	))
}

// chunkCAdvanceAnyText consumes any token and returns its text. When no token is left it panics
// with a ValueError; the message keeps its historical wording ("'NoneType' object has no
// attribute 'text'").
func chunkCAdvanceAnyText(p *Parser) string {
	tok := p.advanceAny(false)
	if tok == nil {
		panic(&ValueError{Msg: "'NoneType' object has no attribute 'text'"})
	}
	return tok.Text
}

// parseMatchRecognize parses MATCH_RECOGNIZE (...) with its PARTITION BY, ORDER BY, MEASURES,
// rows-per-match, AFTER MATCH SKIP, PATTERN and DEFINE clauses, and an optional alias. The
// PATTERN is kept as raw SQL text.
func (p *Parser) parseMatchRecognize() *Expr {
	if !p.match(TK_MATCH_RECOGNIZE) {
		return nil
	}

	p.matchLParen(nil)

	partition := p.parsePartitionBy()
	order := p.parseOrder(nil, false)

	var measures any
	if p.matchTextSeq("MEASURES") {
		measures = p.parseCSV(p.parseMatchRecognizeMeasure, TK_COMMA)
	}

	var rows *Expr
	if p.matchTextSeq("ONE", "ROW", "PER", "MATCH") {
		rows = VarExpr("ONE ROW PER MATCH")
	} else if p.matchTextSeq("ALL", "ROWS", "PER", "MATCH") {
		text := "ALL ROWS PER MATCH"
		if p.matchTextSeq("SHOW", "EMPTY", "MATCHES") {
			text += " SHOW EMPTY MATCHES"
		} else if p.matchTextSeq("OMIT", "EMPTY", "MATCHES") {
			text += " OMIT EMPTY MATCHES"
		} else if p.matchTextSeq("WITH", "UNMATCHED", "ROWS") {
			text += " WITH UNMATCHED ROWS"
		}
		rows = VarExpr(text)
	}

	var after *Expr
	if p.matchTextSeq("AFTER", "MATCH", "SKIP") {
		text := "AFTER MATCH SKIP"
		if p.matchTextSeq("PAST", "LAST", "ROW") {
			text += " PAST LAST ROW"
		} else if p.matchTextSeq("TO", "NEXT", "ROW") {
			text += " TO NEXT ROW"
		} else if p.matchTextSeq("TO", "FIRST") {
			text += " TO FIRST " + chunkCAdvanceAnyText(p)
		} else if p.matchTextSeq("TO", "LAST") {
			text += " TO LAST " + chunkCAdvanceAnyText(p)
		}
		after = VarExpr(text)
	}

	var pattern *Expr
	if p.matchTextSeq("PATTERN") {
		p.matchLParen(nil)

		if !p.curr.ok() {
			p.raiseError("Expecting )", p.curr)
		}

		paren := 1
		start := p.curr
		var end *Token

		for p.curr.ok() && paren > 0 {
			if p.curr.Type == TK_L_PAREN {
				paren++
			}
			if p.curr.Type == TK_R_PAREN {
				paren--
			}

			end = p.prev
			p.advance(1)
		}

		if paren > 0 {
			p.raiseError("Expecting )", p.curr)
		}

		pattern = VarExpr(p.findSQL(start, end))
	}

	var define any
	if p.matchTextSeq("DEFINE") {
		define = p.parseCSV(p.parseNameAsExpression, TK_COMMA)
	}

	p.matchRParen(nil)

	return p.expression(New(
		KMatchRecognize,
		"partition_by", partition,
		"order", order,
		"measures", measures,
		"rows", rows,
		"after", after,
		"pattern", pattern,
		"define", define,
		"alias", p.parseTableAlias(nil),
	))
}

// baseParseLateral parses LATERAL [VIEW] [OUTER] ... and CROSS / OUTER APPLY ..., whose target is a
// subquery, UNNEST, function call or (dotted) name. cross_apply is true for CROSS APPLY, false
// for OUTER APPLY and nil otherwise.
func (p *Parser) baseParseLateral() *Expr {
	var crossApply any // nil, true or false
	if p.matchPair(TK_CROSS, TK_APPLY) {
		crossApply = true
	} else if p.matchPair(TK_OUTER, TK_APPLY) {
		crossApply = false
	}

	var this *Expr
	var view, outer any
	if crossApply != nil {
		this = p.parseSelect(false, true, true, true, true, nil)
		view = nil
		outer = nil
	} else if p.match(TK_LATERAL) {
		this = p.parseSelect(false, true, true, true, true, nil)
		view = p.match(TK_VIEW)
		outer = p.match(TK_OUTER)
	} else {
		return nil
	}

	if this == nil {
		this = p.parseUnnest(true)
		if this == nil {
			this = p.parseFunction(nil, false, true, false)
		}
		if this == nil {
			this = p.parseIdVar(false, nil)
		}

		for p.match(TK_DOT) {
			expression := p.parseFunction(nil, false, true, false)
			if expression == nil {
				expression = p.parseIdVar(false, nil)
			}
			this = New(KDot, "this", this, "expression", expression)
		}
	}

	var ordinality any // nil, true or false
	var tableAlias *Expr

	if truthy(view) {
		table := p.parseIdVar(false, nil)
		columns := []*Expr{}
		if p.match(TK_ALIAS) {
			columns = p.parseCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA)
		}
		tableAlias = p.expression(New(KTableAlias, "this", table, "columns", columns))
	} else if this.IsA(KSubquery, KUnnest) && this.Alias() != "" {
		// We move the alias from the lateral's child node to the lateral itself
		tableAlias = this.ArgE("alias").Pop()
	} else {
		ordinality = p.matchPair(TK_WITH, TK_ORDINALITY)
		tableAlias = p.parseTableAlias(nil)
	}

	return p.expression(New(
		KLateral,
		"this", this,
		"view", view,
		"outer", outer,
		"alias", tableAlias,
		"cross_apply", crossApply,
		"ordinality", ordinality,
	))
}

func (p *Parser) parseStream() *Expr {
	index := p.index
	if p.match(TK_STREAM) {
		if this := p.tryParseExpr(func() *Expr {
			return p.parseTable(false, false, nil, false, false, false, false)
		}, false); this != nil {
			return p.expression(New(KStream, "this", this))
		}
		p.retreat(index)
	}
	return nil
}

// baseParseJoinParts consumes the optional join method, side and kind tokens, in that order.
func (p *Parser) baseParseJoinParts() (*Token, *Token, *Token) {
	var method, side, kind *Token
	if p.matchSet(p.s.JOIN_METHODS) {
		method = p.prev
	}
	if p.matchSet(p.s.JOIN_SIDES) {
		side = p.prev
	}
	if p.matchSet(p.s.JOIN_KINDS) {
		kind = p.prev
	}
	return method, side, kind
}

func (p *Parser) parseUsingIdentifiers() []*Expr {
	parseColumnAsIdentifier := func() *Expr {
		this := p.parseColumn()
		if this.IsA(KColumn) {
			return this.This()
		}
		return this
	}

	return p.parseWrappedCSV(parseColumnAsIdentifier, TK_COMMA, true)
}

// baseParseJoin parses one join: a comma join, or [method] [side] [kind] [DIRECTED] [hint]
// JOIN / APPLY <table> with MATCH_CONDITION, ON or USING, and pivots. It returns nil when no
// join follows. Joins nested after the joined table attach to it when an ON / USING follows them.
func (p *Parser) baseParseJoin(skipJoinToken bool, parseBracket bool, aliasTokens *TokenSet) *Expr {
	if p.match(TK_COMMA) {
		table := p.tryParseExpr(func() *Expr {
			return p.parseTable(false, false, aliasTokens, false, false, false, false)
		}, false)
		var crossJoin *Expr
		if table != nil {
			crossJoin = p.expression(New(KJoin, "this", table))
		}

		if crossJoin != nil && p.s.JOINS_HAVE_EQUAL_PRECEDENCE {
			crossJoin.Set("kind", "CROSS")
		}

		return crossJoin
	}

	index := p.index
	method, side, kind := p.parseJoinParts()
	directed := p.s.SUPPORTS_DIRECTED_JOIN && p.matchTextSeq("DIRECTED")
	hint := ""
	if p.matchTextSet(p.s.JOIN_HINTS) {
		hint = p.prev.Text
	}
	join := p.match(TK_JOIN) || (kind.ok() && kind.Type == TK_STRAIGHT_JOIN)
	joinComments := p.prevComments

	if !skipJoinToken && !join {
		p.retreat(index)
		kind = nil
		method = nil
		side = nil
	}

	outerApply := p.matchPairNoAdvance(TK_OUTER, TK_APPLY)
	crossApply := p.matchPairNoAdvance(TK_CROSS, TK_APPLY)

	if !skipJoinToken && !join && !outerApply && !crossApply {
		return nil
	}

	kwargs := &chunkCKwargs{}
	kwargs.set("this", p.parseTable(false, false, aliasTokens, parseBracket, false, false, false))
	if kind.ok() && kind.Type == TK_ARRAY && p.match(TK_COMMA) {
		kwargs.set("expressions", p.parseCSV(func() *Expr {
			return p.parseTable(false, false, aliasTokens, parseBracket, false, false, false)
		}, TK_COMMA))
	}

	if method.ok() {
		kwargs.set("method", upperText(method))
	}
	if side.ok() {
		kwargs.set("side", upperText(side))
	}
	if kind.ok() {
		kwargs.set("kind", upperText(kind))
	}
	if hint != "" {
		kwargs.set("hint", hint)
	}

	if p.match(TK_MATCH_CONDITION) {
		kwargs.set("match_condition", p.parseWrapped(func() *Expr { return p.parseComparison(nil) }, false))
	}

	thisExpr, _ := kwargs.get("this").(*Expr)
	if p.match(TK_ON) {
		kwargs.set("on", p.parseDisjunction())
	} else if p.match(TK_USING) {
		kwargs.set("using", p.parseUsingIdentifiers())
	} else if !method.ok() &&
		!(outerApply || crossApply) &&
		!thisExpr.IsA(KUnnest) &&
		!(kind.ok() && (kind.Type == TK_CROSS || kind.Type == TK_ARRAY)) {
		index = p.index
		var joins []*Expr
		for j := range p.parseJoins(aliasTokens) {
			joins = append(joins, j)
		}

		if len(joins) > 0 && p.match(TK_ON) {
			kwargs.set("on", p.parseDisjunction())
		} else if len(joins) > 0 && p.match(TK_USING) {
			kwargs.set("using", p.parseUsingIdentifiers())
		} else {
			joins = nil
			p.retreat(index)
		}

		var joinsArg any
		if len(joins) > 0 {
			joinsArg = joins
		}
		thisExpr.Set("joins", joinsArg)
	}

	kwargs.set("pivots", chunkCOptList(p.parsePivots()))

	var tokenComments []string
	for _, token := range []*Token{method, side, kind} {
		if token.ok() {
			tokenComments = append(tokenComments, token.Comments...)
		}
	}
	comments := append(append([]string{}, joinComments...), tokenComments...)

	if p.s.ADD_JOIN_ON_TRUE &&
		!truthy(kwargs.get("on")) &&
		!truthy(kwargs.get("using")) &&
		!truthy(kwargs.get("method")) {
		k := kwargs.get("kind")
		if k == nil || k == "INNER" || k == "OUTER" {
			kwargs.set("on", Boolean(true))
		}
	}

	if directed {
		kwargs.set("directed", directed)
	}

	// A JOIN without any qualifier is a cross join, unlike a comma join
	if join && !hasJoinQualifier(kwargs) {
		kwargs.set("kind", "CROSS")
	}

	return p.expressionC(New(KJoin, kwargs.kv()...), comments)
}

func (p *Parser) parseOpclass() *Expr {
	this := p.parseDisjunction()

	if p.matchTextSetNoAdvance(p.s.OPCLASS_FOLLOW_KEYWORDS) {
		return this
	}

	if !p.matchSetNoAdvance(p.s.OPTYPE_FOLLOW_TOKENS) {
		return p.expression(New(KOpclass, "this", this, "expression", p.parseTableParts(false, false, false, false)))
	}

	return this
}

// parseIndexParams parses index parameters: USING, columns, INCLUDE, PARTITION BY, WITH (...),
// [USING INDEX] TABLESPACE, WHERE and ON. with_storage is false (not nil) when WITH is absent.
func (p *Parser) parseIndexParams() *Expr {
	var using *Expr
	if !p.matchTextSeqNoAdvance("USING", "INDEX", "TABLESPACE") && p.match(TK_USING) {
		using = p.parseVar(true, nil, false)
	}

	var columns any
	if p.matchNoAdvance(TK_L_PAREN) {
		columns = p.parseWrappedCSV(p.parseWithOperator, TK_COMMA, false)
	}

	var include any
	if p.matchTextSeq("INCLUDE") {
		include = p.parseWrappedIdVars(false)
	}
	partitionBy := p.parsePartitionBy()
	var withStorage any = false
	if p.match(TK_WITH) {
		withStorage = p.parseWrappedProperties()
	}
	var tablespace *Expr
	if p.matchTextSeq("USING", "INDEX", "TABLESPACE") || p.matchTextSeq("TABLESPACE") {
		tablespace = p.parseIdVar(true, nil)
	}
	where := p.parseWhere(false)

	var on *Expr
	if p.match(TK_ON) {
		on = p.parseField(false, nil, false)
	}

	return p.expression(New(
		KIndexParameters,
		"using", using,
		"columns", columns,
		"include", include,
		"partition_by", partitionBy,
		"where", where,
		"with_storage", withStorage,
		"tablespace", tablespace,
		"on", on,
	))
}

// parseIndex parses an index definition. Given a name (or anonymous), it parses
// [ON] [TABLE] <table> and the parameters; otherwise [UNIQUE] [PRIMARY] [AMP] INDEX <name> and
// the parameters, returning nil when INDEX is missing.
func (p *Parser) parseIndex(index *Expr, anonymous bool) *Expr {
	var unique, primary, amp any
	var table *Expr

	if index != nil || anonymous {
		unique = nil
		primary = nil
		amp = nil

		p.match(TK_ON)
		p.match(TK_TABLE) // hive
		table = p.parseTableParts(true, false, false, false)
	} else {
		unique = p.match(TK_UNIQUE)
		primary = p.matchTextSeq("PRIMARY")
		amp = p.matchTextSeq("AMP")

		if !p.match(TK_INDEX) {
			return nil
		}

		index = p.parseIdVar(true, nil)
		table = nil
	}

	params := p.parseIndexParams()

	return p.expression(New(
		KIndex,
		"this", index,
		"table", table,
		"unique", unique,
		"primary", primary,
		"amp", amp,
		"params", params,
	))
}

// parseTableHints parses T-SQL WITH (...) table hints or MySQL index hints, returning nil when
// there are none.
func (p *Parser) parseTableHints() []*Expr {
	hints := []*Expr{}
	if p.matchPair(TK_WITH, TK_L_PAREN) {
		// https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
		hints = append(hints, p.expression(New(
			KWithTableHint,
			"expressions", p.parseCSV(func() *Expr {
				if f := p.parseFunction(nil, false, true, false); f != nil {
					return f
				}
				return p.parseVar(true, nil, false)
			}, TK_COMMA),
		)))
		p.matchRParen(nil)
	} else {
		// https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
		for p.matchSet(p.s.TABLE_INDEX_HINT_TOKENS) {
			hint := New(KIndexTableHint, "this", upperText(p.prev))

			p.matchAny(TK_INDEX, TK_KEY)
			if p.match(TK_FOR) {
				var target any
				if p.advanceAny(false).ok() {
					target = upperText(p.prev)
				}
				hint.Set("target", target)
			}

			hint.Set("expressions", p.parseWrappedIdVars(false))
			hints = append(hints, hint)
		}
	}

	if len(hints) == 0 {
		return nil
	}
	return hints
}

func (p *Parser) baseParseTablePart(schema bool) *Expr {
	if !schema {
		if f := p.parseFunction(nil, false, false, false); f != nil {
			return f
		}
	}
	if e := p.parseIdVar(false, nil); e != nil {
		return e
	}
	if e := p.parseStringAsIdentifier(); e != nil {
		return e
	}
	return p.parsePlaceholder()
}

// parseTablePartsFast parses a table name made only of dot-separated identifier tokens, the
// common case. It retreats and returns nil when a postfix token follows the name or a dot is not
// followed by an identifier, leaving those cases to the full parser.
func (p *Parser) parseTablePartsFast() *Expr {
	index := p.index
	var parts []*Expr // nil until the first part
	var allComments []string

	for p.matchSet(p.s.IDENTIFIER_TOKENS) {
		token := p.prev
		comments := p.prevComments

		hasDot := p.match(TK_DOT)
		currTT := p.curr.Type

		if !hasDot {
			if p.s.TABLE_POSTFIX_TOKENS.Has(currTT) {
				p.retreat(index)
				return nil
			}
		} else if !p.s.IDENTIFIER_TOKENS.Has(currTT) {
			p.retreat(index)
			return nil
		}

		if parts == nil {
			parts = []*Expr{}
		}

		if len(comments) > 0 {
			if allComments == nil {
				allComments = []string{}
			}
			allComments = append(allComments, comments...)
			p.prevComments = []string{}
		}

		parts = append(parts, p.expressionTok(
			New(KIdentifier, "this", token.Text, "quoted", token.Type == TK_IDENTIFIER),
			token,
		))

		if !hasDot {
			break
		}
	}

	if parts == nil {
		return nil
	}

	n := len(parts)

	var table *Expr
	if n == 1 {
		table = New(KTable, "this", parts[0])
	} else if n == 2 {
		table = New(KTable, "this", parts[1], "db", parts[0])
	} else if n >= 3 {
		this := parts[2]
		for i := 3; i < n; i++ {
			this = New(KDot, "this", this, "expression", parts[i])
		}

		table = New(KTable, "this", this, "db", parts[1], "catalog", parts[0])
	}

	if table == nil {
		p.retreat(index)
	} else if len(allComments) > 0 {
		table.AddComments(allComments, false)
	}
	return table
}

// baseParseTableParts parses a dotted table name into a Table with catalog, db and this; parts
// beyond the third nest in Dot expressions. A trailing `*` makes a wildcard name when wildcard is
// set, and isDbReference shifts the parts so the last one is the db. Comments on the parts,
// CHANGES, AT / BEFORE and pivots attach to the Table.
func (p *Parser) baseParseTableParts(schema bool, isDbReference bool, wildcard bool, fast bool) *Expr {
	if fast {
		return p.parseTablePartsFast()
	}

	// catalog, db and table hold an *Expr, a string ("") or nil
	var catalog, db any
	var table any = chunkCAnyExpr(p.parseTablePart(schema))

	for p.match(TK_DOT) {
		if truthy(catalog) {
			// This allows nesting the table in arbitrarily many dot expressions if needed
			table = p.expression(New(KDot, "this", table, "expression", p.parseTablePart(schema)))
		} else {
			catalog = db
			db = table
			// "" used for tsql FROM a..b case
			if part := p.parseTablePart(schema); part != nil {
				table = part
			} else {
				table = ""
			}
		}
	}

	tableExpr, _ := table.(*Expr)
	if wildcard &&
		p.isConnected() &&
		(tableExpr.IsA(KIdentifier) || !truthy(table)) &&
		p.match(TK_STAR) {
		if tableExpr.IsA(KIdentifier) {
			tableExpr.SetArgRaw("this", tableExpr.ThisS()+"*")
		} else {
			table = New(KIdentifier, "this", "*")
		}
	}

	if isDbReference {
		catalog = db
		db = table
		table = nil
	}

	if !truthy(table) && !isDbReference {
		p.raiseError("Expected table name but got "+p.curr.String(), nil)
	}
	if !truthy(db) && isDbReference {
		p.raiseError("Expected database name but got "+p.curr.String(), nil)
	}

	result := p.expression(New(KTable, "this", table, "db", db, "catalog", catalog))

	// Bubble up comments from identifier parts to the Table
	var comments []string
	for _, part := range result.Parts() {
		if partComments := part.PopComments(); len(partComments) > 0 {
			comments = append(comments, partComments...)
		}
	}
	if len(comments) > 0 {
		result.AddComments(comments, false)
	}

	if changes := p.parseChanges(); changes != nil {
		result.Set("changes", changes)
	}

	if atBefore := p.parseHistoricalData(); atBefore != nil {
		result.Set("when", atBefore)
	}

	if pivots := p.parsePivots(); len(pivots) > 0 {
		result.Set("pivots", pivots)
	}

	return result
}

// baseParseTable parses a table reference: a plain table name (fast path), STREAM, LATERAL,
// UNNEST, VALUES, a subquery, a bracket, ROWS FROM (...) or a table name with its partition,
// version, sample, alias, index hints, pivots and joins. With schema set, a column list follows
// the name instead.
func (p *Parser) baseParseTable(schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr {
	if !schema && !isDbReference && !consumePipe && !joins {
		index := p.index
		table := p.parseTableParts(false, false, false, true)

		if table != nil {
			currTT := p.curr.Type
			nextTT := p.next.Type

			fastTerminators := p.s.TABLE_TERMINATORS

			// only return the table if we're sure there are no other operators
			// MATCH_CONDITION is a special case because it accepts any alias before it like LIMIT
			if fastTerminators.Has(currTT) && nextTT != TK_MATCH_CONDITION {
				return table
			}

			postfixTokens := p.s.TABLE_POSTFIX_TOKENS

			if !postfixTokens.Has(currTT) && !postfixTokens.Has(nextTT) {
				if alias := p.parseTableAlias(p.chunkCAliasTokensOr(aliasTokens)); alias != nil {
					table.Set("alias", alias)
				}

				if fastTerminators.Has(p.curr.Type) {
					return table
				}
			}

			p.retreat(index)
		}
	}

	if stream := p.parseStream(); stream != nil {
		return stream
	}

	if lateral := p.parseLateral(); lateral != nil {
		return lateral
	}

	if unnest := p.parseUnnest(true); unnest != nil {
		return unnest
	}

	if values := p.parseDerivedTableValues(false); values != nil {
		values.Set("pivots", chunkCOptList(p.parsePivots()))
		return values
	}

	if subquery := p.parseSelect(false, true, true, true, consumePipe, nil); subquery != nil {
		if !subquery.ArgB("pivots") {
			subquery.Set("pivots", chunkCOptList(p.parsePivots()))
		}
		if joins {
			for join := range p.parseJoins(nil) {
				subquery.Append("joins", join)
			}
		}
		return subquery
	}

	var bracket *Expr
	if parseBracket {
		bracket = p.parseBracket(nil)
	}
	if bracket != nil {
		bracket = p.expression(New(KTable, "this", bracket))
	} else {
		bracket = nil
	}

	var rowsFromTables []*Expr
	if p.matchTextSeq("ROWS", "FROM") {
		rowsFromTables = p.parseWrappedCSV(func() *Expr {
			return p.parseTable(false, false, nil, false, false, false, false)
		}, TK_COMMA, false)
	}
	var rowsFrom *Expr
	if len(rowsFromTables) > 0 {
		rowsFrom = p.expression(New(KTable, "rows_from", rowsFromTables))
	}

	only := p.match(TK_ONLY)

	this := bracket
	if this == nil {
		this = rowsFrom
	}
	if this == nil {
		this = p.parseBracket(p.parseTableParts(schema, isDbReference, false, false))
	}

	if only {
		this.Set("only", only)
	}

	// Postgres supports a wildcard (table) suffix operator, which is a no-op in this context
	p.match(TK_STAR)

	parsePartition = parsePartition || p.s.SUPPORTS_PARTITION_SELECTION
	if parsePartition && p.matchNoAdvance(TK_PARTITION) {
		this.Set("partition", p.parsePartition())
	}

	if schema {
		return p.parseSchema(this)
	}

	if p.d.S.ALIAS_POST_VERSION {
		this.Set("version", p.parseVersion())
	}

	if p.d.S.ALIAS_POST_TABLESAMPLE {
		this.Set("sample", p.parseTableSample(false))
		if this.ArgB("sample") && p.d.S.ALIAS_POST_VERSION && !this.ArgB("version") {
			this.Set("version", p.parseVersion())
		}
	}

	alias := p.parseTableAlias(p.chunkCAliasTokensOr(aliasTokens))
	if alias != nil {
		this.Set("alias", alias)

		// DuckDB requires the time-travel clause to come after the alias, e.g.
		// SELECT * FROM t AS a AT (VERSION => 1)
		if this.IsA(KTable) && !this.ArgB("when") {
			this.Set("when", p.parseHistoricalData())
		}
	}

	if p.match(TK_INDEXED_BY) {
		this.Set("indexed", p.parseTableParts(false, false, false, false))
	} else if p.matchTextSeq("NOT", "INDEXED") {
		this.Set("indexed", false)
	}

	if this.IsA(KTable) && p.matchTextSeq("AT") {
		return p.expression(New(
			KAtIndex,
			"this", chunkCTableToColumn(this, false),
			"expression", p.parseIdVar(true, nil),
		))
	}

	this.Set("hints", chunkCOptList(p.parseTableHints()))

	if !this.ArgB("pivots") {
		this.Set("pivots", chunkCOptList(p.parsePivots()))
	}

	if !p.d.S.ALIAS_POST_TABLESAMPLE {
		this.Set("sample", p.parseTableSample(false))
	}

	if !p.d.S.ALIAS_POST_VERSION {
		this.Set("version", p.parseVersion())
	}

	if joins {
		for join := range p.parseJoins(aliasTokens) {
			this.Append("joins", join)
		}
	}

	if p.matchPair(TK_WITH, TK_ORDINALITY) {
		this.Set("ordinality", true)
		this.Set("alias", p.parseTableAlias(nil))
	}

	return this
}

// VersionPhrase is a time travel clause prefix, such as FOR SYSTEM_TIME, and what it pins (a
// timestamp or a version).
type VersionPhrase struct {
	Words []string
	Kind  string
}

// parseVersion parses a time travel clause (a VERSION_PHRASES prefix such as FOR SYSTEM_TIME)
// with FROM / BETWEEN, CONTAINED IN, ALL or AS OF.
func (p *Parser) parseVersion() *Expr {
	var this string
	for _, phrase := range p.s.VERSION_PHRASES {
		if p.matchTextSeq(phrase.Words...) {
			this = phrase.Kind
			break
		}
	}
	if this == "" {
		return nil
	}

	var kind string
	var expression *Expr
	if p.matchAny(TK_FROM, TK_BETWEEN) {
		kind = upperText(p.prev)
		start := p.parseBitwise()
		p.matchTexts("TO", "AND")
		end := p.parseBitwise()
		expression = p.expression(New(KTuple, "expressions", []*Expr{start, end}))
	} else if p.matchTextSeq("CONTAINED", "IN") {
		kind = "CONTAINED IN"
		expression = p.expression(New(KTuple, "expressions", p.parseWrappedCSV(p.parseBitwise, TK_COMMA, false)))
	} else if p.match(TK_ALL) {
		kind = "ALL"
		expression = nil
	} else {
		p.matchTextSeq("AS", "OF")
		kind = "AS OF"
		expression = p.parseBitwise()
	}

	return p.expression(New(KVersion, "this", this, "expression", expression, "kind", kind))
}

// parseHistoricalData parses Snowflake's AT | BEFORE (<kind> => <expr>), backtracking when no
// expression follows.
func (p *Parser) parseHistoricalData() *Expr {
	// https://docs.snowflake.com/en/sql-reference/constructs/at-before
	index := p.index
	var historicalData *Expr
	if p.matchTextSet(p.s.HISTORICAL_DATA_PREFIX) {
		this := upperText(p.prev)
		var kind any = false
		if p.match(TK_L_PAREN) && p.matchTextSet(p.s.HISTORICAL_DATA_KIND) {
			kind = upperText(p.prev)
		}
		var expression *Expr
		if p.match(TK_FARROW) {
			expression = p.parseBitwise()
		}

		if expression != nil {
			p.matchRParen(nil)
			historicalData = p.expression(New(KHistoricalData, "this", this, "kind", kind, "expression", expression))
		} else {
			p.retreat(index)
		}
	}

	return historicalData
}

func (p *Parser) parseChanges() *Expr {
	if !p.matchTextSeq("CHANGES", "(", "INFORMATION", "=>") {
		return nil
	}

	information := p.parseVar(true, nil, false)
	p.matchRParen(nil)

	atBefore := p.parseHistoricalData()
	end := p.parseHistoricalData()
	return p.expression(New(
		KChanges,
		"information", information,
		"at_before", atBefore,
		"end", end,
	))
}

// baseParseUnnest parses UNNEST(...) [WITH ORDINALITY] [alias] [WITH OFFSET [AS] <alias>]. With
// ordinality, an extra trailing column alias becomes the offset.
func (p *Parser) baseParseUnnest(withAlias bool) *Expr {
	if !p.matchPairNoAdvance(TK_UNNEST, TK_L_PAREN) {
		return nil
	}

	p.advance(1)

	expressions := p.parseWrappedCSV(p.parseEquality, TK_COMMA, false)
	var offset any = p.matchPair(TK_WITH, TK_ORDINALITY) // bool or *Expr

	var alias *Expr
	if withAlias {
		alias = p.parseTableAlias(nil)
	}

	if alias != nil {
		if p.d.S.UNNEST_COLUMN_ONLY {
			if alias.ArgB("columns") {
				p.raiseError("Unexpected extra column alias in unnest.", nil)
			}

			alias.Set("columns", []*Expr{alias.This()})
			alias.Set("this", nil)
		}

		columns := alias.ArgL("columns")
		if truthy(offset) && len(expressions) < len(columns) {
			// the last column alias names the offset: take it off the alias
			offset = columns[len(columns)-1]
			alias.SetArgRaw("columns", append([]*Expr{}, columns[:len(columns)-1]...))
		}
	}

	if !truthy(offset) && p.matchPair(TK_WITH, TK_OFFSET) {
		p.match(TK_ALIAS)
		o := p.parseIdVar(false, &p.s.UNNEST_OFFSET_ALIAS_TOKENS)
		if o == nil {
			o = ToIdentifier("offset", nil)
		}
		offset = o
	}

	return p.expression(New(KUnnest, "expressions", expressions, "alias", alias, "offset", offset))
}

// parseDerivedTableValues parses a VALUES list, parenthesized or bare (also ClickHouse's FORMAT
// Values, and VALUE when allowValueSynonym is set), with an optional alias inside or after the
// parentheses.
func (p *Parser) parseDerivedTableValues(allowValueSynonym bool) *Expr {
	isDerived := p.matchPair(TK_L_PAREN, TK_VALUES)
	// ClickHouse's `FORMAT Values` is equivalent to `VALUES`; MySQL accepts VALUE as a synonym
	// for VALUES in INSERT statements
	if !isDerived && !(p.matchTextSeq("VALUES") || p.matchTextSeq("FORMAT", "VALUES") ||
		(allowValueSynonym && p.matchTextSeq("VALUE"))) {
		return nil
	}

	expressions := p.parseCSV(func() *Expr { return p.parseValue(true) }, TK_COMMA)
	alias := p.parseTableAlias(nil)

	if isDerived {
		p.matchRParen(nil)
	}

	if alias == nil {
		alias = p.parseTableAlias(nil)
	}
	return p.expression(New(KValues, "expressions", expressions, "alias", alias))
}

// baseParseTableSample parses TABLESAMPLE (or USING SAMPLE, when asModifier) with its method,
// size or percent, BUCKET ... OUT OF ... ON ..., and seed. seed is false (not nil) when the
// method is called without a seed argument.
func (p *Parser) baseParseTableSample(asModifier bool) *Expr {
	if !p.match(TK_TABLE_SAMPLE) && !(asModifier && p.matchTextSeq("USING", "SAMPLE")) {
		return nil
	}

	var bucketNumerator, bucketDenominator, bucketField, percent, size *Expr
	var seed any // nil, false or *Expr

	method := p.parseVar(false, &chunkCRowTokens, true)
	matchedLParen := p.match(TK_L_PAREN)

	var num *Expr
	var expressions any
	if p.s.TABLESAMPLE_CSV {
		num = nil
		expressions = p.parseCSV(p.parsePrimary, TK_COMMA)
	} else {
		expressions = nil
		if p.matchNoAdvance(TK_NUMBER) {
			num = p.parseFactor()
		} else {
			num = p.parsePrimary()
			if num == nil {
				num = p.parsePlaceholder()
			}
		}
	}

	if p.matchTextSeq("BUCKET") {
		bucketNumerator = p.parseNumber()
		p.matchTextSeq("OUT", "OF")
		bucketDenominator = p.parseNumber()
		p.match(TK_ON)
		bucketField = p.parseField(false, nil, false)
	} else if p.matchAny(TK_PERCENT, TK_MOD) {
		percent = num
	} else if p.match(TK_ROWS) || !p.d.S.TABLESAMPLE_SIZE_IS_PERCENT {
		size = num
	} else {
		percent = num
	}

	if matchedLParen {
		p.matchRParen(nil)
	}

	if p.match(TK_L_PAREN) {
		method = p.parseVar(false, nil, true)
		if p.match(TK_COMMA) {
			seed = chunkCAnyExpr(p.parseNumber())
		} else {
			seed = false
		}
		p.matchRParen(nil)
	} else if p.matchTexts("SEED", "REPEATABLE") {
		seed = chunkCAnyExpr(p.parseWrapped(p.parseNumber, false))
	}

	if method == nil && p.s.DEFAULT_SAMPLING_METHOD != "" {
		method = VarExpr(p.s.DEFAULT_SAMPLING_METHOD)
	}

	return p.expression(New(
		KTableSample,
		"expressions", expressions,
		"method", method,
		"bucket_numerator", bucketNumerator,
		"bucket_denominator", bucketDenominator,
		"bucket_field", bucketField,
		"percent", percent,
		"size", size,
		"seed", seed,
	))
}

func (p *Parser) parsePivots() []*Expr {
	if !p.matchAnyNoAdvance(TK_PIVOT, TK_UNPIVOT) {
		return nil
	}
	var pivots []*Expr
	for {
		pivot := p.parsePivot()
		if pivot == nil {
			break
		}
		pivots = append(pivots, pivot)
	}
	if len(pivots) == 0 {
		return nil
	}
	return pivots
}

func (p *Parser) parseJoins(aliasTokens *TokenSet) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		for {
			join := p.parseJoin(false, false, aliasTokens)
			if join == nil {
				return
			}
			if !yield(join) {
				return
			}
		}
	}
}

// parseUnpivotColumns parses INTO NAME <column> VALUE <columns> of an UNPIVOT statement; missing
// parts are false (not nil).
func (p *Parser) parseUnpivotColumns() *Expr {
	if !p.match(TK_INTO) {
		return nil
	}

	var this any = false
	if p.matchTextSeq("NAME") {
		this = chunkCAnyExpr(p.parseColumn())
	}
	var expressions any = false
	if p.matchTextSeq("VALUE") {
		expressions = p.parseCSV(p.parseColumn, TK_COMMA)
	}
	return p.expression(New(KUnpivotColumns, "this", this, "expressions", expressions))
}

// parseSimplifiedPivot parses DuckDB's simplified PIVOT / UNPIVOT statement:
// https://duckdb.org/docs/sql/statements/pivot
// isUnpivot is a bool, or nil for the PIVOT statement parser.
func (p *Parser) parseSimplifiedPivot(isUnpivot any) *Expr {
	parseOn := func() *Expr {
		this := p.parseBitwise()

		if p.match(TK_IN) {
			// PIVOT ... ON col IN (row_val1, row_val2)
			return p.parseIn(this, false)
		}
		if p.matchNoAdvance(TK_ALIAS) {
			// UNPIVOT ... ON (col1, col2, col3) AS row_val
			return p.parseAlias(this, false)
		}

		return this
	}

	this := p.parseTable(false, false, nil, false, false, false, false)
	var expressions any = false
	if p.match(TK_ON) {
		expressions = p.parseCSV(parseOn, TK_COMMA)
	}
	into := p.parseUnpivotColumns()
	var using any = false
	if p.match(TK_USING) {
		using = p.parseCSV(func() *Expr { return p.parseAlias(p.parseColumn(), false) }, TK_COMMA)
	}
	group := p.parseGroup(false)

	return p.expression(New(
		KPivot,
		"this", this,
		"expressions", expressions,
		"using", using,
		"group", group,
		"unpivot", isUnpivot,
		"into", into,
	))
}

// parsePivotIn parses the <column> IN (...) part of a PIVOT, whose values may be aliased or
// be ANY [ORDER BY ...], or <column> IN <name>.
func (p *Parser) parsePivotIn() *Expr {
	parseAliasedExpression := func() *Expr {
		this := p.parseSelectOrExpression(false)

		p.match(TK_ALIAS)
		alias := p.parseBitwise()
		if alias != nil {
			if alias.IsA(KColumn) && alias.DbName() == "" {
				alias = alias.This()
			}
			return p.expression(New(KPivotAlias, "this", this, "alias", alias))
		}

		return this
	}

	value := p.parseColumn()

	if !p.match(TK_IN) {
		p.raiseError("Expecting IN", nil)
	}

	if p.match(TK_L_PAREN) {
		var exprs []*Expr
		if p.match(TK_ANY) {
			exprs = []*Expr{New(KPivotAny, "this", p.parseOrder(nil, false))}
		} else {
			exprs = p.parseCSV(parseAliasedExpression, TK_COMMA)
		}
		p.matchRParen(nil)
		return p.expression(New(KIn, "this", value, "expressions", exprs))
	}

	return p.expression(New(KIn, "this", value, "field", p.parseIdVar(true, nil)))
}

// baseParsePivotAggregation parses one aliased aggregation of a PIVOT, returning nil after a
// trailing comma.
func (p *Parser) baseParsePivotAggregation() *Expr {
	fn := p.parseFunction(nil, false, true, false)
	if fn == nil {
		if p.prev.Type == TK_COMMA {
			return nil
		}
		p.raiseError("Expecting an aggregation function in PIVOT", nil)
	}

	return p.parseAlias(fn, false)
}

// parsePivot parses one PIVOT or UNPIVOT clause. For PIVOT it also computes the output column
// names from the IN values and the aggregation aliases.
func (p *Parser) parsePivot() *Expr {
	index := p.index
	var includeNulls any // nil, true or false
	var unpivot bool

	if p.match(TK_PIVOT) {
		unpivot = false
	} else if p.match(TK_UNPIVOT) {
		unpivot = true

		// https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-unpivot.html#syntax
		if p.matchTextSeq("INCLUDE", "NULLS") {
			includeNulls = true
		} else if p.matchTextSeq("EXCLUDE", "NULLS") {
			includeNulls = false
		}
	} else {
		return nil
	}

	var expressions []*Expr

	if !p.match(TK_L_PAREN) {
		p.retreat(index)
		return nil
	}

	if unpivot {
		expressions = p.parseCSV(p.parseColumn, TK_COMMA)
	} else {
		expressions = p.parseCSV(p.parsePivotAggregation, TK_COMMA)
	}

	if len(expressions) == 0 {
		p.raiseError("Failed to parse PIVOT's aggregation list", nil)
	}

	if !p.match(TK_FOR) {
		p.raiseError("Expecting FOR", nil)
	}

	fields := []*Expr{}
	for {
		field := p.tryParseExpr(p.parsePivotIn, false)
		if field == nil {
			break
		}
		fields = append(fields, field)
	}

	var defaultOnNull any = false
	if p.matchTextSeq("DEFAULT", "ON", "NULL") {
		defaultOnNull = chunkCAnyExpr(p.parseWrapped(p.parseBitwise, false))
	}

	group := p.parseGroup(false)

	p.matchRParen(nil)

	pivot := p.expression(New(
		KPivot,
		"expressions", expressions,
		"fields", fields,
		"unpivot", unpivot,
		"include_nulls", includeNulls,
		"default_on_null", defaultOnNull,
		"group", group,
	))

	if unpivot {
		pexprs := pivot.Expressions()
		targets := make([]*Expr, len(pexprs))
		for i, e := range pexprs {
			targets[i] = chunkCUnpivotTarget(e)
		}
		pivot.Set("expressions", targets)
		for _, pivotField := range pivot.ArgL("fields") {
			if pivotField.IsA(KIn) {
				pivotField.Set("this", chunkCUnpivotTarget(pivotField.This()))
			}
		}
	}

	if !p.matchAnyNoAdvance(TK_PIVOT, TK_UNPIVOT) {
		pivot.Set("alias", p.parseTableAlias(nil))
	}

	if !unpivot {
		names := p.pivotColumnNames(expressions)

		columns := []*Expr{}
		var allFields [][]string
		for _, pivotField := range pivot.ArgL("fields") {
			pivotFieldExpressions := pivotField.Expressions()

			// The `PivotAny` expression corresponds to `ANY ORDER BY <column>`; we can't infer in this case.
			if seqGet(pivotFieldExpressions, 0).IsA(KPivotAny) {
				continue
			}

			fieldNames := []string{}
			for _, fld := range pivotFieldExpressions {
				// An explicit `<field> AS <alias>` names the output column directly, so it wins
				// over the dialect's string-identifying convention
				if p.s.IDENTIFY_PIVOT_STRINGS && !fld.IsA(KPivotAlias) {
					fieldNames = append(fieldNames, exprSQL(fld))
				} else {
					fieldNames = append(fieldNames, fld.AliasOrName())
				}
			}
			allFields = append(allFields, fieldNames)
		}

		if len(allFields) > 0 {
			if len(names) > 0 {
				allFields = append(allFields, names)
			}

			// Generate all possible combinations of the pivot columns
			// e.g PIVOT(sum(...) as total FOR year IN (2000, 2010) FOR country IN ('NL', 'US'))
			// generates the product between [[2000, 2010], ['NL', 'US'], ['total']]
			for _, fldParts := range chunkCProduct(allFields) {
				if len(names) > 0 && p.s.PREFIXED_PIVOT_COLUMNS {
					last := fldParts[len(fldParts)-1]
					fldParts = append([]string{last}, fldParts[:len(fldParts)-1]...)
				}

				columns = append(columns, ToIdentifier(strings.Join(fldParts, "_"), nil))
			}
		}

		pivot.Set("columns", columns)
		pivot.Set("identify_pivot_strings", p.s.IDENTIFY_PIVOT_STRINGS)
		pivot.Set("prefixed_pivot_columns", p.s.PREFIXED_PIVOT_COLUMNS)
		pivot.Set("pivot_column_naming", p.s.PIVOT_COLUMN_NAMING)
	}

	return pivot
}

func (p *Parser) basePivotColumnNames(aggregations []*Expr) []string {
	out := []string{}
	for _, agg := range aggregations {
		if agg.Alias() != "" {
			out = append(out, agg.Alias())
		}
	}
	return out
}
