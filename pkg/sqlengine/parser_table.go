package sqlengine

import (
	"iter"
	"strings"
)

// Port of sqlglot/parser.py (chunk C, part 1): MATCH_RECOGNIZE, LATERAL, joins, indexes,
// table hints, table parts, _parse_table, versions / historical data, UNNEST, VALUES,
// TABLESAMPLE and PIVOT / UNPIVOT.

// chunkCRowTokens mirrors the ad-hoc `(TokenType.ROW,)` tuple used by _parse_table_sample.
var chunkCRowTokens = newTokenSet(TK_ROW)

// chunkCOptList converts a "list or None" result into an argument value: a nil slice means
// Python None (untyped nil, so that Set removes the key / New keeps None), otherwise the list.
func chunkCOptList(l []*Expr) any {
	if l == nil {
		return nil
	}
	return l
}

// chunkCAnyExpr converts a possibly-nil expression into an untyped value (nil means None).
func chunkCAnyExpr(e *Expr) any {
	if e == nil {
		return nil
	}
	return e
}

// chunkCKwargs mirrors an insertion-ordered Python kwargs dict (also used for defaultdict(list)).
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

func (k *chunkCKwargs) get(key string) any {
	for i, kk := range k.keys {
		if kk == key {
			return k.vals[i]
		}
	}
	return nil
}

// list mirrors defaultdict(list)[key]: returns the list, creating an empty one if absent.
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

// chunkCAliasTokensOr mirrors `alias_tokens or self.TABLE_ALIAS_TOKENS`.
func (p *Parser) chunkCAliasTokensOr(aliasTokens *TokenSet) *TokenSet {
	if aliasTokens != nil && *aliasTokens != (TokenSet{}) {
		return aliasTokens
	}
	return &p.s.TABLE_ALIAS_TOKENS
}

// chunkCToIdentifier mirrors exp.to_identifier(name, copy=copy) for expression (or None) inputs.
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
	panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + name.classRepr()})
}

// chunkCColumn mirrors exp.column(col, table, db, catalog, fields=fields, copy=copy) for expression inputs.
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

// chunkCAlias mirrors exp.alias_(expression, alias, copy=copy) for an expression and an
// Identifier (or None) alias.
func chunkCAlias(e *Expr, alias *Expr, copy bool) *Expr {
	if copy {
		e = e.Copy()
	}
	// to_identifier's own copy flag defaults to True
	alias = chunkCToIdentifier(alias, true)
	if e.Kind().hasArgType("alias") && !e.Is(KWindow) {
		e.Set("alias", alias)
		return e
	}
	return New(KAlias, "this", e, "alias", alias)
}

// chunkCTableToColumn mirrors Table.to_column(copy).
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
		for i := 0; i < n; i++ {
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

// chunkCUnpivotTarget mirrors parser._unpivot_target.
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

// chunkCProduct mirrors itertools.product(*lists) for lists of strings.
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

// _parse_match_recognize_measure (parser.py L4315).
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

// chunkCAdvanceAnyText mirrors `self._advance_any().text`, which raises AttributeError when no
// token can be consumed.
func chunkCAdvanceAnyText(p *Parser) string {
	tok := p.advanceAny(false)
	if tok == nil {
		panic(&ValueError{Msg: "'NoneType' object has no attribute 'text'"})
	}
	return tok.Text
}

// _parse_match_recognize (parser.py L4323).
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

// _parse_lateral (parser.py L4412).
func (p *Parser) baseParseLateral() *Expr {
	var crossApply any // bool | None
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

	var ordinality any // bool | None
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

// _parse_stream (parser.py L4469).
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

// _parse_join_parts (parser.py L4477).
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

// _parse_using_identifiers (parser.py L4486).
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

// _parse_join (parser.py L4495).
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
	directed := p.matchTextSeq("DIRECTED")
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
		kwargs.set("match_condition", p.parseWrapped(p.parseComparison, false))
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

	return p.expressionC(New(KJoin, kwargs.kv()...), comments)
}

// _parse_opclass (parser.py L4591).
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

// _parse_index_params (parser.py L4602).
func (p *Parser) parseIndexParams() *Expr {
	var using *Expr
	if p.match(TK_USING) {
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
	if p.matchTextSeq("USING", "INDEX", "TABLESPACE") {
		tablespace = p.parseVar(true, nil, false)
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

// _parse_index (parser.py L4635).
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

// _parse_table_hints (parser.py L4665).
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

// _parse_table_part (parser.py L4693).
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

// _parse_table_parts_fast (parser.py L4701).
func (p *Parser) parseTablePartsFast() *Expr {
	index := p.index
	var parts []*Expr // None until the first part
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

// _parse_table_parts (parser.py L4764).
func (p *Parser) baseParseTableParts(schema bool, isDbReference bool, wildcard bool, fast bool) *Expr {
	if fast {
		return p.parseTablePartsFast()
	}

	// catalog, db and table hold an *Expr, a string ("") or nil (None)
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

// _parse_table (parser.py L4835).
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

	if values := p.parseDerivedTableValues(); values != nil {
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

// _parse_version (parser.py L4975).
func (p *Parser) parseVersion() *Expr {
	var this string
	if p.match(TK_TIMESTAMP_SNAPSHOT) {
		this = "TIMESTAMP"
	} else if p.match(TK_VERSION_SNAPSHOT) {
		this = "VERSION"
	} else {
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
		expression = p.parseType(true, false)
	}

	return p.expression(New(KVersion, "this", this, "expression", expression, "kind", kind))
}

// _parse_historical_data (parser.py L5004).
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

// _parse_changes (parser.py L5027).
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

// _parse_unnest (parser.py L5042).
func (p *Parser) baseParseUnnest(withAlias bool) *Expr {
	if !p.matchPairNoAdvance(TK_UNNEST, TK_L_PAREN) {
		return nil
	}

	p.advance(1)

	expressions := p.parseWrappedCSV(p.parseEquality, TK_COMMA, false)
	var offset any = p.matchPair(TK_WITH, TK_ORDINALITY) // bool | Expr

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
			// columns.pop(): mutates the alias' column list in place
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

// _parse_derived_table_values (parser.py L5073).
func (p *Parser) parseDerivedTableValues() *Expr {
	isDerived := p.matchPair(TK_L_PAREN, TK_VALUES)
	// ClickHouse's `FORMAT Values` is equivalent to `VALUES`
	if !isDerived && !(p.matchTextSeq("VALUES") || p.matchTextSeq("FORMAT", "VALUES")) {
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

// _parse_table_sample (parser.py L5091).
func (p *Parser) baseParseTableSample(asModifier bool) *Expr {
	if !p.match(TK_TABLE_SAMPLE) && !(asModifier && p.matchTextSeq("USING", "SAMPLE")) {
		return nil
	}

	var bucketNumerator, bucketDenominator, bucketField, percent, size *Expr
	var seed any // None | False | Expr

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

// _parse_pivots (parser.py L5157).
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

// _parse_joins (parser.py L5162).
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

// _parse_unpivot_columns (parser.py L5167).
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

// _parse_simplified_pivot (parser.py L5179)
// https://duckdb.org/docs/sql/statements/pivot
// isUnpivot is `bool | None` (None for the PIVOT statement parser).
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

// _parse_pivot_in (parser.py L5211).
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

// _parse_pivot_aggregation (parser.py L5239).
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

// _parse_pivot (parser.py L5248).
func (p *Parser) parsePivot() *Expr {
	index := p.index
	var includeNulls any // bool | None
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
				if p.s.IDENTIFY_PIVOT_STRINGS {
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
					// Move the "name" to the front of the list
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

// _pivot_column_names (parser.py L5359).
func (p *Parser) basePivotColumnNames(aggregations []*Expr) []string {
	out := []string{}
	for _, agg := range aggregations {
		if agg.Alias() != "" {
			out = append(out, agg.Alias())
		}
	}
	return out
}
