package sqlengine

// chunkBSelectStar returns SELECT *, whose Star has the args and positions of a parsed "*".
func chunkBSelectStar() *Expr {
	star := New(KStar, "ilike", nil, "except_", nil, "replace", nil, "rename", nil)
	star.setPositions(1, 1, 0, 0)
	sel := New(KSelect)
	sel.Set("expressions", []*Expr{star})
	return sel
}

// chunkBSelectFrom sets the FROM of sel to expression, wrapped in a From unless it is one; the
// expression is not copied.
func chunkBSelectFrom(sel *Expr, expression *Expr) *Expr {
	if expression == nil {
		panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	if !expression.IsA(KFrom) {
		expression = New(KFrom, "this", expression)
	}
	sel.Set("from_", expression)
	return sel
}

func chunkBToIdentifier(name any, copy bool) *Expr {
	switch v := name.(type) {
	case nil:
		return nil
	case string:
		return ToIdentifier(v, nil)
	case *Expr:
		if v == nil {
			return nil
		}
		if v.IsA(KIdentifier) {
			if copy {
				return v.Copy()
			}
			return v
		}
		panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + v.Kind().Name()})
	}
	panic(&ValueError{Msg: "Name needs to be a string or an Identifier"})
}

// chunkBAlias aliases expression like AliasExpr, or sets a TableAlias like AliasTableExpr when
// table is true or a non-empty []*Expr of column names. alias is nil, a string or an Identifier.
func chunkBAlias(expression *Expr, alias any, table any, copy bool) *Expr {
	e := expression
	if copy {
		e = e.Copy()
	}
	id := chunkBToIdentifier(alias, true)

	tableTruthy := false
	var columns []*Expr
	switch t := table.(type) {
	case bool:
		tableTruthy = t
	case []*Expr:
		tableTruthy = len(t) > 0
		columns = t
	}

	if tableTruthy {
		tableAlias := New(KTableAlias, "this", id)
		e.Set("alias", tableAlias)
		for _, column := range columns {
			tableAlias.Append("columns", chunkBToIdentifier(column, true))
		}
		return e
	}

	// We don't set the "alias" arg for Window expressions (see aliasImpl)
	if e.Kind().hasArgType("alias") && e.Kind() != KWindow {
		e.Set("alias", id)
		return e
	}
	return New(KAlias, "this", e, "alias", id)
}

func chunkBColumn(col, table, db, catalog *Expr, fields []*Expr, copy bool) *Expr {
	if !col.IsA(KStar) {
		col = chunkBToIdentifier(col, copy)
	}

	this := New(
		KColumn,
		"this", col,
		"table", chunkBToIdentifier(table, copy),
		"db", chunkBToIdentifier(db, copy),
		"catalog", chunkBToIdentifier(catalog, copy),
	)

	if len(fields) > 0 {
		exprs := []*Expr{this}
		for _, field := range fields {
			exprs = append(exprs, chunkBToIdentifier(field, copy))
		}
		return DotBuild(exprs)
	}
	return this
}

// chunkBTableToColumn converts a table reference into a column reference with the same dotted
// name, keeping its alias. A table that wraps a function or array yields that expression.
func chunkBTableToColumn(table *Expr, copy bool) *Expr {
	parts := table.Parts()
	lastPart := parts[len(parts)-1]

	var col *Expr
	if lastPart.IsA(KIdentifier) {
		// the first four parts, reversed, are column, table, db and catalog; the rest become fields
		head := parts
		if len(head) > 4 {
			head = head[:4]
		}
		rev := make([]*Expr, 4)
		for i := range head {
			rev[i] = head[len(head)-1-i]
		}
		var fields []*Expr
		if len(parts) > 4 {
			fields = parts[4:]
		}
		col = chunkBColumn(rev[0], rev[1], rev[2], rev[3], fields, copy)
	} else {
		col = lastPart
	}

	if alias := table.ArgE("alias"); alias != nil {
		col = chunkBAlias(col, alias.Arg("this"), false, copy)
	}

	return col
}

// chunkBNormalizeIdentifiers normalizes the identifiers in expression for dialect d, skipping
// subtrees marked case_sensitive.
func chunkBNormalizeIdentifiers(expression *Expr, d *Dialect) *Expr {
	prune := func(n *Expr) bool { return truthy(n.MetaGet("case_sensitive")) }
	for node := range expression.Walk(true, prune) {
		if !truthy(node.MetaGet("case_sensitive")) {
			if node.IsA(KIdentifier) {
				d.NormalizeIdentifier(node)
			}
		}
	}
	return expression
}

// chunkBParseOneInto parses sql as kind with dialect d. A ParseError panics as a parsePanic, which
// tryParseExpr recovers from; other errors panic as they are.
func chunkBParseOneInto(d *Dialect, kind Kind, sql string) *Expr {
	e, err := d.ParseOneInto(kind, sql, nil)
	if err != nil {
		if pe, ok := err.(*ParseError); ok {
			panic(parsePanic{pe})
		}
		panic(err)
	}
	return e
}

func (p *Parser) baseParsePartition() *Expr {
	if !p.matchTextSet(p.s.PARTITION_KEYWORDS) {
		return nil
	}

	subpartition := upperText(p.prev) == "SUBPARTITION"
	return p.expression(New(
		KPartition,
		"subpartition", subpartition,
		"expressions", p.parseWrappedCSV(p.parseDisjunction, TK_COMMA, false),
	))
}

// baseParseValue parses one VALUES row: a parenthesized tuple (with DEFAULT entries where
// supported) or a single expression as a one-element tuple. values is not used here; dialect
// overrides use it.
func (p *Parser) baseParseValue(values bool) *Expr {
	parseValueExpression := func() *Expr {
		if p.d.S.SUPPORTS_VALUES_DEFAULT && p.match(TK_DEFAULT) {
			return VarExpr(upperText(p.prev))
		}
		return p.parseExpression()
	}

	if p.match(TK_L_PAREN) {
		expressions := p.parseCSV(parseValueExpression, TK_COMMA)
		p.matchRParen(nil)
		return p.expression(New(KTuple, "expressions", expressions))
	}

	// In some dialects we can have VALUES 1, 2 which results in 1 column & 2 rows.
	expression := p.parseExpression()
	if expression != nil {
		return p.expression(New(KTuple, "expressions", []*Expr{expression}))
	}
	return nil
}

// baseParseProjections parses the SELECT list. The second result, an exclude list, is always nil
// here; dialect overrides may set it.
func (p *Parser) baseParseProjections() ([]*Expr, []*Expr) {
	return p.parseExpressions(), nil
}

// baseParseWrappedSelect parses what follows "(": PIVOT/UNPIVOT, a FROM-first query, a table
// (when table is set) or a SELECT, followed by set operations and query modifiers.
func (p *Parser) baseParseWrappedSelect(table bool) *Expr {
	var this *Expr
	if p.matchAny(TK_PIVOT, TK_UNPIVOT) {
		this = p.parseSimplifiedPivot(p.prev.Type == TK_UNPIVOT)
	} else {
		// A FROM-first query or TABLE name query is parsed as a query even when table is set
		if table && !p.matchNoAdvance(TK_FROM) && !p.isTableQuery() {
			this = p.parseTable(false, false, nil, false, false, false, true)
		} else {
			this = p.parseSelect(true, false, true, false, true, nil)
		}

		// Transform Values into a Table to pass through parseQueryModifiers
		// in case a modifier (e.g. join) is following
		if table && this.IsA(KValues) && this.Alias() != "" {
			alias := this.ArgE("alias").Pop()
			this = New(KTable, "this", this, "alias", alias)
		}

		this = p.parseQueryModifiers(p.parseSetOperations(this))
	}

	return this
}

// parseSelect parses a query (see parseSelectQuery) and any pipe syntax (|>) after it. from is
// the FROM already parsed for a FROM-first query, or nil.
func (p *Parser) parseSelect(nested bool, table bool, parseSubqueryAlias bool, parseSetOperation bool, consumePipe bool, from *Expr) *Expr {
	p.enter()
	defer p.leave()
	query := p.parseSelectQuery(nested, table, parseSubqueryAlias, parseSetOperation)

	if consumePipe && p.matchNoAdvance(TK_PIPE_GT) {
		if query == nil && from != nil {
			query = chunkBSelectFrom(chunkBSelectStar(), from)
		}
		if query.IsA(KQuery) {
			query = p.parsePipeSyntaxQuery(query)
			if query != nil && table {
				query = New(KSubquery, "this", query, "alias", nil)
			}
		}
	}

	return query
}

// parseSelectQuery parses a query: WITH followed by a statement, SELECT, TABLE name (when table
// is not set), a parenthesized query (when nested or table is set), VALUES, FROM-first, SUMMARIZE
// or DESCRIBE, followed by set operations when parseSetOperation is set.
func (p *Parser) parseSelectQuery(nested bool, table bool, parseSubqueryAlias bool, parseSetOperation bool) *Expr {
	cte := p.parseWith(false)

	if cte != nil {
		this := p.parseStatement()

		if this == nil {
			p.raiseError("Failed to parse any statement following CTE", nil)
			return cte
		}

		for this.IsA(KSubquery) && this.IsWrapper() {
			this = this.This()
		}

		if this == nil {
			panic("assert this is not None")
		}
		if this.Kind().hasArgType("with_") {
			if innerCte := this.ArgE("with_"); innerCte != nil {
				merged := append(append([]*Expr{}, cte.Expressions()...), innerCte.Expressions()...)
				cte.Set("expressions", merged)
				if innerCte.ArgB("recursive") {
					cte.Set("recursive", true)
				}
			}
			this.Set("with_", cte)
		} else {
			p.raiseError(this.Key()+" does not support CTE", nil)
			this = cte
		}

		return this
	}

	// duckdb supports leading with FROM x
	var from *Expr
	var laterals []*Expr
	if p.matchNoAdvance(TK_FROM) {
		from = p.parseFrom(true, false, true)
		for {
			lateral := p.parseLateral()
			if lateral == nil {
				break
			}
			laterals = append(laterals, lateral)
		}
	}

	// Hive supports multi-table inserts: FROM x INSERT ... SELECT ... [INSERT ... SELECT ...]
	if from != nil && !nested && !table && p.matchNoAdvance(TK_INSERT) {
		source := from.This()
		source.Set("laterals", chunkCOptList(laterals))

		var inserts []*Expr
		for p.match(TK_INSERT) {
			comments := p.prevComments
			insert := p.parseInsert()
			insert.AddComments(comments, true)
			inserts = append(inserts, insert)
		}

		return p.expressionC(New(KMultitableInserts, "expressions", inserts, "source", source), from.Comments())
	}

	var this *Expr
	if p.match(TK_SELECT) {
		comments := p.prevComments

		hint := p.parseHint()

		var all, matchedDistinct bool
		if p.next.ok() && p.next.Type != TK_DOT {
			all = p.match(TK_ALL)
			matchedDistinct = p.matchSet(p.s.DISTINCT_TOKENS)
		}

		var kind any
		if p.match(TK_ALIAS) && p.matchTexts("STRUCT", "VALUE") {
			kind = upperText(p.prev)
		}

		var distinct *Expr
		if matchedDistinct {
			var on *Expr
			if p.match(TK_ON) {
				on = p.parseValue(false)
			}
			distinct = p.expression(New(KDistinct, "on", on))
		}

		operationModifiers := []*Expr{}
		for p.curr.ok() && p.matchTextSet(p.s.OPERATION_MODIFIERS) {
			operationModifiers = append(operationModifiers, VarExpr(upperText(p.prev)))
		}

		limit := p.parseLimit(nil, true, false)

		// Some dialects (e.g. Redshift, T-SQL) allow SELECT TOP N DISTINCT ...
		if limit != nil && !matchedDistinct && !all {
			matchedDistinct = p.matchSet(p.s.DISTINCT_TOKENS)
			if matchedDistinct {
				var on *Expr
				if p.match(TK_ON) {
					on = p.parseValue(false)
				}
				distinct = p.expression(New(KDistinct, "on", on))
			} else {
				all = p.match(TK_ALL)
			}
		}

		if all && distinct != nil {
			p.raiseError("Cannot specify both ALL and DISTINCT after SELECT", nil)
		}

		projections, exclude := p.parseProjections()
		var excludeArg any
		if exclude != nil {
			excludeArg = exclude
		}
		var operationModifiersArg any
		if len(operationModifiers) > 0 {
			operationModifiersArg = operationModifiers
		}

		this = p.expression(New(
			KSelect,
			"kind", kind,
			"hint", hint,
			"distinct", distinct,
			"expressions", projections,
			"limit", limit,
			"exclude", excludeArg,
			"operation_modifiers", operationModifiersArg,
			"laterals", chunkCOptList(laterals),
		))
		this.SetComments(comments)

		into := p.parseInto()
		if into != nil {
			this.Set("into", into)
		}

		if from == nil {
			from = p.parseFrom(false, false, false)
		}

		if from != nil {
			this.Set("from_", from)
		}

		this = p.parseQueryModifiers(this)
	} else if !table && p.isTableQuery() {
		// TABLE name is shorthand for SELECT * FROM name
		p.advance(1)
		comments := p.prevComments
		only := p.match(TK_ONLY)
		relation := p.parseWrapped(func() *Expr { return p.parseTableParts(false, false, false, false) }, true)
		if only && relation != nil {
			relation.Set("only", true)
		}
		p.match(TK_STAR)
		this = p.expressionC(New(
			KSelect,
			"expressions", []*Expr{New(KStar)},
			"from_", New(KFrom, "this", relation),
		), comments)
		this = p.parseQueryModifiers(this)
	} else if (table || nested) && p.match(TK_L_PAREN) {
		comments := p.prevComments
		this = p.parseWrappedSelect(table)

		if this != nil {
			this.AddComments(comments, true)
		}

		// We return early here so that the UNION isn't attached to the subquery by the
		// following call to parseSetOperations, but instead becomes the parent node
		p.matchRParen(nil)
		return p.parseSubquery(this, parseSubqueryAlias)
	} else if p.matchNoAdvance(TK_VALUES) {
		this = p.parseDerivedTableValues(false)
	} else if from != nil {
		this = chunkBSelectFrom(chunkBSelectStar(), from.This())
		this.Set("laterals", chunkCOptList(laterals))
		this = p.parseQueryModifiers(this)
	} else if p.match(TK_SUMMARIZE) {
		summarizeTable := p.match(TK_TABLE)
		this = p.parseSelect(false, false, true, true, true, nil)
		if this == nil {
			this = p.parseString()
		}
		if this == nil {
			this = p.parseTable(false, false, nil, false, false, false, false)
		}
		return p.expression(New(KSummarize, "this", this, "table", summarizeTable))
	} else if p.match(TK_DESCRIBE) {
		this = p.parseDescribe()
	} else {
		this = nil
	}

	if parseSetOperation {
		return p.parseSetOperations(this)
	}
	return this
}

func (p *Parser) isTableQuery() bool {
	return p.s.SUPPORTS_TABLE_QUERY &&
		p.curr.Type == TK_TABLE &&
		(p.s.ID_VAR_TOKENS.Has(p.next.Type) || p.next.Type == TK_ONLY)
}

func (p *Parser) parseRecursiveWithSearch() *Expr {
	p.matchTextSeq("SEARCH")

	kind := ""
	if p.matchTextSet(p.s.RECURSIVE_CTE_SEARCH_KIND) {
		kind = upperText(p.prev)
	}

	if kind == "" {
		return nil
	}

	p.matchTextSeq("FIRST", "BY")

	this := p.parseIdVar(true, nil)
	expression := chunkBAndE(p.matchTextSeq("SET"), func() *Expr { return p.parseIdVar(true, nil) })
	to := chunkBAndE(p.matchTextSeq("TO"), p.parseBitwise)
	def := chunkBAndE(p.match(TK_DEFAULT), p.parseBitwise)
	using := chunkBAndE(p.matchTextSeq("USING"), func() *Expr { return p.parseIdVar(true, nil) })
	return p.expression(New(
		KRecursiveWithSearch,
		"kind", kind,
		"this", this,
		"expression", expression,
		"to", to,
		"default", def,
		"using", using,
	))
}

// parseWith parses WITH [RECURSIVE] cte, ... with an optional SEARCH and / or CYCLE clause.
// skipWithToken means WITH was already consumed.
func (p *Parser) parseWith(skipWithToken bool) *Expr {
	if !skipWithToken && !p.match(TK_WITH) {
		return nil
	}

	comments := p.prevComments
	recursive := p.match(TK_RECURSIVE)

	var lastComments []string
	expressions := []*Expr{}
	var udfs []*Expr
	for {
		cte := p.parseCte()
		if cte != nil {
			// Inline function specifications are kept apart from the CTEs
			if cte.IsA(KFunctionSpecification) {
				udfs = append(udfs, cte)
			} else {
				expressions = append(expressions, cte)
			}
			if len(lastComments) > 0 {
				cte.AddComments(lastComments, false)
			}
		}

		if !p.match(TK_COMMA) && !p.match(TK_WITH) {
			break
		} else {
			p.match(TK_WITH)
			// A later WITH, e.g. the query's own after inline functions, may be RECURSIVE
			recursive = p.match(TK_RECURSIVE) || recursive
		}

		lastComments = p.prevComments
	}

	var recursiveArg any
	if recursive {
		recursiveArg = true
	}
	var udfsArg any
	if len(udfs) > 0 {
		udfsArg = udfs
	}
	search := p.parseRecursiveWithSearch()

	var cycle *Expr
	if search != nil && search.ArgS("kind") != "CYCLE" && p.matchTextSeqNoAdvance("CYCLE") {
		cycle = p.parseRecursiveWithSearch()
	}

	return p.expressionC(New(
		KWith,
		"expressions", expressions,
		"recursive", recursiveArg,
		"search", search,
		"cycle", cycle,
		"udfs", udfsArg,
	), comments)
}

// baseParseCte parses name [(columns)] [USING KEY (...)] AS [[NOT] MATERIALIZED] (statement); a
// VALUES body becomes SELECT * FROM VALUES. When AS is required but missing it rewinds and
// returns nil.
func (p *Parser) baseParseCte() *Expr {
	index := p.index

	alias := p.parseTableAlias(&p.s.ID_VAR_TOKENS)
	if alias == nil || !truthy(alias.Arg("this")) {
		p.raiseError("Expected CTE to have alias", nil)
	}

	var keyExpressions any
	if p.matchTextSeq("USING", "KEY") {
		keyExpressions = p.parseWrappedIdVars(false)
	}

	if !p.match(TK_ALIAS) && !p.s.OPTIONAL_ALIAS_TOKEN_CTE {
		p.retreat(index)
		return nil
	}

	comments := p.prevComments

	var materialized any
	if p.matchTextSeq("NOT", "MATERIALIZED") {
		materialized = false
	} else if p.matchTextSeq("MATERIALIZED") {
		materialized = true
	}

	this := p.parseWrapped(p.parseStatement, false)
	cte := p.expressionC(New(
		KCTE,
		"this", this,
		"alias", alias,
		"materialized", materialized,
		"key_expressions", keyExpressions,
	), comments)

	values := cte.This()
	if values.IsA(KValues) {
		cte.Set("this", p.valuesToSelect(values))
	}

	return cte
}

// valuesToSelect wraps VALUES in SELECT * FROM, aliasing it _values when it has no alias. Query
// modifiers parsed onto the VALUES (e.g. ORDER BY, LIMIT) move to the SELECT.
func (p *Parser) valuesToSelect(values *Expr) *Expr {
	var modifierKeys []string
	var modifiers []any
	for _, key := range QUERY_MODIFIERS {
		if v := values.Arg(key); truthy(v) {
			modifierKeys = append(modifierKeys, key)
			modifiers = append(modifiers, v)
			values.Set(key, nil)
		}
	}

	source := values
	if values.Alias() == "" {
		source = chunkBAlias(values, "_values", true, true)
	}
	sel := chunkBSelectFrom(chunkBSelectStar(), source)
	for i, key := range modifierKeys {
		sel.Set(key, modifiers[i])
	}
	return sel
}

// baseParseTableAlias parses [AS] alias [(columns)]; aliasTokens defaults to TABLE_ALIAS_TOKENS.
func (p *Parser) baseParseTableAlias(aliasTokens *TokenSet) *Expr {
	// In some dialects, LIMIT and OFFSET can act as both identifiers and keywords (clauses)
	// so this section tries to parse the clause version and if it fails, it treats the token
	// as an identifier (alias)
	if p.canParseLimitOrOffset() {
		return nil
	}

	// START followed by WITH is not an implicit alias: it begins a START WITH ... CONNECT BY clause
	if upperText(p.curr) == "START" && upperText(p.next) == "WITH" {
		return nil
	}

	anyToken := p.match(TK_ALIAS)
	tokens := aliasTokens
	if tokens == nil || *tokens == (TokenSet{}) {
		tokens = &p.s.TABLE_ALIAS_TOKENS
	}
	alias := p.parseIdVar(anyToken, tokens)
	if alias == nil {
		alias = p.parseStringAsIdentifier()
	}

	index := p.index
	var columns any
	var cols []*Expr
	if p.match(TK_L_PAREN) {
		cols = p.parseCSV(p.parseFunctionParameter, TK_COMMA)
		if len(cols) > 0 {
			p.matchRParen(nil)
		} else {
			p.retreat(index)
		}
		columns = cols
	}

	if alias == nil && len(cols) == 0 {
		return nil
	}

	tableAlias := p.expression(New(KTableAlias, "this", alias, "columns", columns))

	// We bubble up comments from the Identifier to the TableAlias
	if alias.IsA(KIdentifier) {
		tableAlias.AddComments(alias.PopComments(), false)
	}

	return tableAlias
}

func (p *Parser) parseSubquery(this *Expr, parseAlias bool) *Expr {
	if this == nil {
		return nil
	}

	var pivots any
	if pv := p.parsePivots(); len(pv) > 0 {
		pivots = pv
	}
	var alias *Expr
	if parseAlias {
		alias = p.parseTableAlias(nil)
	}
	sample := p.parseTableSample(false)
	return p.expression(New(
		KSubquery,
		"this", this,
		"pivots", pivots,
		"alias", alias,
		"sample", sample,
	))
}

// implicitUnnestsToExplicit rewrites joins on a column path of an earlier table, as in
// FROM t, t.arr, into explicit UNNESTs.
func (p *Parser) implicitUnnestsToExplicit(this *Expr) *Expr {
	refs := map[string]bool{
		chunkBNormalizeIdentifiers(this.ArgE("from_").This().Copy(), p.d).AliasOrName(): true,
	}
	for _, join := range this.ArgL("joins") {
		table := join.This()
		normalizedTable := table.Copy()
		normalizedTable.Meta()["maybe_column"] = true
		normalizedTable = chunkBNormalizeIdentifiers(normalizedTable, p.d)

		if table.IsA(KTable) && !join.ArgB("on") {
			parts := normalizedTable.Parts()
			if len(parts) > 1 && refs[parts[0].Name()] {
				tableAsColumn := chunkBTableToColumn(table, true)
				unnest := New(KUnnest, "expressions", []*Expr{tableAsColumn})

				// chunkBTableToColumn creates a parent Alias node that we want to convert to
				// a TableAlias and attach to the Unnest, so it matches the parser's output
				if alias := table.ArgE("alias"); alias.IsA(KTableAlias) {
					tableAsColumn.Replace(tableAsColumn.This())
					chunkBAlias(unnest, nil, []*Expr{alias.This()}, false)
				}

				table.Replace(unnest)
			}
		}

		refs[normalizedTable.AliasOrName()] = true
	}

	return this
}

// parseQueryModifiers parses the joins, laterals and QUERY_MODIFIER_PARSERS clauses (WHERE,
// GROUP BY, LIMIT, ...) that follow this, moving a LIMIT's offset into a separate Offset. It then
// makes implicit unnests explicit where the dialect has them.
func (p *Parser) parseQueryModifiers(this *Expr) *Expr {
	if this.IsA(p.s.MODIFIABLES...) {
		for join := range p.parseJoins(nil) {
			this.Append("joins", join)
		}
		for {
			lateral := p.parseLateral()
			if lateral == nil {
				break
			}
			this.Append("laterals", lateral)
		}

		for {
			if parser, ok := p.s.QUERY_MODIFIER_PARSERS[p.curr.Type]; ok {
				modifierToken := p.curr

				// A LIMIT or FETCH after TOP is left for an enclosing set operation, so
				// SELECT 1 UNION ALL SELECT TOP 2 2 LIMIT 1 limits the union.
				if (modifierToken.Type == TK_LIMIT || modifierToken.Type == TK_FETCH) && isTopLimit(this.ArgE("limit")) {
					break
				}
				key, expression := parser(p)

				if truthy(expression) {
					if this.ArgB(key) {
						p.raiseError("Found multiple '"+upperText(modifierToken)+"' clauses", modifierToken)
					}

					this.Set(key, expression)
					if key == "limit" {
						limitExpr := expression.(*Expr)
						offset := limitExpr.Arg("offset")
						limitExpr.Set("offset", nil)

						if truthy(offset) {
							offsetExpr := New(KOffset, "expression", offset)
							this.Set("offset", offsetExpr)

							limitByExpressions := limitExpr.Expressions()
							limitExpr.Set("expressions", nil)
							offsetExpr.Set("expressions", limitByExpressions)
						}
					}
					continue
				}
			}

			if upperText(p.curr) == "START" {
				modifierToken := p.curr
				if connect := p.parseConnect(false); connect != nil {
					if this.ArgB("connect") {
						p.raiseError("Found multiple 'START WITH' clauses", modifierToken)
					}

					this.Set("connect", connect)
					continue
				}
			}
			break
		}
	}

	if p.s.SUPPORTS_IMPLICIT_UNNEST && this != nil && this.ArgB("from_") {
		this = p.implicitUnnestsToExplicit(this)
	}

	return this
}

func isTopLimit(limit *Expr) bool {
	return limit != nil && limit.MetaGet("top") == true
}

// parseHintFallbackToString consumes the remaining tokens and keeps their SQL text as a single
// hint.
func (p *Parser) parseHintFallbackToString() *Expr {
	start := p.curr
	for p.curr.ok() {
		p.advance(1)
	}

	// the last consumed token; an index of -1 wraps around to the last token
	i := p.index - 1
	if i < 0 {
		i += len(p.tokens)
	}
	end := p.tokens[i]
	return New(KHint, "expressions", []string{p.findSQL(start, end)})
}

func (p *Parser) baseParseHintFunctionCall() *Expr {
	return p.parseFunctionCall(nil, false, true, false)
}

// parseHintBody parses a list of hints, falling back to the raw text when parsing fails or tokens
// remain.
func (p *Parser) parseHintBody() *Expr {
	startIndex := p.index
	shouldFallbackToString := false

	hints := []*Expr{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(parsePanic); ok {
					shouldFallbackToString = true
					return
				}
				panic(r)
			}
		}()
		for {
			hint := p.parseCSV(func() *Expr {
				h := p.parseHintFunctionCall()
				if h == nil {
					h = p.parseVar(false, nil, true)
				}
				return h
			}, TK_COMMA)
			if len(hint) == 0 {
				break
			}
			hints = append(hints, hint...)
		}
	}()

	if shouldFallbackToString || p.curr.ok() {
		p.retreat(startIndex)
		return p.parseHintFallbackToString()
	}

	return p.expression(New(KHint, "expressions", hints))
}

// parseHint parses a /*+ ... */ hint, whose text is the comment of the HINT token.
func (p *Parser) parseHint() *Expr {
	if p.match(TK_HINT) && len(p.prevComments) > 0 {
		return chunkBParseOneInto(p.d, KHint, p.prevComments[0])
	}

	return nil
}

func (p *Parser) baseParseInto() *Expr {
	if !p.match(TK_INTO) {
		return nil
	}

	temp := p.match(TK_TEMPORARY)
	unlogged := p.matchTextSeq("UNLOGGED")
	p.match(TK_TABLE)

	this := p.parseTable(true, false, nil, false, false, false, false)
	return p.expression(New(KInto, "this", this, "temporary", temp, "unlogged", unlogged))
}

// parseFrom parses FROM table, with the joins when joins is set. skipFromToken means FROM was
// already consumed.
func (p *Parser) parseFrom(joins bool, skipFromToken bool, consumePipe bool) *Expr {
	if !skipFromToken && !p.match(TK_FROM) {
		return nil
	}

	comments := p.prevComments
	this := p.parseTable(false, joins, nil, false, false, false, consumePipe)
	return p.expressionC(New(KFrom, "this", this), comments)
}
