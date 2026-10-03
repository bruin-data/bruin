package sqlengine

// Port of sqlglot/parser.py L3799-4314 (chunk B, part 2): partition, VALUES tuples, projections,
// wrapped select, SELECT, WITH / CTE, recursive search, table alias, subquery, implicit unnests,
// query modifiers, hints, INTO, FROM.

// chunkBSelectStar mirrors exp.select("*"): a Select whose only projection is the Star parsed
// from "*" (including the positions recorded by _parse_star_ops).
func chunkBSelectStar() *Expr {
	star := New(KStar, "ilike", nil, "except_", nil, "replace", nil, "rename", nil)
	m := star.Meta()
	m["line"] = 1
	m["col"] = 1
	m["start"] = 0
	m["end"] = 0
	sel := New(KSelect)
	sel.Set("expressions", []*Expr{star})
	return sel
}

// chunkBSelectFrom mirrors Select.from_(expression) for an expression argument: non-From
// expressions are wrapped in a From; the expression itself is not copied.
func chunkBSelectFrom(sel *Expr, expression *Expr) *Expr {
	if expression == nil {
		// maybe_parse(None) raises ParseError("SQL cannot be None")
		panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	if !expression.IsA(KFrom) {
		expression = New(KFrom, "this", expression)
	}
	sel.Set("from_", expression)
	return sel
}

// chunkBToIdentifier mirrors exp.to_identifier(name, quoted=None, copy=copy) for nil, string
// and expression inputs.
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
		panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + kindClassRepr(v.Kind())})
	}
	panic(&ValueError{Msg: "Name needs to be a string or an Identifier"})
}

// chunkBAlias mirrors exp.alias_(expression, alias, table=table, copy=copy) for an expression input.
// alias is nil, a string or an Identifier; table is a bool or a []*Expr of column names.
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

	// We don't set the "alias" arg for Window expressions (see exp.alias_)
	if e.Kind().hasArgType("alias") && e.Kind() != KWindow {
		e.Set("alias", id)
		return e
	}
	return New(KAlias, "this", e, "alias", id)
}

// chunkBColumn mirrors exp.column(col, table, db, catalog, fields=fields, copy=copy) for
// expression parts.
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

// chunkBTableToColumn mirrors Table.to_column(copy=copy).
func chunkBTableToColumn(table *Expr, copy bool) *Expr {
	parts := table.Parts()
	lastPart := parts[len(parts)-1]

	var col *Expr
	if lastPart.IsA(KIdentifier) {
		// column(*reversed(parts[0:4]), fields=parts[4:], copy=copy)
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
		// This branch will be reached if a function or array is wrapped in a `Table`
		col = lastPart
	}

	if alias := table.ArgE("alias"); alias != nil {
		col = chunkBAlias(col, alias.Arg("this"), false, copy)
	}

	return col
}

// chunkBNormalizeIdentifiers mirrors optimizer.normalize_identifiers.normalize_identifiers(expression, dialect).
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

// chunkBParseOneInto mirrors exp.maybe_parse(sql, into=kind, dialect=d) for a string input.
// Errors propagate like the Python exceptions (ParseError is catchable by _try_parse).
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

// _parse_partition (parser.py L3799)
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

// _parse_value (parser.py L3810)
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

// _parse_projections (parser.py L3827)
func (p *Parser) baseParseProjections() ([]*Expr, []*Expr) {
	return p.parseExpressions(), nil
}

// _parse_wrapped_select (parser.py L3832)
func (p *Parser) baseParseWrappedSelect(table bool) *Expr {
	var this *Expr
	if p.matchAny(TK_PIVOT, TK_UNPIVOT) {
		this = p.parseSimplifiedPivot(p.prev.Type == TK_UNPIVOT)
	} else if p.match(TK_FROM) {
		from := p.parseFrom(true, true, true)
		// Support parentheses for duckdb FROM-first syntax
		sel := p.parseSelect(false, false, true, true, true, from)
		if sel != nil {
			if !sel.ArgB("from_") {
				sel.Set("from_", from)
			}
			this = sel
		} else {
			this = chunkBSelectFrom(chunkBSelectStar(), from)
			this = p.parseQueryModifiers(p.parseSetOperations(this))
		}
	} else {
		if table {
			this = p.parseTable(false, false, nil, false, false, false, true)
		} else {
			this = p.parseSelect(true, false, true, false, true, nil)
		}

		// Transform exp.Values into a exp.Table to pass through parse_query_modifiers
		// in case a modifier (e.g. join) is following
		if table && this.IsA(KValues) && this.Alias() != "" {
			alias := this.ArgE("alias").Pop()
			this = New(KTable, "this", this, "alias", alias)
		}

		this = p.parseQueryModifiers(p.parseSetOperations(this))
	}

	return this
}

// _parse_select (parser.py L3865)
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
				// query.subquery(copy=False)
				query = New(KSubquery, "this", query, "alias", nil)
			}
		}
	}

	return query
}

// _parse_select_query (parser.py L3890)
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
	if p.matchNoAdvance(TK_FROM) {
		from = p.parseFrom(true, false, true)
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
		))
		this.Comments = comments

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
	} else if (table || nested) && p.match(TK_L_PAREN) {
		comments := p.prevComments
		this = p.parseWrappedSelect(table)

		if this != nil {
			this.AddComments(comments, true)
		}

		// We return early here so that the UNION isn't attached to the subquery by the
		// following call to _parse_set_operations, but instead becomes the parent node
		p.matchRParen(nil)
		return p.parseSubquery(this, parseSubqueryAlias)
	} else if p.matchNoAdvance(TK_VALUES) {
		this = p.parseDerivedTableValues()
	} else if from != nil {
		this = chunkBSelectFrom(chunkBSelectStar(), from.This())
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

// _parse_recursive_with_search (parser.py L4032)
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
	using := chunkBAndE(p.matchTextSeq("USING"), func() *Expr { return p.parseIdVar(true, nil) })
	return p.expression(New(
		KRecursiveWithSearch,
		"kind", kind,
		"this", this,
		"expression", expression,
		"using", using,
	))
}

// _parse_with (parser.py L4051)
func (p *Parser) parseWith(skipWithToken bool) *Expr {
	if !skipWithToken && !p.match(TK_WITH) {
		return nil
	}

	comments := p.prevComments
	recursive := p.match(TK_RECURSIVE)

	var lastComments []string
	expressions := []*Expr{}
	for {
		cte := p.parseCte()
		if cte.IsA(KCTE) {
			expressions = append(expressions, cte)
			if len(lastComments) > 0 {
				cte.AddComments(lastComments, false)
			}
		}

		if !p.match(TK_COMMA) && !p.match(TK_WITH) {
			break
		} else {
			p.match(TK_WITH)
		}

		lastComments = p.prevComments
	}

	var recursiveArg any
	if recursive {
		recursiveArg = true
	}
	search := p.parseRecursiveWithSearch()
	return p.expressionC(New(
		KWith,
		"expressions", expressions,
		"recursive", recursiveArg,
		"search", search,
	), comments)
}

// _parse_cte (parser.py L4083)
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

// _values_to_select (parser.py L4123)
func (p *Parser) valuesToSelect(values *Expr) *Expr {
	if values.Alias() != "" {
		return chunkBSelectFrom(chunkBSelectStar(), values)
	}
	return chunkBSelectFrom(chunkBSelectStar(), chunkBAlias(values, "_values", true, true))
}

// _parse_table_alias (parser.py L4128)
func (p *Parser) parseTableAlias(aliasTokens *TokenSet) *Expr {
	// In some dialects, LIMIT and OFFSET can act as both identifiers and keywords (clauses)
	// so this section tries to parse the clause version and if it fails, it treats the token
	// as an identifier (alias)
	if p.canParseLimitOrOffset() {
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

// _parse_subquery (parser.py L4161)
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

// _implicit_unnests_to_explicit (parser.py L4176)
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

				// Table.to_column creates a parent Alias node that we want to convert to
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

// _parse_query_modifiers (parser.py L4209)
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
			break
		}
	}

	if p.s.SUPPORTS_IMPLICIT_UNNEST && this != nil && this.ArgB("from_") {
		this = p.implicitUnnestsToExplicit(this)
	}

	return this
}

// _parse_hint_fallback_to_string (parser.py L4249)
func (p *Parser) parseHintFallbackToString() *Expr {
	start := p.curr
	for p.curr.ok() {
		p.advance(1)
	}

	// self._tokens[self._index - 1] (Python negative indexing)
	i := p.index - 1
	if i < 0 {
		i += len(p.tokens)
	}
	end := p.tokens[i]
	return New(KHint, "expressions", []string{p.findSQL(start, end)})
}

// _parse_hint_function_call (parser.py L4257)
func (p *Parser) baseParseHintFunctionCall() *Expr {
	return p.parseFunctionCall(nil, false, true, false)
}

// _parse_hint_body (parser.py L4260)
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

// _parse_hint (parser.py L4282)
func (p *Parser) parseHint() *Expr {
	if p.match(TK_HINT) && len(p.prevComments) > 0 {
		return chunkBParseOneInto(p.d, KHint, p.prevComments[0])
	}

	return nil
}

// _parse_into (parser.py L4288)
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

// _parse_from (parser.py L4300)
func (p *Parser) parseFrom(joins bool, skipFromToken bool, consumePipe bool) *Expr {
	if !skipFromToken && !p.match(TK_FROM) {
		return nil
	}

	comments := p.prevComments
	this := p.parseTable(false, joins, nil, false, false, false, consumePipe)
	return p.expressionC(New(KFrom, "this", this), comments)
}
