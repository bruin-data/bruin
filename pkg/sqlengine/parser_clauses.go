package sqlengine

import "fmt"

// Port of sqlglot/parser.py (chunk C, part 2): WHERE / PREWHERE, GROUP BY, HAVING, QUALIFY,
// CONNECT BY, INTERPOLATE, ORDER BY, LIMIT / OFFSET / FETCH, named window detection, locks
// and set operations.

// _parse_prewhere (parser.py L5362)
func (p *Parser) parsePrewhere(skipWhereToken bool) *Expr {
	if !skipWhereToken && !p.match(TK_PREWHERE) {
		return nil
	}

	comments := p.prevComments
	return p.expressionC(New(KPreWhere, "this", p.parseDisjunction()), comments)
}

// _parse_where (parser.py L5372)
func (p *Parser) parseWhere(skipWhereToken bool) *Expr {
	if !skipWhereToken && !p.match(TK_WHERE) {
		return nil
	}

	comments := p.prevComments
	return p.expressionC(New(KWhere, "this", p.parseDisjunction()), comments)
}

// _parse_group (parser.py L5382)
func (p *Parser) parseGroup(skipGroupByToken bool) *Expr {
	if !skipGroupByToken && !p.match(TK_GROUP_BY) {
		return nil
	}
	comments := p.prevComments

	// defaultdict(list): keys are created in first-access order
	elements := &chunkCKwargs{}

	if p.match(TK_ALL) {
		elements.set("all", true)
	} else if p.match(TK_DISTINCT) {
		elements.set("all", false)
	}

	if p.matchSetNoAdvance(p.s.QUERY_MODIFIER_TOKENS) {
		return p.expressionC(New(KGroup, elements.kv()...), comments)
	}

	for {
		index := p.index

		elements.extendList("expressions", p.parseCSV(func() *Expr {
			if p.matchAnyNoAdvance(TK_CUBE, TK_ROLLUP) {
				return nil
			}
			return p.parseDisjunction()
		}, TK_COMMA))

		beforeWithIndex := p.index
		withPrefix := p.match(TK_WITH)

		if cubeOrRollup := p.parseCubeOrRollup(withPrefix); cubeOrRollup != nil {
			key := "cube"
			if cubeOrRollup.IsA(KRollup) {
				key = "rollup"
			}
			elements.appendList(key, cubeOrRollup)
		} else if groupingSets := p.parseGroupingSets(); groupingSets != nil {
			elements.appendList("grouping_sets", groupingSets)
		} else if p.matchTextSeq("TOTALS") {
			elements.set("totals", true)
		}

		if beforeWithIndex <= p.index && p.index <= beforeWithIndex+1 {
			p.retreat(beforeWithIndex)
			break
		}

		if index == p.index {
			break
		}
	}

	return p.expressionC(New(KGroup, elements.kv()...), comments)
}

// _parse_cube_or_rollup (parser.py L5430)
func (p *Parser) parseCubeOrRollup(withPrefix bool) *Expr {
	var kind Kind
	if p.match(TK_CUBE) {
		kind = KCube
	} else if p.match(TK_ROLLUP) {
		kind = KRollup
	} else {
		return nil
	}

	var expressions []*Expr
	if withPrefix {
		expressions = []*Expr{}
	} else {
		expressions = p.parseWrappedCSV(p.parseBitwise, TK_COMMA, false)
	}
	return p.expression(New(kind, "expressions", expressions))
}

// _parse_grouping_sets (parser.py L5442)
func (p *Parser) parseGroupingSets() *Expr {
	if p.match(TK_GROUPING_SETS) {
		return p.expression(New(
			KGroupingSets,
			"expressions", p.parseWrappedCSV(p.parseGroupingSet, TK_COMMA, false),
		))
	}
	return nil
}

// _parse_grouping_set (parser.py L5449)
func (p *Parser) parseGroupingSet() *Expr {
	if e := p.parseGroupingSets(); e != nil {
		return e
	}
	if e := p.parseCubeOrRollup(false); e != nil {
		return e
	}
	return p.parseBitwise()
}

// _parse_having (parser.py L5452)
func (p *Parser) parseHaving(skipHavingToken bool) *Expr {
	if !skipHavingToken && !p.match(TK_HAVING) {
		return nil
	}
	comments := p.prevComments
	return p.expressionC(New(KHaving, "this", p.parseDisjunction()), comments)
}

// _parse_qualify (parser.py L5461)
func (p *Parser) parseQualify() *Expr {
	if !p.match(TK_QUALIFY) {
		return nil
	}
	return p.expression(New(KQualify, "this", p.parseDisjunction()))
}

// _parse_connect_with_prior (parser.py L5466)
//
// sqlglot temporarily mutates the class-level NO_PAREN_FUNCTION_PARSERS table; the Go port keeps
// the temporary entry in a per-parser overlay so concurrent parsers don't race.
func (p *Parser) baseParseConnectWithPrior() *Expr {
	p.setNoParenOverlay("PRIOR", func(p *Parser) *Expr {
		return p.expression(New(KPrior, "this", p.parseBitwise()))
	})
	connect := p.parseDisjunction()
	// dict.pop("PRIOR") raises KeyError if a nested CONNECT BY already removed it
	if !p.popNoParenOverlay("PRIOR") {
		panic(fmt.Errorf("KeyError: 'PRIOR'"))
	}
	return connect
}

// _parse_connect (parser.py L5474)
func (p *Parser) parseConnect(skipStartToken bool) *Expr {
	var start *Expr
	if skipStartToken {
		start = nil
	} else if p.match(TK_START_WITH) {
		start = p.parseDisjunction()
	} else {
		return nil
	}

	p.match(TK_CONNECT_BY)
	nocycle := p.matchTextSeq("NOCYCLE")
	connect := p.parseConnectWithPrior()

	if start == nil && p.match(TK_START_WITH) {
		start = p.parseDisjunction()
	}

	return p.expression(New(KConnect, "start", start, "connect", connect, "nocycle", nocycle))
}

// _parse_name_as_expression (parser.py L5491)
func (p *Parser) parseNameAsExpression() *Expr {
	this := p.parseIdVar(true, nil)
	if p.match(TK_ALIAS) {
		this = p.expression(New(KAlias, "alias", this, "this", p.parseDisjunction()))
	}
	return this
}

// _parse_interpolate (parser.py L5497)
func (p *Parser) parseInterpolate() []*Expr {
	if p.matchTextSeq("INTERPOLATE") {
		return p.parseWrappedCSV(p.parseNameAsExpression, TK_COMMA, false)
	}
	return nil
}

// _parse_order (parser.py L5502)
func (p *Parser) parseOrder(this *Expr, skipOrderToken bool) *Expr {
	var siblings any // True | None
	if !skipOrderToken && !p.match(TK_ORDER_BY) {
		if !p.match(TK_ORDER_SIBLINGS_BY) {
			return this
		}

		siblings = true
	}

	comments := p.prevComments
	return p.expressionC(New(
		KOrder,
		"this", this,
		"expressions", p.parseCSV(func() *Expr { return p.parseOrdered(nil) }, TK_COMMA),
		"siblings", siblings,
	), comments)
}

// _parse_sort (parser.py L5522)
func (p *Parser) parseSort(expClass Kind, token TokenType) *Expr {
	if !p.match(token) {
		return nil
	}
	return p.expression(New(expClass, "expressions", p.parseCSV(func() *Expr { return p.parseOrdered(nil) }, TK_COMMA)))
}

// _parse_ordered (parser.py L5527)
func (p *Parser) parseOrdered(parseMethod func() *Expr) *Expr {
	var this *Expr
	if parseMethod != nil {
		this = parseMethod()
	} else {
		this = p.parseDisjunction()
	}
	if this == nil {
		return nil
	}

	if pyUpper(this.Name()) == "ALL" && p.d.S.SUPPORTS_ORDER_BY_ALL {
		this = VarExpr("ALL")
	}

	asc := p.match(TK_ASC)
	var desc any // bool | None
	if p.match(TK_DESC) {
		desc = true
	} else if asc {
		desc = false
	} else {
		desc = nil
	}

	isNullsFirst := p.matchTextSeq("NULLS", "FIRST")
	isNullsLast := p.matchTextSeq("NULLS", "LAST")

	nullsFirst := isNullsFirst
	explicitlyNullOrdered := isNullsFirst || isNullsLast

	nullOrdering := p.d.S.NULL_ORDERING
	descTruthy := truthy(desc)
	if !explicitlyNullOrdered &&
		((!descTruthy && nullOrdering == "nulls_are_small") ||
			(descTruthy && nullOrdering != "nulls_are_small")) &&
		nullOrdering != "nulls_are_last" {
		nullsFirst = true
	}

	var withFill *Expr
	if p.matchTextSeq("WITH", "FILL") {
		var from, to, step any = false, false, false
		if p.match(TK_FROM) {
			from = chunkCAnyExpr(p.parseBitwise())
		}
		if p.matchTextSeq("TO") {
			to = chunkCAnyExpr(p.parseBitwise())
		}
		if p.matchTextSeq("STEP") {
			step = chunkCAnyExpr(p.parseBitwise())
		}
		withFill = p.expression(New(
			KWithFill,
			"from_", from,
			"to", to,
			"step", step,
			"interpolate", chunkCOptList(p.parseInterpolate()),
		))
	} else {
		withFill = nil
	}

	return p.expression(New(KOrdered, "this", this, "desc", desc, "nulls_first", nullsFirst, "with_fill", withFill))
}

// _parse_limit_options (parser.py L5572)
func (p *Parser) parseLimitOptions() *Expr {
	percent := p.matchAny(TK_PERCENT, TK_MOD)
	rows := p.matchAny(TK_ROW, TK_ROWS)
	p.matchTextSeq("ONLY")
	withTies := p.matchTextSeq("WITH", "TIES")

	if !(percent || rows || withTies) {
		return nil
	}

	return p.expression(New(KLimitOptions, "percent", percent, "rows", rows, "with_ties", withTies))
}

// _parse_limit (parser.py L5583)
func (p *Parser) parseLimit(this *Expr, top bool, skipLimitToken bool) *Expr {
	limitToken := TK_LIMIT
	if top {
		limitToken = TK_TOP
	}
	if skipLimitToken || p.match(limitToken) {
		comments := p.prevComments
		var expression *Expr
		if top {
			limitParen := p.match(TK_L_PAREN)
			if limitParen {
				expression = p.parseTerm()
				if expression == nil {
					expression = p.parseSelect(false, false, true, true, true, nil)
				}
			} else {
				expression = p.parseNumber()
			}

			if limitParen {
				p.matchRParen(nil)
			}
		} else {
			if p.d.S.SUPPORTS_LIMIT_ALL && p.match(TK_ALL) {
				return this
			}

			// Parsing LIMIT x% (i.e x PERCENT) as a term leads to an error, since
			// we try to build an exp.Mod expr. For that matter, we backtrack and instead
			// consume the factor plus parse the percentage separately
			index := p.index
			expression = p.tryParseExpr(p.parseTerm, false)
			if expression.IsA(KMod) {
				p.retreat(index)
				expression = p.parseFactor()
			} else if expression == nil {
				expression = p.parseFactor()
			}
		}
		limitOptions := p.parseLimitOptions()

		var offset *Expr
		if p.match(TK_COMMA) {
			offset = expression
			expression = p.parseTerm()
		} else {
			offset = nil
		}

		limitExp := p.expressionC(New(
			KLimit,
			"this", this,
			"expression", expression,
			"offset", offset,
			"limit_options", limitOptions,
			"expressions", chunkCOptList(p.parseLimitBy()),
		), comments)

		return limitExp
	}

	if p.match(TK_FETCH) {
		direction := "FIRST"
		if p.matchAny(TK_FIRST, TK_NEXT) {
			direction = upperText(p.prev)
		}

		count := p.parseField(false, &p.s.FETCH_TOKENS, false)

		return p.expression(New(
			KFetch,
			"direction", direction,
			"count", count,
			"limit_options", p.parseLimitOptions(),
		))
	}

	return this
}

// _parse_offset (parser.py L5654)
func (p *Parser) parseOffset(this *Expr) *Expr {
	if !p.match(TK_OFFSET) {
		return this
	}

	count := p.parseTerm()
	p.matchAny(TK_ROW, TK_ROWS)

	return p.expression(New(
		KOffset,
		"this", this,
		"expression", count,
		"expressions", chunkCOptList(p.parseLimitBy()),
	))
}

// _can_parse_limit_or_offset (parser.py L5665)
func (p *Parser) canParseLimitOrOffset() bool {
	if !p.matchAnyNoAdvance(p.s.AMBIGUOUS_ALIAS_TOKENS...) {
		return false
	}

	index := p.index
	result := p.tryParseExpr(func() *Expr { return p.parseLimit(nil, false, false) }, true) != nil ||
		p.tryParseExpr(func() *Expr { return p.parseOffset(nil) }, true) != nil
	p.retreat(index)

	// MATCH_CONDITION (...) is a special construct that should not be consumed by limit/offset
	if p.next.Type == TK_MATCH_CONDITION {
		result = false
	}

	return result
}

// _can_parse_named_window (parser.py L5682)
func (p *Parser) canParseNamedWindow() bool {
	// `WINDOW` is in ID_VAR_TOKENS so it could be mistakenly consumed as an implicit alias.
	// Refuse only when the following tokens look like a named-window clause: `WINDOW <id> AS (`.
	if !p.matchNoAdvance(TK_WINDOW) {
		return false
	}

	var name *Token
	if p.index+1 < len(p.tokens) {
		name = p.tokens[p.index+1]
	}
	if name == nil || !p.s.ID_VAR_TOKENS.Has(name.Type) {
		return false
	}

	var aliasTok *Token
	if p.index+2 < len(p.tokens) {
		aliasTok = p.tokens[p.index+2]
	}
	if aliasTok == nil || aliasTok.Type != TK_ALIAS {
		return false
	}

	var body *Token
	if p.index+3 < len(p.tokens) {
		body = p.tokens[p.index+3]
	}
	return body != nil && body.Type == TK_L_PAREN
}

// _parse_limit_by (parser.py L5699)
func (p *Parser) parseLimitBy() []*Expr {
	if p.matchTextSeq("BY") {
		return p.parseCSV(p.parseBitwise, TK_COMMA)
	}
	return nil
}

// _parse_locks (parser.py L5702)
func (p *Parser) parseLocks() []*Expr {
	locks := []*Expr{}
	for {
		var update, key any // bool | None
		if p.matchTextSeq("FOR", "UPDATE") {
			update = true
		} else if p.matchTextSeq("FOR", "SHARE") || p.matchTextSeq("LOCK", "IN", "SHARE", "MODE") {
			update = false
		} else if p.matchTextSeq("FOR", "KEY", "SHARE") {
			update, key = false, true
		} else if p.matchTextSeq("FOR", "NO", "KEY", "UPDATE") {
			update, key = true, true
		} else {
			break
		}

		var expressions any
		if p.matchTextSeq("OF") {
			expressions = p.parseCSV(func() *Expr {
				return p.parseTable(true, false, nil, false, false, false, false)
			}, TK_COMMA)
		}

		var wait any // bool | Expr | None
		if p.matchTextSeq("NOWAIT") {
			wait = true
		} else if p.matchTextSeq("WAIT") {
			wait = chunkCAnyExpr(p.parsePrimary())
		} else if p.matchTextSeq("SKIP", "LOCKED") {
			wait = false
		}

		locks = append(locks, p.expression(New(
			KLock,
			"update", update,
			"expressions", expressions,
			"wait", wait,
			"key", key,
		)))
	}

	return locks
}

// parse_set_operation (parser.py L5739)
func (p *Parser) parseSetOperation(this *Expr, consumePipe bool) *Expr {
	start := p.index
	_, sideToken, kindToken := p.parseJoinParts()

	var side, kind any // str | None
	if sideToken.ok() {
		side = sideToken.Text
	}
	if kindToken.ok() {
		kind = kindToken.Text
	}

	if !p.matchSet(p.s.SET_OPERATIONS) {
		p.retreat(start)
		return nil
	}

	tokenType := p.prev.Type

	var operation Kind
	if tokenType == TK_UNION {
		operation = KUnion
	} else if tokenType == TK_EXCEPT {
		operation = KExcept
	} else {
		operation = KIntersect
	}

	comments := p.prev.Comments

	var distinct any // bool | None
	if p.match(TK_DISTINCT) {
		distinct = true
	} else if p.match(TK_ALL) {
		distinct = false
	} else {
		switch p.d.S.SET_OP_DISTINCT_BY_DEFAULT[operation] {
		case TriTrue:
			distinct = true
		case TriFalse:
			distinct = false
		default:
			distinct = nil
		}
		if distinct == nil {
			p.raiseError("Expected DISTINCT or ALL for "+operation.Name(), nil)
		}
	}

	var byName any // True | None
	if p.matchTextSeq("BY", "NAME") || p.matchTextSeq("STRICT", "CORRESPONDING") {
		byName = true
	}
	if p.matchTextSeq("CORRESPONDING") {
		byName = true
		if !truthy(side) && !truthy(kind) {
			kind = "INNER"
		}
	}

	var onColumnList any
	if truthy(byName) && p.matchTexts("ON", "BY") {
		onColumnList = p.parseWrappedCSV(p.parseColumn, TK_COMMA, false)
	}

	expression := p.parseSelect(true, false, true, false, consumePipe, nil)

	// Wrap VALUES operands in selects, both for consistency with the CTE canonicalization
	// in _parse_cte and so that alias pushdown can reach into set operation branches
	if this.IsA(KValues) {
		this = p.valuesToSelect(this)
	}
	if expression.IsA(KValues) {
		expression = p.valuesToSelect(expression)
	}

	return p.expressionC(New(
		operation,
		"this", this,
		"distinct", distinct,
		"by_name", byName,
		"expression", expression,
		"side", side,
		"kind", kind,
		"on", onColumnList,
	), comments)
}

// _parse_set_operations (parser.py L5810)
func (p *Parser) parseSetOperations(this *Expr) *Expr {
	for this != nil {
		setop := p.parseSetOperation(this, false)
		if setop == nil {
			break
		}
		this = setop
	}

	if this.IsA(KSetOperation) && p.s.MODIFIERS_ATTACHED_TO_SET_OP {
		expression := this.Expression()

		if expression != nil {
			// SET_OP_MODIFIERS is a Python set (unordered); iterate deterministically.
			for _, arg := range p.s.SET_OP_MODIFIERS.Sorted() {
				expr := expression.ArgE(arg)
				if expr != nil {
					this.Set(arg, expr.Pop())
				}
			}
		}
	}

	return this
}
