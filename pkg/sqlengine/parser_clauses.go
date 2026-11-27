package sqlengine

import "errors"

func (p *Parser) parsePrewhere(skipWhereToken bool) *Expr {
	if !skipWhereToken && !p.match(TK_PREWHERE) {
		return nil
	}

	comments := p.prevComments
	return p.expressionC(New(KPreWhere, "this", p.parseDisjunction()), comments)
}

func (p *Parser) parseWhere(skipWhereToken bool) *Expr {
	if !skipWhereToken && !p.match(TK_WHERE) {
		return nil
	}

	comments := p.prevComments
	return p.expressionC(New(KWhere, "this", p.parseDisjunction()), comments)
}

// parseGroup parses GROUP BY [ALL | DISTINCT] with expressions, CUBE, ROLLUP, GROUPING SETS and
// WITH TOTALS. CUBE, ROLLUP and GROUPING SETS written as grouping elements stay among the
// expressions, in order; WITH CUBE / WITH ROLLUP and GROUPING SETS following the expressions
// without a comma (GROUP BY a, b GROUPING SETS (...)) are kept apart.
func (p *Parser) parseGroup(skipGroupByToken bool) *Expr {
	if !skipGroupByToken && !p.match(TK_GROUP_BY) {
		return nil
	}
	comments := p.prevComments

	// Keys are recorded in first-use order.
	elements := &chunkCKwargs{}

	if p.match(TK_ALL) {
		elements.set("all", true)
	} else if p.match(TK_DISTINCT) {
		elements.set("all", false)
	}

	for {
		// Stop before consuming modifier tokens like LIMIT, OFFSET and WINDOW, which are also
		// valid identifiers
		if p.matchSetNoAdvance(p.s.QUERY_MODIFIER_TOKENS) {
			break
		}

		elements.extendList("expressions", p.parseCSV(func() *Expr {
			if e := p.parseGroupingSets(); e != nil {
				return e
			}
			if e := p.parseCubeOrRollup(false); e != nil {
				return e
			}
			return p.parseDisjunction()
		}, TK_COMMA))

		beforeWithIndex := p.index

		var cubeOrRollup *Expr
		if p.match(TK_WITH) {
			cubeOrRollup = p.parseCubeOrRollup(true)
		}

		if cubeOrRollup != nil {
			key := "cube"
			if cubeOrRollup.IsA(KRollup) {
				key = "rollup"
			}
			elements.appendList(key, cubeOrRollup)
		} else if groupingSets := p.parseGroupingSets(); groupingSets != nil {
			// Hive-style suffix syntax: GROUP BY a, b GROUPING SETS (...)
			elements.appendList("grouping_sets", groupingSets)
			break
		} else if p.matchTextSeq("TOTALS") {
			elements.set("totals", true)
		}

		if beforeWithIndex <= p.index && p.index <= beforeWithIndex+1 {
			p.retreat(beforeWithIndex)
			break
		}
	}

	return p.expressionC(New(KGroup, elements.kv()...), comments)
}

// parseCubeOrRollup parses CUBE (...) or ROLLUP (...); the WITH CUBE / WITH ROLLUP form
// (withPrefix) has no column list.
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

func (p *Parser) parseGroupingSets() *Expr {
	if p.match(TK_GROUPING_SETS) {
		return p.expression(New(
			KGroupingSets,
			"expressions", p.parseWrappedCSV(p.parseGroupingSet, TK_COMMA, false),
		))
	}
	return nil
}

func (p *Parser) parseGroupingSet() *Expr {
	if e := p.parseGroupingSets(); e != nil {
		return e
	}
	if e := p.parseCubeOrRollup(false); e != nil {
		return e
	}
	return p.parseBitwise()
}

func (p *Parser) parseHaving(skipHavingToken bool) *Expr {
	if !skipHavingToken && !p.match(TK_HAVING) {
		return nil
	}
	comments := p.prevComments
	return p.expressionC(New(KHaving, "this", p.parseDisjunction()), comments)
}

func (p *Parser) parseQualify() *Expr {
	if !p.match(TK_QUALIFY) {
		return nil
	}
	return p.expression(New(KQualify, "this", p.parseDisjunction()))
}

// baseParseConnectWithPrior parses a CONNECT BY condition in which PRIOR x is accepted. PRIOR is
// registered in a per-parser overlay rather than in the shared NO_PAREN_FUNCTION_PARSERS table,
// so concurrent parsers don't race.
func (p *Parser) baseParseConnectWithPrior() *Expr {
	p.setNoParenOverlay("PRIOR", func(p *Parser) *Expr {
		return p.expression(New(KPrior, "this", p.parseBitwise()))
	})
	connect := p.parseDisjunction()
	// A nested CONNECT BY may already have removed the entry; that is an error (the message keeps
	// its historical wording).
	if !p.popNoParenOverlay("PRIOR") {
		panic(errors.New("KeyError: 'PRIOR'"))
	}
	return connect
}

// parseConnect parses [START WITH ...] CONNECT BY [NOCYCLE] ... [START WITH ...].
func (p *Parser) parseConnect(skipStartToken bool) *Expr {
	var start *Expr
	if skipStartToken {
		start = nil
	} else if p.matchTextSeq("START", "WITH") {
		start = p.parseDisjunction()
	} else {
		return nil
	}

	p.match(TK_CONNECT_BY)
	nocycle := p.matchTextSeq("NOCYCLE")
	connect := p.parseConnectWithPrior()

	if start == nil && p.matchTextSeq("START", "WITH") {
		start = p.parseDisjunction()
	}

	return p.expression(New(KConnect, "start", start, "connect", connect, "nocycle", nocycle))
}

func (p *Parser) parseNameAsExpression() *Expr {
	this := p.parseIdVar(true, nil)
	if p.match(TK_ALIAS) {
		this = p.expression(New(KAlias, "alias", this, "this", p.parseDisjunction()))
	}
	return this
}

func (p *Parser) parseInterpolate() []*Expr {
	if p.matchTextSeq("INTERPOLATE") {
		return p.parseWrappedCSV(p.parseNameAsExpression, TK_COMMA, false)
	}
	return nil
}

func (p *Parser) parseOrder(this *Expr, skipOrderToken bool) *Expr {
	var siblings any // true or nil
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

func (p *Parser) parseSort(expClass Kind, token TokenType) *Expr {
	if !p.match(token) {
		return nil
	}
	return p.expression(New(expClass, "expressions", p.parseCSV(func() *Expr { return p.parseOrdered(nil) }, TK_COMMA)))
}

// parseOrdered parses one ORDER BY item with ASC/DESC, NULLS FIRST/LAST and WITH FILL. Without an
// explicit NULLS FIRST/LAST, nulls_first follows the dialect's NULL_ORDERING.
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
	var desc any // bool or nil
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

	return p.expression(New(KOrdered, "this", this, "desc", desc, "nulls_first", nullsFirst, "with_fill", p.parseWithFill()))
}

func (p *Parser) parseWithFill() *Expr {
	if !p.matchTextSeq("WITH", "FILL") {
		return nil
	}

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
	return p.expression(New(
		KWithFill,
		"from_", from,
		"to", to,
		"step", step,
		"interpolate", chunkCOptList(p.parseInterpolate()),
	))
}

var rowOrRowsTexts = newStrSet("ROW", "ROWS")

func (p *Parser) parseLimitOptions() *Expr {
	percent := p.matchAny(TK_PERCENT, TK_MOD)
	rows := p.matchTexts("ROW", "ROWS")
	p.matchTextSeq("ONLY")
	withTies := p.matchTextSeq("WITH", "TIES")

	if !(percent || rows || withTies) {
		return nil
	}

	return p.expression(New(KLimitOptions, "percent", percent, "rows", rows, "with_ties", withTies))
}

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
				expression = VarExpr("ALL")
			} else {
				// Parsing LIMIT x% (i.e x PERCENT) as a term leads to an error, since
				// we try to build a Mod expression. For that matter, we backtrack and instead
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

		if top {
			limitExp.Meta()["top"] = true
		}

		return limitExp
	}

	if p.match(TK_FETCH) {
		direction := "FIRST"
		if p.matchAny(TK_FIRST, TK_NEXT) {
			direction = upperText(p.prev)
		}

		// FETCH FIRST ROW ONLY has no count
		var count *Expr
		if !p.matchTextSetNoAdvance(rowOrRowsTexts) {
			count = p.parseField(false, &p.s.FETCH_TOKENS, false)
		}

		return p.expression(New(
			KFetch,
			"direction", direction,
			"count", count,
			"limit_options", p.parseLimitOptions(),
		))
	}

	return this
}

func (p *Parser) parseOffset(this *Expr) *Expr {
	if !p.match(TK_OFFSET) {
		return this
	}

	count := p.parseTerm()
	p.matchTexts("ROW", "ROWS")

	return p.expression(New(
		KOffset,
		"this", this,
		"expression", count,
		"expressions", chunkCOptList(p.parseLimitBy()),
	))
}

// canParseLimitOrOffset reports whether an ambiguous alias token at the cursor actually starts a
// LIMIT or OFFSET clause.
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

func (p *Parser) parseLimitBy() []*Expr {
	if p.matchTextSeq("BY") {
		return p.parseCSV(p.parseBitwise, TK_COMMA)
	}
	return nil
}

func (p *Parser) parseLocks() []*Expr {
	locks := []*Expr{}
	for {
		var update, key any // bool or nil
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

		var wait any // bool, *Expr or nil
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

// parseSetOperation parses one UNION, EXCEPT or INTERSECT (only INTERSECT when intersectOnly is
// set) with its right operand; nil (with the cursor restored) when none follows.
func (p *Parser) parseSetOperation(this *Expr, consumePipe bool, intersectOnly bool) *Expr {
	start := p.index
	_, sideToken, kindToken := p.parseJoinParts()

	var side, kind any // string or nil
	if sideToken.ok() {
		side = sideToken.Text
	}
	if kindToken.ok() {
		kind = kindToken.Text
	}

	if !p.matchSet(p.s.SET_OPERATIONS) || (intersectOnly && p.prev.Type != TK_INTERSECT) {
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

	var distinct any // bool or nil
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

	var byName any // true or nil
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
	// in parseCte and so that alias pushdown can reach into set operation branches
	if this.IsA(KValues) {
		this = p.valuesToSelect(this)
	}
	if expression.IsA(KValues) {
		expression = p.valuesToSelect(expression)
	}
	// and likewise inside parenthesized operands
	for _, operand := range []*Expr{this, expression} {
		if operand.IsA(KSubquery) {
			if values := operand.Unnest(); values.IsA(KValues) {
				values.Parent().Set("this", p.valuesToSelect(values))
			}
		}
	}

	// An alias parsed after a parenthesized left operand belongs to the subquery
	if this.IsA(KAlias) && this.This().IsA(KSubquery) {
		subquery := this.This()
		subquery.Set("alias", New(KTableAlias, "this", this.Arg("alias")))
		subquery.AddComments(this.PopComments(), false)
		this = subquery
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

// parseSetOperations parses a chain of set operations on this. Where the dialect has INTERSECT
// bind tighter than UNION and EXCEPT, the INTERSECTs that follow a UNION or EXCEPT are grouped
// with its right operand (a UNION b INTERSECT c -> a UNION (b INTERSECT c)); otherwise the chain
// associates left to right, and a UNION or EXCEPT left of an INTERSECT is parenthesized. When the
// dialect attaches modifiers to set operations, the trailing modifiers move from the last operand
// to the set operation, and modifiers following a parenthesized last operand are parsed onto it.
func (p *Parser) parseSetOperations(this *Expr) *Expr {
	intersectBindsTighter := p.d.S.INTERSECT_BINDS_TIGHTER_THAN_UNION_AND_EXCEPT
	var lastSetop *Expr

	for this != nil {
		setop := p.parseSetOperation(this, false, false)
		if setop == nil {
			break
		}

		lastSetop = setop
		if setop.IsA(KIntersect) && setop.This().IsA(KUnion, KExcept) && !intersectBindsTighter {
			setop.Set("this", setop.This().QuerySubquery(nil, false))
		} else if !setop.IsA(KIntersect) && intersectBindsTighter {
			operand := setop.Expression()
			for {
				intersect := p.parseSetOperation(operand, false, true)
				if intersect == nil {
					break
				}
				lastSetop, operand = intersect, intersect
			}
			if operand != setop.Expression() {
				setop.Set("expression", operand.QuerySubquery(nil, false))
			}
		}

		this = setop
	}

	if this.IsA(KSetOperation) && p.s.MODIFIERS_ATTACHED_TO_SET_OP {
		var expression *Expr
		if lastSetop != nil {
			expression = lastSetop.Expression()
		}

		if expression != nil {
			// SET_OP_MODIFIERS is a set; iterate it in sorted order so the result is deterministic.
			for _, arg := range p.s.SET_OP_MODIFIERS.Sorted() {
				value := expression.Arg(arg)
				// a TOP stays on its operand
				if truthy(value) && !(arg == "limit" && isTopLimit(expression.ArgE(arg))) {
					expression.Set(arg, nil)
					this.Set(arg, value)
				}
			}
		}

		// Modifiers after a parenthesized final operand (SELECT 1 UNION (SELECT 2) ORDER BY 1), or a
		// LIMIT or FETCH that a final operand with TOP leaves for the whole result
		if _, ok := p.s.QUERY_MODIFIER_PARSERS[p.curr.Type]; ok {
			this = p.parseQueryModifiers(this)
		}
	}

	return this
}
