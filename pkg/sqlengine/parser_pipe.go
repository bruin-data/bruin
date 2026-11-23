package sqlengine

import (
	"fmt"
	"math/big"
)

// buildPipeCte wraps query in a CTE (named by aliasCte, or a fresh __tmpN) and returns a SELECT
// of expressions from it that carries over the query's existing CTEs.
func (p *Parser) buildPipeCte(query *Expr, expressions []*Expr, aliasCte *Expr) *Expr {
	// The CTE is named by the given TableAlias or by a generated "__tmpN", which is parsed (with
	// the default dialect) into the FROM and alias nodes below.
	newCteName := ""
	if aliasCte == nil {
		p.pipeCteCounter++
		newCteName = fmt.Sprintf("__tmp%d", p.pipeCteCounter)
	}

	var ctes *Expr
	if with := query.ArgE("with_"); with != nil {
		ctes = with.Pop()
	}

	fromArg := aliasCte
	if fromArg == nil {
		fromArg = MaybeParse(newCteName, KFrom, "FROM", nil)
	}
	newSelect := New(KSelect).QuerySelect(expressions, true, false).SelectFrom(fromArg, false)
	if ctes != nil {
		newSelect.Set("with_", ctes)
	}

	aliasArg := aliasCte
	if aliasArg == nil {
		aliasArg = MaybeParse(newCteName, KTableAlias, "", nil)
	}
	return newSelect.QueryWith(aliasArg, query, nil, nil, true, false, nil)
}

func (p *Parser) parsePipeSyntaxSelect(query *Expr) *Expr {
	sel := p.parseSelect(false, false, true, true, false, nil)
	if sel == nil {
		return query
	}

	return p.buildPipeCte(query.QuerySelect(sel.Expressions(), false, true), []*Expr{Star()}, nil)
}

// parsePipeSyntaxLimit parses |> LIMIT [OFFSET]: the smaller of the current and new limits wins,
// and offsets add up.
func (p *Parser) parsePipeSyntaxLimit(query *Expr) *Expr {
	limit := p.parseLimit(nil, false, false)
	offset := p.parseOffset(nil)
	if limit.IsA(KLimit) {
		// the current limit, or the new one when the query has none
		currLimit := limit
		if query.HasArgKey("limit") {
			currLimit = query.ArgE("limit")
		}
		currIsLimitAll := currLimit.isLimitAll()

		// LIMIT ALL means "no cap", so it's never tighter than a numeric limit.
		var replace bool
		if limit.isLimitAll() {
			replace = currIsLimitAll
		} else {
			replace = currIsLimitAll || chunkFToPy(currLimit.Expression()).cmp(chunkFToPy(limit.Expression())) >= 0
		}
		if replace {
			query.QueryLimit(limit, false)
		}
	}
	if offset != nil {
		currOffset := chunkFPyNum{isInt: true, i: big.NewInt(0)}
		if co := query.ArgE("offset"); co != nil {
			currOffset = chunkFToPy(co.Expression())
		}
		query.QueryOffset(LiteralNumber(currOffset.add(chunkFToPy(offset.Expression())).String()), false)
	}

	return query
}

func (p *Parser) parsePipeSyntaxAggregateFields() *Expr {
	this := p.parseDisjunction()
	if p.matchTextSeqNoAdvance("GROUP", "AND") {
		return this
	}

	this = p.parseAlias(this, false)

	if p.matchAnyNoAdvance(TK_ASC, TK_DESC) {
		return p.parseOrdered(func() *Expr { return this })
	}

	return this
}

// parsePipeSyntaxAggregateGroupOrderBy parses the fields of AGGREGATE (or of GROUP BY, with
// groupByExists), adding them to the projections (and grouping keys); ordered fields also go to
// ORDER BY.
func (p *Parser) parsePipeSyntaxAggregateGroupOrderBy(query *Expr, groupByExists bool) *Expr {
	expr := p.parseCSV(p.parsePipeSyntaxAggregateFields, TK_COMMA)
	aggregatesOrGroups, orders := []*Expr{}, []*Expr{}
	for _, element := range expr {
		var this *Expr
		if element.IsA(KOrdered) {
			this = element.This()
			if this.IsA(KAlias) {
				element.Set("this", this.Arg("alias"))
			}
			orders = append(orders, element)
		} else {
			this = element
		}
		aggregatesOrGroups = append(aggregatesOrGroups, this)
	}

	if groupByExists {
		projections := append(append([]*Expr{}, aggregatesOrGroups...), query.Expressions()...)
		selected := query.QuerySelect(projections, false, false)
		groups := make([]*Expr, 0, len(aggregatesOrGroups))
		for _, projection := range aggregatesOrGroups {
			if projection.HasArgKey("alias") {
				groups = append(groups, projection.ArgE("alias"))
			} else {
				groups = append(groups, projection)
			}
		}
		selected.SelectGroupBy(groups, true, false)
	} else {
		query.QuerySelect(aggregatesOrGroups, false, false)
	}

	if len(orders) > 0 {
		return query.QueryOrderBy(orders, false, false)
	}

	return query
}

// parsePipeSyntaxAggregate parses |> AGGREGATE ... [GROUP [AND ORDER] BY ...] and wraps the
// result in a new CTE.
func (p *Parser) parsePipeSyntaxAggregate(query *Expr) *Expr {
	p.matchTextSeq("AGGREGATE")
	query = p.parsePipeSyntaxAggregateGroupOrderBy(query, false)

	if p.match(TK_GROUP_BY) || (p.matchTextSeq("GROUP", "AND") && p.match(TK_ORDER_BY)) {
		query = p.parsePipeSyntaxAggregateGroupOrderBy(query, true)
	}

	return p.buildPipeCte(query, []*Expr{Star()}, nil)
}

// parsePipeSyntaxSetOperator parses |> UNION/EXCEPT/INTERSECT with one or more parenthesized
// queries; it returns nil when no set operation follows.
func (p *Parser) parsePipeSyntaxSetOperator(query *Expr) *Expr {
	firstSetop := p.parseSetOperation(query, false, false)
	if firstSetop == nil {
		return nil
	}

	parseAndUnwrapQuery := func() *Expr {
		expr := p.parseParen()
		if expr != nil {
			return chunkFAssertIs(expr, KSubquery).UnnestSubquery()
		}
		return nil
	}

	firstSetop.This().Pop()

	setops := []*Expr{chunkFAssertIs(firstSetop.Expression().Pop(), KSubquery).UnnestSubquery()}
	setops = append(setops, p.parseCSV(parseAndUnwrapQuery, TK_COMMA)...)

	query = p.buildPipeCte(query, []*Expr{Star()}, nil)
	var ctes *Expr
	if with := query.ArgE("with_"); with != nil {
		ctes = with.Pop()
	}

	// Build the set operation over query and setops: the parsed operation's distinct arg is
	// passed on its own, and its remaining args are copied onto every set operation node.
	var distinct any = true
	var opts []any
	for _, a := range firstSetop.args {
		if a.key == "distinct" {
			distinct = a.val
			continue
		}
		opts = append(opts, a.key, a.val)
	}
	operands := append([]*Expr{query}, setops...)

	if firstSetop.IsA(KUnion) {
		query = applySetOperation(operands, KUnion, distinct, false, opts...)
	} else if firstSetop.IsA(KExcept) {
		query = applySetOperation(operands, KExcept, distinct, false, opts...)
	} else {
		query = applySetOperation(operands, KIntersect, distinct, false, opts...)
	}

	query.Set("with_", ctes)

	return p.buildPipeCte(query, []*Expr{Star()}, nil)
}

func (p *Parser) parsePipeSyntaxJoin(query *Expr) *Expr {
	join := p.parseJoin(false, false, nil)
	if join == nil {
		return nil
	}

	if query.IsA(KSelect) {
		return query.SelectJoin(join, nil, nil, true, "", nil, false)
	}

	return query
}

func (p *Parser) parsePipeSyntaxPivot(query *Expr) *Expr {
	pivots := p.parsePivots()
	if len(pivots) == 0 {
		return query
	}

	if from := query.ArgE("from_"); from != nil {
		from.This().Set("pivots", pivots)
	}

	return p.buildPipeCte(query, []*Expr{Star()}, nil)
}

func (p *Parser) parsePipeSyntaxExtend(query *Expr) *Expr {
	p.matchTextSeq("EXTEND")
	query.QuerySelect(append([]*Expr{Star()}, p.parseExpressions()...), false, false)
	return p.buildPipeCte(query, []*Expr{Star()}, nil)
}

func (p *Parser) parsePipeSyntaxTablesample(query *Expr) *Expr {
	sample := p.parseTableSample(false)

	if with := query.ArgE("with_"); with != nil {
		exprs := with.Expressions()
		exprs[len(exprs)-1].This().Set("sample", sample)
	} else {
		query.Set("sample", sample)
	}

	return query
}

// parsePipeSyntaxQuery turns query into a SELECT * FROM ... when needed and applies each |>
// operator in turn; an unsupported operator raises a parse error.
func (p *Parser) parsePipeSyntaxQuery(query *Expr) *Expr {
	if query.IsA(KSubquery) {
		query = SelectExpr(MaybeParse("*", KExpr, "", nil)).SelectFrom(query, false)
	}

	if query.ArgE("from_") == nil {
		query = SelectExpr(MaybeParse("*", KExpr, "", nil)).SelectFrom(query.QuerySubquery(nil, false), false)
	}

	for p.match(TK_PIPE_GT) {
		startIndex := p.index
		startText := upperText(p.curr)
		parser := p.s.PIPE_SYNTAX_TRANSFORM_PARSERS[startText]
		if parser == nil {
			// The set operators (UNION, etc) and the JOIN operator have a few common starting
			// keywords, making it tricky to disambiguate them without lookahead. The approach
			// here is to try and parse a set operation and if that fails, then try to parse a
			// join operator. If that fails as well, then the operator is not supported.
			parsedQuery := p.parsePipeSyntaxSetOperator(query)
			if parsedQuery == nil {
				parsedQuery = p.parsePipeSyntaxJoin(query)
			}
			if parsedQuery == nil {
				p.retreat(startIndex)
				p.raiseError(fmt.Sprintf("Unsupported pipe syntax operator: '%s'.", startText), nil)
				break
			}
			query = parsedQuery
		} else {
			query = parser(p, query)
		}
	}

	return query
}

// chunkFPyNum is the value of a numeric literal: an exact integer, or a decimal held as a
// big.Float.
type chunkFPyNum struct {
	isInt bool
	i     *big.Int
	f     *big.Float
}

// chunkFToPy returns the value of a numeric literal (pipe-syntax LIMIT and OFFSET values). Any
// other expression, including a string literal, panics with a ValueError.
func chunkFToPy(e *Expr) chunkFPyNum {
	if e.IsNumber() {
		f, isInt := e.toPyNumber()
		if f != nil {
			if isInt {
				i, _ := f.Int(nil)
				return chunkFPyNum{isInt: true, i: i}
			}
			return chunkFPyNum{f: f}
		}
	}
	panic(&ValueError{Msg: chunkFPyStr(e) + " is not a numeric literal."})
}

func (n chunkFPyNum) float() *big.Float {
	if n.isInt {
		return new(big.Float).SetPrec(200).SetInt(n.i)
	}
	return n.f
}

func (n chunkFPyNum) cmp(o chunkFPyNum) int {
	if n.isInt && o.isInt {
		return n.i.Cmp(o.i)
	}
	return n.float().Cmp(o.float())
}

func (n chunkFPyNum) add(o chunkFPyNum) chunkFPyNum {
	if n.isInt && o.isInt {
		return chunkFPyNum{isInt: true, i: new(big.Int).Add(n.i, o.i)}
	}
	return chunkFPyNum{f: new(big.Float).SetPrec(200).Add(n.float(), o.float())}
}

// String formats the number. Decimals use the shortest plain notation, so trailing zeros of the
// operands are not kept (1.0 + 1 gives "2", not "2.0").
func (n chunkFPyNum) String() string {
	if n.isInt {
		return n.i.String()
	}
	return n.f.Text('f', -1)
}
