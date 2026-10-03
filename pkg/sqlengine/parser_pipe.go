package sqlengine

import (
	"fmt"
	"math/big"
)

// Port of sqlglot/parser.py (chunk F, part 2): the BigQuery-style pipe syntax (`|>`).

// _build_pipe_cte (parser.py L9894).
func (p *Parser) buildPipeCte(query *Expr, expressions []*Expr, aliasCte *Expr) *Expr {
	// new_cte is either the given TableAlias or the string "__tmpN"; sqlglot's builders run
	// strings through maybe_parse, so the string form is parsed here (default dialect).
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

// _parse_pipe_syntax_select (parser.py L9916).
func (p *Parser) parsePipeSyntaxSelect(query *Expr) *Expr {
	sel := p.parseSelect(false, false, true, true, false, nil)
	if sel == nil {
		return query
	}

	return p.buildPipeCte(query.QuerySelect(sel.Expressions(), false, true), []*Expr{Star()}, nil)
}

// _parse_pipe_syntax_limit (parser.py L9925).
func (p *Parser) parsePipeSyntaxLimit(query *Expr) *Expr {
	limit := p.parseLimit(nil, false, false)
	offset := p.parseOffset(nil)
	if limit != nil {
		// query.args.get("limit", limit)
		currLimit := limit
		if query.HasArgKey("limit") {
			currLimit = query.ArgE("limit")
		}
		if chunkFToPy(currLimit.Expression()).cmp(chunkFToPy(limit.Expression())) >= 0 {
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

// _parse_pipe_syntax_aggregate_fields (parser.py L9939).
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

// _parse_pipe_syntax_aggregate_group_order_by (parser.py L9951).
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
			// projection.args.get("alias", projection)
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

// _parse_pipe_syntax_aggregate (parser.py L9981).
func (p *Parser) parsePipeSyntaxAggregate(query *Expr) *Expr {
	p.matchTextSeq("AGGREGATE")
	query = p.parsePipeSyntaxAggregateGroupOrderBy(query, false)

	if p.match(TK_GROUP_BY) || (p.matchTextSeq("GROUP", "AND") && p.match(TK_ORDER_BY)) {
		query = p.parsePipeSyntaxAggregateGroupOrderBy(query, true)
	}

	return p.buildPipeCte(query, []*Expr{Star()}, nil)
}

// _parse_pipe_syntax_set_operator (parser.py L9992).
func (p *Parser) parsePipeSyntaxSetOperator(query *Expr) *Expr {
	firstSetop := p.parseSetOperation(query, false)
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

	// query.union(*setops, copy=False, **first_setop.args): `distinct` binds to the named
	// parameter, the remaining args become constructor kwargs of every set operation node.
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

// _parse_pipe_syntax_join (parser.py L10023).
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

// _parse_pipe_syntax_pivot (parser.py L10033).
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

// _parse_pipe_syntax_extend (parser.py L10044).
func (p *Parser) parsePipeSyntaxExtend(query *Expr) *Expr {
	p.matchTextSeq("EXTEND")
	query.QuerySelect(append([]*Expr{Star()}, p.parseExpressions()...), false, false)
	return p.buildPipeCte(query, []*Expr{Star()}, nil)
}

// _parse_pipe_syntax_tablesample (parser.py L10049).
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

// _parse_pipe_syntax_query (parser.py L10060).
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

// ---------------------------------------------------------------------------------------------
// Literal.to_py arithmetic used by _parse_pipe_syntax_limit
// ---------------------------------------------------------------------------------------------

// chunkFPyNum is a Python int or Decimal produced by Literal.to_py().
type chunkFPyNum struct {
	isInt bool
	i     *big.Int
	f     *big.Float
}

// chunkFToPy mirrors expression.to_py() for numeric literals (Literal / Neg); any other value
// raises like sqlglot's Expr.to_py (comparisons with strings are reported the same way).
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
	panic(&ValueError{Msg: chunkFPyStr(e) + " cannot be converted to a Python object."})
}

func (n chunkFPyNum) float() *big.Float {
	if n.isInt {
		return new(big.Float).SetPrec(200).SetInt(n.i)
	}
	return n.f
}

// cmp compares two numbers like Python's int/Decimal comparison operators.
func (n chunkFPyNum) cmp(o chunkFPyNum) int {
	if n.isInt && o.isInt {
		return n.i.Cmp(o.i)
	}
	return n.float().Cmp(o.float())
}

// add mirrors int + int (int) or arithmetic involving a Decimal (Decimal).
func (n chunkFPyNum) add(o chunkFPyNum) chunkFPyNum {
	if n.isInt && o.isInt {
		return chunkFPyNum{isInt: true, i: new(big.Int).Add(n.i, o.i)}
	}
	return chunkFPyNum{f: new(big.Float).SetPrec(200).Add(n.float(), o.float())}
}

// String mirrors str(number). Decimal formatting is best effort (Python keeps the exponent of
// the operands, e.g. Decimal("1.0") + 1 -> "2.0").
func (n chunkFPyNum) String() string {
	if n.isInt {
		return n.i.String()
	}
	return n.f.Text('f', -1)
}
