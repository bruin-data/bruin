package sqlengine

import "strings"

// Port of sqlglot.parser.Parser (chunk D, part 2): atoms, columns, JSON/variant extraction,
// column operators, parens, primaries, fields, function calls, UDFs, lambdas and schemas.

// SQLGLOT_ANONYMOUS mirrors exp.SQLGLOT_ANONYMOUS.
const chunkDSQLGlotAnonymous = "sqlglot.anonymous"

// _parse_atom (parser.py L6607)
func (p *Parser) parseAtom() *Expr {
	if p.s.IDENTIFIER_TOKENS.Has(p.curr.Type) {
		if column := p.parseColumn(); column != nil {
			return column
		}
	}

	token := p.curr
	tokenType := token.Type

	primaryParser, ok := p.s.PRIMARY_PARSERS[tokenType]
	if !ok || primaryParser == nil {
		return nil
	}

	nextType := p.next.Type

	if _, isOp := p.s.COLUMN_OPERATORS[nextType]; isOp ||
		p.s.COLUMN_POSTFIX_TOKENS.Has(nextType) ||
		(tokenType == TK_STRING && nextType == TK_STRING) {
		return nil
	}

	p.advance(1)
	return primaryParser(p, token)
}

// _parse_column (parser.py L6632)
func (p *Parser) baseParseColumn() *Expr {
	column := p.parseColumnPartsFast()
	if column == nil {
		this := p.parseColumnReference()
		if this == nil {
			this = p.parseBracket(this)
		}
		if this != nil {
			column = p.parseColumnOps(this)
		} else {
			column = this
		}
	}

	if column != nil {
		if p.d.S.SUPPORTS_COLUMN_JOIN_MARKS {
			column.Set("join_mark", p.match(TK_JOIN_MARKER))
		}
		if p.s.COLON_IS_VARIANT_EXTRACT {
			column = p.parseColonAsVariantExtract(column)
		}
	}

	return column
}

// _parse_column_parts_fast (parser.py L6648)
//
// Fast path for simple column and dot references (a, a.b, ...).
//
// Greedily consumes VAR/IDENTIFIER tokens separated by DOTs, then checks
// that nothing complex follows. If it does, retreats and returns None so
// the slow path can handle it. For >4 parts, wraps in exp.Dot nodes.
func (p *Parser) parseColumnPartsFast() *Expr {
	index := p.index
	var parts []*Expr
	partsNone := true
	var allComments []string

	for p.matchSet(p.s.IDENTIFIER_TOKENS) {
		token := p.prev
		comments := p.prevComments

		if partsNone {
			if _, ok := p.noParenFunctionParser(upperText(token)); ok {
				p.retreat(index)
				return nil
			}
		}

		hasDot := p.match(TK_DOT)
		currTT := p.curr.Type

		if !hasDot {
			if _, isOp := p.s.COLUMN_OPERATORS[currTT]; isOp || p.s.COLUMN_POSTFIX_TOKENS.Has(currTT) {
				p.retreat(index)
				return nil
			}
		} else if !p.s.IDENTIFIER_TOKENS.Has(currTT) {
			p.retreat(index)
			return nil
		}

		if partsNone {
			parts = []*Expr{}
			partsNone = false
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

	if partsNone {
		return nil
	}

	n := len(parts)

	var column *Expr
	switch {
	case n == 1:
		column = New(KColumn, "this", parts[0])
	case n == 2:
		column = New(KColumn, "this", parts[1], "table", parts[0])
	case n == 3:
		column = New(KColumn, "this", parts[2], "table", parts[1], "db", parts[0])
	default:
		column = New(KColumn, "this", parts[3], "table", parts[2], "db", parts[1], "catalog", parts[0])

		for i := 4; i < n; i++ {
			column = New(KDot, "this", column, "expression", parts[i])
		}
	}

	if len(allComments) > 0 {
		column.AddComments(allComments, false)
	}

	return column
}

// _parse_column_reference (parser.py L6721)
func (p *Parser) parseColumnReference() *Expr {
	this := p.parseField(false, nil, false)
	if this == nil &&
		p.matchNoAdvance(TK_VALUES) &&
		p.s.VALUES_FOLLOWED_BY_PAREN &&
		(!p.next.ok() || p.next.Type != TK_L_PAREN) {
		this = p.parseIdVar(true, nil)
	}

	if this.IsA(KIdentifier) {
		// We bubble up comments from the Identifier to the Column
		column := New(KColumn, "this", this)
		this = p.expressionC(column, this.PopComments())
	}

	return this
}

// _build_json_extract (parser.py L6737)
func (p *Parser) buildJsonExtract(this *Expr, pathParts []*Expr) (*Expr, []*Expr) {
	if len(pathParts) > 1 {
		this = p.expression(New(
			KJSONExtract,
			"this", this,
			"expression", New(KJSONPath, "expressions", pathParts),
			"variant_extract", true,
			"requires_json", p.s.JSON_EXTRACT_REQUIRES_JSON_EXPRESSION,
		))
		pathParts = []*Expr{New(KJSONPathRoot)}
	}

	return this, pathParts
}

// _parse_colon_as_variant_extract (parser.py L6755)
func (p *Parser) parseColonAsVariantExtract(this *Expr) *Expr {
	pathParts := []*Expr{New(KJSONPathRoot)}
	selectTokens := tsPtr(newTokenSet(TK_SELECT))

	for p.match(TK_COLON) {
		if !p.s.COLON_CHAIN_IS_SINGLE_EXTRACT {
			this, pathParts = p.buildJsonExtract(this, pathParts)
		}

		key := p.parseIdVar(true, selectTokens)

		if key != nil {
			quoted := key.IsA(KIdentifier) && key.ArgB("quoted")
			pathParts = append(pathParts, New(KJSONPathKey, "this", key.Name(), "quoted", quoted))
		}

		for {
			if p.match(TK_DOT) {
				nextKey := p.parseIdVar(true, selectTokens)

				if nextKey != nil {
					quoted := nextKey.IsA(KIdentifier) && nextKey.ArgB("quoted")
					pathParts = append(pathParts, New(KJSONPathKey, "this", nextKey.Name(), "quoted", quoted))
				}
			} else if p.match(TK_L_BRACKET) {
				bracketExpr := p.parseBracketKeyValue(false)

				if !p.match(TK_R_BRACKET) {
					p.raiseError("Expected ]", nil)
				}

				if bracketExpr != nil {
					if bracketExpr.IsString() {
						pathParts = append(pathParts, New(KJSONPathKey, "this", bracketExpr.Name(), "quoted", true))
					} else if bracketExpr.IsStar() {
						pathParts = append(pathParts, New(KJSONPathSubscript, "this", New(KJSONPathWildcard)))
					} else if bracketExpr.IsNumber() {
						pathParts = append(pathParts, New(KJSONPathSubscript, "this", chunkDLiteralToPy(bracketExpr)))
					} else {
						this, pathParts = p.buildJsonExtract(this, pathParts)

						this = p.expression(New(
							KBracket,
							"this", this,
							"expressions", []*Expr{bracketExpr},
							"json_access", true,
						))
					}
				}
			} else if p.match(TK_DCOLON) {
				this, pathParts = p.buildJsonExtract(this, pathParts)

				castType := p.parseTypes(false, false, true, false)
				if castType != nil {
					this = p.expression(New(KCast, "this", this, "to", castType))
				} else {
					p.raiseError("Expected type after '::'", nil)
				}
			} else {
				break
			}
		}
	}

	this, _ = p.buildJsonExtract(this, pathParts)

	return this
}

// _parse_dcolon (parser.py L6812)
func (p *Parser) baseParseDcolon() *Expr {
	return p.parseTypes(false, false, true, false)
}

// _parse_column_ops (parser.py L6815)
func (p *Parser) baseParseColumnOps(this *Expr) *Expr {
	for p.s.BRACKETS.Has(p.curr.Type) {
		this = p.parseBracket(this)
	}

	columnOperators := p.s.COLUMN_OPERATORS
	castColumnOperators := p.s.CAST_COLUMN_OPERATORS
	for p.curr.ok() {
		opToken := p.curr.Type

		op, isOp := columnOperators[opToken]
		if !isOp {
			break
		}
		p.advance(1)

		var field *Expr
		if castColumnOperators.Has(opToken) {
			field = p.parseDcolon()
			if field == nil {
				p.raiseError("Expected type", nil)
			}
		} else if op != nil && p.curr.ok() {
			field = p.parseColumnReference()
			if field == nil {
				field = p.parseBitwise()
			}
			if field.IsA(KColumn) && p.matchNoAdvance(TK_DOT) {
				field = p.parseColumnOps(field)
			}
		} else {
			dot := p.isConnected() && p.prev.Type == TK_DOT
			field = p.parseField(true, nil, true)

			// In t.true, t.null we should produce an Identifier node
			if dot && field.IsA(KNull, KBoolean) {
				field = p.expressionC(New(KIdentifier, "this", p.prev.Text), field.Comments())
			}
		}

		// Function calls can be qualified, e.g., x.y.FOO()
		// This converts the final AST to a series of Dots leading to the function call
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#function_call_rules
		if field.IsA(KFunc, KWindow) && this != nil {
			this = this.Transform(func(n *Expr) *Expr {
				if n.IsA(KColumn) {
					return n.ToDot(false)
				}
				return n
			}, true)
		}

		if op != nil {
			this = op(p, this, field)
		} else if this.IsA(KColumn) && !this.ArgB("catalog") {
			this = p.expressionC(New(
				KColumn,
				"this", field,
				"table", this.Arg("this"),
				"db", this.Arg("table"),
				"catalog", this.Arg("db"),
			), this.Comments())
		} else if field.IsA(KWindow) {
			// Move the exp.Dot's to the window's function
			windowFunc := p.expression(New(KDot, "this", this, "expression", field.This()))
			field.Set("this", windowFunc)
			this = field
		} else {
			this = p.expression(New(KDot, "this", this, "expression", field))
		}

		if field != nil && len(field.Comments()) > 0 {
			this.AddComments(field.PopComments(), false)
		}

		this = p.parseBracket(this)
	}

	return this
}

// _parse_paren (parser.py L6883)
func (p *Parser) parseParen() *Expr {
	if !p.match(TK_L_PAREN) {
		return nil
	}

	comments := p.prevComments
	query := p.parseSelect(false, false, true, true, true, nil)

	var expressions []*Expr
	if query != nil {
		expressions = []*Expr{query}
	} else {
		expressions = p.parseExpressions()
	}

	this := seqGet(expressions, 0)

	if this == nil && p.matchNoAdvance(TK_R_PAREN) {
		this = p.expression(New(KTuple))
	} else if len(expressions) > 1 || p.prev.Type == TK_COMMA {
		this = p.expression(New(KTuple, "expressions", expressions))
	} else if this.IsA(KSelect, KSetOperation) {
		this = p.parseSubquery(this, false)
	} else if this.IsA(KSubquery, KValues) {
		this = p.parseSubquery(p.parseQueryModifiers(p.parseSetOperations(this)), false)
	} else {
		this = p.expression(New(KParen, "this", this))
	}

	if this != nil {
		this.AddComments(comments, false)
	}

	p.matchRParen(this)

	if this.IsA(KParen) && this.This().IsA(KAggFunc) {
		return p.parseWindow(this, false)
	}

	return this
}

// _parse_primary (parser.py L6921)
func (p *Parser) baseParsePrimary() *Expr {
	if chunkDMatchKey(p, p.s.PRIMARY_PARSERS) {
		tokenType := p.prev.Type
		primary := p.s.PRIMARY_PARSERS[tokenType](p, p.prev)

		if tokenType == TK_STRING {
			expressions := []*Expr{primary}
			for p.matchNoAdvance(TK_STRING) {
				if p.isConnected() && p.s.ADJACENT_STRINGS_CANNOT_BE_CONNECTED {
					p.raiseError("Adjacent string literals need to be separated by whitespace or comments", nil)
				}

				p.advance(1)
				expressions = append(expressions, LiteralString(p.prev.Text))
			}

			if len(expressions) > 1 {
				return p.expression(New(
					KConcat,
					"expressions", expressions,
					"coalesce", p.d.S.CONCAT_COALESCE,
				))
			}
		}

		return primary
	}

	if p.matchPair(TK_DOT, TK_NUMBER) {
		return LiteralNumber("0." + p.prev.Text)
	}

	return p.parseParen()
}

// _parse_field (parser.py L6949)
func (p *Parser) parseField(anyToken bool, tokens *TokenSet, anonymousFunc bool) *Expr {
	var field *Expr
	if anonymousFunc {
		field = p.parseFunction(nil, anonymousFunc, true, anyToken)
		if field == nil {
			field = p.parsePrimary()
		}
	} else {
		field = p.parsePrimary()
		if field == nil {
			field = p.parseFunction(nil, anonymousFunc, true, anyToken)
		}
	}
	if field != nil {
		return field
	}
	return p.parseIdVar(anyToken, tokens)
}

// _parse_function (parser.py L6966)
func (p *Parser) baseParseFunction(functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr {
	// This allows us to also parse {fn <function>} syntax (Snowflake, MySQL support this)
	// See: https://community.snowflake.com/s/article/SQL-Escape-Sequences
	fnSyntax := false
	if p.matchNoAdvance(TK_L_BRACE) && p.next.ok() && upperText(p.next) == "FN" {
		p.advance(2)
		fnSyntax = true
	}

	fn := p.parseFunctionCall(functions, anonymous, optionalParens, anyToken)

	if fnSyntax {
		p.match(TK_R_BRACE)
	}

	return fn
}

// _parse_function_args (parser.py L6996)
func (p *Parser) parseFunctionArgs(alias bool) []*Expr {
	return p.parseCSV(func() *Expr { return p.parseLambda(alias) }, TK_COMMA)
}

// _parse_function_call (parser.py L6999)
func (p *Parser) baseParseFunctionCall(functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr {
	if !p.curr.ok() {
		return nil
	}

	comments := p.curr.Comments
	prev := p.prev
	token := p.curr
	tokenType := p.curr.Type
	this := p.curr.Text
	upper := upperText(p.curr)

	afterDot := prev.Type == TK_DOT
	noParenParser, _ := p.noParenFunctionParser(upper)
	if optionalParens &&
		noParenParser != nil &&
		!p.s.INVALID_FUNC_NAME_TOKENS.Has(tokenType) &&
		!afterDot {
		p.advance(1)
		return p.parseWindow(noParenParser(p), false)
	}

	if p.next.Type != TK_L_PAREN {
		if optionalParens && !afterDot {
			if kind, ok := p.s.NO_PAREN_FUNCTIONS[tokenType]; ok {
				p.advance(1)
				return p.expression(New(kind))
			}
		}

		return nil
	}

	if anyToken {
		if p.s.RESERVED_TOKENS.Has(tokenType) {
			return nil
		}
	} else if !p.s.FUNC_TOKENS.Has(tokenType) {
		return nil
	}

	p.advance(2)

	// Note: Python reuses the `parser` variable here, so the final R_PAREN match below
	// depends on FUNCTION_PARSERS even when `anonymous` is set.
	parser := p.s.FUNCTION_PARSERS[upper]
	var result *Expr
	if parser != nil && !anonymous {
		result = parser(p)
	} else {
		if subqueryPredicate, ok := p.s.SUBQUERY_PREDICATES[tokenType]; ok {
			var expr *Expr
			if p.s.SUBQUERY_TOKENS.Has(p.curr.Type) {
				expr = p.parseSelect(false, false, true, true, true, nil)
				p.matchRParen(nil)
			} else if prev.ok() && (prev.Type == TK_LIKE || prev.Type == TK_ILIKE) {
				// Backtrack one token since we've consumed the L_PAREN here. Instead, we'd like
				// to parse "LIKE [ANY | ALL] (...)" as a whole into an exp.Tuple or exp.Paren
				p.advance(-1)
				expr = p.parseBitwise()
			}

			if expr != nil {
				return p.expressionC(New(subqueryPredicate, "this", expr), comments)
			}
		}

		if functions == nil {
			functions = p.s.FUNCTIONS
		}

		function := functions[upper]
		knownFunction := function != nil && !anonymous

		alias := !knownFunction || p.s.FUNCTIONS_WITH_ALIASED_ARGS.Has(upper)
		args := p.parseFunctionArgs(alias)

		var postFuncComments []string
		if p.curr.ok() {
			postFuncComments = p.curr.Comments
		}
		if knownFunction && len(postFuncComments) > 0 {
			// If the user-inputted comment "/* sqlglot.anonymous */" is following the function
			// call we'll construct it as exp.Anonymous, even if it's "known"
			for _, comment := range postFuncComments {
				if strings.HasPrefix(strings.TrimLeftFunc(comment, pyIsSpaceRune), chunkDSQLGlotAnonymous) {
					knownFunction = false
					break
				}
			}
		}

		if alias && knownFunction {
			args = p.kvToPropEq(args, false)
		}

		if knownFunction {
			fn, validateArgs := callFuncBuilder(function, args, p.d)

			fn = p.validateExpression(fn, validateArgs)
			if p.d.S.PRESERVE_ORIGINAL_NAMES {
				fn.Meta()["name"] = this
			}

			result = fn
		} else {
			var anonThis any = this
			if tokenType == TK_IDENTIFIER {
				ident := New(KIdentifier, "this", this, "quoted", true)
				ident.updatePositionsTok(token)
				anonThis = ident
			}

			result = p.expression(New(KAnonymous, "this", anonThis, "expressions", args))
		}

		result.updatePositionsTok(token)
	}

	if result != nil {
		result.AddComments(comments, false)
	}

	if parser != nil {
		p.matchExpr(TK_R_PAREN, result)
	} else {
		p.matchRParen(result)
	}
	return p.parseWindow(result, false)
}

// _to_prop_eq (parser.py L7116)
func (p *Parser) baseToPropEq(expression *Expr, index int) *Expr {
	return expression
}

// _kv_to_prop_eq (parser.py L7119)
func (p *Parser) kvToPropEq(expressions []*Expr, parseMap bool) []*Expr {
	transformed := []*Expr{}

	for index, e := range expressions {
		if e.IsA(p.s.KEY_VALUE_DEFINITIONS...) {
			if e.IsA(KAlias) {
				e = p.expression(New(KPropertyEQ, "this", e.Arg("alias"), "expression", e.This()))
			}

			if !e.IsA(KPropertyEQ) {
				var this *Expr
				if parseMap {
					this = e.This()
				} else {
					if e.This() == nil {
						// e.this.name raises AttributeError when `this` is missing (e.g. `{: 1}`)
						panic(&ValueError{Msg: "'NoneType' object has no attribute 'name'"})
					}
					this = ToIdentifier(e.This().Name(), nil)
				}
				e = p.expression(New(KPropertyEQ, "this", this, "expression", e.Expression()))
			}

			if e.This().IsA(KColumn) {
				e.This().Replace(e.This().This())
			}
		} else {
			e = p.toPropEq(e, index)
		}

		transformed = append(transformed, e)
	}

	return transformed
}

// _parse_function_properties (parser.py L7146)
func (p *Parser) baseParseFunctionProperties() *Expr {
	// Skip the generic `key = value` fallback in _parse_property since this
	// runs post-AS where a function body like `name = expr` can be misread
	// as a property.
	properties := []*Expr{}
	for {
		var prop any
		if matchTextKeys(p, p.s.PROPERTY_PARSERS) {
			prop = p.s.PROPERTY_PARSERS[upperText(p.prev)](p, propKwargs{})
		} else if p.match(TK_DEFAULT) && matchTextKeys(p, p.s.PROPERTY_PARSERS) {
			prop = p.s.PROPERTY_PARSERS[upperText(p.prev)](p, propKwargs{default_: true})
		} else {
			break
		}
		switch v := prop.(type) {
		case *Expr:
			if v != nil {
				properties = append(properties, v)
			}
		case []*Expr:
			properties = append(properties, v...)
		}
	}

	if len(properties) > 0 {
		return p.expression(New(KProperties, "expressions", properties))
	}
	return nil
}

// _parse_user_defined_function_expression (parser.py L7163)
func (p *Parser) baseParseUserDefinedFunctionExpression() *Expr {
	return p.parseStatement()
}

// _parse_function_parameter (parser.py L7166)
func (p *Parser) baseParseFunctionParameter() *Expr {
	return p.parseColumnDef(p.parseIdVar(true, nil), false)
}

// _parse_user_defined_function (parser.py L7169)
func (p *Parser) baseParseUserDefinedFunction(kind TokenType) *Expr {
	this := p.parseTableParts(true, false, false, false)

	if !p.match(TK_L_PAREN) {
		return this
	}

	expressions := p.parseCSV(p.parseFunctionParameter, TK_COMMA)
	p.matchRParen(nil)
	return p.expression(New(KUserDefinedFunction, "this", this, "expressions", expressions, "wrapped", true))
}

// _parse_macro_overloads (parser.py L7181)
func (p *Parser) parseMacroOverloads(this *Expr, firstBody *Expr, firstIsTable bool) *Expr {
	var firstExpressions any
	if exprs := this.Expressions(); len(exprs) > 0 {
		firstExpressions = exprs
	}
	overloads := []*Expr{
		p.expression(New(
			KMacroOverload,
			"this", firstBody,
			"expressions", firstExpressions,
			"is_table", firstIsTable,
		)),
	}
	this.Set("expressions", nil)
	this.Set("wrapped", false)

	for p.match(TK_COMMA) {
		if !p.match(TK_L_PAREN) {
			break
		}

		params := p.parseCSV(p.parseFunctionParameter, TK_COMMA)
		p.matchRParen(nil)

		if !p.match(TK_ALIAS) {
			break
		}

		isTable := p.match(TK_TABLE)
		body := p.parseExpression()
		macro := New(KMacroOverload, "this", body, "expressions", params, "is_table", isTable)
		overloads = append(overloads, p.expression(macro))
	}

	return p.expression(New(KMacroOverloads, "expressions", overloads))
}

// _parse_introducer (parser.py L7216)
func (p *Parser) parseIntroducer(token *Token) *Expr {
	literal := p.parsePrimary()
	if literal != nil {
		return p.expressionTok(New(KIntroducer, "this", token.Text, "expression", literal), token)
	}

	return p.identifierExpression(token, TriNone)
}

// _parse_session_parameter (parser.py L7223)
func (p *Parser) parseSessionParameter() *Expr {
	var kind any
	this := p.parseIdVar(true, nil)
	if this == nil {
		this = p.parsePrimary()
	}

	if this != nil && p.match(TK_DOT) {
		kind = this.Name()
		this = p.parseVar(false, nil, false)
		if this == nil {
			this = p.parsePrimary()
		}
	}

	return p.expression(New(KSessionParameter, "this", this, "kind", kind))
}

// _parse_lambda_arg (parser.py L7233)
func (p *Parser) baseParseLambdaArg() *Expr {
	return p.parseIdVar(true, nil)
}

// _parse_lambda (parser.py L7236)
func (p *Parser) baseParseLambda(alias bool) *Expr {
	nextTokenType := p.next.Type

	// Fast path: simple atom (column, literal, null, bool) followed by , or )
	if p.s.LAMBDA_ARG_TERMINATORS.Has(nextTokenType) {
		if atom := p.parseAtom(); atom != nil {
			return atom
		}
	}

	index := p.index

	if p.match(TK_L_PAREN) {
		expressions := p.parseCSV(p.parseLambdaArg, TK_COMMA)

		if !p.match(TK_R_PAREN) {
			p.retreat(index)
		} else if chunkDMatchKey(p, p.s.LAMBDAS) {
			return p.s.LAMBDAS[p.prev.Type](p, expressions)
		} else {
			p.retreat(index)
		}
	} else if _, isLambda := p.s.LAMBDAS[nextTokenType]; p.s.TYPED_LAMBDA_ARGS || isLambda {
		expressions := []*Expr{p.parseLambdaArg()}

		if chunkDMatchKey(p, p.s.LAMBDAS) {
			return p.s.LAMBDAS[p.prev.Type](p, expressions)
		}

		p.retreat(index)
	}

	var this *Expr

	if p.match(TK_DISTINCT) {
		this = p.expression(New(KDistinct, "expressions", p.parseCSV(p.parseDisjunction, TK_COMMA)))
	} else {
		p.match(TK_ALL) // ALL is the default/no-op aggregate modifier (SQL-92)
		this = p.parseSelectOrExpression(alias)
	}

	return p.parseLimit(
		p.parseRespectOrIgnoreNulls(
			p.parseOrder(p.parseHavingMax(p.parseRespectOrIgnoreNulls(this)), false),
		),
		false,
		false,
	)
}

// _parse_schema (parser.py L7283)
func (p *Parser) parseSchema(this *Expr) *Expr {
	index := p.index
	if !p.match(TK_L_PAREN) {
		return this
	}

	// Disambiguate between schema and subquery/CTE, e.g. in INSERT INTO table (<expr>),
	// expr can be of both types
	if p.matchSet(p.s.SELECT_START_TOKENS) {
		p.retreat(index)
		return this
	}
	args := p.parseCSV(func() *Expr {
		if c := p.parseConstraint(); c != nil {
			return c
		}
		return p.parseFieldDef()
	}, TK_COMMA)
	p.matchRParen(nil)
	return p.expression(New(KSchema, "this", this, "expressions", args))
}

// _parse_field_def (parser.py L7297)
func (p *Parser) parseFieldDef() *Expr {
	return p.parseColumnDef(p.parseField(true, nil, false), true)
}
