package sqlengine

import "strings"

// anonymousFuncHint is the comment prefix that makes a known function call parse as an
// anonymous function.
const anonymousFuncHint = "sqlglot.anonymous"

// parseAtom parses a simple operand quickly: a column (when the current token can start one)
// or a primary literal not followed by a column operator, a postfix token or another string.
// Returns nil, consuming nothing, when the full expression parser is needed.
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

// baseParseColumn parses a column reference with its column operators (dots, brackets, casts),
// an optional join mark and, with COLON_IS_VARIANT_EXTRACT, a :path variant extraction.
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
		if p.d.S.SUPPORTS_COLUMN_JOIN_MARKS && p.match(TK_JOIN_MARKER) {
			column.Set("join_mark", true)
		}
		if p.s.COLON_IS_VARIANT_EXTRACT {
			column = p.parseColonAsVariantExtract(column)
		}
	}

	return column
}

// parseColumnPartsFast is the fast path for simple column and dot references (a, a.b, ...).
//
// Greedily consumes VAR/IDENTIFIER tokens separated by DOTs, then checks
// that nothing complex follows. If it does, retreats and returns nil so
// the slow path can handle it. For >4 parts, wraps in Dot nodes.
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

// parseColumnReference parses a field (or a VALUES keyword used as a name) and wraps a bare
// identifier in a Column.
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

// buildJsonExtract wraps this in a variant JSONExtract over the collected path parts, when
// there is more than the root, and returns it with a new path holding only the root.
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

var selectTokenSet = newTokenSet(TK_SELECT)

// parseColonAsVariantExtract parses a variant extraction, this:key[.key | [index] | ::type]...,
// into JSONExtract nodes, with Bracket nodes for non-literal subscripts and casts in between.
// With COLON_CHAIN_IS_SINGLE_EXTRACT, chained colons build a single path.
func (p *Parser) parseColonAsVariantExtract(this *Expr) *Expr {
	if !p.match(TK_COLON) {
		// No path: the final buildJsonExtract on a lone JSONPathRoot is a no-op.
		return this
	}
	pathParts := []*Expr{New(KJSONPathRoot)}
	selectTokens := &selectTokenSet

	for first := true; first || p.match(TK_COLON); first = false {
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

func (p *Parser) baseParseDcolon() *Expr {
	return p.parseTypes(false, false, true, false)
}

// baseParseColumnOps parses the operators following a column: brackets, dots (building
// qualified columns or Dot chains), casts and the dialect's COLUMN_OPERATORS. Qualified function
// calls (x.y.FOO()) become Dot chains.
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
			// Move the Dots to the window's function
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

// parseParen parses a parenthesized expression: () or (a, b) as a Tuple, a parenthesized query
// as a subquery, otherwise a Paren. A parenthesized aggregate may be followed by a window.
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

// baseParsePrimary parses a primary through PRIMARY_PARSERS (adjacent strings become a CONCAT),
// .5 as 0.5, or else a parenthesized expression.
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

// parseField parses a primary or a function call (the function first with anonymousFunc), or
// else an identifier. Where field names can be digit-prefixed, a number after a dot is a quoted
// identifier, joined with a name glued to it (data.144A_FLAG).
func (p *Parser) parseField(anyToken bool, tokens *TokenSet, anonymousFunc bool) *Expr {
	afterDot := p.s.SUPPORTS_DIGIT_PREFIXED_FIELD_NAMES && p.prev.ok() && p.prev.Type == TK_DOT

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
	if field == nil {
		field = p.parseIdVar(anyToken, tokens)
	}

	if afterDot && field.IsA(KLiteral) && field.IsNumber() {
		name := field.Name()
		if p.isConnected() && p.parseVar(true, nil, false) != nil {
			name += p.prev.Text
		}

		field = New(KIdentifier, "this", name, "quoted", true).updatePositionsFrom(field)
	}

	return field
}

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

func (p *Parser) parseFunctionArgs(alias bool) []*Expr {
	return p.parseCSV(func() *Expr { return p.parseLambda(alias) }, TK_COMMA)
}

// parseConnectorFunction parses the arguments of a function form of AND / OR (kind) and joins
// them with the connector, parenthesized so it keeps its precedence where it is used.
func (p *Parser) parseConnectorFunction(kind Kind) *Expr {
	args := p.parseFunctionArgs(false)
	if len(args) == 0 {
		p.raiseError("Expected at least one argument", nil)
	}
	return New(KParen, "this", combineConditions(args, kind, false, true))
}

// baseParseFunctionCall parses a function call at the current token: a no-paren function, a
// FUNCTION_PARSERS entry, a subquery predicate (EXISTS/ANY/... (query)), a known function
// through its builder or an anonymous function, followed by any window. It returns nil when
// the current token does not start a function call. A non-nil functions replaces FUNCTIONS;
// anonymous forces an anonymous function.
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

	// Note: the final R_PAREN match below depends on FUNCTION_PARSERS even when `anonymous`
	// is set.
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
				// to parse "LIKE [ANY | ALL] (...)" as a whole into a Tuple or Paren
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
			// If a user comment starting with anonymousFuncHint follows the function call,
			// we'll construct it as an Anonymous function, even if it's "known"
			for _, comment := range postFuncComments {
				if strings.HasPrefix(strings.TrimLeftFunc(comment, pyIsSpaceRune), anonymousFuncHint) {
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

// baseToPropEq converts a non key-value argument for kvToPropEq; the base parser keeps it as is.
func (p *Parser) baseToPropEq(expression *Expr, index int) *Expr {
	return expression
}

// kvToPropEq converts key-value function arguments (aliases, key/value pairs) into PropertyEQ
// nodes with identifier keys (expression keys with parseMap); other arguments go through
// toPropEq.
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
						// A missing key (e.g. `{: 1}`) is an error; the message keeps its
						// historical wording
						panic(&ValueError{Msg: "'NoneType' object has no attribute 'name'"})
					}
					this = ToIdentifier(e.This().Name(), nil)
				}
				e = p.expression(New(KPropertyEQ, "this", this, "expression", e.Expression()))
			}

			if !parseMap && e.This().IsA(KColumn) {
				e.This().Replace(e.This().This())
			}
		} else {
			e = p.toPropEq(e, index)
		}

		transformed = append(transformed, e)
	}

	return transformed
}

// baseParseFunctionProperties parses the properties following a function body (keyword
// PROPERTY_PARSERS only, optionally after DEFAULT); nil when there are none.
func (p *Parser) baseParseFunctionProperties() *Expr {
	// Skip the generic `key = value` fallback in parseProperty since this
	// runs post-AS where a function body like `name = expr` can be misread
	// as a property.
	properties := []*Expr{}
	for {
		var prop any
		if matchTextKeys(p, p.s.PROPERTY_PARSERS) {
			prop = p.s.PROPERTY_PARSERS[upperText(p.prev)](p, propKwargs{})
		} else if p.match(TK_DEFAULT) && matchTextKeys(p, p.s.PROPERTY_PARSERS) {
			prop = p.callPropertyParser(p.s.PROPERTY_PARSERS[upperText(p.prev)], propKwargs{default_: true})
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

func (p *Parser) baseParseUserDefinedFunctionExpression() *Expr {
	return p.parseStatement()
}

func (p *Parser) baseParseFunctionParameter() *Expr {
	return p.parseColumnDef(p.parseIdVar(true, nil), false)
}

func (p *Parser) baseParseUserDefinedFunction(kind TokenType) *Expr {
	this := p.parseTableParts(true, false, false, false)

	if !p.match(TK_L_PAREN) {
		return this
	}

	expressions := p.parseCSV(p.parseFunctionParameter, TK_COMMA)
	p.matchRParen(nil)
	return p.expression(New(KUserDefinedFunction, "this", this, "expressions", expressions, "wrapped", true))
}

// parseMacroOverloads collects a macro's first definition and the following
// `, (params) AS [TABLE] body` overloads into MacroOverloads, moving the first parameter list
// off this.
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

// parseIntroducer parses a charset introducer followed by a literal (e.g. _utf8'x'); without a
// literal the token is an identifier.
func (p *Parser) parseIntroducer(token *Token) *Expr {
	literal := p.parsePrimary()
	if literal != nil {
		return p.expressionTok(New(KIntroducer, "this", token.Text, "expression", literal), token)
	}

	return p.identifierExpression(token, TriNone)
}

// parseSessionParameter parses a session parameter name after @@, as [kind.]name.
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

func (p *Parser) baseParseLambdaArg() *Expr {
	return p.parseIdVar(true, nil)
}

// baseParseLambda parses a function argument: a lambda ((a, b) -> ... or a -> ...), or else
// [DISTINCT|ALL] expression with IGNORE/RESPECT NULLS, HAVING MAX, ORDER BY and LIMIT suffixes.
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
		this = p.expression(New(KDistinct, "expressions", p.parseCSV(func() *Expr { return p.parsePer(p.parseDisjunction()) }, TK_COMMA)))
	} else {
		p.match(TK_ALL) // ALL is the default/no-op aggregate modifier (SQL-92)
		this = p.parsePer(p.parseSelectOrExpression(alias))
	}

	return p.parseLimit(
		p.parseRespectOrIgnoreNulls(
			p.parseOrder(p.parseHavingMax(p.parseRespectOrIgnoreNulls(this)), false),
		),
		false,
		false,
	)
}

// parseSchema parses a parenthesized list of column definitions and constraints after this; it
// retreats when the parenthesis opens a query.
func (p *Parser) parseSchema(this *Expr) *Expr {
	index := p.index
	if !p.match(TK_L_PAREN) {
		return this
	}

	// Disambiguate between schema and subquery/CTE, e.g. in INSERT INTO table (<expr>),
	// expr can be of both types
	if p.matchSet(p.s.SELECT_START_TOKENS) || p.isTableQuery() {
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

// baseParseFieldDef parses a column definition whose name may be any token.
func (p *Parser) baseParseFieldDef() *Expr {
	return p.parseColumnDef(p.parseField(true, nil, false), true)
}
