package sqlengine

// Ports of sqlglot.parser.Parser column definition and column/table constraint parsing
// (parser.py L7300-L7713).

// _parse_column_def (parser.py L7300).
func (p *Parser) baseParseColumnDef(this *Expr, computedColumn bool) *Expr {
	// column defs are not really columns, they're identifiers
	if this.IsA(KColumn) {
		this = this.This()
	}

	if !computedColumn {
		p.match(TK_ALIAS)
	}

	kind := p.parseTypes(false, true, true, false)

	if p.matchTextSeq("FOR", "ORDINALITY") {
		return p.expression(New(KColumnDef, "this", this, "ordinality", true))
	}

	constraints := []*Expr{}

	if (kind == nil && p.match(TK_ALIAS)) || p.matchTexts("ALIAS", "MATERIALIZED") {
		persisted := upperText(p.prev) == "MATERIALIZED"
		ccThis := p.parseDisjunction()
		ccPersisted := persisted || p.matchTextSeq("PERSISTED")
		var dataType *Expr
		if p.matchTextSeq("AUTO") {
			dataType = New(KVar, "this", "AUTO")
		} else {
			dataType = p.parseTypes(false, false, true, false)
		}
		notNull := p.matchPair(TK_NOT, TK_NULL)
		constraintKind := New(
			KComputedColumnConstraint,
			"this", ccThis,
			"persisted", ccPersisted,
			"data_type", dataType,
			"not_null", notNull,
		)
		constraints = append(constraints, p.expression(New(KColumnConstraint, "kind", constraintKind)))
	} else if kind == nil && p.matchAnyNoAdvance(TK_IN, TK_OUT) {
		input := p.match(TK_IN)
		output := p.match(TK_OUT)
		inOutConstraint := p.expression(New(KInOutColumnConstraint, "input_", input, "output", output))
		constraints = append(constraints, inOutConstraint)
		kind = p.parseTypes(false, false, true, false)
	} else if kind != nil &&
		p.matchNoAdvance(TK_ALIAS) &&
		(!p.s.WRAPPED_TRANSFORM_COLUMN_CONSTRAINT || p.next.Type == TK_L_PAREN) {
		p.advance(1)
		ccThis := p.parseDisjunction()
		persisted := p.matchTexts("STORED", "VIRTUAL") && upperText(p.prev) == "STORED"
		constraints = append(constraints, p.expression(New(
			KColumnConstraint,
			"kind", New(KComputedColumnConstraint, "this", ccThis, "persisted", persisted),
		)))
	}

	for {
		constraint := p.parseColumnConstraint()
		if constraint == nil {
			break
		}
		constraints = append(constraints, constraint)
	}

	if kind == nil && len(constraints) == 0 {
		return this
	}

	var position *Expr
	if p.matchTexts("FIRST", "AFTER") {
		pos := p.prev.Text
		position = p.expression(New(KColumnPosition, "this", p.parseColumn(), "position", pos))
	}

	return p.expression(New(KColumnDef, "this", this, "kind", kind, "constraints", constraints, "position", position))
}

// _parse_auto_increment (parser.py L7377).
func (p *Parser) parseAutoIncrement() *Expr {
	var start, increment *Expr
	var order any // None / True / False

	if p.matchNoAdvance(TK_L_PAREN) {
		args := p.parseWrappedCSV(p.parseBitwise, TK_COMMA, false)
		start = seqGet(args, 0)
		increment = seqGet(args, 1)
	}

	// The remaining parts form an unordered bag and any of them can be omitted, in which
	// case the engine falls back to its own default, so they're parsed independently.
	for {
		if p.matchTextSeq("START") {
			start = p.parseBitwise()
		} else if p.matchTextSeq("INCREMENT") {
			increment = p.parseBitwise()
		} else if p.matchTextSeq("ORDER") {
			order = true
		} else if p.matchTextSeq("NOORDER") {
			order = false
		} else {
			break
		}
	}

	if start != nil || increment != nil || order != nil {
		return New(KGeneratedAsIdentityColumnConstraint,
			"start", start, "increment", increment, "this", false, "order", order)
	}

	return New(KAutoIncrementColumnConstraint)
}

// _parse_check_constraint (parser.py L7410).
func (p *Parser) baseParseCheckConstraint() *Expr {
	if !p.matchNoAdvance(TK_L_PAREN) {
		return nil
	}

	this := p.parseWrapped(p.parseAssignment, false)
	enforced := p.matchTextSeq("ENFORCED")
	return p.expression(New(KCheckColumnConstraint, "this", this, "enforced", enforced))
}

// _parse_auto_property (parser.py L7421).
func (p *Parser) parseAutoProperty() *Expr {
	if !p.matchTextSeq("REFRESH") {
		p.retreat(p.index - 1)
		return nil
	}
	return p.expression(New(KAutoRefreshProperty, "this", p.parseVar(false, nil, true)))
}

// _parse_compress (parser.py L7427).
func (p *Parser) parseCompress() *Expr {
	if p.matchNoAdvance(TK_L_PAREN) {
		return p.expression(New(KCompressColumnConstraint,
			"this", p.parseWrappedCSV(p.parseBitwise, TK_COMMA, false)))
	}

	return p.expression(New(KCompressColumnConstraint, "this", p.parseBitwise()))
}

// _parse_generated_as_identity (parser.py L7435).
func (p *Parser) baseParseGeneratedAsIdentity() *Expr {
	var this *Expr
	if p.matchTextSeq("BY", "DEFAULT") {
		onNull := p.matchPair(TK_ON, TK_NULL)
		this = p.expression(New(KGeneratedAsIdentityColumnConstraint, "this", false, "on_null", onNull))
	} else {
		p.matchTextSeq("ALWAYS")
		this = p.expression(New(KGeneratedAsIdentityColumnConstraint, "this", true))
	}

	p.match(TK_ALIAS)

	if p.matchTextSeq("ROW") {
		start := p.matchTextSeq("START")
		if !start {
			p.match(TK_END)
		}
		hidden := p.matchTextSeq("HIDDEN")
		return p.expression(New(KGeneratedAsRowColumnConstraint, "start", start, "hidden", hidden))
	}

	identity := p.matchTextSeq("IDENTITY")

	if p.match(TK_L_PAREN) {
		if p.match(TK_START_WITH) {
			this.Set("start", p.parseBitwise())
		}
		if p.matchTextSeq("INCREMENT", "BY") {
			this.Set("increment", p.parseBitwise())
		}
		if p.matchTextSeq("MINVALUE") {
			this.Set("minvalue", p.parseBitwise())
		}
		if p.matchTextSeq("MAXVALUE") {
			this.Set("maxvalue", p.parseBitwise())
		}

		if p.matchTextSeq("CYCLE") {
			this.Set("cycle", true)
		} else if p.matchTextSeq("NO", "CYCLE") {
			this.Set("cycle", false)
		}

		if !identity {
			this.Set("expression", p.parseRange(nil))
		} else if !this.ArgB("start") && p.matchNoAdvance(TK_NUMBER) {
			args := p.parseCSV(p.parseBitwise, TK_COMMA)
			this.Set("start", seqGet(args, 0))
			this.Set("increment", seqGet(args, 1))
		}

		p.matchRParen(nil)
	}

	return this
}

// _parse_inline (parser.py L7488).
func (p *Parser) parseInline() *Expr {
	p.matchTextSeq("LENGTH")
	return p.expression(New(KInlineLengthColumnConstraint, "this", p.parseBitwise()))
}

// _parse_not_constraint (parser.py L7492).
func (p *Parser) parseNotConstraint() *Expr {
	if p.matchTextSeq("NULL") {
		return p.expression(New(KNotNullColumnConstraint))
	}
	if p.matchTextSeq("CASESPECIFIC") {
		return p.expression(New(KCaseSpecificColumnConstraint, "not_", true))
	}
	if p.matchTextSeq("FOR", "REPLICATION") {
		return p.expression(New(KNotForReplicationColumnConstraint))
	}

	// Unconsume the `NOT` token
	p.retreat(p.index - 1)
	return nil
}

// _parse_column_constraint (parser.py L7504).
func (p *Parser) parseColumnConstraint() *Expr {
	var this *Expr
	if p.match(TK_CONSTRAINT) {
		this = p.parseIdVar(true, nil)
	}

	procedureOptionFollows := false
	if p.matchNoAdvance(TK_WITH) && p.next.ok() {
		_, procedureOptionFollows = p.s.PROCEDURE_OPTIONS[upperText(p.next)]
	}

	if !procedureOptionFollows && matchTextKeys(p, p.s.CONSTRAINT_PARSERS) {
		constraint := p.s.CONSTRAINT_PARSERS[upperText(p.prev)](p)
		if constraint == nil {
			p.retreat(p.index - 1)
			return nil
		}

		return p.expression(New(KColumnConstraint, "this", this, "kind", constraint))
	}

	return this
}

// _parse_constraint (parser.py L7523).
func (p *Parser) baseParseConstraint() *Expr {
	if !p.match(TK_CONSTRAINT) {
		return p.parseUnnamedConstraint(p.s.SCHEMA_UNNAMED_CONSTRAINTS)
	}

	this := p.parseIdVar(true, nil)
	return p.expression(New(KConstraint, "this", this, "expressions", p.parseUnnamedConstraints()))
}

// _parse_unnamed_constraints (parser.py L7531).
func (p *Parser) parseUnnamedConstraints() []*Expr {
	constraints := []*Expr{}
	for {
		constraint := p.parseUnnamedConstraint(nil)
		if constraint == nil {
			constraint = p.parseFunction(nil, false, true, false)
		}
		if constraint == nil {
			break
		}
		constraints = append(constraints, constraint)
	}

	return constraints
}

// _parse_unnamed_constraint (parser.py L7541).
func (p *Parser) parseUnnamedConstraint(constraints StrSet) *Expr {
	index := p.index

	if p.matchNoAdvance(TK_IDENTIFIER) {
		return nil
	}
	var matched bool
	if len(constraints) > 0 {
		matched = p.matchTextSet(constraints)
	} else {
		matched = matchTextKeys(p, p.s.CONSTRAINT_PARSERS)
	}
	if !matched {
		return nil
	}

	constraintKey := upperText(p.prev)
	parser, ok := p.s.CONSTRAINT_PARSERS[constraintKey]
	if !ok {
		p.raiseError("No parser found for schema constraint "+constraintKey+".", nil)
		// Python then fails with a KeyError on the CONSTRAINT_PARSERS lookup.
		panic(&ValueError{Msg: pyRepr(constraintKey)})
	}

	result := parser(p)
	if result == nil {
		p.retreat(index)
	}

	return result
}

// _parse_unique_key (parser.py L7559).
func (p *Parser) baseParseUniqueKey() *Expr {
	if p.curr.ok() && p.curr.Type != TK_IDENTIFIER {
		if _, ok := p.s.CONSTRAINT_PARSERS[upperText(p.curr)]; ok {
			return nil
		}
	}
	return p.parseIdVar(false, nil)
}

// _parse_unique (parser.py L7568).
func (p *Parser) baseParseUnique() *Expr {
	p.matchTexts("KEY", "INDEX")
	nulls := p.matchTextSeq("NULLS", "NOT", "DISTINCT")
	this := p.parseSchema(p.parseUniqueKey())
	// self._match(TokenType.USING) and self._advance_any() and self._prev.text: False when USING is
	// unmatched, None when no token follows it.
	var indexType any = false
	if p.match(TK_USING) {
		indexType = nil
		if p.advanceAny(false) != nil {
			indexType = p.prev.Text
		}
	}
	onConflict := p.parseOnConflict()
	options := p.parseKeyConstraintOptions()
	return p.expression(New(
		KUniqueColumnConstraint,
		"nulls", nulls,
		"this", this,
		"index_type", indexType,
		"on_conflict", onConflict,
		"options", options,
	))
}

// _parse_key_constraint_options (parser.py L7580)
//
// NOTE: sqlglot returns list[str]; the generated signature said []*Expr, but this method is only
// called from this file, so it returns []string (stored as-is in the "options" arg).
func (p *Parser) parseKeyConstraintOptions() []string {
	options := []string{}
	for {
		if !p.curr.ok() {
			break
		}

		if p.match(TK_ON) {
			action := "None"
			on := "None"
			if p.advanceAny(false) != nil {
				on = p.prev.Text
			}

			if p.matchTextSeq("NO", "ACTION") {
				action = "NO ACTION"
			} else if p.matchTextSeq("CASCADE") {
				action = "CASCADE"
			} else if p.matchTextSeq("RESTRICT") {
				action = "RESTRICT"
			} else if p.matchPair(TK_SET, TK_NULL) {
				action = "SET NULL"
			} else if p.matchPair(TK_SET, TK_DEFAULT) {
				action = "SET DEFAULT"
			} else {
				p.raiseError("Invalid key constraint", nil)
			}

			options = append(options, "ON "+on+" "+action)
		} else {
			v := p.parseVarFromOptions(p.s.KEY_CONSTRAINT_OPTIONS, false)
			if v == nil {
				break
			}
			options = append(options, v.Name())
		}
	}

	return options
}

// _parse_references (parser.py L7614).
func (p *Parser) parseReferences(match bool) *Expr {
	if match && !p.match(TK_REFERENCES) {
		return nil
	}

	var expressions any // None
	this := p.parseTable(true, false, nil, false, false, false, false)
	options := p.parseKeyConstraintOptions()
	return p.expression(New(KReference, "this", this, "expressions", expressions, "options", options))
}

// _parse_foreign_key (parser.py L7623).
func (p *Parser) baseParseForeignKey() *Expr {
	var expressions any // None unless the column list is present
	if !p.matchNoAdvance(TK_REFERENCES) {
		expressions = p.parseWrappedIdVars(false)
	}
	reference := p.parseReferences(true)
	var onKeys []string
	onOptions := map[string]string{}

	for p.match(TK_ON) {
		if !p.matchAny(TK_DELETE, TK_UPDATE) {
			p.raiseError("Expected DELETE or UPDATE", nil)
		}

		kind := pyLower(p.prev.Text)

		var action string
		if p.matchTextSeq("NO", "ACTION") {
			action = "NO ACTION"
		} else if p.match(TK_SET) {
			p.matchAny(TK_NULL, TK_DEFAULT)
			action = "SET " + upperText(p.prev)
		} else {
			p.advance(1)
			action = upperText(p.prev)
		}

		if _, seen := onOptions[kind]; !seen {
			onKeys = append(onKeys, kind)
		}
		onOptions[kind] = action
	}

	kv := []any{
		"expressions", expressions,
		"reference", reference,
		"options", p.parseKeyConstraintOptions(),
	}
	for _, k := range onKeys {
		kv = append(kv, k, onOptions[k])
	}
	return p.expression(New(KForeignKey, kv...))
}

// _parse_primary_key_part (parser.py L7658).
func (p *Parser) baseParsePrimaryKeyPart() *Expr {
	return p.parseField(false, nil, false)
}

// _parse_period_for_system_time (parser.py L7661).
func (p *Parser) parsePeriodForSystemTime() *Expr {
	if !p.match(TK_TIMESTAMP_SNAPSHOT) {
		p.retreat(p.index - 1)
		return nil
	}

	idVars := p.parseWrappedIdVars(false)
	return p.expression(New(KPeriodForSystemTimeConstraint,
		"this", seqGet(idVars, 0), "expression", seqGet(idVars, 1)))
}

// _parse_primary_key (parser.py L7673).
func (p *Parser) baseParsePrimaryKey(wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr {
	var desc any // None / True / False
	if p.matchAny(TK_ASC, TK_DESC) {
		desc = p.prev.Type == TK_DESC
	}

	var this *Expr
	if namedPrimaryKey {
		if _, isConstraint := p.s.CONSTRAINT_PARSERS[upperText(p.curr)]; !isConstraint &&
			p.next.ok() && p.next.Type == TK_L_PAREN {
			this = p.parseIdVar(true, nil)
		}
	}

	if !inProps && !p.matchNoAdvance(TK_L_PAREN) {
		return p.expression(New(KPrimaryKeyColumnConstraint,
			"desc", desc, "options", p.parseKeyConstraintOptions()))
	}

	expressions := p.parseWrappedCSV(p.parsePrimaryKeyPart, TK_COMMA, wrappedOptional)

	include := p.parseIndexParams()
	options := p.parseKeyConstraintOptions()
	return p.expression(New(
		KPrimaryKey,
		"this", this,
		"expressions", expressions,
		"include", include,
		"options", options,
	))
}
