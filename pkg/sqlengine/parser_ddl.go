package sqlengine

import "fmt"

// Port of sqlglot.parser.Parser (chunk A, part 1): commands, COMMENT, TTL, blocks, statements,
// DROP, CREATE, sequence properties and triggers (parser.py L1430, L2211-L2793).

// _parse_partitioned_by_bucket_or_truncate (parser.py L1430)
func (p *Parser) parsePartitionedByBucketOrTruncate() *Expr {
	if !p.matchNoAdvance(TK_L_PAREN) {
		// Partitioning by bucket or truncate follows the syntax:
		// PARTITION BY (BUCKET(..) | TRUNCATE(..))
		// If we don't have parenthesis after each keyword, we should instead parse this as an identifier
		p.retreat(p.index - 1)
		return nil
	}

	klass := KPartitionByTruncate
	if upperText(p.prev) == "BUCKET" {
		klass = KPartitionedByBucket
	}

	args := p.parseWrappedCSV(func() *Expr { return exprOr(p.parsePrimary(), p.parseColumn) }, TK_COMMA, false)
	this, expression := seqGet(args, 0), seqGet(args, 1)

	if this.IsA(KLiteral) {
		// Check for Iceberg partition transforms (bucket / truncate) and ensure their arguments are in the right order
		//  - For Hive, it's `bucket(<num buckets>, <col name>)` or `truncate(<num_chars>, <col_name>)`
		//  - For Trino, it's reversed - `bucket(<col name>, <num buckets>)` or `truncate(<col_name>, <num_chars>)`
		// Both variants are canonicalized in the latter i.e `bucket(<col name>, <num buckets>)`
		this, expression = expression, this
	}

	return p.expression(New(klass, "this", this, "expression", expression))
}

// _parse_command (parser.py L2211)
func (p *Parser) parseCommand() *Expr {
	p.warnUnsupported()
	comments := p.prevComments
	this := upperText(p.prev)
	return p.expressionC(New(KCommand, "this", this, "expression", p.parseString()), comments)
}

// _parse_comment (parser.py L2219)
func (p *Parser) parseComment(allowExists bool) *Expr {
	start := p.prev
	exists := false
	if allowExists {
		exists = p.parseExists(false)
	}

	p.match(TK_ON)

	materialized := p.matchTextSeq("MATERIALIZED")
	var kind *Token
	if p.matchSet(p.s.CREATABLES) {
		kind = p.prev
	}
	if kind == nil {
		return p.parseAsCommand(start)
	}

	var this *Expr
	if kind.Type == TK_FUNCTION || kind.Type == TK_PROCEDURE {
		this = p.parseUserDefinedFunction(kind.Type)
	} else if kind.Type == TK_TABLE {
		this = p.parseTable(false, false, &p.s.COMMENT_TABLE_ALIAS_TOKENS, false, false, false, false)
	} else if kind.Type == TK_COLUMN {
		this = p.parseColumn()
	} else {
		this = p.parseTableParts(true, false, false, false)
	}

	p.match(TK_IS)

	return p.expression(New(
		KComment,
		"this", this,
		"kind", kind.Text,
		"expression", p.parseString(),
		"exists", exists,
		"materialized", materialized,
	))
}

// _parse_to_table (parser.py L2251)
func (p *Parser) parseToTable() *Expr {
	table := p.parseTableParts(true, false, false, false)
	return p.expression(New(KToTableProperty, "this", table))
}

// _parse_ttl (parser.py L2258)
// https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl
func (p *Parser) parseTtl() *Expr {
	parseTtlAction := func() *Expr {
		this := p.parseBitwise()

		if p.matchTextSeq("DELETE") {
			return p.expression(New(KMergeTreeTTLAction, "this", this, "delete", true))
		}
		if p.matchTextSeq("RECOMPRESS") {
			return p.expression(New(KMergeTreeTTLAction, "this", this, "recompress", p.parseBitwise()))
		}
		if p.matchTextSeq("TO", "DISK") {
			return p.expression(New(KMergeTreeTTLAction, "this", this, "to_disk", p.parseString()))
		}
		if p.matchTextSeq("TO", "VOLUME") {
			return p.expression(New(KMergeTreeTTLAction, "this", this, "to_volume", p.parseString()))
		}

		return this
	}

	expressions := p.parseCSV(parseTtlAction, TK_COMMA)
	where := p.parseWhere(false)
	group := p.parseGroup(false)

	var aggregates any
	if group != nil && p.match(TK_SET) {
		aggregates = p.parseCSV(p.parseSetItem, TK_COMMA)
	}

	return p.expression(New(
		KMergeTreeTTL,
		"expressions", expressions,
		"where", where,
		"group", group,
		"aggregates", aggregates,
	))
}

// _parse_condition (parser.py L2293)
func (p *Parser) parseCondition() *Expr {
	return p.parseWrapped(p.parseExpression, true)
}

// _parse_block (parser.py L2296)
func (p *Parser) parseBlock() *Expr {
	return p.expression(New(
		KBlock,
		"expressions", p.parseBatchStatements(func(p *Parser) *Expr { return p.parseStatement() }, true),
	))
}

// _parse_whileblock (parser.py L2305)
func (p *Parser) parseWhileblock() *Expr {
	return p.expression(New(KWhileBlock, "this", p.parseCondition(), "body", p.parseBlock()))
}

// _parse_statement (parser.py L2310)
func (p *Parser) parseStatement() *Expr {
	p.enter()
	defer p.leave()
	if !p.curr.ok() {
		return nil
	}

	if _, ok := p.s.STATEMENT_PARSERS[p.curr.Type]; ok {
		p.advance(1)
		comments := p.prevComments
		stmt := p.s.STATEMENT_PARSERS[p.prev.Type](p)
		stmt.AddComments(comments, true)
		return stmt
	}

	if p.matchSet(p.d.T.COMMANDS) {
		return p.parseCommand()
	}

	if p.matchTextSeq("WHILE") {
		return p.parseWhileblock()
	}

	expression := p.parseExpression()
	if expression != nil {
		expression = p.parseSetOperations(expression)
	} else {
		expression = p.parseSelect(false, false, true, true, true, nil)
	}

	if expression.IsA(KSubquery) && p.matchNoAdvance(TK_PIPE_GT) {
		expression = p.parsePipeSyntaxQuery(expression)
	}

	return p.parseQueryModifiers(expression)
}

// _parse_drop (parser.py L2334)
func (p *Parser) parseDrop(exists bool) *Expr {
	start := p.prev
	temporary := p.match(TK_TEMPORARY)
	materialized := p.matchTextSeq("MATERIALIZED")
	iceberg := p.matchTextSeq("ICEBERG")

	kind := ""
	if p.matchSet(p.s.CREATABLES) {
		kind = upperText(p.prev)
	}
	if kind == "" || (iceberg && kind != "" && kind != "TABLE") {
		return p.parseAsCommand(start)
	}

	concurrently := p.matchTextSeq("CONCURRENTLY")
	ifExists := exists || p.parseExists(false)

	var this *Expr
	if kind == "COLUMN" {
		this = p.parseColumn()
	} else {
		this = p.parseTableParts(true, kind == "SCHEMA", false, false)
	}

	var cluster *Expr
	if p.match(TK_ON) {
		cluster = p.parseOnProperty()
	}

	var expressions any
	if p.matchNoAdvance(TK_L_PAREN) {
		expressions = p.parseWrappedCSV(func() *Expr { return p.parseTypes(false, false, true, false) }, TK_COMMA, false)
	}

	cascadeOrRestrict := ""
	if p.matchTexts("CASCADE", "RESTRICT") {
		cascadeOrRestrict = upperText(p.prev)
	}

	mappedKind := p.d.S.CREATABLE_KIND_MAPPING[kind]
	if mappedKind == "" {
		mappedKind = kind
	}

	// Keyword arguments are evaluated in order in Python; keep the matching order.
	constraints := p.matchTextSeq("CONSTRAINTS")
	purge := p.matchTextSeq("PURGE")
	sync := p.matchTextSeq("SYNC")

	return p.expression(New(
		KDrop,
		"exists", ifExists,
		"this", this,
		"expressions", expressions,
		"kind", mappedKind,
		"temporary", temporary,
		"materialized", materialized,
		"cascade", cascadeOrRestrict == "CASCADE",
		"restrict", cascadeOrRestrict == "RESTRICT",
		"constraints", constraints,
		"purge", purge,
		"cluster", cluster,
		"concurrently", concurrently,
		"sync", sync,
		"iceberg", iceberg,
	))
}

// _parse_exists (parser.py L2380)
func (p *Parser) parseExists(not bool) bool {
	return p.matchTextSeq("IF") && (!not || p.match(TK_NOT)) && p.match(TK_EXISTS)
}

// _parse_create (parser.py L2387)
func (p *Parser) baseParseCreate() *Expr {
	// Note: this can't be None because we've matched a statement parser
	start := p.prev

	replace := start.Type == TK_REPLACE || p.matchPair(TK_OR, TK_REPLACE) || p.matchPair(TK_OR, TK_ALTER)
	refresh := p.matchPair(TK_OR, TK_REFRESH)

	unique := p.match(TK_UNIQUE)

	var clustered any
	if p.matchTextSeq("CLUSTERED", "COLUMNSTORE") {
		clustered = true
	} else if p.matchTextSeq("NONCLUSTERED", "COLUMNSTORE") || p.matchTextSeq("COLUMNSTORE") {
		clustered = false
	}

	if p.matchPairNoAdvance(TK_TABLE, TK_FUNCTION) {
		p.advance(1)
	}

	var properties *Expr
	var createToken *Token
	if p.matchSet(p.s.CREATABLES) {
		createToken = p.prev
	}

	if createToken == nil {
		// exp.Properties.Location.POST_CREATE
		properties = p.parseProperties(false)
		if p.matchSet(p.s.CREATABLES) {
			createToken = p.prev
		}

		if properties == nil || createToken == nil {
			return p.parseAsCommand(start)
		}
	}

	createTokenType := createToken.Type

	concurrently := p.matchTextSeq("CONCURRENTLY")
	exists := p.parseExists(true)
	var this *Expr
	var expression *Expr
	var indexes any
	var noSchemaBinding any
	var begin any
	var clone *Expr

	extendProps := func(tempProps *Expr) {
		if properties != nil && tempProps != nil {
			// properties.expressions.extend(...) mutates the list in place (no parent bookkeeping);
			// `.expressions` returns a fresh list when the arg is empty, so extending it is a no-op then.
			if existing := properties.Expressions(); len(existing) > 0 {
				properties.SetArgRaw("expressions", append(existing, tempProps.Expressions()...))
			}
		} else if tempProps != nil {
			properties = tempProps
		}
	}

	if createTokenType == TK_FUNCTION || createTokenType == TK_PROCEDURE {
		this = p.parseUserDefinedFunction(createTokenType)

		// exp.Properties.Location.POST_SCHEMA ("schema" here is the UDF's type signature)
		extendProps(p.parseProperties(false))

		if p.match(TK_ALIAS) {
			expression = p.parseHeredoc()
		}

		var isTable, overloadMode bool
		if expression == nil &&
			createTokenType == TK_FUNCTION &&
			this.IsA(KUserDefinedFunction) &&
			this.ArgB("wrapped") {
			preTableIndex := p.index
			isTable = p.match(TK_TABLE)

			expression = p.parseExpression()
			overloadMode = expression != nil &&
				p.curr.Type == TK_COMMA &&
				p.next.Type == TK_L_PAREN
			if !overloadMode {
				p.retreat(preTableIndex)
				isTable = false
				expression = nil
			}
		} else {
			isTable = false
			overloadMode = false
		}

		extendProps(p.parseFunctionProperties())

		if expression == nil {
			if p.match(TK_COMMAND) {
				expression = p.parseAsCommand(p.prev)
			} else {
				begin = p.match(TK_BEGIN)
				return_ := p.matchTextSeq("RETURN")

				if p.matchNoAdvance(TK_STRING) {
					// Takes care of BigQuery's JavaScript UDF definitions that end in an OPTIONS property
					// # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
					expression = p.parseString()
					extendProps(p.parseProperties(false))
				} else if createTokenType == TK_FUNCTION {
					expression = p.parseUserDefinedFunctionExpression()
				} else {
					expression = p.parseBlock()
				}

				if return_ {
					expression = p.expression(New(KReturn, "this", expression))
				}
			}
		}

		if overloadMode && expression != nil {
			expression = p.parseMacroOverloads(this, expression, isTable)
		}
	} else if createTokenType == TK_INDEX {
		// Postgres allows anonymous indexes, eg. CREATE INDEX IF NOT EXISTS ON t(c)
		var index *Expr
		var anonymous bool
		if !p.match(TK_ON) {
			index = p.parseIdVar(true, nil)
			anonymous = false
		} else {
			index = nil
			anonymous = true
		}

		this = p.parseIndex(index, anonymous)
	} else if (createTokenType == TK_CONSTRAINT && p.match(TK_TRIGGER)) || createTokenType == TK_TRIGGER {
		isConstraint := createTokenType == TK_CONSTRAINT
		if isConstraint {
			createToken = p.prev
		}

		triggerName := p.parseIdVar(true, nil)
		if triggerName == nil {
			return p.parseAsCommand(start)
		}

		timingVar := p.parseVarFromOptions(p.s.TRIGGER_TIMING, false)
		timing := ""
		if timingVar != nil {
			timing = timingVar.ThisS()
		}
		if timing == "" {
			return p.parseAsCommand(start)
		}

		events := p.parseTriggerEvents()
		if !p.match(TK_ON) {
			p.raiseError("Expected ON in trigger definition", nil)
		}

		table := p.parseTableParts(false, false, false, false)
		var referencedTable *Expr
		if p.match(TK_FROM) {
			referencedTable = p.parseTableParts(false, false, false, false)
		}
		deferrable, initially := p.parseTriggerDeferrable()
		referencing := p.parseTriggerReferencing()
		forEach := p.parseTriggerForEach()
		// self._match_text_seq("WHEN") and self._parse_wrapped(...) -> False when unmatched
		var when any = false
		if p.matchTextSeq("WHEN") {
			when = p.parseWrapped(p.parseDisjunction, true)
		}
		execute := p.parseTriggerExecute()

		if execute == nil {
			return p.parseAsCommand(start)
		}

		triggerProps := p.expression(New(
			KTriggerProperties,
			"table", table,
			"timing", timing,
			"events", events,
			"execute", execute,
			"constraint", isConstraint,
			"referenced_table", referencedTable,
			"deferrable", chunkAStrOrNil(deferrable),
			"initially", chunkAStrOrNil(initially),
			"referencing", referencing,
			"for_each", chunkAStrOrNil(forEach),
			"when", when,
		))

		this = triggerName
		triggerPropsList := []*Expr{}
		if triggerProps != nil {
			triggerPropsList = []*Expr{triggerProps}
		}
		extendProps(New(KProperties, "expressions", triggerPropsList))
	} else if createTokenType == TK_TYPE {
		this = p.parseTableParts(true, false, false, false)
		if this == nil || !p.match(TK_ALIAS) {
			return p.parseAsCommand(start)
		}

		if p.match(TK_ENUM) {
			expression = New(
				KDataType,
				"this", DT_ENUM,
				"expressions", p.parseWrappedCSV(p.parseString, TK_COMMA, false),
			)
		} else if p.matchNoAdvance(TK_L_PAREN) {
			expression = p.parseSchema(nil)
		} else {
			return p.parseAsCommand(start)
		}
	} else if p.s.DB_CREATABLES.Has(createTokenType) {
		tableParts := p.parseTableParts(true, createTokenType == TK_SCHEMA, false, false)

		// exp.Properties.Location.POST_NAME
		p.match(TK_COMMA)
		extendProps(p.parseProperties(true))

		this = p.parseSchema(tableParts)

		// exp.Properties.Location.POST_SCHEMA and POST_WITH
		extendProps(p.parseProperties(false))

		hasAlias := p.match(TK_ALIAS)
		if !p.matchSetNoAdvance(p.s.DDL_SELECT_TOKENS) {
			// exp.Properties.Location.POST_ALIAS
			extendProps(p.parseProperties(false))
		}

		if createTokenType == TK_SEQUENCE {
			expression = p.parseTypes(false, false, true, false)
			props := p.parseProperties(false)
			if props != nil {
				sequenceProps := New(KSequenceProperties)
				var options []*Expr
				// `for prop in props` iterates the live expressions list while prop.pop() removes
				// items from it, so re-read the list on every step (like Python's list iterator).
				for i := 0; i < len(props.Expressions()); i++ {
					prop := props.Expressions()[i]
					if prop.IsA(KSequenceProperties) {
						for _, arg := range prop.ArgKeys() {
							value := prop.Arg(arg)
							if arg == "options" {
								options = append(options, prop.ArgL("options")...)
							} else {
								sequenceProps.Set(arg, value)
							}
						}
						prop.Pop()
					}
				}

				if len(options) > 0 {
					sequenceProps.Set("options", options)
				}

				props.Append("expressions", sequenceProps)
				extendProps(props)
			}
		} else {
			expression = p.parseDdlSelect()

			// Some dialects also support using a table as an alias instead of a SELECT.
			// Here we fallback to this as an alternative.
			if expression == nil && hasAlias {
				expression = p.tryParseExpr(func() *Expr { return p.parseTableParts(false, false, false, false) }, false)
			}
		}

		if createTokenType == TK_TABLE {
			// exp.Properties.Location.POST_EXPRESSION
			extendProps(p.parseProperties(false))

			indexList := []*Expr{}
			for {
				index := p.parseIndex(nil, false)

				// exp.Properties.Location.POST_INDEX
				extendProps(p.parseProperties(false))
				if index == nil {
					break
				}
				p.match(TK_COMMA)
				indexList = append(indexList, index)
			}
			indexes = indexList
		} else if createTokenType == TK_VIEW {
			if p.matchTextSeq("WITH", "NO", "SCHEMA", "BINDING") {
				noSchemaBinding = true
			}
		} else if createTokenType == TK_SINK || createTokenType == TK_SOURCE {
			extendProps(p.parseProperties(false))
		}

		shallow := p.matchTextSeq("SHALLOW")

		if p.matchTextSet(p.s.CLONE_KEYWORDS) {
			isCopy := pyLower(p.prev.Text) == "copy"
			clone = p.expression(New(
				KClone,
				"this", p.parseTable(true, false, nil, false, false, false, false),
				"shallow", shallow,
				"copy", isCopy,
			))
		}
	}

	if p.curr.ok() && !p.matchAnyNoAdvance(TK_R_PAREN, TK_COMMA) {
		return p.parseAsCommand(start)
	}

	createKindText := upperText(createToken)
	kind := p.d.S.CREATABLE_KIND_MAPPING[createKindText]
	if kind == "" {
		kind = createKindText
	}
	return p.expression(New(
		KCreate,
		"this", this,
		"kind", kind,
		"replace", replace,
		"refresh", refresh,
		"unique", unique,
		"expression", expression,
		"exists", exists,
		"properties", properties,
		"indexes", indexes,
		"no_schema_binding", noSchemaBinding,
		"begin", begin,
		"clone", clone,
		"concurrently", concurrently,
		"clustered", clustered,
	))
}

// _parse_sequence_properties (parser.py L2673)
func (p *Parser) parseSequenceProperties() *Expr {
	seq := New(KSequenceProperties)

	options := []*Expr{}
	index := p.index

	for p.curr.ok() {
		p.match(TK_COMMA)
		if p.matchTextSeq("INCREMENT") {
			p.matchTextSeq("BY")
			p.matchTextSeq("=")
			seq.Set("increment", p.parseTerm())
		} else if p.matchTextSeq("MINVALUE") {
			seq.Set("minvalue", p.parseTerm())
		} else if p.matchTextSeq("MAXVALUE") {
			seq.Set("maxvalue", p.parseTerm())
		} else if p.match(TK_START_WITH) || p.matchTextSeq("START") {
			p.matchTextSeq("=")
			seq.Set("start", p.parseTerm())
		} else if p.matchTextSeq("CACHE") {
			// T-SQL allows empty CACHE which is initialized dynamically
			if n := p.parseNumber(); n != nil {
				seq.Set("cache", n)
			} else {
				seq.Set("cache", true)
			}
		} else if p.matchTextSeq("OWNED", "BY") {
			// "OWNED BY NONE" is the default
			if p.matchTextSeq("NONE") {
				seq.Set("owned", nil)
			} else {
				seq.Set("owned", p.parseColumn())
			}
		} else {
			opt := p.parseVarFromOptions(p.s.CREATE_SEQUENCE, false)
			if opt != nil {
				options = append(options, opt)
			} else {
				break
			}
		}
	}

	if len(options) > 0 {
		seq.Set("options", options)
	} else {
		seq.Set("options", nil)
	}
	if p.index == index {
		return nil
	}
	return seq
}

// _parse_trigger_events (parser.py L2708)
func (p *Parser) parseTriggerEvents() []*Expr {
	events := []*Expr{}

	for {
		// self._match_set(self.TRIGGER_EVENTS) and self._prev.text.upper() -> False when unmatched
		var eventType any = false
		if p.matchSet(p.s.TRIGGER_EVENTS) {
			eventType = upperText(p.prev)
		}

		if eventType == false {
			p.raiseError("Expected trigger event (INSERT, UPDATE, DELETE, TRUNCATE)", nil)
		}

		var columns any
		if eventType == "UPDATE" && p.matchTextSeq("OF") {
			columns = p.parseCSV(p.parseColumn, TK_COMMA)
		}

		events = append(events, p.expression(New(KTriggerEvent, "this", eventType, "columns", columns)))

		if !p.match(TK_OR) {
			break
		}
	}

	return events
}

// _parse_trigger_deferrable (parser.py L2730)
func (p *Parser) parseTriggerDeferrable() (string, string) {
	deferrableVar := p.parseVarFromOptions(p.s.TRIGGER_DEFERRABLE, false)
	deferrable := ""
	if deferrableVar != nil {
		deferrable = deferrableVar.ThisS()
	}

	initially := ""
	if deferrable != "" && p.matchTextSeq("INITIALLY") {
		if p.matchTexts("IMMEDIATE", "DEFERRED") {
			initially = upperText(p.prev)
		}
	}

	return deferrable, initially
}

// _parse_trigger_referencing_clause (parser.py L2746)
func (p *Parser) parseTriggerReferencingClause(keyword string) *Expr {
	if !p.matchTextSeq(keyword) {
		return nil
	}
	if !p.matchTextSeq("TABLE") {
		p.raiseError(fmt.Sprintf("Expected TABLE after %s in REFERENCING clause", keyword), nil)
	}
	p.matchTextSeq("AS")
	return p.parseIdVar(true, nil)
}

// _parse_trigger_referencing (parser.py L2754)
func (p *Parser) parseTriggerReferencing() *Expr {
	if !p.matchTextSeq("REFERENCING") {
		return nil
	}

	var oldAlias, newAlias *Expr

	for {
		if alias := p.parseTriggerReferencingClause("OLD"); alias != nil {
			if oldAlias != nil {
				p.raiseError("Duplicate OLD clause in REFERENCING", nil)
			}
			oldAlias = alias
		} else if alias := p.parseTriggerReferencingClause("NEW"); alias != nil {
			if newAlias != nil {
				p.raiseError("Duplicate NEW clause in REFERENCING", nil)
			}
			newAlias = alias
		} else {
			break
		}
	}

	if oldAlias == nil && newAlias == nil {
		p.raiseError("REFERENCING clause requires at least OLD TABLE or NEW TABLE", nil)
	}

	return p.expression(New(KTriggerReferencing, "old", oldAlias, "new", newAlias))
}

// _parse_trigger_for_each (parser.py L2778)
func (p *Parser) parseTriggerForEach() string {
	if !p.matchTextSeq("FOR", "EACH") {
		return ""
	}

	if p.matchTexts("ROW", "STATEMENT") {
		return upperText(p.prev)
	}
	return ""
}

// _parse_trigger_execute (parser.py L2784)
func (p *Parser) parseTriggerExecute() *Expr {
	if !p.match(TK_EXECUTE) {
		return nil
	}

	if !p.matchAny(TK_FUNCTION, TK_PROCEDURE) {
		p.raiseError("Expected FUNCTION or PROCEDURE after EXECUTE", nil)
	}

	funcCall := p.parseColumn()
	return p.expression(New(KTriggerExecute, "this", funcCall))
}

// chunkAStrOrNil maps an optional string (Python `str | None`, "" meaning None) to an arg value.
func chunkAStrOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
