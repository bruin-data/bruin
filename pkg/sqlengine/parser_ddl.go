package sqlengine

import "fmt"

func (p *Parser) parsePartitionedByBucketOrTruncate() *Expr {
	if !p.matchNoAdvance(TK_L_PAREN) {
		// Without a parenthesis, BUCKET / TRUNCATE is an identifier, not a partition transform.
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

func (p *Parser) parseCommand() *Expr {
	p.warnUnsupported()
	comments := p.prevComments
	this := upperText(p.prev)
	return p.expressionC(New(KCommand, "this", this, "expression", p.parseString()), comments)
}

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

func (p *Parser) parseToTable() *Expr {
	table := p.parseTableParts(true, false, false, false)
	return p.expression(New(KToTableProperty, "this", table))
}

// parseTtl parses a ClickHouse TTL clause.
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

func (p *Parser) parseCondition() *Expr {
	return p.parseWrapped(p.parseExpression, true)
}

func (p *Parser) parseBlock() *Expr {
	return p.expression(New(
		KBlock,
		"expressions", p.parseBatchStatements(func(p *Parser) *Expr { return p.parseStatement() }, true),
	))
}

func (p *Parser) parseWhileblock() *Expr {
	return p.expression(New(KWhileBlock, "this", p.parseCondition(), "body", p.parseBlock()))
}

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

	if p.isTableQuery() {
		return p.parseSelect(false, false, true, true, true, nil)
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

// parseDrop takes a non-empty kind as the already-known kind of the dropped object, which then
// follows DROP directly.
func (p *Parser) parseDrop(exists bool, kind string) *Expr {
	start := p.prev
	temporary, materialized, iceberg := false, false, false
	if kind == "" {
		temporary = p.match(TK_TEMPORARY)
		materialized = p.matchTextSeq("MATERIALIZED")
		iceberg = p.matchTextSeq("ICEBERG")
		if p.matchSet(p.s.CREATABLES) {
			kind = upperText(p.prev)
		}
	}
	if kind == "" || (iceberg && kind != "TABLE") {
		return p.parseAsCommand(start)
	}

	// CONCURRENTLY is only a modifier when an index name follows it
	concurrently := kind == "INDEX" && p.s.ID_VAR_TOKENS.Has(p.next.Type) && p.matchTextSeq("CONCURRENTLY")
	ifExists := exists || p.parseExists(false)

	var this *Expr
	if kind == "COLUMN" {
		// A dropped column named after a keyword, like `any` or `charset`, is only a name
		var name *Expr
		if _, ok := p.s.NO_PAREN_FUNCTION_PARSERS[upperText(p.curr)]; !ok {
			name = p.parseColumn()
		}
		if name == nil {
			name = p.parseIdVar(true, nil)
		}
		if name.IsA(KIdentifier) {
			name = p.expression(New(KColumn, "this", name))
		}
		this = name
	} else {
		this = p.parseTableParts(true, kind == "SCHEMA", false, false)
	}

	// DROP TABLE and DROP VIEW take a list: the first name is this, the rest are tables. Other
	// kinds stop at the comma, which separates ALTER actions such as DROP COLUMN a, DROP COLUMN b.
	var tables any
	if (kind == "TABLE" || kind == "VIEW") && p.match(TK_COMMA) {
		tables = p.parseCSV(func() *Expr { return p.parseTableParts(true, false, false, false) }, TK_COMMA)
	}

	var cluster *Expr
	if p.match(TK_ON) {
		if kind == "INDEX" || kind == "TRIGGER" {
			// DROP INDEX i ON [db.]t, DROP TRIGGER t ON [db.]t
			cluster = p.expression(New(KOnProperty, "this", p.parseTableParts(false, false, false, false)))
		} else {
			cluster = p.parseOnProperty()
		}
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

	// Match CONSTRAINTS, PURGE, SYNC and FORCE in this order, before building the node.
	constraints := p.matchTextSeq("CONSTRAINTS")
	purge := p.matchTextSeq("PURGE")
	sync := p.matchTextSeq("SYNC")
	force := p.matchTextSeq("FORCE")

	return p.expression(New(
		KDrop,
		"exists", ifExists,
		"this", this,
		"tables", tables,
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
		"force", force,
	))
}

func (p *Parser) parseExists(not bool) bool {
	if not {
		return p.matchTextSeq("IF", "NOT", "EXISTS")
	}
	return p.matchTextSeq("IF", "EXISTS")
}

func (p *Parser) baseParseCreate() *Expr {
	// Note: this can't be nil because we've matched a statement parser
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
		// Properties at Loc_POST_CREATE
		properties = p.parseProperties(false)
		if p.matchSet(p.s.CREATABLES) {
			createToken = p.prev
		}

		if properties == nil || createToken == nil {
			return p.parseAsCommand(start)
		}
	}

	createTokenType := createToken.Type

	// CONCURRENTLY only modifies CREATE INDEX; elsewhere it can be a name (CREATE TABLE concurrently)
	concurrently := createTokenType == TK_INDEX && p.matchTextSeq("CONCURRENTLY")
	exists := p.parseExists(true)
	var this *Expr
	var expression *Expr
	var indexes any
	var noSchemaBinding any
	var begin any
	var clone *Expr

	extendProps := func(tempProps *Expr) {
		if properties != nil && tempProps != nil {
			// Append to the existing list without parent bookkeeping; when properties has no
			// expressions yet, nothing is added.
			if existing := properties.Expressions(); len(existing) > 0 {
				properties.SetArgRaw("expressions", append(existing, tempProps.Expressions()...))
			}
		} else if tempProps != nil {
			properties = tempProps
		}
	}

	if createTokenType == TK_FUNCTION || createTokenType == TK_PROCEDURE {
		this = p.parseUserDefinedFunction(createTokenType)

		// Properties at Loc_POST_SCHEMA ("schema" here is the UDF's type signature)
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
					// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
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
		// false (not nil) when WHEN is absent
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

		// Properties at Loc_POST_NAME
		p.match(TK_COMMA)
		extendProps(p.parseProperties(true))

		this = p.parseSchema(tableParts)

		// Properties at Loc_POST_SCHEMA and Loc_POST_WITH
		extendProps(p.parseProperties(false))

		hasAlias := p.match(TK_ALIAS)
		if !p.matchSetNoAdvance(p.s.DDL_SELECT_TOKENS) {
			// Properties at Loc_POST_ALIAS
			extendProps(p.parseProperties(false))
		}

		if createTokenType == TK_SEQUENCE {
			expression = p.parseTypes(false, false, true, false)
			props := p.parseProperties(false)
			if props != nil {
				sequenceProps := New(KSequenceProperties)
				var options []*Expr
				// Iterate the live expressions list, re-reading it on every step while prop.Pop()
				// removes items from it: the item right after a popped one is skipped.
				for i := 0; i < len(props.Expressions()); i++ { //nolint:intrange // the list shrinks while iterating
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
			// Properties at Loc_POST_EXPRESSION
			extendProps(p.parseProperties(false))

			indexList := []*Expr{}
			for {
				index := p.parseIndex(nil, false)

				// Properties at Loc_POST_INDEX
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

func (p *Parser) parseSequenceProperties() *Expr {
	// The node is created on first use: most calls match nothing and return nil.
	var seq *Expr

	options := []*Expr{}
	index := p.index

	for p.curr.ok() {
		p.match(TK_COMMA)
		if p.matchTextSeq("INCREMENT") {
			p.matchTextSeq("BY")
			p.matchTextSeq("=")
			lazySet(&seq, KSequenceProperties, "increment", p.parseTerm())
		} else if p.matchTextSeq("MINVALUE") {
			lazySet(&seq, KSequenceProperties, "minvalue", p.parseTerm())
		} else if p.matchTextSeq("MAXVALUE") {
			lazySet(&seq, KSequenceProperties, "maxvalue", p.parseTerm())
		} else if p.matchTextSeq("START") {
			p.matchTextSeq("WITH")
			p.matchTextSeq("=")
			lazySet(&seq, KSequenceProperties, "start", p.parseTerm())
		} else if p.matchTextSeq("CACHE") {
			// T-SQL allows empty CACHE which is initialized dynamically
			if n := p.parseNumber(); n != nil {
				lazySet(&seq, KSequenceProperties, "cache", n)
			} else {
				lazySet(&seq, KSequenceProperties, "cache", true)
			}
		} else if p.matchTextSeq("OWNED", "BY") {
			// "OWNED BY NONE" is the default
			if p.matchTextSeq("NONE") {
				lazySet(&seq, KSequenceProperties, "owned", nil)
			} else {
				lazySet(&seq, KSequenceProperties, "owned", p.parseColumn())
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

	if p.index == index {
		return nil
	}
	if len(options) > 0 {
		lazySet(&seq, KSequenceProperties, "options", options)
	} else {
		lazySet(&seq, KSequenceProperties, "options", nil)
	}
	return seq
}

func lazySet(e **Expr, kind Kind, key string, v any) {
	if *e == nil {
		*e = New(kind)
	}
	(*e).Set(key, v)
}

func (p *Parser) parseTriggerEvents() []*Expr {
	events := []*Expr{}

	for {
		// false (not a string) when no trigger event matches
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

func (p *Parser) parseTriggerForEach() string {
	if !p.matchTextSeq("FOR", "EACH") {
		return ""
	}

	if p.matchTexts("ROW", "STATEMENT") {
		return upperText(p.prev)
	}
	return ""
}

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

func chunkAStrOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
