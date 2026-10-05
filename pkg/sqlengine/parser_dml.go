package sqlengine

// Port of sqlglot/parser.py L3443-3797 (chunk B, part 1): DESCRIBE, multitable inserts, INSERT,
// KILL, ON CONFLICT, RETURNING, row format / serde, LOAD, DELETE, UPDATE, USE, CACHE / UNCACHE.

// chunkBAndE mirrors the Python idiom `cond and parse()`: returns False when cond is false,
// otherwise the parsed expression (None when the parse method returns None).
func chunkBAndE(cond bool, fn func() *Expr) any {
	if !cond {
		return false
	}
	if e := fn(); e != nil {
		return e
	}
	return nil
}

// chunkBKwargs mirrors an insertion-ordered kwargs dict (re-assigning a key keeps its position).
type chunkBKwargs struct{ kv []any }

func (k *chunkBKwargs) set(key string, v any) {
	for i := 0; i+1 < len(k.kv); i += 2 {
		if k.kv[i] == key {
			k.kv[i+1] = v
			return
		}
	}
	k.kv = append(k.kv, key, v)
}

// _parse_describe (parser.py L3443).
func (p *Parser) baseParseDescribe() *Expr {
	var kind any
	if p.matchSet(p.s.CREATABLES) {
		kind = p.prev.Text
	}
	var style any
	if p.matchTextSet(p.s.DESCRIBE_STYLES) {
		style = upperText(p.prev)
	}
	if p.match(TK_DOT) {
		style = nil
		p.retreat(p.index - 2)
	}

	var format any
	if p.matchNoAdvance(TK_FORMAT) {
		format = p.parseProperty()
	}

	var this *Expr
	if _, ok := p.s.STATEMENT_PARSERS[p.curr.Type]; ok {
		this = p.parseStatement()
	} else {
		this = p.parseTable(true, false, nil, false, false, false, false)
	}

	properties := p.parseProperties(false)
	var expressions any
	if properties != nil {
		expressions = properties.Expressions()
	}
	partition := p.parsePartition()
	asJSON := p.matchTextSeq("AS", "JSON")
	return p.expression(New(
		KDescribe,
		"this", this,
		"style", style,
		"kind", kind,
		"expressions", expressions,
		"partition", partition,
		"format", format,
		"as_json", asJSON,
	))
}

// _parse_multitable_inserts (parser.py L3474).
func (p *Parser) parseMultitableInserts(comments []string) *Expr {
	kind := upperText(p.prev)
	expressions := []*Expr{}

	parseConditionalInsert := func() *Expr {
		var expression *Expr
		if p.match(TK_WHEN) {
			expression = p.parseDisjunction()
			p.match(TK_THEN)
		}

		else_ := p.match(TK_ELSE)

		if !p.match(TK_INTO) {
			return nil
		}

		insThis := p.parseTable(true, false, nil, false, false, false, false)
		insExpression := p.parseDerivedTableValues()
		insert := p.expression(New(KInsert, "this", insThis, "expression", insExpression))
		return p.expression(New(
			KConditionalInsert,
			"this", insert,
			"expression", expression,
			"else_", else_,
		))
	}

	expression := parseConditionalInsert()
	for expression != nil {
		expressions = append(expressions, expression)
		expression = parseConditionalInsert()
	}

	source := p.parseTable(false, false, nil, false, false, false, false)
	return p.expressionC(
		New(KMultitableInserts, "kind", kind, "expressions", expressions, "source", source),
		comments,
	)
}

// _parse_insert (parser.py L3513).
func (p *Parser) parseInsert() *Expr {
	comments := []string{}
	hint := p.parseHint()
	overwrite := p.match(TK_OVERWRITE)
	ignore := p.match(TK_IGNORE)
	local := p.matchTextSeq("LOCAL")
	var alternative any
	var isFunction any

	var this *Expr
	if p.matchTextSeq("DIRECTORY") {
		dirThis := p.parseVarOrString(false)
		rowFormat := p.parseRowFormat(true)
		this = p.expression(New(
			KDirectory,
			"this", dirThis,
			"local", local,
			"row_format", rowFormat,
		))
	} else {
		if p.matchAny(TK_FIRST, TK_ALL) {
			comments = append(comments, p.prevComments...)
			return p.parseMultitableInserts(comments)
		}

		if p.match(TK_OR) {
			// alternative = self._match_texts(self.INSERT_ALTERNATIVES) and self._prev.text
			if p.matchTextSet(p.s.INSERT_ALTERNATIVES) {
				alternative = p.prev.Text
			} else {
				alternative = false
			}
		}

		p.match(TK_INTO)
		comments = append(comments, p.prevComments...)
		p.match(TK_TABLE)
		isFn := p.match(TK_FUNCTION)
		isFunction = isFn

		if isFn {
			this = p.parseFunction(nil, false, true, false)
		} else {
			this = p.parseInsertTable()
		}
	}

	returning := p.parseReturning() // TSQL allows RETURNING before source

	stored := chunkBAndE(p.matchTextSeq("STORED"), p.parseStored)
	byName := p.matchTextSeq("BY", "NAME")
	exists := p.parseExists(false)
	where := chunkBAndE(p.matchPair(TK_REPLACE, TK_WHERE), p.parseDisjunction)
	partition := chunkBAndE(p.match(TK_PARTITION_BY), p.parsePartitionedBy)
	settings := chunkBAndE(p.matchTextSeq("SETTINGS"), p.parseSettingsProperty)
	default_ := p.matchTextSeq("DEFAULT", "VALUES")
	expression := p.parseDerivedTableValues()
	if expression == nil {
		expression = p.parseDdlSelect()
	}
	conflict := p.parseOnConflict()
	if returning == nil {
		returning = p.parseReturning()
	}
	source := chunkBAndE(p.match(TK_TABLE), func() *Expr {
		return p.parseTable(false, false, nil, false, false, false, false)
	})

	return p.expressionC(New(
		KInsert,
		"hint", hint,
		"is_function", isFunction,
		"this", this,
		"stored", stored,
		"by_name", byName,
		"exists", exists,
		"where", where,
		"partition", partition,
		"settings", settings,
		"default", default_,
		"expression", expression,
		"conflict", conflict,
		"returning", returning,
		"overwrite", overwrite,
		"alternative", alternative,
		"ignore", ignore,
		"source", source,
	), comments)
}

// _parse_insert_table (parser.py L3571).
func (p *Parser) baseParseInsertTable() *Expr {
	this := p.parseTable(true, false, nil, false, false, true, false)
	if this.IsA(KTable) && p.matchNoAdvance(TK_ALIAS) {
		this.Set("alias", p.parseTableAlias(nil))
	}
	return this
}

// _parse_kill (parser.py L3577).
func (p *Parser) parseKill() *Expr {
	var kind *Expr
	if p.matchTexts("CONNECTION", "QUERY") {
		kind = VarExpr(p.prev.Text)
	}

	return p.expression(New(KKill, "this", p.parsePrimary(), "kind", kind))
}

// _parse_on_conflict (parser.py L3582).
func (p *Parser) parseOnConflict() *Expr {
	conflict := p.matchTextSeq("ON", "CONFLICT")
	duplicate := p.matchTextSeq("ON", "DUPLICATE", "KEY")

	if !conflict && !duplicate {
		return nil
	}

	var conflictKeys any
	var constraint *Expr

	if conflict {
		if p.matchTextSeq("ON", "CONSTRAINT") {
			constraint = p.parseIdVar(true, nil)
		} else if p.match(TK_L_PAREN) {
			conflictKeys = p.parseCSV(p.parseIndexedColumn, TK_COMMA)
			p.matchRParen(nil)
		}
	}

	indexPredicate := p.parseWhere(false)

	action := p.parseVarFromOptions(p.s.CONFLICT_ACTIONS, true)
	var expressions any
	if p.prev.Type == TK_UPDATE {
		p.match(TK_SET)
		expressions = p.parseCSV(p.parseEquality, TK_COMMA)
	}

	where := p.parseWhere(false)
	return p.expression(New(
		KOnConflict,
		"duplicate", duplicate,
		"expressions", expressions,
		"action", action,
		"conflict_keys", conflictKeys,
		"index_predicate", indexPredicate,
		"constraint", constraint,
		"where", where,
	))
}

// _parse_returning (parser.py L3620).
func (p *Parser) parseReturning() *Expr {
	if !p.match(TK_RETURNING) {
		return nil
	}
	expressions := p.parseCSV(p.parseExpression, TK_COMMA)
	into := chunkBAndE(p.match(TK_INTO), func() *Expr { return p.parseTablePart(false) })
	return p.expression(New(KReturning, "expressions", expressions, "into", into))
}

// _parse_row (parser.py L3630).
func (p *Parser) parseRow() *Expr {
	if !p.match(TK_FORMAT) {
		return nil
	}
	return p.parseRowFormat(false)
}

// _parse_serde_properties (parser.py L3635).
func (p *Parser) parseSerdeProperties(with bool) *Expr {
	index := p.index
	with = with || p.matchTextSeq("WITH")

	if !p.match(TK_SERDE_PROPERTIES) {
		p.retreat(index)
		return nil
	}
	return p.expression(New(
		KSerdeProperties,
		"expressions", p.parseWrappedProperties(),
		"with_", with,
	))
}

// _parse_row_format (parser.py L3646).
func (p *Parser) parseRowFormat(matchRow bool) *Expr {
	if matchRow && !p.matchPair(TK_ROW, TK_FORMAT) {
		return nil
	}

	if p.matchTextSeq("SERDE") {
		this := p.parseString()

		serdeProperties := p.parseSerdeProperties(false)

		return p.expression(New(
			KRowFormatSerdeProperty,
			"this", this,
			"serde_properties", serdeProperties,
		))
	}

	p.matchTextSeq("DELIMITED")

	kwargs := []any{}

	if p.matchTextSeq("FIELDS", "TERMINATED", "BY") {
		kwargs = append(kwargs, "fields", p.parseString())
		if p.matchTextSeq("ESCAPED", "BY") {
			kwargs = append(kwargs, "escaped", p.parseString())
		}
	}
	if p.matchTextSeq("COLLECTION", "ITEMS", "TERMINATED", "BY") {
		kwargs = append(kwargs, "collection_items", p.parseString())
	}
	if p.matchTextSeq("MAP", "KEYS", "TERMINATED", "BY") {
		kwargs = append(kwargs, "map_keys", p.parseString())
	}
	if p.matchTextSeq("LINES", "TERMINATED", "BY") {
		kwargs = append(kwargs, "lines", p.parseString())
	}
	if p.matchTextSeq("NULL", "DEFINED", "AS") {
		kwargs = append(kwargs, "null", p.parseString())
	}

	return p.expression(New(KRowFormatDelimitedProperty, kwargs...))
}

// _parse_load (parser.py L3680).
func (p *Parser) parseLoad() *Expr {
	if p.matchTextSeq("DATA") {
		local := p.matchTextSeq("LOCAL")
		p.matchTextSeq("INPATH")
		inpath := p.parseString()
		overwrite := p.match(TK_OVERWRITE)
		var temp any
		if p.match(TK_INTO) {
			temp = p.match(TK_TEMPORARY)
			p.match(TK_TABLE)
		}

		this := p.parseTable(true, false, nil, false, false, false, false)
		var files any = false
		if p.matchTextSeq("FROM", "FILES") {
			files = New(KProperties, "expressions", p.parseWrappedProperties())
		}
		partition := p.parsePartition()
		inputFormat := chunkBAndE(p.matchTextSeq("INPUTFORMAT"), p.parseString)
		serde := chunkBAndE(p.matchTextSeq("SERDE"), p.parseString)

		return p.expression(New(
			KLoadData,
			"this", this,
			"local", local,
			"overwrite", overwrite,
			"temp", temp,
			"inpath", inpath,
			"files", files,
			"partition", partition,
			"input_format", inputFormat,
			"serde", serde,
		))
	}
	return p.parseAsCommand(p.prev)
}

// _parse_delete (parser.py L3707).
func (p *Parser) parseDelete() *Expr {
	hint := p.parseHint()

	// This handles MySQL's "Multiple-Table Syntax"
	// https://dev.mysql.com/doc/refman/8.0/en/delete.html
	var tables any
	if !p.matchNoAdvance(TK_FROM) {
		if ts := p.parseCSV(func() *Expr {
			return p.parseTable(false, false, nil, false, false, false, false)
		}, TK_COMMA); len(ts) > 0 {
			tables = ts
		}
	}

	returning := p.parseReturning()

	this := chunkBAndE(p.match(TK_FROM), func() *Expr {
		return p.parseTable(false, true, nil, false, false, false, false)
	})
	var using any = false
	if p.match(TK_USING) {
		using = p.parseCSV(func() *Expr {
			return p.parseTable(false, true, nil, false, false, false, false)
		}, TK_COMMA)
	}
	cluster := chunkBAndE(p.match(TK_ON), p.parseOnProperty)
	where := p.parseWhere(false)
	if returning == nil {
		returning = p.parseReturning()
	}
	order := p.parseOrder(nil, false)
	limit := p.parseLimit(nil, false, false)

	return p.expression(New(
		KDelete,
		"hint", hint,
		"tables", tables,
		"this", this,
		"using", using,
		"cluster", cluster,
		"where", where,
		"returning", returning,
		"order", order,
		"limit", limit,
	))
}

// _parse_update (parser.py L3733).
func (p *Parser) baseParseUpdate() *Expr {
	hint := p.parseHint()
	kwargs := &chunkBKwargs{}
	kwargs.set("hint", hint)
	kwargs.set("this", p.parseTable(false, true, &p.s.UPDATE_ALIAS_TOKENS, false, false, false, false))
	for p.curr.ok() {
		if p.match(TK_SET) {
			kwargs.set("expressions", p.parseCSV(p.parseEquality, TK_COMMA))
		} else if p.matchNoAdvance(TK_RETURNING) {
			kwargs.set("returning", p.parseReturning())
		} else if p.matchNoAdvance(TK_FROM) {
			from := p.parseFrom(true, false, false)
			var table *Expr
			if from != nil {
				table = from.This()
			}
			if table.IsA(KSubquery) && p.matchNoAdvance(TK_JOIN) {
				var joins []*Expr
				for join := range p.parseJoins(nil) {
					joins = append(joins, join)
				}
				if len(joins) > 0 {
					table.Set("joins", joins)
				} else {
					table.Set("joins", nil)
				}
			}

			kwargs.set("from_", from)
		} else if p.matchNoAdvance(TK_WHERE) {
			kwargs.set("where", p.parseWhere(false))
		} else if p.matchNoAdvance(TK_ORDER_BY) {
			kwargs.set("order", p.parseOrder(nil, false))
		} else if p.matchNoAdvance(TK_LIMIT) {
			kwargs.set("limit", p.parseLimit(nil, false, false))
		} else {
			break
		}
	}

	return p.expression(New(KUpdate, kwargs.kv...))
}

// _parse_use (parser.py L3762).
func (p *Parser) baseParseUse() *Expr {
	kind := p.parseVarFromOptions(p.s.USABLES, false)
	this := p.parseTable(false, false, nil, false, false, false, false)
	return p.expression(New(KUse, "kind", kind, "this", this))
}

// _parse_uncache (parser.py L3770).
func (p *Parser) parseUncache() *Expr {
	if !p.match(TK_TABLE) {
		p.raiseError("Expecting TABLE after UNCACHE", nil)
	}

	exists := p.parseExists(false)
	this := p.parseTable(true, false, nil, false, false, false, false)
	return p.expression(New(KUncache, "exists", exists, "this", this))
}

// _parse_cache (parser.py L3778).
func (p *Parser) parseCache() *Expr {
	lazy := p.matchTextSeq("LAZY")
	p.match(TK_TABLE)
	table := p.parseTable(true, false, nil, false, false, false, false)

	options := []*Expr{}
	if p.matchTextSeq("OPTIONS") {
		p.matchLParen(nil)
		k := p.parseString()
		p.match(TK_EQ)
		v := p.parseString()
		options = []*Expr{k, v}
		p.matchRParen(nil)
	}

	p.match(TK_ALIAS)
	expression := p.parseSelect(true, false, true, true, true, nil)
	return p.expression(New(
		KCache,
		"this", table,
		"lazy", lazy,
		"options", options,
		"expression", expression,
	))
}
