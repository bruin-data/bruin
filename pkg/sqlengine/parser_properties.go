package sqlengine

import "fmt"

func (p *Parser) baseParsePropertyBefore() any {
	// only used for teradata currently
	p.match(TK_COMMA)

	var kw propKwargs
	kw.no = p.matchTextSeq("NO")
	kw.dual = p.matchTextSeq("DUAL")
	kw.before = p.matchTextSeq("BEFORE")
	kw.default_ = p.matchTextSeq("DEFAULT")
	if p.matchTextSeq("LOCAL") {
		kw.local = "LOCAL"
	} else if p.matchTextSeq("NOT", "LOCAL") {
		kw.local = "NOT LOCAL"
	}
	kw.after = p.matchTextSeq("AFTER")
	kw.minimum = p.matchTexts("MIN", "MINIMUM")
	kw.maximum = p.matchTexts("MAX", "MAXIMUM")

	if matchTextKeys(p, p.s.PROPERTY_PARSERS) {
		parser := p.s.PROPERTY_PARSERS[upperText(p.prev)]
		// Only set modifiers are passed (propKwargs zero values mean "not passed").
		return p.callPropertyParser(parser, kw)
	}

	// CHARACTER SET is two tokens, so it is matched here rather than in PROPERTY_PARSERS
	if p.matchPair(TK_CHAR, TK_SET) {
		return anyExpr(p.parseCharacterSet(kw.default_))
	}

	return nil
}

func (p *Parser) parseWrappedProperties() []*Expr {
	items := parseWrappedAny(p, func() []any {
		return parseCSVAny(p, p.parseProperty, TK_COMMA, chunkAIsNone)
	}, false)
	out := []*Expr{}
	for _, item := range items {
		switch v := item.(type) {
		case []*Expr:
			out = append(out, v...)
		case *Expr:
			out = append(out, v)
		}
	}
	return out
}

// baseParseProperty returns nil, an *Expr or a []*Expr.
func (p *Parser) baseParseProperty() any {
	if matchTextKeys(p, p.s.PROPERTY_PARSERS) {
		return chunkANormAny(p.s.PROPERTY_PARSERS[upperText(p.prev)](p, propKwargs{}))
	}

	if p.matchPair(TK_CHAR, TK_SET) {
		return anyExpr(p.parseCharacterSet(false))
	}

	if p.match(TK_DEFAULT) {
		if matchTextKeys(p, p.s.PROPERTY_PARSERS) {
			return p.callPropertyParser(p.s.PROPERTY_PARSERS[upperText(p.prev)], propKwargs{default_: true})
		}

		if p.matchPair(TK_CHAR, TK_SET) {
			return anyExpr(p.parseCharacterSet(true))
		}
	}

	if p.matchTextSeq("COMPOUND", "SORTKEY") {
		return p.parseSortkey(true)
	}

	if p.matchTextSeq("PARAMETER", "STYLE", "PANDAS") {
		return p.expression(New(KParameterStyleProperty, "this", "PANDAS"))
	}

	index := p.index

	if seqProps := p.parseSequenceProperties(); seqProps != nil {
		return seqProps
	}

	p.retreat(index)
	return chunkANormAny(p.parseKeyValueProperty(nil))
}

// parseKeyValueProperty parses key = value; nil (cursor restored) without the =. A nil parseValue
// parses a bitwise expression or a var.
func (p *Parser) parseKeyValueProperty(parseValue func() *Expr) *Expr {
	return p.parseKeyValuePropertyOpt(parseValue, false)
}

// parseKeyValuePropertyOpt is parseKeyValueProperty; with optional, the key may also be a string
// and the = may be omitted, as in 'key' value.
func (p *Parser) parseKeyValuePropertyOpt(parseValue func() *Expr, optional bool) *Expr {
	index := p.index
	var key *Expr
	if optional {
		key = p.parseString()
	}
	if key == nil {
		key = p.parseColumn()
	}

	if !p.match(TK_EQ) && !optional {
		p.retreat(index)
		return nil
	}

	if key.IsA(KColumn) {
		if len(key.Parts()) > 1 {
			key = key.ToDot(true)
		} else {
			key = chunkAVar(key.Name())
		}
	}

	var value *Expr
	if parseValue != nil {
		value = parseValue()
	} else {
		value = p.parseBitwise()
		if value == nil {
			value = p.parseVar(true, nil, false)
		}
	}

	if value.IsA(KColumn) {
		value = chunkAVar(value.Name())
	}

	return p.expression(New(KProperty, "this", key, "value", value))
}

func (p *Parser) parseStored() *Expr {
	if p.matchTextSeq("BY") {
		return p.expression(New(KStorageHandlerProperty, "this", p.parseVarOrString(false)))
	}

	p.match(TK_ALIAS)
	var inputFormat, outputFormat *Expr
	if p.matchTextSeq("INPUTFORMAT") {
		inputFormat = p.parseString()
	}
	if p.matchTextSeq("OUTPUTFORMAT") {
		outputFormat = p.parseString()
	}

	var this *Expr
	if inputFormat != nil || outputFormat != nil {
		this = p.expression(New(
			KInputOutputFormat,
			"input_format", inputFormat,
			"output_format", outputFormat,
		))
	} else {
		this = p.parseVarOrString(false)
		if this == nil {
			this = p.parseNumber()
		}
		if this == nil {
			this = p.parseIdVar(true, nil)
		}
	}

	return p.expression(New(KFileFormatProperty, "this", this, "hive_format", true))
}

func (p *Parser) parseUnquotedField() *Expr {
	field := p.parseField(false, nil, false)
	if field.IsA(KIdentifier) && !field.ArgB("quoted") {
		// VarExpr rather than chunkAVar: an empty name is not rejected here.
		field = p.expressionC(VarExpr(field.Name()), field.Comments())
	}

	return field
}

func (p *Parser) parsePropertyAssignment(expClass Kind, kv ...any) *Expr {
	p.match(TK_EQ)
	p.match(TK_ALIAS)

	args := append([]any{"this", p.parseUnquotedField()}, kv...)
	return p.expression(New(expClass, args...))
}

// parseProperties parses consecutive properties into a Properties node (nil when there are none);
// before selects parsePropertyBefore over parseProperty.
func (p *Parser) parseProperties(before bool) *Expr {
	properties := []*Expr{}
	for {
		var prop any
		if before {
			prop = p.parsePropertyBefore()
		} else {
			prop = p.parseProperty()
		}
		if !truthy(prop) {
			break
		}
		properties = append(properties, chunkAEnsureList(prop)...)

		p.matchExpr(TK_COMMA, properties[len(properties)-1])
	}

	if len(properties) > 0 {
		return p.expression(New(KProperties, "expressions", properties))
	}

	return nil
}

func (p *Parser) parseFallback(no bool) *Expr {
	return p.expression(New(KFallbackProperty, "no", no, "protection", p.matchTextSeq("PROTECTION")))
}

func (p *Parser) parseSqlSecurity() *Expr {
	// false (not nil) when no security keyword follows.
	var this any = false
	if p.matchTextSet(p.s.SECURITY_PROPERTY_KEYWORDS) {
		this = upperText(p.prev)
	}
	return p.expression(New(KSqlSecurityProperty, "this", this))
}

func (p *Parser) parseSettingsProperty() *Expr {
	return p.expression(New(KSettingsProperty, "expressions", p.parseCSV(p.parseAssignment, TK_COMMA)))
}

// parseCalledOnNullInputProperty parses CALLED ON NULL INPUT; otherwise it backs up over CALLED
// and returns nil.
func (p *Parser) parseCalledOnNullInputProperty() *Expr {
	if !p.matchTextSeq("ON", "NULL", "INPUT") {
		p.retreat(p.index - 1)
		return nil
	}

	return p.expression(New(KCalledOnNullInputProperty))
}

// parseVolatileProperty parses VOLATILE: a table property after a PRE_VOLATILE_TOKENS token, a
// function stability property otherwise.
func (p *Parser) parseVolatileProperty() *Expr {
	var preVolatileToken *Token
	if p.index >= 2 {
		preVolatileToken = p.tokens[p.index-2]
	}

	if preVolatileToken.ok() && p.s.PRE_VOLATILE_TOKENS.Has(preVolatileToken.Type) {
		return New(KVolatileProperty)
	}

	return p.expression(New(KStabilityProperty, "this", LiteralString("VOLATILE")))
}

func (p *Parser) parseRetentionPeriod() *Expr {
	// Parse TSQL's HISTORY_RETENTION_PERIOD: {INFINITE | <number> DAY | DAYS | MONTH ...}
	number := p.parseNumber()
	numberStr := ""
	if number != nil {
		numberStr = exprSQL(number) + " "
	}
	unit := p.parseVar(true, nil, false)
	unitStr := "None" // a missing unit renders as None
	if unit != nil {
		unitStr = exprSQL(unit)
	}
	return chunkAVar(numberStr + unitStr)
}

// parseSystemVersioningProperty parses SYSTEM_VERSIONING = ON [(options)] | OFF; with marks the
// WITH (SYSTEM_VERSIONING ...) form.
func (p *Parser) parseSystemVersioningProperty(with bool) *Expr {
	p.match(TK_EQ)
	prop := p.expression(New(KWithSystemVersioningProperty, "on", true, "with_", with))

	if p.matchTextSeq("OFF") {
		prop.Set("on", false)
		return prop
	}

	p.match(TK_ON)
	if p.match(TK_L_PAREN) {
		for p.curr.ok() && !p.match(TK_R_PAREN) {
			if p.matchTextSeq("HISTORY_TABLE", "=") {
				prop.Set("this", p.parseTableParts(false, false, false, false))
			} else if p.matchTextSeq("DATA_CONSISTENCY_CHECK", "=") {
				if p.advanceAny(false) != nil {
					prop.Set("data_consistency", upperText(p.prev))
				} else {
					prop.Set("data_consistency", nil)
				}
			} else if p.matchTextSeq("HISTORY_RETENTION_PERIOD", "=") {
				prop.Set("retention_period", p.parseRetentionPeriod())
			}

			p.match(TK_COMMA)
		}
	}

	return prop
}

func (p *Parser) parseDataDeletionProperty() *Expr {
	p.match(TK_EQ)
	on := p.matchTextSeq("ON") || !p.matchTextSeq("OFF")
	prop := p.expression(New(KDataDeletionProperty, "on", on))

	if p.match(TK_L_PAREN) {
		for p.curr.ok() && !p.match(TK_R_PAREN) {
			if p.matchTextSeq("FILTER_COLUMN", "=") {
				prop.Set("filter_column", p.parseColumn())
			} else if p.matchTextSeq("RETENTION_PERIOD", "=") {
				prop.Set("retention_period", p.parseRetentionPeriod())
			}

			p.match(TK_COMMA)
		}
	}

	return prop
}

func (p *Parser) parseDistributedProperty() *Expr {
	kind := "HASH"
	var expressions any
	if p.matchTextSeq("BY", "HASH") {
		expressions = p.parseWrappedCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA, false)
	} else if p.matchTextSeq("BY", "RANDOM") {
		kind = "RANDOM"
	}

	// If the BUCKETS keyword is not present, the number of buckets is AUTO
	var buckets *Expr
	if p.matchTextSeq("BUCKETS") && !p.matchTextSeq("AUTO") {
		buckets = p.parseNumber()
	}

	return p.expression(New(
		KDistributedByProperty,
		"expressions", expressions,
		"kind", kind,
		"buckets", buckets,
		"order", p.parseOrder(nil, false),
	))
}

func (p *Parser) parseCompositeKeyProperty(exprType Kind) *Expr {
	p.matchTextSeq("KEY")
	expressions := p.parseWrappedIdVars(false)
	return p.expression(New(exprType, "expressions", expressions))
}

// baseParseWithProperty parses the properties introduced by WITH. It returns nil, an *Expr or a
// []*Expr.
func (p *Parser) baseParseWithProperty() any {
	if p.matchTextSeq("(", "SYSTEM_VERSIONING") {
		prop := p.parseSystemVersioningProperty(true)
		p.matchRParen(nil)
		return prop
	}

	if p.matchNoAdvance(TK_L_PAREN) {
		result := []*Expr{}
		// parseWrappedProperties already flattens nested lists.
		result = append(result, p.parseWrappedProperties()...)
		return result
	}

	if p.matchTextSeq("JOURNAL") {
		return p.parseWithjournaltable()
	}

	if p.matchTextSet(p.s.VIEW_ATTRIBUTES) {
		return p.expression(New(KViewAttributeProperty, "this", upperText(p.prev)))
	}

	if p.matchTextSeq("DATA") {
		return p.parseWithdata(false)
	} else if p.matchTextSeq("NO", "DATA") {
		return p.parseWithdata(true)
	}

	if p.matchNoAdvance(TK_SERDE_PROPERTIES) {
		return chunkANormAny(p.parseSerdeProperties(true))
	}

	if p.match(TK_SCHEMA) {
		return p.expression(New(
			KWithSchemaBindingProperty,
			"this", p.parseVarFromOptions(p.s.SCHEMA_BINDING_OPTIONS, true),
		))
	}

	if chunkAMatchTextKeysNoAdvance(p, p.s.PROCEDURE_OPTIONS) {
		return p.expression(New(
			KWithProcedureOptions,
			"expressions", p.parseCSV(p.parseProcedureOption, TK_COMMA),
		))
	}

	if !p.next.ok() {
		return nil
	}

	return chunkANormAny(p.parseWithisolatedloading())
}

func (p *Parser) parseProcedureOption() *Expr {
	if p.matchTextSeq("EXECUTE", "AS") {
		this := p.parseVarFromOptions(p.s.EXECUTE_AS_OPTIONS, false)
		if this == nil {
			this = p.parseString()
		}
		return p.expression(New(KExecuteAsProperty, "this", this))
	}

	return p.parseVarFromOptions(p.s.PROCEDURE_OPTIONS, true)
}

// baseParseDefiner parses DEFINER = user@host; nil when the user or host is missing.
// https://dev.mysql.com/doc/refman/8.0/en/create-view.html
func (p *Parser) baseParseDefiner() *Expr {
	p.match(TK_EQ)

	user := p.parseIdVar(true, nil)
	p.match(TK_PARAMETER)
	// The host is an identifier or %.
	host := p.parseIdVar(true, nil)
	hostText := ""
	if host == nil && p.match(TK_MOD) {
		hostText = p.prev.Text
	}

	if user == nil || (host == nil && hostText == "") {
		return nil
	}

	hostStr := hostText
	if host != nil {
		hostStr = exprSQL(host)
	}
	return New(KDefinerProperty, "this", exprSQL(user)+"@"+hostStr)
}

func (p *Parser) parseWithjournaltable() *Expr {
	p.match(TK_TABLE)
	p.match(TK_EQ)
	return p.expression(New(KWithJournalTableProperty, "this", p.parseTableParts(false, false, false, false)))
}

func (p *Parser) parseLog(no bool) *Expr {
	return p.expression(New(KLogProperty, "no", no))
}

func (p *Parser) parseJournal(kw propKwargs) *Expr {
	return p.expression(New(KJournalProperty, chunkAPropKwargsPairs(kw)...))
}

func (p *Parser) parseChecksum() *Expr {
	p.match(TK_EQ)

	var on any
	if p.match(TK_ON) {
		on = true
	} else if p.matchTextSeq("OFF") {
		on = false
	}

	return p.expression(New(KChecksumProperty, "on", on, "default", p.match(TK_DEFAULT)))
}

func (p *Parser) parseCluster() *Expr {
	p.match(TK_CLUSTER_BY)
	return p.expression(New(KCluster, "expressions", p.parseCSV(p.parseColumn, TK_COMMA)))
}

func (p *Parser) baseParseClusterProperty() *Expr {
	return p.expression(New(
		KClusterProperty,
		"expressions", p.parseWrappedCSV(p.parseColumn, TK_COMMA, false),
	))
}

func (p *Parser) parseClusteredBy() *Expr {
	p.matchTextSeq("BY")

	p.matchLParen(nil)
	expressions := p.parseCSV(p.parseColumn, TK_COMMA)
	p.matchRParen(nil)

	var sortedBy any
	if p.matchTextSeq("SORTED", "BY") {
		p.matchLParen(nil)
		sortedBy = p.parseCSV(func() *Expr { return p.parseOrdered(nil) }, TK_COMMA)
		p.matchRParen(nil)
	}

	p.match(TK_INTO)
	buckets := p.parseNumber()
	p.matchTextSeq("BUCKETS")

	return p.expression(New(
		KClusteredByProperty,
		"expressions", expressions,
		"sorted_by", sortedBy,
		"buckets", buckets,
	))
}

// parseCopyProperty parses COPY GRANTS; otherwise it backs up over COPY and returns nil.
func (p *Parser) parseCopyProperty() *Expr {
	if !p.matchTextSeq("GRANTS") {
		p.retreat(p.index - 1)
		return nil
	}

	return p.expression(New(KCopyGrantsProperty))
}

func (p *Parser) parseFreespace() *Expr {
	p.match(TK_EQ)
	this := p.parseNumber()
	percent := p.match(TK_PERCENT)
	return p.expression(New(KFreespaceProperty, "this", this, "percent", percent))
}

func (p *Parser) parseMergeblockratio(no bool, default_ bool) *Expr {
	if p.match(TK_EQ) {
		this := p.parseNumber()
		percent := p.match(TK_PERCENT)
		return p.expression(New(KMergeBlockRatioProperty, "this", this, "percent", percent))
	}

	return p.expression(New(KMergeBlockRatioProperty, "no", no, "default", default_))
}

func (p *Parser) parseDatablocksize(default_ bool, minimum bool, maximum bool) *Expr {
	p.match(TK_EQ)
	size := p.parseNumber()

	var units any
	if p.matchTexts("BYTES", "KBYTES", "KILOBYTES") {
		units = p.prev.Text
	}

	return p.expression(New(
		KDataBlocksizeProperty,
		"size", size,
		"units", units,
		"default", default_,
		"minimum", minimum,
		"maximum", maximum,
	))
}

func (p *Parser) parseBlockcompression() *Expr {
	p.match(TK_EQ)
	always := p.matchTextSeq("ALWAYS")
	manual := p.matchTextSeq("MANUAL")
	never := p.matchTextSeq("NEVER")
	default_ := p.matchTextSeq("DEFAULT")

	var autotemp *Expr
	if p.matchTextSeq("AUTOTEMP") {
		autotemp = p.parseSchema(nil)
	}

	return p.expression(New(
		KBlockCompressionProperty,
		"always", always,
		"manual", manual,
		"never", never,
		"default", default_,
		"autotemp", autotemp,
	))
}

// parseWithisolatedloading parses [NO] [CONCURRENT] ISOLATED LOADING [FOR ...]; nil (cursor
// restored) otherwise.
func (p *Parser) parseWithisolatedloading() *Expr {
	index := p.index
	no := p.matchTextSeq("NO")
	concurrent := p.matchTextSeq("CONCURRENT")

	if !p.matchTextSeq("ISOLATED", "LOADING") {
		p.retreat(index)
		return nil
	}

	target := p.parseVarFromOptions(p.s.ISOLATED_LOADING_OPTIONS, false)
	return p.expression(New(
		KIsolatedLoadingProperty,
		"no", no,
		"concurrent", concurrent,
		"target", target,
	))
}

func (p *Parser) parseLocking() *Expr {
	kind := ""
	if p.match(TK_TABLE) {
		kind = "TABLE"
	} else if p.match(TK_VIEW) {
		kind = "VIEW"
	} else if p.match(TK_ROW) {
		kind = "ROW"
	} else if p.matchTextSeq("DATABASE") {
		kind = "DATABASE"
	}

	var this *Expr
	if kind == "DATABASE" || kind == "TABLE" || kind == "VIEW" {
		this = p.parseTableParts(false, false, false, false)
	}

	forOrIn := ""
	if p.match(TK_FOR) {
		forOrIn = "FOR"
	} else if p.match(TK_IN) {
		forOrIn = "IN"
	}

	lockType := ""
	if p.matchTextSeq("ACCESS") {
		lockType = "ACCESS"
	} else if p.matchTexts("EXCL", "EXCLUSIVE") {
		lockType = "EXCLUSIVE"
	} else if p.matchTextSeq("SHARE") {
		lockType = "SHARE"
	} else if p.matchTextSeq("READ") {
		lockType = "READ"
	} else if p.matchTextSeq("WRITE") {
		lockType = "WRITE"
	} else if p.matchTextSeq("CHECKSUM") {
		lockType = "CHECKSUM"
	}

	override := p.matchTextSeq("OVERRIDE")

	return p.expression(New(
		KLockingProperty,
		"this", this,
		"kind", chunkAStrOrNil(kind),
		"for_or_in", chunkAStrOrNil(forOrIn),
		"lock_type", chunkAStrOrNil(lockType),
		"override", override,
	))
}

// parsePartitionBy parses PARTITION BY expressions, or PARTITION BY EXCLUDING expressions as a
// single PartitionByExcluding; empty when absent.
func (p *Parser) parsePartitionBy() []*Expr {
	if !p.match(TK_PARTITION_BY) {
		return []*Expr{}
	}

	if p.s.ID_VAR_TOKENS.Has(p.next.Type) && p.matchTextSeq("EXCLUDING") {
		return []*Expr{p.expression(New(KPartitionByExcluding, "expressions", p.parseCSV(p.parseDisjunction, TK_COMMA)))}
	}

	return p.parseCSV(p.parseDisjunction, TK_COMMA)
}

func (p *Parser) parsePartitionBoundSpec() *Expr {
	parsePartitionBoundExpr := func() *Expr {
		if p.matchTextSeq("MINVALUE") {
			return VarExpr("MINVALUE")
		}
		if p.matchTextSeq("MAXVALUE") {
			return VarExpr("MAXVALUE")
		}
		return p.parseBitwise()
	}

	var this any
	var expression *Expr
	var fromExpressions any
	var toExpressions any

	if p.match(TK_IN) {
		this = p.parseWrappedCSV(p.parseBitwise, TK_COMMA, false)
	} else if p.match(TK_FROM) {
		fromExpressions = p.parseWrappedCSV(parsePartitionBoundExpr, TK_COMMA, false)
		p.matchTextSeq("TO")
		toExpressions = p.parseWrappedCSV(parsePartitionBoundExpr, TK_COMMA, false)
	} else if p.matchTextSeq("WITH", "(", "MODULUS") {
		this = p.parseNumber()
		p.matchTextSeq(",", "REMAINDER")
		expression = p.parseNumber()
		p.matchRParen(nil)
	} else {
		p.raiseError("Failed to parse partition bound spec.", nil)
	}

	return p.expression(New(
		KPartitionBoundSpec,
		"this", this,
		"expression", expression,
		"from_expressions", fromExpressions,
		"to_expressions", toExpressions,
	))
}

// parsePartitionedOf parses PARTITION OF parent {DEFAULT | FOR VALUES ...}.
// https://www.postgresql.org/docs/current/sql-createtable.html
func (p *Parser) parsePartitionedOf() *Expr {
	if !p.matchTextSeq("OF") {
		p.retreat(p.index - 1)
		return nil
	}

	this := p.parseTable(true, false, nil, false, false, false, false)

	var expression *Expr
	if p.match(TK_DEFAULT) {
		expression = VarExpr("DEFAULT")
	} else if p.matchTextSeq("FOR", "VALUES") {
		expression = p.parsePartitionBoundSpec()
	} else {
		p.raiseError("Expecting either DEFAULT or FOR VALUES clause.", nil)
		// raiseError does not panic at lenient error levels; fail anyway (the message keeps its
		// historical wording).
		panic(&ValueError{Msg: "cannot access local variable 'expression' where it is not associated with a value"})
	}

	return p.expression(New(KPartitionedOfProperty, "this", this, "expression", expression))
}

func (p *Parser) baseParsePartitionedBy() *Expr {
	p.match(TK_EQ)
	this := p.parseSchema(nil)
	if this == nil {
		this = p.parseBracket(p.parseField(false, nil, false))
	}
	return p.expression(New(KPartitionedByProperty, "this", this))
}

// parseWithdata parses the rest of WITH [NO] DATA [AND [NO] STATISTICS].
func (p *Parser) parseWithdata(no bool) *Expr {
	var statistics any
	if p.matchTextSeq("AND", "STATISTICS") {
		statistics = true
	} else if p.matchTextSeq("AND", "NO", "STATISTICS") {
		statistics = false
	}

	return p.expression(New(KWithDataProperty, "no", no, "statistics", statistics))
}

func (p *Parser) parseContainsProperty() *Expr {
	if p.matchTextSeq("SQL") {
		return p.expression(New(KSqlReadWriteProperty, "this", "CONTAINS SQL"))
	}
	return nil
}

func (p *Parser) parseModifiesProperty() *Expr {
	if p.matchTextSeq("SQL", "DATA") {
		return p.expression(New(KSqlReadWriteProperty, "this", "MODIFIES SQL DATA"))
	}
	return nil
}

func (p *Parser) parseNoProperty() *Expr {
	if p.matchTextSeq("PRIMARY", "INDEX") {
		return New(KNoPrimaryIndexProperty)
	}
	if p.matchTextSeq("SQL") {
		return p.expression(New(KSqlReadWriteProperty, "this", "NO SQL"))
	}
	return nil
}

func (p *Parser) baseParseOnProperty() *Expr {
	if p.matchTextSeq("COMMIT", "PRESERVE", "ROWS") {
		return New(KOnCommitProperty)
	}
	if p.matchTextSeq("COMMIT", "DELETE", "ROWS") {
		return New(KOnCommitProperty, "delete", true)
	}
	return p.expression(New(KOnProperty, "this", p.parseSchema(p.parseIdVar(true, nil))))
}

func (p *Parser) parseReadsProperty() *Expr {
	if p.matchTextSeq("SQL", "DATA") {
		return p.expression(New(KSqlReadWriteProperty, "this", "READS SQL DATA"))
	}
	return nil
}

func (p *Parser) parseDistkey() *Expr {
	return p.expression(New(
		KDistKeyProperty,
		"this", p.parseWrapped(func() *Expr { return p.parseIdVar(true, nil) }, false),
	))
}

func (p *Parser) parseCreateLike() *Expr {
	table := p.parseTable(true, false, nil, false, false, false, false)

	options := []*Expr{}
	for p.matchTexts("INCLUDING", "EXCLUDING") {
		this := upperText(p.prev)

		idVar := p.parseIdVar(true, nil)
		if idVar == nil {
			return nil
		}

		options = append(options, p.expression(New(
			KProperty,
			"this", this,
			"value", chunkAVar(pyUpper(idVar.ThisS())),
		)))
	}

	return p.expression(New(KLikeProperty, "this", table, "expressions", options))
}

func (p *Parser) parseSortkey(compound bool) *Expr {
	return p.expression(New(
		KSortKeyProperty,
		"this", p.parseWrappedIdVars(false),
		"compound", compound,
	))
}

func (p *Parser) parseCharacterSet(default_ bool) *Expr {
	p.match(TK_EQ)
	return p.expression(New(
		KCharacterSetProperty,
		"this", p.parseVarOrString(false),
		"default", default_,
	))
}

func (p *Parser) parseRemoteWithConnection() *Expr {
	p.matchTextSeq("WITH", "CONNECTION")
	return p.expression(New(
		KRemoteWithConnectionModelProperty,
		"this", p.parseTableParts(false, false, false, false),
	))
}

func (p *Parser) baseParseReturns() *Expr {
	var value *Expr
	var null any
	isTable := p.match(TK_TABLE)

	if isTable {
		if p.match(TK_LT) {
			value = p.expression(New(
				KSchema,
				"this", "TABLE",
				"expressions", p.parseCSV(func() *Expr { return p.parseStructTypes(false) }, TK_COMMA),
			))
			if !p.match(TK_GT) {
				p.raiseError("Expecting >", nil)
			}
		} else {
			value = p.parseSchema(VarExpr("TABLE"))
		}
	} else if p.matchTextSeq("NULL", "ON", "NULL", "INPUT") {
		null = true
		value = nil
	} else {
		value = p.parseTypes(false, false, true, false)
	}

	return p.expression(New(KReturnsProperty, "this", value, "is_table", isTable, "null", null))
}

// chunkAVar returns a Var named name; an empty name panics with a ValueError.
func chunkAVar(name string) *Expr {
	if name == "" {
		panic(&ValueError{Msg: "Cannot convert empty name into var."})
	}
	return VarExpr(name)
}

// chunkAIsNone reports whether a parse result is empty: nil or a nil *Expr.
func chunkAIsNone(v any) bool {
	if v == nil {
		return true
	}
	if e, ok := v.(*Expr); ok && e == nil {
		return true
	}
	return false
}

// chunkANormAny converts a typed nil *Expr held in an interface to an untyped nil.
func chunkANormAny(v any) any {
	if chunkAIsNone(v) {
		return nil
	}
	return v
}

// chunkAEnsureList returns a property parse result (nil, *Expr or []*Expr) as a new slice.
func chunkAEnsureList(v any) []*Expr {
	switch x := v.(type) {
	case nil:
		return []*Expr{}
	case []*Expr:
		return append([]*Expr{}, x...)
	case *Expr:
		if x == nil {
			return []*Expr{}
		}
		return []*Expr{x}
	}
	return []*Expr{}
}

func chunkAMatchTextKeysNoAdvance[V any](p *Parser, m map[string]V) bool {
	if !p.curr.ok() || p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) {
		return false
	}
	_, ok := m[pyUpper(p.curr.Text)]
	return ok
}

// chunkAPropKwargsPairs returns the set property modifiers as New(...) key/value pairs, in the
// order baseParsePropertyBefore matches them, followed by set.
func chunkAPropKwargsPairs(kw propKwargs) []any {
	var out []any
	if kw.no {
		out = append(out, "no", true)
	}
	if kw.dual {
		out = append(out, "dual", true)
	}
	if kw.before {
		out = append(out, "before", true)
	}
	if kw.default_ {
		out = append(out, "default", true)
	}
	if kw.local != "" {
		out = append(out, "local", kw.local)
	}
	if kw.after {
		out = append(out, "after", true)
	}
	if kw.minimum {
		out = append(out, "minimum", true)
	}
	if kw.maximum {
		out = append(out, "maximum", true)
	}
	if kw.set {
		out = append(out, "set", true)
	}
	return out
}

// callPropertyParser runs a PROPERTY_PARSERS entry (the property keyword just matched) with the
// given modifiers. A modifier the entry does not accept is the parse error "Cannot parse
// property '<keyword>'"; the result is then nil.
func (p *Parser) callPropertyParser(parser propertyParseFn, kw propKwargs) any {
	result, rejected := tryPropertyParser(p, parser, kw)
	if rejected {
		p.raiseError(fmt.Sprintf("Cannot parse property '%s'", p.prev.Text), nil)
		return nil
	}
	return result
}

// tryPropertyParser calls a PROPERTY_PARSERS entry, reporting whether it rejected a modifier
// (panicked with *propKwargsTypeError); other panics propagate.
func tryPropertyParser(p *Parser, parser propertyParseFn, kw propKwargs) (result any, rejected bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(*propKwargsTypeError); ok {
				result, rejected = nil, true
				return
			}
			panic(r)
		}
	}()
	return chunkANormAny(parser(p, kw)), false
}
