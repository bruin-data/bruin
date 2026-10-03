package sqlengine

import "fmt"

// Port of sqlglot.parser.Parser (chunk A, part 2): property parsing (parser.py L2794-L3442).

// _parse_property_before (parser.py L2794).
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
		// Only truthy kwargs are passed (propKwargs zero values mean "not passed").
		result, typeErr := chunkACallPropertyParser(p, parser, kw)
		if !typeErr {
			return result
		}
		p.raiseError(fmt.Sprintf("Cannot parse property '%s'", p.prev.Text), nil)
	}

	return nil
}

// _parse_wrapped_properties (parser.py L2819)
//
// Python returns list[Expr | list[Expr]]; nested lists (from property parsers that return lists)
// are flattened here since the Go signature is []*Expr.
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

// _parse_property (parser.py L2822).
func (p *Parser) parseProperty() any {
	if matchTextKeys(p, p.s.PROPERTY_PARSERS) {
		return chunkANormAny(p.s.PROPERTY_PARSERS[upperText(p.prev)](p, propKwargs{}))
	}

	if p.match(TK_DEFAULT) && matchTextKeys(p, p.s.PROPERTY_PARSERS) {
		return chunkANormAny(p.s.PROPERTY_PARSERS[upperText(p.prev)](p, propKwargs{default_: true}))
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

// _parse_key_value_property (parser.py L2844).
func (p *Parser) parseKeyValueProperty(parseValue func() *Expr) *Expr {
	index := p.index
	key := p.parseColumn()

	if !p.match(TK_EQ) {
		p.retreat(index)
		return nil
	}

	// Transform the key to exp.Dot if it's dotted identifiers wrapped in exp.Column or to exp.Var otherwise
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

	// Transform the value to exp.Var if it was parsed as exp.Column(exp.Identifier())
	if value.IsA(KColumn) {
		value = chunkAVar(value.Name())
	}

	return p.expression(New(KProperty, "this", key, "value", value))
}

// _parse_stored (parser.py L2870).
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

// _parse_unquoted_field (parser.py L2893).
func (p *Parser) parseUnquotedField() *Expr {
	field := p.parseField(false, nil, false)
	if field.IsA(KIdentifier) && !field.ArgB("quoted") {
		// exp.var(<expression>) takes the expression's name (no empty-name check for expressions)
		field = VarExpr(field.Name())
	}

	return field
}

// _parse_property_assignment (parser.py L2900).
func (p *Parser) parsePropertyAssignment(expClass Kind, kv ...any) *Expr {
	p.match(TK_EQ)
	p.match(TK_ALIAS)

	args := append([]any{"this", p.parseUnquotedField()}, kv...)
	return p.expression(New(expClass, args...))
}

// _parse_properties (parser.py L2906).
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
	}

	if len(properties) > 0 {
		return p.expression(New(KProperties, "expressions", properties))
	}

	return nil
}

// _parse_fallback (parser.py L2923).
func (p *Parser) parseFallback(no bool) *Expr {
	return p.expression(New(KFallbackProperty, "no", no, "protection", p.matchTextSeq("PROTECTION")))
}

// _parse_sql_security (parser.py L2928).
func (p *Parser) parseSqlSecurity() *Expr {
	// self._match_texts(...) and self._prev.text.upper() -> False when unmatched
	var this any = false
	if p.matchTextSet(p.s.SECURITY_PROPERTY_KEYWORDS) {
		this = upperText(p.prev)
	}
	return p.expression(New(KSqlSecurityProperty, "this", this))
}

// _parse_settings_property (parser.py L2935).
func (p *Parser) parseSettingsProperty() *Expr {
	return p.expression(New(KSettingsProperty, "expressions", p.parseCSV(p.parseAssignment, TK_COMMA)))
}

// _parse_called_on_null_input_property (parser.py L2940).
func (p *Parser) parseCalledOnNullInputProperty() *Expr {
	if !p.matchTextSeq("ON", "NULL", "INPUT") {
		p.retreat(p.index - 1)
		return nil
	}

	return p.expression(New(KCalledOnNullInputProperty))
}

// _parse_volatile_property (parser.py L2947).
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

// _parse_retention_period (parser.py L2958).
func (p *Parser) parseRetentionPeriod() *Expr {
	// Parse TSQL's HISTORY_RETENTION_PERIOD: {INFINITE | <number> DAY | DAYS | MONTH ...}
	number := p.parseNumber()
	numberStr := ""
	if number != nil {
		numberStr = exprSQL(number) + " "
	}
	unit := p.parseVar(true, nil, false)
	unitStr := "None" // f"{None}"
	if unit != nil {
		unitStr = exprSQL(unit)
	}
	return chunkAVar(numberStr + unitStr)
}

// _parse_system_versioning_property (parser.py L2965).
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

// _parse_data_deletion_property (parser.py L2989).
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

// _parse_distributed_property (parser.py L3005).
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

// _parse_composite_key_property (parser.py L3024).
func (p *Parser) parseCompositeKeyProperty(exprType Kind) *Expr {
	p.matchTextSeq("KEY")
	expressions := p.parseWrappedIdVars(false)
	return p.expression(New(exprType, "expressions", expressions))
}

// _parse_with_property (parser.py L3029).
func (p *Parser) baseParseWithProperty() any {
	if p.matchTextSeq("(", "SYSTEM_VERSIONING") {
		prop := p.parseSystemVersioningProperty(true)
		p.matchRParen(nil)
		return prop
	}

	if p.matchNoAdvance(TK_L_PAREN) {
		result := []*Expr{}
		// parseWrappedProperties already flattens nested lists (result.extend(i) if isinstance(i, list)).
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

// _parse_procedure_option (parser.py L3072).
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

// _parse_definer (parser.py L3086)
// https://dev.mysql.com/doc/refman/8.0/en/create-view.html
func (p *Parser) baseParseDefiner() *Expr {
	p.match(TK_EQ)

	user := p.parseIdVar(true, nil)
	p.match(TK_PARAMETER)
	// host = self._parse_id_var() or (self._match(TokenType.MOD) and self._prev.text)
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

// _parse_withjournaltable (parser.py L3098).
func (p *Parser) parseWithjournaltable() *Expr {
	p.match(TK_TABLE)
	p.match(TK_EQ)
	return p.expression(New(KWithJournalTableProperty, "this", p.parseTableParts(false, false, false, false)))
}

// _parse_log (parser.py L3103).
func (p *Parser) parseLog(no bool) *Expr {
	return p.expression(New(KLogProperty, "no", no))
}

// _parse_journal (parser.py L3106).
func (p *Parser) parseJournal(kw propKwargs) *Expr {
	return p.expression(New(KJournalProperty, chunkAPropKwargsPairs(kw)...))
}

// _parse_checksum (parser.py L3109).
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

// _parse_cluster (parser.py L3120).
func (p *Parser) parseCluster() *Expr {
	p.match(TK_CLUSTER_BY)
	return p.expression(New(KCluster, "expressions", p.parseCSV(p.parseColumn, TK_COMMA)))
}

// _parse_cluster_property (parser.py L3128).
func (p *Parser) baseParseClusterProperty() *Expr {
	return p.expression(New(
		KClusterProperty,
		"expressions", p.parseWrappedCSV(p.parseColumn, TK_COMMA, false),
	))
}

// _parse_clustered_by (parser.py L3135).
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

// _parse_copy_property (parser.py L3157).
func (p *Parser) parseCopyProperty() *Expr {
	if !p.matchTextSeq("GRANTS") {
		p.retreat(p.index - 1)
		return nil
	}

	return p.expression(New(KCopyGrantsProperty))
}

// _parse_freespace (parser.py L3164).
func (p *Parser) parseFreespace() *Expr {
	p.match(TK_EQ)
	this := p.parseNumber()
	percent := p.match(TK_PERCENT)
	return p.expression(New(KFreespaceProperty, "this", this, "percent", percent))
}

// _parse_mergeblockratio (parser.py L3170).
func (p *Parser) parseMergeblockratio(no bool, default_ bool) *Expr {
	if p.match(TK_EQ) {
		this := p.parseNumber()
		percent := p.match(TK_PERCENT)
		return p.expression(New(KMergeBlockRatioProperty, "this", this, "percent", percent))
	}

	return p.expression(New(KMergeBlockRatioProperty, "no", no, "default", default_))
}

// _parse_datablocksize (parser.py L3182).
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

// _parse_blockcompression (parser.py L3201).
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

// _parse_withisolatedloading (parser.py L3218).
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

// _parse_locking (parser.py L3232).
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

// _parse_partition_by (parser.py L3279).
func (p *Parser) parsePartitionBy() []*Expr {
	if p.match(TK_PARTITION_BY) {
		return p.parseCSV(p.parseDisjunction, TK_COMMA)
	}
	return []*Expr{}
}

// _parse_partition_bound_spec (parser.py L3284).
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

// _parse_partitioned_of (parser.py L3321)
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
		// When raise_error doesn't raise, Python hits an unbound local `expression`.
		panic(&ValueError{Msg: "cannot access local variable 'expression' where it is not associated with a value"})
	}

	return p.expression(New(KPartitionedOfProperty, "this", this, "expression", expression))
}

// _parse_partitioned_by (parser.py L3337).
func (p *Parser) baseParsePartitionedBy() *Expr {
	p.match(TK_EQ)
	this := p.parseSchema(nil)
	if this == nil {
		this = p.parseBracket(p.parseField(false, nil, false))
	}
	return p.expression(New(KPartitionedByProperty, "this", this))
}

// _parse_withdata (parser.py L3345).
func (p *Parser) parseWithdata(no bool) *Expr {
	var statistics any
	if p.matchTextSeq("AND", "STATISTICS") {
		statistics = true
	} else if p.matchTextSeq("AND", "NO", "STATISTICS") {
		statistics = false
	}

	return p.expression(New(KWithDataProperty, "no", no, "statistics", statistics))
}

// _parse_contains_property (parser.py L3355).
func (p *Parser) parseContainsProperty() *Expr {
	if p.matchTextSeq("SQL") {
		return p.expression(New(KSqlReadWriteProperty, "this", "CONTAINS SQL"))
	}
	return nil
}

// _parse_modifies_property (parser.py L3360).
func (p *Parser) parseModifiesProperty() *Expr {
	if p.matchTextSeq("SQL", "DATA") {
		return p.expression(New(KSqlReadWriteProperty, "this", "MODIFIES SQL DATA"))
	}
	return nil
}

// _parse_no_property (parser.py L3365).
func (p *Parser) parseNoProperty() *Expr {
	if p.matchTextSeq("PRIMARY", "INDEX") {
		return New(KNoPrimaryIndexProperty)
	}
	if p.matchTextSeq("SQL") {
		return p.expression(New(KSqlReadWriteProperty, "this", "NO SQL"))
	}
	return nil
}

// _parse_on_property (parser.py L3372).
func (p *Parser) baseParseOnProperty() *Expr {
	if p.matchTextSeq("COMMIT", "PRESERVE", "ROWS") {
		return New(KOnCommitProperty)
	}
	if p.matchTextSeq("COMMIT", "DELETE", "ROWS") {
		return New(KOnCommitProperty, "delete", true)
	}
	return p.expression(New(KOnProperty, "this", p.parseSchema(p.parseIdVar(true, nil))))
}

// _parse_reads_property (parser.py L3379).
func (p *Parser) parseReadsProperty() *Expr {
	if p.matchTextSeq("SQL", "DATA") {
		return p.expression(New(KSqlReadWriteProperty, "this", "READS SQL DATA"))
	}
	return nil
}

// _parse_distkey (parser.py L3384).
func (p *Parser) parseDistkey() *Expr {
	return p.expression(New(
		KDistKeyProperty,
		"this", p.parseWrapped(func() *Expr { return p.parseIdVar(true, nil) }, false),
	))
}

// _parse_create_like (parser.py L3387).
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

// _parse_sortkey (parser.py L3404).
func (p *Parser) parseSortkey(compound bool) *Expr {
	return p.expression(New(
		KSortKeyProperty,
		"this", p.parseWrappedIdVars(false),
		"compound", compound,
	))
}

// _parse_character_set (parser.py L3409).
func (p *Parser) parseCharacterSet(default_ bool) *Expr {
	p.match(TK_EQ)
	return p.expression(New(
		KCharacterSetProperty,
		"this", p.parseVarOrString(false),
		"default", default_,
	))
}

// _parse_remote_with_connection (parser.py L3415).
func (p *Parser) parseRemoteWithConnection() *Expr {
	p.matchTextSeq("WITH", "CONNECTION")
	return p.expression(New(
		KRemoteWithConnectionModelProperty,
		"this", p.parseTableParts(false, false, false, false),
	))
}

// _parse_returns (parser.py L3421).
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

// ---------------------------------------------------------------------------------------------
// Chunk A helpers (not Parser methods).

// chunkAVar mirrors exp.var(name) for a string name (raises on an empty name).
func chunkAVar(name string) *Expr {
	if name == "" {
		panic(&ValueError{Msg: "Cannot convert empty name into var."})
	}
	return VarExpr(name)
}

// chunkAIsNone reports whether a parse result is Python None (nil or a nil *Expr).
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

// chunkAEnsureList mirrors sqlglot.helper.ensure_list for property parse results.
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

// chunkAMatchTextKeysNoAdvance mirrors Parser._match_texts(dict, advance=False).
func chunkAMatchTextKeysNoAdvance[V any](p *Parser, m map[string]V) bool {
	if !p.curr.ok() || p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) {
		return false
	}
	_, ok := m[pyUpper(p.curr.Text)]
	return ok
}

// chunkAPropKwargsPairs returns the passed (truthy) property kwargs as New(...) key/value pairs,
// in the order of the kwargs dict built by _parse_property_before.
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

// chunkAPropertyTypeError mirrors the TypeError Python raises when a PROPERTY_PARSERS entry is
// called with a keyword argument it does not accept.
type chunkAPropertyTypeError struct{ msg string }

func (e *chunkAPropertyTypeError) Error() string { return e.msg }

// chunkACallPropertyParser calls a PROPERTY_PARSERS entry, reporting (like `except TypeError`)
// whether it raised *chunkAPropertyTypeError.
func chunkACallPropertyParser(p *Parser, parser propertyParseFn, kw propKwargs) (result any, typeErr bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(*chunkAPropertyTypeError); ok {
				result, typeErr = nil, true
				return
			}
			panic(r)
		}
	}()
	return chunkANormAny(parser(p, kw)), false
}
