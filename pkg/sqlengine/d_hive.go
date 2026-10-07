package sqlengine

// Port of sqlglot/parsers/hive.py (HiveParser) and the Hive dialect wiring.
// The generator (sqlglot/generators/hive.py) lives in d_hive_generator.go.

func init() { registerCustomizer("hive", customizeHive) }

func customizeHive(d *Dialect) {
	customizeHiveParser(d)
	customizeHiveGenerator(d)
}

// ---------------------------------------------------------------------------------------------
// Module-level builders (parsers/hive.py)
// ---------------------------------------------------------------------------------------------

// hiveBuildWithIgnoreNulls mirrors parsers.hive.build_with_ignore_nulls.
func hiveBuildWithIgnoreNulls(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		this := New(kind, "this", seqGet(args, 0))
		if a := seqGet(args, 1); a != nil && a.Equal(Boolean(true)) {
			return New(KIgnoreNulls, "this", this)
		}
		return this
	}
}

// hiveBuildToDate mirrors parsers.hive._build_to_date.
func hiveBuildToDate(args []*Expr, d *Dialect) *Expr {
	expr := buildFormattedTime(KTsOrDsToDate, "", nil)(args, d)
	expr.Set("safe", true)
	return expr
}

// hiveBuildNamedStruct mirrors parsers.hive._build_named_struct.
// Map named_struct('k', v, ...) to exp.Struct so _annotate_struct sees it.
func hiveBuildNamedStruct(args []*Expr, d *Dialect) *Expr {
	expressions := []*Expr{}
	for i := 0; i < len(args)-1; i += 2 {
		key, value := args[i], args[i+1]
		name := key.Name()
		expressions = append(expressions, New(KPropertyEQ, "this", ToIdentifier(name, nil), "expression", value))
	}
	return New(KStruct, "expressions", expressions)
}

// hiveBuildDateAdd mirrors parsers.hive._build_date_add (used for DATE_SUB).
func hiveBuildDateAdd(args []*Expr, d *Dialect) *Expr {
	expression := seqGet(args, 1)
	if expression != nil {
		expression = dhBinop(KMul, expression, -1)
	}

	return New(KTsOrDsAdd, "this", seqGet(args, 0), "expression", expression, "unit", LiteralString("DAY"))
}

// ---------------------------------------------------------------------------------------------
// HiveParser
// ---------------------------------------------------------------------------------------------

func customizeHiveParser(d *Dialect) {
	P := d.P

	P.FUNCTION_PARSERS["PERCENTILE"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KQuantile, 0) }
	P.FUNCTION_PARSERS["PERCENTILE_APPROX"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KApproxQuantile, 0) }

	F := P.FUNCTIONS
	F["BASE64"] = fromArgList(KToBase64)
	F["COLLECT_LIST"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KArrayAgg, "this", seqGet(args, 0), "nulls_excluded", true)
	}
	F["COLLECT_SET"] = fromArgList(KArrayUniqueAgg)
	F["DATE_ADD"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KTsOrDsAdd, "this", seqGet(args, 0), "expression", seqGet(args, 1), "unit", LiteralString("DAY"))
	}
	F["DATE_FORMAT"] = func(args []*Expr, d *Dialect) *Expr {
		return buildFormattedTime(KTimeToStr, "", nil)(
			[]*Expr{
				New(KTimeStrToTime, "this", seqGet(args, 0)),
				seqGet(args, 1),
			},
			d,
		)
	}
	F["DATE_SUB"] = hiveBuildDateAdd
	F["DATEDIFF"] = func(args []*Expr, d *Dialect) *Expr {
		return New(
			KDateDiff,
			"this", New(KTsOrDsToDate, "this", seqGet(args, 0)),
			"expression", New(KTsOrDsToDate, "this", seqGet(args, 1)),
		)
	}
	F["DAY"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDay, "this", New(KTsOrDsToDate, "this", seqGet(args, 0)))
	}
	F["FIRST"] = hiveBuildWithIgnoreNulls(KFirst)
	F["FIRST_VALUE"] = hiveBuildWithIgnoreNulls(KFirstValue)
	F["FROM_UNIXTIME"] = buildFormattedTime(KUnixToStr, "", true)
	F["GET_JSON_OBJECT"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KJSONExtractScalar, "this", seqGet(args, 0), "expression", d.toJSONPath(seqGet(args, 1)))
	}
	F["LAST"] = hiveBuildWithIgnoreNulls(KLast)
	F["LAST_VALUE"] = hiveBuildWithIgnoreNulls(KLastValue)
	F["MAP"] = func(args []*Expr, d *Dialect) *Expr { return buildVarMap(args) }
	F["MONTH"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KMonth, "this", FromArgList(KTsOrDsToDate, args))
	}
	F["NAMED_STRUCT"] = hiveBuildNamedStruct
	F["REGEXP_EXTRACT"] = buildRegexpExtract(KRegexpExtract)
	F["REGEXP_EXTRACT_ALL"] = buildRegexpExtract(KRegexpExtractAll)
	F["SEQUENCE"] = fromArgList(KGenerateSeries)
	F["SIZE"] = fromArgList(KArraySize)
	F["SPLIT"] = fromArgList(KRegexpSplit)
	F["STR_TO_MAP"] = func(args []*Expr, d *Dialect) *Expr {
		pairDelim := seqGet(args, 1)
		if pairDelim == nil {
			pairDelim = LiteralString(",")
		}
		keyValueDelim := seqGet(args, 2)
		if keyValueDelim == nil {
			keyValueDelim = LiteralString(":")
		}
		return New(
			KStrToMap,
			"this", seqGet(args, 0),
			"pair_delim", pairDelim,
			"key_value_delim", keyValueDelim,
		)
	}
	F["TO_DATE"] = hiveBuildToDate
	F["TO_JSON"] = fromArgList(KJSONFormat)
	F["TRUNC"] = fromArgList(KTimestampTrunc)
	F["UNBASE64"] = fromArgList(KFromBase64)
	F["UNIX_TIMESTAMP"] = func(args []*Expr, d *Dialect) *Expr {
		if len(args) == 0 {
			args = []*Expr{New(KCurrentTimestamp)}
		}
		return buildFormattedTime(KStrToUnix, "", true)(args, d)
	}
	F["YEAR"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KYear, "this", FromArgList(KTsOrDsToDate, args))
	}

	P.NO_PAREN_FUNCTION_PARSERS["TRANSFORM"] = hiveParseTransform

	P.PROPERTY_PARSERS["SERDEPROPERTIES"] = noKwargsE(func(p *Parser) *Expr {
		return New(KSerdeProperties, "expressions", p.parseWrappedCSV(func() *Expr {
			if e, ok := p.parseProperty().(*Expr); ok {
				return e
			}
			return nil
		}, TK_COMMA, false))
	})
	P.PROPERTY_PARSERS["USING"] = noKwargsE(hiveParseUsingProperty)

	P.ALTER_PARSERS["CHANGE"] = func(p *Parser) any { return anyExpr(hiveParseAlterTableChange(p)) }

	P.h.parseTypes = hiveParseTypes
	P.h.parsePartitionAndOrder = hiveParsePartitionAndOrder
	P.h.parseParameter = hiveParseParameter
	P.h.toPropEq = hiveToPropEq
}

// hiveParseTransform mirrors HiveParser._parse_transform.
func hiveParseTransform(p *Parser) *Expr {
	if !p.matchNoAdvance(TK_L_PAREN) {
		p.retreat(p.index - 1)
		return nil
	}

	args := p.parseWrappedCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA, false)
	rowFormatBefore := p.parseRowFormat(true)

	var recordWriter *Expr
	if p.matchTextSeq("RECORDWRITER") {
		recordWriter = p.parseString()
	}

	if !p.match(TK_USING) {
		return FromArgList(KTransform, args)
	}

	commandScript := p.parseString()

	p.match(TK_ALIAS)
	schema := p.parseSchema(nil)

	rowFormatAfter := p.parseRowFormat(true)
	var recordReader *Expr
	if p.matchTextSeq("RECORDREADER") {
		recordReader = p.parseString()
	}

	return p.expression(New(
		KQueryTransform,
		"expressions", args,
		"command_script", commandScript,
		"schema", schema,
		"row_format_before", rowFormatBefore,
		"record_writer", recordWriter,
		"row_format_after", rowFormatAfter,
		"record_reader", recordReader,
	))
}

// hiveParseTypes mirrors HiveParser._parse_types.
//
// Spark (and most likely Hive) treats casts to CHAR(length) and VARCHAR(length) as casts to
// STRING in all contexts except for schema definitions, so we drop the length to transpile
// correctly (see the Python docstring).
func hiveParseTypes(p *Parser, checkFunc bool, schema bool, allowIdentifiers bool, withCollation bool) *Expr {
	this := p.baseParseTypes(checkFunc, schema, allowIdentifiers, withCollation)

	if this != nil && !schema {
		toText := func(node *Expr) *Expr {
			if node.IsA(KDataType) && dhIsType(node, DT_CHAR, DT_VARCHAR) {
				node.Set("this", DT_TEXT)
				node.Set("expressions", nil)
			}
			return node
		}

		return this.Transform(toText, false)
	}

	return this
}

// hiveParseAlterTableChange mirrors HiveParser._parse_alter_table_change.
func hiveParseAlterTableChange(p *Parser) *Expr {
	p.match(TK_COLUMN)
	this := p.parseField(true, nil, false)

	if p.s.CHANGE_COLUMN_ALTER_SYNTAX && p.matchTextSeq("TYPE") {
		return p.expression(New(KAlterColumn, "this", this, "dtype", p.parseTypes(false, true, true, false)))
	}

	columnNew := p.parseField(true, nil, false)
	dtype := p.parseTypes(false, true, true, false)

	// self._match(TokenType.COMMENT) and self._parse_string() -> False when unmatched
	var comment any = false
	if p.match(TK_COMMENT) {
		comment = p.parseString()
	}

	if this == nil || columnNew == nil || dtype == nil {
		p.raiseError("Expected 'CHANGE COLUMN' to be followed by 'column_name' 'column_name' 'data_type'", nil)
	}

	return p.expression(New(KAlterColumn, "this", this, "rename_to", columnNew, "dtype", dtype, "comment", comment))
}

// hiveParseUsingProperty mirrors HiveParser._parse_using_property.
func hiveParseUsingProperty(p *Parser) *Expr {
	if p.matchTexts("JAR", "FILE", "ARCHIVE") {
		kind := upperText(p.prev)
		return New(KUsingProperty, "this", p.parseString(), "kind", kind)
	}

	return p.parsePropertyAssignment(KFileFormatProperty)
}

// hiveParsePartitionAndOrder mirrors HiveParser._parse_partition_and_order.
func hiveParsePartitionAndOrder(p *Parser) ([]*Expr, *Expr) {
	partition := []*Expr{}
	if p.matchAny(TK_PARTITION_BY, TK_DISTRIBUTE_BY) {
		partition = p.parseCSV(p.parseAssignment, TK_COMMA)
	}
	order := p.parseOrder(nil, p.match(TK_SORT_BY))
	return partition, order
}

// hiveParseParameter mirrors HiveParser._parse_parameter.
func hiveParseParameter(p *Parser) *Expr {
	p.match(TK_L_BRACE)
	this := p.parseIdentifier()
	if this == nil {
		this = p.parsePrimaryOrVar()
	}
	// self._match(TokenType.COLON) and (...) -> False when unmatched
	var expression any = false
	if p.match(TK_COLON) {
		e := p.parseIdentifier()
		if e == nil {
			e = p.parsePrimaryOrVar()
		}
		expression = e
	}
	p.match(TK_R_BRACE)
	return p.expression(New(KParameter, "this", this, "expression", expression))
}

// hiveToPropEq mirrors HiveParser._to_prop_eq.
func hiveToPropEq(p *Parser, expression *Expr, index int) *Expr {
	if expression.IsStar() {
		return expression
	}

	var key *Expr
	if expression.IsA(KColumn) {
		key = expression.This()
	} else {
		key = ToIdentifier("col"+itoa(index+1), nil)
	}

	return p.expression(New(KPropertyEQ, "this", key, "expression", expression))
}
