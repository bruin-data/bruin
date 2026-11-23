package sqlengine

import (
	"strconv"
	"strings"
)

func init() { registerCustomizer("hive", customizeHive) }

func customizeHive(d *Dialect) {
	customizeHiveParser(d)
	customizeHiveGenerator(d)
}

func hiveBuildWithIgnoreNulls(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		this := New(kind, "this", seqGet(args, 0))
		if a := seqGet(args, 1); a != nil && a.Equal(Boolean(true)) {
			return New(KIgnoreNulls, "this", this)
		}
		return this
	}
}

func hiveBuildToDate(args []*Expr, d *Dialect) *Expr {
	expr := buildFormattedTime(KTsOrDsToDate, "", nil)(args, d)
	expr.Set("safe", true)
	return expr
}

// hiveBuildNamedStruct maps named_struct('k', v, ...) to a Struct of PropertyEQ pairs so
// annotateStruct can type it.
func hiveBuildNamedStruct(args []*Expr, d *Dialect) *Expr {
	expressions := []*Expr{}
	for i := 0; i < len(args)-1; i += 2 {
		key, value := args[i], args[i+1]
		name := key.Name()
		expressions = append(expressions, New(KPropertyEQ, "this", ToIdentifier(name, nil), "expression", value))
	}
	return New(KStruct, "expressions", expressions)
}

// hiveBuildDateAdd builds DATE_SUB(date, n) as a day-unit TsOrDsAdd with n negated.
func hiveBuildDateAdd(args []*Expr, d *Dialect) *Expr {
	expression := seqGet(args, 1)
	if expression != nil {
		expression = dhBinop(KMul, expression, -1)
	}

	return New(KTsOrDsAdd, "this", seqGet(args, 0), "expression", expression, "unit", LiteralString("DAY"))
}

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
	P.ALTER_PARSERS["REPLACE"] = func(p *Parser) any { return anyExpr(hiveParseAlterTableReplaceColumns(p)) }

	P.h.parseTypes = hiveParseTypes
	P.h.parsePartitionAndOrder = hiveParsePartitionAndOrder
	P.h.parseParameter = hiveParseParameter
	P.h.toPropEq = hiveToPropEq
	// Strip trailing fractional zeros and normalize zero when inferring BD literal types, and
	// widen negative scales to a zero scale.
	P.h.buildCast = hiveBuildCast(true, false)
}

// hiveBuildCast returns the cast builder for a dialect whose BD-suffixed literals (1.5BD) are
// DECIMALs of the literal's own precision and scale: a literal like 1.50BD becomes
// CAST('1.50' AS DECIMAL(3, 2)), cast from text so the value is not rounded through DOUBLE.
// normalize strips trailing fractional zeros (DECIMAL(2, 1)) and gives zero DECIMAL(1, 0);
// allowNegativeScale keeps the negative scale of an exponent literal (1E3BD as
// DECIMAL(1, -3), cast from the number) instead of widening the precision to a zero scale.
func hiveBuildCast(normalize, allowNegativeScale bool) func(p *Parser, strict bool, kv ...any) *Expr {
	return func(p *Parser, strict bool, kv ...any) *Expr {
		if upperText(p.prev) != "BD" {
			return p.baseBuildCast(strict, kv...)
		}

		thisIndex := -1
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i] == "this" {
				thisIndex = i + 1
			}
		}
		if thisIndex < 0 {
			return p.baseBuildCast(strict, kv...)
		}
		this, _ := kv[thisIndex].(*Expr)

		digits, exponent, ok := hiveDecimalTuple(this.Name())
		if !ok {
			p.raiseError("Invalid decimal literal "+this.Name(), nil)
			return p.baseBuildCast(strict, kv...)
		}
		precision, scale := len(digits), -exponent

		if normalize {
			if strings.Trim(digits, "0") == "" {
				precision, scale = 1, 0
			} else {
				for scale > 0 && digits[precision-1] == '0' {
					precision--
					scale--
				}
			}
		}

		if scale < 0 && !allowNegativeScale {
			precision -= scale
			scale = 0
		}

		// Unsuffixed exponent literals can be DOUBLE, so cast from text to avoid rounding.
		if scale >= 0 {
			this = LiteralString(this.Name())
		}
		to := New(KDataType, "this", DT_DECIMAL, "expressions", []*Expr{
			New(KDataTypeParam, "this", LiteralInt(max(precision, scale))),
			New(KDataTypeParam, "this", LiteralInt(scale)),
		})

		out := make([]any, 0, len(kv)+2)
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i] != "this" && kv[i] != "to" {
				out = append(out, kv[i], kv[i+1])
			}
		}
		out = append(out, "this", this, "to", to)
		return p.baseBuildCast(strict, out...)
	}
}

// hiveDecimalTuple splits a decimal number such as 12.30 or 1.5E-3 into its coefficient digits,
// without leading zeros (zero keeps a single digit), and its exponent: 12.30 is ("1230", -2).
// ok is false when s is not a decimal number.
func hiveDecimalTuple(s string) (digits string, exponent int, ok bool) {
	mantissa, exp, hasExp := strings.Cut(strings.ToUpper(s), "E")
	if hasExp {
		e, err := strconv.Atoi(exp)
		if err != nil {
			return "", 0, false
		}
		exponent = e
	}
	intPart, fracPart, _ := strings.Cut(mantissa, ".")
	if intPart+fracPart == "" {
		return "", 0, false
	}
	for _, r := range intPart + fracPart {
		if r < '0' || r > '9' {
			return "", 0, false
		}
	}
	digits = strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		digits = "0"
	}
	return digits, exponent - len(fracPart), true
}

// hiveParseTransform returns nil, backing up one token, when no parenthesis follows TRANSFORM.
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

// hiveParseTypes turns CHAR(n) and VARCHAR(n) outside a schema into TEXT without a length:
// Spark (and most likely Hive) treats such casts as casts to STRING in all contexts except
// schema definitions.
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

func hiveParseAlterTableChange(p *Parser) *Expr {
	p.match(TK_COLUMN)
	this := p.parseField(true, nil, false)

	if p.s.CHANGE_COLUMN_ALTER_SYNTAX && p.matchTextSeq("TYPE") {
		return p.expression(New(KAlterColumn, "this", this, "dtype", p.parseTypes(false, true, true, false)))
	}

	columnNew := p.parseField(true, nil, false)
	dtype := p.parseTypes(false, true, true, false)

	// false (not nil) when COMMENT is absent.
	var comment any = false
	if p.match(TK_COMMENT) {
		comment = p.parseString()
	}

	if this == nil || columnNew == nil || dtype == nil {
		p.raiseError("Expected 'CHANGE COLUMN' to be followed by 'column_name' 'column_name' 'data_type'", nil)
	}

	return p.expression(New(KAlterColumn, "this", this, "rename_to", columnNew, "dtype", dtype, "comment", comment))
}

func hiveParseUsingProperty(p *Parser) *Expr {
	if p.matchTexts("JAR", "FILE", "ARCHIVE") {
		kind := upperText(p.prev)
		return New(KUsingProperty, "this", p.parseString(), "kind", kind)
	}

	return p.parsePropertyAssignment(KFileFormatProperty)
}

func hiveParsePartitionAndOrder(p *Parser) ([]*Expr, *Expr) {
	var partition []*Expr
	if p.match(TK_DISTRIBUTE_BY) {
		partition = p.parseCSV(p.parseAssignment, TK_COMMA)
	} else {
		partition = p.parsePartitionBy()
	}
	order := p.parseOrder(nil, p.match(TK_SORT_BY))
	return partition, order
}

// hiveParseAlterTableReplaceColumns returns nil, so the statement falls back to a command, when
// there are no columns or a definition does not parse.
func hiveParseAlterTableReplaceColumns(p *Parser) *Expr {
	if !p.matchTextSeq("COLUMNS") {
		return nil
	}

	expressions := tryParse(p, func() []*Expr {
		return p.parseWrappedCSV(p.parseFieldDef, TK_COMMA, true)
	}, false, func(l []*Expr) bool { return len(l) == 0 })
	if len(expressions) == 0 {
		return nil
	}
	return p.expression(New(KReplaceColumns, "expressions", expressions))
}

func hiveParseParameter(p *Parser) *Expr {
	p.match(TK_L_BRACE)
	this := p.parseIdentifier()
	if this == nil {
		this = p.parsePrimaryOrVar()
	}
	// false (not nil) when COLON is absent.
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
