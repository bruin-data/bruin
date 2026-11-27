package sqlengine

import (
	"strings"
)

func bigqueryBuildContainsSubstring(args []*Expr, _ *Dialect) *Expr {
	this := New(KLower, "this", seqGet(args, 0))
	expr := New(KLower, "this", seqGet(args, 1))
	return New(KContains, "this", this, "expression", expr, "json_scope", seqGet(args, 2))
}

func bigqueryBuildDate(args []*Expr, _ *Dialect) *Expr {
	exprType := KDate
	if len(args) == 3 {
		exprType = KDateFromParts
	}
	return FromArgList(exprType, args)
}

// bigqueryBuildDateDiff maps a WEEK unit to WEEK(SUNDAY), BigQuery's default week start.
func bigqueryBuildDateDiff(exprType Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		expr := New(
			exprType,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"unit", seqGet(args, 2),
			"date_part_boundary", true,
		)

		unit := expr.ArgE("unit")
		if unit.IsA(KVar) && pyUpper(unit.Name()) == "WEEK" {
			expr.Set("unit", New(KWeekStart, "this", VarExpr("SUNDAY")))
		}

		return expr
	}
}

func bigqueryBuildDatetime(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		return FromArgList(KTsOrDsToDatetime, args)
	}
	if len(args) == 2 {
		return FromArgList(KDatetime, args)
	}
	return FromArgList(KTimestampFromParts, args)
}

func bigqueryBuildExtractJSONWithDefaultPath(exprType Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		if len(args) == 1 {
			// The default path counts as a passed argument: record it for arity validation.
			args = append(args, LiteralString("$"))
			return WithValidateArgs(buildExtractJSONWithPath(exprType)(args, d), args)
		}
		return buildExtractJSONWithPath(exprType)(args, d)
	}
}

func bigqueryBuildFormatTime(exprType Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		formattedTime := buildFormattedTime(KTimeToStr, "", nil)(
			[]*Expr{New(exprType, "this", seqGet(args, 1)), seqGet(args, 0)}, d,
		)
		formattedTime.Set("zone", seqGet(args, 2))
		return formattedTime
	}
}

func bigqueryBuildJSONStripNulls(args []*Expr, _ *Dialect) *Expr {
	expression := New(KJSONStripNulls, "this", seqGet(args, 0))
	for _, a := range argsFrom(args, 1) {
		if a.IsA(KKwarg) {
			expression.Set(pyLower(a.This().Name()), a)
		} else {
			expression.Set("expression", a)
		}
	}
	return expression
}

// bigqueryBuildLevenshtein takes max_dist from the value of EDIT_DISTANCE's named third argument.
func bigqueryBuildLevenshtein(args []*Expr, _ *Dialect) *Expr {
	maxDist := seqGet(args, 2)
	var md *Expr
	if maxDist != nil {
		md = maxDist.Expression()
	}
	return New(
		KLevenshtein,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"max_dist", md,
	)
}

func bigqueryBuildParseDate(args []*Expr, d *Dialect) *Expr {
	this := buildFormattedTime(KStrToDate, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	this.Set("default_year", LiteralInt(1970))
	return this
}

func bigqueryBuildParseTimestamp(args []*Expr, d *Dialect) *Expr {
	this := buildFormattedTime(KStrToTime, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	this.Set("zone", seqGet(args, 2))
	this.Set("default_year", LiteralInt(1970))
	return this
}

func bigqueryBuildParseDatetime(args []*Expr, d *Dialect) *Expr {
	this := buildFormattedTime(KParseDatetime, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	this.Set("default_year", LiteralInt(1970))
	return this
}

func bigqueryBuildRegexpExtract(exprType Kind, defaultGroup func() *Expr) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		// A pattern that is not a string or fails to compile counts as having no single group.
		n, ok := regexpCaptureGroupCount(seqGet(args, 1))
		group := ok && n == 1

		var groupArg *Expr
		if group {
			groupArg = LiteralInt(1)
		} else if defaultGroup != nil {
			groupArg = defaultGroup()
		}

		kv := []any{
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"position", seqGet(args, 2),
			"occurrence", seqGet(args, 3),
			"group", groupArg,
		}
		if exprType == KRegexpExtract {
			kv = append(kv, "null_if_pos_overflow", d.S.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL)
		}
		return New(exprType, kv...)
	}
}

func bigqueryBuildTime(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		return New(KTsOrDsToTime, "this", args[0])
	}
	if len(args) == 2 {
		return FromArgList(KTime, args)
	}
	return FromArgList(KTimeFromParts, args)
}

func bigqueryBuildTimestamp(args []*Expr, _ *Dialect) *Expr {
	timestamp := FromArgList(KTimestamp, args)
	timestamp.Set("with_tz", true)
	return timestamp
}

func bigqueryBuildToHex(args []*Expr, _ *Dialect) *Expr {
	arg := seqGet(args, 0)
	if arg.IsA(KMD5Digest) {
		return New(KMD5, "this", arg.This())
	}
	return New(KLowerHex, "this", arg)
}

// bigqueryDomainDot masks dots in a domain-scoped project ID; it cannot occur in a SQL identifier.
const bigqueryDomainDot = "\x00"

func bigquerySplitNumWords(value, sep string, minNumWords int) []*string {
	words := strings.Split(value, sep)
	var out []*string
	for range minNumWords - len(words) {
		out = append(out, nil)
	}
	for _, w := range words {
		out = append(out, &w)
	}
	return out
}

func bigquerySplitQualifiedName(name string, minNumWords int) []*string {
	// Domain-scoped (legacy) project IDs look like `domain.com:project-id`: the dots before the
	// colon belong to the domain, not the path (a project ID itself can't contain dots). Mask
	// them so splitting doesn't corrupt the project ID.
	colon := strings.Index(name, ":")
	if colon != -1 && strings.Contains(name[:colon], ".") {
		name = strings.ReplaceAll(name[:colon], ".", bigqueryDomainDot) + name[colon:]
		parts := bigquerySplitNumWords(name, ".", minNumWords)
		for i, p := range parts {
			if p != nil && *p != "" {
				s := strings.ReplaceAll(*p, bigqueryDomainDot, ".")
				parts[i] = &s
			}
		}
		return parts
	}

	return bigquerySplitNumWords(name, ".", minNumWords)
}

func bigqueryQuotedIdentifiers(parts []*string) []*Expr {
	out := make([]*Expr, len(parts))
	for i, p := range parts {
		if p != nil {
			out[i] = ToIdentifier(*p, boolp(true))
		}
	}
	return out
}

func bigqueryJoinPartNames(parts []*Expr) string {
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = p.Name()
	}
	return strings.Join(names, ".")
}

var bigqueryMakeIntervalKwargs = []string{"year", "month", "day", "hour", "minute", "second"}

var bigqueryBracketOffsets = map[string]struct {
	offset int
	safe   bool
}{
	"OFFSET":       {0, false},
	"ORDINAL":      {1, false},
	"SAFE_OFFSET":  {0, true},
	"SAFE_ORDINAL": {1, true},
}

func customizeBigQueryParser(d *Dialect) {
	P := d.P

	delete(P.FUNCTIONS, "SEARCH")
	P.FUNCTIONS["APPROX_TOP_COUNT"] = fromArgList(KApproxTopK)
	P.FUNCTIONS["BIT_AND"] = fromArgList(KBitwiseAndAgg)
	P.FUNCTIONS["BIT_OR"] = fromArgList(KBitwiseOrAgg)
	P.FUNCTIONS["BIT_XOR"] = fromArgList(KBitwiseXorAgg)
	P.FUNCTIONS["BIT_COUNT"] = fromArgList(KBitwiseCount)
	P.FUNCTIONS["BOOL"] = fromArgList(KJSONBool)
	P.FUNCTIONS["CONTAINS_SUBSTR"] = bigqueryBuildContainsSubstring
	P.FUNCTIONS["DATE"] = bigqueryBuildDate
	P.FUNCTIONS["DATE_ADD"] = buildDateDeltaWithInterval(KDateAdd, "")
	P.FUNCTIONS["DATE_DIFF"] = bigqueryBuildDateDiff(KDateDiff)
	P.FUNCTIONS["DATE_SUB"] = buildDateDeltaWithInterval(KDateSub, "")
	P.FUNCTIONS["DATE_TRUNC"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KDateTrunc,
			"unit", seqGet(args, 1),
			"this", seqGet(args, 0),
			"zone", seqGet(args, 2),
		)
	}
	P.FUNCTIONS["DATETIME"] = bigqueryBuildDatetime
	P.FUNCTIONS["DATETIME_ADD"] = buildDateDeltaWithInterval(KDatetimeAdd, "")
	P.FUNCTIONS["DATETIME_DIFF"] = bigqueryBuildDateDiff(KDatetimeDiff)
	P.FUNCTIONS["DATETIME_SUB"] = buildDateDeltaWithInterval(KDatetimeSub, "")
	P.FUNCTIONS["DIV"] = binaryFromFunction(KIntDiv)
	P.FUNCTIONS["EDIT_DISTANCE"] = bigqueryBuildLevenshtein
	P.FUNCTIONS["EMBED"] = fromArgList(KAIEmbed)
	P.FUNCTIONS["FORMAT_DATE"] = bigqueryBuildFormatTime(KTsOrDsToDate)
	P.FUNCTIONS["GENERATE"] = fromArgList(KAIGenerate)
	P.FUNCTIONS["GENERATE_ARRAY"] = fromArgList(KGenerateSeries)
	P.FUNCTIONS["JSON_EXTRACT_SCALAR"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractScalar)
	P.FUNCTIONS["JSON_EXTRACT_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractArray)
	P.FUNCTIONS["JSON_EXTRACT_STRING_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONValueArray)
	P.FUNCTIONS["JSON_KEYS"] = fromArgList(KJSONKeysAtDepth)
	P.FUNCTIONS["JSON_QUERY"] = buildExtractJSONWithPath(KJSONExtract)
	P.FUNCTIONS["JSON_QUERY_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractArray)
	P.FUNCTIONS["JSON_STRIP_NULLS"] = bigqueryBuildJSONStripNulls
	P.FUNCTIONS["JSON_VALUE"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractScalar)
	P.FUNCTIONS["JSON_VALUE_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONValueArray)
	P.FUNCTIONS["LENGTH"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KLength, "this", seqGet(args, 0), "binary", true)
	}
	P.FUNCTIONS["MD5"] = fromArgList(KMD5Digest)
	P.FUNCTIONS["SHA1"] = fromArgList(KSHA1Digest)
	P.FUNCTIONS["NORMALIZE_AND_CASEFOLD"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KNormalize, "this", seqGet(args, 0), "form", seqGet(args, 1), "is_casefold", true)
	}
	P.FUNCTIONS["OCTET_LENGTH"] = fromArgList(KByteLength)
	P.FUNCTIONS["TO_HEX"] = bigqueryBuildToHex
	P.FUNCTIONS["PARSE_DATE"] = bigqueryBuildParseDate
	P.FUNCTIONS["PARSE_TIME"] = func(args []*Expr, d *Dialect) *Expr {
		return buildFormattedTime(KParseTime, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	}
	P.FUNCTIONS["PARSE_TIMESTAMP"] = bigqueryBuildParseTimestamp
	P.FUNCTIONS["PARSE_DATETIME"] = bigqueryBuildParseDatetime
	P.FUNCTIONS["REGEXP_CONTAINS"] = fromArgList(KRegexpLike)
	P.FUNCTIONS["REGEXP_EXTRACT"] = bigqueryBuildRegexpExtract(KRegexpExtract, nil)
	P.FUNCTIONS["REGEXP_SUBSTR"] = bigqueryBuildRegexpExtract(KRegexpExtract, nil)
	P.FUNCTIONS["REGEXP_EXTRACT_ALL"] = bigqueryBuildRegexpExtract(KRegexpExtractAll, func() *Expr { return LiteralInt(0) })
	P.FUNCTIONS["SHA256"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2Digest, "this", seqGet(args, 0), "length", LiteralInt(256))
	}
	P.FUNCTIONS["SHA512"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2Digest, "this", seqGet(args, 0), "length", LiteralInt(512))
	}
	P.FUNCTIONS["SIMILARITY"] = fromArgList(KAISimilarity)
	P.FUNCTIONS["SPLIT"] = func(args []*Expr, _ *Dialect) *Expr {
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
		expression := seqGet(args, 1)
		if expression == nil {
			expression = LiteralString(",")
		}
		return New(KSplit, "this", seqGet(args, 0), "expression", expression)
	}
	P.FUNCTIONS["STRPOS"] = fromArgList(KStrPosition)
	P.FUNCTIONS["TIME"] = bigqueryBuildTime
	P.FUNCTIONS["TIME_ADD"] = buildDateDeltaWithInterval(KTimeAdd, "")
	P.FUNCTIONS["TIME_SUB"] = buildDateDeltaWithInterval(KTimeSub, "")
	P.FUNCTIONS["TIMESTAMP"] = bigqueryBuildTimestamp
	P.FUNCTIONS["TIMESTAMP_ADD"] = buildDateDeltaWithInterval(KTimestampAdd, "")
	P.FUNCTIONS["TIMESTAMP_SUB"] = buildDateDeltaWithInterval(KTimestampSub, "")
	P.FUNCTIONS["TIMESTAMP_MICROS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnixToTime, "this", seqGet(args, 0), "scale", LiteralInt(6)) // microseconds
	}
	P.FUNCTIONS["TIMESTAMP_MILLIS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnixToTime, "this", seqGet(args, 0), "scale", LiteralInt(3)) // milliseconds
	}
	P.FUNCTIONS["TIMESTAMP_SECONDS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnixToTime, "this", seqGet(args, 0))
	}
	P.FUNCTIONS["TO_JSON"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KJSONFormat, "this", seqGet(args, 0), "options", seqGet(args, 1), "to_json", true)
	}
	P.FUNCTIONS["TO_JSON_STRING"] = fromArgList(KJSONFormat)
	P.FUNCTIONS["FORMAT_DATETIME"] = bigqueryBuildFormatTime(KTsOrDsToDatetime)
	P.FUNCTIONS["FORMAT_TIMESTAMP"] = bigqueryBuildFormatTime(KTsOrDsToTimestamp)
	P.FUNCTIONS["FORMAT_TIME"] = bigqueryBuildFormatTime(KTsOrDsToTime)
	P.FUNCTIONS["FROM_HEX"] = fromArgList(KUnhex)
	P.FUNCTIONS["WEEK"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KWeekStart, "this", VarOf(seqGet(args, 0)))
	}

	delete(P.FUNCTION_PARSERS, "TRIM")
	P.FUNCTION_PARSERS["ARRAY"] = func(p *Parser) *Expr {
		return p.expression(New(KArray, "expressions", []*Expr{p.parseStatement()}, "struct_name_inheritance", true))
	}
	P.FUNCTION_PARSERS["JSON_ARRAY"] = func(p *Parser) *Expr {
		return p.expression(New(KJSONArray, "expressions", p.parseCSV(p.parseBitwise, TK_COMMA)))
	}
	P.FUNCTION_PARSERS["MAKE_INTERVAL"] = func(p *Parser) *Expr { return bigqueryParseMakeInterval(p) }
	P.FUNCTION_PARSERS["PREDICT"] = func(p *Parser) *Expr { return bigqueryParseML(p, KPredict) }
	P.FUNCTION_PARSERS["TRANSLATE"] = func(p *Parser) *Expr { return bigqueryParseTranslate(p) }
	P.FUNCTION_PARSERS["FEATURES_AT_TIME"] = func(p *Parser) *Expr { return bigqueryParseFeaturesAtTime(p) }
	P.FUNCTION_PARSERS["GENERATE_EMBEDDING"] = func(p *Parser) *Expr { return bigqueryParseML(p, KGenerateEmbedding) }
	P.FUNCTION_PARSERS["GENERATE_TEXT_EMBEDDING"] = func(p *Parser) *Expr {
		return bigqueryParseML(p, KGenerateEmbedding, "is_text", true)
	}
	P.FUNCTION_PARSERS["GENERATE_TEXT"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateText) }
	P.FUNCTION_PARSERS["GENERATE_TABLE"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateTable) }
	P.FUNCTION_PARSERS["GENERATE_BOOL"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateBool) }
	P.FUNCTION_PARSERS["GENERATE_INT"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateInt) }
	P.FUNCTION_PARSERS["GENERATE_DOUBLE"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateDouble) }
	P.FUNCTION_PARSERS["VECTOR_SEARCH"] = func(p *Parser) *Expr { return bigqueryParseVectorSearch(p) }
	P.FUNCTION_PARSERS["FORECAST"] = func(p *Parser) *Expr { return bigqueryParseForecast(p) }

	P.PROPERTY_PARSERS["OPTIONS"] = noKwargs(func(p *Parser) any { return p.parseWithProperty() })

	P.CONSTRAINT_PARSERS["OPTIONS"] = func(p *Parser) *Expr {
		return New(KProperties, "expressions", p.parseWithProperty())
	}

	delete(P.RANGE_PARSERS, TK_OVERLAPS)

	P.STATEMENT_PARSERS[TK_ELSE] = func(p *Parser) *Expr { return p.parseAsCommand(p.prev) }
	P.STATEMENT_PARSERS[TK_END] = func(p *Parser) *Expr { return p.parseAsCommand(p.prev) }
	P.STATEMENT_PARSERS[TK_FOR] = func(p *Parser) *Expr { return bigqueryParseForIn(p) }
	P.STATEMENT_PARSERS[TK_EXPORT] = func(p *Parser) *Expr { return bigqueryParseExportData(p) }
	P.STATEMENT_PARSERS[TK_DECLARE] = func(p *Parser) *Expr { return p.parseDeclare() }

	P.h.parseTablePart = bigqueryParseTablePart
	P.h.parseTableParts = bigqueryParseTableParts
	P.h.parseColumn = bigqueryParseColumn
	P.h.parseClusterProperty = bigqueryParseClusterProperty
	P.h.parseProperty = bigqueryParseProperty
	P.h.parseJsonObject = bigqueryParseJsonObject
	P.h.parseBracket = bigqueryParseBracket
	P.h.parseUnnest = bigqueryParseUnnest
	P.h.parseColumnOps = bigqueryParseColumnOps
}

func bigqueryParseForIn(p *Parser) *Expr {
	index := p.index
	this := p.parseRange(nil, false)
	p.matchTextSeq("DO")
	if p.match(TK_COMMAND) {
		p.retreat(index)
		return p.parseAsCommand(p.prev)
	}
	return p.expression(New(KForIn, "this", this, "expression", p.parseStatement()))
}

// bigqueryParseProperty parses NOT DETERMINISTIC as a VOLATILE stability property; NOT and
// DETERMINISTIC are separate tokens, so a bare NOT stays an operator elsewhere.
func bigqueryParseProperty(p *Parser) any {
	if p.matchTextSeq("NOT", "DETERMINISTIC") {
		return anyExpr(p.expression(New(KStabilityProperty, "this", LiteralString("VOLATILE"))))
	}

	return p.baseParseProperty()
}

// bigqueryParseTablePart parses one table name part, joining dashed names (my-project) and
// number-led parts into a single identifier.
func bigqueryParseTablePart(p *Parser, schema bool) *Expr {
	this := p.baseParseTablePart(schema)
	if this == nil {
		this = p.parseNumber()
	}

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#table_names
	if this.IsA(KIdentifier) {
		tableName := this.Name()
		var dashedParts strings.Builder
		for p.matchNoAdvance(TK_DASH) && p.next.ok() {
			start := p.curr
			for p.isConnected() && !p.matchSetNoAdvance(p.s.DASHED_TABLE_PART_FOLLOW_TOKENS) {
				p.advance(1)
			}

			if start == p.curr {
				break
			}

			dashedParts.WriteString(p.findSQL(start, p.prev))
		}
		tableName += dashedParts.String()

		this = New(KIdentifier, "this", tableName, "quoted", this.Arg("quoted")).updatePositionsFrom(this)
	} else if this.IsA(KLiteral) {
		tableName := this.Name()

		if p.isConnected() && p.parseVar(true, nil, false) != nil {
			tableName += p.prev.Text
		}

		this = New(KIdentifier, "this", tableName, "quoted", true).updatePositionsFrom(this)
	}

	return this
}

func bigqueryParseTableParts(p *Parser, schema bool, isDbReference bool, wildcard bool, fast bool) *Expr {
	table := p.baseParseTableParts(schema, isDbReference, true, fast)

	if !table.IsA(KTable) {
		return table
	}

	// proj-1.db.tbl -- `1.` is tokenized as a float so we need to unravel it here
	if table.CatalogName() == "" {
		if table.DbName() != "" {
			previousDb := table.ArgE("db")
			parts := strings.Split(table.DbName(), ".")
			if len(parts) == 2 && !previousDb.ArgB("quoted") {
				table.Set("catalog", New(KIdentifier, "this", parts[0]).updatePositionsFrom(previousDb))
				table.Set("db", New(KIdentifier, "this", parts[1]).updatePositionsFrom(previousDb))
			}
		} else {
			previousThis := table.This()
			parts := strings.Split(table.Name(), ".")
			if len(parts) == 2 && !previousThis.ArgB("quoted") {
				table.Set("db", New(KIdentifier, "this", parts[0]).updatePositionsFrom(previousThis))
				table.Set("this", New(KIdentifier, "this", parts[1]).updatePositionsFrom(previousThis))
			}
		}
	}

	var alias *Expr
	anyDot := false
	if table.This().IsA(KIdentifier) {
		for _, part := range table.Parts() {
			if strings.Contains(part.Name(), ".") {
				anyDot = true
				break
			}
		}
	}
	if anyDot {
		alias = table.This()
		ids := bigqueryQuotedIdentifiers(bigquerySplitQualifiedName(bigqueryJoinPartNames(table.Parts()), 3))
		catalog, db, thisID, rest := ids[0], ids[1], ids[2], ids[3:]

		for _, part := range []*Expr{catalog, db, thisID} {
			if part != nil {
				part.updatePositionsFrom(table.This())
			}
		}

		this := thisID
		if len(rest) > 0 && this != nil {
			this = DotBuild(append([]*Expr{this}, rest...))
		}

		table = New(KTable, "this", this, "db", db, "catalog", catalog, "pivots", table.Arg("pivots"))
		table.Meta()["quoted_table"] = true
	}

	// BigQuery INFORMATION_SCHEMA views are qualified by a region or dataset, so without a project
	// the "catalog" part would really be the region/dataset. Fold `INFORMATION_SCHEMA.X` into one
	// quoted identifier so such tables qualify correctly and don't become 4-part references, which
	// break the schema module when mixed with 3-part names.
	// See: https://cloud.google.com/bigquery/docs/information-schema-intro#syntax
	tableParts := table.Parts()
	if len(tableParts) > 1 && pyUpper(tableParts[len(tableParts)-2].Name()) == "INFORMATION_SCHEMA" {
		// Alias the table so existing qualified columns keep working. An explicit alias later in the
		// token stream overwrites this one; otherwise it only exposes the view's own name.
		aliasID := alias
		if aliasID == nil {
			aliasID = tableParts[len(tableParts)-1]
		}
		AliasTableExpr(table, aliasID, nil, nil, false)

		second := tableParts[len(tableParts)-2]
		last := tableParts[len(tableParts)-1]
		infoSchemaView := second.Name() + "." + last.Name()
		newThis := New(KIdentifier, "this", infoSchemaView, "quoted", true)
		m := newThis.Meta()
		m["line"] = second.MetaGet("line")
		m["col"] = last.MetaGet("col")
		m["start"] = second.MetaGet("start")
		m["end"] = last.MetaGet("end")
		table.Set("this", newThis)
		table.Set("db", seqGet(tableParts, -3))
		table.Set("catalog", seqGet(tableParts, -4))
		table.Meta()["quoted_table"] = true
	}

	return table
}

func bigqueryParseColumn(p *Parser) *Expr {
	column := p.baseParseColumn()
	if column.IsA(KColumn) {
		parts := column.Parts()
		anyDot := false
		for _, part := range parts {
			if strings.Contains(part.Name(), ".") {
				anyDot = true
				break
			}
		}
		if anyDot {
			ids := bigqueryQuotedIdentifiers(bigquerySplitQualifiedName(bigqueryJoinPartNames(parts), 4))
			catalog, db, table, thisID, rest := ids[0], ids[1], ids[2], ids[3], ids[4:]

			this := thisID
			if len(rest) > 0 && this != nil {
				this = DotBuild(append([]*Expr{this}, rest...))
			}

			column = New(KColumn, "this", this, "table", table, "db", db, "catalog", catalog)
			column.Meta()["quoted_column"] = true
		}
	}

	return column
}

func bigqueryParseClusterProperty(p *Parser) *Expr {
	return p.expression(New(
		KClusterProperty,
		"expressions", p.parseCSV(p.parseColumn, TK_COMMA),
	))
}

func bigqueryParseJsonObject(p *Parser, agg bool) *Expr {
	jsonObject := p.baseParseJsonObject(false)
	arrayKVPair := seqGet(jsonObject.Expressions(), 0)

	// Converts BQ's "signature 2" of JSON_OBJECT into the canonical key/value representation
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object_signature2
	if arrayKVPair != nil &&
		arrayKVPair.This().IsA(KArray) &&
		arrayKVPair.Expression().IsA(KArray) {
		keys := arrayKVPair.This().Expressions()
		values := arrayKVPair.Expression().Expressions()

		n := min(len(keys), len(values))
		kvs := make([]*Expr, 0, n)
		for i := range n {
			kvs = append(kvs, New(KJSONKeyValue, "this", keys[i], "expression", values[i]))
		}
		jsonObject.Set("expressions", kvs)
	}

	return jsonObject
}

func bigqueryParseBracket(p *Parser, this *Expr) *Expr {
	bracket := p.baseParseBracket(this)

	if bracket.IsA(KArray) {
		bracket.Set("struct_name_inheritance", true)
	}

	if this == bracket {
		return bracket
	}

	if bracket.IsA(KBracket) {
		for _, expression := range bracket.Expressions() {
			name := pyUpper(expression.Name())

			expressions := expression.Expressions()

			bo, ok := bigqueryBracketOffsets[name]
			if !ok || len(expressions) == 0 {
				break
			}

			bracket.Set("offset", bo.offset)
			bracket.Set("safe", bo.safe)
			expression.Replace(expressions[0])
		}
	}

	return bracket
}

func bigqueryParseUnnest(p *Parser, withAlias bool) *Expr {
	unnest := p.baseParseUnnest(withAlias)

	if unnest == nil {
		return nil
	}

	unnestExpr := seqGet(unnest.Expressions(), 0)
	if unnestExpr != nil {
		unnestExpr = annotateTypes(unnestExpr, p.d)

		// Unnesting a nested array (i.e array of structs) explodes the top-level struct fields,
		// in contrast to other dialects such as DuckDB which flattens only the array by default
		if dhIsType(unnestExpr, DT_ARRAY) {
			typ := unnestExpr.RawType()
			if typ == nil {
				typ = unnestExpr.Type()
			}
			for _, arrayElem := range typ.Expressions() {
				if dhIsType(arrayElem, DT_STRUCT) {
					unnest.Set("explode_array", true)
					break
				}
			}
		}
	}

	return unnest
}

func bigqueryParseMakeInterval(p *Parser) *Expr {
	expr := New(KMakeInterval)

	for _, argKey := range bigqueryMakeIntervalKwargs {
		value := p.parseLambda(false)

		if value == nil {
			break
		}

		// Non-named arguments are filled sequentially, (optionally) followed by named arguments
		// that can appear in any order e.g MAKE_INTERVAL(1, minute => 5, day => 2)
		if value.IsA(KKwarg) {
			argKey = value.This().Name()
		}

		expr.Set(argKey, value)

		p.match(TK_COMMA)
	}

	return expr
}

func bigqueryParseML(p *Parser, exprType Kind, kwargs ...any) *Expr {
	p.matchTextSeq("MODEL")
	this := p.parseTable(false, false, nil, false, false, false, false)

	p.match(TK_COMMA)
	p.matchTextSeq("TABLE")

	// Certain functions like ML.FORECAST require a STRUCT argument but not a TABLE/SELECT one
	var expression *Expr
	if !p.matchNoAdvance(TK_STRUCT) {
		expression = p.parseTable(false, false, nil, false, false, false, false)
	}

	p.match(TK_COMMA)

	kv := []any{"this", this, "expression", expression, "params_struct", p.parseBitwise()}
	kv = append(kv, kwargs...)
	return p.expression(New(exprType, kv...))
}

func bigqueryParseGenerate(p *Parser, exprType Kind, kwargs ...any) *Expr {
	p.matchTextSeq("MODEL")
	this := p.parseTable(false, false, nil, false, false, false, false)

	p.match(TK_COMMA)

	var expression *Expr
	if p.matchTextSeq("TABLE") {
		expression = p.parseTable(false, false, nil, false, false, false, false)
	} else if p.matchNoAdvance(TK_L_PAREN) {
		expression = p.parseTable(false, false, nil, false, false, false, false)
	} else {
		expression = p.parseBitwise()
	}

	// false (not nil) when there is no comma.
	var paramsStruct any = false
	if p.match(TK_COMMA) {
		paramsStruct = p.parseBitwise()
	}

	kv := []any{"this", this, "expression", expression, "params_struct", paramsStruct}
	kv = append(kv, kwargs...)
	return p.expression(New(exprType, kv...))
}

func bigqueryTokenAt(p *Parser, i int) *Token {
	if i < 0 {
		i += len(p.tokens)
	}
	if i < 0 || i >= len(p.tokens) {
		return nil
	}
	return p.tokens[i]
}

func bigqueryParseTranslate(p *Parser) *Expr {
	// Check if this is ML.TRANSLATE by looking at previous tokens
	token := bigqueryTokenAt(p, p.index-4)
	if token != nil && pyUpper(token.Text) == "ML" {
		return bigqueryParseML(p, KMLTranslate)
	}

	return FromArgList(KTranslate, p.parseFunctionArgs(false))
}

func bigqueryParseForecast(p *Parser) *Expr {
	// Check if this is ML.FORECAST by looking at previous tokens.
	token := bigqueryTokenAt(p, p.index-4)
	if token != nil && pyUpper(token.Text) == "ML" {
		return bigqueryParseML(p, KMLForecast)
	}

	// AI.FORECAST is a TVF, where the first argument is either TABLE <table>
	// or a parenthesized query statement, followed by named arguments.
	p.match(TK_TABLE)
	this := p.parseTable(false, false, nil, false, false, false, false)
	if this == nil {
		p.raiseError("Expected table or query statement", nil)
	}

	expr := p.expression(New(KAIForecast, "this", this))
	for p.match(TK_COMMA) {
		a := p.parseLambda(false)
		if a.IsA(KKwarg) {
			expr.Set(a.This().Name(), a)
		} else {
			p.raiseError("Expected key => value syntax for AI.FORECAST, got "+bigqueryPyStrExpr(a), nil)
			break
		}
	}

	return expr
}

func bigqueryPyStrExpr(e *Expr) string {
	if e == nil {
		return "None"
	}
	return exprSQL(e)
}

func bigqueryParseFeaturesAtTime(p *Parser) *Expr {
	p.match(TK_TABLE)
	this := p.parseTable(false, false, nil, false, false, false, false)

	expr := p.expression(New(KFeaturesAtTime, "this", this))

	for p.match(TK_COMMA) {
		a := p.parseLambda(false)

		if a != nil {
			expr.Set(a.This().Name(), a)
		}
	}

	return expr
}

func bigqueryParseVectorSearch(p *Parser) *Expr {
	p.match(TK_TABLE)
	baseTable := p.parseTable(false, false, nil, false, false, false, false)

	p.match(TK_COMMA)

	columnToSearch := p.parseBitwise()
	p.match(TK_COMMA)

	p.match(TK_TABLE)
	queryTable := p.parseTable(false, false, nil, false, false, false, false)

	expr := p.expression(New(
		KVectorSearch,
		"this", baseTable,
		"column_to_search", columnToSearch,
		"query_table", queryTable,
	))

	for p.match(TK_COMMA) {
		// query_column_to_search can be named argument or positional
		if p.matchNoAdvance(TK_STRING) {
			queryColumn := p.parseString()
			expr.Set("query_column_to_search", queryColumn)
		} else {
			a := p.parseLambda(false)
			if a != nil {
				expr.Set(a.This().Name(), a)
			}
		}
	}

	return expr
}

func bigqueryParseExportData(p *Parser) *Expr {
	p.matchTextSeq("DATA")

	// connection and this are false (not nil) when their keywords are absent.
	var connection any = false
	if p.matchTextSeq("WITH", "CONNECTION") {
		connection = p.parseTableParts(false, false, false, false)
	}
	options := p.parseProperties(false)
	var this any = false
	if p.matchTextSeq("AS") {
		this = p.parseSelect(true, false, true, true, true, nil)
	}

	return p.expression(New(
		KExport,
		"connection", connection,
		"options", options,
		"this", this,
	))
}

func bigqueryParseColumnOps(p *Parser, this *Expr) *Expr {
	funcIndex := p.index + 1
	this = p.baseParseColumnOps(this)

	if this.IsA(KDot) && this.Expression().IsA(KFunc) {
		prefix := pyUpper(this.This().Name())

		var fn Kind
		hasFn := false
		if prefix == "NET" {
			fn, hasFn = KNetFunc, true
		} else if prefix == "SAFE" {
			fn, hasFn = KSafeFunc, true
		}

		if hasFn {
			// Re-parse as a known function: the base column ops parser parses functions in anonymous
			// mode.
			p.retreat(funcIndex)
			this = New(fn, "this", p.parseFunction(nil, false, true, true))
		} else if prefix == "AI" || prefix == "ML" {
			// AI.* and ML.* function calls can use custom BigQuery signatures that rely on
			// function parsers, so re-parse the function in non-anonymous mode.
			p.retreat(funcIndex)
			parsed := p.parseFunction(nil, false, true, true)
			if parsed != nil {
				this = p.expression(New(KDot, "this", this.This(), "expression", parsed))
			}
		}
	}

	return this
}
