package sqlengine

import (
	"strings"
)

func init() { registerCustomizer("mysql", customizeMySQL) }

// Virtual dispatch for MySQL-only parser methods that derived dialects (Doris, StarRocks)
// override. These methods have no hook in parserHooks, so the overrides are registered here
// (in init functions) keyed by dialect name and resolved along the dialect's ancestry.

var mysqlPartitionPropertyImpls = map[string]func(p *Parser) any{}

var mysqlPartitionRangeValueImpls = map[string]func(p *Parser) *Expr{}

// mysqlResolveImpl finds the override registered for d or, failing that, for its closest
// ancestor dialect.
func mysqlResolveImpl[F any](d *Dialect, impls map[string]F) (F, bool) {
	if f, ok := impls[d.Name]; ok {
		return f, true
	}
	for _, parent := range d.parents {
		if f, ok := impls[parent]; ok {
			return f, true
		}
	}
	var zero F
	return zero, false
}

func mysqlCallParsePartitionProperty(p *Parser) any {
	if f, ok := mysqlResolveImpl(p.d, mysqlPartitionPropertyImpls); ok {
		return f(p)
	}
	return mysqlParsePartitionProperty(p)
}

func mysqlCallParsePartitionRangeValue(p *Parser) *Expr {
	if f, ok := mysqlResolveImpl(p.d, mysqlPartitionRangeValueImpls); ok {
		return f(p)
	}
	return mysqlParsePartitionRangeValue(p)
}

// mysqlTimeSpecifiers holds the format specifiers for time parts (as opposed to date parts).
// https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
var mysqlTimeSpecifiers = map[rune]bool{
	'f': true, 'H': true, 'h': true, 'I': true, 'i': true, 'k': true,
	'l': true, 'p': true, 'r': true, 'S': true, 's': true, 'T': true,
}

func mysqlHasTimeSpecifier(dateFormat string) bool {
	r := []rune(dateFormat)
	i := 0
	length := len(r)

	for i < length {
		if r[i] == '%' {
			i++
			if i < length && mysqlTimeSpecifiers[r[i]] {
				return true
			}
		}
		i++
	}
	return false
}

func mysqlBuildTimestampadd(args []*Expr, d *Dialect) *Expr {
	expression := buildDateDelta(KTimestampAdd, nil, "DAY", false)(args, d)
	name := "DAY"
	if unit := expression.ArgE("unit"); unit != nil {
		name = unit.Name()
	}
	expression.Set("unit", VarChecked(strings.TrimPrefix(pyUpper(name), "SQL_TSI_")))
	return expression
}

func mysqlStrToDate(args []*Expr, d *Dialect) *Expr {
	mysqlDateFormat := seqGet(args, 1)
	dateFormat := MustDialect("mysql").formatTimeExpr(mysqlDateFormat)
	this := seqGet(args, 0)

	if mysqlDateFormat != nil && mysqlHasTimeSpecifier(mysqlDateFormat.Name()) {
		return New(KStrToTime, "this", this, "format", dateFormat)
	}

	return New(KStrToDate, "this", this, "format", dateFormat)
}

// mysqlShowParser returns a parser for SHOW this. target is nil/false, true or a string.
func mysqlShowParser(this string, target any, full any, global any) parseFn {
	return func(p *Parser) *Expr {
		return mysqlParseShowMySQL(p, this, target, full, global)
	}
}

func customizeMySQL(d *Dialect) {
	P := d.P

	P.RANGE_PARSERS[TK_MEMBER_OF] = func(p *Parser, this *Expr) *Expr {
		return p.expression(New(
			KJSONArrayContains,
			"this", this,
			"expression", p.parseWrapped(func() *Expr { return p.parseExpression() }, false),
		))
	}

	P.UNARY_PARSERS[TK_NOT] = mysqlParseNot

	tsOrDsToDate0 := func(args []*Expr) *Expr { return New(KTsOrDsToDate, "this", seqGet(args, 0)) }
	P.FUNCTIONS["BIT_AND"] = fromArgList(KBitwiseAndAgg)
	P.FUNCTIONS["BIT_OR"] = fromArgList(KBitwiseOrAgg)
	P.FUNCTIONS["BIT_XOR"] = fromArgList(KBitwiseXorAgg)
	P.FUNCTIONS["BIT_COUNT"] = fromArgList(KBitwiseCount)
	P.FUNCTIONS["CONVERT_TZ"] = func(args []*Expr, d *Dialect) *Expr {
		return New(
			KConvertTimezone,
			"source_tz", seqGet(args, 1),
			"target_tz", seqGet(args, 2),
			"timestamp", seqGet(args, 0),
		)
	}
	P.FUNCTIONS["CURDATE"] = fromArgList(KCurrentDate)
	P.FUNCTIONS["CURTIME"] = fromArgList(KCurrentTime)
	P.FUNCTIONS["DATE"] = func(args []*Expr, d *Dialect) *Expr { return tsOrDsToDate0(args) }
	P.FUNCTIONS["DATEDIFF"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDateDiff, "this", seqGet(args, 0), "expression", seqGet(args, 1), "date_part_boundary", true)
	}
	P.FUNCTIONS["DATE_ADD"] = buildDateDeltaWithInterval(KDateAdd, "")
	P.FUNCTIONS["DATE_FORMAT"] = func(args []*Expr, d *Dialect) *Expr {
		return New(
			KTimeToStr,
			"this", New(KTsOrDsToTimestamp, "this", seqGet(args, 0)),
			"format", MustDialect("mysql").formatTimeExpr(seqGet(args, 1)),
		)
	}
	P.FUNCTIONS["DATE_SUB"] = buildDateDeltaWithInterval(KDateSub, "")
	P.FUNCTIONS["DAY"] = func(args []*Expr, d *Dialect) *Expr { return New(KDay, "this", tsOrDsToDate0(args)) }
	P.FUNCTIONS["DAYOFMONTH"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDayOfMonth, "this", tsOrDsToDate0(args))
	}
	P.FUNCTIONS["DAYOFWEEK"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDayOfWeek, "this", tsOrDsToDate0(args))
	}
	P.FUNCTIONS["DAYOFYEAR"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDayOfYear, "this", tsOrDsToDate0(args))
	}
	P.FUNCTIONS["FORMAT"] = fromArgList(KNumberToStr)
	P.FUNCTIONS["FROM_UNIXTIME"] = buildFormattedTime(KUnixToTime, "", nil)
	P.FUNCTIONS["ISNULL"] = isnullToIsNull
	P.FUNCTIONS["LENGTH"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KLength, "this", seqGet(args, 0), "binary", true)
	}
	P.FUNCTIONS["MAKETIME"] = fromArgList(KTimeFromParts)
	P.FUNCTIONS["MONTH"] = func(args []*Expr, d *Dialect) *Expr { return New(KMonth, "this", tsOrDsToDate0(args)) }
	P.FUNCTIONS["MONTHNAME"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KTimeToStr, "this", tsOrDsToDate0(args), "format", LiteralString("%B"))
	}
	P.FUNCTIONS["SCHEMA"] = fromArgList(KCurrentSchema)
	P.FUNCTIONS["DATABASE"] = fromArgList(KCurrentSchema)
	P.FUNCTIONS["STD"] = fromArgList(KStddevPop)
	P.FUNCTIONS["STDDEV"] = fromArgList(KStddevPop)
	P.FUNCTIONS["STR_TO_DATE"] = mysqlStrToDate
	P.FUNCTIONS["TIMESTAMPADD"] = mysqlBuildTimestampadd
	P.FUNCTIONS["TIMESTAMPDIFF"] = buildDateDelta(KTimestampDiff, nil, "DAY", false)
	P.FUNCTIONS["TO_DAYS"] = func(args []*Expr, d *Dialect) *Expr {
		diff := New(
			KDateDiff,
			"this", tsOrDsToDate0(args),
			"expression", New(KTsOrDsToDate, "this", LiteralString("0000-01-01")),
			"unit", VarExpr("DAY"),
		)
		// (DATEDIFF(x, '0000-01-01') + 1)
		return Paren(New(KAdd, "this", diff, "expression", LiteralInt(1)))
	}
	P.FUNCTIONS["VARIANCE"] = fromArgList(KVariancePop)
	P.FUNCTIONS["VERSION"] = fromArgList(KCurrentVersion)
	P.FUNCTIONS["WEEK"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KWeek, "this", tsOrDsToDate0(args), "mode", seqGet(args, 1))
	}
	P.FUNCTIONS["WEEKOFYEAR"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KWeekOfYear, "this", tsOrDsToDate0(args))
	}
	P.FUNCTIONS["YEAR"] = func(args []*Expr, d *Dialect) *Expr { return New(KYear, "this", tsOrDsToDate0(args)) }

	P.FUNCTION_PARSERS["GROUP_CONCAT"] = func(p *Parser) *Expr { return p.parseGroupConcat() }
	// https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	P.FUNCTION_PARSERS["VALUES"] = func(p *Parser) *Expr {
		return p.expression(New(KAnonymous, "this", "VALUES", "expressions", []*Expr{p.parseIdVar(true, nil)}))
	}
	P.FUNCTION_PARSERS["JSON_VALUE"] = func(p *Parser) *Expr { return p.parseJsonValue(true) }
	P.FUNCTION_PARSERS["SUBSTR"] = func(p *Parser) *Expr { return p.parseSubstring() }

	P.STATEMENT_PARSERS[TK_SHOW] = func(p *Parser) *Expr { return p.parseShow() }

	// A fresh map, not merged with the parent's.
	P.SHOW_PARSERS = map[string]parseFn{
		"BINARY LOGS":       mysqlShowParser("BINARY LOGS", nil, nil, nil),
		"MASTER LOGS":       mysqlShowParser("BINARY LOGS", nil, nil, nil),
		"BINLOG EVENTS":     mysqlShowParser("BINLOG EVENTS", nil, nil, nil),
		"CHARACTER SET":     mysqlShowParser("CHARACTER SET", nil, nil, nil),
		"CHARSET":           mysqlShowParser("CHARACTER SET", nil, nil, nil),
		"COLLATION":         mysqlShowParser("COLLATION", nil, nil, nil),
		"FULL COLUMNS":      mysqlShowParser("COLUMNS", "FROM", true, nil),
		"COLUMNS":           mysqlShowParser("COLUMNS", "FROM", nil, nil),
		"CREATE DATABASE":   mysqlShowParser("CREATE DATABASE", true, nil, nil),
		"CREATE EVENT":      mysqlShowParser("CREATE EVENT", true, nil, nil),
		"CREATE FUNCTION":   mysqlShowParser("CREATE FUNCTION", true, nil, nil),
		"CREATE PROCEDURE":  mysqlShowParser("CREATE PROCEDURE", true, nil, nil),
		"CREATE TABLE":      mysqlShowParser("CREATE TABLE", true, nil, nil),
		"CREATE TRIGGER":    mysqlShowParser("CREATE TRIGGER", true, nil, nil),
		"CREATE VIEW":       mysqlShowParser("CREATE VIEW", true, nil, nil),
		"DATABASES":         mysqlShowParser("DATABASES", nil, nil, nil),
		"SCHEMAS":           mysqlShowParser("DATABASES", nil, nil, nil),
		"ENGINE":            mysqlShowParser("ENGINE", true, nil, nil),
		"STORAGE ENGINES":   mysqlShowParser("ENGINES", nil, nil, nil),
		"ENGINES":           mysqlShowParser("ENGINES", nil, nil, nil),
		"ERRORS":            mysqlShowParser("ERRORS", nil, nil, nil),
		"EVENTS":            mysqlShowParser("EVENTS", nil, nil, nil),
		"FUNCTION CODE":     mysqlShowParser("FUNCTION CODE", true, nil, nil),
		"FUNCTION STATUS":   mysqlShowParser("FUNCTION STATUS", nil, nil, nil),
		"GRANTS":            mysqlShowParser("GRANTS", "FOR", nil, nil),
		"INDEX":             mysqlShowParser("INDEX", "FROM", nil, nil),
		"MASTER STATUS":     mysqlShowParser("MASTER STATUS", nil, nil, nil),
		"OPEN TABLES":       mysqlShowParser("OPEN TABLES", nil, nil, nil),
		"PLUGINS":           mysqlShowParser("PLUGINS", nil, nil, nil),
		"PROCEDURE CODE":    mysqlShowParser("PROCEDURE CODE", true, nil, nil),
		"PROCEDURE STATUS":  mysqlShowParser("PROCEDURE STATUS", nil, nil, nil),
		"PRIVILEGES":        mysqlShowParser("PRIVILEGES", nil, nil, nil),
		"FULL PROCESSLIST":  mysqlShowParser("PROCESSLIST", nil, true, nil),
		"PROCESSLIST":       mysqlShowParser("PROCESSLIST", nil, nil, nil),
		"PROFILE":           mysqlShowParser("PROFILE", nil, nil, nil),
		"PROFILES":          mysqlShowParser("PROFILES", nil, nil, nil),
		"RELAYLOG EVENTS":   mysqlShowParser("RELAYLOG EVENTS", nil, nil, nil),
		"REPLICAS":          mysqlShowParser("REPLICAS", nil, nil, nil),
		"SLAVE HOSTS":       mysqlShowParser("REPLICAS", nil, nil, nil),
		"REPLICA STATUS":    mysqlShowParser("REPLICA STATUS", nil, nil, nil),
		"SLAVE STATUS":      mysqlShowParser("REPLICA STATUS", nil, nil, nil),
		"GLOBAL STATUS":     mysqlShowParser("STATUS", nil, nil, true),
		"SESSION STATUS":    mysqlShowParser("STATUS", nil, nil, nil),
		"STATUS":            mysqlShowParser("STATUS", nil, nil, nil),
		"TABLE STATUS":      mysqlShowParser("TABLE STATUS", nil, nil, nil),
		"FULL TABLES":       mysqlShowParser("TABLES", nil, true, nil),
		"TABLES":            mysqlShowParser("TABLES", nil, nil, nil),
		"TRIGGERS":          mysqlShowParser("TRIGGERS", nil, nil, nil),
		"GLOBAL VARIABLES":  mysqlShowParser("VARIABLES", nil, nil, true),
		"SESSION VARIABLES": mysqlShowParser("VARIABLES", nil, nil, nil),
		"VARIABLES":         mysqlShowParser("VARIABLES", nil, nil, nil),
		"WARNINGS":          mysqlShowParser("WARNINGS", nil, nil, nil),
	}

	P.PROPERTY_PARSERS["LOCK"] = noKwargs(func(p *Parser) any {
		return anyExpr(p.parsePropertyAssignment(KLockProperty))
	})
	P.PROPERTY_PARSERS["PARTITION BY"] = noKwargs(func(p *Parser) any {
		return mysqlCallParsePartitionProperty(p)
	})

	P.SET_PARSERS["PERSIST"] = func(p *Parser) *Expr { return p.parseSetItemAssignment("PERSIST") }
	P.SET_PARSERS["PERSIST_ONLY"] = func(p *Parser) *Expr { return p.parseSetItemAssignment("PERSIST_ONLY") }
	P.SET_PARSERS["CHARACTER SET"] = func(p *Parser) *Expr { return mysqlParseSetItemCharset(p, "CHARACTER SET") }
	P.SET_PARSERS["CHARSET"] = func(p *Parser) *Expr { return mysqlParseSetItemCharset(p, "CHARACTER SET") }
	P.SET_PARSERS["NAMES"] = func(p *Parser) *Expr { return mysqlParseSetItemNames(p) }

	P.CONSTRAINT_PARSERS["BINARY"] = func(p *Parser) *Expr { return p.expression(New(KBinaryColumnConstraint)) }
	P.CONSTRAINT_PARSERS["FULLTEXT"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "FULLTEXT") }
	P.CONSTRAINT_PARSERS["INDEX"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "") }
	P.CONSTRAINT_PARSERS["KEY"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "") }
	P.CONSTRAINT_PARSERS["SPATIAL"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "SPATIAL") }
	P.CONSTRAINT_PARSERS["ZEROFILL"] = func(p *Parser) *Expr { return p.expression(New(KZeroFillColumnConstraint)) }
	P.CONSTRAINT_PARSERS["INVISIBLE"] = func(p *Parser) *Expr { return p.expression(New(KInvisibleColumnConstraint)) }
	// MySQL defines ASCII as latin1, not the ascii character set, and UNICODE as ucs2
	P.CONSTRAINT_PARSERS["ASCII"] = func(p *Parser) *Expr {
		return p.expression(New(KCharacterSetColumnConstraint, "this", VarExpr("latin1")))
	}
	P.CONSTRAINT_PARSERS["UNICODE"] = func(p *Parser) *Expr {
		return p.expression(New(KCharacterSetColumnConstraint, "this", VarExpr("ucs2")))
	}

	P.ALTER_PARSERS["CHANGE"] = func(p *Parser) any { return anyExpr(mysqlParseAlterTableModify(p, true)) }
	P.ALTER_PARSERS["MODIFY"] = func(p *Parser) any { return anyExpr(mysqlParseAlterTableModify(p, false)) }
	P.ALTER_PARSERS["AUTO_INCREMENT"] = func(p *Parser) any {
		return anyExpr(p.parsePropertyAssignment(KAutoIncrementProperty))
	}

	P.ALTER_ALTER_PARSERS["INDEX"] = func(p *Parser) *Expr { return mysqlParseAlterTableAlterIndex(p) }

	P.h.parseAlterTableRename = mysqlParseAlterTableRename
	P.h.parseRange = mysqlParseRange
	P.h.parseColumnConstraint = mysqlParseColumnConstraint
	P.h.parseAlterDropAction = mysqlParseAlterDropAction
	P.h.parseGeneratedAsIdentity = mysqlParseGeneratedAsIdentity
	P.h.parsePrimaryKeyPart = mysqlParsePrimaryKeyPart
	P.h.parseCharsetName = mysqlParseCharsetName
	P.h.parseType = mysqlParseType
	P.h.parsePrimaryKey = mysqlParsePrimaryKey
	P.h.parseUnique = mysqlParseUnique
	P.h.parseForeignKey = mysqlParseForeignKey

	customizeMySQLGenerator(d)
}

// mysqlParseNot parses the operand of NOT or !. ! binds more tightly than NOT, so its operand is
// a unary expression and the result is parenthesized to keep that grouping when generated as NOT.
func mysqlParseNot(p *Parser) *Expr {
	if p.prev.Text == "!" {
		return p.expression(New(KParen, "this", p.expression(New(KNot, "this", p.parseUnary()))))
	}
	return p.expression(New(KNot, "this", p.parseEquality()))
}

// mysqlParseRange parses a SOUNDS LIKE b as SOUNDEX(a) = SOUNDEX(b), with bitwise operands, before
// the base range operators.
func mysqlParseRange(p *Parser, this *Expr, parseIs bool) *Expr {
	if this == nil {
		this = p.parseBitwise()
	}

	if p.matchTextSeq("SOUNDS", "LIKE") {
		this = p.expression(New(
			KEQ,
			"this", p.expression(New(KSoundex, "this", this)),
			"expression", p.expression(New(KSoundex, "this", p.parseBitwise())),
		))
		// MySQL evaluates = and IS left to right because they have the same precedence.
		if p.matchNoAdvance(TK_IS) {
			this = p.expression(New(KParen, "this", this))
		}
	}

	return p.baseParseRange(this, parseIs)
}

// mysqlParseColumnConstraint parses a column constraint, skipping SIGNED (the default for numeric
// types, which MySQL ignores) and reading a bare KEY as PRIMARY KEY.
func mysqlParseColumnConstraint(p *Parser) *Expr {
	p.matchTextSeq("SIGNED")
	if p.match(TK_KEY) {
		return p.expression(New(KColumnConstraint, "kind", p.parsePrimaryKey(false, false, false)))
	}
	return p.baseParseColumnConstraint()
}

func mysqlParseAlterTableRename(p *Parser) *Expr {
	if p.matchTexts("INDEX", "KEY") {
		old := p.parseIdVar(true, nil)
		p.matchTextSeq("TO")
		newName := p.parseIdVar(true, nil)
		return p.expression(New(KRenameIndex, "this", old, "to", newName))
	}
	return p.baseParseAlterTableRename()
}

func mysqlParseAlterDropAction(p *Parser) *Expr {
	if p.matchPair(TK_DROP, TK_PRIMARY_KEY) {
		return p.expression(New(KDropPrimaryKey))
	}
	if p.matchPair(TK_DROP, TK_KEY) {
		return p.parseDrop(false, "INDEX")
	}
	if p.matchTextSeq("DROP", "CHECK") {
		return p.parseDrop(false, "CHECK")
	}
	return p.baseParseAlterDropAction()
}

func mysqlParseAlterTableModify(p *Parser, rename bool) *Expr {
	// MODIFY [COLUMN]            col_name      column_definition [FIRST | AFTER col_name]
	// CHANGE [COLUMN] old_col_name new_col_name column_definition [FIRST | AFTER col_name]
	p.match(TK_COLUMN)

	column := p.parseField(true, nil, false)
	if column == nil {
		return nil
	}

	var renameFrom *Expr
	if rename {
		renameFrom = column
		column = p.parseField(true, nil, false)
		if column == nil {
			return nil
		}
	}

	columnDef := p.parseColumnDef(column, true)
	if !columnDef.IsA(KColumnDef) {
		return nil
	}

	return p.expression(New(KModifyColumn, "this", columnDef, "rename_from", renameFrom))
}

func mysqlParseGeneratedAsIdentity(p *Parser) *Expr {
	this := p.baseParseGeneratedAsIdentity()

	if p.matchTexts("STORED", "VIRTUAL") {
		persisted := upperText(p.prev) == "STORED"

		if this.IsA(KComputedColumnConstraint) {
			this.Set("persisted", persisted)
		} else if this.IsA(KGeneratedAsIdentityColumnConstraint) {
			this = p.expression(New(KComputedColumnConstraint, "this", this.Expression(), "persisted", persisted))
		}
	}

	return this
}

func mysqlParsePrimaryKeyPart(p *Parser) *Expr {
	this := p.parseIdVar(true, nil)
	if !p.match(TK_L_PAREN) {
		return this
	}

	expression := p.parseNumber()
	p.matchRParen(nil)
	return p.expression(New(KColumnPrefix, "this", this, "expression", expression))
}

// mysqlParseIndexConstraintPart parses a key part, {col_name [(length)] | (expr)}: only a prefixed
// column is followed by "(".
func mysqlParseIndexConstraintPart(p *Parser) *Expr {
	if !p.matchNoAdvance(TK_L_PAREN) && p.next.ok() && p.next.Type == TK_L_PAREN {
		return p.parsePrimaryKeyPart()
	}
	return p.parseDisjunction()
}

// mysqlParseIndexKeyPart parses a key part with an optional ASC or DESC. The Ordered wrapper is
// only kept for an explicit direction, since its implicit null ordering would otherwise show up
// as NULLS FIRST in other dialects.
func mysqlParseIndexKeyPart(p *Parser) *Expr {
	ordered := p.parseOrdered(func() *Expr { return mysqlParseIndexConstraintPart(p) })
	if ordered.IsA(KOrdered) && ordered.Arg("desc") == nil {
		return ordered.This()
	}
	return ordered
}

// mysqlParseUnique parses UNIQUE [INDEX | KEY] [index_name] [index_type] (key_part, ...)
// [index_option] ..., or a bare UNIQUE column constraint.
func mysqlParseUnique(p *Parser) *Expr {
	p.matchTexts("KEY", "INDEX")
	this := p.parseUniqueKey()
	indexType := mysqlParseIndexType(p)

	if !p.matchNoAdvance(TK_L_PAREN) {
		return p.expression(New(KUniqueColumnConstraint, "this", this, "index_type", indexType))
	}

	expressions := p.parseWrappedCSV(func() *Expr { return mysqlParseIndexKeyPart(p) }, TK_COMMA, false)
	if indexType == nil {
		indexType = mysqlParseIndexType(p)
	}

	return p.expression(New(
		KUniqueColumnConstraint,
		"this", p.expression(New(KSchema, "this", this, "expressions", expressions)),
		"index_type", indexType,
		"options", mysqlParseIndexConstraintOptions(p),
	))
}

func mysqlParseIndexType(p *Parser) any {
	if p.match(TK_USING) && p.advanceAny(false) != nil {
		return p.prev.Text
	}
	return nil
}

// mysqlParseIndexConstraint parses an INDEX, KEY, FULLTEXT or SPATIAL column constraint with its
// USING type and index options. kind "" means a plain index.
func mysqlParseIndexConstraint(p *Parser, kind string) *Expr {
	if kind != "" {
		p.matchTexts("INDEX", "KEY")
	}

	this := p.parseIdVar(false, nil)

	// false without USING, nil when nothing follows USING, otherwise the following token's text.
	var indexType any = false
	if p.match(TK_USING) {
		if p.advanceAny(false) != nil {
			indexType = p.prev.Text
		} else {
			indexType = nil
		}
	}
	expressions := p.parseWrappedCSV(func() *Expr { return mysqlParseIndexKeyPart(p) }, TK_COMMA, false)

	var kindArg any
	if kind != "" {
		kindArg = kind
	}
	return p.expression(New(
		KIndexColumnConstraint,
		"this", this,
		"expressions", expressions,
		"kind", kindArg,
		"index_type", indexType,
		"options", mysqlParseIndexConstraintOptions(p),
	))
}

func mysqlParseIndexConstraintOptions(p *Parser) []*Expr {
	options := []*Expr{}
	for {
		var opt *Expr
		if p.matchTextSeq("KEY_BLOCK_SIZE") {
			p.match(TK_EQ)
			opt = New(KIndexConstraintOption, "key_block_size", p.parseNumber())
		} else if p.match(TK_USING) {
			var using any
			if p.advanceAny(false) != nil {
				using = p.prev.Text
			}
			opt = New(KIndexConstraintOption, "using", using)
		} else if p.matchTextSeq("WITH", "PARSER") {
			opt = New(KIndexConstraintOption, "parser", p.parseVar(true, nil, false))
		} else if p.match(TK_COMMENT) {
			opt = New(KIndexConstraintOption, "comment", p.parseString())
		} else if p.matchTextSeq("VISIBLE") {
			opt = New(KIndexConstraintOption, "visible", true)
		} else if p.matchTextSeq("INVISIBLE") {
			opt = New(KIndexConstraintOption, "visible", false)
		} else if p.matchTextSeq("ENGINE_ATTRIBUTE") {
			p.match(TK_EQ)
			opt = New(KIndexConstraintOption, "engine_attr", p.parseString())
		} else if p.matchTextSeq("SECONDARY_ENGINE_ATTRIBUTE") {
			p.match(TK_EQ)
			opt = New(KIndexConstraintOption, "secondary_engine_attr", p.parseString())
		} else {
			opt = nil
		}

		if opt == nil {
			break
		}

		options = append(options, opt)
	}
	return options
}

// mysqlParseShowMySQL parses the remainder of a SHOW statement of kind this. target is nil/false,
// true or a string; full and global are nil or true.
func mysqlParseShowMySQL(p *Parser, this string, target any, full any, global any) *Expr {
	json := p.matchTextSeq("JSON")

	var targetID *Expr
	if truthy(target) {
		if s, ok := target.(string); ok {
			p.matchTextSeq(strings.Split(s, " ")...)
		}
		targetID = p.parseIdVar(true, nil)
	}

	index := p.index
	var log *Expr
	if p.matchTextSeq("IN") {
		log = p.parseString()
		if log == nil {
			p.retreat(index)
		}
	}

	var position, db *Expr
	if this == "BINLOG EVENTS" || this == "RELAYLOG EVENTS" {
		if p.matchTextSeq("FROM") {
			position = p.parseNumber()
		}
	} else {
		if p.match(TK_FROM) || p.matchTextSeq("IN") {
			db = p.parseIdVar(true, nil)
		} else if p.match(TK_DOT) {
			db = targetID
			targetID = p.parseIdVar(true, nil)
		}
	}

	var channel *Expr
	if p.matchTextSeq("FOR", "CHANNEL") {
		channel = p.parseIdVar(true, nil)
	}

	var like *Expr
	if p.matchTextSeq("LIKE") {
		like = p.parseString()
	}
	where := p.parseWhere(false)

	var types any
	var query, offset, limit *Expr
	if this == "PROFILE" {
		types = p.parseCSV(func() *Expr { return p.parseVarFromOptions(p.s.PROFILE_TYPES, true) }, TK_COMMA)
		if p.matchTextSeq("FOR", "QUERY") {
			query = p.parseNumber()
		}
		if p.matchTextSeq("OFFSET") {
			offset = p.parseNumber()
		}
		if p.matchTextSeq("LIMIT") {
			limit = p.parseNumber()
		}
	} else {
		offset, limit = mysqlParseOldstyleLimit(p)
	}

	var mutex any
	if p.matchTextSeq("MUTEX") {
		mutex = true
	}
	if p.matchTextSeq("STATUS") {
		mutex = false
	}

	var forTable, forGroup, forUser, forRole, intoOutfile *Expr
	if p.matchTextSeq("FOR", "TABLE") {
		forTable = p.parseIdVar(true, nil)
	}
	if p.matchTextSeq("FOR", "GROUP") {
		forGroup = p.parseString()
	}
	if p.matchTextSeq("FOR", "USER") {
		forUser = p.parseString()
	}
	if p.matchTextSeq("FOR", "ROLE") {
		forRole = p.parseString()
	}
	if p.matchTextSeq("INTO", "OUTFILE") {
		intoOutfile = p.parseString()
	}

	return p.expression(New(
		KShow,
		"this", this,
		"target", targetID,
		"full", full,
		"log", log,
		"position", position,
		"db", db,
		"channel", channel,
		"like", like,
		"where", where,
		"types", types,
		"query", query,
		"offset", offset,
		"limit", limit,
		"mutex", mutex,
		"for_table", forTable,
		"for_group", forGroup,
		"for_user", forUser,
		"for_role", forRole,
		"into_outfile", intoOutfile,
		"json", json,
		"global_", global,
	))
}

// mysqlParseOldstyleLimit parses LIMIT n or LIMIT offset, n; returns (offset, limit).
func mysqlParseOldstyleLimit(p *Parser) (*Expr, *Expr) {
	var limit, offset *Expr
	if p.matchTextSeq("LIMIT") {
		parts := p.parseCSV(func() *Expr { return p.parseNumber() }, TK_COMMA)
		if len(parts) == 1 {
			limit = parts[0]
		} else if len(parts) == 2 {
			limit = parts[1]
			offset = parts[0]
		}
	}

	return offset, limit
}

func mysqlParseSetItemCharset(p *Parser, kind string) *Expr {
	this := p.parseString()
	if this == nil {
		this = p.parseUnquotedField()
	}
	return p.expression(New(KSetItem, "this", this, "kind", kind))
}

// mysqlParseCharsetName preserves quoting when a charset name has characters that require it
// (e.g. spaces, as allowed for custom XML-registered charsets). Safe names unwrap to a bare Var
// so roundtrips remain minimal.
func mysqlParseCharsetName(p *Parser) *Expr {
	identifier := p.parseIdentifier()
	if identifier != nil {
		name := identifier.Name()
		if isSafeIdentifier(name) {
			return New(KVar, "this", name)
		}
		return identifier
	}
	return p.parseVar(false, tsPtr(newTokenSet(TK_BINARY)), false)
}

func mysqlParseSetItemNames(p *Parser) *Expr {
	charset := p.parseString()
	if charset == nil {
		charset = p.parseUnquotedField()
	}
	var collate *Expr
	if p.matchTextSeq("COLLATE") {
		collate = p.parseString()
		if collate == nil {
			collate = p.parseUnquotedField()
		}
	}

	return p.expression(New(KSetItem, "this", charset, "collate", collate, "kind", "NAMES"))
}

func mysqlParseType(p *Parser, parseInterval bool, fallbackToIdentifier bool) *Expr {
	// mysql binary is special and can work anywhere, even in order by operations
	// it operates like a no paren func
	if p.matchNoAdvance(TK_BINARY) {
		dataType := p.parseTypes(true, false, false, false)

		if dataType.IsA(KDataType) {
			return p.expression(New(KCast, "this", p.parseColumn(), "to", dataType))
		}
	}

	return p.baseParseType(parseInterval, fallbackToIdentifier)
}

func mysqlParseAlterTableAlterIndex(p *Parser) *Expr {
	index := p.parseField(true, nil, false)

	var visible any
	if p.matchTextSeq("VISIBLE") {
		visible = true
	} else if p.matchTextSeq("INVISIBLE") {
		visible = false
	}

	return p.expression(New(KAlterIndex, "this", index, "visible", visible))
}

// mysqlParsePartitionProperty parses PARTITION BY RANGE|LIST (...). It returns nil, an *Expr or a
// []*Expr: the bare partition expressions unless partition definitions follow.
func mysqlParsePartitionProperty(p *Parser) any {
	var partitionCls Kind
	var valueParser func() *Expr

	if p.matchTextSeq("RANGE") {
		partitionCls = KPartitionByRangeProperty
		valueParser = func() *Expr { return mysqlCallParsePartitionRangeValue(p) }
	} else if p.matchTextSeq("LIST") {
		partitionCls = KPartitionByListProperty
		valueParser = func() *Expr { return mysqlParsePartitionListValue(p) }
	}

	if valueParser == nil {
		return nil
	}

	partitionExpressions := p.parseWrappedCSV(func() *Expr { return p.parseAssignment() }, TK_COMMA, false)

	// For Doris and Starrocks
	if !p.matchTextSeqNoAdvance("(", "PARTITION") {
		return partitionExpressions
	}

	createExpressions := p.parseWrappedCSV(valueParser, TK_COMMA, false)

	return p.expression(New(
		partitionCls,
		"partition_expressions", partitionExpressions,
		"create_expressions", createExpressions,
	))
}

func mysqlParsePartitionRangeValue(p *Parser) *Expr {
	p.matchTextSeq("PARTITION")
	name := p.parseIdVar(true, nil)

	if !p.matchTextSeq("VALUES", "LESS", "THAN") {
		return name
	}

	values := p.parseWrappedCSV(func() *Expr { return p.parseExpression() }, TK_COMMA, false)

	if len(values) == 1 && values[0].IsA(KColumn) && pyUpper(values[0].Name()) == "MAXVALUE" {
		values = []*Expr{VarExpr("MAXVALUE")}
	}

	partRange := p.expression(New(KPartitionRange, "this", name, "expressions", values))
	return p.expression(New(KPartition, "expressions", []*Expr{partRange}))
}

func mysqlParsePartitionListValue(p *Parser) *Expr {
	p.matchTextSeq("PARTITION")
	name := p.parseIdVar(true, nil)
	p.matchTextSeq("VALUES", "IN")
	values := p.parseWrappedCSV(func() *Expr { return p.parseExpression() }, TK_COMMA, false)
	partList := p.expression(New(KPartitionList, "this", name, "expressions", values))
	return p.expression(New(KPartition, "expressions", []*Expr{partList}))
}

// mysqlParseForeignKey parses FOREIGN KEY [index_name] (columns) REFERENCES ...
func mysqlParseForeignKey(p *Parser) *Expr {
	// MySQL only uses this name when CONSTRAINT has none and no existing index fits
	var this *Expr
	if p.next.Type == TK_L_PAREN {
		this = p.parseIdVar(true, nil)
	}
	foreignKey := p.baseParseForeignKey()
	foreignKey.Set("this", this)
	return foreignKey
}

// mysqlParsePrimaryKey parses a primary key, always allowing a key name before the column list
// (namedPrimaryKey is ignored). Index options after the key parts of a table-level key, such as
// KEY_BLOCK_SIZE or COMMENT, belong to the key, not the table.
func mysqlParsePrimaryKey(p *Parser, wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr {
	this := p.baseParsePrimaryKey(wrappedOptional, inProps, true)
	if !this.IsA(KPrimaryKey) {
		return this
	}

	indexOptions := mysqlParseIndexConstraintOptions(p)
	if len(indexOptions) == 0 {
		return this
	}

	// The key constraint options parsed so far are strings, e.g. NOT ENFORCED
	keyOptions, _ := this.Arg("options").([]string)
	if len(keyOptions) == 0 {
		this.Set("options", indexOptions)
		return this
	}

	options := make([]any, 0, len(keyOptions)+len(indexOptions))
	for _, o := range keyOptions {
		options = append(options, o)
	}
	for _, o := range indexOptions {
		options = append(options, o)
	}
	this.Set("options", options)
	return this
}
