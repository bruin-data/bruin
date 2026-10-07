package sqlengine

// Port of sqlglot/dialects/mysql.py (class MySQL) and sqlglot/parsers/mysql.py (MySQLParser).
// The generator part (sqlglot/generators/mysql.py) lives in d_mysql_generator.go.

import (
	"strings"
)

func init() { registerCustomizer("mysql", customizeMySQL) }

// ---------------------------------------------------------------------------------------------
// Virtual dispatch for dialect-only MySQLParser methods that subclasses (Doris, StarRocks)
// override. These methods have no hook in parserHooks, so the overrides are registered here
// (in init functions) keyed by dialect name and resolved along the dialect's MRO.
// ---------------------------------------------------------------------------------------------

// mysqlPartitionPropertyImpls maps dialect name -> _parse_partition_property override.
var mysqlPartitionPropertyImpls = map[string]func(p *Parser) any{}

// mysqlPartitionRangeValueImpls maps dialect name -> _parse_partition_range_value override.
var mysqlPartitionRangeValueImpls = map[string]func(p *Parser) *Expr{}

// mysqlResolveImpl resolves a dialect-only method override along the dialect's MRO.
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

// mysqlCallParsePartitionProperty mirrors the virtual call self._parse_partition_property().
func mysqlCallParsePartitionProperty(p *Parser) any {
	if f, ok := mysqlResolveImpl(p.d, mysqlPartitionPropertyImpls); ok {
		return f(p)
	}
	return mysqlParsePartitionProperty(p)
}

// mysqlCallParsePartitionRangeValue mirrors the virtual call self._parse_partition_range_value().
func mysqlCallParsePartitionRangeValue(p *Parser) *Expr {
	if f, ok := mysqlResolveImpl(p.d, mysqlPartitionRangeValueImpls); ok {
		return f(p)
	}
	return mysqlParsePartitionRangeValue(p)
}

// mysqlNoKwargs wraps a PROPERTY_PARSERS entry defined as `lambda self: ...` in a dialect parser
// class (accepts no kwargs). owner is the Python qualname prefix, e.g. "MySQLParser.<lambda>".
func mysqlNoKwargs(owner string, fn func(p *Parser) any) propertyParseFn {
	return func(p *Parser, kw propKwargs) any {
		checkPropKwargs(kw, owner)
		return fn(p)
	}
}

// ---------------------------------------------------------------------------------------------
// Module-level helpers of sqlglot/parsers/mysql.py
// ---------------------------------------------------------------------------------------------

// mysqlTimeSpecifiers mirrors TIME_SPECIFIERS: all specifiers for time parts (as opposed to date parts).
// https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
var mysqlTimeSpecifiers = map[rune]bool{
	'f': true, 'H': true, 'h': true, 'I': true, 'i': true, 'k': true,
	'l': true, 'p': true, 'r': true, 'S': true, 's': true, 'T': true,
}

// mysqlHasTimeSpecifier mirrors _has_time_specifier.
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

// mysqlStrToDate mirrors _str_to_date.
func mysqlStrToDate(args []*Expr, d *Dialect) *Expr {
	mysqlDateFormat := seqGet(args, 1)
	dateFormat := MustDialect("mysql").formatTimeExpr(mysqlDateFormat)
	this := seqGet(args, 0)

	if mysqlDateFormat != nil && mysqlHasTimeSpecifier(mysqlDateFormat.Name()) {
		return New(KStrToTime, "this", this, "format", dateFormat)
	}

	return New(KStrToDate, "this", this, "format", dateFormat)
}

// mysqlShowParser mirrors _show_parser(*args, **kwargs). target is nil/false, true or a string.
func mysqlShowParser(this string, target any, full any, global any) parseFn {
	return func(p *Parser) *Expr {
		return mysqlParseShowMySQL(p, this, target, full, global)
	}
}

// ---------------------------------------------------------------------------------------------
// Customizer (class body of MySQLParser + MySQL dialect)
// ---------------------------------------------------------------------------------------------

func customizeMySQL(d *Dialect) {
	P := d.P

	// RANGE_PARSERS
	P.RANGE_PARSERS[TK_SOUNDS_LIKE] = func(p *Parser, this *Expr) *Expr {
		left := p.expression(New(KSoundex, "this", this))
		right := p.expression(New(KSoundex, "this", p.parseTerm()))
		return p.expression(New(KEQ, "this", left, "expression", right))
	}
	P.RANGE_PARSERS[TK_MEMBER_OF] = func(p *Parser, this *Expr) *Expr {
		return p.expression(New(
			KJSONArrayContains,
			"this", this,
			"expression", p.parseWrapped(func() *Expr { return p.parseExpression() }, false),
		))
	}

	// FUNCTIONS
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
	P.FUNCTIONS["STR_TO_DATE"] = mysqlStrToDate
	P.FUNCTIONS["TIMESTAMPDIFF"] = buildDateDelta(KTimestampDiff, nil, "DAY", false)
	P.FUNCTIONS["TO_DAYS"] = func(args []*Expr, d *Dialect) *Expr {
		diff := New(
			KDateDiff,
			"this", tsOrDsToDate0(args),
			"expression", New(KTsOrDsToDate, "this", LiteralString("0000-01-01")),
			"unit", VarExpr("DAY"),
		)
		// exp.paren(DateDiff(...) + 1)
		return Paren(New(KAdd, "this", diff, "expression", LiteralInt(1)))
	}
	P.FUNCTIONS["VERSION"] = fromArgList(KCurrentVersion)
	P.FUNCTIONS["WEEK"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KWeek, "this", tsOrDsToDate0(args), "mode", seqGet(args, 1))
	}
	P.FUNCTIONS["WEEKOFYEAR"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KWeekOfYear, "this", tsOrDsToDate0(args))
	}
	P.FUNCTIONS["YEAR"] = func(args []*Expr, d *Dialect) *Expr { return New(KYear, "this", tsOrDsToDate0(args)) }

	// FUNCTION_PARSERS
	P.FUNCTION_PARSERS["GROUP_CONCAT"] = func(p *Parser) *Expr { return p.parseGroupConcat() }
	// https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	P.FUNCTION_PARSERS["VALUES"] = func(p *Parser) *Expr {
		return p.expression(New(KAnonymous, "this", "VALUES", "expressions", []*Expr{p.parseIdVar(true, nil)}))
	}
	P.FUNCTION_PARSERS["JSON_VALUE"] = func(p *Parser) *Expr { return p.parseJsonValue() }
	P.FUNCTION_PARSERS["SUBSTR"] = func(p *Parser) *Expr { return p.parseSubstring() }

	// STATEMENT_PARSERS
	P.STATEMENT_PARSERS[TK_SHOW] = func(p *Parser) *Expr { return p.parseShow() }

	// SHOW_PARSERS (a fresh dict, not merged with the parent's)
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

	// PROPERTY_PARSERS
	P.PROPERTY_PARSERS["LOCK"] = mysqlNoKwargs("MySQLParser.<lambda>", func(p *Parser) any {
		return anyExpr(p.parsePropertyAssignment(KLockProperty))
	})
	P.PROPERTY_PARSERS["PARTITION BY"] = mysqlNoKwargs("MySQLParser.<lambda>", func(p *Parser) any {
		return mysqlCallParsePartitionProperty(p)
	})

	// SET_PARSERS
	P.SET_PARSERS["PERSIST"] = func(p *Parser) *Expr { return p.parseSetItemAssignment("PERSIST") }
	P.SET_PARSERS["PERSIST_ONLY"] = func(p *Parser) *Expr { return p.parseSetItemAssignment("PERSIST_ONLY") }
	P.SET_PARSERS["CHARACTER SET"] = func(p *Parser) *Expr { return mysqlParseSetItemCharset(p, "CHARACTER SET") }
	P.SET_PARSERS["CHARSET"] = func(p *Parser) *Expr { return mysqlParseSetItemCharset(p, "CHARACTER SET") }
	P.SET_PARSERS["NAMES"] = func(p *Parser) *Expr { return mysqlParseSetItemNames(p) }

	// CONSTRAINT_PARSERS
	P.CONSTRAINT_PARSERS["FULLTEXT"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "FULLTEXT") }
	P.CONSTRAINT_PARSERS["INDEX"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "") }
	P.CONSTRAINT_PARSERS["KEY"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "") }
	P.CONSTRAINT_PARSERS["SPATIAL"] = func(p *Parser) *Expr { return mysqlParseIndexConstraint(p, "SPATIAL") }
	P.CONSTRAINT_PARSERS["ZEROFILL"] = func(p *Parser) *Expr { return p.expression(New(KZeroFillColumnConstraint)) }
	P.CONSTRAINT_PARSERS["INVISIBLE"] = func(p *Parser) *Expr { return p.expression(New(KInvisibleColumnConstraint)) }

	// ALTER_PARSERS
	P.ALTER_PARSERS["CHANGE"] = func(p *Parser) any { return anyExpr(mysqlParseAlterTableModify(p, true)) }
	P.ALTER_PARSERS["MODIFY"] = func(p *Parser) any { return anyExpr(mysqlParseAlterTableModify(p, false)) }
	P.ALTER_PARSERS["AUTO_INCREMENT"] = func(p *Parser) any {
		return anyExpr(p.parsePropertyAssignment(KAutoIncrementProperty))
	}

	// ALTER_ALTER_PARSERS
	P.ALTER_ALTER_PARSERS["INDEX"] = func(p *Parser) *Expr { return mysqlParseAlterTableAlterIndex(p) }

	// Method overrides
	P.h.parseAlterTableRename = mysqlParseAlterTableRename
	P.h.parseAlterDropAction = mysqlParseAlterDropAction
	P.h.parseGeneratedAsIdentity = mysqlParseGeneratedAsIdentity
	P.h.parsePrimaryKeyPart = mysqlParsePrimaryKeyPart
	P.h.parseCharsetName = mysqlParseCharsetName
	P.h.parseType = mysqlParseType
	P.h.parsePrimaryKey = mysqlParsePrimaryKey

	customizeMySQLGenerator(d)
}

// ---------------------------------------------------------------------------------------------
// MySQLParser methods
// ---------------------------------------------------------------------------------------------

// mysqlParseAlterTableRename mirrors MySQLParser._parse_alter_table_rename.
func mysqlParseAlterTableRename(p *Parser) *Expr {
	if p.matchTexts("INDEX", "KEY") {
		old := p.parseField(true, nil, false)
		p.matchTextSeq("TO")
		newName := p.parseField(true, nil, false)
		return p.expression(New(KRenameIndex, "this", old, "to", newName))
	}
	return p.baseParseAlterTableRename()
}

// mysqlParseAlterDropAction mirrors MySQLParser._parse_alter_drop_action.
func mysqlParseAlterDropAction(p *Parser) *Expr {
	if p.matchPair(TK_DROP, TK_PRIMARY_KEY) {
		return p.expression(New(KDropPrimaryKey))
	}
	return p.baseParseAlterDropAction()
}

// mysqlParseAlterTableModify mirrors MySQLParser._parse_alter_table_modify.
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

// mysqlParseGeneratedAsIdentity mirrors MySQLParser._parse_generated_as_identity.
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

// mysqlParsePrimaryKeyPart mirrors MySQLParser._parse_primary_key_part.
func mysqlParsePrimaryKeyPart(p *Parser) *Expr {
	this := p.parseIdVar(true, nil)
	if !p.match(TK_L_PAREN) {
		return this
	}

	expression := p.parseNumber()
	p.matchRParen(nil)
	return p.expression(New(KColumnPrefix, "this", this, "expression", expression))
}

// mysqlParseIndexConstraint mirrors MySQLParser._parse_index_constraint. kind "" means None.
func mysqlParseIndexConstraint(p *Parser, kind string) *Expr {
	if kind != "" {
		p.matchTexts("INDEX", "KEY")
	}

	this := p.parseIdVar(false, nil)

	// self._match(TokenType.USING) and self._advance_any() and self._prev.text
	var indexType any = false
	if p.match(TK_USING) {
		if p.advanceAny(false) != nil {
			indexType = p.prev.Text
		} else {
			indexType = nil
		}
	}
	expressions := p.parseWrappedCSV(func() *Expr { return p.parseOrdered(nil) }, TK_COMMA, false)

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
		"options", options,
	))
}

// mysqlParseShowMySQL mirrors MySQLParser._parse_show_mysql. target is nil/false, true or a
// string; full and global are nil (None) or true.
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

// mysqlParseOldstyleLimit mirrors MySQLParser._parse_oldstyle_limit; returns (offset, limit).
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

// mysqlParseSetItemCharset mirrors MySQLParser._parse_set_item_charset.
func mysqlParseSetItemCharset(p *Parser, kind string) *Expr {
	this := p.parseString()
	if this == nil {
		this = p.parseUnquotedField()
	}
	return p.expression(New(KSetItem, "this", this, "kind", kind))
}

// mysqlParseCharsetName mirrors MySQLParser._parse_charset_name.
//
// Preserve quoting when a charset name has characters that require it (e.g. spaces, as allowed
// for custom XML-registered charsets). Safe names unwrap to a bare Var so roundtrips remain minimal.
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

// mysqlParseSetItemNames mirrors MySQLParser._parse_set_item_names.
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

// mysqlParseType mirrors MySQLParser._parse_type.
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

// mysqlParseAlterTableAlterIndex mirrors MySQLParser._parse_alter_table_alter_index.
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

// mysqlParsePartitionProperty mirrors MySQLParser._parse_partition_property. It returns nil
// (None), an *Expr or a []*Expr.
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

// mysqlParsePartitionRangeValue mirrors MySQLParser._parse_partition_range_value.
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

// mysqlParsePartitionListValue mirrors MySQLParser._parse_partition_list_value.
func mysqlParsePartitionListValue(p *Parser) *Expr {
	p.matchTextSeq("PARTITION")
	name := p.parseIdVar(true, nil)
	p.matchTextSeq("VALUES", "IN")
	values := p.parseWrappedCSV(func() *Expr { return p.parseExpression() }, TK_COMMA, false)
	partList := p.expression(New(KPartitionList, "this", name, "expressions", values))
	return p.expression(New(KPartition, "expressions", []*Expr{partList}))
}

// mysqlParsePrimaryKey mirrors MySQLParser._parse_primary_key.
func mysqlParsePrimaryKey(p *Parser, wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr {
	return p.baseParsePrimaryKey(wrappedOptional, inProps, true)
}
