package sqlengine

import (
	"fmt"
	"strings"
)

// chunkFAnd returns false (not nil) when cond is false, and fn()'s result otherwise (nil when fn
// returns nil). cond is usually the result of a match call.
func chunkFAnd(cond bool, fn func() *Expr) any {
	if !cond {
		return false
	}
	if e := fn(); e != nil {
		return e
	}
	return nil
}

// chunkFAndList is chunkFAnd for list-returning parse methods; a nil list becomes an empty one.
func chunkFAndList(cond bool, fn func() []*Expr) any {
	if !cond {
		return false
	}
	l := fn()
	if l == nil {
		return []*Expr{}
	}
	return l
}

// chunkFAndAny is chunkFAnd for parse methods returning an *Expr, a list or nil.
func chunkFAndAny(cond bool, fn func() any) any {
	if !cond {
		return false
	}
	v := fn()
	if chunkFIsNone(v) {
		return nil
	}
	return v
}

func chunkFStrOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func chunkFIsNone(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case *Expr:
		return x == nil
	}
	return false
}

func chunkFEnsureList(v any) []*Expr {
	switch x := v.(type) {
	case nil:
		return []*Expr{}
	case *Expr:
		if x == nil {
			return []*Expr{}
		}
		return []*Expr{x}
	case []*Expr:
		if x == nil {
			return []*Expr{}
		}
		return x
	}
	panic(&ValueError{Msg: fmt.Sprintf("ensure_list: unexpected value %T", v)})
}

func chunkFPyStr(e *Expr) string {
	if e == nil {
		return "None"
	}
	return exprSQL(e)
}

// baseParseTransaction parses the rest of BEGIN / START TRANSACTION: an optional kind and the
// transaction modes.
func (p *Parser) baseParseTransaction() *Expr {
	var this any
	if p.matchTextSet(p.s.TRANSACTION_KIND) {
		this = p.prev.Text
	}

	p.matchTexts("TRANSACTION", "WORK")

	modes := []string{}
	for {
		mode := []string{}
		for p.match(TK_VAR) || p.match(TK_NOT) {
			mode = append(mode, p.prev.Text)
		}

		if len(mode) > 0 {
			modes = append(modes, strings.Join(mode, " "))
		}
		if !p.match(TK_COMMA) {
			break
		}
	}

	return p.expression(New(KTransaction, "this", this, "modes", modes))
}

// baseParseCommitOrRollback parses the rest of COMMIT or ROLLBACK: [TRANSACTION | WORK],
// TO [SAVEPOINT] name and AND [NO] CHAIN.
func (p *Parser) baseParseCommitOrRollback() *Expr {
	var chain any
	var savepoint *Expr
	isRollback := p.prev.Type == TK_ROLLBACK

	p.matchTexts("TRANSACTION", "WORK")

	if p.matchTextSeq("TO") {
		p.matchTextSeq("SAVEPOINT")
		savepoint = p.parseIdVar(true, nil)
	}

	if p.match(TK_AND) {
		chain = !p.matchTextSeq("NO")
		p.matchTextSeq("CHAIN")
	}

	if isRollback {
		return p.expression(New(KRollback, "savepoint", savepoint))
	}

	return p.expression(New(KCommit, "chain", chain))
}

func (p *Parser) parseRefresh() *Expr {
	start := p.prev

	var kind string
	if p.matchTextSeq("EXTERNAL", "TABLE") {
		kind = "EXTERNAL TABLE"
	} else if p.match(TK_TABLE) {
		kind = "TABLE"
	} else if p.matchTextSeq("MATERIALIZED", "VIEW") {
		kind = "MATERIALIZED VIEW"
	} else {
		kind = ""
	}

	this := p.parseString()
	if kind == "" && !this.IsA(KLiteral) {
		return p.parseAsCommand(start)
	}
	if this == nil {
		this = p.parseTable(false, false, nil, false, false, false, false)
	}
	return p.expression(New(KRefresh, "this", this, "kind", kind))
}

func (p *Parser) parseColumnDefWithExists() *Expr {
	start := p.index
	p.match(TK_COLUMN)

	existsColumn := p.parseExists(true)
	expression := p.parseFieldDef()

	if !expression.IsA(KColumnDef) {
		p.retreat(start)
		return nil
	}

	expression.Set("exists", existsColumn)

	return expression
}

func (p *Parser) parseAddColumn() *Expr {
	if !(upperText(p.prev) == "ADD") {
		return nil
	}

	return p.parseColumnDefWithExists()
}

// baseParseDropColumn parses a DROP action of ALTER TABLE. Where the COLUMN keyword is optional,
// a bare DROP [IF EXISTS] c [CASCADE | RESTRICT] drops a column.
func (p *Parser) baseParseDropColumn() *Expr {
	if !p.match(TK_DROP) {
		return nil
	}

	kind := ""
	if !p.s.ALTER_DROP_REQUIRES_COLUMN {
		index := p.index
		p.parseExists(false)
		if p.curr.Type != TK_COLUMN && p.curr.Type != TK_CONSTRAINT && p.matchSet(p.s.ID_VAR_TOKENS) &&
			(!p.curr.ok() || p.matchNoAdvance(TK_COMMA) ||
				p.matchTextSeqNoAdvance("CASCADE") || p.matchTextSeqNoAdvance("RESTRICT")) {
			kind = "COLUMN"
		}
		p.retreat(index)
	}

	return p.parseDrop(false, kind)
}

func (p *Parser) baseParseAlterDropAction() *Expr {
	return p.parseDropColumn()
}

// parseDropPartition parses the PARTITION (...) list of ALTER TABLE ... DROP [IF EXISTS].
// https://docs.aws.amazon.com/athena/latest/ug/alter-table-drop-partition.html
func (p *Parser) parseDropPartition(exists bool) *Expr {
	return p.expression(New(
		KDropPartition,
		"expressions", p.parseCSV(p.parsePartition, TK_COMMA),
		"exists", exists,
	))
}

func (p *Parser) parseAlterTableAdd() []*Expr {
	parseAddAlteration := func() *Expr {
		p.matchTextSeq("ADD")
		// constraint kinds that are plain words, e.g. FULLTEXT and SPATIAL indexes, also start one
		if p.matchSetNoAdvance(p.s.ADD_CONSTRAINT_TOKENS) || p.matchTextSetNoAdvance(p.s.ADD_CONSTRAINT_KEYWORDS) {
			return p.expression(New(
				KAddConstraint,
				"expressions", p.parseCSV(p.parseConstraint, TK_COMMA),
			))
		}

		columnDef := p.parseAddColumn()
		if columnDef.IsA(KColumnDef) {
			return columnDef
		}

		exists := p.parseExists(true)
		if p.matchPairNoAdvance(TK_PARTITION, TK_L_PAREN) {
			this := p.parseField(true, nil, false)
			location := chunkFAndAny(p.matchTextSeqNoAdvance("LOCATION"), p.parseProperty)
			return p.expression(New(
				KAddPartition,
				"exists", exists,
				"this", this,
				"location", location,
			))
		}

		return nil
	}

	if !p.matchSetNoAdvance(p.s.ADD_CONSTRAINT_TOKENS) &&
		(!p.d.S.ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN || p.matchTextSeq("COLUMNS")) {
		schema := p.parseSchema(nil)

		if schema != nil {
			return chunkFEnsureList(schema)
		}
		return p.parseCSV(p.parseColumnDefWithExists, TK_COMMA)
	}

	return p.parseCSV(parseAddAlteration, TK_COMMA)
}

func (p *Parser) baseParseAlterTableAlter() *Expr {
	if matchTextKeys(p, p.s.ALTER_ALTER_PARSERS) {
		return p.s.ALTER_ALTER_PARSERS[upperText(p.prev)](p)
	}

	// Many dialects support the ALTER [COLUMN] syntax, so if there is no
	// keyword after ALTER we default to parsing this statement
	p.match(TK_COLUMN)
	var exists any
	if p.parseExists(false) {
		exists = true
	}
	column := p.parseField(true, nil, false)

	if p.matchPair(TK_DROP, TK_DEFAULT) {
		return p.expression(New(KAlterColumn, "this", column, "drop", true, "exists", exists))
	}
	if p.matchPair(TK_SET, TK_DEFAULT) {
		return p.expression(New(KAlterColumn, "this", column, "default", p.parseDisjunction(), "exists", exists))
	}
	if p.match(TK_COMMENT) {
		return p.expression(New(KAlterColumn, "this", column, "comment", p.parseString(), "exists", exists))
	}
	if p.matchTextSeq("DROP", "NOT", "NULL") {
		return p.expression(New(KAlterColumn, "this", column, "drop", true, "allow_null", true, "exists", exists))
	}
	if p.matchTextSeq("SET", "NOT", "NULL") {
		return p.expression(New(KAlterColumn, "this", column, "allow_null", false, "exists", exists))
	}

	if p.matchTextSeq("SET", "VISIBLE") {
		return p.expression(New(KAlterColumn, "this", column, "visible", "VISIBLE", "exists", exists))
	}
	if p.matchTextSeq("SET", "INVISIBLE") {
		return p.expression(New(KAlterColumn, "this", column, "visible", "INVISIBLE", "exists", exists))
	}

	p.matchTextSeq("SET", "DATA")
	var dtype *Expr
	if p.matchTextSeq("TYPE") || !p.s.ALTER_COLUMN_TYPE_REQUIRES_KEYWORD {
		dtype = p.parseTypes(false, false, true, false)
	}
	if dtype == nil {
		return nil
	}
	collate := chunkFAnd(p.match(TK_COLLATE), p.parseTerm)
	using := chunkFAnd(p.match(TK_USING), p.parseDisjunction)
	return p.expression(New(
		KAlterColumn,
		"this", column,
		"dtype", dtype,
		"collate", collate,
		"using", using,
		"exists", exists,
	))
}

func (p *Parser) parseAlterDiststyle() *Expr {
	if p.matchTexts("ALL", "EVEN", "AUTO") {
		return p.expression(New(KAlterDistStyle, "this", VarExpr(upperText(p.prev))))
	}

	p.matchTextSeq("KEY", "DISTKEY")
	return p.expression(New(KAlterDistStyle, "this", p.parseColumn()))
}

// parseAlterSortkey parses ALTER [COMPOUND] SORTKEY (columns) | AUTO | NONE.
// compound=false leaves the compound arg unset (nil) rather than false.
func (p *Parser) parseAlterSortkey(compound bool) *Expr {
	var compoundV any
	if compound {
		compoundV = true
		p.matchTextSeq("SORTKEY")
	}

	if p.matchNoAdvance(TK_L_PAREN) {
		return p.expression(New(
			KAlterSortKey,
			"expressions", p.parseWrappedIdVars(false),
			"compound", compoundV,
		))
	}

	p.matchTexts("AUTO", "NONE")
	return p.expression(New(
		KAlterSortKey,
		"this", VarChecked(upperText(p.prev)),
		"compound", compoundV,
	))
}

func (p *Parser) parseAlterTableDrop() []*Expr {
	index := p.index - 1

	partitionExists := p.parseExists(false)
	if p.matchNoAdvance(TK_PARTITION) {
		return p.parseCSV(func() *Expr { return p.parseDropPartition(partitionExists) }, TK_COMMA)
	}

	p.retreat(index)
	return p.parseCSV(p.parseAlterDropAction, TK_COMMA)
}

func (p *Parser) baseParseAlterTableRename() *Expr {
	if p.match(TK_COLUMN) ||
		(!p.s.ALTER_RENAME_REQUIRES_COLUMN && !p.matchTextSeqNoAdvance("TO")) {
		exists := p.parseExists(false)
		oldColumn := p.parseColumn()
		to := p.matchTextSeq("TO")
		newColumn := p.parseColumn()

		if oldColumn == nil || !to || newColumn == nil {
			return nil
		}

		return p.expression(New(KRenameColumn, "this", oldColumn, "to", newColumn, "exists", exists))
	}

	p.matchTextSeq("TO")
	return p.expression(New(KAlterRename, "this", p.parseTable(true, false, nil, false, false, false, false)))
}

func (p *Parser) baseParseAlterTableSet() *Expr {
	alterSet := p.expression(New(KAlterSet))

	if p.matchNoAdvance(TK_L_PAREN) || p.matchTextSeq("TABLE", "PROPERTIES") {
		alterSet.Set("expressions", p.parseWrappedCSV(p.parseAssignment, TK_COMMA, false))
	} else if p.matchTextSeqNoAdvance("FILESTREAM_ON") {
		alterSet.Set("expressions", []*Expr{p.parseAssignment()})
	} else if p.matchTexts("LOGGED", "UNLOGGED") {
		alterSet.Set("option", VarExpr(upperText(p.prev)))
	} else if p.matchTextSeq("WITHOUT") && p.matchTexts("CLUSTER", "OIDS") {
		alterSet.Set("option", VarExpr("WITHOUT "+upperText(p.prev)))
	} else if p.matchTextSeq("LOCATION") {
		alterSet.Set("location", p.parseField(false, nil, false))
	} else if p.matchTextSeq("ACCESS", "METHOD") {
		alterSet.Set("access_method", p.parseField(false, nil, false))
	} else if p.matchTextSeq("TABLESPACE") {
		alterSet.Set("tablespace", p.parseField(false, nil, false))
	} else if p.matchTextSeq("FILE", "FORMAT") || p.matchTextSeq("FILEFORMAT") {
		alterSet.Set("file_format", []*Expr{p.parseField(false, nil, false)})
	} else if p.matchTextSeq("STAGE_FILE_FORMAT") {
		alterSet.Set("file_format", p.parseWrappedOptions())
	} else if p.matchTextSeq("STAGE_COPY_OPTIONS") {
		alterSet.Set("copy_options", p.parseWrappedOptions())
	} else if p.matchTextSeq("TAG") || p.matchTextSeq("TAGS") {
		alterSet.Set("tag", p.parseCSV(p.parseAssignment, TK_COMMA))
	} else {
		if p.matchTextSeq("SERDE") {
			alterSet.Set("serde", p.parseField(false, nil, false))
		}

		properties := p.parseWrapped(func() *Expr { return p.parseProperties(false) }, true)
		alterSet.Set("expressions", []*Expr{properties})
	}

	return alterSet
}

func (p *Parser) parseAlterSession() *Expr {
	if p.match(TK_SET) {
		expressions := p.parseCSV(func() *Expr { return p.parseSetItemAssignment("") }, TK_COMMA)
		return p.expression(New(KAlterSession, "expressions", expressions, "unset", false))
	}

	p.matchTextSeq("UNSET")
	expressions := p.parseCSV(func() *Expr {
		return p.expression(New(KSetItem, "this", p.parseIdVar(true, nil)))
	}, TK_COMMA)
	return p.expression(New(KAlterSession, "expressions", expressions, "unset", true))
}

// parseAlter parses ALTER statements through ALTER_PARSERS, falling back to a Command when the
// statement is not fully understood.
func (p *Parser) parseAlter() *Expr {
	start := p.prev

	iceberg := p.matchTextSeq("ICEBERG")

	var alterToken *Token
	if p.matchSet(p.s.ALTERABLES) {
		alterToken = p.prev
	}
	if !alterToken.ok() {
		return p.parseAsCommand(start)
	}
	if iceberg && alterToken.Type != TK_TABLE {
		return p.parseAsCommand(start)
	}

	exists := p.parseExists(false)
	only := p.matchTextSeq("ONLY")

	var this, cluster *Expr
	var check any
	if alterToken.Type == TK_SESSION {
		this = nil
		check = nil
		cluster = nil
	} else {
		this = p.parseTable(true, false, nil, false, false, p.s.ALTER_TABLE_PARTITIONS, false)
		check = p.matchTextSeq("WITH", "CHECK")
		if p.match(TK_ON) {
			cluster = p.parseOnProperty()
		}

		if p.next.ok() {
			p.advance(1)
		}
	}

	var parser parseAnyFn
	if p.prev.ok() {
		parser = p.s.ALTER_PARSERS[upperText(p.prev)]
	}
	var actions []*Expr
	if parser != nil {
		actions = chunkFEnsureList(parser(p))
	}
	// without an action the statement is kept as a command
	if len(actions) > 0 {
		options := []*Expr{}
		// Actions of different kinds and table options may alternate, e.g. MySQL's
		// ADD c INT, AUTO_INCREMENT=5, DROP d. The comma before an item may have already been
		// consumed by the previous action's parser.
		for p.s.ALTER_TABLE_MIXED_ACTIONS && (p.match(TK_COMMA) || p.prev.Type == TK_COMMA) {
			if !matchTextKeysNoAdvance(p, p.s.PROPERTY_PARSERS) && matchTextKeys(p, p.s.ALTER_PARSERS) {
				actionParser := p.s.ALTER_PARSERS[upperText(p.prev)]
				parsed := chunkFEnsureList(tryParse(p, func() any { return actionParser(p) }, false, func(v any) bool { return !truthy(v) }))
				if len(parsed) == 0 {
					return p.parseAsCommand(start)
				}
				for _, action := range parsed {
					if action.IsA(KCommand) {
						return p.parseAsCommand(start)
					}
				}
				actions = append(actions, parsed...)
			} else if properties := p.tryParseExpr(func() *Expr { return p.parseProperties(false) }, false); properties != nil {
				options = append(options, properties.Expressions()...)
			} else {
				break
			}
		}

		notValid := p.matchTextSeq("NOT", "VALID")
		if properties := p.parseProperties(false); properties != nil {
			options = append(options, properties.Expressions()...)
		}
		cascade := p.d.S.ALTER_TABLE_SUPPORTS_CASCADE && p.matchTextSeq("CASCADE")

		if !p.curr.ok() {
			return p.expression(New(
				KAlter,
				"this", this,
				"kind", upperText(alterToken),
				"exists", exists,
				"actions", actions,
				"only", only,
				"options", options,
				"cluster", cluster,
				"not_valid", notValid,
				"check", check,
				"cascade", cascade,
				"iceberg", iceberg,
			))
		}
	}

	return p.parseAsCommand(start)
}

// parseAnalyze parses ANALYZE in its dialect forms; an unsupported partition clause makes it a
// Command.
func (p *Parser) parseAnalyze() *Expr {
	start := p.prev
	// https://duckdb.org/docs/sql/statements/analyze
	if !p.curr.ok() {
		return p.expression(New(KAnalyze))
	}

	options := []string{}
	for p.matchTextSet(p.s.ANALYZE_STYLES) {
		if upperText(p.prev) == "BUFFER_USAGE_LIMIT" {
			options = append(options, "BUFFER_USAGE_LIMIT "+chunkFPyStr(p.parseNumber()))
		} else {
			options = append(options, upperText(p.prev))
		}
	}

	var this, innerExpression *Expr
	// ANALYZE [TABLE] takes a list: the first table is this, the rest are tables
	var tables any
	parseTables := func() {
		this = p.parseTableParts(false, false, false, false)
		if p.match(TK_COMMA) {
			tables = p.parseCSV(func() *Expr { return p.parseTableParts(false, false, false, false) }, TK_COMMA)
		}
	}

	var kind any
	if p.curr.ok() {
		kind = upperText(p.curr)
	}

	if p.match(TK_TABLE) {
		parseTables()
	} else if p.match(TK_INDEX) {
		this = p.parseTableParts(false, false, false, false)
	} else if p.matchTextSeq("TABLES") {
		if p.matchAny(TK_FROM, TK_IN) {
			kind = fmt.Sprintf("%v %s", chunkFPyStrAny(kind), upperText(p.prev))
			this = p.parseTable(true, false, nil, false, true, false, false)
		}
	} else if p.matchTextSeq("DATABASE") {
		this = p.parseTable(true, false, nil, false, true, false, false)
	} else if p.matchTextSeq("CLUSTER") {
		this = p.parseTable(false, false, nil, false, false, false, false)
	} else if matchTextKeys(p, p.s.ANALYZE_EXPRESSION_PARSERS) {
		// Try matching inner expr keywords before fallback to parse table.
		kind = nil
		innerExpression = p.s.ANALYZE_EXPRESSION_PARSERS[upperText(p.prev)](p)
	} else {
		// Empty kind  https://prestodb.io/docs/current/sql/analyze.html
		kind = nil
		parseTables()
	}

	partition := p.tryParseExpr(p.parsePartition, false)
	if partition == nil && p.matchTextSet(p.s.PARTITION_KEYWORDS) {
		return p.parseAsCommand(start)
	}

	// https://docs.starrocks.io/docs/sql-reference/sql-statements/cbo_stats/ANALYZE_TABLE/
	var mode any
	if p.matchTextSeq("WITH", "SYNC", "MODE") || p.matchTextSeq("WITH", "ASYNC", "MODE") {
		mode = "WITH " + upperText(p.tokens[p.index-2]) + " MODE"
	} else {
		mode = nil
	}

	if matchTextKeys(p, p.s.ANALYZE_EXPRESSION_PARSERS) {
		innerExpression = p.s.ANALYZE_EXPRESSION_PARSERS[upperText(p.prev)](p)
	}

	properties := p.parseProperties(false)
	return p.expression(New(
		KAnalyze,
		"kind", kind,
		"this", this,
		"tables", tables,
		"mode", mode,
		"partition", partition,
		"properties", properties,
		"expression", innerExpression,
		"options", options,
	))
}

func chunkFPyStrAny(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case *Expr:
		return chunkFPyStr(x)
	}
	return fmt.Sprint(v)
}

// parseAnalyzeStatistics parses COMPUTE | ESTIMATE [DELTA] STATISTICS [NOSCAN |
// FOR [ALL] COLUMNS ... | SAMPLE n [PERCENT]].
// https://spark.apache.org/docs/3.5.1/sql-ref-syntax-aux-analyze-table.html
func (p *Parser) parseAnalyzeStatistics() *Expr {
	var this any
	kind := upperText(p.prev)
	var option any
	if p.matchTextSeq("DELTA") {
		option = upperText(p.prev)
	}
	expressions := []*Expr{}

	if !p.matchTextSeq("STATISTICS") {
		p.raiseError("Expecting token STATISTICS", nil)
	}

	if p.matchTextSeq("NOSCAN") {
		this = "NOSCAN"
	} else if p.match(TK_FOR) {
		if p.matchTextSeq("ALL", "COLUMNS") {
			this = "FOR ALL COLUMNS"
		}
		if p.matchTextSeq("COLUMNS") {
			this = "FOR COLUMNS"
			expressions = p.parseCSV(p.parseColumnReference, TK_COMMA)
		}
	} else if p.matchTextSeq("SAMPLE") {
		sample := p.parseNumber()
		var sampleKind any
		if p.match(TK_PERCENT) {
			sampleKind = upperText(p.prev)
		}
		expressions = []*Expr{
			p.expression(New(KAnalyzeSample, "sample", sample, "kind", sampleKind)),
		}
	}

	return p.expression(New(
		KAnalyzeStatistics,
		"kind", kind,
		"option", option,
		"this", this,
		"expressions", expressions,
	))
}

// parseAnalyzeValidate parses VALIDATE REF UPDATE [SET DANGLING TO NULL] or VALIDATE STRUCTURE
// [CASCADE ...].
// https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ANALYZE.html
func (p *Parser) parseAnalyzeValidate() *Expr {
	var kind, this any
	var expression *Expr
	if p.matchTextSeq("REF", "UPDATE") {
		kind = "REF"
		this = "UPDATE"
		if p.matchTextSeq("SET", "DANGLING", "TO", "NULL") {
			this = "UPDATE SET DANGLING TO NULL"
		}
	} else if p.matchTextSeq("STRUCTURE") {
		kind = "STRUCTURE"
		if p.matchTextSeq("CASCADE", "FAST") {
			this = "CASCADE FAST"
		} else if p.matchTextSeq("CASCADE", "COMPLETE") && p.matchTexts("ONLINE", "OFFLINE") {
			this = "CASCADE COMPLETE " + upperText(p.prev)
			expression = p.parseInto()
		}
	}

	return p.expression(New(KAnalyzeValidate, "kind", kind, "this", this, "expression", expression))
}

// parseAnalyzeColumns parses ALL COLUMNS or PREDICATE COLUMNS; nil otherwise.
func (p *Parser) parseAnalyzeColumns() *Expr {
	this := upperText(p.prev)
	if p.matchTextSeq("COLUMNS") {
		return p.expression(New(KAnalyzeColumns, "this", this+" "+upperText(p.prev)))
	}
	return nil
}

func (p *Parser) parseAnalyzeDelete() *Expr {
	var kind any
	if p.matchTextSeq("SYSTEM") {
		kind = upperText(p.prev)
	}
	if p.matchTextSeq("STATISTICS") {
		return p.expression(New(KAnalyzeDelete, "kind", kind))
	}
	return nil
}

// parseAnalyzeList parses LIST CHAINED ROWS [INTO ...]; nil otherwise.
func (p *Parser) parseAnalyzeList() *Expr {
	if p.matchTextSeq("CHAINED", "ROWS") {
		return p.expression(New(KAnalyzeListChainedRows, "expression", p.parseInto()))
	}
	return nil
}

// parseAnalyzeHistogram parses UPDATE | DROP HISTOGRAM ON columns with its WITH n BUCKETS,
// WITH SYNC | ASYNC MODE, MANUAL | AUTO UPDATE and USING DATA options.
// https://dev.mysql.com/doc/refman/8.4/en/analyze-table.html
func (p *Parser) parseAnalyzeHistogram() *Expr {
	this := upperText(p.prev)
	var expression *Expr
	expressions := []*Expr{}
	var updateOptions any

	if p.matchTextSeq("HISTOGRAM", "ON") {
		expressions = p.parseCSV(p.parseColumnReference, TK_COMMA)
		withExpressions := []string{}
		for p.match(TK_WITH) {
			// https://docs.starrocks.io/docs/sql-reference/sql-statements/cbo_stats/ANALYZE_TABLE/
			if p.matchTexts("SYNC", "ASYNC") {
				if p.matchTextSeqNoAdvance("MODE") {
					withExpressions = append(withExpressions, upperText(p.prev)+" MODE")
					p.advance(1)
				}
			} else {
				buckets := p.parseNumber()
				if p.matchTextSeq("BUCKETS") {
					withExpressions = append(withExpressions, chunkFPyStr(buckets)+" BUCKETS")
				}
			}
		}
		if len(withExpressions) > 0 {
			expression = p.expression(New(KAnalyzeWith, "expressions", withExpressions))
		}

		if p.matchTexts("MANUAL", "AUTO") && p.matchNoAdvance(TK_UPDATE) {
			updateOptions = upperText(p.prev)
			p.advance(1)
		} else if p.matchTextSeq("USING", "DATA") {
			expression = p.expression(New(KUsingData, "this", p.parseString()))
		}
	}

	return p.expression(New(
		KAnalyzeHistogram,
		"this", this,
		"expressions", expressions,
		"expression", expression,
		"update_options", updateOptions,
	))
}

func (p *Parser) parseMerge() *Expr {
	p.match(TK_INTO)
	target := p.parseTable(false, false, nil, false, false, false, false)

	if target != nil && p.matchNoAdvance(TK_ALIAS) {
		target.Set("alias", p.parseTableAlias(nil))
	}

	p.match(TK_USING)
	using := p.parseTable(false, false, nil, false, false, false, false)

	on := chunkFAnd(p.match(TK_ON), p.parseDisjunction)
	usingCond := chunkFAndList(p.match(TK_USING), p.parseUsingIdentifiers)
	whens := p.parseWhenMatched()
	returning := p.parseReturning()
	return p.expression(New(
		KMerge,
		"this", target,
		"using", using,
		"on", on,
		"using_cond", usingCond,
		"whens", whens,
		"returning", returning,
	))
}

// parseWhenMatched parses the WHEN [NOT] MATCHED [BY TARGET | BY SOURCE] [AND cond] THEN action
// clauses of MERGE.
func (p *Parser) parseWhenMatched() *Expr {
	whens := []*Expr{}

	for p.match(TK_WHEN) {
		matched := !p.match(TK_NOT)
		p.matchTextSeq("MATCHED")
		var source bool
		if p.matchTextSeq("BY", "TARGET") {
			source = false
		} else {
			source = p.matchTextSeq("BY", "SOURCE")
		}
		var condition *Expr
		if p.match(TK_AND) {
			condition = p.parseDisjunction()
		}

		p.match(TK_THEN)

		var then *Expr
		if p.match(TK_INSERT) {
			var this *Expr
			if p.matchTextSeq("ALL", "BY", "NAME") {
				this = Star()
			} else {
				this = p.parseStar()
			}
			if this != nil {
				then = p.expression(New(KInsert, "this", this))
			} else {
				var insertThis *Expr
				if p.matchTextSeq("ROW") {
					insertThis = VarExpr("ROW")
				} else {
					insertThis = p.parseValue(false)
				}
				expression := chunkFAnd(p.matchTextSeq("VALUES"), func() *Expr { return p.parseValue(true) })
				where := p.parseWhere(false)
				then = p.expression(New(
					KInsert,
					"this", insertThis,
					"expression", expression,
					"where", where,
				))
			}
		} else if p.match(TK_UPDATE) {
			expressions := p.parseStar()
			if expressions != nil {
				then = p.expression(New(KUpdate, "expressions", expressions))
			} else if p.matchTextSeq("ALL", "BY", "NAME") {
				then = p.expression(New(KUpdate, "expressions", []*Expr{Star()}))
			} else {
				updExpressions := chunkFAndList(p.match(TK_SET), func() []*Expr {
					return p.parseCSV(p.parseUpdateAssignment, TK_COMMA)
				})
				where := p.parseWhere(false)
				then = p.expression(New(
					KUpdate,
					"expressions", updExpressions,
					"where", where,
				))
			}
		} else if p.match(TK_DELETE) {
			then = p.expression(New(KVar, "this", p.prev.Text))
		} else {
			then = p.parseVarFromOptions(p.s.CONFLICT_ACTIONS, true)
		}

		whens = append(whens, p.expression(New(
			KWhen,
			"matched", matched,
			"source", source,
			"condition", condition,
			"then", then,
		)))
	}
	return p.expression(New(KWhens, "expressions", whens))
}

func (p *Parser) parseShow() *Expr {
	parser := p.findParser(p.s.SHOW_PARSERS, p.s.SHOW_TRIE)
	if parser != nil {
		return parser(p)
	}
	return p.parseAsCommand(p.prev)
}

// parseSetItemAssignment parses one SET assignment (name [=] value), or SET [GLOBAL | SESSION]
// TRANSACTION; nil (cursor restored) when no assignment follows. kind "" means no kind.
func (p *Parser) parseSetItemAssignment(kind string) *Expr {
	index := p.index

	if (kind == "GLOBAL" || kind == "SESSION") && p.matchTextSeq("TRANSACTION") {
		return p.parseSetTransaction(kind == "GLOBAL")
	}

	left := p.parsePrimary()
	if left == nil {
		left = p.parseColumn()
	}
	assignmentDelimiter := p.matchTextSet(p.s.SET_ASSIGNMENT_DELIMITERS)

	if left == nil || (p.s.SET_REQUIRES_ASSIGNMENT_DELIMITER && !assignmentDelimiter) {
		p.retreat(index)
		return nil
	}

	right := p.parseStatement()
	if right == nil {
		right = p.parseIdVar(true, nil)
	}
	if right.IsA(KColumn, KIdentifier) {
		right = VarChecked(right.Name())
	}

	this := p.expression(New(KEQ, "this", left, "expression", right))
	return p.expression(New(KSetItem, "this", this, "kind", chunkFStrOrNil(kind)))
}

func (p *Parser) parseSetTransaction(global bool) *Expr {
	p.matchTextSeq("TRANSACTION")
	characteristics := p.parseCSV(func() *Expr {
		return p.parseVarFromOptions(p.s.TRANSACTION_CHARACTERISTICS, true)
	}, TK_COMMA)
	return p.expression(New(
		KSetItem,
		"expressions", characteristics,
		"kind", "TRANSACTION",
		"global_", global,
	))
}

func (p *Parser) parseSetItem() *Expr {
	parser := p.findParser(p.s.SET_PARSERS, p.s.SET_TRIE)
	if parser != nil {
		return parser(p)
	}
	return p.parseSetItemAssignment("")
}

func (p *Parser) baseParseSet(unset bool, tag bool) *Expr {
	index := p.index
	set := p.expression(New(
		KSet,
		"expressions", p.parseCSV(p.parseSetItem, TK_COMMA),
		"unset", unset,
		"tag", tag,
	))

	if p.curr.ok() {
		p.retreat(index)
		return p.parseAsCommand(p.prev)
	}

	return set
}

// parseVarFromOptions parses a keyword option from options, with its continuation keywords, into a
// Var. An unknown or incomplete option returns nil (cursor restored), after a parse error when
// raiseUnmatched is set.
func (p *Parser) parseVarFromOptions(options OptionsType, raiseUnmatched bool) *Expr {
	start := p.curr
	if !start.ok() {
		return nil
	}

	option := upperText(start)
	// isNone marks an excluded token or an unknown option, as opposed to a known option without
	// continuations.
	var continuations [][]string
	isNone := true
	if !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(start.Type) {
		if c, ok := options[option]; ok {
			continuations = c
			isNone = false
		}
	}

	index := p.index
	p.advance(1)
	matched := false
	for _, keywords := range continuations {
		if p.matchTextSeq(keywords...) {
			option = option + " " + strings.Join(keywords, " ")
			matched = true
			break
		}
	}
	if !matched {
		if len(continuations) > 0 || isNone {
			if raiseUnmatched {
				p.raiseError("Unknown option "+option, nil)
			}

			p.retreat(index)
			return nil
		}
	}

	return VarChecked(option)
}

// parseAsCommand consumes the rest of the statement into a Command: start's text, then the
// remaining SQL.
func (p *Parser) parseAsCommand(start *Token) *Expr {
	for p.curr.ok() {
		p.advance(1)
	}
	text := []rune(p.findSQL(start, p.prev))
	size := len([]rune(start.Text))
	if size > len(text) {
		size = len(text)
	}
	p.warnUnsupported()
	return New(KCommand, "this", string(text[:size]), "expression", string(text[size:]))
}

// parseDictProperty parses a dictionary property: this(kind([key value ...])).
func (p *Parser) parseDictProperty(this string) *Expr {
	settings := []*Expr{}

	p.matchLParen(nil)
	kind := p.parseIdVar(true, nil)

	if p.match(TK_L_PAREN) {
		for {
			key := p.parseIdVar(true, nil)
			value := p.parseFunction(nil, false, true, false)
			if value == nil {
				value = p.parsePrimaryOrVar()
			}
			if key == nil && value == nil {
				break
			}
			settings = append(settings, p.expression(New(KDictSubProperty, "this", key, "value", value)))
		}
		p.match(TK_R_PAREN)
	}

	p.matchRParen(nil)

	var kindV any
	if kind != nil {
		kindV = kind.Arg("this")
	}
	return p.expression(New(KDictProperty, "this", this, "kind", kindV, "settings", settings))
}

func (p *Parser) parseDictRange(this string) *Expr {
	p.matchLParen(nil)
	hasMin := p.matchTextSeq("MIN")
	var min, max *Expr
	if hasMin {
		min = p.parseVar(false, nil, false)
		if min == nil {
			min = p.parsePrimary()
		}
		p.matchTextSeq("MAX")
		max = p.parseVar(false, nil, false)
		if max == nil {
			max = p.parsePrimary()
		}
	} else {
		max = p.parseVar(false, nil, false)
		if max == nil {
			max = p.parsePrimary()
		}
		min = LiteralInt(0)
	}
	p.matchRParen(nil)
	return p.expression(New(KDictRange, "this", this, "min", min, "max", max))
}

// parseComprehension parses a comprehension tail: expr [, position] IN iterator [IF cond]. It
// returns nil, backing up one token before the start, when IN is missing.
func (p *Parser) parseComprehension(this *Expr) *Expr {
	index := p.index
	expression := p.parseColumn()
	position := chunkFAnd(p.match(TK_COMMA), p.parseColumn)

	if !p.match(TK_IN) {
		p.retreat(index - 1)
		return nil
	}
	iterator := p.parseColumn()
	var condition *Expr
	if p.matchTextSeq("IF") {
		condition = p.parseDisjunction()
	}
	return p.expression(New(
		KComprehension,
		"this", this,
		"expression", expression,
		"position", position,
		"iterator", iterator,
		"condition", condition,
	))
}

// parseHeredoc parses a heredoc string: a HEREDOC_STRING token or $[tag]$ ... $[tag]$.
func (p *Parser) parseHeredoc() *Expr {
	if p.match(TK_HEREDOC_STRING) {
		return p.expression(New(KHeredoc, "this", p.prev.Text))
	}

	if !p.matchTextSeq("$") {
		return nil
	}

	tags := []string{"$"}
	var tagText any

	if p.isConnected() {
		p.advance(1)
		tags = append(tags, upperText(p.prev))
	} else {
		p.raiseError("No closing $ found", nil)
	}

	if tags[len(tags)-1] != "$" {
		if p.isConnected() && p.matchTextSeq("$") {
			tagText = tags[len(tags)-1]
			tags = append(tags, "$")
		} else {
			p.raiseError("No closing $ found", nil)
		}
	}

	heredocStart := p.curr

	for p.curr.ok() {
		if p.matchTextSeqNoAdvance(tags...) {
			this := p.findSQL(heredocStart, p.prev)
			p.advance(len(tags))
			return p.expression(New(KHeredoc, "this", this, "tag", tagText))
		}

		p.advance(1)
	}

	p.raiseError("No closing "+strings.Join(tags, "")+" found", nil)
	return nil
}

// replaceLambda replaces column references to lambda parameters inside node with identifiers (or
// dots), cast to the parameter's type when it declares one.
func (p *Parser) replaceLambda(node *Expr, expressions []*Expr) *Expr {
	if node == nil {
		return node
	}

	lambdaTypes := map[string]*Expr{}
	for _, e := range expressions {
		lambdaTypes[e.Name()] = e.ArgE("to")
	}

	for column := range node.FindAll(KColumn) {
		typ, ok := lambdaTypes[column.Parts()[0].Name()]
		if ok {
			var dotOrID *Expr
			if column.TableName() != "" {
				dotOrID = column.ToDot(true)
			} else {
				dotOrID = column.This()
			}

			if typ != nil {
				dotOrID = p.expression(New(KCast, "this", dotOrID, "to", typ))
			}

			parent := column.Parent()

			if parent.IsA(KDot) {
				for parent.IsA(KDot) {
					if !parent.Parent().IsA(KDot) {
						parent.Replace(dotOrID)
						break
					}
					parent = parent.Parent()
				}
			} else {
				if column == node {
					node = dotOrID
				} else {
					column.Replace(dotOrID)
				}
			}
		}
	}
	return node
}

func (p *Parser) parseTruncateTable() *Expr {
	start := p.prev

	// Not to be confused with TRUNCATE(number, decimals) function call
	if p.match(TK_L_PAREN) {
		p.retreat(p.index - 2)
		return p.parseFunction(nil, false, true, false)
	}

	// Clickhouse supports TRUNCATE DATABASE as well
	isDatabase := p.match(TK_DATABASE)

	p.match(TK_TABLE)

	exists := p.parseExists(false)

	expressions := p.parseCSV(func() *Expr {
		return p.parseTable(true, false, nil, false, isDatabase, false, false)
	}, TK_COMMA)

	var cluster *Expr
	if p.match(TK_ON) {
		cluster = p.parseOnProperty()
	}

	var identity any
	if p.matchTextSeq("RESTART", "IDENTITY") {
		identity = "RESTART"
	} else if p.matchTextSeq("CONTINUE", "IDENTITY") {
		identity = "CONTINUE"
	} else {
		identity = nil
	}

	var option any
	if p.matchTextSeq("CASCADE") || p.matchTextSeq("RESTRICT") {
		option = p.prev.Text
	} else {
		option = nil
	}

	partition := p.parsePartition()

	if p.curr.ok() {
		return p.parseAsCommand(start)
	}

	return p.expression(New(
		KTruncateTable,
		"expressions", expressions,
		"is_database", isDatabase,
		"exists", exists,
		"cluster", cluster,
		"identity", identity,
		"option", option,
		"partition", partition,
	))
}

func (p *Parser) parseIndexedColumn() *Expr {
	return p.parseOrdered(p.parseOpclass)
}

func (p *Parser) parseWithOperator() *Expr {
	this := p.parseIndexedColumn()

	if !p.match(TK_WITH) {
		return this
	}

	op := p.parseVar(true, &p.s.RESERVED_TOKENS, false)

	return p.expression(New(KWithOperator, "this", this, "op", op))
}

func (p *Parser) parseWrappedOptions() []*Expr {
	p.match(TK_EQ)
	p.match(TK_L_PAREN)

	opts := []*Expr{}
	for p.curr.ok() && !p.match(TK_R_PAREN) {
		var option any
		if p.matchTextSeq("FORMAT_NAME", "=") {
			// The FORMAT_NAME can be set to an identifier for Snowflake and T-SQL
			option = p.parseFormatName()
		} else {
			option = p.parseProperty()
		}

		if chunkFIsNone(option) {
			p.raiseError("Unable to parse option", nil)
			break
		}

		opts = append(opts, chunkFEnsureList(option)...)
	}

	return opts
}

func (p *Parser) parseCopyParameters() []*Expr {
	hasSep := p.d.S.COPY_PARAMS_ARE_CSV
	sep := TK_COMMA

	options := []*Expr{}
	for p.curr.ok() && !p.matchNoAdvance(TK_R_PAREN) {
		option := p.parseVar(true, nil, false)
		if option == nil {
			break
		}

		prev := upperText(p.prev)

		// Different dialects might separate options and values by white space, "=" and "AS"
		p.match(TK_EQ)
		p.match(TK_ALIAS)

		param := p.expression(New(KCopyParameter, "this", option))

		if p.s.COPY_INTO_VARLEN_OPTIONS.Has(prev) && p.matchNoAdvance(TK_L_PAREN) {
			// Snowflake FILE_FORMAT case, Databricks COPY & FORMAT options
			param.Set("expressions", p.parseWrappedOptions())
		} else if prev == "FILE_FORMAT" {
			// T-SQL's external file format case, Snowflake's possibly qualified format name
			expression := p.parseString()
			if expression == nil {
				expression = p.parseTableParts(false, false, false, false)
			}
			param.Set("expression", expression)
		} else if prev == "FORMAT" && p.prev.Type == TK_ALIAS && p.matchTexts("AVRO", "JSON") {
			param.Set("this", VarExpr("FORMAT AS "+upperText(p.prev)))
			param.Set("expression", p.parseField(false, nil, false))
		} else {
			value := p.parseUnquotedField()
			if value == nil {
				value = p.parseBracket(nil)
			}
			param.Set("expression", value)
		}

		options = append(options, param)

		if hasSep {
			p.match(sep)
		}
	}

	return options
}

func (p *Parser) parseCredentials() *Expr {
	expr := p.expression(New(KCredentials))

	if p.matchTextSeq("STORAGE_INTEGRATION", "=") {
		expr.Set("storage", p.parseField(false, nil, false))
	}
	if p.matchTextSeq("CREDENTIALS") {
		// Snowflake case: CREDENTIALS = (...), Redshift case: CREDENTIALS <string>
		var creds any
		if p.match(TK_EQ) {
			creds = p.parseWrappedOptions()
		} else {
			creds = p.parseField(false, nil, false)
		}
		expr.Set("credentials", creds)
	}
	if p.matchTextSeq("ENCRYPTION") {
		expr.Set("encryption", p.parseWrappedOptions())
	}
	if p.matchTextSeq("IAM_ROLE") {
		var iamRole *Expr
		if p.match(TK_DEFAULT) {
			iamRole = VarChecked(p.prev.Text)
		} else {
			iamRole = p.parseField(false, nil, false)
		}
		expr.Set("iam_role", iamRole)
	}
	if p.matchTextSeq("REGION") {
		expr.Set("region", p.parseField(false, nil, false))
	}

	return expr
}

func (p *Parser) baseParseFileLocation() *Expr {
	return p.parseField(false, nil, false)
}

func (p *Parser) parseCopy() *Expr {
	start := p.prev

	p.match(TK_INTO)

	var this *Expr
	if p.matchNoAdvance(TK_L_PAREN) {
		this = p.parseSelect(true, false, false, true, true, nil)
	} else {
		this = p.parseTable(true, false, nil, false, false, false, false)
	}

	kind := p.match(TK_FROM) || !p.matchTextSeq("TO")

	files := p.parseCSV(p.parseFileLocation, TK_COMMA)
	if p.matchNoAdvance(TK_EQ) {
		// Backtrack one token since we've consumed the lhs of a parameter assignment here.
		// This can happen for Snowflake dialect. Instead, we'd like to parse the parameter
		// list via parseWrappedList below.
		p.advance(-1)
		files = []*Expr{}
	}

	credentials := p.parseCredentials()

	p.matchTextSeq("WITH")

	params := p.parseWrappedList(p.parseCopyParameters, true)

	if p.curr.ok() {
		return p.parseAsCommand(start)
	}

	return p.expression(New(
		KCopy,
		"this", this,
		"kind", kind,
		"credentials", credentials,
		"files", files,
		"params", params,
	))
}

func (p *Parser) parseNormalize() *Expr {
	this := p.parseBitwise()
	form := chunkFAnd(p.match(TK_COMMA), func() *Expr { return p.parseVar(false, nil, false) })
	return p.expression(New(KNormalize, "this", this, "form", form))
}

func (p *Parser) parseCeilFloor(exprType Kind) *Expr {
	args := p.parseCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA)

	this := seqGet(args, 0)
	decimals := seqGet(args, 1)

	var to *Expr
	if p.matchTextSeq("TO") {
		to = p.parseVar(false, nil, false)
	}
	return New(exprType, "this", this, "decimals", decimals, "to", to)
}

func (p *Parser) parseStarOps() *Expr {
	starToken := p.prev

	if p.matchTextSeqNoAdvance("COLUMNS", "(") {
		this := p.parseFunction(nil, false, true, false)
		if this.IsA(KColumns) {
			this.Set("unpack", true)
		}
		return this
	}

	var ilike *Expr
	if p.match(TK_ILIKE) {
		ilike = p.parseString()
	}

	except := chunkFListOrNone(p.parseStarOp("EXCEPT", "EXCLUDE"))
	replace := chunkFListOrNone(p.parseStarOp("REPLACE"))
	rename := chunkFListOrNone(p.parseStarOp("RENAME"))
	star := p.expression(New(
		KStar,
		"ilike", ilike,
		"except_", except,
		"replace", replace,
		"rename", rename,
	))
	star.updatePositionsTok(starToken)
	return star
}

// chunkFListOrNone maps a nil list (absent) to an untyped nil argument value.
func chunkFListOrNone(l []*Expr) any {
	if l == nil {
		return nil
	}
	return l
}

func (p *Parser) parseGrantPrivilege() *Expr {
	privilegeParts := []string{}

	// Keep consuming consecutive keywords until comma (end of this privilege) or ON
	// (end of privilege list) or L_PAREN (start of column list) are met
	for p.curr.ok() && !p.matchSetNoAdvance(p.s.PRIVILEGE_FOLLOW_TOKENS) {
		privilegeParts = append(privilegeParts, upperText(p.curr))
		p.advance(1)
	}

	this := VarChecked(strings.Join(privilegeParts, " "))
	var expressions any
	if p.matchNoAdvance(TK_L_PAREN) {
		expressions = p.parseWrappedCSV(p.parseColumn, TK_COMMA, false)
	}

	return p.expression(New(KGrantPrivilege, "this", this, "expressions", expressions))
}

// parseGrantPrincipal parses [ROLE | GROUP] name; kind is false (not nil) without ROLE or GROUP.
func (p *Parser) parseGrantPrincipal() *Expr {
	var kind any = false
	if p.matchTexts("ROLE", "GROUP") {
		kind = upperText(p.prev)
	}
	principal := p.parseIdVar(true, nil)

	if principal == nil {
		return nil
	}

	return p.expression(New(KGrantPrincipal, "this", principal, "kind", kind))
}

// parseGrantRevokeCommon parses the privileges, ON [kind] and securable shared by GRANT and
// REVOKE. The kind result "" means no kind.
func (p *Parser) parseGrantRevokeCommon() ([]*Expr, string, *Expr) {
	privileges := p.parseCSV(p.parseGrantPrivilege, TK_COMMA)

	p.match(TK_ON)
	kind := ""
	if p.matchSet(p.s.CREATABLES) {
		kind = upperText(p.prev)
	}

	// Attempt to parse the securable e.g. MySQL allows names
	// such as "foo.*", "*.*" which are not easily parseable yet
	securable := p.tryParseExpr(func() *Expr { return p.parseTableParts(false, false, false, false) }, false)

	return privileges, kind, securable
}

func (p *Parser) parseGrant() *Expr {
	start := p.prev

	privileges, kind, securable := p.parseGrantRevokeCommon()

	if securable == nil || !p.matchTextSeq("TO") {
		return p.parseAsCommand(start)
	}

	principals := p.parseCSV(p.parseGrantPrincipal, TK_COMMA)

	grantOption := p.matchTextSeq("WITH", "GRANT", "OPTION")

	if p.curr.ok() {
		return p.parseAsCommand(start)
	}

	return p.expression(New(
		KGrant,
		"privileges", privileges,
		"kind", chunkFStrOrNil(kind),
		"securable", securable,
		"principals", principals,
		"grant_option", grantOption,
	))
}

func (p *Parser) parseRevoke() *Expr {
	start := p.prev

	grantOption := p.matchTextSeq("GRANT", "OPTION", "FOR")

	privileges, kind, securable := p.parseGrantRevokeCommon()

	if securable == nil || !p.matchTextSeq("FROM") {
		return p.parseAsCommand(start)
	}

	principals := p.parseCSV(p.parseGrantPrincipal, TK_COMMA)

	var cascade any
	if p.matchTexts("CASCADE", "RESTRICT") {
		cascade = upperText(p.prev)
	}

	if p.curr.ok() {
		return p.parseAsCommand(start)
	}

	return p.expression(New(
		KRevoke,
		"privileges", privileges,
		"kind", chunkFStrOrNil(kind),
		"securable", securable,
		"principals", principals,
		"grant_option", grantOption,
		"cascade", cascade,
	))
}

// parseOverlay parses the arguments of OVERLAY(this PLACING x FROM n [FOR m]), commas allowed.
func (p *Parser) parseOverlay() *Expr {
	parseOverlayArg := func(text string) *Expr {
		if p.match(TK_COMMA) || p.matchTextSeq(text) {
			return p.parseBitwise()
		}
		return nil
	}

	this := p.parseBitwise()
	expression := parseOverlayArg("PLACING")
	from := parseOverlayArg("FROM")
	for_ := parseOverlayArg("FOR")
	return p.expression(New(
		KOverlay,
		"this", this,
		"expression", expression,
		"from_", from,
		"for_", for_,
	))
}

func (p *Parser) parseFormatName() *Expr {
	// Note: Although not specified in the docs, Snowflake does accept a string/identifier
	// for FILE_FORMAT = <format_name>
	value := p.parseString()
	if value == nil {
		value = p.parseTableParts(false, false, false, false)
	}
	return p.expression(New(KProperty, "this", VarExpr("FORMAT_NAME"), "value", value))
}

// parseDistinctArgFunction parses the arguments of func_ with an optional DISTINCT or ALL; with
// DISTINCT, the argument at distinctIndex (negative counts from the end) is wrapped in a Distinct.
func (p *Parser) parseDistinctArgFunction(func_ Kind, distinctIndex int) *Expr {
	isDistinct := p.match(TK_DISTINCT)
	if !isDistinct {
		p.match(TK_ALL)
	}

	args := []*Expr{p.parseLambda(false)}
	if p.match(TK_COMMA) {
		args = append(args, p.parseFunctionArgs(false)...)
	}

	target := seqGet(args, distinctIndex)
	if isDistinct && target != nil {
		i := distinctIndex
		if i < 0 {
			i += len(args)
		}
		args[i] = p.expression(New(KDistinct, "expressions", []*Expr{target}))
	}

	return FromArgList(func_, args)
}

// parseDeclareitem parses one DECLARE item: [VAR | VARIABLE] names [AS] type | TABLE (...)
// [DEFAULT | = value].
func (p *Parser) parseDeclareitem() *Expr {
	p.matchTexts("VAR", "VARIABLE")

	vars := p.parseCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA)
	if len(vars) == 0 {
		return nil
	}

	p.match(TK_ALIAS)
	var kind *Expr
	if p.match(TK_TABLE) {
		kind = p.parseSchema(nil)
	} else {
		kind = p.parseTypes(false, false, true, false)
	}
	default_ := chunkFAnd(p.match(TK_DEFAULT) || p.match(TK_EQ), p.parseBitwise)

	return p.expression(New(KDeclareItem, "this", vars, "kind", kind, "default", default_))
}

func (p *Parser) parseDeclare() *Expr {
	start := p.prev
	replace := p.matchTextSeq("OR", "REPLACE")
	expressions := tryParse(p, func() []*Expr {
		return p.parseCSV(p.parseDeclareitem, TK_COMMA)
	}, false, func(l []*Expr) bool { return len(l) == 0 })

	if len(expressions) == 0 || p.curr.ok() {
		return p.parseAsCommand(start)
	}

	return p.expression(New(KDeclare, "expressions", expressions, "replace", replace))
}

func (p *Parser) baseBuildCast(strict bool, kv ...any) *Expr {
	expClass := KCast
	if !strict {
		expClass = KTryCast
	}

	if expClass == KTryCast {
		var requiresString any
		switch p.d.S.TRY_CAST_REQUIRES_STRING {
		case TriTrue:
			requiresString = true
		case TriFalse:
			requiresString = false
		}
		// Set requires_string, replacing an existing value in place.
		out := make([]any, 0, len(kv)+2)
		found := false
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i] == "requires_string" {
				out = append(out, kv[i], requiresString)
				found = true
			} else {
				out = append(out, kv[i], kv[i+1])
			}
		}
		if !found {
			out = append(out, "requires_string", requiresString)
		}
		kv = out
	}

	return p.expression(New(expClass, kv...))
}

func (p *Parser) parseJsonValue(jsonPath bool) *Expr {
	this := p.parseBitwise()
	p.match(TK_COMMA)
	path := p.parseBitwise()

	returning := chunkFAnd(p.match(TK_RETURNING), func() *Expr { return p.parseTypes(false, false, true, false) })

	if jsonPath {
		path = p.d.toJSONPath(path)
	}
	onCondition := p.parseOnCondition()
	return p.expression(New(
		KJSONValue,
		"this", this,
		"path", path,
		"returning", returning,
		"on_condition", onCondition,
	))
}

// baseParseGroupConcat parses the arguments of GROUP_CONCAT: expressions (concatenated when there
// are several) with optional ORDER BY and SEPARATOR.
func (p *Parser) baseParseGroupConcat() *Expr {
	var args []*Expr

	concatExprs := func(node *Expr, exprs []*Expr) *Expr {
		if node.IsA(KDistinct) && len(node.Expressions()) > 1 {
			concatExprs := []*Expr{
				p.expression(New(
					KConcat,
					"expressions", node.Expressions(),
					"safe", true,
					"coalesce", p.d.S.CONCAT_COALESCE,
				)),
			}
			node.Set("expressions", concatExprs)
			return node
		}
		if len(exprs) == 1 {
			return exprs[0]
		}
		return p.expression(New(
			KConcat,
			"expressions", args,
			"safe", true,
			"coalesce", p.d.S.CONCAT_COALESCE,
		))
	}

	args = p.parseCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA)

	var this *Expr
	if len(args) > 0 {
		var order *Expr
		if args[len(args)-1].IsA(KOrder) {
			order = args[len(args)-1]
		}

		if order != nil {
			// Order By is the last (or only) expression in the list and has consumed the 'expr' before it,
			// remove 'expr' from the Order and add it back to args
			args[len(args)-1] = order.This()
			order.Set("this", concatExprs(order.This(), args))
		}

		this = order
		if this == nil {
			this = concatExprs(args[0], args)
		}
	} else {
		this = nil
	}

	var separator *Expr
	if p.match(TK_SEPARATOR) {
		separator = p.parseField(false, nil, false)
	}

	return p.expression(New(KGroupConcat, "this", this, "separator", separator))
}

// parseInitcap parses INITCAP, adding the dialect's default delimiters when none are given.
func (p *Parser) parseInitcap() *Expr {
	expr := FromArgList(KInitcap, p.parseFunctionArgs(false))

	if expr.Arg("expression") == nil {
		expr.Set("expression", LiteralString(p.d.S.INITCAP_DEFAULT_DELIMITER_CHARS))
	}

	return expr
}

// parseOperator parses OPERATOR(op) <operand> after this. Without the parenthesis it gives back
// the OPERATOR token and returns nil, so OPERATOR can be an implicit alias.
func (p *Parser) parseOperator(this *Expr) *Expr {
	if !p.match(TK_L_PAREN) {
		p.retreat(p.index - 1)
		return nil
	}

	var opText strings.Builder
	for p.curr.ok() && !p.match(TK_R_PAREN) {
		opText.WriteString(p.curr.Text)
		p.advance(1)
	}
	op := opText.String()

	comments := p.prevComments
	return p.expressionC(New(KOperator, "this", this, "operator", op, "expression", p.parseBitwise()), comments)
}
