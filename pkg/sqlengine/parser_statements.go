package sqlengine

import (
	"fmt"
	"strings"
)

// Port of sqlglot/parser.py (chunk F, part 1): transactions, REFRESH, ALTER, ANALYZE, MERGE,
// SHOW, SET, option vars, commands, dictionary properties, comprehensions, heredocs, lambda
// replacement, TRUNCATE, COPY, misc function parsers, GRANT/REVOKE, DECLARE, build_cast,
// JSON_VALUE, GROUP_CONCAT, INITCAP and OPERATOR. The pipe syntax lives in parser_pipe.go.

// ---------------------------------------------------------------------------------------------
// Small helpers mirroring Python value semantics used by this chunk.
// ---------------------------------------------------------------------------------------------

// chunkFAnd mirrors `cond and fn()` where cond is a bool returned by a _match* call:
// the result is False when cond is false, otherwise fn()'s value (None when nil).
func chunkFAnd(cond bool, fn func() *Expr) any {
	if !cond {
		return false
	}
	if e := fn(); e != nil {
		return e
	}
	return nil
}

// chunkFAndList mirrors `cond and fn()` for list-returning parse methods.
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

// chunkFAndAny mirrors `cond and fn()` for parse methods returning Expr | list | None.
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

// chunkFStrOrNil maps the Go "" (Python None) to nil.
func chunkFStrOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// chunkFIsNone reports whether a dynamically typed parse result is Python None.
func chunkFIsNone(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case *Expr:
		return x == nil
	}
	return false
}

// chunkFEnsureList mirrors helper.ensure_list for Expr | list | None values.
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

// chunkFPyStr mirrors str(expression) inside an f-string (None renders as "None").
func chunkFPyStr(e *Expr) string {
	if e == nil {
		return "None"
	}
	return exprSQL(e)
}

// chunkFParsePropertyCSV mirrors self._parse_csv(self._parse_property). Property parsers may
// return a list; Python would nest it inside the result list, which an Expr argument cannot
// represent, so list results are flattened in place.
func (p *Parser) chunkFParsePropertyCSV() []*Expr {
	items := parseCSVAny(p, p.parseProperty, TK_COMMA, chunkFIsNone)
	out := []*Expr{}
	for _, it := range items {
		out = append(out, chunkFEnsureList(it)...)
	}
	return out
}

// ---------------------------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------------------------

// _parse_transaction (parser.py L8721)
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

// _parse_commit_or_rollback (parser.py L8741)
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

// _parse_refresh (parser.py L8761)
func (p *Parser) parseRefresh() *Expr {
	var kind string
	if p.match(TK_TABLE) {
		kind = "TABLE"
	} else if p.matchTextSeq("MATERIALIZED", "VIEW") {
		kind = "MATERIALIZED VIEW"
	} else {
		kind = ""
	}

	this := p.parseString()
	if this == nil {
		this = p.parseTable(false, false, nil, false, false, false, false)
	}
	if kind == "" && !this.IsA(KLiteral) {
		return p.parseAsCommand(p.prev)
	}

	return p.expression(New(KRefresh, "this", this, "kind", kind))
}

// ---------------------------------------------------------------------------------------------
// ALTER
// ---------------------------------------------------------------------------------------------

// _parse_column_def_with_exists (parser.py L8775)
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

// _parse_add_column (parser.py L8790)
func (p *Parser) parseAddColumn() *Expr {
	if !(upperText(p.prev) == "ADD") {
		return nil
	}

	return p.parseColumnDefWithExists()
}

// _parse_drop_column (parser.py L8796)
func (p *Parser) baseParseDropColumn() *Expr {
	var drop *Expr
	if p.match(TK_DROP) {
		drop = p.parseDrop(false)
	}
	if drop != nil && !drop.IsA(KCommand) {
		// drop.args.get("kind", "COLUMN")
		var kind any = "COLUMN"
		if drop.HasArgKey("kind") {
			kind = drop.Arg("kind")
		}
		drop.Set("kind", kind)
	}
	return drop
}

// _parse_alter_drop_action (parser.py L8802)
func (p *Parser) baseParseAlterDropAction() *Expr {
	return p.parseDropColumn()
}

// _parse_drop_partition (parser.py L8806)
// https://docs.aws.amazon.com/athena/latest/ug/alter-table-drop-partition.html
func (p *Parser) parseDropPartition(exists bool) *Expr {
	return p.expression(New(
		KDropPartition,
		"expressions", p.parseCSV(p.parsePartition, TK_COMMA),
		"exists", exists,
	))
}

// _parse_alter_table_add (parser.py L8811)
func (p *Parser) parseAlterTableAdd() []*Expr {
	parseAddAlteration := func() *Expr {
		p.matchTextSeq("ADD")
		if p.matchSetNoAdvance(p.s.ADD_CONSTRAINT_TOKENS) {
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

// _parse_alter_table_alter (parser.py L8850)
func (p *Parser) baseParseAlterTableAlter() *Expr {
	if matchTextKeys(p, p.s.ALTER_ALTER_PARSERS) {
		return p.s.ALTER_ALTER_PARSERS[upperText(p.prev)](p)
	}

	// Many dialects support the ALTER [COLUMN] syntax, so if there is no
	// keyword after ALTER we default to parsing this statement
	p.match(TK_COLUMN)
	column := p.parseField(true, nil, false)

	if p.matchPair(TK_DROP, TK_DEFAULT) {
		return p.expression(New(KAlterColumn, "this", column, "drop", true))
	}
	if p.matchPair(TK_SET, TK_DEFAULT) {
		return p.expression(New(KAlterColumn, "this", column, "default", p.parseDisjunction()))
	}
	if p.match(TK_COMMENT) {
		return p.expression(New(KAlterColumn, "this", column, "comment", p.parseString()))
	}
	if p.matchTextSeq("DROP", "NOT", "NULL") {
		return p.expression(New(KAlterColumn, "this", column, "drop", true, "allow_null", true))
	}
	if p.matchTextSeq("SET", "NOT", "NULL") {
		return p.expression(New(KAlterColumn, "this", column, "allow_null", false))
	}

	if p.matchTextSeq("SET", "VISIBLE") {
		return p.expression(New(KAlterColumn, "this", column, "visible", "VISIBLE"))
	}
	if p.matchTextSeq("SET", "INVISIBLE") {
		return p.expression(New(KAlterColumn, "this", column, "visible", "INVISIBLE"))
	}

	p.matchTextSeq("SET", "DATA")
	p.matchTextSeq("TYPE")
	dtype := p.parseTypes(false, false, true, false)
	collate := chunkFAnd(p.match(TK_COLLATE), p.parseTerm)
	using := chunkFAnd(p.match(TK_USING), p.parseDisjunction)
	return p.expression(New(
		KAlterColumn,
		"this", column,
		"dtype", dtype,
		"collate", collate,
		"using", using,
	))
}

// _parse_alter_diststyle (parser.py L8886)
func (p *Parser) parseAlterDiststyle() *Expr {
	if p.matchTexts("ALL", "EVEN", "AUTO") {
		return p.expression(New(KAlterDistStyle, "this", VarExpr(upperText(p.prev))))
	}

	p.matchTextSeq("KEY", "DISTKEY")
	return p.expression(New(KAlterDistStyle, "this", p.parseColumn()))
}

// _parse_alter_sortkey (parser.py L8893)
// compound=false stands for the Python default None (callers never pass False explicitly).
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

// _parse_alter_table_drop (parser.py L8907)
func (p *Parser) parseAlterTableDrop() []*Expr {
	index := p.index - 1

	partitionExists := p.parseExists(false)
	if p.matchNoAdvance(TK_PARTITION) {
		return p.parseCSV(func() *Expr { return p.parseDropPartition(partitionExists) }, TK_COMMA)
	}

	p.retreat(index)
	return p.parseCSV(p.parseAlterDropAction, TK_COMMA)
}

// _parse_alter_table_rename (parser.py L8917)
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

// _parse_alter_table_set (parser.py L8934)
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

// _parse_alter_session (parser.py L8970)
// Parse ALTER SESSION SET/UNSET statements.
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

// _parse_alter (parser.py L8982)
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
	if parser != nil {
		actions := chunkFEnsureList(parser(p))
		notValid := p.matchTextSeq("NOT", "VALID")
		options := p.chunkFParsePropertyCSV()
		cascade := p.d.S.ALTER_TABLE_SUPPORTS_CASCADE && p.matchTextSeq("CASCADE")

		if !p.curr.ok() && len(actions) > 0 {
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

// ---------------------------------------------------------------------------------------------
// ANALYZE
// ---------------------------------------------------------------------------------------------

// _parse_analyze (parser.py L9034)
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

	var kind any
	if p.curr.ok() {
		kind = upperText(p.curr)
	}

	if p.match(TK_TABLE) || p.match(TK_INDEX) {
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
		this = p.parseTableParts(false, false, false, false)
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
		"mode", mode,
		"partition", partition,
		"properties", properties,
		"expression", innerExpression,
		"options", options,
	))
}

// chunkFPyStrAny mirrors str(v) for the simple values used in f-strings here.
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

// _parse_analyze_statistics (parser.py L9100)
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

// _parse_analyze_validate (parser.py L9133)
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

// _parse_analyze_columns (parser.py L9154)
func (p *Parser) parseAnalyzeColumns() *Expr {
	this := upperText(p.prev)
	if p.matchTextSeq("COLUMNS") {
		return p.expression(New(KAnalyzeColumns, "this", this+" "+upperText(p.prev)))
	}
	return nil
}

// _parse_analyze_delete (parser.py L9160)
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

// _parse_analyze_list (parser.py L9166)
func (p *Parser) parseAnalyzeList() *Expr {
	if p.matchTextSeq("CHAINED", "ROWS") {
		return p.expression(New(KAnalyzeListChainedRows, "expression", p.parseInto()))
	}
	return nil
}

// _parse_analyze_histogram (parser.py L9172)
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

// ---------------------------------------------------------------------------------------------
// MERGE
// ---------------------------------------------------------------------------------------------

// _parse_merge (parser.py L9211)
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

// _parse_when_matched (parser.py L9232)
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
			this := p.parseStar()
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
			} else {
				updExpressions := chunkFAndList(p.match(TK_SET), func() []*Expr {
					return p.parseCSV(p.parseEquality, TK_COMMA)
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

// ---------------------------------------------------------------------------------------------
// SHOW / SET
// ---------------------------------------------------------------------------------------------

// _parse_show (parser.py L9285)
func (p *Parser) parseShow() *Expr {
	parser := p.findParser(p.s.SHOW_PARSERS, p.s.SHOW_TRIE)
	if parser != nil {
		return parser(p)
	}
	return p.parseAsCommand(p.prev)
}

// _parse_set_item_assignment (parser.py L9291)
// kind "" stands for Python None.
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

// _parse_set_transaction (parser.py L9311)
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

// _parse_set_item (parser.py L9320)
func (p *Parser) parseSetItem() *Expr {
	parser := p.findParser(p.s.SET_PARSERS, p.s.SET_TRIE)
	if parser != nil {
		return parser(p)
	}
	return p.parseSetItemAssignment("")
}

// _parse_set (parser.py L9324)
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

// _parse_var_from_options (parser.py L9336)
func (p *Parser) parseVarFromOptions(options OptionsType, raiseUnmatched bool) *Expr {
	start := p.curr
	if !start.ok() {
		return nil
	}

	option := upperText(start)
	// continuations is None when the token is excluded or the option is unknown.
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

// _parse_as_command (parser.py L9367)
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

// ---------------------------------------------------------------------------------------------
// Dictionary properties, comprehensions, heredocs
// ---------------------------------------------------------------------------------------------

// _parse_dict_property (parser.py L9375)
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

// _parse_dict_range (parser.py L9396)
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

// _parse_comprehension (parser.py L9409)
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

// _parse_heredoc (parser.py L9429)
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

// _replace_lambda (parser.py L9497)
func (p *Parser) replaceLambda(node *Expr, expressions []*Expr) *Expr {
	if node == nil {
		return node
	}

	// lambda_types = {e.name: e.args.get("to") or False}; a nil value stands for False.
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

// ---------------------------------------------------------------------------------------------
// TRUNCATE, index columns, options, COPY
// ---------------------------------------------------------------------------------------------

// _parse_truncate_table (parser.py L9527)
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

	// Fallback case
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

// _parse_indexed_column (parser.py L9578)
func (p *Parser) parseIndexedColumn() *Expr {
	return p.parseOrdered(p.parseOpclass)
}

// _parse_with_operator (parser.py L9581)
func (p *Parser) parseWithOperator() *Expr {
	this := p.parseIndexedColumn()

	if !p.match(TK_WITH) {
		return this
	}

	op := p.parseVar(true, &p.s.RESERVED_TOKENS, false)

	return p.expression(New(KWithOperator, "this", this, "op", op))
}

// _parse_wrapped_options (parser.py L9591)
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

// _parse_copy_parameters (parser.py L9612)
func (p *Parser) parseCopyParameters() []*Expr {
	hasSep := p.d.S.COPY_PARAMS_ARE_CSV
	sep := TK_COMMA

	options := []*Expr{}
	for p.curr.ok() && !p.matchNoAdvance(TK_R_PAREN) {
		option := p.parseVar(true, nil, false)
		prev := upperText(p.prev)

		// Different dialects might separate options and values by white space, "=" and "AS"
		p.match(TK_EQ)
		p.match(TK_ALIAS)

		param := p.expression(New(KCopyParameter, "this", option))

		if p.s.COPY_INTO_VARLEN_OPTIONS.Has(prev) && p.matchNoAdvance(TK_L_PAREN) {
			// Snowflake FILE_FORMAT case, Databricks COPY & FORMAT options
			param.Set("expressions", p.parseWrappedOptions())
		} else if prev == "FILE_FORMAT" {
			// T-SQL's external file format case
			param.Set("expression", p.parseField(false, nil, false))
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

// _parse_credentials (parser.py L9651)
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

// _parse_file_location (parser.py L9674)
func (p *Parser) baseParseFileLocation() *Expr {
	return p.parseField(false, nil, false)
}

// _parse_copy (parser.py L9677)
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
		// list via `_parse_wrapped(..)` below.
		p.advance(-1)
		files = []*Expr{}
	}

	credentials := p.parseCredentials()

	p.matchTextSeq("WITH")

	params := p.parseWrappedList(p.parseCopyParameters, true)

	// Fallback case
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

// ---------------------------------------------------------------------------------------------
// Misc function parsers
// ---------------------------------------------------------------------------------------------

// _parse_normalize (parser.py L9712)
func (p *Parser) parseNormalize() *Expr {
	this := p.parseBitwise()
	form := chunkFAnd(p.match(TK_COMMA), func() *Expr { return p.parseVar(false, nil, false) })
	return p.expression(New(KNormalize, "this", this, "form", form))
}

// _parse_ceil_floor (parser.py L9719)
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

// _parse_star_ops (parser.py L9731)
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

// chunkFListOrNone maps a nil list (Python None) to an untyped nil argument value.
func chunkFListOrNone(l []*Expr) any {
	if l == nil {
		return nil
	}
	return l
}

// ---------------------------------------------------------------------------------------------
// GRANT / REVOKE
// ---------------------------------------------------------------------------------------------

// _parse_grant_privilege (parser.py L9751)
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

// _parse_grant_principal (parser.py L9769)
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

// _parse_grant_revoke_common (parser.py L9778)
// The kind result "" stands for Python None.
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

// _parse_grant (parser.py L9792)
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

// _parse_revoke (parser.py L9817)
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

// _parse_overlay (parser.py L9847)
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

// _parse_format_name (parser.py L9864)
func (p *Parser) parseFormatName() *Expr {
	// Note: Although not specified in the docs, Snowflake does accept a string/identifier
	// for FILE_FORMAT = <format_name>
	value := p.parseString()
	if value == nil {
		value = p.parseTableParts(false, false, false, false)
	}
	return p.expression(New(KProperty, "this", VarExpr("FORMAT_NAME"), "value", value))
}

// _parse_distinct_arg_function (parser.py L9873)
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

// ---------------------------------------------------------------------------------------------
// DECLARE, casts, JSON_VALUE, GROUP_CONCAT, INITCAP, OPERATOR
// ---------------------------------------------------------------------------------------------

// _parse_declareitem (parser.py L10088)
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

// _parse_declare (parser.py L10103)
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

// build_cast (parser.py L10113)
// kv holds the Python **kwargs as alternating key/value pairs.
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
		// kwargs["requires_string"] = ... (overwrites in place when already present)
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

// _parse_json_value (parser.py L10121)
func (p *Parser) parseJsonValue() *Expr {
	this := p.parseBitwise()
	p.match(TK_COMMA)
	path := p.parseBitwise()

	returning := chunkFAnd(p.match(TK_RETURNING), func() *Expr { return p.parseType(true, false) })

	jsonPath := p.d.toJSONPath(path)
	onCondition := p.parseOnCondition()
	return p.expression(New(
		KJSONValue,
		"this", this,
		"path", jsonPath,
		"returning", returning,
		"on_condition", onCondition,
	))
}

// _parse_group_concat (parser.py L10137)
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
			// remove 'expr' from exp.Order and add it back to args
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

// _parse_initcap (parser.py L10176)
func (p *Parser) parseInitcap() *Expr {
	expr := FromArgList(KInitcap, p.parseFunctionArgs(false))

	// attach dialect's default delimiters
	if expr.Arg("expression") == nil {
		expr.Set("expression", LiteralString(p.d.S.INITCAP_DEFAULT_DELIMITER_CHARS))
	}

	return expr
}

// _parse_operator (parser.py L10185)
func (p *Parser) parseOperator(this *Expr) *Expr {
	for {
		if !p.match(TK_L_PAREN) {
			break
		}

		op := ""
		for p.curr.ok() && !p.match(TK_R_PAREN) {
			op += p.curr.Text
			p.advance(1)
		}

		comments := p.prevComments
		e := New(KOperator, "this", this, "operator", op, "expression", p.parseBitwise())
		this = p.expressionC(e, comments)

		if !p.match(TK_OPERATOR) {
			break
		}
	}

	return this
}
