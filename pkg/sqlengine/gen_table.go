package sqlengine

import (
	"strings"
	"unicode/utf8"
)

// Generator chunk B (part 1): sqlglot/generator.py L2254-2715 (insert_sql .. values_sql).

// insert_sql (generator.py L2254)
func (g *Generator) insertSQL(expression *Expr) string {
	hint := g.sqlKey(expression, "hint")
	overwrite := expression.ArgB("overwrite")

	var this string
	if expression.This().IsA(KDirectory) {
		if overwrite {
			this = " OVERWRITE"
		} else {
			this = " INTO"
		}
	} else {
		if overwrite {
			this = g.s.INSERT_OVERWRITE
		} else {
			this = " INTO"
		}
	}

	stored := g.sqlKey(expression, "stored")
	if stored != "" {
		stored = " " + stored
	}
	alternative := ""
	if v := expression.Arg("alternative"); truthy(v) {
		alternative = " OR " + gchunkBPyStr(v)
	}
	ignore := ""
	if expression.ArgB("ignore") {
		ignore = " IGNORE"
	}
	if expression.ArgB("is_function") {
		this = this + " FUNCTION"
	}
	this = this + " " + g.sqlKey(expression, "this")

	exists := ""
	if expression.ArgB("exists") {
		exists = " IF EXISTS"
	}
	where := g.sqlKey(expression, "where")
	if where != "" {
		where = g.sep1() + "REPLACE WHERE " + where
	}
	expressionSQL := g.sep1() + g.sqlKey(expression, "expression")
	onConflict := g.sqlKey(expression, "conflict")
	if onConflict != "" {
		onConflict = " " + onConflict
	}
	byName := ""
	if expression.ArgB("by_name") {
		byName = " BY NAME"
	}
	defaultValues := ""
	if expression.ArgB("default") {
		defaultValues = "DEFAULT VALUES"
	}
	returning := g.sqlKey(expression, "returning")

	if g.s.RETURNING_END {
		expressionSQL = expressionSQL + onConflict + defaultValues + returning
	} else {
		expressionSQL = returning + expressionSQL + onConflict
	}

	partitionBy := g.sqlKey(expression, "partition")
	if partitionBy != "" {
		partitionBy = " " + partitionBy
	}
	settings := g.sqlKey(expression, "settings")
	if settings != "" {
		settings = " " + settings
	}

	source := g.sqlKey(expression, "source")
	if source != "" {
		source = "TABLE " + source
	}

	sql := "INSERT" + hint + alternative + ignore + this + stored + byName + exists + partitionBy + settings + where + expressionSQL + source
	return g.prependCtes(expression, sql)
}

// introducer_sql (generator.py L2299)
func (g *Generator) introducerSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + " " + g.sqlKey(expression, "expression")
}

// kill_sql (generator.py L2302)
func (g *Generator) killSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = " " + kind
	}
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	return "KILL" + kind + this
}

// pseudotype_sql (generator.py L2309)
func (g *Generator) pseudotypeSQL(expression *Expr) string {
	return expression.Name()
}

// objectidentifier_sql (generator.py L2312)
func (g *Generator) objectidentifierSQL(expression *Expr) string {
	return expression.Name()
}

// onconflict_sql (generator.py L2315)
func (g *Generator) onconflictSQL(expression *Expr) string {
	conflict := "ON CONFLICT"
	if expression.ArgB("duplicate") {
		conflict = "ON DUPLICATE KEY"
	}

	constraint := g.sqlKey(expression, "constraint")
	if constraint != "" {
		constraint = " ON CONSTRAINT " + constraint
	}

	conflictKeys := g.expressions(expression, exprsOpts{key: "conflict_keys", flat: true})
	if conflictKeys != "" {
		conflictKeys = "(" + conflictKeys + ")"
	}

	indexPredicate := g.sqlKey(expression, "index_predicate")
	conflictKeys = conflictKeys + indexPredicate + " "

	action := g.sqlKey(expression, "action")

	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		setKeyword := ""
		if g.s.DUPLICATE_KEY_UPDATE_WITH_SET {
			setKeyword = "SET "
		}
		expressions = " " + setKeyword + expressions
	}

	where := g.sqlKey(expression, "where")
	return conflict + constraint + conflictKeys + action + expressions + where
}

// returning_sql (generator.py L2338)
func (g *Generator) baseReturningSQL(expression *Expr) string {
	return g.seg("RETURNING") + " " + g.expressions(expression, exprsOpts{flat: true})
}

// rowformatdelimitedproperty_sql (generator.py L2341)
func (g *Generator) rowformatdelimitedpropertySQL(expression *Expr) string {
	fields := g.sqlKey(expression, "fields")
	if fields != "" {
		fields = " FIELDS TERMINATED BY " + fields
	}
	escaped := g.sqlKey(expression, "escaped")
	if escaped != "" {
		escaped = " ESCAPED BY " + escaped
	}
	items := g.sqlKey(expression, "collection_items")
	if items != "" {
		items = " COLLECTION ITEMS TERMINATED BY " + items
	}
	keys := g.sqlKey(expression, "map_keys")
	if keys != "" {
		keys = " MAP KEYS TERMINATED BY " + keys
	}
	lines := g.sqlKey(expression, "lines")
	if lines != "" {
		lines = " LINES TERMINATED BY " + lines
	}
	null := g.sqlKey(expression, "null")
	if null != "" {
		null = " NULL DEFINED AS " + null
	}
	return "ROW FORMAT DELIMITED" + fields + escaped + items + keys + lines + null
}

// withtablehint_sql (generator.py L2356)
func (g *Generator) withtablehintSQL(expression *Expr) string {
	return "WITH (" + g.expressions(expression, exprsOpts{flat: true}) + ")"
}

// indextablehint_sql (generator.py L2359)
func (g *Generator) indextablehintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this") + " INDEX"
	target := g.sqlKey(expression, "target")
	if target != "" {
		target = " FOR " + target
	}
	return this + target + " (" + g.expressions(expression, exprsOpts{flat: true}) + ")"
}

// historicaldata_sql (generator.py L2365)
func (g *Generator) historicaldataSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	kind := g.sqlKey(expression, "kind")
	expr := g.sqlKey(expression, "expression")
	return this + " (" + kind + " => " + expr + ")"
}

// table_parts (generator.py L2371)
func (g *Generator) baseTableParts(expression *Expr) string {
	var parts []string
	for _, k := range []string{"catalog", "db", "this"} {
		if part := expression.Arg(k); part != nil {
			parts = append(parts, g.sql(part))
		}
	}
	return strings.Join(parts, ".")
}

// table_sql (generator.py L2382)
func (g *Generator) baseTableSQL(expression *Expr, sep string) string {
	table := g.tableParts(expression)
	only := ""
	if expression.ArgB("only") {
		only = "ONLY "
	}
	partition := g.sqlKey(expression, "partition")
	if partition != "" {
		partition = " " + partition
	}
	version := g.sqlKey(expression, "version")
	if version != "" {
		version = " " + version
	}
	alias := g.sqlKey(expression, "alias")
	if alias != "" {
		alias = sep + alias
	}

	sample := g.sqlKey(expression, "sample")
	postAlias := ""
	preAlias := ""

	if g.d.S.ALIAS_POST_TABLESAMPLE {
		preAlias = sample
	} else {
		postAlias = sample
	}

	if g.d.S.ALIAS_POST_VERSION {
		preAlias = preAlias + version
	} else {
		postAlias = postAlias + version
	}

	hints := g.expressions(expression, exprsOpts{key: "hints", sep: strp2(" ")})
	if hints != "" && g.s.TABLE_HINTS {
		hints = " " + hints
	} else {
		hints = ""
	}
	pivots := g.expressions(expression, exprsOpts{key: "pivots", sep: strp2(""), flat: true})
	joins := g.indent(g.expressions(expression, exprsOpts{key: "joins", sep: strp2(""), flat: true}), 0, -1, true, false)
	laterals := g.expressions(expression, exprsOpts{key: "laterals", sep: strp2("")})

	fileFormat := g.sqlKey(expression, "format")
	pattern := g.sqlKey(expression, "pattern")
	if fileFormat != "" {
		if pattern != "" {
			pattern = ", PATTERN => " + pattern
		} else {
			pattern = ""
		}
		fileFormat = " (FILE_FORMAT => " + fileFormat + pattern + ")"
	} else if pattern != "" {
		fileFormat = " (PATTERN => " + pattern + ")"
	}

	ordinality := ""
	if expression.ArgB("ordinality") {
		ordinality = " WITH ORDINALITY" + alias
		alias = ""
	}

	when := g.sqlKey(expression, "when")
	if when != "" {
		if g.s.HISTORICAL_DATA_POST_ALIAS {
			alias = alias + " " + when
		} else {
			table = table + " " + when
		}
	}

	changes := g.sqlKey(expression, "changes")
	if changes != "" {
		changes = " " + changes
	}

	rowsFrom := g.expressions(expression, exprsOpts{key: "rows_from"})
	if rowsFrom != "" {
		table = "ROWS FROM " + g.wrap(rowsFrom)
	}

	indexed := ""
	if v := expression.Arg("indexed"); v != nil {
		if truthy(v) {
			indexed = " INDEXED BY " + g.sql(v)
		} else {
			indexed = " NOT INDEXED"
		}
	}

	return only + table + changes + partition + fileFormat + preAlias + alias + indexed + hints + pivots + postAlias + joins + laterals + ordinality
}

// tablefromrows_sql (generator.py L2449)
func (g *Generator) baseTablefromrowsSQL(expression *Expr) string {
	table := g.fn("TABLE", expression.Arg("this"))
	alias := g.sqlKey(expression, "alias")
	if alias != "" {
		alias = " AS " + alias
	}
	sample := g.sqlKey(expression, "sample")
	pivots := g.expressions(expression, exprsOpts{key: "pivots", sep: strp2(""), flat: true})
	joins := g.indent(g.expressions(expression, exprsOpts{key: "joins", sep: strp2(""), flat: true}), 0, -1, true, false)
	return table + alias + pivots + sample + joins
}

// tablesample_sql (generator.py L2460)
func (g *Generator) baseTablesampleSQL(expression *Expr, tablesampleKeyword string) string {
	method := g.sqlKey(expression, "method")
	if method != "" && g.s.TABLESAMPLE_WITH_METHOD {
		method = method + " "
	} else {
		method = ""
	}
	numerator := g.sqlKey(expression, "bucket_numerator")
	denominator := g.sqlKey(expression, "bucket_denominator")
	field := g.sqlKey(expression, "bucket_field")
	if field != "" {
		field = " ON " + field
	}
	bucket := ""
	if numerator != "" {
		bucket = "BUCKET " + numerator + " OUT OF " + denominator + field
	}
	seed := g.sqlKey(expression, "seed")
	if seed != "" {
		seed = " " + g.s.TABLESAMPLE_SEED_KEYWORD + " (" + seed + ")"
	}

	size := g.sqlKey(expression, "size")
	if size != "" && g.s.TABLESAMPLE_SIZE_IS_ROWS {
		size = size + " ROWS"
	}

	percent := g.sqlKey(expression, "percent")
	if percent != "" && !g.d.S.TABLESAMPLE_SIZE_IS_PERCENT {
		percent = percent + " PERCENT"
	}

	expr := bucket + percent + size
	if g.s.TABLESAMPLE_REQUIRES_PARENS {
		expr = "(" + expr + ")"
	}

	keyword := tablesampleKeyword
	if keyword == "" {
		keyword = g.s.TABLESAMPLE_KEYWORDS
	}
	return " " + keyword + " " + method + expr + seed
}

// _pivot_in_value_aliases (generator.py L2489)
func (g *Generator) pivotInValueAliases(expression *Expr) []*Expr {
	// Returns the rewritten field.expressions list with PivotAlias wrappers injected where
	// the stored column name differs from the target dialect's natural output.
	columns := expression.ArgL("columns")
	fields := expression.ArgL("fields")
	if len(columns) == 0 || len(fields) != 1 {
		return nil
	}

	parserCls := g.d.P

	tgtIdentifyPivotStrings := parserCls.IDENTIFY_PIVOT_STRINGS
	tgtPrefixedPivotColumns := parserCls.PREFIXED_PIVOT_COLUMNS
	tgtPivotColumnNaming := parserCls.PIVOT_COLUMN_NAMING

	// args.get(key, default)
	argGet := func(key string, def any) any {
		if expression.HasArgKey(key) {
			return expression.Arg(key)
		}
		return def
	}
	srcIdentifyPivotStrings := argGet("identify_pivot_strings", tgtIdentifyPivotStrings)
	srcPrefixedPivotColumns := argGet("prefixed_pivot_columns", tgtPrefixedPivotColumns)
	srcPivotColumnNaming := argGet("pivot_column_naming", tgtPivotColumnNaming)

	if srcIdentifyPivotStrings == any(tgtIdentifyPivotStrings) &&
		srcPrefixedPivotColumns == any(tgtPrefixedPivotColumns) &&
		srcPivotColumnNaming == any(tgtPivotColumnNaming) {
		return nil
	}

	inExprs := fields[0].Expressions()
	step := len(columns) / len(inExprs)

	// Derive the per-value suffix from the first stored column vs the first IN-list value.
	// This correctly handles dialects (e.g. Spark single-agg) that ignore agg aliases.
	var firstBase string
	if truthy(srcIdentifyPivotStrings) {
		firstBase = exprSQL(inExprs[0])
	} else {
		firstBase = inExprs[0].AliasOrName()
	}
	firstStored := columns[0].Name()

	// exit if only suffix matches, not prefix. (e.g. BigQuery, which cannot be fixed)
	if !strings.HasPrefix(firstStored, firstBase) {
		return nil
	}

	suffix := firstStored[len(firstBase):]

	// Whether the target dialect would append an agg-name suffix for this pivot.
	// Spark single-agg uniquely drops the agg alias entirely.
	aggs := expression.Expressions()
	anyAlias := false
	for _, a := range aggs {
		if a.Alias() != "" {
			anyAlias = true
			break
		}
	}
	targetHasSuffix := (len(aggs) > 1 || tgtPivotColumnNaming != "agg_name_if_multiple") && anyAlias
	sourceHasSuffix := suffix != ""

	var newExprs []*Expr
	modified := false
	for valIdx, e := range inExprs {
		if e.IsA(KPivotAlias) {
			newExprs = append(newExprs, e)
			continue
		}

		i := valIdx * step
		storedFull := columns[i].Name()
		storedValue := storedFull
		if suffix != "" {
			// stored_full[: -len(suffix)]
			r := []rune(storedFull)
			n := len(r) - utf8.RuneCountInString(suffix)
			if n < 0 {
				n = 0
			}
			storedValue = string(r[:n])
		}
		var targetValue string
		if tgtIdentifyPivotStrings {
			targetValue = exprSQL(e)
		} else {
			targetValue = e.AliasOrName()
		}

		if sourceHasSuffix && !targetHasSuffix {
			// Source had a suffix, but target won't apply one
			newExprs = append(newExprs, New(KPivotAlias, "this", e, "alias", ToIdentifier(storedFull, boolp(true))))
			modified = true
		} else if storedValue != targetValue {
			// Value-part mismatch (e.g. Snowflake's literal-style values vs others).
			newExprs = append(newExprs, New(KPivotAlias, "this", e, "alias", ToIdentifier(storedValue, boolp(true))))
			modified = true
		} else {
			newExprs = append(newExprs, e)
		}
	}

	if modified {
		return newExprs
	}
	return nil
}

// pivot_sql (generator.py L2564)
func (g *Generator) pivotSQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{flat: true})
	direction := "PIVOT"
	if expression.ArgB("unpivot") {
		direction = "UNPIVOT"
	}

	group := g.sqlKey(expression, "group")

	if expression.ArgB("this") {
		this := g.sqlKey(expression, "this")
		var sql string
		if expressions == "" {
			sql = "UNPIVOT " + this
		} else {
			on := g.seg("ON") + " " + expressions
			into := g.sqlKey(expression, "into")
			if into != "" {
				into = g.seg("INTO") + " " + into
			}
			using := g.expressions(expression, exprsOpts{key: "using", flat: true})
			if using != "" {
				using = g.seg("USING") + " " + using
			}
			sql = direction + " " + this + on + into + using + group
		}
		return g.prependCtes(expression, sql)
	}

	if !expression.ArgB("unpivot") {
		// Wrap IN-list values with explicit aliases where the target dialect would differ
		newFieldExprs := g.pivotInValueAliases(expression)
		if newFieldExprs != nil {
			expression.ArgL("fields")[0].Set("expressions", newFieldExprs)
		}
	}

	alias := g.sqlKey(expression, "alias")
	if alias != "" {
		alias = " AS " + alias
	}

	fields := g.expressions(expression, exprsOpts{
		key:       "fields",
		sep:       strp2(" "),
		dynamic:   true,
		newLine:   true,
		skipFirst: true,
		skipLast:  true,
	})

	nulls := ""
	if includeNulls := expression.Arg("include_nulls"); includeNulls != nil {
		if truthy(includeNulls) {
			nulls = " INCLUDE NULLS "
		} else {
			nulls = " EXCLUDE NULLS "
		}
	}

	defaultOnNull := g.sqlKey(expression, "default_on_null")
	if defaultOnNull != "" {
		defaultOnNull = " DEFAULT ON NULL (" + defaultOnNull + ")"
	}
	sql := g.seg(direction) + nulls + "(" + expressions + " FOR " + fields + defaultOnNull + group + ")" + alias
	return g.prependCtes(expression, sql)
}

// version_sql (generator.py L2613)
func (g *Generator) baseVersionSQL(expression *Expr) string {
	this := "FOR " + expression.Name()
	kind := expression.Text("kind")
	expr := g.sqlKey(expression, "expression")
	return this + " " + kind + " " + expr
}

// tuple_sql (generator.py L2619)
func (g *Generator) tupleSQL(expression *Expr) string {
	return "(" + g.expressions(expression, exprsOpts{dynamic: true, newLine: true, skipFirst: true, skipLast: true}) + ")"
}

// _update_from_joins_sql (generator.py L2622)
//
// Returns (join_sql, from_sql) for UPDATE statements.
// - join_sql: placed after UPDATE table, before SET
// - from_sql: placed after SET clause (standard position)
// Dialects like MySQL need to convert FROM to JOIN syntax.
func (g *Generator) updateFromJoinsSQL(expression *Expr) (string, string) {
	if g.s.UPDATE_STATEMENT_SUPPORTS_FROM || !expression.ArgB("from_") {
		return "", g.sqlKey(expression, "from_")
	}
	fromExpr := expression.ArgE("from_")

	// Qualify unqualified columns in SET clause with the target table
	// MySQL requires qualified column names in multi-table UPDATE to avoid ambiguity
	targetTable := expression.This()
	if targetTable.IsA(KTable) {
		targetName := ToIdentifier(targetTable.AliasOrName(), nil)
		for _, eq := range expression.Expressions() {
			col := eq.This()
			if col.IsA(KColumn) && col.TableName() == "" {
				col.Set("table", targetName)
			}
		}
	}

	table := fromExpr.This()
	nestedJoins := table.ArgL("joins")
	if len(nestedJoins) > 0 {
		table.Set("joins", nil)
	}

	joinSQL := g.sql(New(KJoin, "this", table, "on", Boolean(true)))
	for _, nested := range nestedJoins {
		if !nested.ArgB("on") && !nested.ArgB("using") {
			nested.Set("on", Boolean(true))
		}
		joinSQL += g.sql(nested)
	}

	return joinSQL, ""
}

// update_sql (generator.py L2654)
func (g *Generator) updateSQL(expression *Expr) string {
	hint := g.sqlKey(expression, "hint")
	this := g.sqlKey(expression, "this")
	joinSQL, fromSQL := g.updateFromJoinsSQL(expression)
	setSQL := g.expressions(expression, exprsOpts{flat: true})
	whereSQL := g.sqlKey(expression, "where")
	returning := g.sqlKey(expression, "returning")
	order := g.sqlKey(expression, "order")
	limit := g.sqlKey(expression, "limit")
	var expressionSQL string
	if g.s.RETURNING_END {
		expressionSQL = fromSQL + whereSQL + returning
	} else {
		expressionSQL = returning + fromSQL + whereSQL
	}
	options := g.expressions(expression, exprsOpts{key: "options"})
	if options != "" {
		options = " OPTION(" + options + ")"
	}
	sql := "UPDATE" + hint + " " + this + joinSQL + " SET " + setSQL + expressionSQL + order + limit + options
	return g.prependCtes(expression, sql)
}

// values_sql (generator.py L2672)
func (g *Generator) baseValuesSQL(expression *Expr, valuesAsTable bool) string {
	valuesAsTable = valuesAsTable && g.s.VALUES_AS_TABLE

	// The VALUES clause is still valid in an `INSERT INTO ..` statement, for example
	if valuesAsTable || expression.FindAncestor(KFrom, KJoin) == nil {
		args := g.expressions(expression, exprsOpts{})
		alias := g.sqlKey(expression, "alias")
		values := "VALUES" + g.seg("") + args
		if g.s.WRAP_DERIVED_VALUES && (alias != "" || expression.Parent().IsA(KFrom, KTable)) {
			values = "(" + values + ")"
		}
		values = g.queryModifiers(expression, values)
		if alias != "" {
			return values + " AS " + alias
		}
		return values
	}

	// Converts `VALUES...` expression into a series of select unions.
	aliasNode := expression.ArgE("alias")
	var columnNames []*Expr
	if aliasNode != nil {
		columnNames = aliasNode.ArgL("columns")
	}

	var selects []*Expr

	for i, tup := range expression.Expressions() {
		row := tup.Expressions()

		if i == 0 && len(columnNames) > 0 {
			n := len(row)
			if len(columnNames) < n {
				n = len(columnNames)
			}
			aliased := make([]*Expr, 0, n)
			for j := 0; j < n; j++ {
				aliased = append(aliased, gchunkBAlias(row[j], columnNames[j]))
			}
			row = aliased
		}

		selects = append(selects, New(KSelect, "expressions", row))
	}

	if g.pretty {
		// This may result in poor performance for large-cardinality `VALUES` tables, due to
		// the deep nesting of the resulting exp.Unions.
		query := selects[0]
		for _, y := range selects[1:] {
			// exp.union(x, y, distinct=False, copy=False)
			query = New(KUnion, "this", query, "expression", y, "distinct", false)
		}
		// query.subquery(alias_node and alias_node.this, copy=False)
		var subAlias *Expr
		if aliasNode != nil {
			subAlias = aliasNode.This()
		}
		return g.subquerySQL(New(KSubquery, "this", query, "alias", subAlias), " AS ")
	}

	alias := ""
	if aliasNode != nil {
		alias = " AS " + g.sqlKey(aliasNode, "this")
	}
	unions := make([]string, 0, len(selects))
	for _, sel := range selects {
		unions = append(unions, g.sql(sel))
	}
	return "(" + strings.Join(unions, " UNION ALL ") + ")" + alias
}
