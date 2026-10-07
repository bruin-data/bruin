package sqlengine

// Port of sqlglot/dialects/sqlite.py, sqlglot/parsers/sqlite.py and sqlglot/generators/sqlite.py
// (sqlglot v30.13.0). Data-only class attributes live in the generated zz_*_settings.go files.

func init() { registerCustomizer("sqlite", customizeSQLite) }

func customizeSQLite(d *Dialect) {
	// ---- SQLiteParser ----
	P := d.P

	P.FUNCTIONS["DATETIME"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KAnonymous, "this", "DATETIME", "expressions", args)
	}
	P.FUNCTIONS["EDITDIST3"] = fromArgList(KLevenshtein)
	P.FUNCTIONS["JSON_GROUP_ARRAY"] = fromArgList(KJSONArrayAgg)
	P.FUNCTIONS["JSON_GROUP_OBJECT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KJSONObjectAgg, "expressions", args)
	}
	P.FUNCTIONS["STRFTIME"] = sqliteBuildStrftime
	P.FUNCTIONS["SQLITE_VERSION"] = fromArgList(KCurrentVersion)
	P.FUNCTIONS["TIME"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KAnonymous, "this", "TIME", "expressions", args)
	}

	// "USING" is `lambda self, **kwargs: ...` (accepts any kwargs).
	P.PROPERTY_PARSERS["USING"] = func(p *Parser, _ propKwargs) any {
		return anyExpr(sqliteParseModuleProperty(p))
	}
	P.PROPERTY_PARSERS["VIRTUAL"] = noKwargsE(func(p *Parser) *Expr {
		return p.expression(New(KVirtualProperty))
	})

	P.STATEMENT_PARSERS[TK_ATTACH] = func(p *Parser) *Expr { return sqliteParseAttachDetach(p, true) }
	P.STATEMENT_PARSERS[TK_DETACH] = func(p *Parser) *Expr { return sqliteParseAttachDetach(p, false) }
	P.STATEMENT_PARSERS[TK_PRAGMA] = sqliteParsePragma

	// https://www.sqlite.org/lang_expr.html
	P.RANGE_PARSERS[TK_MATCH] = binaryRangeParser(KMatch, false)

	P.h.parseUnique = sqliteParseUnique

	// ---- SQLiteGenerator ----
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (module level)
	for _, k := range []string{"cluster", "distribute", "sort"} {
		delete(G.AFTER_HAVING_MODIFIER_TRANSFORMS, k)
	}
	keys := make([]string, 0, len(G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS))
	for _, k := range G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if k != "cluster" && k != "distribute" && k != "sort" {
			keys = append(keys, k)
		}
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	T := G.TRANSFORMS
	T[KAnyValue] = anyValueToMaxSQL
	T[KChr] = renameFunc("CHAR")
	T[KConcat] = concatToDpipeSQL
	T[KCountIf] = countIfToSum
	T[KCreate] = transformPreprocess([]func(*Expr) *Expr{sqliteTransformCreate}, nil)
	T[KCurrentDate] = func(_ *Generator, _ *Expr) string { return "CURRENT_DATE" }
	T[KCurrentTime] = func(_ *Generator, _ *Expr) string { return "CURRENT_TIME" }
	T[KCurrentTimestamp] = func(_ *Generator, _ *Expr) string { return "CURRENT_TIMESTAMP" }
	T[KCurrentVersion] = func(_ *Generator, _ *Expr) string { return "SQLITE_VERSION()" }
	T[KColumnDef] = transformPreprocess([]func(*Expr) *Expr{sqliteGeneratedToAutoIncrement}, nil)
	T[KDateStrToDate] = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }
	T[KIf] = renameFunc("IIF")
	T[KILike] = noIlikeSQL
	jsonGroupArray := renameFunc("JSON_GROUP_ARRAY")
	T[KJSONArrayAgg] = func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "order", "null_handling", "return_type", "strict")
		return jsonGroupArray(g, e)
	}
	T[KJSONExtractScalar] = arrowJSONExtractSQL
	T[KJSONObjectAgg] = func(g *Generator, e *Expr) string { return g.jsonobjectSQL(e, "JSON_GROUP_OBJECT") }
	editdist3 := renameFunc("EDITDIST3")
	T[KLevenshtein] = func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "ins_cost", "del_cost", "sub_cost", "max_dist")
		return editdist3(g, e)
	}
	T[KLogicalOr] = renameFunc("MAX")
	T[KLogicalAnd] = renameFunc("MIN")
	T[KPivot] = noPivotSQL
	T[KRand] = renameFunc("RANDOM")
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		sqliteOffsetToLimit,
		transformEliminateDistinctOn,
		transformEliminateQualify,
		transformEliminateSemiAndAntiJoins,
	}, nil)
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "INSTR", false, false, true)
	}
	T[KTableSample] = noTablesampleSQL
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }
	T[KTimeToStr] = func(g *Generator, e *Expr) string {
		return g.fn("STRFTIME", e.Arg("format"), e.Arg("this"))
	}
	T[KTryCast] = noTrycastSQL
	T[KTsOrDsToTimestamp] = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }

	// <key>_sql method overrides of base methods that are not hooks (only reached via dispatch)
	G.methods[KInsert] = sqliteInsertSQL
	G.methods[KDateAdd] = sqliteDateaddSQL
	G.methods[KWindowSpec] = sqliteWindowspecSQL

	// <key>_sql method overrides (hooks)
	G.h.castSQL = sqliteCastSQL
	G.h.transactionSQL = sqliteTransactionSQL
	G.h.ignorenullsSQL = sqliteIgnorenullsSQL
	G.h.respectnullsSQL = sqliteRespectnullsSQL

	// dialect-only <key>_sql methods
	G.methods[KBitwiseAndAgg] = sqliteBitwiseandaggSQL
	G.methods[KBitwiseOrAgg] = sqliteBitwiseoraggSQL
	G.methods[KBitwiseXorAgg] = sqliteBitwisexoraggSQL
	G.methods[KJSONExtract] = sqliteJsonextractSQL
	G.methods[KTrunc] = sqliteTruncSQL
	G.methods[KGenerateSeries] = sqliteGenerateseriesSQL
	G.methods[KDateDiff] = sqliteDatediffSQL
	G.methods[KGroupConcat] = sqliteGroupconcatSQL
	G.methods[KLeast] = sqliteLeastSQL
	G.methods[KGreatest] = sqliteGreatestSQL
	G.methods[KIsAscii] = sqliteIsasciiSQL
	G.methods[KCurrentSchema] = sqliteCurrentschemaSQL
}

// ---------------------------------------------------------------------------------------------
// Parser

// sqliteBuildStrftime mirrors parsers/sqlite.py:_build_strftime (which appends to args in place).
func sqliteBuildStrftime(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		args = append(append(make([]*Expr, 0, 2), args...), New(KCurrentTimestamp))
		return WithValidateArgs(New(KTimeToStr, "this", New(KTsOrDsToTimestamp, "this", args[1]), "format", args[0]), args)
	}
	if len(args) == 2 {
		return New(KTimeToStr, "this", New(KTsOrDsToTimestamp, "this", args[1]), "format", args[0])
	}
	return New(KAnonymous, "this", "STRFTIME", "expressions", args)
}

// sqliteParseUnique mirrors SQLiteParser._parse_unique.
func sqliteParseUnique(p *Parser) *Expr {
	// Do not consume more tokens if UNIQUE is used as a standalone constraint, e.g:
	// CREATE TABLE foo (bar TEXT UNIQUE REFERENCES baz ...)
	if _, ok := p.s.CONSTRAINT_PARSERS[upperText(p.curr)]; ok {
		return p.expression(New(KUniqueColumnConstraint))
	}

	return p.baseParseUnique()
}

// sqliteParseModuleProperty mirrors SQLiteParser._parse_module_property.
func sqliteParseModuleProperty(p *Parser) *Expr {
	name := p.parseIdVar(true, nil)
	var expressions any
	if p.matchNoAdvance(TK_L_PAREN) {
		expressions = p.parseWrappedCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA, false)
	}
	return p.expression(New(KModuleProperty, "this", name, "expressions", expressions))
}

// sqliteParsePragma mirrors SQLiteParser._parse_pragma.
func sqliteParsePragma(p *Parser) *Expr {
	name := p.parseVar(true, nil, false)

	if p.match(TK_DOT) {
		name = New(KDot, "this", name, "expression", p.parseVar(true, nil, false))
	}

	p.match(TK_EQ)

	value := p.parseWrapped(func() *Expr {
		if v := p.parseUnary(); v != nil {
			return v
		}
		return p.parseVar(true, nil, false)
	}, true)

	if value != nil {
		return p.expression(New(KPragma, "this", New(KEQ, "this", name, "expression", value)))
	}

	return p.expression(New(KPragma, "this", name))
}

// sqliteParseAttachDetach mirrors SQLiteParser._parse_attach_detach.
func sqliteParseAttachDetach(p *Parser, isAttach bool /*=True*/) *Expr {
	p.match(TK_DATABASE)
	this := p.parseExpression()

	if isAttach {
		return p.expression(New(KAttach, "this", this))
	}
	return p.expression(New(KDetach, "this", this))
}

// ---------------------------------------------------------------------------------------------
// Generator: module-level transforms

// sqliteListRemove mirrors Python list.remove(x) (first structurally equal element) on a copy.
func sqliteListRemove(list []*Expr, x *Expr) []*Expr {
	for i, y := range list {
		if y.Equal(x) {
			out := make([]*Expr, 0, len(list)-1)
			out = append(out, list[:i]...)
			return append(out, list[i+1:]...)
		}
	}
	panic(&ValueError{Msg: "list.remove(x): x not in list"})
}

// sqliteTransformCreate mirrors generators/sqlite.py:_transform_create.
// Move primary key to a column and enforce auto_increment on primary keys.
func sqliteTransformCreate(expression *Expr) *Expr {
	schema := expression.This()

	if expression.IsA(KCreate) && schema.IsA(KSchema) {
		defs := map[string]*Expr{}
		var defsOrder []string
		var primaryKey *Expr

		for _, e := range schema.Expressions() {
			if e.IsA(KColumnDef) {
				name := e.Name()
				if _, ok := defs[name]; !ok {
					defsOrder = append(defsOrder, name)
				}
				defs[name] = e
			} else if e.IsA(KPrimaryKey) {
				primaryKey = e
			}
		}

		if primaryKey != nil && len(primaryKey.Expressions()) == 1 {
			key := primaryKey.Expressions()[0].Name()
			column, ok := defs[key]
			if !ok {
				panic(&ValueError{Msg: "'" + key + "'"})
			}
			column.Append("constraints", New(KColumnConstraint, "kind", New(KPrimaryKeyColumnConstraint)))
			// schema.expressions.remove(primary_key): in-place list mutation (no parent bookkeeping)
			schema.SetArgRaw("expressions", sqliteListRemove(schema.Expressions(), primaryKey))
		}

		for _, name := range defsOrder {
			column := defs[name]
			primaryKeyIndex := -1
			var autoIncrement *Expr
			autoIncrementIndex := -1

			constraints := column.ArgL("constraints")
			for i, constraint := range constraints {
				kind := constraint.ArgE("kind")
				if kind.IsA(KPrimaryKeyColumnConstraint) {
					primaryKeyIndex = i
				} else if kind.IsA(KAutoIncrementColumnConstraint) {
					autoIncrement = constraint
					autoIncrementIndex = i
				}
			}

			if autoIncrement != nil && (primaryKeyIndex == -1 || autoIncrementIndex < primaryKeyIndex) {
				constraints = sqliteListRemove(constraints, autoIncrement)
				if primaryKeyIndex != -1 {
					// list.insert(i, x) appends when i is past the end
					idx := min(primaryKeyIndex, len(constraints))
					out := make([]*Expr, 0, len(constraints)+1)
					out = append(out, constraints[:idx]...)
					out = append(out, autoIncrement)
					constraints = append(out, constraints[idx:]...)
				}
				column.SetArgRaw("constraints", constraints)
			}
		}
	}

	return expression
}

// sqliteGeneratedToAutoIncrement mirrors generators/sqlite.py:_generated_to_auto_increment.
func sqliteGeneratedToAutoIncrement(expression *Expr) *Expr {
	if !expression.IsA(KColumnDef) {
		return expression
	}

	generated := expression.Find(KGeneratedAsIdentityColumnConstraint)

	if generated != nil {
		generated.Parent().Pop()

		notNull := expression.Find(KNotNullColumnConstraint)
		if notNull != nil {
			notNull.Parent().Pop()
		}

		expression.Append("constraints", New(KColumnConstraint, "kind", New(KAutoIncrementColumnConstraint)))
	}

	return expression
}

// sqliteOffsetToLimit mirrors generators/sqlite.py:_offset_to_limit.
func sqliteOffsetToLimit(expression *Expr) *Expr {
	if !expression.IsA(KSelect) {
		return expression
	}

	offset := expression.Arg("offset")

	if truthy(offset) && !expression.ArgB("limit") {
		// expression.limit(-1, copy=False): maybe_parse("LIMIT -1", into=exp.Limit)
		expression.QueryLimit(MaybeParse("-1", KLimit, "LIMIT", nil), false)
	}

	return expression
}

// ---------------------------------------------------------------------------------------------
// Generator: methods

// sqliteInsertSQL mirrors SQLiteGenerator.insert_sql.
func sqliteInsertSQL(g *Generator, expression *Expr) string {
	if expression.ArgB("ignore") {
		expression.Set("ignore", false)
		expression.Set("alternative", "IGNORE")
	}

	return g.insertSQL(expression)
}

// sqliteBitwiseandaggSQL mirrors SQLiteGenerator.bitwiseandagg_sql.
func sqliteBitwiseandaggSQL(g *Generator, expression *Expr) string {
	g.unsupported("BITWISE_AND aggregation is not supported in SQLite")
	return g.functionFallbackSQL(expression)
}

// sqliteBitwiseoraggSQL mirrors SQLiteGenerator.bitwiseoragg_sql.
func sqliteBitwiseoraggSQL(g *Generator, expression *Expr) string {
	g.unsupported("BITWISE_OR aggregation is not supported in SQLite")
	return g.functionFallbackSQL(expression)
}

// sqliteBitwisexoraggSQL mirrors SQLiteGenerator.bitwisexoragg_sql.
func sqliteBitwisexoraggSQL(g *Generator, expression *Expr) string {
	g.unsupported("BITWISE_XOR aggregation is not supported in SQLite")
	return g.functionFallbackSQL(expression)
}

// sqliteJsonextractSQL mirrors SQLiteGenerator.jsonextract_sql.
func sqliteJsonextractSQL(g *Generator, expression *Expr) string {
	if len(expression.Expressions()) > 0 {
		return g.functionFallbackSQL(expression)
	}
	return arrowJSONExtractSQL(g, expression)
}

// sqliteDateaddSQL mirrors SQLiteGenerator.dateadd_sql.
func sqliteDateaddSQL(g *Generator, expression *Expr) string {
	modifier := expression.Expression()
	unit := expression.ArgE("unit")
	// An INTERVAL amount carries its own unit, e.g. DATE_ADD(d, INTERVAL 1 DAY);
	// unwrap it so the unit is not left inside the quoted modifier string.
	if modifier.IsA(KInterval) {
		if unit == nil {
			unit = modifier.ArgE("unit")
		}
		modifier = modifier.This()
	}
	var modifierSQL string
	if modifier.IsString() {
		modifierSQL = modifier.Name()
	} else {
		modifierSQL = g.sql(modifier)
	}
	if unit != nil {
		modifierSQL = "'" + modifierSQL + " " + unit.Name() + "'"
	} else {
		modifierSQL = "'" + modifierSQL + "'"
	}
	return g.fn("DATE", expression.Arg("this"), modifierSQL)
}

// sqliteCastSQL mirrors SQLiteGenerator.cast_sql.
func sqliteCastSQL(g *Generator, expression *Expr, safePrefix string) string {
	if expression.IsTypeOf(DT_DATE) {
		return g.fn("DATE", expression.Arg("this"))
	}

	return g.baseCastSQL(expression, "")
}

// sqliteTruncSQL mirrors SQLiteGenerator.trunc_sql.
//
// Note: SQLite's TRUNC always returns REAL (e.g., trunc(10.99) -> 10.0), not INTEGER.
// This creates a transpilation gap affecting division semantics, similar to Presto.
func sqliteTruncSQL(g *Generator, expression *Expr) string {
	dhUnsupportedArgs(g, expression, "decimals")
	return g.fn("TRUNC", expression.Arg("this"))
}

// sqliteGenerateseriesSQL mirrors SQLiteGenerator.generateseries_sql.
func sqliteGenerateseriesSQL(g *Generator, expression *Expr) string {
	parent := expression.Parent()
	var alias *Expr
	if parent != nil {
		alias, _ = parent.Arg("alias").(*Expr)
	}

	var sql string
	if alias.IsA(KTableAlias) && len(alias.ArgL("columns")) > 0 {
		columnAlias := alias.ArgL("columns")[0]
		alias.Set("columns", nil)
		value := AliasExpr(MaybeParse("value", KNone, "", nil), columnAlias, nil, true)
		sql = g.sql(SelectExpr(value).SelectFrom(expression, true).QuerySubquery(nil, true))
	} else {
		sql = g.functionFallbackSQL(expression)
	}

	return sql
}

// sqliteDatediffSQL mirrors SQLiteGenerator.datediff_sql.
func sqliteDatediffSQL(g *Generator, expression *Expr) string {
	unitE := expression.ArgE("unit")
	unit := "DAY"
	if unitE != nil {
		unit = pyUpper(unitE.Name())
	}

	sql := "(JULIANDAY(" + g.sqlKey(expression, "this") + ") - JULIANDAY(" + g.sqlKey(expression, "expression") + "))"

	switch unit {
	case "MONTH":
		sql = sql + " / 30.0"
	case "YEAR":
		sql = sql + " / 365.0"
	case "HOUR":
		sql = sql + " * 24.0"
	case "MINUTE":
		sql = sql + " * 1440.0"
	case "SECOND":
		sql = sql + " * 86400.0"
	case "MILLISECOND":
		sql = sql + " * 86400000.0"
	case "MICROSECOND":
		sql = sql + " * 86400000000.0"
	case "NANOSECOND":
		sql = sql + " * 8640000000000.0"
	default:
		g.unsupported("DATEDIFF unsupported for '" + unit + "'.")
	}

	return "CAST(" + sql + " AS INTEGER)"
}

// sqliteGroupconcatSQL mirrors SQLiteGenerator.groupconcat_sql.
// https://www.sqlite.org/lang_aggfunc.html#group_concat
func sqliteGroupconcatSQL(g *Generator, expression *Expr) string {
	this := expression.Arg("this")
	distinct := expression.Find(KDistinct)

	var distinctSQL string
	if distinct != nil {
		exprs := distinct.Expressions()
		if len(exprs) == 0 {
			panic(&ValueError{Msg: "IndexError: list index out of range"})
		}
		this = exprs[0]
		distinctSQL = "DISTINCT "
	}

	if expression.This().IsA(KOrder) {
		g.unsupported("SQLite GROUP_CONCAT doesn't support ORDER BY.")
		if truthy(expression.This().Arg("this")) && distinct == nil {
			this = expression.This().Arg("this")
		}
	}

	separator := expression.ArgE("separator")
	return "GROUP_CONCAT(" + distinctSQL + g.formatArgs(", ", this, separator) + ")"
}

// sqliteLeastSQL mirrors SQLiteGenerator.least_sql.
func sqliteLeastSQL(g *Generator, expression *Expr) string {
	if len(expression.Expressions()) > 0 {
		return renameFunc("MIN")(g, expression)
	}

	return g.sqlKey(expression, "this")
}

// sqliteGreatestSQL mirrors SQLiteGenerator.greatest_sql.
func sqliteGreatestSQL(g *Generator, expression *Expr) string {
	if len(expression.Expressions()) > 0 {
		return renameFunc("MAX")(g, expression)
	}

	return g.sqlKey(expression, "this")
}

// sqliteTransactionSQL mirrors SQLiteGenerator.transaction_sql.
func sqliteTransactionSQL(g *Generator, expression *Expr) string {
	this := ""
	if v := expression.Arg("this"); truthy(v) {
		this = " " + gchunkCPyStr(v)
	}
	return "BEGIN" + this + " TRANSACTION"
}

// sqliteIsasciiSQL mirrors SQLiteGenerator.isascii_sql.
func sqliteIsasciiSQL(g *Generator, expression *Expr) string {
	return "(NOT " + g.sql(expression.Arg("this")) + " GLOB CAST(x'2a5b5e012d7f5d2a' AS TEXT))"
}

// sqliteCurrentschemaSQL mirrors SQLiteGenerator.currentschema_sql.
func sqliteCurrentschemaSQL(g *Generator, expression *Expr) string {
	dhUnsupportedArgs(g, expression, "this")
	return "'main'"
}

// sqliteIgnorenullsSQL mirrors SQLiteGenerator.ignorenulls_sql.
func sqliteIgnorenullsSQL(g *Generator, expression *Expr) string {
	g.unsupported("SQLite does not support IGNORE NULLS.")
	return g.sql(expression.Arg("this"))
}

// sqliteRespectnullsSQL mirrors SQLiteGenerator.respectnulls_sql.
func sqliteRespectnullsSQL(g *Generator, expression *Expr) string {
	return g.sql(expression.Arg("this"))
}

// sqliteWindowspecSQL mirrors SQLiteGenerator.windowspec_sql.
func sqliteWindowspecSQL(g *Generator, expression *Expr) string {
	if pyUpper(expression.Text("kind")) == "RANGE" && pyUpper(expression.Text("start")) == "CURRENT ROW" {
		return "RANGE CURRENT ROW"
	}

	return g.windowspecSQL(expression)
}
