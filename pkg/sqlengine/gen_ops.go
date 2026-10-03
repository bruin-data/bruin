package sqlengine

import (
	"fmt"
	"strings"
)

// Generator chunk D (part 1): sqlglot/generator.py currentdate_sql .. format_time.

// currentdate_sql (generator.py L4110)
func (g *Generator) baseCurrentdateSQL(expression *Expr) string {
	zone := g.sqlKey(expression, "this")
	if zone != "" {
		return "CURRENT_DATE(" + zone + ")"
	}
	return "CURRENT_DATE"
}

// collate_sql (generator.py L4114)
func (g *Generator) baseCollateSQL(expression *Expr) string {
	if g.s.COLLATE_IS_FUNC {
		return g.functionFallbackSQL(expression)
	}
	return g.binary(expression, "COLLATE")
}

// command_sql (generator.py L4119)
func (g *Generator) commandSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + " " + pyStrip(expression.Text("expression"))
}

// comment_sql (generator.py L4122)
func (g *Generator) commentSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	kind := expression.Arg("kind")
	materialized := ""
	if expression.ArgB("materialized") {
		materialized = " MATERIALIZED"
	}
	existsSQL := " "
	if expression.ArgB("exists") {
		existsSQL = " IF EXISTS "
	}
	expressionSQL := g.sqlKey(expression, "expression")
	return "COMMENT" + existsSQL + "ON" + materialized + " " + genDPyStr(kind) + " " + this + " IS " + expressionSQL
}

// mergetreettlaction_sql (generator.py L4130)
func (g *Generator) mergetreettlactionSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	delete := ""
	if expression.ArgB("delete") {
		delete = " DELETE"
	}
	recompress := g.sqlKey(expression, "recompress")
	if recompress != "" {
		recompress = " RECOMPRESS " + recompress
	}
	toDisk := g.sqlKey(expression, "to_disk")
	if toDisk != "" {
		toDisk = " TO DISK " + toDisk
	}
	toVolume := g.sqlKey(expression, "to_volume")
	if toVolume != "" {
		toVolume = " TO VOLUME " + toVolume
	}
	return this + delete + recompress + toDisk + toVolume
}

// mergetreettl_sql (generator.py L4141)
func (g *Generator) mergetreettlSQL(expression *Expr) string {
	where := g.sqlKey(expression, "where")
	group := g.sqlKey(expression, "group")
	aggregates := g.expressions(expression, exprsOpts{key: "aggregates"})
	if aggregates != "" {
		aggregates = g.seg("SET") + g.seg(aggregates)
	}

	if !(where != "" || group != "" || aggregates != "") && len(expression.Expressions()) == 1 {
		return "TTL " + g.expressions(expression, exprsOpts{flat: true})
	}

	return "TTL" + g.seg(g.expressions(expression, exprsOpts{})) + where + group + aggregates
}

// transaction_sql (generator.py L4152)
func (g *Generator) baseTransactionSQL(expression *Expr) string {
	// "modes" is a list of strings (see Parser._parse_transaction).
	modes := g.expressions(nil, exprsOpts{sqls: genDArgSqls(expression.Arg("modes")), hasSqls: true})
	if modes != "" {
		modes = " " + modes
	}
	return "BEGIN" + modes
}

// commit_sql (generator.py L4157)
func (g *Generator) baseCommitSQL(expression *Expr) string {
	chain := expression.Arg("chain")
	chainSQL := ""
	if chain != nil {
		if truthy(chain) {
			chainSQL = " AND CHAIN"
		} else {
			chainSQL = " AND NO CHAIN"
		}
	}

	return "COMMIT" + chainSQL
}

// rollback_sql (generator.py L4164)
func (g *Generator) baseRollbackSQL(expression *Expr) string {
	savepoint := expression.Arg("savepoint")
	savepointSQL := ""
	if truthy(savepoint) {
		savepointSQL = " TO " + genDPyStr(savepoint)
	}
	return "ROLLBACK" + savepointSQL
}

// altercolumn_sql (generator.py L4169)
func (g *Generator) baseAltercolumnSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")

	dtype := g.sqlKey(expression, "dtype")
	if dtype != "" {
		collate := g.sqlKey(expression, "collate")
		if collate != "" {
			collate = " COLLATE " + collate
		}
		using := g.sqlKey(expression, "using")
		if using != "" {
			using = " USING " + using
		}
		alterSetType := ""
		if g.s.ALTER_SET_TYPE != "" {
			alterSetType = g.s.ALTER_SET_TYPE + " "
		}
		return "ALTER COLUMN " + this + " " + alterSetType + dtype + collate + using
	}

	def := g.sqlKey(expression, "default")
	if def != "" {
		return "ALTER COLUMN " + this + " SET DEFAULT " + def
	}

	comment := g.sqlKey(expression, "comment")
	if comment != "" {
		return "ALTER COLUMN " + this + " COMMENT " + comment
	}

	visible := expression.Arg("visible")
	if truthy(visible) {
		return "ALTER COLUMN " + this + " SET " + genDPyStr(visible)
	}

	allowNull := expression.Arg("allow_null")
	drop := expression.Arg("drop")

	if !truthy(drop) && !truthy(allowNull) {
		g.unsupported("Unsupported ALTER COLUMN syntax")
	}

	if allowNull != nil {
		keyword := "SET"
		if truthy(drop) {
			keyword = "DROP"
		}
		return "ALTER COLUMN " + this + " " + keyword + " NOT NULL"
	}

	return "ALTER COLUMN " + this + " DROP DEFAULT"
}

// modifycolumn_sql (generator.py L4205)
func (g *Generator) modifycolumnSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	renameFrom := g.sqlKey(expression, "rename_from")
	if renameFrom != "" {
		if !g.s.SUPPORTS_CHANGE_COLUMN {
			g.unsupported("CHANGE COLUMN is not supported in this dialect")
		}
		return "CHANGE COLUMN " + renameFrom + " " + this
	}
	if !g.s.SUPPORTS_MODIFY_COLUMN {
		g.unsupported("MODIFY COLUMN is not supported in this dialect")
	}
	return "MODIFY COLUMN " + this
}

// alterindex_sql (generator.py L4216)
func (g *Generator) alterindexSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")

	visibleSQL := "INVISIBLE"
	if expression.ArgB("visible") {
		visibleSQL = "VISIBLE"
	}

	return "ALTER INDEX " + this + " " + visibleSQL
}

// alterdiststyle_sql (generator.py L4224)
func (g *Generator) alterdiststyleSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if !expression.This().IsA(KVar) {
		this = "KEY DISTKEY " + this
	}
	return "ALTER DISTSTYLE " + this
}

// altersortkey_sql (generator.py L4230)
func (g *Generator) altersortkeySQL(expression *Expr) string {
	compound := ""
	if expression.ArgB("compound") {
		compound = " COMPOUND"
	}
	this := g.sqlKey(expression, "this")
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = "(" + expressions + ")"
	}
	if this == "" {
		this = expressions
	}
	return "ALTER" + compound + " SORTKEY " + this
}

// alterrename_sql (generator.py L4237)
func (g *Generator) baseAlterrenameSQL(expression *Expr, includeTo bool) string {
	if !g.s.RENAME_TABLE_WITH_DB {
		// Remove db from tables
		expression = expression.Transform(func(n *Expr) *Expr {
			if n.IsA(KTable) {
				return genDTable(n.This())
			}
			return n
		}, true)
	}
	this := g.sqlKey(expression, "this")
	toKw := ""
	if includeTo {
		toKw = " TO"
	}
	return "RENAME" + toKw + " " + this
}

// renamecolumn_sql (generator.py L4247)
func (g *Generator) baseRenamecolumnSQL(expression *Expr) string {
	exists := ""
	if expression.ArgB("exists") {
		exists = " IF EXISTS"
	}
	oldColumn := g.sqlKey(expression, "this")
	newColumn := g.sqlKey(expression, "to")
	return "RENAME COLUMN" + exists + " " + oldColumn + " TO " + newColumn
}

// alterset_sql (generator.py L4253)
func (g *Generator) baseAltersetSQL(expression *Expr) string {
	exprs := g.expressions(expression, exprsOpts{flat: true})
	if g.s.ALTER_SET_WRAPPED {
		exprs = "(" + exprs + ")"
	}

	return "SET " + exprs
}

// alter_sql (generator.py L4260)
func (g *Generator) baseAlterSQL(expression *Expr) string {
	actions := expression.ArgL("actions")

	var actionsSQL string
	if !g.d.S.ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN && actions[0].IsA(KColumnDef) {
		actionsSQL = g.expressions(expression, exprsOpts{key: "actions", flat: true})
		actionsSQL = "ADD " + actionsSQL
	} else {
		var actionsList []any
		for _, action := range actions {
			var actionSQL string
			if action.IsA(KColumnDef, KSchema) {
				actionSQL = g.addColumnSQL(action)
			} else {
				actionSQL = g.sql(action)
				if action.IsA(KQuery) {
					actionSQL = "AS " + actionSQL
				}
			}

			actionsList = append(actionsList, actionSQL)
		}

		actionsSQL = strings.TrimLeft(g.formatArgs(", ", actionsList...), "\n")
	}

	iceberg := ""
	if expression.ArgB("iceberg") && g.s.SUPPORTS_DROP_ALTER_ICEBERG_PROPERTY {
		iceberg = "ICEBERG "
	}
	exists := ""
	if expression.ArgB("exists") {
		exists = " IF EXISTS"
	}
	onCluster := g.sqlKey(expression, "cluster")
	if onCluster != "" {
		onCluster = " " + onCluster
	}
	only := ""
	if expression.ArgB("only") {
		only = " ONLY"
	}
	options := g.expressions(expression, exprsOpts{key: "options"})
	if options != "" {
		options = ", " + options
	}
	kind := g.sqlKey(expression, "kind")
	notValid := ""
	if expression.ArgB("not_valid") {
		notValid = " NOT VALID"
	}
	check := ""
	if expression.ArgB("check") {
		check = " WITH CHECK"
	}
	cascade := ""
	if expression.ArgB("cascade") && g.d.S.ALTER_TABLE_SUPPORTS_CASCADE {
		cascade = " CASCADE"
	}
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}

	return "ALTER " + iceberg + kind + exists + only + this + onCluster + check + g.sep(" ") + actionsSQL + notValid + options + cascade
}

// altersession_sql (generator.py L4306)
func (g *Generator) altersessionSQL(expression *Expr) string {
	itemsSQL := g.expressions(expression, exprsOpts{flat: true})
	keyword := "SET"
	if expression.ArgB("unset") {
		keyword = "UNSET"
	}
	return keyword + " " + itemsSQL
}

// add_column_sql (generator.py L4311)
func (g *Generator) baseAddColumnSQL(expression *Expr) string {
	sql := g.sql(expression)
	var columnText string
	if expression.IsA(KSchema) {
		columnText = " COLUMNS"
	} else if expression.IsA(KColumnDef) && g.s.ALTER_TABLE_INCLUDE_COLUMN_KEYWORD {
		columnText = " COLUMN"
	} else {
		columnText = ""
	}

	return "ADD" + columnText + " " + sql
}

// droppartition_sql (generator.py L4322)
func (g *Generator) droppartitionSQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{})
	exists := " "
	if expression.ArgB("exists") {
		exists = " IF EXISTS "
	}
	return "DROP" + exists + expressions
}

// dropprimarykey_sql (generator.py L4327)
func (g *Generator) dropprimarykeySQL(expression *Expr) string {
	return "DROP PRIMARY KEY"
}

// addconstraint_sql (generator.py L4330)
func (g *Generator) addconstraintSQL(expression *Expr) string {
	return "ADD " + g.expressions(expression, exprsOpts{noIndent: true})
}

// addpartition_sql (generator.py L4333)
func (g *Generator) addpartitionSQL(expression *Expr) string {
	exists := ""
	if expression.ArgB("exists") {
		exists = "IF NOT EXISTS "
	}
	location := g.sqlKey(expression, "location")
	if location != "" {
		location = " " + location
	}
	return "ADD " + exists + g.sql(expression.Arg("this")) + location
}

// distinct_sql (generator.py L4339)
func (g *Generator) distinctSQL(expression *Expr) string {
	this := g.expressions(expression, exprsOpts{flat: true})

	if !g.s.MULTI_ARG_DISTINCT && len(expression.Expressions()) > 1 {
		caseExpr := New(KCase, "this", nil, "ifs", []*Expr{})
		for _, arg := range expression.Expressions() {
			caseExpr = genDCaseWhen(caseExpr, genDBinop(KIs, arg, Null()), Null(), true)
		}
		// case.else_(str) parses the string with the default dialect.
		this = g.sql(genDCaseElse(caseExpr, "("+this+")", true))
	}

	if this != "" {
		this = " " + this
	}

	on := g.sqlKey(expression, "on")
	if on != "" {
		on = " ON " + on
	}
	return "DISTINCT" + this + on
}

// ignorenulls_sql (generator.py L4354)
func (g *Generator) baseIgnorenullsSQL(expression *Expr) string {
	return g.embedIgnoreNulls(expression, "IGNORE NULLS")
}

// respectnulls_sql (generator.py L4357)
func (g *Generator) baseRespectnullsSQL(expression *Expr) string {
	return g.embedIgnoreNulls(expression, "RESPECT NULLS")
}

// havingmax_sql (generator.py L4360)
func (g *Generator) havingmaxSQL(expression *Expr) string {
	thisSQL := g.sqlKey(expression, "this")
	expressionSQL := g.sqlKey(expression, "expression")
	kind := "MIN"
	if expression.ArgB("max") {
		kind = "MAX"
	}
	return thisSQL + " HAVING " + kind + " " + expressionSQL
}

// intdiv_sql (generator.py L4366)
func (g *Generator) intdivSQL(expression *Expr) string {
	return g.sql(
		New(
			KCast,
			"this", New(KDiv, "this", expression.This(), "expression", expression.Expression()),
			"to", New(KDataType, "this", DT_INT),
		),
	)
}

// dpipe_sql (generator.py L4374)
func (g *Generator) baseDpipeSQL(expression *Expr) string {
	if g.d.S.STRICT_STRING_CONCAT && expression.ArgB("safe") {
		var args []any
		for _, e := range expression.Flatten(true) {
			args = append(args, genDCast(e, DT_TEXT, nil))
		}
		return g.fn("CONCAT", args...)
	}
	return g.binary(expression, "||")
}

// div_sql (generator.py L4379)
func (g *Generator) divSQL(expression *Expr) string {
	l, r := expression.Left(), expression.Right()

	if !g.d.S.SAFE_DIVISION && expression.ArgB("safe") {
		r.Replace(New(KNullif, "this", r.Copy(), "expression", LiteralInt(0)))
	}

	if g.d.S.TYPED_DIVISION && !expression.ArgB("typed") {
		if !genDIsTypeSet(l, DataType_REAL_TYPES) && !genDIsTypeSet(r, DataType_REAL_TYPES) {
			l.Replace(genDCast(l.Copy(), DT_DOUBLE, nil))
		}
	} else if !g.d.S.TYPED_DIVISION && expression.ArgB("typed") {
		if genDIsTypeSet(l, DataType_INTEGER_TYPES) && genDIsTypeSet(r, DataType_INTEGER_TYPES) {
			return g.sql(genDCast(genDBinop(KDiv, l, r), DT_BIGINT, nil))
		}
	}

	return g.binary(expression, "/")
}

// safedivide_sql (generator.py L4400)
func (g *Generator) safedivideSQL(expression *Expr) string {
	n := genDWrap(expression.This(), KBinary)
	d := genDWrap(expression.Expression(), KBinary)
	return g.sql(New(
		KIf,
		"this", genDBinop(KNEQ, d, LiteralInt(0)),
		"true", genDBinop(KDiv, n, d),
		"false", New(KNull),
	))
}

// overlaps_sql (generator.py L4405)
func (g *Generator) overlapsSQL(expression *Expr) string {
	return g.binary(expression, "OVERLAPS")
}

// distance_sql (generator.py L4408)
func (g *Generator) distanceSQL(expression *Expr) string {
	return g.binary(expression, "<->")
}

// distancend_sql (generator.py L4411)
func (g *Generator) distancendSQL(expression *Expr) string {
	return g.binary(expression, "<<->>")
}

// dot_sql (generator.py L4414)
func (g *Generator) baseDotSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + "." + g.sqlKey(expression, "expression")
}

// eq_sql (generator.py L4417)
func (g *Generator) baseEqSQL(expression *Expr) string {
	return g.binary(expression, "=")
}

// propertyeq_sql (generator.py L4420)
func (g *Generator) propertyeqSQL(expression *Expr) string {
	return g.binary(expression, ":=")
}

// escape_sql (generator.py L4423)
func (g *Generator) escapeSQL(expression *Expr) string {
	this := expression.This()
	if this.IsA(KLike, KILike) &&
		this.Expression().IsA(KAll, KAny) &&
		!g.s.SUPPORTS_LIKE_QUANTIFIERS {
		return g.likeSQL(this, expression)
	}
	return g.binary(expression, "ESCAPE")
}

// glob_sql (generator.py L4433)
func (g *Generator) globSQL(expression *Expr) string {
	return g.binary(expression, "GLOB")
}

// gt_sql (generator.py L4436)
func (g *Generator) gtSQL(expression *Expr) string {
	return g.binary(expression, ">")
}

// gte_sql (generator.py L4439)
func (g *Generator) gteSQL(expression *Expr) string {
	return g.binary(expression, ">=")
}

// is_sql (generator.py L4442)
func (g *Generator) baseIsSQL(expression *Expr) string {
	if !g.s.IS_BOOL_ALLOWED && expression.Expression().IsA(KBoolean) {
		if expression.Expression().ArgB("this") {
			return g.sql(expression.Arg("this"))
		}
		return g.sql(genDNot(expression.This(), true))
	}
	return g.binary(expression, "IS")
}

// _like_sql (generator.py L4449)
func (g *Generator) likeSQL(expression *Expr, escape *Expr) string {
	this := expression.This()
	rhs := expression.Expression()

	var expClass Kind
	var op string
	if expression.IsA(KLike) {
		expClass = KLike
		op = "LIKE"
	} else {
		expClass = KILike
		op = "ILIKE"
	}

	if expression.ArgB("negate") {
		op = "NOT " + op
	}

	if rhs.IsA(KAll, KAny) && !g.s.SUPPORTS_LIKE_QUANTIFIERS {
		unnested := rhs.This().Unnest()

		var exprs []*Expr
		if unnested.IsA(KTuple) {
			exprs = unnested.Expressions()
		} else {
			exprs = []*Expr{unnested}
		}

		connective := KAnd
		if rhs.IsA(KAny) {
			connective = KOr
		}

		makeLike := func(expr *Expr) *Expr {
			like := New(expClass, "this", this, "expression", expr, "negate", expression.Arg("negate"))
			if escape != nil {
				like = New(KEscape, "this", like, "expression", escape.Expression().Copy())
			}
			return like
		}

		likeExpr := makeLike(exprs[0])
		for _, expr := range exprs[1:] {
			likeExpr = genDCombine([]*Expr{likeExpr, makeLike(expr)}, connective, false, true)
		}

		var parent *Expr
		if escape != nil {
			parent = escape.Parent()
		} else {
			parent = expression.Parent()
		}
		if !parent.IsA(likeExpr.Kind(), KParen) && parent.IsA(KCondition) {
			likeExpr = New(KParen, "this", likeExpr)
		}

		return g.sql(likeExpr)
	}

	return g.binary(expression, op)
}

// like_sql (generator.py L4499)
func (g *Generator) likeSQL_(expression *Expr) string {
	return g.likeSQL(expression, nil)
}

// ilike_sql (generator.py L4502)
func (g *Generator) ilikeSQL(expression *Expr) string {
	return g.likeSQL(expression, nil)
}

// match_sql (generator.py L4505)
func (g *Generator) matchSQL(expression *Expr) string {
	return g.binary(expression, "MATCH")
}

// similarto_sql (generator.py L4508)
func (g *Generator) similartoSQL(expression *Expr) string {
	return g.binary(expression, "SIMILAR TO")
}

// lt_sql (generator.py L4511)
func (g *Generator) ltSQL(expression *Expr) string {
	return g.binary(expression, "<")
}

// lte_sql (generator.py L4514)
func (g *Generator) lteSQL(expression *Expr) string {
	return g.binary(expression, "<=")
}

// mod_sql (generator.py L4517)
func (g *Generator) baseModSQL(expression *Expr) string {
	return g.binary(expression, "%")
}

// mul_sql (generator.py L4520)
func (g *Generator) mulSQL(expression *Expr) string {
	return g.binary(expression, "*")
}

// neq_sql (generator.py L4523)
func (g *Generator) baseNeqSQL(expression *Expr) string {
	return g.binary(expression, "<>")
}

// nullsafeeq_sql (generator.py L4526)
func (g *Generator) nullsafeeqSQL(expression *Expr) string {
	return g.binary(expression, "IS NOT DISTINCT FROM")
}

// nullsafeneq_sql (generator.py L4529)
func (g *Generator) nullsafeneqSQL(expression *Expr) string {
	return g.binary(expression, "IS DISTINCT FROM")
}

// sub_sql (generator.py L4532)
func (g *Generator) subSQL(expression *Expr) string {
	return g.binary(expression, "-")
}

// trycast_sql (generator.py L4535)
func (g *Generator) baseTrycastSQL(expression *Expr) string {
	return g.castSQL(expression, "TRY_")
}

// jsoncast_sql (generator.py L4538)
func (g *Generator) jsoncastSQL(expression *Expr) string {
	return g.castSQL(expression, "")
}

// try_sql (generator.py L4541)
func (g *Generator) trySQL(expression *Expr) string {
	if !g.s.TRY_SUPPORTED {
		g.unsupported("Unsupported TRY function")
		return g.sqlKey(expression, "this")
	}

	return g.fn("TRY", expression.Arg("this"))
}

// log_sql (generator.py L4548)
func (g *Generator) baseLogSQL(expression *Expr) string {
	this := expression.This()
	expr := expression.Expression()

	if g.d.S.LOG_BASE_FIRST == TriFalse {
		this, expr = expr, this
	} else if g.d.S.LOG_BASE_FIRST == TriNone && expr != nil {
		if name := this.Name(); name == "2" || name == "10" {
			return g.fn("LOG"+name, expr)
		}

		g.unsupported("Unsupported logarithm with base " + g.sql(this))
	}

	return g.fn("LOG", this, expr)
}

// use_sql (generator.py L4562)
func (g *Generator) useSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = " " + kind
	}
	this := g.sqlKey(expression, "this")
	if this == "" {
		this = g.expressions(expression, exprsOpts{flat: true})
	}
	if this != "" {
		this = " " + this
	}
	return "USE" + kind + this
}

// ceil_floor (generator.py L4590)
func (g *Generator) ceilFloor(expression *Expr) string {
	toClause := g.sqlKey(expression, "to")
	if toClause != "" {
		return expression.Kind().SQLName() + "(" + g.sqlKey(expression, "this") + " TO " + toClause + ")"
	}

	return g.functionFallbackSQL(expression)
}

// format_time (generator.py L4640). Returns "" for None.
func (g *Generator) baseFormatTime(expression *Expr, inverseTimeMapping map[string]string, inverseTimeTrie *trie) string {
	mapping := inverseTimeMapping
	if len(mapping) == 0 {
		mapping = g.d.S.INVERSE_TIME_MAPPING
	}
	tr := inverseTimeTrie
	if genDTrieEmpty(tr) {
		tr = g.d.inverseTimeTrie
	}
	if genDTrieEmpty(tr) {
		// time.format_time: trie = trie or new_trie(mapping)
		tr = nil
	}
	s, _ := formatTime(g.sqlKey(expression, "format"), mapping, tr)
	return s
}

// genDUnsupportedArgs mirrors the @unsupported_args(...) decorator for plain argument names.
func (g *Generator) genDUnsupportedArgs(expression *Expr, args ...string) {
	for _, arg := range args {
		if expression.ArgB(arg) {
			g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", arg, expression.Kind().Name(), g.d.ClassName))
		}
	}
}
