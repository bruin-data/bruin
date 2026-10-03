package sqlengine

import (
	"slices"
	"strings"
)

// Generator chunk C (part 1): sqlglot/generator.py select_sql .. concatws_sql.

// select_sql (generator.py L3302)
func (g *Generator) baseSelectSQL(expression *Expr) string {
	into := expression.ArgE("into")
	if !g.s.SUPPORTS_SELECT_INTO && into != nil {
		into.Pop()
	}

	hint := g.sqlKey(expression, "hint")
	distinct := g.sqlKey(expression, "distinct")
	if distinct != "" {
		distinct = " " + distinct
	}
	kind := g.sqlKey(expression, "kind")

	limit := expression.ArgE("limit")
	top := ""
	if limit.IsA(KLimit) && g.s.LIMIT_IS_TOP {
		top = g.limitSQL(limit, true)
		limit.Pop()
	}

	expressions := g.expressions(expression, exprsOpts{})

	if kind != "" {
		if slices.Contains(g.s.SELECT_KINDS, kind) {
			kind = " AS " + kind
		} else {
			if kind == "STRUCT" {
				items := make([]*Expr, 0, len(expression.Expressions()))
				for _, e := range expression.Expressions() {
					if e.IsA(KAlias) {
						items = append(items, New(KPropertyEQ, "this", e.Arg("alias"), "expression", e.This()))
					} else {
						items = append(items, e)
					}
				}
				expressions = g.expressions(nil, exprsOpts{
					sqls:    []any{g.sql(New(KStruct, "expressions", items))},
					hasSqls: true,
				})
			}
			kind = ""
		}
	}

	operationModifiers := g.expressions(expression, exprsOpts{key: "operation_modifiers", sep: strp2(" ")})
	if operationModifiers != "" {
		operationModifiers = g.sep1() + operationModifiers
	}

	exclude := expression.ArgL("exclude")

	if !g.s.STAR_EXCLUDE_REQUIRES_DERIVED_TABLE && len(exclude) > 0 {
		excludeSQL := g.expressions(nil, exprsOpts{sqls: gchunkCExprsToAny(exclude), hasSqls: true, flat: true})
		expressions = expressions + g.seg("EXCLUDE") + " (" + excludeSQL + ")"
	}

	// We use LIMIT_IS_TOP as a proxy for whether DISTINCT should go first because tsql and Teradata
	// are the only dialects that use LIMIT_IS_TOP and both place DISTINCT first.
	var topDistinct string
	if g.s.LIMIT_IS_TOP {
		topDistinct = distinct + hint + top
	} else {
		topDistinct = top + hint + distinct
	}
	if expressions != "" {
		expressions = g.sep1() + expressions
	}
	sql := g.queryModifiers(
		expression,
		"SELECT"+topDistinct+operationModifiers+kind+expressions,
		g.sqlKey(expression, "into"),
		g.sqlKey(expression, "from_"),
	)

	// If both the CTE and SELECT clauses have comments, generate the latter earlier
	if expression.ArgB("with_") {
		sql = g.maybeComment(sql, expression)
		expression.PopComments()
	}

	sql = g.prependCtes(expression, sql)

	if g.s.STAR_EXCLUDE_REQUIRES_DERIVED_TABLE && len(exclude) > 0 {
		expression.Set("exclude", nil)
		// expression.subquery(copy=False)
		subquery := New(KSubquery, "this", expression, "alias", nil)
		star := New(KStar, "except_", exclude)
		// exp.select(star).from_(subquery, copy=False)
		sel := New(KSelect)
		sel.Set("expressions", []*Expr{star})
		sel.Set("from_", New(KFrom, "this", subquery))
		sql = g.sql(sel)
	}

	if !g.s.SUPPORTS_SELECT_INTO && into != nil {
		tableKind := ""
		if into.ArgB("temporary") {
			tableKind = " TEMPORARY"
		} else if g.s.SUPPORTS_UNLOGGED_TABLES && into.ArgB("unlogged") {
			tableKind = " UNLOGGED"
		}
		sql = "CREATE" + tableKind + " TABLE " + g.sql(into.Arg("this")) + " AS " + sql
	}

	return sql
}

// schema_sql (generator.py L3386)
func (g *Generator) baseSchemaSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	sql := g.schemaColumnsSQL(expression)
	if this != "" && sql != "" {
		return this + " " + sql
	}
	if this != "" {
		return this
	}
	return sql
}

// schema_columns_sql (generator.py L3391)
func (g *Generator) schemaColumnsSQL(expression *Expr) string {
	if len(expression.Expressions()) > 0 {
		return "(" + g.sep("") + g.expressions(expression, exprsOpts{}) + g.segSep(")", "")
	}
	return ""
}

// star_sql (generator.py L3396)
func (g *Generator) starSQL(expression *Expr) string {
	except := g.expressions(expression, exprsOpts{key: "except_", flat: true})
	if except != "" {
		except = g.seg(g.s.STAR_EXCEPT) + " (" + except + ")"
	}
	replace := g.expressions(expression, exprsOpts{key: "replace", flat: true})
	if replace != "" {
		replace = g.seg("REPLACE") + " (" + replace + ")"
	}
	rename := g.expressions(expression, exprsOpts{key: "rename", flat: true})
	if rename != "" {
		rename = g.seg("RENAME") + " (" + rename + ")"
	}
	ilike := g.sqlKey(expression, "ilike")
	if ilike != "" {
		ilike = g.seg("ILIKE") + " " + ilike
	}
	return "*" + ilike + except + replace + rename
}

// parameter_sql (generator.py L3407)
func (g *Generator) baseParameterSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	return g.s.PARAMETER_TOKEN + this
}

// sessionparameter_sql (generator.py L3411)
func (g *Generator) sessionparameterSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	kind := expression.Text("kind")
	if kind != "" {
		kind = kind + "."
	}
	return "@@" + kind + this
}

// placeholder_sql (generator.py L3418)
func (g *Generator) basePlaceholderSQL(expression *Expr) string {
	if expression.ArgB("this") {
		return g.s.NAMED_PLACEHOLDER_TOKEN + expression.Name()
	}
	return "?"
}

// subquery_sql (generator.py L3421)
func (g *Generator) subquerySQL(expression *Expr, sep string) string {
	alias := g.sqlKey(expression, "alias")
	if alias != "" {
		alias = sep + alias
	}
	sample := g.sqlKey(expression, "sample")
	if g.d.S.ALIAS_POST_TABLESAMPLE && sample != "" {
		alias = sample + alias

		// Set to None so it's not generated again by self.query_modifiers()
		expression.Set("sample", nil)
	}

	pivots := g.expressions(expression, exprsOpts{key: "pivots", sep: strp2(""), flat: true})
	sql := g.queryModifiers(expression, g.wrap(expression), alias, pivots)
	return g.prependCtes(expression, sql)
}

// qualify_sql (generator.py L3435)
func (g *Generator) qualifySQL(expression *Expr) string {
	this := g.indentDefault(g.sqlKey(expression, "this"))
	return g.seg("QUALIFY") + g.sep1() + this
}

// unnest_sql (generator.py L3439)
func (g *Generator) baseUnnestSQL(expression *Expr) string {
	args := g.expressions(expression, exprsOpts{flat: true})

	alias := expression.ArgE("alias")
	offset := expression.Arg("offset")
	offsetExpr, offsetIsExpr := offset.(*Expr)
	offsetIsExpr = offsetIsExpr && offsetExpr != nil

	if g.s.UNNEST_WITH_ORDINALITY {
		if alias != nil && offsetIsExpr {
			alias.Append("columns", offsetExpr)
			expression.Set("offset", nil)
		}
	}

	var aliasSQL string
	if alias != nil && g.d.S.UNNEST_COLUMN_ONLY {
		columns := alias.TableAliasColumns()
		if len(columns) > 0 {
			aliasSQL = g.sql(columns[0])
		}
	} else {
		aliasSQL = g.sql(alias)
	}

	if aliasSQL != "" {
		aliasSQL = " AS " + aliasSQL
	}
	var suffix string
	if g.s.UNNEST_WITH_ORDINALITY {
		if truthy(offset) {
			suffix = " WITH ORDINALITY" + aliasSQL
		} else {
			suffix = aliasSQL
		}
	} else {
		if offsetIsExpr {
			suffix = aliasSQL + " WITH OFFSET AS " + g.sql(offsetExpr)
		} else if truthy(offset) {
			suffix = aliasSQL + " WITH OFFSET"
		} else {
			suffix = aliasSQL
		}
	}

	return "UNNEST(" + args + ")" + suffix
}

// prewhere_sql (generator.py L3469)
func (g *Generator) basePrewhereSQL(expression *Expr) string {
	return ""
}

// where_sql (generator.py L3472)
func (g *Generator) whereSQL(expression *Expr) string {
	this := g.indentDefault(g.sqlKey(expression, "this"))
	return g.seg("WHERE") + g.sep1() + this
}

// window_sql (generator.py L3476)
func (g *Generator) baseWindowSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	partition := g.partitionBySQL(expression)
	order := ""
	if o := expression.ArgE("order"); o != nil {
		order = g.orderSQL(o, true)
	}
	spec := g.sqlKey(expression, "spec")
	alias := g.sqlKey(expression, "alias")
	over := g.sqlKey(expression, "over")
	if over == "" {
		over = "OVER"
	}

	if expression.ArgKey() == "windows" {
		this = this + " AS"
	} else {
		this = this + " " + over
	}

	first := ""
	if f := expression.Arg("first"); f != nil {
		if truthy(f) {
			first = "FIRST"
		} else {
			first = "LAST"
		}
	}

	if partition == "" && order == "" && spec == "" && alias != "" {
		return this + " " + alias
	}

	var args []any
	for _, a := range []string{alias, first, partition, order, spec} {
		if a != "" {
			args = append(args, a)
		}
	}
	return this + " (" + g.formatArgs(" ", args...) + ")"
}

// partition_by_sql (generator.py L3501)
func (g *Generator) partitionBySQL(expression *Expr) string {
	partition := g.expressions(expression, exprsOpts{key: "partition_by", flat: true})
	if partition != "" {
		return "PARTITION BY " + partition
	}
	return ""
}

// windowspec_sql (generator.py L3505)
func (g *Generator) windowspecSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	start := gchunkCCsv(" ", g.sqlKey(expression, "start"), g.sqlKey(expression, "start_side"))
	end := gchunkCCsv(" ", g.sqlKey(expression, "end"), g.sqlKey(expression, "end_side"))
	if end == "" {
		end = "CURRENT ROW"
	}

	windowSpec := kind + " BETWEEN " + start + " AND " + end

	exclude := g.sqlKey(expression, "exclude")
	if exclude != "" {
		if g.s.SUPPORTS_WINDOW_EXCLUDE {
			windowSpec += " EXCLUDE " + exclude
		} else {
			g.unsupported("EXCLUDE clause is not supported in the WINDOW clause")
		}
	}

	return windowSpec
}

// withingroup_sql (generator.py L3524)
func (g *Generator) baseWithingroupSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressionSQL := g.sqlKey(expression, "expression")
	// order has a leading space
	if r := []rune(expressionSQL); len(r) > 0 {
		expressionSQL = string(r[1:])
	}
	return this + " WITHIN GROUP (" + expressionSQL + ")"
}

// between_sql (generator.py L3529)
func (g *Generator) betweenSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	low := g.sqlKey(expression, "low")
	high := g.sqlKey(expression, "high")
	symmetric := expression.Arg("symmetric")

	if truthy(symmetric) && !g.s.SUPPORTS_BETWEEN_FLAGS {
		return "(" + this + " BETWEEN " + low + " AND " + high + " OR " + this + " BETWEEN " + high + " AND " + low + ")"
	}

	flag := ""
	if truthy(symmetric) {
		flag = " SYMMETRIC"
	} else if b, ok := symmetric.(bool); ok && !b && g.s.SUPPORTS_BETWEEN_FLAGS {
		flag = " ASYMMETRIC"
	} // else: silently drop ASYMMETRIC – semantics identical
	return this + " BETWEEN" + flag + " " + low + " AND " + high
}

// bracket_offset_expressions (generator.py L3547). indexOffset 0 stands for None (both are falsy).
func (g *Generator) bracketOffsetExpressions(expression *Expr, indexOffset int) []*Expr {
	if expression.ArgB("json_access") {
		return expression.Expressions()
	}

	if indexOffset == 0 {
		indexOffset = g.d.S.INDEX_OFFSET
	}
	exprOffset := 0
	switch v := expression.Arg("offset").(type) {
	case int:
		exprOffset = v
	case bool:
		if v {
			exprOffset = 1
		}
	}
	return gchunkCApplyIndexOffset(
		expression.This(),
		expression.Expressions(),
		indexOffset-exprOffset,
		g.d,
	)
}

// bracket_sql (generator.py L3560)
func (g *Generator) baseBracketSQL(expression *Expr) string {
	expressions := g.bracketOffsetExpressions(expression, 0)
	parts := make([]string, len(expressions))
	for i, e := range expressions {
		parts[i] = g.sql(e)
	}
	expressionsSQL := strings.Join(parts, ", ")
	return g.sqlKey(expression, "this") + "[" + expressionsSQL + "]"
}

// all_sql (generator.py L3565)
func (g *Generator) allSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if !expression.This().IsA(KTuple, KParen) {
		this = g.wrap(this)
	}
	return "ALL " + this
}

// any_sql (generator.py L3571)
func (g *Generator) anySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	// exp.UNWRAPPED_QUERIES = (Select, SetOperation)
	if expression.This().IsA(KSelect, KSetOperation, KParen) {
		if expression.This().IsA(KSelect, KSetOperation) {
			this = g.wrap(this)
		}
		return "ANY" + this
	}
	return "ANY " + this
}

// exists_sql (generator.py L3579)
func (g *Generator) baseExistsSQL(expression *Expr) string {
	return "EXISTS" + g.wrap(expression)
}

// case_sql (generator.py L3582)
func (g *Generator) caseSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	var statements []string
	if this != "" {
		statements = append(statements, "CASE "+this)
	} else {
		statements = append(statements, "CASE")
	}

	for _, e := range expression.ArgL("ifs") {
		statements = append(statements, "WHEN "+g.sqlKey(e, "this"))
		statements = append(statements, "THEN "+g.sqlKey(e, "true"))
	}

	def := g.sqlKey(expression, "default")

	if def != "" {
		statements = append(statements, "ELSE "+def)
	}

	statements = append(statements, "END")

	if g.pretty && g.tooWide(statements) {
		return g.indent(strings.Join(statements, "\n"), 0, -1, true, true)
	}

	return strings.Join(statements, " ")
}

// constraint_sql (generator.py L3602)
func (g *Generator) baseConstraintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressions := g.expressions(expression, exprsOpts{flat: true})
	return "CONSTRAINT " + this + " " + expressions
}

// nextvaluefor_sql (generator.py L3607)
func (g *Generator) nextvalueforSQL(expression *Expr) string {
	order := ""
	if o := expression.ArgE("order"); o != nil {
		order = " OVER (" + g.orderSQL(o, true) + ")"
	}
	return "NEXT VALUE FOR " + g.sqlKey(expression, "this") + order
}

// extract_sql (generator.py L3612)
func (g *Generator) baseExtractSQL(expression *Expr) string {
	this := expression.This()
	if g.s.NORMALIZE_EXTRACT_DATE_PARTS {
		this = gchunkCMapDatePart(expression.This(), g.d)
	}
	var thisSQL string
	if g.s.EXTRACT_ALLOWS_QUOTES {
		thisSQL = g.sql(this)
	} else {
		thisSQL = this.Name()
	}
	expressionSQL := g.sqlKey(expression, "expression")

	return "EXTRACT(" + thisSQL + " FROM " + expressionSQL + ")"
}

// trim_sql (generator.py L3625)
func (g *Generator) baseTrimSQL(expression *Expr) string {
	trimType := g.sqlKey(expression, "position")

	var funcName string
	if trimType == "LEADING" {
		funcName = "LTRIM"
	} else if trimType == "TRAILING" {
		funcName = "RTRIM"
	} else {
		funcName = "TRIM"
	}

	return g.fn(funcName, expression.Arg("this"), expression.Arg("expression"))
}

// convert_concat_args (generator.py L3637)
func (g *Generator) convertConcatArgs(expression *Expr) []*Expr {
	args := expression.Expressions()
	if expression.IsA(KConcatWs) && len(args) > 0 {
		args = args[1:] // Skip the delimiter
	}

	if g.d.S.STRICT_STRING_CONCAT && expression.ArgB("safe") {
		casted := make([]*Expr, len(args))
		for i, e := range args {
			casted[i] = gchunkCCast(e, DT_TEXT)
		}
		args = casted
	}

	concatCoalesce := g.d.S.CONCAT_COALESCE
	if expression.IsA(KConcatWs) {
		concatCoalesce = g.d.S.CONCAT_WS_COALESCE
	}

	if !concatCoalesce && expression.ArgB("coalesce") {
		wrapWithCoalesce := func(e *Expr) *Expr {
			if e.Type() == nil {
				e = gchunkCAnnotateTypes(e, g.d)
			}

			if e.IsString() || gchunkCIsType(e, DT_ARRAY) {
				return e
			}

			// exp.func("coalesce", e, exp.Literal.string("")) -> build_coalesce on copied args
			return New(KCoalesce, "this", e.Copy(), "expressions", []*Expr{LiteralString("")}, "is_nvl", nil, "is_null", nil)
		}

		wrapped := make([]*Expr, len(args))
		for i, e := range args {
			wrapped[i] = wrapWithCoalesce(e)
		}
		args = wrapped
	}

	return args
}

// concat_sql (generator.py L3668)
func (g *Generator) concatSQL(expression *Expr) string {
	if g.d.S.CONCAT_COALESCE && !expression.ArgB("coalesce") {
		// Dialect's CONCAT function coalesces NULLs to empty strings, but the expression does not.
		// Transpile to double pipe operators, which typically returns NULL if any args are NULL
		// instead of coalescing them to empty string.
		return gchunkCConcatToDpipeSQL(g, expression)
	}

	expressions := g.convertConcatArgs(expression)

	// Some dialects don't allow a single-argument CONCAT call
	if !g.s.SUPPORTS_SINGLE_ARG_CONCAT && len(expressions) == 1 {
		return g.sql(expressions[0])
	}

	return g.fn("CONCAT", gchunkCExprsToAny(expressions)...)
}

// concatws_sql (generator.py L3685)
func (g *Generator) baseConcatwsSQL(expression *Expr) string {
	if g.d.S.CONCAT_WS_COALESCE && !expression.ArgB("coalesce") {
		// Dialect's CONCAT_WS function skips NULL args, but the expression does not.
		// Wrap the entire call in a CASE expression that returns NULL if any input IS NULL.
		allArgs := expression.Expressions()
		expression.Set("coalesce", true)
		conds := make([]*Expr, 0, len(allArgs))
		for _, arg := range allArgs {
			conds = append(conds, gchunkCIs(arg, Null()))
		}
		c := New(KCase, "this", nil, "ifs", []*Expr{})
		c = gchunkCCaseWhen(c, gchunkCOr(conds...), Null())
		c = gchunkCCaseElse(c, expression)
		return g.sql(c)
	}

	args := []any{seqGet(expression.Expressions(), 0)}
	for _, a := range g.convertConcatArgs(expression) {
		args = append(args, a)
	}
	return g.fn("CONCAT_WS", args...)
}
