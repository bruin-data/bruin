package sqlengine

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

func (g *Generator) varSQL(expression *Expr) string {
	return g.sqlKey(expression, "this")
}

func (g *Generator) baseIntoSQL(expression *Expr) string {
	if expression.ArgB("expressions") {
		g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", "expressions", expression.Kind().Name(), g.d.ClassName))
	}

	temporary := ""
	if expression.ArgB("temporary") {
		temporary = " TEMPORARY"
	}
	unlogged := ""
	if expression.ArgB("unlogged") {
		unlogged = " UNLOGGED"
	}
	tempOrUnlogged := temporary
	if tempOrUnlogged == "" {
		tempOrUnlogged = unlogged
	}
	return g.seg("INTO") + tempOrUnlogged + " " + g.sqlKey(expression, "this")
}

func (g *Generator) fromSQL(expression *Expr) string {
	return g.seg("FROM") + " " + g.sqlKey(expression, "this")
}

func (g *Generator) groupingsetsSQL(expression *Expr) string {
	groupingSets := g.expressions(expression, exprsOpts{noIndent: true})
	return "GROUPING SETS " + g.wrap(groupingSets)
}

func (g *Generator) rollupSQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{noIndent: true})
	if expressions != "" {
		return "ROLLUP " + g.wrap(expressions)
	}
	return "WITH ROLLUP"
}

func (g *Generator) rollupindexSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")

	columns := g.expressions(expression, exprsOpts{flat: true})

	fromSQL := g.sqlKey(expression, "from_index")
	if fromSQL != "" {
		fromSQL = " FROM " + fromSQL
	}

	propertiesSQL := ""
	if properties := expression.ArgE("properties"); properties != nil {
		propertiesSQL = " " + g.properties(properties, "PROPERTIES", ", ", "", true)
	}

	return this + "(" + columns + ")" + fromSQL + propertiesSQL
}

func (g *Generator) rolluppropertySQL(expression *Expr) string {
	return "ROLLUP (" + g.expressions(expression, exprsOpts{flat: true}) + ")"
}

func (g *Generator) cubeSQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{noIndent: true})
	if expressions != "" {
		return "CUBE " + g.wrap(expressions)
	}
	return "WITH CUBE"
}

func (g *Generator) groupSQL(expression *Expr) string {
	modifier := ""
	if groupByAll, ok := expression.Arg("all").(bool); ok {
		if groupByAll {
			modifier = " ALL"
		} else {
			modifier = " DISTINCT"
		}
	}

	groupBy := g.opExpressions("GROUP BY"+modifier, expression, false)

	groupingSets := g.expressions(expression, exprsOpts{key: "grouping_sets"})
	cube := g.expressions(expression, exprsOpts{key: "cube"})
	rollup := g.expressions(expression, exprsOpts{key: "rollup"})

	segOrEmpty := func(s string) string {
		if s != "" {
			return g.seg(s)
		}
		return ""
	}
	totals := ""
	if expression.ArgB("totals") {
		totals = g.seg("WITH TOTALS")
	}
	groupings := gchunkBCsv(
		g.s.GROUPINGS_SEP,
		segOrEmpty(groupingSets),
		segOrEmpty(cube),
		segOrEmpty(rollup),
		totals,
	)

	if len(expression.Expressions()) > 0 && groupings != "" {
		if stripped := pyStrip(groupings); stripped != "WITH CUBE" && stripped != "WITH ROLLUP" {
			// GROUPING SETS kept apart follow the expressions without a comma where supported
			addSeparator := true
			if groupingSets != "" {
				if g.s.SUPPORTS_GROUPING_SETS_AS_SUFFIX {
					addSeparator = false
				} else {
					g.unsupported("GROUPING SETS without a comma after GROUP BY expressions is not supported")
				}
			}
			if addSeparator {
				groupBy = groupBy + g.s.GROUPINGS_SEP
			}
		}
	}

	return groupBy + groupings
}

func (g *Generator) havingSQL(expression *Expr) string {
	this := g.indentDefault(g.sqlKey(expression, "this"))
	return g.seg("HAVING") + g.sep1() + this
}

func (g *Generator) connectSQL(expression *Expr) string {
	start := g.sqlKey(expression, "start")
	if start != "" {
		start = g.seg("START WITH " + start)
	}
	nocycle := ""
	if expression.ArgB("nocycle") {
		nocycle = " NOCYCLE"
	}
	connect := g.sqlKey(expression, "connect")
	connect = g.seg("CONNECT BY" + nocycle + " " + connect)
	return start + connect
}

func (g *Generator) priorSQL(expression *Expr) string {
	return "PRIOR " + g.sqlKey(expression, "this")
}

func (g *Generator) baseJoinSQL(expression *Expr) string {
	return g.joinSQLWithOp(expression, "")
}

// joinSQLWithOp is baseJoinSQL with the join operator (e.g. LEFT JOIN) replaced by joinOp when
// it is set.
func (g *Generator) joinSQLWithOp(expression *Expr, joinOp string) string {
	var side string
	if kind := expression.KindText(); !g.s.SEMI_ANTI_JOIN_WITH_SIDE && (kind == "SEMI" || kind == "ANTI") {
		side = ""
	} else {
		side = expression.SideText()
	}

	var ops []string
	addOp := func(op string) {
		if op != "" {
			ops = append(ops, op)
		}
	}
	addOp(expression.MethodText())
	if expression.ArgB("global_") {
		addOp("GLOBAL")
	}
	addOp(side)
	addOp(expression.KindText())
	if g.s.JOIN_HINTS {
		addOp(pyUpper(expression.Text("hint")))
	}
	if expression.ArgB("directed") && g.s.DIRECTED_JOINS {
		addOp("DIRECTED")
	}
	opSQL := strings.Join(ops, " ")

	matchCond := g.sqlKey(expression, "match_condition")
	if matchCond != "" {
		matchCond = " MATCH_CONDITION (" + matchCond + ")"
	}
	nearest := g.sqlKey(expression, "nearest")
	if nearest != "" {
		nearest = " " + nearest
	}
	onSQL := g.sqlKey(expression, "on")
	using := expression.ArgL("using")

	if onSQL == "" && len(using) > 0 {
		cols := make([]string, 0, len(using))
		for _, column := range using {
			cols = append(cols, g.sql(column))
		}
		onSQL = gchunkBCsv(", ", cols...)
	}

	this := expression.This()
	thisSQL := g.sql(expression.Arg("this"))

	exprs := g.expressions(expression, exprsOpts{})
	if exprs != "" {
		thisSQL = thisSQL + "," + g.seg(exprs)
	}

	if onSQL != "" {
		onSQL = g.indent(onSQL, 0, -1, true, false)
		space := " "
		if g.pretty {
			space = g.seg(strings.Repeat(" ", g.pad))
		}
		if len(using) > 0 {
			onSQL = space + "USING (" + onSQL + ")"
		} else {
			onSQL = space + "ON " + onSQL
		}
	} else if opSQL == "" && nearest == "" {
		if this.IsA(KLateral) && this.Arg("cross_apply") != nil {
			return " " + thisSQL
		}

		return ", " + thisSQL
	}

	if joinOp != "" {
		opSQL = joinOp
	} else if opSQL != "STRAIGHT_JOIN" {
		if opSQL != "" {
			opSQL = opSQL + " JOIN"
		} else {
			opSQL = "JOIN"
		}
	}

	pivots := g.expressions(expression, exprsOpts{key: "pivots", sep: strp2(""), flat: true})
	return g.seg(opSQL) + " " + thisSQL + matchCond + nearest + onSQL + pivots
}

func (g *Generator) nearestSQL(expression *Expr) string {
	if !g.s.SUPPORTS_NEAREST_JOIN {
		g.unsupported("NEAREST joins are not supported")
	}

	approximate := ""
	if expression.ArgB("approximate") {
		approximate = "APPROX "
	}
	metric := "DISTANCE"
	if expression.ArgB("similarity") {
		metric = "SIMILARITY"
	}
	return approximate + "NEAREST " + g.sqlKey(expression, "count") + " BY " + metric + " " + g.sqlKey(expression, "this")
}

func (g *Generator) baseLambdaSQL(expression *Expr, arrowSep string, wrap bool) string {
	args := g.expressions(expression, exprsOpts{flat: true})
	if wrap && len(strings.Split(args, ",")) > 1 {
		args = "(" + args + ")"
	}
	return args + " " + arrowSep + " " + g.sqlKey(expression, "this")
}

func (g *Generator) baseLateralOp(expression *Expr) string {
	// https://www.mssqltips.com/sqlservertip/1958/sql-server-cross-apply-and-outer-apply/
	op := ""
	if crossApply, ok := expression.Arg("cross_apply").(bool); ok {
		if crossApply {
			op = "INNER JOIN "
		} else {
			op = "LEFT JOIN "
		}
	}

	return op + "LATERAL"
}

func (g *Generator) baseLateralSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")

	if expression.ArgB("view") {
		alias := expression.ArgE("alias")
		columns := g.expressions(alias, exprsOpts{key: "columns", flat: true})
		table := ""
		if alias.Name() != "" {
			table = " " + alias.Name()
		}
		if columns != "" {
			columns = " AS " + columns
		}
		outer := ""
		if expression.ArgB("outer") {
			outer = " OUTER"
		}
		opSQL := g.seg("LATERAL VIEW" + outer)
		return opSQL + g.sep1() + this + table + columns
	}

	tableAlias := expression.ArgE("alias")
	offset := expression.This().ArgE("offset")
	if g.s.UNNEST_WITH_ORDINALITY && tableAlias != nil && offset.IsA(KIdentifier) {
		// UNNEST ... WITH ORDINALITY stores the ordinality column's name in Unnest.offset
		tableAlias = tableAlias.Copy()
		tableAlias.Append("columns", offset.Copy())
	}

	alias := g.sql(tableAlias)
	if alias != "" {
		alias = " AS " + alias
	}

	ordinality := ""
	if expression.ArgB("ordinality") {
		ordinality = " WITH ORDINALITY" + alias
		alias = ""
	}

	return g.lateralOp(expression) + " " + this + alias + ordinality
}

func (g *Generator) limitSQL(expression *Expr, top bool) string {
	this := g.sqlKey(expression, "this")

	// LIMIT ALL caps nothing, so it is dropped where it can't be written.
	if expression.isLimitAll() && !g.d.S.SUPPORTS_LIMIT_ALL {
		return this
	}

	var args []*Expr
	for _, k := range []string{"offset", "expression"} {
		e := expression.ArgE(k)
		if e == nil {
			continue
		}
		if g.s.LIMIT_ONLY_LITERALS {
			e = g.simplifyUnlessLiteral(e)
		}
		args = append(args, e)
	}

	argSQLs := make([]string, 0, len(args))
	for _, e := range args {
		argSQLs = append(argSQLs, g.sql(e))
	}
	argsSQL := strings.Join(argSQLs, ", ")
	if top {
		for _, e := range args {
			if !e.IsNumber() {
				argsSQL = "(" + argsSQL + ")"
				break
			}
		}
	}
	expressions := g.expressions(expression, exprsOpts{flat: true})
	limitOptions := g.sqlKey(expression, "limit_options")
	if expressions != "" {
		expressions = " BY " + expressions
	}

	kw := "LIMIT"
	if top {
		kw = "TOP"
	}
	return this + g.seg(kw) + " " + argsSQL + limitOptions + expressions
}

func (g *Generator) baseOffsetSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	value := expression.Expression()
	if g.s.LIMIT_ONLY_LITERALS {
		value = g.simplifyUnlessLiteral(value)
	}
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " BY " + expressions
	}
	return this + g.seg("OFFSET") + " " + g.sql(value) + expressions
}

func (g *Generator) baseSetitemSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	if !g.s.SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD && kind == "VARIABLE" {
		kind = ""
	} else if kind != "" {
		kind = kind + " "
	}
	this := g.sqlKey(expression, "this")
	expressions := g.expressions(expression, exprsOpts{})
	collate := g.sqlKey(expression, "collate")
	if collate != "" {
		collate = " COLLATE " + collate
	}
	global := ""
	if expression.ArgB("global_") {
		global = "GLOBAL "
	}
	return global + kind + this + expressions + collate
}

func (g *Generator) setSQL(expression *Expr) string {
	expressions := " " + g.expressions(expression, exprsOpts{flat: true})
	tag := ""
	if expression.ArgB("tag") {
		tag = " TAG"
	}
	kw := "SET"
	if expression.ArgB("unset") {
		kw = "UNSET"
	}
	return kw + tag + expressions
}

func (g *Generator) querybandSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	update := ""
	if expression.ArgB("update") {
		update = " UPDATE"
	}
	scope := g.sqlKey(expression, "scope")
	if scope != "" {
		scope = " FOR " + scope
	}

	return "QUERY_BAND = " + this + update + scope
}

func (g *Generator) pragmaSQL(expression *Expr) string {
	return "PRAGMA " + g.sqlKey(expression, "this")
}

func (g *Generator) lockSQL(expression *Expr) string {
	if !g.s.LOCKING_READS_SUPPORTED {
		g.unsupported("Locking reads using 'FOR UPDATE/SHARE' are not supported")
		return ""
	}

	update := expression.ArgB("update")
	key := expression.ArgB("key")
	var lockType string
	if update {
		if key {
			lockType = "FOR NO KEY UPDATE"
		} else {
			lockType = "FOR UPDATE"
		}
	} else {
		if key {
			lockType = "FOR KEY SHARE"
		} else {
			lockType = "FOR SHARE"
		}
	}
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " OF " + expressions
	}
	wait := ""
	if w := expression.Arg("wait"); w != nil {
		if we, ok := w.(*Expr); ok && we.IsA(KLiteral) {
			wait = " WAIT " + g.sql(we)
		} else if truthy(w) {
			wait = " NOWAIT"
		} else {
			wait = " SKIP LOCKED"
		}
	}

	return lockType + expressions + wait
}

func (g *Generator) literalSQL(expression *Expr) string {
	text := expression.ThisS()
	if expression.IsString() {
		text = g.d.S.QUOTE_START + g.escapeStr(text, true, "", "", false) + g.d.S.QUOTE_END
	}
	return text
}

// escapeStr escapes text for a string literal: escape sequences where the dialect supports them,
// line breaks and the delimiter. An empty delimiter or escapedDelimiter selects the dialect's quote
// end and escaped quote end.
func (g *Generator) escapeStr(text string, escapeBackslash bool, delimiter string, escapedDelimiter string, isByteString bool) string {
	var supportsEscapeSequences bool
	if isByteString {
		supportsEscapeSequences = g.d.S.BYTE_STRINGS_SUPPORT_ESCAPED_SEQUENCES
	} else {
		supportsEscapeSequences = g.d.S.STRINGS_SUPPORT_ESCAPED_SEQUENCES
	}

	if supportsEscapeSequences {
		var b strings.Builder
		for i := 0; i < len(text); {
			r, size := utf8.DecodeRuneInString(text[i:])
			ch := text[i : i+size]
			i += size
			if r == utf8.RuneError && size == 1 {
				// A raw byte from a byte escape: escaped again where the dialect reads one as a byte.
				if f := g.rawByteEscape(); f != "" {
					ch = fmt.Sprintf(f, ch[0])
				}
				b.WriteString(ch)
				continue
			}
			if escapeBackslash || ch != "\\" {
				if m, ok := g.d.S.ESCAPED_SEQUENCES[ch]; ok {
					b.WriteString(m)
					continue
				}
			}
			b.WriteString(ch)
		}
		text = b.String()
	}

	if delimiter == "" {
		delimiter = g.d.S.QUOTE_END
	}
	if escapedDelimiter == "" {
		escapedDelimiter = g.escapedQuoteEnd
	}

	return strings.ReplaceAll(g.replaceLineBreaks(text), delimiter, escapedDelimiter)
}

// rawByteEscape is the format of the escape that the dialect reads as one byte: \xHH, or \OOO
// where only octal escapes are read (Redshift). It is "" when numeric escapes are not bytes.
func (g *Generator) rawByteEscape() string {
	if !g.d.T.NUMERIC_ESCAPES_ARE_BYTES {
		return ""
	}
	if spec, ok := g.d.T.NUMERIC_ESCAPES["x"]; ok && spec[2] >= 2 && spec[3] >= 0xFF {
		return "\\x%02x"
	}
	if spec, ok := g.d.T.NUMERIC_ESCAPES["0"]; ok && spec[2] >= 3 && spec[3] >= 0xFF {
		return "\\%03o"
	}
	return ""
}

// escapeRawBytes writes each byte of s that is not part of a UTF-8 character with format.
func escapeRawBytes(s, format string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			fmt.Fprintf(&b, format, s[i])
		} else {
			b.WriteString(s[i : i+size])
		}
		i += size
	}
	return b.String()
}

func (g *Generator) loaddataSQL(expression *Expr) string {
	isOverwrite := expression.ArgB("overwrite")
	overwrite := ""
	if isOverwrite {
		overwrite = " OVERWRITE"
	}
	this := g.sqlKey(expression, "this")

	if files := expression.ArgE("files"); files != nil {
		filesSQL := g.expressions(files, exprsOpts{flat: true})
		filesSQL = "FILES" + g.wrap(filesSQL)
		if isOverwrite {
			this = " " + this
		} else if expression.ArgB("temp") {
			this = " INTO TEMP TABLE " + this
		} else {
			this = " INTO TABLE " + this
		}
		return "LOAD DATA" + overwrite + this + " FROM " + filesSQL
	}

	local := ""
	if expression.ArgB("local") {
		local = " LOCAL"
	}
	inpath := " INPATH " + g.sqlKey(expression, "inpath")
	this = " INTO TABLE " + this
	partition := g.sqlKey(expression, "partition")
	if partition != "" {
		partition = " " + partition
	}
	inputFormat := g.sqlKey(expression, "input_format")
	if inputFormat != "" {
		inputFormat = " INPUTFORMAT " + inputFormat
	}
	serde := g.sqlKey(expression, "serde")
	if serde != "" {
		serde = " SERDE " + serde
	}
	return "LOAD DATA" + local + inpath + overwrite + this + partition + inputFormat + serde
}

func (g *Generator) nullSQL(expression ...any) string {
	return "NULL"
}

func (g *Generator) baseBooleanSQL(expression *Expr) string {
	if expression.ArgB("this") {
		return "TRUE"
	}
	return "FALSE"
}

func (g *Generator) boolandSQL(expression *Expr) string {
	return "((" + g.sqlKey(expression, "this") + ") AND (" + g.sqlKey(expression, "expression") + "))"
}

func (g *Generator) boolorSQL(expression *Expr) string {
	return "((" + g.sqlKey(expression, "this") + ") OR (" + g.sqlKey(expression, "expression") + "))"
}

func (g *Generator) orderSQL(expression *Expr, flat bool) string {
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = this + " "
	}
	siblings := ""
	if expression.ArgB("siblings") {
		siblings = "SIBLINGS "
	}
	return g.opExpressions(this+"ORDER "+siblings+"BY", expression, this != "" || flat)
}

func (g *Generator) withfillSQL(expression *Expr) string {
	fromSQL := g.sqlKey(expression, "from_")
	if fromSQL != "" {
		fromSQL = " FROM " + fromSQL
	}
	toSQL := g.sqlKey(expression, "to")
	if toSQL != "" {
		toSQL = " TO " + toSQL
	}
	stepSQL := g.sqlKey(expression, "step")
	if stepSQL != "" {
		stepSQL = " STEP " + stepSQL
	}
	var interpolatedValues []string
	for _, e := range expression.ArgL("interpolate") {
		if e.IsA(KAlias) {
			interpolatedValues = append(interpolatedValues, g.sqlKey(e, "alias")+" AS "+g.sqlKey(e, "this"))
		} else {
			interpolatedValues = append(interpolatedValues, g.sqlKey(e, "this"))
		}
	}
	interpolate := ""
	if len(interpolatedValues) > 0 {
		interpolate = " INTERPOLATE (" + strings.Join(interpolatedValues, ", ") + ")"
	}
	return "WITH FILL" + fromSQL + toSQL + stepSQL + interpolate
}

func (g *Generator) clusterSQL(expression *Expr) string {
	return g.opExpressions("CLUSTER BY", expression, false)
}

func (g *Generator) baseClusterpropertySQL(expression *Expr) string {
	if expression.ArgB("this") {
		g.unsupported("Unsupported CLUSTER BY " + g.sqlKey(expression, "this"))
		return ""
	}
	expressions := g.expressions(expression, exprsOpts{flat: true})
	return "CLUSTER BY (" + expressions + ")"
}

func (g *Generator) distributeSQL(expression *Expr) string {
	return g.opExpressions("DISTRIBUTE BY", expression, false)
}

func (g *Generator) sortSQL(expression *Expr) string {
	return g.opExpressions("SORT BY", expression, false)
}

// resolveOrderedForNullOrderingSimulation resolves a bare ORDER BY name against the enclosing
// SELECT projection and returns the matching projection's expression (alias stripped) for the
// NULLS FIRST/LAST CASE simulation, which is evaluated in FROM-clause scope rather than alias
// scope (MySQL error 1052). It returns nil when no safe substitution applies.
func (g *Generator) resolveOrderedForNullOrderingSimulation(expression *Expr) *Expr {
	this := expression.This()
	if !(this.IsA(KColumn) && this.TableName() == "") {
		return nil
	}

	ancestor := expression.FindAncestor(KSelect, KWindow)
	if !ancestor.IsA(KSelect) {
		return nil
	}

	columnName := this.Name()
	var matched []*Expr
	for _, p := range ancestor.Selects() {
		if p.OutputName() == columnName {
			if p.IsA(KAlias) {
				matched = append(matched, p.This())
			} else {
				matched = append(matched, p)
			}
		}
	}
	var match *Expr
	if len(matched) == 1 {
		match = matched[0]
	}

	// Skip the substitution when it would be identical to the existing
	// reference (e.g. `SELECT col FROM t ORDER BY col`).
	if match.IsA(KColumn) && match.TableName() == "" && match.Name() == columnName {
		return nil
	}

	return match
}

func (g *Generator) orderedSQL(expression *Expr) string {
	descV := expression.Arg("desc")
	desc := truthy(descV)
	asc := !desc

	nullsFirst := expression.ArgB("nulls_first")
	nullsLast := !nullsFirst
	nullsAreLarge := g.d.S.NULL_ORDERING == "nulls_are_large"
	nullsAreSmall := g.d.S.NULL_ORDERING == "nulls_are_small"
	nullsAreLast := g.d.S.NULL_ORDERING == "nulls_are_last"

	this := g.sqlKey(expression, "this")

	sortOrder := ""
	if desc {
		sortOrder = " DESC"
	} else if b, ok := descV.(bool); ok && !b {
		sortOrder = " ASC"
	}
	nullsSortChange := ""
	if nullsFirst && ((asc && nullsAreLarge) || (desc && nullsAreSmall) || nullsAreLast) {
		nullsSortChange = " NULLS FIRST"
	} else if nullsLast && ((asc && nullsAreSmall) || (desc && nullsAreLarge)) && !nullsAreLast {
		nullsSortChange = " NULLS LAST"
	}

	// If the NULLS FIRST/LAST clause is unsupported, we add another sort key to simulate it
	if nullsSortChange != "" && g.s.NULL_ORDERING_SUPPORTED != TriTrue {
		window := expression.FindAncestor(KWindow, KSelect)

		var windowThis, spec *Expr
		if window.IsA(KWindow) {
			windowThis = window.This()
			if windowThis.IsA(KIgnoreNulls, KRespectNulls) {
				windowThis = windowThis.This()
			}
			spec = window.ArgE("spec")
		}

		// Some window functions (e.g. LAST_VALUE, RANK) support NULLS FIRST/LAST
		// without a spec or with a ROWS spec, but not with RANGE
		if !(windowThis.IsA(g.s.WINDOW_FUNCS_WITH_NULL_ORDERING...) &&
			(spec == nil || pyUpper(spec.Text("kind")) == "ROWS")) {
			if windowThis != nil && spec != nil {
				g.unsupported(fmt.Sprintf("'%s' translation not supported in window function %s", pyStrip(nullsSortChange), windowThis.Kind().SQLName()))
				nullsSortChange = ""
			} else if g.s.NULL_ORDERING_SUPPORTED == TriFalse &&
				((asc && nullsSortChange == " NULLS LAST") || (desc && nullsSortChange == " NULLS FIRST")) {
				// BigQuery does not allow these ordering/nulls combinations when used under
				// an aggregation func or under a window containing one
				ancestor := expression.FindAncestor(KAggFunc, KWindow, KSelect)

				if ancestor.IsA(KWindow) {
					ancestor = ancestor.This()
				}
				if ancestor.IsA(KAggFunc) {
					g.unsupported(fmt.Sprintf("'%s' translation not supported for aggregate function %s with %s sort order", pyStrip(nullsSortChange), ancestor.Kind().SQLName(), sortOrder))
					nullsSortChange = ""
				}
			} else if g.s.NULL_ORDERING_SUPPORTED == TriNone {
				if expression.This().IsInt() {
					g.unsupported(fmt.Sprintf("'%s' translation not supported with positional ordering", pyStrip(nullsSortChange)))
				} else if !expression.This().IsA(KRand) {
					resolved := g.resolveOrderedForNullOrderingSimulation(expression)
					target := this
					if resolved != nil {
						target = g.sql(resolved)
					}
					nullSortOrder := ""
					if nullsSortChange == " NULLS FIRST" {
						nullSortOrder = " DESC"
					}
					this = "CASE WHEN " + target + " IS NULL THEN 1 ELSE 0 END" + nullSortOrder + ", " + target
				}
				nullsSortChange = ""
			}
		}
	}

	withFill := g.sqlKey(expression, "with_fill")
	if withFill != "" {
		withFill = " " + withFill
	}

	return this + sortOrder + nullsSortChange + withFill
}

func (g *Generator) matchrecognizemeasureSQL(expression *Expr) string {
	windowFrame := g.sqlKey(expression, "window_frame")
	if windowFrame != "" {
		windowFrame = windowFrame + " "
	}

	this := g.sqlKey(expression, "this")

	return windowFrame + this
}

func (g *Generator) matchrecognizeSQL(expression *Expr) string {
	partition := g.partitionBySQL(expression)
	order := g.sqlKey(expression, "order")
	measures := g.expressions(expression, exprsOpts{key: "measures"})
	if measures != "" {
		measures = g.seg("MEASURES" + g.seg(measures))
	}
	rows := g.sqlKey(expression, "rows")
	if rows != "" {
		rows = g.seg(rows)
	}
	after := g.sqlKey(expression, "after")
	if after != "" {
		after = g.seg(after)
	}
	pattern := g.sqlKey(expression, "pattern")
	if pattern != "" {
		pattern = g.seg("PATTERN (" + pattern + ")")
	}
	var definitionSQLs []any
	for _, definition := range expression.ArgL("define") {
		definitionSQLs = append(definitionSQLs, g.sqlKey(definition, "alias")+" AS "+g.sqlKey(definition, "this"))
	}
	definitions := g.expressions(nil, exprsOpts{sqls: definitionSQLs, hasSqls: true})
	define := ""
	if definitions != "" {
		define = g.seg("DEFINE" + g.seg(definitions))
	}
	body := partition + order + measures + rows + after + pattern + define
	alias := g.sqlKey(expression, "alias")
	if alias != "" {
		alias = " " + alias
	}
	return g.seg("MATCH_RECOGNIZE") + " " + g.wrap(body) + alias
}

// queryModifiers renders the clauses after a query's FROM (joins through OPTION) after sqls,
// converting between LIMIT and FETCH as the dialect requires.
func (g *Generator) queryModifiers(expression *Expr, sqls ...string) string {
	limit := expression.ArgE("limit")

	if g.s.LIMIT_FETCH == "LIMIT" && limit.IsA(KFetch) {
		// "FETCH FIRST ROWS ONLY" without a count means one row per the SQL
		// standard; emitting a bare "LIMIT" here would produce invalid SQL.
		var count *Expr
		if c := limit.ArgE("count"); c != nil {
			count = c.Copy()
		} else {
			count = LiteralInt(1)
		}
		limit = New(KLimit, "expression", count)
	} else if g.s.LIMIT_FETCH == "FETCH" && limit.IsA(KLimit) && !limit.isLimitAll() {
		limit = New(KFetch, "direction", "FIRST", "count", limit.Expression().Copy())
	}

	parts := append([]string{}, sqls...)
	for _, join := range expression.ArgL("joins") {
		parts = append(parts, g.sql(join))
	}
	parts = append(parts, g.sqlKey(expression, "match"))
	for _, lateral := range expression.ArgL("laterals") {
		parts = append(parts, g.sql(lateral))
	}
	parts = append(
		parts,
		g.sqlKey(expression, "prewhere"),
		g.sqlKey(expression, "where"),
		g.sqlKey(expression, "connect"),
		g.sqlKey(expression, "group"),
		g.sqlKey(expression, "having"),
	)
	for _, k := range g.s.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		gen := g.s.AFTER_HAVING_MODIFIER_TRANSFORMS[k]
		parts = append(parts, gen(g, expression))
	}
	parts = append(parts, g.sqlKey(expression, "order"))
	parts = append(parts, g.offsetLimitModifiers(expression, limit.IsA(KFetch), limit)...)
	parts = append(parts, g.afterLimitModifiers(expression)...)
	parts = append(parts, g.sqlKey(expression, "for_"))
	parts = append(parts, g.optionsModifier(expression))
	return gchunkBCsv("", parts...)
}

func (g *Generator) baseOptionsModifier(expression *Expr) string {
	options := g.expressions(expression, exprsOpts{key: "options"})
	if options != "" {
		return " " + options
	}
	return ""
}

func (g *Generator) forclauseSQL(expression *Expr) string {
	kind := expression.Arg("kind")
	if s, ok := kind.(string); ok && s == "BROWSE" {
		return g.sep1() + "FOR BROWSE"
	}
	// FOR XML/JSON always carry at least AUTO/PATH. An empty rendering means
	// the target dialect doesn't support QueryOption, so we drop the clause.
	options := g.expressions(expression, exprsOpts{key: "expressions"})
	if options == "" {
		return ""
	}
	return g.sep1() + "FOR " + gchunkBPyStr(kind) + g.seg(options)
}

func (g *Generator) baseQueryoptionSQL(expression *Expr) string {
	g.unsupported("Unsupported query option.")
	return ""
}

func (g *Generator) baseOffsetLimitModifiers(expression *Expr, fetch bool, limit *Expr) []string {
	if fetch {
		first := g.sqlKey(expression, "offset")
		return []string{first, g.sql(limit)}
	}
	first := g.sql(limit)
	return []string{first, g.sqlKey(expression, "offset")}
}

func (g *Generator) baseAfterLimitModifiers(expression *Expr) []string {
	locks := g.expressions(expression, exprsOpts{key: "locks", sep: strp2(" ")})
	if locks != "" {
		locks = " " + locks
	}
	return []string{locks, g.sqlKey(expression, "sample")}
}

func gchunkBCsv(sep string, args ...string) string {
	parts := make([]string, 0, len(args))
	for _, a := range args {
		if a != "" {
			parts = append(parts, a)
		}
	}
	return strings.Join(parts, sep)
}

// gchunkBPyStr formats a raw argument value for SQL text: nil as None, booleans as True/False,
// expressions as default-dialect SQL.
func gchunkBPyStr(v any) string {
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
	case int:
		return itoa(x)
	case *Expr:
		if x == nil {
			return "None"
		}
		return exprSQL(x)
	}
	return fmt.Sprint(v)
}

// gchunkBAlias returns a copy of expression aliased as alias (an Identifier or nil): the alias is
// set on kinds that take one and an Alias wraps the rest.
func gchunkBAlias(expression *Expr, alias *Expr) *Expr {
	e := expression.Copy()
	var id *Expr
	if alias != nil {
		if !alias.IsA(KIdentifier) {
			panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + alias.Kind().Name()})
		}
		id = alias.Copy()
	}

	// We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
	// the AST, representing a "named_window" construct (eg. bigquery).
	if e.Kind().hasArgType("alias") && e.Kind() != KWindow {
		e.Set("alias", id)
		return e
	}
	return New(KAlias, "this", e, "alias", id)
}
