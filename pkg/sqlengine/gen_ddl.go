package sqlengine

import (
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

func (g *Generator) uncacheSQL(expression *Expr) string {
	table := g.sqlKey(expression, "this")
	existsSQL := ""
	if expression.ArgB("exists") {
		existsSQL = " IF EXISTS"
	}
	return "UNCACHE TABLE" + existsSQL + " " + table
}

func (g *Generator) cacheSQL(expression *Expr) string {
	lazy := ""
	if expression.ArgB("lazy") {
		lazy = " LAZY"
	}
	table := g.sqlKey(expression, "this")
	options := ""
	if opts := expression.ArgL("options"); len(opts) > 0 {
		options = " OPTIONS(" + g.sql(opts[0]) + " = " + g.sql(opts[1]) + ")"
	}
	sql := g.sqlKey(expression, "expression")
	if sql != "" {
		sql = " AS" + g.sep1() + sql
	}
	sql = "CACHE" + lazy + " TABLE " + table + options + sql
	return g.prependCtes(expression, sql)
}

func (g *Generator) charactersetSQL(expression *Expr) string {
	def := ""
	if expression.ArgB("default") {
		def = "DEFAULT "
	}
	return def + "CHARACTER SET=" + g.sqlKey(expression, "this")
}

func (g *Generator) baseColumnParts(expression *Expr) string {
	var parts []string
	for _, k := range []string{"catalog", "db", "table", "this"} {
		if part := expression.Arg(k); truthy(part) {
			parts = append(parts, g.sql(part))
		}
	}
	return strings.Join(parts, ".")
}

func (g *Generator) columnSQL(expression *Expr) string {
	joinMark := ""
	if expression.ArgB("join_mark") {
		joinMark = " (+)"
	}
	if joinMark != "" && !g.d.S.SUPPORTS_COLUMN_JOIN_MARKS {
		joinMark = ""
		g.unsupported("Outer join syntax using the (+) operator is not supported.")
	}
	return g.columnParts(expression) + joinMark
}

func (g *Generator) pseudocolumnSQL(expression *Expr) string {
	return g.columnSQL(expression)
}

func (g *Generator) columnpositionSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	position := g.sqlKey(expression, "position")
	return position + this
}

func (g *Generator) baseColumndefSQL(expression *Expr, sep string) string {
	column := g.sqlKey(expression, "this")
	kind := g.sqlKey(expression, "kind")
	constraints := g.expressions(expression, exprsOpts{key: "constraints", sep: strp2(" "), flat: true})
	exists := ""
	if expression.ArgB("exists") {
		exists = "IF NOT EXISTS "
	}
	if kind != "" {
		kind = sep + kind
	}
	if constraints != "" {
		constraints = " " + constraints
	}
	position := g.sqlKey(expression, "position")
	if position != "" {
		position = " " + position
	}

	if expression.Find(KComputedColumnConstraint) != nil && !g.s.COMPUTED_COLUMN_WITH_TYPE {
		kind = ""
	}

	return exists + column + kind + constraints + position
}

func (g *Generator) columnconstraintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	kindSQL := pyStrip(g.sqlKey(expression, "kind"))
	if this != "" {
		return "CONSTRAINT " + this + " " + kindSQL
	}
	return kindSQL
}

func (g *Generator) baseComputedcolumnconstraintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	var persisted string
	if expression.ArgB("not_null") {
		persisted = " PERSISTED NOT NULL"
	} else if expression.ArgB("persisted") {
		persisted = " PERSISTED"
	} else {
		persisted = ""
	}
	return "AS " + this + persisted
}

func (g *Generator) baseAutoincrementcolumnconstraintSQL(expression *Expr) string {
	return g.tokenSQL(TK_AUTO_INCREMENT)
}

func (g *Generator) compresscolumnconstraintSQL(expression *Expr) string {
	var this string
	if _, ok := expression.Arg("this").([]*Expr); ok {
		this = g.wrap(g.expressions(expression, exprsOpts{key: "this", flat: true}))
	} else {
		this = g.sqlKey(expression, "this")
	}
	return "COMPRESS " + this
}

func (g *Generator) baseGeneratedasidentitycolumnconstraintSQL(expression *Expr) string {
	this := ""
	if thisV := expression.Arg("this"); thisV != nil {
		onNull := ""
		if expression.ArgB("on_null") {
			onNull = " ON NULL"
		}
		if truthy(thisV) {
			this = " ALWAYS"
		} else {
			this = " BY DEFAULT" + onNull
		}
	}

	start := ""
	if v := expression.Arg("start"); truthy(v) {
		start = "START WITH " + chunkAPyStr(v)
	}
	increment := ""
	if v := expression.Arg("increment"); truthy(v) {
		increment = " INCREMENT BY " + chunkAPyStr(v)
	}
	minvalue := ""
	if v := expression.Arg("minvalue"); truthy(v) {
		minvalue = " MINVALUE " + chunkAPyStr(v)
	}
	maxvalue := ""
	if v := expression.Arg("maxvalue"); truthy(v) {
		maxvalue = " MAXVALUE " + chunkAPyStr(v)
	}
	cycle := expression.Arg("cycle")
	cycleSQL := ""

	if cycle != nil {
		no := ""
		if !truthy(cycle) {
			no = " NO"
		}
		cycleSQL = no + " CYCLE"
		if start == "" && increment == "" {
			cycleSQL = pyStrip(cycleSQL)
		}
	}

	sequenceOpts := ""
	if start != "" || increment != "" || cycleSQL != "" {
		sequenceOpts = start + increment + minvalue + maxvalue + cycleSQL
		sequenceOpts = " (" + pyStrip(sequenceOpts) + ")"
	}

	expr := g.sqlKey(expression, "expression")
	if expr != "" {
		expr = "(" + expr + ")"
	} else {
		expr = "IDENTITY"
	}

	return "GENERATED" + this + " AS " + expr + sequenceOpts
}

func (g *Generator) generatedasrowcolumnconstraintSQL(expression *Expr) string {
	start := "END"
	if expression.ArgB("start") {
		start = "START"
	}
	hidden := ""
	if expression.ArgB("hidden") {
		hidden = " HIDDEN"
	}
	return "GENERATED ALWAYS AS ROW " + start + hidden
}

func (g *Generator) periodforsystemtimeconstraintSQL(expression *Expr) string {
	return "PERIOD FOR SYSTEM_TIME (" + g.sqlKey(expression, "this") + ", " + g.sqlKey(expression, "expression") + ")"
}

func (g *Generator) notnullcolumnconstraintSQL(expression *Expr) string {
	if expression.ArgB("allow_null") {
		return "NULL"
	}
	return "NOT NULL"
}

func (g *Generator) primarykeycolumnconstraintSQL(expression *Expr) string {
	desc := expression.Arg("desc")
	if desc != nil {
		if truthy(desc) {
			return "PRIMARY KEY DESC"
		}
		return "PRIMARY KEY ASC"
	}
	options := g.expressions(expression, exprsOpts{key: "options", flat: true, sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}
	include := g.sqlKey(expression, "include")
	return "PRIMARY KEY" + include + options
}

func (g *Generator) uniquecolumnconstraintSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	indexType := ""
	if v := expression.Arg("index_type"); truthy(v) {
		indexType = " USING " + chunkAPyStr(v)
	}
	onConflict := g.sqlKey(expression, "on_conflict")
	if onConflict != "" {
		onConflict = " " + onConflict
	}
	nullsSQL := ""
	if expression.ArgB("nulls") {
		nullsSQL = " NULLS NOT DISTINCT"
	}
	options := g.expressions(expression, exprsOpts{key: "options", flat: true, sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}
	include := g.sqlKey(expression, "include")
	return "UNIQUE" + nullsSQL + this + indexType + onConflict + include + options
}

func (g *Generator) inoutcolumnconstraintSQL(expression *Expr) string {
	input := expression.ArgB("input_")
	output := expression.ArgB("output")
	variadic := expression.ArgB("variadic")

	// VARIADIC is mutually exclusive with IN/OUT/INOUT
	if variadic {
		return "VARIADIC"
	}

	if input && output {
		return "IN" + g.s.INOUT_SEPARATOR + "OUT"
	}
	if input {
		return "IN"
	}
	if output {
		return "OUT"
	}

	return ""
}

func (g *Generator) baseCreateableSQL(expression *Expr, locations propLocations) string {
	return g.sqlKey(expression, "this")
}

func (g *Generator) baseCreateSQL(expression *Expr) string {
	kind := g.sqlKey(expression, "kind")
	if mapped := g.d.S.INVERSE_CREATABLE_KIND_MAPPING[kind]; mapped != "" {
		kind = mapped
	}

	properties := expression.ArgE("properties")

	if kind == "TRIGGER" && properties != nil && len(properties.Expressions()) > 0 &&
		properties.Expressions()[0].IsA(KTriggerProperties) &&
		properties.Expressions()[0].ArgB("constraint") {
		kind = "CONSTRAINT " + kind
	}

	var propertiesLocs propLocations
	if properties != nil {
		propertiesLocs = g.locateProperties(properties)
	} else {
		propertiesLocs = propLocations{}
	}

	this := g.createableSQL(expression, propertiesLocs)

	propertiesSQL := ""
	if len(propertiesLocs[Loc_POST_SCHEMA]) > 0 || len(propertiesLocs[Loc_POST_WITH]) > 0 {
		exprs := make([]*Expr, 0, len(propertiesLocs[Loc_POST_SCHEMA])+len(propertiesLocs[Loc_POST_WITH]))
		exprs = append(exprs, propertiesLocs[Loc_POST_SCHEMA]...)
		exprs = append(exprs, propertiesLocs[Loc_POST_WITH]...)
		propsAST := New(KProperties, "expressions", exprs)
		propsAST.parent = expression
		propertiesSQL = g.sql(propsAST)

		if len(propertiesLocs[Loc_POST_SCHEMA]) > 0 {
			propertiesSQL = g.sep1() + propertiesSQL
		} else if !g.pretty {
			// Standalone POST_WITH properties need a leading whitespace in non-pretty mode
			propertiesSQL = " " + propertiesSQL
		}
	}

	begin := ""
	if expression.ArgB("begin") {
		begin = " BEGIN"
	}

	expressionSQL := g.sqlKey(expression, "expression")
	if expressionSQL != "" {
		expressionSQL = begin + g.sep1() + expressionSQL

		inner := expression.Expression()
		if !inner.IsA(KMacroOverloads) && (g.s.CREATE_FUNCTION_RETURN_AS || !inner.IsA(KReturn)) {
			postaliasPropsSQL := ""
			if len(propertiesLocs[Loc_POST_ALIAS]) > 0 {
				postaliasPropsSQL = g.properties(
					New(KProperties, "expressions", propertiesLocs[Loc_POST_ALIAS]),
					"", ", ", "", false,
				)
			}
			if postaliasPropsSQL != "" {
				postaliasPropsSQL = " " + postaliasPropsSQL
			}
			expressionSQL = " AS" + postaliasPropsSQL + expressionSQL
		}
	}

	postindexPropsSQL := ""
	if len(propertiesLocs[Loc_POST_INDEX]) > 0 {
		postindexPropsSQL = g.properties(
			New(KProperties, "expressions", propertiesLocs[Loc_POST_INDEX]),
			" ", ", ", "", false,
		)
	}

	indexes := g.expressions(expression, exprsOpts{key: "indexes", noIndent: true, sep: strp2(" ")})
	if indexes != "" {
		indexes = " " + indexes
	}
	indexSQL := indexes + postindexPropsSQL

	replace := ""
	if expression.ArgB("replace") {
		replace = " OR REPLACE"
	}
	refresh := ""
	if expression.ArgB("refresh") {
		refresh = " OR REFRESH"
	}
	unique := ""
	if expression.ArgB("unique") {
		unique = " UNIQUE"
	}

	clustered := expression.Arg("clustered")
	var clusteredSQL string
	if clustered == nil {
		clusteredSQL = ""
	} else if truthy(clustered) {
		clusteredSQL = " CLUSTERED COLUMNSTORE"
	} else {
		clusteredSQL = " NONCLUSTERED COLUMNSTORE"
	}

	postcreatePropsSQL := ""
	if len(propertiesLocs[Loc_POST_CREATE]) > 0 {
		postcreatePropsSQL = g.properties(
			New(KProperties, "expressions", propertiesLocs[Loc_POST_CREATE]),
			" ", " ", "", false,
		)
	}

	modifiers := clusteredSQL + replace + refresh + unique + postcreatePropsSQL

	postexpressionPropsSQL := ""
	if len(propertiesLocs[Loc_POST_EXPRESSION]) > 0 {
		postexpressionPropsSQL = g.properties(
			New(KProperties, "expressions", propertiesLocs[Loc_POST_EXPRESSION]),
			" ", " ", "", false,
		)
	}

	concurrently := ""
	if expression.ArgB("concurrently") {
		concurrently = " CONCURRENTLY"
	}
	existsSQL := ""
	if expression.ArgB("exists") {
		existsSQL = " IF NOT EXISTS"
	}
	noSchemaBinding := ""
	if expression.ArgB("no_schema_binding") {
		noSchemaBinding = " WITH NO SCHEMA BINDING"
	}

	clone := g.sqlKey(expression, "clone")
	if clone != "" {
		clone = " " + clone
	}

	var propertiesExpression string
	if g.s.EXPRESSION_PRECEDES_PROPERTIES_CREATABLES.Has(kind) {
		propertiesExpression = expressionSQL + propertiesSQL
	} else {
		propertiesExpression = propertiesSQL + expressionSQL
	}

	expressionSQL = "CREATE" + modifiers + " " + kind + concurrently + existsSQL + " " + this +
		propertiesExpression + postexpressionPropsSQL + indexSQL + noSchemaBinding + clone
	return g.prependCtes(expression, expressionSQL)
}

func (g *Generator) sequencepropertiesSQL(expression *Expr) string {
	start := g.sqlKey(expression, "start")
	if start != "" {
		start = "START WITH " + start
	}
	increment := g.sqlKey(expression, "increment")
	if increment != "" {
		increment = " INCREMENT BY " + increment
	}
	minvalue := g.sqlKey(expression, "minvalue")
	if minvalue != "" {
		minvalue = " MINVALUE " + minvalue
	}
	maxvalue := g.sqlKey(expression, "maxvalue")
	if maxvalue != "" {
		maxvalue = " MAXVALUE " + maxvalue
	}
	owned := g.sqlKey(expression, "owned")
	if owned != "" {
		owned = " OWNED BY " + owned
	}

	cache := expression.Arg("cache")
	var cacheStr string
	if cache == nil {
		cacheStr = ""
	} else if b, ok := cache.(bool); ok && b {
		cacheStr = " CACHE"
	} else {
		cacheStr = " CACHE " + chunkAPyStr(cache)
	}

	options := g.expressions(expression, exprsOpts{key: "options", flat: true, sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}

	return strings.TrimLeftFunc(start+increment+minvalue+maxvalue+cacheStr+options+owned, pyIsSpaceRune)
}

func (g *Generator) triggerpropertiesSQL(expression *Expr) string {
	// timing is "" when the arg is absent, but stays nil when it is set to nil.
	var timing any = ""
	if expression.HasArgKey("timing") {
		timing = expression.Arg("timing")
	}
	var eventSQLs []string
	for _, event := range expression.ArgL("events") {
		eventSQLs = append(eventSQLs, g.sql(event))
	}
	events := strings.Join(eventSQLs, " OR ")
	timingEvents := ""
	if truthy(timing) || events != "" {
		timingEvents = pyStrip(chunkAPyStr(timing) + " " + events)
	}

	parts := []string{timingEvents, "ON", g.sqlKey(expression, "table")}

	if referencedTable := expression.Arg("referenced_table"); truthy(referencedTable) {
		parts = append(parts, "FROM", g.sql(referencedTable))
	}

	if deferrable := expression.Arg("deferrable"); truthy(deferrable) {
		parts = append(parts, chunkAPyStr(deferrable))
	}

	if initially := expression.Arg("initially"); truthy(initially) {
		parts = append(parts, "INITIALLY "+chunkAPyStr(initially))
	}

	if referencing := expression.Arg("referencing"); truthy(referencing) {
		parts = append(parts, g.sql(referencing))
	}

	if forEach := expression.Arg("for_each"); truthy(forEach) {
		parts = append(parts, "FOR EACH "+chunkAPyStr(forEach))
	}

	if when := expression.Arg("when"); truthy(when) {
		parts = append(parts, "WHEN ("+g.sql(when)+")")
	}

	parts = append(parts, g.sqlKey(expression, "execute"))

	return strings.Join(parts, g.sep1())
}

func (g *Generator) triggerreferencingSQL(expression *Expr) string {
	var parts []string

	if oldAlias := expression.Arg("old"); truthy(oldAlias) {
		parts = append(parts, "OLD TABLE AS "+g.sql(oldAlias))
	}

	if newAlias := expression.Arg("new"); truthy(newAlias) {
		parts = append(parts, "NEW TABLE AS "+g.sql(newAlias))
	}

	return "REFERENCING " + strings.Join(parts, " ")
}

func (g *Generator) triggereventSQL(expression *Expr) string {
	if expression.ArgB("columns") {
		return chunkAPyStr(expression.Arg("this")) + " OF " + g.expressions(expression, exprsOpts{key: "columns", flat: true})
	}

	return g.sqlKey(expression, "this")
}

func (g *Generator) cloneSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	shallow := ""
	if expression.ArgB("shallow") {
		shallow = "SHALLOW "
	}
	keyword := "CLONE"
	if expression.ArgB("copy") && g.s.SUPPORTS_TABLE_COPY {
		keyword = "COPY"
	}
	return shallow + keyword + " " + this
}

func (g *Generator) baseDescribeSQL(expression *Expr) string {
	style := ""
	if v := expression.Arg("style"); truthy(v) {
		style = " " + chunkAPyStr(v)
	}
	partition := g.sqlKey(expression, "partition")
	if partition != "" {
		partition = " " + partition
	}
	format := g.sqlKey(expression, "format")
	if format != "" {
		format = " " + format
	}
	asJSON := ""
	if expression.ArgB("as_json") {
		asJSON = " AS JSON"
	}

	return "DESCRIBE" + style + format + " " + g.sqlKey(expression, "this") + partition + asJSON
}

func (g *Generator) heredocSQL(expression *Expr) string {
	tag := g.sqlKey(expression, "tag")
	return "$" + tag + "$" + g.sqlKey(expression, "this") + "$" + tag + "$"
}

func (g *Generator) prependCtes(expression *Expr, sql_ string) string {
	with := g.sqlKey(expression, "with_")
	if with != "" {
		sql_ = with + g.sep1() + sql_
	}
	return sql_
}

func (g *Generator) withSQL(expression *Expr) string {
	// Inline function specifications get their own WITH, before the CTEs'
	udfs := g.expressions(expression, exprsOpts{key: "udfs", flat: true})
	if udfs != "" {
		udfs = "WITH " + udfs
	}

	sql := g.expressions(expression, exprsOpts{flat: true})
	recursive := ""
	if g.s.CTE_RECURSIVE_KEYWORD_REQUIRED && expression.ArgB("recursive") {
		recursive = "RECURSIVE "
	}
	search := g.sqlKey(expression, "search")
	if search != "" {
		search = " " + search
	}
	cycle := g.sqlKey(expression, "cycle")
	if cycle != "" {
		cycle = " " + cycle
	}

	if sql != "" {
		sql = "WITH " + recursive + sql + search + cycle
	}
	if udfs != "" && sql != "" {
		return udfs + " " + sql
	}
	return udfs + sql
}

func (g *Generator) baseCteSQL(expression *Expr) string {
	alias := expression.ArgE("alias")
	if alias != nil {
		alias.AddComments(expression.PopComments(), false)
	}

	aliasSQL := g.sqlKey(expression, "alias")

	materializedV := expression.Arg("materialized")
	materialized := ""
	if b, ok := materializedV.(bool); ok && !b {
		materialized = "NOT MATERIALIZED "
	} else if truthy(materializedV) {
		materialized = "MATERIALIZED "
	}

	keyExpressions := g.expressions(expression, exprsOpts{key: "key_expressions", flat: true})
	if keyExpressions != "" {
		keyExpressions = " USING KEY (" + keyExpressions + ")"
	}

	return aliasSQL + keyExpressions + " AS " + materialized + g.wrap(expression)
}

func (g *Generator) tablealiasSQL(expression *Expr) string {
	alias := g.sqlKey(expression, "this")
	columns := g.expressions(expression, exprsOpts{key: "columns", flat: true})
	if columns != "" {
		columns = "(" + columns + ")"
	}

	if columns != "" &&
		!g.s.SUPPORTS_TABLE_ALIAS_COLUMNS &&
		!(g.s.SUPPORTS_NAMED_CTE_COLUMNS && expression.Parent().IsA(KCTE)) {
		columns = ""
		g.unsupported("Named columns are not supported in table alias.")
	}

	if alias == "" && !g.d.S.UNNEST_COLUMN_ONLY {
		alias = g.nextName()
	}

	return alias + columns
}

func (g *Generator) bitstringSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if g.d.S.BIT_START != "" {
		return g.d.S.BIT_START + this + g.d.S.BIT_END
	}
	return chunkAPyIntBase(this, 2)
}

func (g *Generator) baseHexstringSQL(expression *Expr, binaryFunctionRepr string) string {
	this := g.sqlKey(expression, "this")
	isIntegerType := expression.ArgB("is_integer")

	if (isIntegerType && !g.d.S.HEX_STRING_IS_INTEGER_TYPE) ||
		(g.d.S.HEX_START == "" && binaryFunctionRepr == "") {
		// Integer representation will be returned if:
		// - The read dialect treats the hex value as integer literal but not the write
		// - The transpilation is not supported (write dialect hasn't set HEX_START or the param flag)
		return chunkAPyIntBase(this, 16)
	}

	if !isIntegerType {
		// Read dialect treats the hex value as BINARY/BLOB
		if binaryFunctionRepr != "" {
			// The write dialect supports the transpilation to its equivalent BINARY/BLOB
			return g.fn(binaryFunctionRepr, LiteralString(this))
		}
		if g.d.S.HEX_STRING_IS_INTEGER_TYPE {
			// The write dialect does not support the transpilation, it'll treat the hex value as INTEGER
			g.unsupported("Unsupported transpilation from BINARY/BLOB hex string")
		}
	}

	return g.d.S.HEX_START + this + g.d.S.HEX_END
}

// bytestringSQL casts to BINARY or VARCHAR when the parsed literal and the target dialect disagree
// on whether byte strings are bytes. The high bytes of a bytes literal are written as \xHH when
// the dialect reads hex escapes, so they are not re-read as UTF-8.
func (g *Generator) bytestringSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if g.d.S.BYTE_START != "" {
		escapeBackslash := len(g.d.T.NUMERIC_ESCAPES) > 0 || g.d.T.DROP_UNKNOWN_ESCAPES
		escapedByteString := g.escapeStr(this, escapeBackslash, g.d.S.BYTE_END, g.escapedByteQuoteEnd, true)
		isBytes := expression.ArgB("is_bytes")
		if _, hex := g.d.T.NUMERIC_ESCAPES["x"]; isBytes && hex && g.d.S.BYTE_STRINGS_SUPPORT_ESCAPED_SEQUENCES {
			escapedByteString = escapeHighBytes(escapedByteString)
		}
		delimitedByteString := g.d.S.BYTE_START + escapedByteString + g.d.S.BYTE_END
		if isBytes && !g.d.S.BYTE_STRING_IS_BYTES_TYPE {
			return g.sql(chunkACast(delimitedByteString, DT_BINARY, g.d))
		}
		if !isBytes && g.d.S.BYTE_STRING_IS_BYTES_TYPE {
			return g.sql(chunkACast(delimitedByteString, DT_VARCHAR, g.d))
		}

		return delimitedByteString
	}

	if slices.Contains(g.d.T.STRING_ESCAPES, "\\") {
		return g.sql(LiteralString(this))
	}

	g.unsupported("Byte strings are not supported for " + g.d.ClassName)
	return ""
}

func escapeHighBytes(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 0x80 && r <= 0xFF {
			fmt.Fprintf(&b, "\\x%02x", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

var chunkAEscapedUnicodeRE = regexp.MustCompile(`\\(\p{Nd}+)`)

func (g *Generator) unicodestringSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	escape := expression.ArgE("escape")

	var escapeSubstitute func(groups []string) string
	var leftQuote, rightQuote string
	if g.d.S.UNICODE_START != "" {
		escapeSubstitute = func(groups []string) string { return "\\" + groups[1] }
		leftQuote, rightQuote = g.d.S.UNICODE_START, g.d.S.UNICODE_END
	} else {
		escapeSubstitute = func(groups []string) string { return "\\u" + groups[1] }
		leftQuote, rightQuote = g.d.S.QUOTE_START, g.d.S.QUOTE_END
	}

	var escapePattern *regexp.Regexp
	var escapeSQL string
	if escape != nil {
		// The escape's name is used as a raw regex fragment, followed by the captured digits.
		re, err := regexp.Compile(escape.Name() + `(\p{Nd}+)`)
		if err != nil {
			panic(&ValueError{Msg: err.Error()})
		}
		escapePattern = re
		if g.s.SUPPORTS_UESCAPE {
			escapeSQL = " UESCAPE " + g.sql(escape)
		}
	} else {
		escapePattern = chunkAEscapedUnicodeRE
		escapeSQL = ""
	}

	if g.d.S.UNICODE_START == "" || (escape != nil && !g.s.SUPPORTS_UESCAPE) {
		repl := escapeSubstitute
		if g.s.UNICODE_SUBSTITUTE != nil {
			repl = func(groups []string) string { return g.s.UNICODE_SUBSTITUTE(g, groups[1]) }
		}
		this = chunkARegexSub(escapePattern, this, repl)
	}

	if g.d.S.UNICODE_START != "" {
		// A Unicode literal only escapes its delimiter by doubling it; the escape character
		// introduces a code point, so the dialect's ordinary string escapes don't apply here
		this = strings.ReplaceAll(g.replaceLineBreaks(this), rightQuote, rightQuote+rightQuote)
	} else {
		this = g.escapeStr(this, false, "", "", false)
	}

	return leftQuote + this + rightQuote + escapeSQL
}

func (g *Generator) rawstringSQL(expression *Expr) string {
	str := expression.ThisS()
	if slices.Contains(g.d.T.STRING_ESCAPES, "\\") {
		str = strings.ReplaceAll(str, "\\", "\\\\")
	}

	str = g.escapeStr(str, false, "", "", false)
	return g.d.S.QUOTE_START + str + g.d.S.QUOTE_END
}

func (g *Generator) datatypeparamSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	specifier := g.sqlKey(expression, "expression")
	if specifier != "" && g.s.DATA_TYPE_SPECIFIERS_ALLOWED {
		specifier = " " + specifier
	} else {
		specifier = ""
	}
	return this + specifier
}

func (g *Generator) datatypeParamBoundLimiter(expression *Expr, typeValue DType, defaults []int, bounds []int) *Expr {
	params := expression.Expressions()

	if len(params) == 0 {
		if len(defaults) > 0 {
			ps := make([]*Expr, 0, len(defaults))
			for _, d := range defaults {
				ps = append(ps, New(KDataTypeParam, "this", LiteralInt(d)))
			}
			expression.Set("expressions", ps)
		}
		return expression
	}

	if len(bounds) == 0 {
		return expression
	}

	for i, param := range params {
		if i >= len(bounds) {
			continue
		}
		bound := bounds[i]

		paramValue := param
		if param.IsA(KDataTypeParam) {
			paramValue = param.This()
		}
		if paramValue.IsA(KLiteral) && paramValue.IsNumber() {
			v, _ := paramValue.toPyNumber()
			if v == nil {
				panic(&ValueError{Msg: "[<class 'decimal.ConversionSyntax'>]"})
			}
			iv, _ := v.Int(nil)
			if iv.Cmp(big.NewInt(int64(bound))) > 0 {
				g.unsupported(fmt.Sprintf(
					"%s parameter %s exceeds %s's maximum of %d; capping",
					dtypeValues[typeValue], paramValue.Name(), g.d.ClassName, bound,
				))
				// Assigned straight into the slice: the new param's parent is not set.
				params[i] = New(KDataTypeParam, "this", LiteralInt(bound))
			}
		}
	}

	return expression
}

func (g *Generator) baseDatatypeSQL(expression *Expr) string {
	nested := ""
	values := ""

	exprNested := expression.ArgB("nested")
	typeValueRaw := expression.Arg("this")
	typeValue, isDType := typeValueRaw.(DType)

	if !exprNested && isDType {
		if settings, ok := g.s.TYPE_PARAM_SETTINGS[typeValue]; ok && len(settings) > 0 {
			var defaults, bounds []int
			defaults = settings[0]
			if len(settings) > 1 {
				bounds = settings[1]
			}
			expression = g.datatypeParamBoundLimiter(expression, typeValue, defaults, bounds)
		}
	}

	var interior string
	if exprNested && g.pretty {
		interior = g.expressions(expression, exprsOpts{dynamic: true, newLine: true, skipFirst: true, skipLast: true})
	} else {
		interior = g.expressions(expression, exprsOpts{flat: true})
	}

	if isDType && chunkAInDTypeSet(g.s.UNSUPPORTED_TYPES, typeValue) {
		g.unsupported("Data type " + dtypeValues[typeValue] + " is not supported when targeting " + g.d.ClassName)
	}

	var typeSQL string
	if isDType && typeValue == DT_USERDEFINED && expression.ArgB("kind") {
		typeSQL = g.sqlKey(expression, "kind")
	} else if isDType && typeValue == DT_CHARACTER_SET {
		char := "CHAR"
		if interior != "" {
			char = "CHAR(" + interior + ")"
		}
		return char + " CHARACTER SET " + g.sqlKey(expression, "kind")
	} else if isDType {
		if mapped, ok := g.s.TYPE_MAPPING[typeValue]; ok {
			typeSQL = mapped
		} else {
			typeSQL = dtypeValues[typeValue]
		}
	} else {
		typeSQL = chunkAPyStr(typeValueRaw)
	}

	if interior != "" {
		if exprNested {
			nested = g.s.STRUCT_DELIMITER[0] + interior + g.s.STRUCT_DELIMITER[1]
			if expression.Arg("values") != nil {
				d0, d1 := "(", ")"
				if isDType && typeValue == DT_ARRAY {
					d0, d1 = "[", "]"
				}
				values = g.expressions(expression, exprsOpts{key: "values", flat: true})
				values = d0 + values + d1
			}
		} else if isDType && typeValue == DT_INTERVAL {
			nested = " " + interior
		} else {
			nested = "(" + interior + ")"
		}
	}

	typeSQL = typeSQL + nested + values
	if g.s.TZ_TO_WITH_TIME_ZONE && isDType && (typeValue == DT_TIMETZ || typeValue == DT_TIMESTAMPTZ) {
		typeSQL = typeSQL + " WITH TIME ZONE"
	}

	collate := g.sqlKey(expression, "collate")
	if collate != "" {
		typeSQL = typeSQL + " COLLATE " + collate
	}

	return typeSQL
}

func (g *Generator) directorySQL(expression *Expr) string {
	local := ""
	if expression.ArgB("local") {
		local = "LOCAL "
	}
	rowFormat := g.sqlKey(expression, "row_format")
	if rowFormat != "" {
		rowFormat = " " + rowFormat
	}
	return local + "DIRECTORY " + g.sqlKey(expression, "this") + rowFormat
}

func (g *Generator) baseDeleteSQL(expression *Expr) string {
	hint := g.sqlKey(expression, "hint")
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " FROM " + this
	}
	using := g.expressions(expression, exprsOpts{key: "using"})
	if using != "" {
		using = " USING " + using
	}
	cluster := g.sqlKey(expression, "cluster")
	if cluster != "" {
		cluster = " " + cluster
	}
	where := g.sqlKey(expression, "where")
	returning := g.sqlKey(expression, "returning")
	order := g.sqlKey(expression, "order")
	limit := g.sqlKey(expression, "limit")
	tables := g.expressions(expression, exprsOpts{key: "tables"})
	if tables != "" {
		tables = " " + tables
	}
	var expressionSQL string
	if g.s.RETURNING_END {
		expressionSQL = this + using + cluster + where + returning + order + limit
	} else {
		expressionSQL = returning + this + using + cluster + where + order + limit
	}
	return g.prependCtes(expression, "DELETE"+hint+tables+expressionSQL)
}

func (g *Generator) baseDropSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if tables := g.expressions(expression, exprsOpts{key: "tables", flat: true}); tables != "" {
		this += ", " + tables
	}
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " (" + expressions + ")"
	}
	kindV := expression.Arg("kind")
	if s, ok := kindV.(string); ok {
		if mapped := g.d.S.INVERSE_CREATABLE_KIND_MAPPING[s]; mapped != "" {
			kindV = mapped
		}
	}
	kind := chunkAPyStr(kindV)
	iceberg := ""
	if expression.ArgB("iceberg") && g.s.SUPPORTS_DROP_ALTER_ICEBERG_PROPERTY {
		iceberg = " ICEBERG"
	}
	existsSQL := " "
	if expression.ArgB("exists") {
		existsSQL = " IF EXISTS "
	}
	concurrentlySQL := ""
	if expression.ArgB("concurrently") {
		concurrentlySQL = " CONCURRENTLY"
	}
	onCluster := g.sqlKey(expression, "cluster")
	if onCluster != "" {
		onCluster = " " + onCluster
	}
	temporary := ""
	if expression.ArgB("temporary") {
		temporary = " TEMPORARY"
	}
	materialized := ""
	if expression.ArgB("materialized") {
		materialized = " MATERIALIZED"
	}
	cascade := ""
	if expression.ArgB("cascade") {
		cascade = " CASCADE"
	}
	restrict := ""
	if expression.ArgB("restrict") {
		restrict = " RESTRICT"
	}
	constraints := ""
	if expression.ArgB("constraints") {
		constraints = " CONSTRAINTS"
	}
	purge := ""
	if expression.ArgB("purge") {
		purge = " PURGE"
	}
	sync := ""
	if expression.ArgB("sync") {
		sync = " SYNC"
	}
	force := ""
	if expression.ArgB("force") {
		force = " FORCE"
	}
	return "DROP" + temporary + materialized + iceberg + " " + kind + concurrentlySQL + existsSQL + this +
		onCluster + expressions + cascade + restrict + constraints + purge + sync + force
}

func (g *Generator) setOperation(expression *Expr) string {
	opType := expression.Kind()
	opName := pyUpper(expression.Key())

	distinct := chunkATri(expression.Arg("distinct"))
	if b, ok := expression.Arg("distinct").(bool); ok && !b &&
		(opType == KExcept || opType == KIntersect) &&
		!g.s.EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE {
		g.unsupported(opName + " ALL is not supported")
	}

	defaultDistinct, ok := g.d.S.SET_OP_DISTINCT_BY_DEFAULT[opType]
	if !ok {
		panic(&ValueError{Msg: opType.Name()})
	}

	if distinct == TriNone {
		distinct = defaultDistinct
		if distinct == TriNone {
			g.unsupported(opName + " requires DISTINCT or ALL to be specified")
		}
	}

	var distinctOrAll string
	if distinct == defaultDistinct {
		distinctOrAll = ""
	} else if distinct == TriTrue {
		distinctOrAll = " DISTINCT"
	} else {
		distinctOrAll = " ALL"
	}

	var sideKindParts []string
	for _, s := range []string{expression.SideText(), expression.KindText()} {
		if s != "" {
			sideKindParts = append(sideKindParts, s)
		}
	}
	sideKind := strings.Join(sideKindParts, " ")
	if sideKind != "" {
		sideKind = sideKind + " "
	}

	byName := ""
	if expression.ArgB("by_name") {
		byName = " BY NAME"
	}
	on := g.expressions(expression, exprsOpts{key: "on", flat: true})
	if on != "" {
		on = " ON (" + on + ")"
	}

	return sideKind + opName + distinctOrAll + byName + on
}

// baseSetOperations renders a chain of set operations iteratively, so deeply nested chains do
// not recurse.
func (g *Generator) baseSetOperations(expression *Expr) string {
	if !g.s.SET_OP_MODIFIERS {
		if expression.ArgB("limit") || expression.ArgB("order") || expression.ArgB("offset") {
			sel := g.wrapSetOperation(expression)
			for _, arg := range []string{"limit", "order", "offset"} {
				if value := expression.ArgE(arg); value != nil {
					sel.Set(arg, value.Pop())
				}
			}
			return g.sql(sel)
		}
	}

	var sqls []string
	stack := []any{expression}

	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		n, _ := node.(*Expr)
		if n.IsA(KSetOperation) {
			stack = append(stack, n.Arg("expression"))
			stack = append(stack, g.maybeCommentFull(g.setOperation(n), nil, n.Comments(), true, true))
			stack = append(stack, g.setOperationLeft(n))
		} else {
			if !g.s.SET_OP_LIMITS && n.IsA(KSelect) && n.ArgB("limit") {
				n = n.QuerySubquery(nil, false)
				if !g.s.SET_OP_PARENTHESIZED_OPERANDS {
					n = SelectExpr(Star()).SelectFrom(n, false)
				}
				node = n
			} else if !g.s.SET_OP_PARENTHESIZED_OPERANDS && n.IsA(KSubquery) && n.UnnestSubquery().IsA(KSetOperation) {
				// Without parenthesized operands, a grouped set operation is selected from instead
				node = SelectExpr(Star()).SelectFrom(n, false)
			}
			sqls = append(sqls, g.sql(node))
		}
	}

	this := strings.Join(sqls, g.sep1())
	this = g.queryModifiers(expression, this)
	return g.prependCtes(expression, this)
}

// setOperationLeft returns the left operand of a set operation to render. Where operands can't
// be parenthesized but set operations associate left to right, the subqueries grouping a left
// set operation are dropped (keeping their comments), as the grouping is implicit; one with CTEs
// or modifiers of its own keeps them.
func (g *Generator) setOperationLeft(expression *Expr) *Expr {
	left := expression.This()
	if g.s.SET_OP_PARENTHESIZED_OPERANDS || g.d.S.INTERSECT_BINDS_TIGHTER_THAN_UNION_AND_EXCEPT {
		return left
	}

	var wrappers []*Expr
	inner := left
	for inner.IsA(KSubquery) && inner.IsWrapper() {
		wrappers = append(wrappers, inner)
		inner = inner.This()
	}

	if !inner.IsA(KSetOperation) || inner.ArgB("with_") {
		return left
	}
	for _, key := range QUERY_MODIFIERS {
		if inner.ArgB(key) {
			return left
		}
	}
	for _, wrapper := range wrappers {
		inner.AddComments(wrapper.PopComments(), false)
	}
	return inner
}

// wrapSetOperation wraps expression, without copying it, in SELECT * FROM (...) AS _l_0.
func (g *Generator) wrapSetOperation(expression *Expr) *Expr {
	sub := New(KSubquery, "this", expression, "alias", New(KTableAlias, "this", ToIdentifier("_l_0", nil)))
	sel := New(KSelect)
	sel.Set("from_", New(KFrom, "this", sub))
	sel.Set("expressions", []*Expr{Star()})
	return g.moveCtesToTopLevel(sel)
}

func (g *Generator) fetchSQL(expression *Expr) string {
	direction := ""
	if v := expression.Arg("direction"); truthy(v) {
		direction = " " + chunkAPyStr(v)
	}
	count := g.sqlKey(expression, "count")
	if count != "" {
		count = " " + count
	}
	limitOptions := g.sqlKey(expression, "limit_options")
	if limitOptions == "" {
		limitOptions = " ROWS ONLY"
	}
	return g.seg("FETCH") + direction + count + limitOptions
}

func (g *Generator) limitoptionsSQL(expression *Expr) string {
	percent := ""
	if expression.ArgB("percent") {
		percent = " PERCENT"
	}
	rows := ""
	if expression.ArgB("rows") {
		rows = " ROWS"
	}
	withTies := ""
	if expression.ArgB("with_ties") {
		withTies = " WITH TIES"
	}
	if withTies == "" && rows != "" {
		withTies = " ONLY"
	}
	return percent + rows + withTies
}

func (g *Generator) baseFilterSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	where := pyStrip(g.sqlKey(expression, "expression"))
	return this + " FILTER(" + where + ")"
}

func (g *Generator) perSQL(expression *Expr) string {
	keys := make([]string, 0, len(expression.Expressions()))
	for _, key := range expression.Expressions() {
		if key.IsA(KOrdered) && key.ArgE("with_fill") != nil {
			keys = append(keys, g.sqlKey(key, "this")+" "+g.sqlKey(key, "with_fill"))
		} else {
			keys = append(keys, g.sql(key))
		}
	}
	return g.sqlKey(expression, "this") + " PER (" + strings.Join(keys, ", ") + ")"
}

func (g *Generator) baseHintSQL(expression *Expr) string {
	if !g.s.QUERY_HINTS {
		g.unsupported("Hints are not supported")
		return ""
	}

	return " /*+ " + pyStrip(g.expressions(expression, exprsOpts{sep: strp2(g.s.QUERY_HINT_SEP)})) + " */"
}

func (g *Generator) indexparametersSQL(expression *Expr) string {
	using := g.sqlKey(expression, "using")
	if using != "" {
		using = " USING " + using
	}
	columns := g.expressions(expression, exprsOpts{key: "columns", flat: true})
	if columns != "" {
		columns = "(" + columns + ")"
	}
	partitionBy := g.expressions(expression, exprsOpts{key: "partition_by", flat: true})
	if partitionBy != "" {
		partitionBy = " PARTITION BY " + partitionBy
	}
	where := g.sqlKey(expression, "where")
	include := g.expressions(expression, exprsOpts{key: "include", flat: true})
	if include != "" {
		include = " INCLUDE (" + include + ")"
	}
	withStorage := g.expressions(expression, exprsOpts{key: "with_storage", flat: true})
	if withStorage != "" {
		withStorage = " WITH (" + withStorage + ")"
	}
	tablespace := g.sqlKey(expression, "tablespace")
	if tablespace != "" {
		// an index's own parameters name the tablespace without USING INDEX
		if expression.Parent().IsA(KIndex) {
			tablespace = " TABLESPACE " + tablespace
		} else {
			tablespace = " USING INDEX TABLESPACE " + tablespace
		}
	}
	on := g.sqlKey(expression, "on")
	if on != "" {
		on = " ON " + on
	}

	return using + columns + include + withStorage + tablespace + partitionBy + where + on
}

func (g *Generator) indexSQL(expression *Expr) string {
	unique := ""
	if expression.ArgB("unique") {
		unique = "UNIQUE "
	}
	primary := ""
	if expression.ArgB("primary") {
		primary = "PRIMARY "
	}
	amp := ""
	if expression.ArgB("amp") {
		amp = "AMP "
	}
	name := g.sqlKey(expression, "this")
	if name != "" {
		name = name + " "
	}
	table := g.sqlKey(expression, "table")
	if table != "" {
		table = g.s.INDEX_ON + " " + table
	}

	index := ""
	if table == "" {
		index = "INDEX "
	}

	params := g.sqlKey(expression, "params")
	return unique + primary + amp + index + name + table + params
}

func (g *Generator) baseDynamicidentifierSQL(expression *Expr) string {
	this := expression.This()
	if this != nil && this.IsString() {
		parsed, err := MustDialect("").ParseOne(this.Name(), nil)
		if err != nil {
			panic(genPanic{err})
		}
		resolved, err := g.d.Generate(parsed, nil)
		if err != nil {
			panic(genPanic{err})
		}
		if expression.HasArgKey("expressions") {
			// `IDENTIFIER(...)` invoked as a function, e.g. `IDENTIFIER('my_func')(1, 2)`
			// We can't safely emit the call to other dialects since name/arg semantics may differ
			g.unsupported("Transpiling dynamically-invoked IDENTIFIER() functions is unsupported")
		}
		return resolved
	}
	g.unsupported("IDENTIFIER() with non-literal arguments is not supported")
	return g.fn("IDENTIFIER", this)
}

func (g *Generator) baseIdentifierSQL(expression *Expr) string {
	text := expression.Name()
	lower := pyLower(text)
	quoted := expression.ArgB("quoted")
	if g.normalize && !quoted {
		text = lower
	}
	for _, seq := range g.d.S.IDENTIFIER_ESCAPED_SEQUENCES {
		text = strings.ReplaceAll(text, seq[0], seq[1])
	}
	text = strings.ReplaceAll(text, g.identifierEnd, g.escapedIdentifierEnd)
	firstChar := ""
	if r := []rune(text); len(r) > 0 {
		firstChar = string(r[0])
	}
	if quoted ||
		g.d.CanQuote(expression, g.identify) ||
		g.s.RESERVED_KEYWORDS.Has(lower) ||
		(!g.d.S.IDENTIFIERS_CAN_START_WITH_DIGIT && pyIsDigit(firstChar)) {
		text = g.identifierStart + g.replaceLineBreaks(text) + g.identifierEnd
	}
	return text
}

func (g *Generator) baseHexSQL(expression *Expr) string {
	text := g.fn(g.s.HEX_FUNC, g.sqlKey(expression, "this"))
	if g.d.S.HEX_LOWERCASE {
		text = g.fn("LOWER", text)
	}

	return text
}

func (g *Generator) lowerhexSQL(expression *Expr) string {
	text := g.fn(g.s.HEX_FUNC, g.sqlKey(expression, "this"))
	if !g.d.S.HEX_LOWERCASE {
		text = g.fn("LOWER", text)
	}
	return text
}

func (g *Generator) inputoutputformatSQL(expression *Expr) string {
	inputFormat := g.sqlKey(expression, "input_format")
	if inputFormat != "" {
		inputFormat = "INPUTFORMAT " + inputFormat
	}
	outputFormat := g.sqlKey(expression, "output_format")
	if outputFormat != "" {
		outputFormat = "OUTPUTFORMAT " + outputFormat
	}
	return inputFormat + g.sep1() + outputFormat
}

func (g *Generator) nationalSQL(expression *Expr, prefix string) string {
	str := g.sql(LiteralString(expression.Name()))
	return prefix + str
}

func (g *Generator) basePartitionSQL(expression *Expr) string {
	partitionKeyword := "PARTITION"
	if expression.ArgB("subpartition") {
		partitionKeyword = "SUBPARTITION"
	}
	return partitionKeyword + "(" + g.expressions(expression, exprsOpts{flat: true}) + ")"
}

func chunkAPyStr(v any) string {
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
		return strconv.Itoa(x)
	case *Expr:
		if x == nil {
			return "None"
		}
		return exprSQL(x)
	case DType:
		return "DType." + dtypeNames[x]
	}
	return fmt.Sprint(v)
}

func chunkATri(v any) Tri {
	switch x := v.(type) {
	case nil:
		return TriNone
	case bool:
		if x {
			return TriTrue
		}
		return TriFalse
	}
	if truthy(v) {
		return TriTrue
	}
	return TriFalse
}

// chunkAPyIntBase parses s as a base-2 or base-16 integer (optional sign and 0b / 0x prefix,
// single underscores between digits) and returns it in base 10. Invalid input panics with a
// ValueError; the message keeps its historical wording ("invalid literal for int() with base N").
func chunkAPyIntBase(s string, base int) string {
	fail := func() {
		panic(&ValueError{Msg: fmt.Sprintf("invalid literal for int() with base %d: %s", base, pyRepr(s))})
	}
	t := pyStrip(s)
	sign := ""
	if t != "" && (t[0] == '+' || t[0] == '-') {
		if t[0] == '-' {
			sign = "-"
		}
		t = t[1:]
	}
	lt := strings.ToLower(t)
	if (base == 16 && strings.HasPrefix(lt, "0x")) || (base == 2 && strings.HasPrefix(lt, "0b")) {
		t = t[2:]
		// A single underscore is allowed right after the base prefix.
		if strings.HasPrefix(t, "_") {
			t = t[1:]
		}
	}
	if t == "" || t[0] == '+' || t[0] == '-' ||
		strings.HasPrefix(t, "_") || strings.HasSuffix(t, "_") || strings.Contains(t, "__") {
		fail()
	}
	t = strings.ReplaceAll(t, "_", "")
	n, ok := new(big.Int).SetString(t, base)
	if !ok {
		fail()
	}
	if sign == "-" {
		n.Neg(n)
	}
	return n.String()
}

// chunkARegexSub replaces every match of re in s with repl(groups), where groups[0] is the whole
// match and groups that did not participate are "".
func chunkARegexSub(re *regexp.Regexp, s string, repl func(groups []string) string) string {
	var b strings.Builder
	last := 0
	for _, m := range re.FindAllStringSubmatchIndex(s, -1) {
		b.WriteString(s[last:m[0]])
		groups := make([]string, len(m)/2)
		for i := range groups {
			if m[2*i] >= 0 {
				groups[i] = s[m[2*i]:m[2*i+1]]
			}
		}
		b.WriteString(repl(groups))
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

func chunkAInDTypeSet(set any, d DType) bool {
	switch s := set.(type) {
	case DTypeSet:
		return s.Has(d)
	case StrSet:
		return s.Has(dtypeValues[d])
	}
	return false
}

func chunkACast(sql string, to DType, d *Dialect) *Expr {
	expr, err := d.ParseOne(sql, nil)
	if err != nil {
		panic(genPanic{err})
	}
	dataType := NewDataType(to)

	// dont re-cast if the expression is already a cast to the correct type
	if expr.IsA(KCast) {
		typeName := func(t DType) string {
			if m, ok := d.G.TYPE_MAPPING[t]; ok {
				return m
			}
			return dtypeValues[t]
		}
		existing := expr.ArgE("to").DTypeOf()
		// For a plain DType target, types with the same rendered name are equivalent.
		if typeName(existing) == typeName(to) {
			return expr
		}
	}

	c := New(KCast, "this", expr, "to", dataType)
	c.SetType(dataType)
	return c
}
