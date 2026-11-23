package sqlengine

import (
	"fmt"
	"strconv"
	"strings"
)

// GenFunc renders one expression kind (TRANSFORMS entries and per-kind generator methods).
type GenFunc func(g *Generator, e *Expr) string

// propLocations groups properties by the location in a statement where they render.
type propLocations map[string][]*Expr

// GeneratorSettings is the fully-resolved generator configuration of a dialect.
type GeneratorSettings struct {
	*GeneratorData

	// TRANSFORMS maps exact kinds to their rendering functions.
	TRANSFORMS map[Kind]GenFunc
	methods    map[Kind]GenFunc
	// dispatch merges TRANSFORMS and methods, with TRANSFORMS taking precedence.
	dispatch map[Kind]GenFunc

	AFTER_HAVING_MODIFIER_TRANSFORMS      map[string]GenFunc
	AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS []string
	UNICODE_SUBSTITUTE                    func(g *Generator, match string) string

	h generatorHooks
}

func (s *GeneratorSettings) buildDispatch() {
	s.dispatch = make(map[Kind]GenFunc, len(s.TRANSFORMS)+len(s.methods))
	for k, f := range s.TRANSFORMS {
		if f != nil {
			s.dispatch[k] = f
		}
	}
	for k, f := range s.methods {
		if _, ok := s.dispatch[k]; !ok && f != nil {
			s.dispatch[k] = f
		}
	}
}

// GenerateOptions configures a Generator; zero values select the defaults.
type GenerateOptions struct {
	Pretty bool
	// Identify: "" or "false" (default), "true", "safe", "unsafe".
	Identify           string
	Normalize          bool
	Pad                int
	Indent             int
	NormalizeFunctions *string
	UnsupportedLevel   *ErrorLevel
	MaxUnsupported     int
	LeadingComma       bool
	MaxTextWidth       int
	NoComments         bool
}

// Generator renders expression trees as SQL text for one dialect.
type Generator struct {
	d *Dialect
	s *GeneratorSettings

	pretty             bool
	identify           string
	normalize          bool
	pad                int
	indentSize         int
	normalizeFunctions string
	unsupportedLevel   ErrorLevel
	maxUnsupported     int
	leadingComma       bool
	maxTextWidth       int
	comments           bool

	unsupportedMessages []string

	escapedQuoteEnd               string
	escapedByteQuoteEnd           string
	escapedIdentifierEnd          string
	nextNameIndex                 int
	identifierStart               string
	identifierEnd                 string
	quoteJSONPathKeyUsingBrackets bool
}

func (d *Dialect) NewGenerator(opts *GenerateOptions) *Generator {
	g := &Generator{
		d:                d,
		s:                d.G,
		identify:         "false",
		pad:              2,
		indentSize:       2,
		unsupportedLevel: ErrorLevelWarn,
		maxUnsupported:   3,
		maxTextWidth:     80,
		comments:         true,
	}
	if opts != nil {
		g.pretty = opts.Pretty
		if opts.Identify != "" {
			g.identify = opts.Identify
		}
		g.normalize = opts.Normalize
		if opts.Pad > 0 {
			g.pad = opts.Pad
		}
		if opts.Indent > 0 {
			g.indentSize = opts.Indent
		}
		if opts.UnsupportedLevel != nil {
			g.unsupportedLevel = *opts.UnsupportedLevel
		}
		if opts.MaxUnsupported > 0 {
			g.maxUnsupported = opts.MaxUnsupported
		}
		g.leadingComma = opts.LeadingComma
		if opts.MaxTextWidth > 0 {
			g.maxTextWidth = opts.MaxTextWidth
		}
		g.comments = !opts.NoComments
	}
	g.normalizeFunctions = d.S.NORMALIZE_FUNCTIONS
	if opts != nil && opts.NormalizeFunctions != nil {
		g.normalizeFunctions = *opts.NormalizeFunctions
	}
	gs := d.generatorStrings()
	g.escapedQuoteEnd = gs.escapedQuoteEnd
	g.escapedByteQuoteEnd = gs.escapedByteQuoteEnd
	g.escapedIdentifierEnd = gs.escapedIdentifierEnd
	g.identifierStart = d.S.IDENTIFIER_START
	g.identifierEnd = d.S.IDENTIFIER_END
	g.quoteJSONPathKeyUsingBrackets = true
	if g.s.h.init != nil {
		g.s.h.init(g)
	}
	return g
}

func (g *Generator) nextName() string {
	s := "_t" + strconv.Itoa(g.nextNameIndex)
	g.nextNameIndex++
	return s
}

func nameSequence(prefix string) func() string {
	i := 0
	return func() string {
		s := fmt.Sprintf("%s%d", prefix, i)
		i++
		return s
	}
}

type genPanic struct{ err error }

// Generate renders e as SQL (copying it first when copy is set) and returns panics raised while
// rendering as errors.
func (g *Generator) Generate(e *Expr, copy bool) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case genPanic:
				err = x.err
			case *ValueError:
				err = x
			case *UnsupportedError:
				err = x
			case parsePanic:
				err = x.err
			default:
				err = internalError(x)
			}
		}
	}()
	return g.generate(e, copy), nil
}

func (g *Generator) generate(e *Expr, copy bool) string { return g.s.h.generate(g, e, copy) }

func (g *Generator) baseGenerate(e *Expr, copy bool) string {
	if copy {
		e = e.Copy()
	}
	e = g.preprocess(e)
	g.unsupportedMessages = nil
	sql := pyStrip(g.sql(e))
	if g.pretty {
		sql = strings.ReplaceAll(sql, g.s.SENTINEL_LINE_BREAK, "\n")
	}
	if g.unsupportedLevel == ErrorLevelIgnore {
		return sql
	}
	if g.unsupportedLevel == ErrorLevelWarn {
		for _, m := range g.unsupportedMessages {
			_ = m // Warnings are not logged; the messages are dropped.
		}
	} else if g.unsupportedLevel == ErrorLevelRaise && len(g.unsupportedMessages) > 0 {
		errs := make([]error, len(g.unsupportedMessages))
		for i, m := range g.unsupportedMessages {
			errs[i] = fmt.Errorf("%s", m)
		}
		panic(&UnsupportedError{Msg: concatMessages(errs, g.maxUnsupported)})
	}
	return sql
}

// preprocess moves nested CTEs to the top level and ensures boolean conditions where the dialect
// requires them.
func (g *Generator) preprocess(e *Expr) *Expr {
	e = g.moveCtesToTopLevel(e)
	if g.s.ENSURE_BOOLS {
		e = transformEnsureBools(e)
	}
	return e
}

func (g *Generator) moveCtesToTopLevel(e *Expr) *Expr {
	if e.parent == nil && g.s.EXPRESSIONS_WITHOUT_NESTED_CTES.Has(e.kind) {
		nested := false
		for w := range e.FindAll(KWith) {
			if w.parent != e {
				nested = true
				break
			}
		}
		if nested {
			e = transformMoveCtesToTopLevel(e)
		}
	}
	return e
}

func (g *Generator) unsupported(message string) {
	if g.unsupportedLevel == ErrorLevelImmediate {
		panic(&UnsupportedError{Msg: message})
	}
	g.unsupportedMessages = append(g.unsupportedMessages, message)
}

func (g *Generator) sep(sep string) string {
	if g.pretty {
		return pyStrip(sep) + "\n"
	}
	return sep
}

func (g *Generator) sep1() string { return g.sep(" ") }

func (g *Generator) seg(sql string) string { return g.sep(" ") + sql }

func (g *Generator) segSep(sql, sep string) string { return g.sep(sep) + sql }

func (g *Generator) sanitizeComment(comment string) string {
	r := []rune(comment)
	if len(r) > 0 && pyStrip(string(r[0])) != "" {
		comment = " " + comment
	}
	r = []rune(comment)
	if len(r) > 0 && pyStrip(string(r[len(r)-1])) != "" {
		comment = comment + " "
	}
	comment = strings.ReplaceAll(comment, "*/", "* /")
	comment = strings.ReplaceAll(comment, "/*", "/ *")
	return comment
}

func (g *Generator) maybeComment(sql string, e *Expr) string {
	return g.maybeCommentFull(sql, e, nil, false, false)
}

func (g *Generator) maybeCommentC(sql string, e *Expr, comments []string) string {
	return g.maybeCommentFull(sql, e, comments, true, false)
}

// maybeCommentFull attaches comments to sql as /* ... */ blocks, before sql when separated or for
// kinds with separated comments, after it otherwise. hasComments distinguishes an explicit
// comments list from using e's comments.
func (g *Generator) maybeCommentFull(sql string, e *Expr, comments []string, hasComments bool, separated bool) string {
	var cs []string
	if g.comments {
		if hasComments {
			cs = comments
		} else if e != nil {
			cs = e.Comments()
		}
	}
	if len(cs) == 0 || (e != nil && e.IsA(g.s.EXCLUDE_COMMENTS...)) {
		return sql
	}
	var list []string
	for _, c := range cs {
		if c != "" {
			list = append(list, "/*"+g.replaceLineBreaks(g.sanitizeComment(c))+"*/")
		}
	}
	if len(list) == 0 {
		return sql
	}
	if separated || (e != nil && e.IsA(g.s.WITH_SEPARATED_COMMENTS...)) {
		commentsSQL := strings.Join(list, g.sep(" "))
		if sql == "" || pyIsSpaceRune([]rune(sql)[0]) {
			return g.sep(" ") + commentsSQL + sql
		}
		return commentsSQL + g.sep(" ") + sql
	}
	return sql + " " + strings.Join(list, " ")
}

// wrap renders x in parentheses: queries in full, other expressions by their this argument and
// strings as is.
func (g *Generator) wrap(x any) string {
	var thisSQL string
	if e, ok := x.(*Expr); ok && e.IsA(KSelect, KSetOperation) {
		thisSQL = g.sql(e)
	} else if e, ok := x.(*Expr); ok {
		thisSQL = g.sqlKey(e, "this")
	} else if s, ok := x.(string); ok {
		thisSQL = s
	}
	if thisSQL == "" {
		return "()"
	}
	thisSQL = g.indent(thisSQL, 1, 0, false, false)
	return "(" + g.sep("") + thisSQL + g.segSep(")", "")
}

func (g *Generator) noIdentify(fn func() string) string {
	original := g.identify
	g.identify = "false"
	result := fn()
	g.identify = original
	return result
}

func (g *Generator) normalizeFunc(name string) string {
	switch g.normalizeFunctions {
	case "upper", "true":
		return pyUpper(name)
	case "lower":
		return pyLower(name)
	}
	return name
}

// indent prefixes the lines of sql with level indents plus pad spaces in pretty mode. A negative
// pad selects g.pad.
func (g *Generator) indent(sql string, level int, pad int, skipFirst, skipLast bool) string {
	if !g.pretty || sql == "" {
		return sql
	}
	if pad < 0 {
		pad = g.pad
	}
	lines := strings.Split(sql, "\n")
	prefix := strings.Repeat(" ", level*g.indentSize+pad)
	for i, line := range lines {
		if (skipFirst && i == 0) || (skipLast && i == len(lines)-1) {
			continue
		}
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

func (g *Generator) indentDefault(sql string) string { return g.indent(sql, 0, -1, false, false) }

// sql renders x: nil, false and empty lists as "", strings as is and expressions via dispatch.
func (g *Generator) sql(x any) string { return g.sqlFull(x, "", true) }

func (g *Generator) sqlKey(e *Expr, key string) string {
	if e == nil {
		return ""
	}
	return g.sqlFull(e, key, true)
}

func (g *Generator) sqlNoComment(x any) string { return g.sqlFull(x, "", false) }

func (g *Generator) sqlFull(x any, key string, comment bool) string {
	switch v := x.(type) {
	case nil:
		return ""
	case string:
		return v
	case *Expr:
		if v == nil {
			return ""
		}
		if key != "" {
			val := v.Arg(key)
			if truthy(val) {
				return g.sql(val)
			}
			return ""
		}
		var sql string
		if handler := g.s.dispatch[v.kind]; handler != nil {
			sql = handler(g, v)
		} else if v.IsA(KFunc) {
			sql = g.functionFallbackSQL(v)
		} else if v.IsA(KProperty) {
			sql = g.propertySQL(v)
		} else {
			panic(&ValueError{Msg: "Unsupported expression type " + v.kind.Name()})
		}
		if g.comments && comment {
			return g.maybeComment(sql, v)
		}
		return sql
	case bool:
		// false renders as "" (e.g. an optional arg stored as false when its keyword was absent).
		if !v {
			return ""
		}
		panic(&ValueError{Msg: "Unsupported expression type bool"})
	case []*Expr:
		if len(v) == 0 {
			return ""
		}
		panic(&ValueError{Msg: "Unsupported expression type list"})
	}
	panic(&ValueError{Msg: fmt.Sprintf("Unsupported expression type %T", x)})
}

// functionFallbackSQL renders a function call from the kind's SQL name (or the original name
// when preserved) and its arguments in declaration order.
func (g *Generator) functionFallbackSQL(e *Expr) string {
	var args []any
	for _, at := range e.kind.ArgTypes() {
		v := e.Arg(at.name)
		switch x := v.(type) {
		case []*Expr:
			for _, y := range x {
				args = append(args, y)
			}
		case nil:
		default:
			args = append(args, x)
		}
	}
	return g.fn(g.functionName(e), args...)
}

func (g *Generator) functionName(e *Expr) string {
	if g.d.S.PRESERVE_ORIGINAL_NAMES {
		if n, ok := e.MetaGet("name").(string); ok && n != "" {
			return n
		}
	}
	return e.kind.SQLName()
}

func (g *Generator) fn(name string, args ...any) string {
	return g.funcFull(name, "(", ")", true, args...)
}

func (g *Generator) funcFull(name, prefix, suffix string, normalize bool, args ...any) string {
	if normalize {
		name = g.normalizeFunc(name)
	}
	return name + prefix + g.formatArgs(", ", args...) + suffix
}

// formatArgs renders args joined by sep, skipping nil and boolean values; in pretty mode,
// too-wide argument lists go one per line.
func (g *Generator) formatArgs(sep string, args ...any) string {
	var argSQLs []string
	for _, a := range args {
		switch x := a.(type) {
		case nil:
			continue
		case bool:
			continue
		case *Expr:
			if x == nil {
				continue
			}
		}
		argSQLs = append(argSQLs, g.sql(a))
	}
	if g.pretty && g.tooWide(argSQLs) {
		return g.indent("\n"+strings.Join(argSQLs, pyStrip(sep)+"\n")+"\n", 0, -1, true, true)
	}
	return strings.Join(argSQLs, sep)
}

func (g *Generator) tooWide(args []string) bool {
	n := 0
	for _, a := range args {
		n += len([]rune(a))
	}
	return n > g.maxTextWidth
}

type exprsOpts struct {
	key       string
	sqls      []any
	hasSqls   bool
	flat      bool
	noIndent  bool
	skipFirst bool
	skipLast  bool
	sep       *string // nil = ", "
	prefix    string
	dynamic   bool
	newLine   bool
}

func strp2(s string) *string { return &s }

// expressions renders e's list argument o.key ("expressions" by default), or o.sqls when e is
// nil, joined by o.sep with comments, indentation and pretty line breaks.
func (g *Generator) expressions(e *Expr, o exprsOpts) string {
	var items []any
	if e != nil {
		key := o.key
		if key == "" {
			key = "expressions"
		}
		switch v := e.Arg(key).(type) {
		case []*Expr:
			for _, x := range v {
				items = append(items, x)
			}
		case *Expr:
			// A single expression stands for its own expressions list; kinds without that
			// argument panic with a "not iterable" error.
			if v != nil {
				if !v.kind.hasArgType("expressions") {
					panic(&ValueError{Msg: fmt.Sprintf("'%s' object is not iterable", v.kind.Name())})
				}
				for _, x := range v.Expressions() {
					items = append(items, x)
				}
			}
		case []string:
			for _, x := range v {
				items = append(items, x)
			}
		case []any:
			items = append(items, v...)
		}
	} else {
		items = o.sqls
	}
	if len(items) == 0 {
		return ""
	}
	sep := ", "
	if o.sep != nil {
		sep = *o.sep
	}
	if o.flat {
		var parts []string
		for _, x := range items {
			if s := g.sql(x); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, sep)
	}
	num := len(items)
	var result []string
	for i, x := range items {
		sql := g.sqlNoComment(x)
		if sql == "" {
			continue
		}
		comments := ""
		if xe, ok := x.(*Expr); ok && xe != nil {
			comments = g.maybeComment("", xe)
		}
		if g.pretty {
			if g.leadingComma {
				s := ""
				if i > 0 {
					s = sep
				}
				result = append(result, s+o.prefix+sql+comments)
			} else {
				tail := ""
				if i+1 < num {
					if comments != "" {
						tail = strings.TrimRightFunc(sep, pyIsSpaceRune)
					} else {
						tail = sep
					}
				}
				result = append(result, o.prefix+sql+tail+comments)
			}
		} else {
			tail := ""
			if i+1 < num {
				tail = sep
			}
			result = append(result, o.prefix+sql+comments+tail)
		}
	}
	var resultSQL string
	if g.pretty && (!o.dynamic || g.tooWide(result)) {
		if o.newLine {
			result = append([]string{""}, result...)
			result = append(result, "")
		}
		for i := range result {
			result[i] = strings.TrimRightFunc(result[i], pyIsSpaceRune)
		}
		resultSQL = strings.Join(result, "\n")
	} else {
		resultSQL = strings.Join(result, "")
	}
	if !o.noIndent {
		return g.indent(resultSQL, 0, -1, o.skipFirst, o.skipLast)
	}
	return resultSQL
}

func (g *Generator) opExpressions(op string, e *Expr, flat bool) string {
	flat = flat || e.parent.IsA(KProperties)
	exprsSQL := g.expressions(e, exprsOpts{flat: flat})
	if flat {
		return op + " " + exprsSQL
	}
	s := ""
	if exprsSQL != "" {
		s = g.sep(" ")
	}
	return g.seg(op) + s + exprsSQL
}

// binary renders a chain of same-kind binary nodes iteratively (deep chains do not recurse),
// with op between operands and any operator comments. Only nodes negated like e join the chain,
// except in an IS / IS NOT chain, where each non-boolean test keeps its own negation.
func (g *Generator) binary(e *Expr, op string) string {
	var sqls []string
	stack := []any{e}
	binaryType := e.kind
	isIs := binaryType == KIs && (op == "IS" || op == "IS NOT")
	negate := e.ArgB("negate")
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		// A nested node of the same kind joins the chain only when it renders the same operator
		if n, ok := node.(*Expr); ok && n != nil && n.kind == binaryType &&
			(n == e || (isIs && !n.Expression().IsA(KBoolean)) || n.ArgB("negate") == negate) {
			if opFunc := n.Arg("operator"); truthy(opFunc) {
				op = "OPERATOR(" + g.sql(opFunc) + ")"
			} else if isIs {
				op = "IS"
				if n.ArgB("negate") {
					op = "IS NOT"
				}
			}
			stack = append(stack, n.Arg("expression"))
			stack = append(stack, " "+g.maybeCommentC(op, nil, n.Comments())+" ")
			stack = append(stack, n.Arg("this"))
		} else {
			sqls = append(sqls, g.sql(node))
		}
	}
	return strings.Join(sqls, "")
}

// nakedProperty renders a property as NAME value; unknown properties are unsupported and render
// as None.
func (g *Generator) nakedProperty(e *Expr) string {
	name, ok := PROPERTY_TO_NAME[e.kind]
	if !ok || name == "" {
		g.unsupported("Unsupported property " + e.kind.Name())
		name = "None"
	}
	return name + " " + g.sqlKey(e, "this")
}

// replaceLineBreaks protects newlines in s from pretty-mode reformatting with a sentinel.
func (g *Generator) replaceLineBreaks(s string) string {
	if g.pretty {
		return strings.ReplaceAll(s, "\n", g.s.SENTINEL_LINE_BREAK)
	}
	return s
}

// SQL renders a copy of the expression using the given dialect.
func (e *Expr) SQL(dialect string, opts *GenerateOptions) (string, error) {
	d, err := GetDialect(dialect)
	if err != nil {
		return "", err
	}
	return d.NewGenerator(opts).Generate(e, true)
}

// Generate renders a copy of an expression with this dialect.
func (d *Dialect) Generate(e *Expr, opts *GenerateOptions) (string, error) {
	return d.NewGenerator(opts).Generate(e, true)
}

// GenerateOwned is Generate for an expression the caller does not use afterwards.
func (d *Dialect) GenerateOwned(e *Expr, opts *GenerateOptions) (string, error) {
	return d.NewGenerator(opts).Generate(e.Own(), false)
}
