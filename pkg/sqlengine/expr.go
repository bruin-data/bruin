package sqlengine

import (
	"hash/maphash"
	"iter"
	"strings"
	"unicode/utf8"
)

// Expr is a node of a SQL syntax tree: a Kind plus an ordered mapping of argument keys to
// values.
//
// Argument values are one of: nil, *Expr, []*Expr, string, bool, int, DType.
type Expr struct {
	kind  Kind
	flags exprFlags
	// posLine..posEnd hold the line/col/start/end position metadata (MetaGet("line"), ...)
	// when flagPosSet is set, kept inline instead of in the meta map.
	posLine  int32
	posCol   int32
	posStart int32
	args     []arg
	parent   *Expr
	argKey   string
	index    int32 // -1 when e is not in a list argument
	posEnd   int32
	// ext holds the rarely set fields (comments, metadata), keeping Expr at 96 bytes.
	ext  *exprExt
	typ  *Expr
	hash uint64
}

type exprFlags uint8

const (
	flagHashed        exprFlags = 1 << iota // hash holds the cached structural hash
	flagPosSet                              // posLine..posEnd are set
	flagEmptyComments                       // comments is empty (not nil) and ext holds none
)

func (e *Expr) hashed() bool { return e.flags&flagHashed != 0 }
func (e *Expr) posSet() bool { return e.flags&flagPosSet != 0 }

type exprExt struct {
	comments []string // nil means no comments, as opposed to an empty list
	meta     map[string]any
}

func (e *Expr) extEnsure() *exprExt {
	if e.ext == nil {
		e.ext = &exprExt{}
	}
	return e.ext
}

// Comments returns the attached comments: nil when there are none, or an empty list when an empty
// list was set.
func (e *Expr) Comments() []string {
	if e.ext == nil || e.ext.comments == nil {
		if e.flags&flagEmptyComments != 0 {
			return []string{}
		}
		return nil
	}
	return e.ext.comments
}

// SetComments replaces the attached comments. An empty, non-nil list is kept as a flag.
func (e *Expr) SetComments(c []string) {
	e.flags &^= flagEmptyComments
	if c != nil && len(c) == 0 {
		e.flags |= flagEmptyComments
		if e.ext != nil {
			e.ext.comments = nil
		}
		return
	}
	if c == nil && e.ext == nil {
		return
	}
	e.extEnsure().comments = c
}

func (e *Expr) metaMap() map[string]any {
	if e.ext == nil {
		return nil
	}
	return e.ext.meta
}

type arg struct {
	key string
	val any
}

// New creates an expression of the given kind from alternating key/value pairs.
// Keys with nil values are kept, so HasArgKey reports them.
func New(kind Kind, kv ...any) *Expr {
	if kind == KDateTrunc {
		kv = dateTruncCtorArgs(kv)
	}
	e := newExprArgs(len(kv) / 2)
	e.kind = kind
	e.index = -1
	if len(kv) > 0 {
		for i := 0; i+1 < len(kv); i += 2 {
			key := kv[i].(string)
			val := normalizeArg(kv[i+1])
			e.args = append(e.args, arg{key, val})
			if !kind.isPrimitive() {
				e.setParent(key, val, -1)
			}
		}
	}
	if kind.IsA(KTimeUnit) {
		timeUnitCtor(e)
	}
	return e
}

// Expressions are allocated together with the backing array of their arguments (one
// allocation per node instead of two); args beyond the inline capacity grow normally.
type (
	exprArgs1 struct {
		e Expr
		a [1]arg
	}
	exprArgs2 struct {
		e Expr
		a [2]arg
	}
	exprArgs3 struct {
		e Expr
		a [3]arg
	}
	exprArgs4 struct {
		e Expr
		a [4]arg
	}
	exprArgs6 struct {
		e Expr
		a [6]arg
	}
	exprArgs8 struct {
		e Expr
		a [8]arg
	}
)

func newExprArgs(n int) *Expr {
	switch {
	case n <= 0:
		return &Expr{}
	case n == 1:
		x := &exprArgs1{}
		x.e.args = x.a[:0]
		return &x.e
	case n == 2:
		x := &exprArgs2{}
		x.e.args = x.a[:0]
		return &x.e
	case n == 3:
		x := &exprArgs3{}
		x.e.args = x.a[:0]
		return &x.e
	case n == 4:
		x := &exprArgs4{}
		x.e.args = x.a[:0]
		return &x.e
	case n <= 6:
		x := &exprArgs6{}
		x.e.args = x.a[:0]
		return &x.e
	case n <= 8:
		x := &exprArgs8{}
		x.e.args = x.a[:0]
		return &x.e
	}
	e := &Expr{}
	e.args = make([]arg, 0, n)
	return e
}

var unabbreviatedUnitName = map[string]string{
	"D": "DAY", "H": "HOUR", "M": "MINUTE", "MS": "MILLISECOND", "NS": "NANOSECOND",
	"Q": "QUARTER", "S": "SECOND", "US": "MICROSECOND", "W": "WEEK", "Y": "YEAR",
}

// timeUnitCtor normalizes the unit of a new TimeUnit-kind node into an uppercase, unabbreviated
// Var.
func timeUnitCtor(e *Expr) {
	unit := e.ArgE("unit")
	if unit == nil {
		return
	}
	if (unit.kind == KColumn || unit.kind == KLiteral || unit.kind == KVar) &&
		!(unit.IsA(KColumn) && len(unit.Parts()) != 1) {
		name := unit.Name()
		if n, ok := unabbreviatedUnitName[name]; ok && n != "" {
			name = n
		}
		v := New(KVar, "this", pyUpper(name))
		e.setRaw("unit", v)
		e.setParent("unit", v, -1)
		// The unit is replaced as part of construction, so the original node must not stay
		// attached to e.
		if unit.parent == e {
			unit.parent, unit.argKey, unit.index = nil, "", -1
		}
	} else if unit.kind == KWeek {
		unit.Set("this", New(KVar, "this", pyUpper(unit.This().Name())))
	}
}

// dateTruncCtorArgs prepares the arguments of a new DateTrunc: it removes the `unabbreviate`
// option and turns a simple unit into an uppercase string literal, unabbreviated unless the
// option is false.
func dateTruncCtorArgs(kv []any) []any {
	unabbreviate := true
	out := make([]any, 0, len(kv))
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i].(string) == "unabbreviate" {
			if b, ok := kv[i+1].(bool); ok {
				unabbreviate = b
			} else {
				unabbreviate = truthy(kv[i+1])
			}
			continue
		}
		out = append(out, kv[i], kv[i+1])
	}
	for i := 0; i+1 < len(out); i += 2 {
		if out[i].(string) != "unit" {
			continue
		}
		unit, ok := out[i+1].(*Expr)
		if !ok || unit == nil {
			continue
		}
		if unit.IsA(KColumn, KLiteral, KVar) && !(unit.IsA(KColumn) && len(unit.Parts()) != 1) {
			name := pyUpper(unit.Name())
			if n, ok := unabbreviatedUnitName[name]; unabbreviate && ok {
				name = n
			}
			out[i+1] = LiteralString(name)
		}
	}
	return out
}

// normalizeArg converts a nil *Expr to untyped nil (so v == nil checks work) and a nil []*Expr
// to an empty list.
func normalizeArg(v any) any {
	switch x := v.(type) {
	case *Expr:
		if x == nil {
			return nil
		}
	case []*Expr:
		if x == nil {
			return []*Expr{}
		}
	}
	return v
}

// Kind returns the expression kind.
func (e *Expr) Kind() Kind { return e.kind }

// IsA reports whether e's kind is, or derives from, any of kinds. A nil expression never matches.
func (e *Expr) IsA(kinds ...Kind) bool {
	if e == nil {
		return false
	}
	return e.kind.IsA(kinds...)
}

// Is reports whether e's kind is exactly kind (no subkinds).
func (e *Expr) Is(kind Kind) bool { return e != nil && e.kind == kind }

// Key returns the lowercase kind key.
func (e *Expr) Key() string { return e.kind.Key() }

// Parent returns the parent node.
func (e *Expr) Parent() *Expr { return e.parent }

// ArgKey returns the key under which e is stored in its parent.
func (e *Expr) ArgKey() string { return e.argKey }

// Index returns the position of e in its parent's list argument, or -1.
func (e *Expr) Index() int { return int(e.index) }

func (e *Expr) ArgKeys() []string {
	out := make([]string, len(e.args))
	for i, a := range e.args {
		out[i] = a.key
	}
	return out
}

func (e *Expr) argIndex(key string) int {
	for i := range e.args {
		if e.args[i].key == key {
			return i
		}
	}
	return -1
}

// Arg returns the raw argument value (nil if absent).
func (e *Expr) Arg(key string) any {
	if e == nil {
		return nil
	}
	if i := e.argIndex(key); i >= 0 {
		return e.args[i].val
	}
	return nil
}

// HasArgKey reports whether key is present in args (even with a nil value).
func (e *Expr) HasArgKey(key string) bool { return e.argIndex(key) >= 0 }

// ArgE returns the argument as an expression (nil if absent or not an expression).
func (e *Expr) ArgE(key string) *Expr {
	if v, ok := e.Arg(key).(*Expr); ok {
		return v
	}
	return nil
}

// ArgL returns the argument as an expression list (nil if absent or not a list).
func (e *Expr) ArgL(key string) []*Expr {
	if v, ok := e.Arg(key).([]*Expr); ok {
		return v
	}
	return nil
}

func (e *Expr) ArgS(key string) string {
	if v, ok := e.Arg(key).(string); ok {
		return v
	}
	return ""
}

// ArgB reports whether the argument is set to a truthy value (see truthy).
func (e *Expr) ArgB(key string) bool { return truthy(e.Arg(key)) }

// truthy reports whether an argument value counts as set: non-nil expressions, non-empty lists
// and strings, true, non-zero ints and data types other than DT_NONE. Other values count as set.
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case *Expr:
		return x != nil
	case []*Expr:
		return len(x) > 0
	case []string:
		return len(x) > 0
	case []any:
		return len(x) > 0
	case string:
		return x != ""
	case bool:
		return x
	case int:
		return x != 0
	case DType:
		return x != DT_NONE
	}
	return true
}

// This returns args["this"] as an expression.
func (e *Expr) This() *Expr { return e.ArgE("this") }

// ThisS returns args["this"] as a string.
func (e *Expr) ThisS() string { return e.ArgS("this") }

// Expression returns args["expression"] as an expression.
func (e *Expr) Expression() *Expr { return e.ArgE("expression") }

// Expressions returns args["expressions"] (or an empty list).
func (e *Expr) Expressions() []*Expr { return e.ArgL("expressions") }

func (e *Expr) invalidateHash() {
	for n := e; n != nil && n.hashed(); n = n.parent {
		n.flags &^= flagHashed
	}
}

// Set stores value under key and links its parent pointers. A nil value removes the key.
func (e *Expr) Set(key string, value any) {
	e.invalidateHash()
	value = normalizeArg(value)
	if value == nil {
		if i := e.argIndex(key); i >= 0 {
			e.args = append(e.args[:i], e.args[i+1:]...)
		}
		return
	}
	e.setRaw(key, value)
	e.setParent(key, value, -1)
}

// SetIndex changes element index (negative counts from the end) of the list under key: nil
// removes it, a list is spliced in its place, and an expression replaces it (overwrite) or is
// inserted before it. Out-of-range indexes are ignored.
func (e *Expr) SetIndex(key string, value any, index int, overwrite bool) {
	e.invalidateHash()
	value = normalizeArg(value)
	exprs := e.ArgL(key)
	if index < 0 {
		index += len(exprs)
	}
	if index < 0 || index >= len(exprs) {
		return
	}
	if value == nil {
		exprs = append(exprs[:index:index], exprs[index+1:]...)
		for _, v := range exprs[index:] {
			v.index--
		}
		e.setRaw(key, exprs)
		return
	}
	switch v := value.(type) {
	case []*Expr:
		n := make([]*Expr, 0, len(exprs)-1+len(v))
		n = append(n, exprs[:index]...)
		n = append(n, v...)
		n = append(n, exprs[index+1:]...)
		exprs = n
	case *Expr:
		if overwrite {
			exprs[index] = v
		} else {
			exprs = append(exprs[:index], append([]*Expr{v}, exprs[index:]...)...)
		}
	}
	e.setRaw(key, exprs)
	e.setParent(key, exprs, index)
}

// setRaw stores value under key without parent bookkeeping.
func (e *Expr) setRaw(key string, value any) {
	if i := e.argIndex(key); i >= 0 {
		e.args[i].val = value
		return
	}
	e.args = append(e.args, arg{key, value})
}

// SetArgRaw stores value under key without updating parent pointers.
func (e *Expr) SetArgRaw(key string, value any) {
	e.invalidateHash()
	e.setRaw(key, normalizeArg(value))
}

func (e *Expr) DeleteArg(key string) any {
	if i := e.argIndex(key); i >= 0 {
		v := e.args[i].val
		e.args = append(e.args[:i], e.args[i+1:]...)
		e.invalidateHash()
		return v
	}
	return nil
}

// Append adds value to the end of the list under key, creating the list if needed.
func (e *Expr) Append(key string, value *Expr) {
	e.invalidateHash()
	list, ok := e.Arg(key).([]*Expr)
	if !ok {
		list = []*Expr{}
	}
	if value != nil {
		value.parent = e
		value.argKey = key
		value.index = int32(len(list))
	}
	list = append(list, value)
	e.setRaw(key, list)
}

func (e *Expr) setParent(key string, value any, index int) {
	switch v := value.(type) {
	case *Expr:
		v.parent = e
		v.argKey = key
		v.index = int32(index)
	case []*Expr:
		for i, x := range v {
			if x != nil {
				x.parent = e
				x.argKey = key
				x.index = int32(i)
			}
		}
	case []any:
		for i, y := range v {
			if x, ok := y.(*Expr); ok && x != nil {
				x.parent = e
				x.argKey = key
				x.index = int32(i)
			}
		}
	}
}

func (e *Expr) SetKwargs(kv ...any) *Expr {
	for i := 0; i+1 < len(kv); i += 2 {
		e.Set(kv[i].(string), kv[i+1])
	}
	return e
}

// Text returns the argument under key as text: strings as is, the text of identifiers, literals
// and variables, the name of stars and NULL, and "" for anything else.
func (e *Expr) Text(key string) string {
	if e == nil {
		return ""
	}
	switch v := e.Arg(key).(type) {
	case string:
		return v
	case *Expr:
		if v.IsA(KIdentifier, KLiteral, KVar) {
			if s, ok := v.Arg("this").(string); ok {
				return s
			}
			return ""
		}
		if v.IsA(KStar, KNull) {
			return v.Name()
		}
	}
	return ""
}

// Name returns the name of e, which depends on its kind (for example a column's identifier text,
// "*" for a star or NULL for null).
func (e *Expr) Name() string {
	if e == nil {
		return ""
	}
	switch propOwner_name[e.kind] {
	case KStar:
		return "*"
	case KPlaceholder:
		if s := e.Text("this"); s != "" {
			return s
		}
		return "?"
	case KNull:
		return "NULL"
	case KDot:
		return e.Expression().Name()
	case KAnonymous:
		if s, ok := e.Arg("this").(string); ok {
			return s
		}
		return e.This().Name()
	case KOrdered, KExecute, KCast, KFrom, KDataTypeParam:
		return e.This().Name()
	case KTable:
		this := e.This()
		if this == nil || this.IsA(KFunc) {
			return ""
		}
		return this.Name()
	}
	return e.Text("this")
}

// Alias returns the alias name of e, or "" when it has none.
func (e *Expr) Alias() string {
	if e == nil {
		return ""
	}
	if a, ok := e.Arg("alias").(*Expr); ok {
		return a.Name()
	}
	return e.Text("alias")
}

func (e *Expr) AliasColumnNames() []string {
	ta := e.ArgE("alias")
	if ta == nil {
		return nil
	}
	var out []string
	for _, c := range ta.ArgL("columns") {
		out = append(out, c.Name())
	}
	return out
}

// AliasOrName returns the alias of e, or its name when it has no alias.
func (e *Expr) AliasOrName() string {
	if e == nil {
		return ""
	}
	switch propOwner_alias_or_name[e.kind] {
	case KFrom, KJoin:
		return e.This().AliasOrName()
	}
	if a := e.Alias(); a != "" {
		return a
	}
	return e.Name()
}

// OutputName returns the name under which e appears in a query's output, or "" when it has none.
func (e *Expr) OutputName() string {
	if e == nil {
		return ""
	}
	switch propOwner_output_name[e.kind] {
	case KColumn, KLiteral, KIdentifier, KStar, KDot, KCast, KTableColumn:
		return e.Name()
	case KAlias, KSubquery:
		return e.Alias()
	case KBracket:
		if ex := e.Expressions(); len(ex) == 1 {
			return ex[0].OutputName()
		}
		return ""
	case KParen:
		return e.This().Name()
	case KJSONExtract:
		if len(e.Expressions()) == 0 {
			// Panics when expression is unset or not an expression (see argAttr).
			return e.argAttr("expression", "output_name").OutputName()
		}
		return ""
	case KJSONExtractScalar:
		return e.argAttr("expression", "output_name").OutputName()
	case KJSONPath:
		ex := e.Expressions()
		if len(ex) == 0 {
			return ""
		}
		if s, ok := ex[len(ex)-1].Arg("this").(string); ok {
			return s
		}
		return ""
	}
	return ""
}

// IsString reports whether e is a string literal.
func (e *Expr) IsString() bool {
	return e.IsA(KLiteral) && e.ArgB("is_string")
}

// IsNumber reports whether e is a numeric literal, possibly negated.
func (e *Expr) IsNumber() bool {
	if e.IsA(KLiteral) && !e.ArgB("is_string") {
		return true
	}
	return e.IsA(KNeg) && e.This().IsNumber()
}

// IsInt reports whether e is an integer literal, possibly negated.
func (e *Expr) IsInt() bool {
	if !e.IsNumber() {
		return false
	}
	v, isInt := e.toPyNumber()
	if v == nil && !isInt {
		n := e
		for n.IsA(KNeg) {
			n = n.This()
		}
		if !pyDecimalValid(n.ThisS()) {
			// The literal is not a valid decimal number. The message keeps its historical
			// wording; tests pin it.
			panic(&ValueError{Msg: "[<class 'decimal.ConversionSyntax'>]"})
		}
	}
	return isInt
}

// IsStar reports whether e is or selects a star (*).
func (e *Expr) IsStar() bool {
	if e == nil {
		return false
	}
	switch propOwner_is_star[e.kind] {
	case KDot:
		return e.Expression().IsStar()
	case KSetOperation:
		return e.This().IsStar() || e.Expression().IsStar()
	case KSelect:
		for _, x := range e.Expressions() {
			if x.IsStar() {
				return true
			}
		}
		return false
	case KSubquery:
		return e.This().IsStar()
	}
	return e.IsA(KStar) || (e.IsA(KColumn) && e.This().IsA(KStar))
}

// Type returns the type of e: e itself for data types, the annotated type (or target type) for
// casts and the annotated type otherwise.
func (e *Expr) Type() *Expr {
	if e == nil {
		return nil
	}
	if e.kind.isDataType() {
		return e
	}
	if e.kind.isCast() {
		if e.typ != nil {
			return e.typ
		}
		return e.ArgE("to")
	}
	return e.typ
}

// RawType returns the annotated type as stored, without the data-type and cast rules of Type.
func (e *Expr) RawType() *Expr { return e.typ }

// SetType assigns the annotated type. dt must be a DataType expression or nil.
func (e *Expr) SetType(dt *Expr) { e.typ = dt }

func (e *Expr) SetTypeD(d DType) { e.typ = NewDataType(d) }

func (e *Expr) IsLeaf() bool {
	for _, a := range e.args {
		switch v := a.val.(type) {
		case *Expr:
			if v != nil {
				return false
			}
		case []*Expr:
			if len(v) > 0 {
				return false
			}
		case []any:
			if len(v) > 0 {
				return false
			}
		}
	}
	return true
}

// Meta returns the metadata map, allocating it if needed.
func (e *Expr) Meta() map[string]any {
	x := e.extEnsure()
	if x.meta == nil {
		x.meta = map[string]any{}
	}
	return x.meta
}

// MetaGet reads a metadata value without allocating. Explicit map entries take precedence over
// the inline position fields.
func (e *Expr) MetaGet(key string) any {
	if m := e.metaMap(); m != nil {
		if v, ok := m[key]; ok {
			return v
		}
	}
	if e.posSet() {
		switch key {
		case "line":
			return int(e.posLine)
		case "col":
			return int(e.posCol)
		case "start":
			return int(e.posStart)
		case "end":
			return int(e.posEnd)
		}
	}
	return nil
}

func (e *Expr) setPositions(line, col, start, end int) {
	e.posLine, e.posCol, e.posStart, e.posEnd = int32(line), int32(col), int32(start), int32(end)
	e.flags |= flagPosSet
	e.clearPositionMeta()
}

// clearPositionMeta drops explicit line/col/start/end metadata entries (the inline fields win).
func (e *Expr) clearPositionMeta() {
	if m := e.metaMap(); m != nil {
		delete(m, "line")
		delete(m, "col")
		delete(m, "start")
		delete(m, "end")
	}
}

// Copy returns a deep copy of the tree rooted at e.
func (e *Expr) Copy() *Expr {
	if e == nil {
		return nil
	}
	return e.deepCopy()
}

// Own stands in for e.Copy() when the caller hands e over and never uses it (or any of its
// nodes) again. A copy of a parentless root whose nodes are each held exactly once, with no types,
// string-list arguments or non-scalar meta, is indistinguishable from the original, so such trees
// are returned as they are (with comment lists un-shared, as the copy would have them); anything
// else is copied.
func (e *Expr) Own() *Expr {
	if e == nil {
		return nil
	}
	if e.parent != nil || e.argKey != "" || e.index != -1 || !e.copyEquivalent() {
		return e.deepCopy()
	}
	e.unshareComments()
	return e
}

// copyEquivalent reports whether every node below e is held exactly once, by its parent slot,
// and carries nothing deepCopy would duplicate apart from comment lists.
func (e *Expr) copyEquivalent() bool {
	if e.typ != nil {
		return false
	}
	if e.ext != nil {
		for _, v := range e.ext.meta {
			switch v.(type) {
			case nil, bool, string, int, float64:
			default:
				return false
			}
		}
	}
	for _, a := range e.args {
		switch v := a.val.(type) {
		case *Expr:
			if v.parent != e || v.argKey != a.key || v.index != -1 || !v.copyEquivalent() {
				return false
			}
		case []*Expr:
			for j, x := range v {
				if x != nil && (x.parent != e || x.argKey != a.key || int(x.index) != j || !x.copyEquivalent()) {
					return false
				}
			}
		case []any:
			for j, y := range v {
				if x, ok := y.(*Expr); ok && x != nil && (x.parent != e || x.argKey != a.key || int(x.index) != j || !x.copyEquivalent()) {
					return false
				}
			}
		case []string:
			return false
		}
	}
	return true
}

func (e *Expr) unshareComments() {
	if e.ext != nil && e.ext.comments != nil {
		e.ext.comments = append([]string{}, e.ext.comments...)
	}
	for _, a := range e.args {
		switch v := a.val.(type) {
		case *Expr:
			v.unshareComments()
		case []*Expr:
			for _, x := range v {
				if x != nil {
					x.unshareComments()
				}
			}
		case []any:
			for _, y := range v {
				if x, ok := y.(*Expr); ok && x != nil {
					x.unshareComments()
				}
			}
		}
	}
}

func (e *Expr) deepCopy() *Expr {
	c := newExprArgs(len(e.args))
	c.kind, c.index, c.hash, c.flags = e.kind, -1, e.hash, e.flags
	c.posLine, c.posCol, c.posStart, c.posEnd = e.posLine, e.posCol, e.posStart, e.posEnd
	if e.ext != nil {
		c.ext = &exprExt{}
		if e.ext.comments != nil {
			c.ext.comments = append([]string{}, e.ext.comments...)
		}
		if e.ext.meta != nil {
			c.ext.meta = make(map[string]any, len(e.ext.meta))
			for k, v := range e.ext.meta {
				c.ext.meta[k] = v
			}
		}
	}
	if e.typ != nil {
		c.typ = e.typ.deepCopy()
	}
	if len(e.args) > 0 {
		c.args = c.args[:len(e.args)]
		for i, a := range e.args {
			c.args[i].key = a.key
			switch v := a.val.(type) {
			case *Expr:
				cv := v.deepCopy()
				cv.parent = c
				cv.argKey = a.key
				cv.index = -1
				c.args[i].val = cv
			case []*Expr:
				list := make([]*Expr, len(v))
				for j, x := range v {
					if x == nil {
						continue
					}
					cx := x.deepCopy()
					cx.parent = c
					cx.argKey = a.key
					cx.index = int32(j)
					list[j] = cx
				}
				c.args[i].val = list
			case []string:
				c.args[i].val = append([]string{}, v...)
			case []any:
				list := make([]any, len(v))
				for j, y := range v {
					if x, ok := y.(*Expr); ok && x != nil {
						cx := x.deepCopy()
						cx.parent = c
						cx.argKey = a.key
						cx.index = int32(j)
						list[j] = cx
					} else {
						list[j] = y
					}
				}
				c.args[i].val = list
			default:
				c.args[i].val = v
			}
		}
	}
	return c
}

const metaCommentPrefix = "sqlglot.meta"

// AddComments attaches comments to e, after or (prepend) before the existing ones. Comments that
// contain metaCommentPrefix followed by k=v, k2 pairs also set metadata: k to v (true/false/1/0
// become booleans) and k2 to true. An empty list marks e as having empty, non-nil comments.
func (e *Expr) AddComments(comments []string, prepend bool) {
	if len(comments) == 0 {
		if e.Comments() == nil {
			e.SetComments([]string{})
		}
		return
	}
	e.extEnsure()
	e.flags &^= flagEmptyComments
	for _, c := range comments {
		parts := strings.Split(c, metaCommentPrefix)
		if len(parts) > 1 {
			for _, kv := range strings.Split(strings.Join(parts[1:], ""), ",") {
				pair := strings.Split(kv, "=")
				k := strings.TrimSpace(pair[0])
				if len(pair) > 1 {
					e.Meta()[k] = toBool(strings.TrimSpace(pair[1]))
				} else {
					e.Meta()[k] = true
				}
			}
		}
		if !prepend {
			e.ext.comments = append(e.ext.comments, c)
		}
	}
	if prepend {
		e.ext.comments = append(append([]string{}, comments...), e.ext.comments...)
	}
}

func toBool(v string) any {
	switch strings.ToLower(v) {
	case "true", "1":
		return true
	case "false", "0":
		return false
	}
	return v
}

// PopComments removes and returns the attached comments, as an empty list when there are none.
func (e *Expr) PopComments() []string {
	c := e.Comments()
	e.SetComments(nil)
	if c == nil {
		return []string{}
	}
	return c
}

func (e *Expr) IterExpressions(reverse bool) []*Expr {
	return e.appendChildren(nil, reverse)
}

func (e *Expr) nodeCount() int {
	n := 1
	for _, a := range e.args {
		switch v := a.val.(type) {
		case *Expr:
			if v != nil {
				n += v.nodeCount()
			}
		case []*Expr:
			for _, x := range v {
				if x != nil {
					n += x.nodeCount()
				}
			}
		case []any:
			for _, y := range v {
				if x, ok := y.(*Expr); ok && x != nil {
					n += x.nodeCount()
				}
			}
		}
	}
	return n
}

func (e *Expr) appendChildren(out []*Expr, reverse bool) []*Expr {
	if !reverse {
		for _, a := range e.args {
			switch v := a.val.(type) {
			case *Expr:
				out = append(out, v)
			case []*Expr:
				for _, x := range v {
					if x != nil {
						out = append(out, x)
					}
				}
			case []any:
				for _, y := range v {
					if x, ok := y.(*Expr); ok && x != nil {
						out = append(out, x)
					}
				}
			}
		}
		return out
	}
	for i := len(e.args) - 1; i >= 0; i-- {
		switch v := e.args[i].val.(type) {
		case *Expr:
			out = append(out, v)
		case []*Expr:
			for j := len(v) - 1; j >= 0; j-- {
				if v[j] != nil {
					out = append(out, v[j])
				}
			}
		case []any:
			for j := len(v) - 1; j >= 0; j-- {
				if x, ok := v[j].(*Expr); ok && x != nil {
					out = append(out, x)
				}
			}
		}
	}
	return out
}

// BFS iterates the tree breadth-first, skipping the children of nodes for which prune returns
// true. Children are collected after the consumer has seen their parent, so changes the consumer
// makes to a node are visible when its children are visited.
func (e *Expr) BFS(prune func(*Expr) bool) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		var buf [32]*Expr
		queue := append(buf[:0], e)
		head := 0
		for head < len(queue) {
			node := queue[head]
			queue[head] = nil
			head++
			if !yield(node) {
				return
			}
			if prune != nil && prune(node) {
				continue
			}
			if head == len(queue) {
				queue, head = queue[:0], 0
			} else if head >= 64 && head*2 >= len(queue) {
				n := copy(queue, queue[head:])
				queue, head = queue[:n], 0
			}
			queue = node.appendChildren(queue, false)
		}
	}
}

// DFS iterates the tree depth-first in pre-order, skipping the children of nodes for which prune
// returns true.
func (e *Expr) DFS(prune func(*Expr) bool) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		var buf [32]*Expr
		stack := append(buf[:0], e)
		for len(stack) > 0 {
			node := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if !yield(node) {
				return
			}
			if prune != nil && prune(node) {
				continue
			}
			stack = node.appendChildren(stack, true)
		}
	}
}

// Walk iterates the tree with BFS or DFS.
func (e *Expr) Walk(bfs bool, prune func(*Expr) bool) iter.Seq[*Expr] {
	if bfs {
		return e.BFS(prune)
	}
	return e.DFS(prune)
}

// FindAll iterates, breadth-first, the nodes whose kind is or derives from one of kinds.
func (e *Expr) FindAll(kinds ...Kind) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		for n := range e.BFS(nil) {
			if n.kind.IsA(kinds...) {
				if !yield(n) {
					return
				}
			}
		}
	}
}

func (e *Expr) FindAllDFS(kinds ...Kind) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		for n := range e.DFS(nil) {
			if n.kind.IsA(kinds...) {
				if !yield(n) {
					return
				}
			}
		}
	}
}

// FindAllList collects FindAll into a slice.
func (e *Expr) FindAllList(kinds ...Kind) []*Expr {
	var out []*Expr
	for n := range e.FindAll(kinds...) {
		out = append(out, n)
	}
	return out
}

// Find returns the first node FindAll yields, or nil.
func (e *Expr) Find(kinds ...Kind) *Expr {
	if e == nil {
		return nil
	}
	for n := range e.FindAll(kinds...) {
		return n
	}
	return nil
}

// FindAncestor returns the nearest ancestor whose kind is or derives from one of kinds, or nil.
func (e *Expr) FindAncestor(kinds ...Kind) *Expr {
	a := e.parent
	for a != nil && !a.kind.IsA(kinds...) {
		a = a.parent
	}
	return a
}

func (e *Expr) findAncestorIn(kinds KindSet) *Expr {
	a := e.parent
	for a != nil && !kinds.Has(a.kind) {
		a = a.parent
	}
	return a
}

func (e *Expr) ParentSelect() *Expr { return e.FindAncestor(KSelect) }

func (e *Expr) SameParent() bool { return e.parent != nil && e.parent.kind == e.kind }

// Root returns the root of the tree.
func (e *Expr) Root() *Expr {
	r := e
	for r.parent != nil {
		r = r.parent
	}
	return r
}

func (e *Expr) Depth() int {
	d := 0
	for p := e.parent; p != nil; p = p.parent {
		d++
	}
	return d
}

// Unnest returns the first non-subquery inside a subquery (see UnnestSubquery) and, for anything
// else, e with enclosing Paren nodes stripped.
func (e *Expr) Unnest() *Expr {
	if e != nil && e.IsA(KSubquery) {
		return e.UnnestSubquery()
	}
	x := e
	for x != nil && x.kind == KParen {
		x = x.This()
	}
	return x
}

func (e *Expr) Unalias() *Expr {
	if e.IsA(KAlias) {
		return e.This()
	}
	return e
}

func (e *Expr) UnnestOperands() []*Expr {
	children := e.IterExpressions(false)
	for i, c := range children {
		children[i] = c.Unnest()
	}
	return children
}

// Flatten returns the operands of a chain of same-kind nodes (such as a AND b AND c), unnesting
// them when unnest is set.
func (e *Expr) Flatten(unnest bool) []*Expr {
	var out []*Expr
	prune := func(n *Expr) bool { return n.parent != nil && n.kind != e.kind }
	for n := range e.DFS(prune) {
		if n.kind != e.kind {
			if unnest && !n.kind.isSubquery() {
				out = append(out, n.Unnest())
			} else {
				out = append(out, n)
			}
		}
	}
	return out
}

// Transform applies fn to every node depth-first, replacing each node with fn's result, and
// returns the new root. Children of replaced nodes are not visited. copy transforms a copy.
func (e *Expr) Transform(fn func(*Expr) *Expr, copy bool) *Expr {
	start := e
	if copy {
		start = e.Copy()
	}
	var root, newNode *Expr
	prune := func(n *Expr) bool { return n != newNode }
	for node := range start.DFS(prune) {
		parent, key, index := node.parent, node.argKey, node.index
		newNode = fn(node)
		if root == nil {
			root = newNode
		} else if parent != nil && key != "" && newNode != node {
			if index >= 0 {
				parent.SetIndex(key, newNode, int(index), true)
			} else {
				parent.Set(key, newNode)
			}
		}
	}
	return root
}

// Replace puts expression in e's place in its parent (nil removes e), detaches e and returns
// expression.
func (e *Expr) Replace(expression *Expr) *Expr {
	parent := e.parent
	if parent == nil || parent == expression {
		return expression
	}
	key := e.argKey
	if key != "" {
		if e.index >= 0 {
			if expression == nil {
				parent.SetIndex(key, nil, int(e.index), true)
			} else {
				parent.SetIndex(key, expression, int(e.index), true)
			}
		} else {
			parent.Set(key, expression)
		}
	}
	if expression != e {
		e.parent = nil
		e.argKey = ""
		e.index = -1
	}
	return expression
}

func (e *Expr) ReplaceWithList(list []*Expr) {
	parent := e.parent
	if parent == nil {
		return
	}
	key := e.argKey
	if key != "" {
		if v, ok := parent.Arg(key).(*Expr); ok {
			if v.parent != nil {
				v.parent.ReplaceWithList(list)
			}
		} else if e.index >= 0 {
			parent.SetIndex(key, list, int(e.index), true)
		} else {
			parent.Set(key, list)
		}
	}
	e.parent = nil
	e.argKey = ""
	e.index = -1
}

func (e *Expr) Pop() *Expr {
	e.Replace(nil)
	return e
}

var hashSeed = maphash.MakeSeed()

// Hash returns the cached structural hash of e (string args are compared case-insensitively).
func (e *Expr) Hash() uint64 {
	if e.hashed() {
		return e.hash
	}
	var h maphash.Hash
	h.SetSeed(hashSeed)
	h.WriteString(e.kind.Key())
	// Visit args in key order (keys are distinct, so any sort gives the same order).
	var buf [16]int
	var keys []int
	if len(e.args) <= len(buf) {
		keys = buf[:len(e.args)]
	} else {
		keys = make([]int, len(e.args))
	}
	for i := range keys {
		keys[i] = i
		for j := i; j > 0 && e.args[keys[j]].key < e.args[keys[j-1]].key; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	raw := e.kind.hashRawArgs()
	for _, i := range keys {
		a := e.args[i]
		if raw {
			if truthy(a.val) {
				h.WriteByte(0)
				h.WriteString(a.key)
				hashValue(&h, a.val, false)
			}
			continue
		}
		switch v := a.val.(type) {
		case []*Expr:
			for _, x := range v {
				h.WriteByte(1)
				h.WriteString(a.key)
				if x != nil {
					writeU64(&h, x.Hash())
				}
			}
		case []string:
			for _, x := range v {
				h.WriteByte(1)
				h.WriteString(a.key)
				writeLower(&h, x)
			}
		case []any:
			for _, x := range v {
				h.WriteByte(1)
				h.WriteString(a.key)
				if x != nil && x != false {
					hashValue(&h, x, true)
				}
			}
		default:
			if a.val != nil && a.val != false {
				h.WriteByte(2)
				h.WriteString(a.key)
				hashValue(&h, a.val, true)
			}
		}
	}
	e.hash = h.Sum64()
	e.flags |= flagHashed
	return e.hash
}

// writeLower writes strings.ToLower(s) without allocating for ASCII s.
func writeLower(h *maphash.Hash, s string) {
	var buf [64]byte
	n := 0
	for i := range len(s) {
		c := s[i]
		if c >= utf8.RuneSelf {
			h.Write(buf[:n])
			h.WriteString(strings.ToLower(s[i:]))
			return
		}
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		buf[n] = c
		n++
		if n == len(buf) {
			h.Write(buf[:n])
			n = 0
		}
	}
	h.Write(buf[:n])
}

func writeU64(h *maphash.Hash, v uint64) {
	var b [8]byte
	for i := range 8 {
		b[i] = byte(v >> (8 * i))
	}
	h.Write(b[:])
}

func hashValue(h *maphash.Hash, v any, lower bool) {
	switch x := v.(type) {
	case *Expr:
		writeU64(h, x.Hash())
	case string:
		h.WriteByte('s')
		if lower {
			writeLower(h, x)
		} else {
			h.WriteString(x)
		}
	case bool:
		// Booleans hash like the integers 0 and 1.
		h.WriteByte('i')
		if x {
			writeU64(h, 1)
		} else {
			writeU64(h, 0)
		}
	case int:
		h.WriteByte('i')
		writeU64(h, uint64(x))
	case DType:
		h.WriteByte('d')
		writeU64(h, uint64(x))
	case []*Expr:
		for _, y := range x {
			if y != nil {
				writeU64(h, y.Hash())
			}
		}
	default:
		h.WriteByte('?')
	}
}

// Equal reports whether e and o have the same kind and structure (see Hash).
func (e *Expr) Equal(o *Expr) bool {
	if e == o {
		return true
	}
	if e == nil || o == nil {
		return false
	}
	return e.kind == o.kind && e.Hash() == o.Hash()
}

// ErrorMessages returns validation errors for e: missing required arguments and, for functions
// given args, more arguments than the function accepts.
func (e *Expr) ErrorMessages(args []*Expr) []string {
	var errs []string
	for _, a := range e.kind.ArgTypes() {
		if !a.required {
			continue
		}
		v := e.Arg(a.name)
		if v == nil {
			errs = append(errs, "Required keyword: '"+a.name+"' missing for "+e.Kind().Name())
			continue
		}
		if l, ok := v.([]*Expr); ok && len(l) == 0 {
			errs = append(errs, "Required keyword: '"+a.name+"' missing for "+e.Kind().Name())
		}
	}
	if args != nil && e.IsA(KFunc) && len(args) > len(e.kind.ArgTypes()) && !e.kind.isVarLenArgs() {
		errs = append(errs, "The number of provided arguments ("+itoa(len(args))+") is greater than the maximum number of supported arguments ("+itoa(len(e.kind.ArgTypes()))+")")
	}
	return errs
}

// metaValidateArgs is a transient metadata key set by function builders that drop, insert or
// truncate arguments: the parser validates the built function against that adjusted list, which
// a Go builder cannot hand back by shrinking or growing the caller's slice.
const metaValidateArgs = "\x00validate_args"

// WithValidateArgs records the post-mutation argument list of a function builder on e and returns e.
func WithValidateArgs(e *Expr, args []*Expr) *Expr {
	if e != nil {
		e.Meta()[metaValidateArgs] = args
	}
	return e
}

// callFuncBuilder invokes a FUNCTIONS builder and returns the built expression together with the
// argument list to validate it against (the builder's adjusted list, if it recorded one).
func callFuncBuilder(b FuncBuilder, args []*Expr, d *Dialect) (*Expr, []*Expr) {
	e := b(args, d)
	if e == nil || e.metaMap() == nil {
		return e, args
	}
	v, ok := e.ext.meta[metaValidateArgs]
	if !ok {
		return e, args
	}
	delete(e.ext.meta, metaValidateArgs)
	if len(e.ext.meta) == 0 {
		e.ext.meta = nil
		if e.ext.comments == nil {
			e.ext = nil
		}
	}
	return e, v.([]*Expr)
}
