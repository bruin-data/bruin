package sqlengine

import (
	"hash/maphash"
	"iter"
	"sort"
	"strings"
)

// Expr is a node of a sqlglot syntax tree. It mirrors sqlglot.expressions.Expression:
// a class (Kind) plus an ordered mapping of argument keys to values.
//
// Argument values are one of: nil, *Expr, []*Expr, string, bool, int, DType.
type Expr struct {
	kind  Kind
	flags exprFlags
	// posLine..posEnd hold the line/col/start/end position metadata (Python's meta["line"], ...)
	// when flagPosSet is set, kept inline instead of in the meta map.
	posLine  int32
	posCol   int32
	posStart int32
	args     []arg
	parent   *Expr
	argKey   string
	index    int32 // -1 means None
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
	flagEmptyComments                       // comments is [] (not None) and ext holds none
)

func (e *Expr) hashed() bool { return e.flags&flagHashed != 0 }
func (e *Expr) posSet() bool { return e.flags&flagPosSet != 0 }

func (e *Expr) setFlag(f exprFlags, on bool) {
	if on {
		e.flags |= f
	} else {
		e.flags &^= f
	}
}

// exprExt holds the optional parts of an Expr.
type exprExt struct {
	comments []string // nil mirrors Python's None
	meta     map[string]any
}

func (e *Expr) extEnsure() *exprExt {
	if e.ext == nil {
		e.ext = &exprExt{}
	}
	return e.ext
}

// Comments returns the attached comments (nil when there are none, like Python's None).
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

// metaMap returns the metadata map or nil.
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
// Keys with nil values are kept (like Python keyword arguments set to None).
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

// newExprArgs allocates a zero Expr with room for n arguments.
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

// unabbreviatedUnitName mirrors TimeUnit.UNABBREVIATED_UNIT_NAME.
var unabbreviatedUnitName = map[string]string{
	"D": "DAY", "H": "HOUR", "M": "MINUTE", "MS": "MILLISECOND", "NS": "NANOSECOND",
	"Q": "QUARTER", "S": "SECOND", "US": "MICROSECOND", "W": "WEEK", "Y": "YEAR",
}

// timeUnitCtor mirrors TimeUnit.__init__ (unit normalization into a Var).
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
		// Python replaces the unit before Expr.__init__ links parents, so the original node is
		// never attached to e.
		if unit.parent == e {
			unit.parent, unit.argKey, unit.index = nil, "", -1
		}
	} else if unit.kind == KWeek {
		unit.Set("this", New(KVar, "this", pyUpper(unit.This().Name())))
	}
}

// dateTruncCtorArgs mirrors DateTrunc.__init__ (pops `unabbreviate`, normalizes `unit`).
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

// normalizeArg converts typed nils to untyped nil so that "value is None" checks work.
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

// Kind returns the expression class.
func (e *Expr) Kind() Kind { return e.kind }

// IsA mirrors isinstance(e, kinds). A nil expression is never an instance.
func (e *Expr) IsA(kinds ...Kind) bool {
	if e == nil {
		return false
	}
	return e.kind.IsA(kinds...)
}

// Is mirrors type(e) is kind (exact class match).
func (e *Expr) Is(kind Kind) bool { return e != nil && e.kind == kind }

// Key returns the lowercase class key.
func (e *Expr) Key() string { return e.kind.Key() }

// Parent returns the parent node.
func (e *Expr) Parent() *Expr { return e.parent }

// ArgKey returns the key under which e is stored in its parent.
func (e *Expr) ArgKey() string { return e.argKey }

// Index returns the position of e in its parent's list argument, or -1.
func (e *Expr) Index() int { return int(e.index) }

// Args returns the ordered arguments. Callers must not mutate the result.
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

// ArgS returns a string argument ("" if absent or not a string).
func (e *Expr) ArgS(key string) string {
	if v, ok := e.Arg(key).(string); ok {
		return v
	}
	return ""
}

// ArgB returns the truthiness of an argument, like bool(expression.args.get(key)).
func (e *Expr) ArgB(key string) bool { return truthy(e.Arg(key)) }

// Truthy mirrors Python truthiness of an argument value.
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

// Set mirrors Expression.set(arg_key, value). A nil value removes the key.
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

// SetIndex mirrors Expression.set(arg_key, value, index, overwrite).
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

// setRaw mirrors self.args[key] = value without parent bookkeeping.
func (e *Expr) setRaw(key string, value any) {
	if i := e.argIndex(key); i >= 0 {
		e.args[i].val = value
		return
	}
	e.args = append(e.args, arg{key, value})
}

// SetArgRaw mirrors direct assignment to expression.args[key].
func (e *Expr) SetArgRaw(key string, value any) {
	e.invalidateHash()
	e.setRaw(key, normalizeArg(value))
}

// DeleteArg mirrors expression.args.pop(key, None).
func (e *Expr) DeleteArg(key string) any {
	if i := e.argIndex(key); i >= 0 {
		v := e.args[i].val
		e.args = append(e.args[:i], e.args[i+1:]...)
		e.invalidateHash()
		return v
	}
	return nil
}

// Append mirrors Expression.append(arg_key, value).
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

// SetKwargs sets several args in order.
func (e *Expr) SetKwargs(kv ...any) *Expr {
	for i := 0; i+1 < len(kv); i += 2 {
		e.Set(kv[i].(string), kv[i+1])
	}
	return e
}

// Text mirrors Expression.text(key).
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

// Name mirrors the `name` property.
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

// Alias mirrors the `alias` property.
func (e *Expr) Alias() string {
	if e == nil {
		return ""
	}
	if a, ok := e.Arg("alias").(*Expr); ok {
		return a.Name()
	}
	return e.Text("alias")
}

// AliasColumnNames mirrors the `alias_column_names` property.
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

// AliasOrName mirrors the `alias_or_name` property.
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

// OutputName mirrors the `output_name` property.
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
			// self.expression.output_name (AttributeError when expression is None/False)
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

// IsString mirrors the `is_string` property.
func (e *Expr) IsString() bool {
	return e.IsA(KLiteral) && e.ArgB("is_string")
}

// IsNumber mirrors the `is_number` property.
func (e *Expr) IsNumber() bool {
	if e.IsA(KLiteral) && !e.ArgB("is_string") {
		return true
	}
	return e.IsA(KNeg) && e.This().IsNumber()
}

// IsInt mirrors the `is_int` property.
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
			// Literal.to_py: Decimal(self.this) raises InvalidOperation.
			panic(&ValueError{Msg: "[<class 'decimal.ConversionSyntax'>]"})
		}
	}
	return isInt
}

// IsStar mirrors the `is_star` property.
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

// Type mirrors the `type` property.
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

// RawType returns the annotated type (the `_type` slot).
func (e *Expr) RawType() *Expr { return e.typ }

// SetType assigns the annotated type. dt must be a DataType expression or nil.
func (e *Expr) SetType(dt *Expr) { e.typ = dt }

// SetTypeD assigns a simple data type.
func (e *Expr) SetTypeD(d DType) { e.typ = NewDataType(d) }

// IsLeaf mirrors Expression.is_leaf().
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

// setPositions sets the line/col/start/end position metadata.
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

const sqlglotMeta = "sqlglot.meta"

// AddComments mirrors Expression.add_comments.
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
		parts := strings.Split(c, sqlglotMeta)
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

// PopComments mirrors Expression.pop_comments.
func (e *Expr) PopComments() []string {
	c := e.Comments()
	e.SetComments(nil)
	if c == nil {
		return []string{}
	}
	return c
}

// IterExpressions yields child expressions in argument order.
func (e *Expr) IterExpressions(reverse bool) []*Expr {
	return e.appendChildren(nil, reverse)
}

// appendChildren appends e's child expressions (IterExpressions order) to out.
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

// BFS mirrors Expression.bfs. Children are collected after the consumer has seen their parent,
// like Python's lazy iter_expressions.
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

// DFS mirrors Expression.dfs.
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

// Walk mirrors Expression.walk.
func (e *Expr) Walk(bfs bool, prune func(*Expr) bool) iter.Seq[*Expr] {
	if bfs {
		return e.BFS(prune)
	}
	return e.DFS(prune)
}

// FindAll mirrors Expression.find_all (BFS).
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

// FindAllDFS mirrors Expression.find_all(bfs=False).
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

// Find mirrors Expression.find.
func (e *Expr) Find(kinds ...Kind) *Expr {
	if e == nil {
		return nil
	}
	for n := range e.FindAll(kinds...) {
		return n
	}
	return nil
}

// FindAncestor mirrors Expression.find_ancestor.
func (e *Expr) FindAncestor(kinds ...Kind) *Expr {
	a := e.parent
	for a != nil && !a.kind.IsA(kinds...) {
		a = a.parent
	}
	return a
}

// findAncestorIn is FindAncestor for a precomputed kindMatcher set.
func (e *Expr) findAncestorIn(kinds KindSet) *Expr {
	a := e.parent
	for a != nil && !kinds.Has(a.kind) {
		a = a.parent
	}
	return a
}

// ParentSelect mirrors Expression.parent_select.
func (e *Expr) ParentSelect() *Expr { return e.FindAncestor(KSelect) }

// SameParent mirrors Expression.same_parent.
func (e *Expr) SameParent() bool { return e.parent != nil && e.parent.kind == e.kind }

// Root returns the root of the tree.
func (e *Expr) Root() *Expr {
	r := e
	for r.parent != nil {
		r = r.parent
	}
	return r
}

// Depth mirrors Expression.depth.
func (e *Expr) Depth() int {
	d := 0
	for p := e.parent; p != nil; p = p.parent {
		d++
	}
	return d
}

// Unnest mirrors the polymorphic Expression.unnest: Subquery.unnest (first non-subquery) for
// subqueries, otherwise strip enclosing Paren nodes.
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

// Unalias mirrors Expression.unalias.
func (e *Expr) Unalias() *Expr {
	if e.IsA(KAlias) {
		return e.This()
	}
	return e
}

// UnnestOperands mirrors Expression.unnest_operands.
func (e *Expr) UnnestOperands() []*Expr {
	children := e.IterExpressions(false)
	for i, c := range children {
		children[i] = c.Unnest()
	}
	return children
}

// Flatten mirrors Expression.flatten.
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

// Transform mirrors Expression.transform.
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

// Replace mirrors Expression.replace.
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

// ReplaceWithList mirrors Expression.replace(list) for list arguments.
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

// Pop mirrors Expression.pop.
func (e *Expr) Pop() *Expr {
	e.Replace(nil)
	return e
}

var hashSeed = maphash.MakeSeed()

// Hash mirrors Expression.__hash__ (string args are compared case-insensitively).
func (e *Expr) Hash() uint64 {
	if e.hashed() {
		return e.hash
	}
	var h maphash.Hash
	h.SetSeed(hashSeed)
	h.WriteString(e.kind.Key())
	keys := make([]int, len(e.args))
	for i := range keys {
		keys[i] = i
	}
	sort.Slice(keys, func(a, b int) bool { return e.args[keys[a]].key < e.args[keys[b]].key })
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
				h.WriteString(strings.ToLower(x))
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

func writeU64(h *maphash.Hash, v uint64) {
	var b [8]byte
	for i := 0; i < 8; i++ {
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
			h.WriteString(strings.ToLower(x))
		} else {
			h.WriteString(x)
		}
	case bool:
		// Python: hash(True) == hash(1)
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

// Equal mirrors Expression.__eq__.
func (e *Expr) Equal(o *Expr) bool {
	if e == o {
		return true
	}
	if e == nil || o == nil {
		return false
	}
	return e.kind == o.kind && e.Hash() == o.Hash()
}

// ErrorMessages mirrors Expression.error_messages.
func (e *Expr) ErrorMessages(args []*Expr) []string {
	var errs []string
	for _, a := range e.kind.ArgTypes() {
		if !a.required {
			continue
		}
		v := e.Arg(a.name)
		if v == nil {
			errs = append(errs, "Required keyword: '"+a.name+"' missing for "+e.classRepr())
			continue
		}
		if l, ok := v.([]*Expr); ok && len(l) == 0 {
			errs = append(errs, "Required keyword: '"+a.name+"' missing for "+e.classRepr())
		}
	}
	if args != nil && e.IsA(KFunc) && len(args) > len(e.kind.ArgTypes()) && !e.kind.isVarLenArgs() {
		errs = append(errs, "The number of provided arguments ("+itoa(len(args))+") is greater than the maximum number of supported arguments ("+itoa(len(e.kind.ArgTypes()))+")")
	}
	return errs
}

func (e *Expr) classRepr() string {
	return "<class 'sqlglot.expressions." + kindModule(e.kind) + "." + e.kind.Name() + "'>"
}

// metaValidateArgs is a transient metadata key set by function builders that mutate their argument
// list in place in Python (`args.pop(0)`, `args.insert(...)`, `del args[2:]`): the parser validates
// the built function against the caller's (mutated) list, which a Go builder cannot shrink or grow.
const metaValidateArgs = "\x00validate_args"

// WithValidateArgs records the post-mutation argument list of a function builder on e and returns e.
func WithValidateArgs(e *Expr, args []*Expr) *Expr {
	if e != nil {
		e.Meta()[metaValidateArgs] = args
	}
	return e
}

// callFuncBuilder invokes a FUNCTIONS builder and returns the built expression together with the
// argument list as Python's caller would see it after the call.
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
