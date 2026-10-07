package sqlengine

// Port of sqlglot/optimizer/simplify.py (sqlglot v30.13.0): module-level helpers and the
// Simplifier entry points. The rewrite rules live in opt_simplify_rules.go, the pseudo SQL
// generator `gen` in opt_simplify_gen.go and the Python number / date models in
// opt_simplify_num.go and opt_simplify_dates.go.

import (
	"math/big"
)

// smpFINAL means that an expression should not be simplified.
const smpFINAL = "final"

var smpSIMPLIFIABLE = []Kind{KBinary, KFunc, KLambda, KPredicate, KUnary}

// SimplifyOptions mirrors the keyword arguments of sqlglot.optimizer.simplify.simplify.
type SimplifyOptions struct {
	// ConstantPropagation mirrors constant_propagation.
	ConstantPropagation bool
	// Coalesce mirrors coalesce_simplification.
	Coalesce bool
	// Dialect mirrors dialect (nil means the default dialect).
	Dialect *Dialect
	// MaxDepth is accepted for API compatibility only: sqlglot 30.13's simplify has no max_depth
	// parameter, so it is ignored.
	MaxDepth int
}

// Simplify mirrors sqlglot.optimizer.simplify.simplify: rewrites a sqlglot AST to simplify
// expressions, e.g. "TRUE AND TRUE" -> "TRUE". The expression is mutated in place.
//
// Python exceptions that simplify lets escape (e.g. decimal.DivisionByZero for `1.0 / 0`,
// OverflowError for out-of-range date arithmetic) are raised as panics.
func Simplify(e *Expr, o SimplifyOptions) *Expr {
	return NewSimplifier(o.Dialect, true).Simplify(e, o.ConstantPropagation, o.Coalesce)
}

// simplifyExpr mirrors simplify(expression, dialect=d) with default options.
func simplifyExpr(e *Expr, d *Dialect) *Expr {
	return Simplify(e, SimplifyOptions{Dialect: d})
}

// smpUnsupportedUnit mirrors simplify.UnsupportedUnit.
type smpUnsupportedUnit struct{ msg string }

func (e *smpUnsupportedUnit) Error() string { return e.msg }

// smpCatch mirrors the @catch(ModuleNotFoundError, UnsupportedUnit) decorator: if fn raises
// UnsupportedUnit, the original expression is returned.
func smpCatch(expression *Expr, fn func() *Expr) (res *Expr) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(*smpUnsupportedUnit); ok {
				res = expression
				return
			}
			panic(r)
		}
	}()
	return fn()
}

// smpTypeAnnotator is the part of sqlglot.optimizer.annotate_types.TypeAnnotator used by the
// Simplifier (`annotate_types_on_change`).
type smpTypeAnnotator interface {
	// Clear mirrors TypeAnnotator.clear().
	Clear()
	// Annotate mirrors TypeAnnotator.annotate(expression, annotate_scope=annotateScope).
	Annotate(e *Expr, annotateScope bool) *Expr
}

// smpNewTypeAnnotator builds the Simplifier's annotator, mirroring
// TypeAnnotator(schema=ensure_schema(None, dialect=d), overwrite_types=False). It is wired by the
// annotate_types port; while it is nil, Simplifier behaves like annotate_new_expressions=False.
var smpNewTypeAnnotator func(d *Dialect) smpTypeAnnotator

// Simplifier mirrors sqlglot.optimizer.simplify.Simplifier.
type Simplifier struct {
	dialect                *Dialect
	annotateNewExpressions bool
	annotator              smpTypeAnnotator
}

// NewSimplifier mirrors Simplifier(dialect=d, annotate_new_expressions=...). d == nil means the
// default dialect.
func NewSimplifier(d *Dialect, annotateNewExpressions bool) *Simplifier {
	if d == nil {
		d = prototype("")
	}
	s := &Simplifier{dialect: d, annotateNewExpressions: annotateNewExpressions}
	if annotateNewExpressions && smpNewTypeAnnotator != nil {
		s.annotator = smpNewTypeAnnotator(d)
	}
	return s
}

// Dialect returns the Simplifier's dialect.
func (s *Simplifier) Dialect() *Dialect { return s.dialect }

// onChange mirrors the @annotate_types_on_change decorator applied to a rule that turned
// `expression` into `newExpression`.
func (s *Simplifier) onChange(expression, newExpression *Expr) *Expr {
	if newExpression == nil {
		return newExpression
	}
	if s.annotateNewExpressions && s.annotator != nil && !expression.Equal(newExpression) {
		s.annotator.Clear()

		// We annotate this to ensure new children nodes are also annotated
		newExpression = s.annotator.Annotate(newExpression, false)

		// Whatever expression the original expression is transformed into needs to preserve
		// the original type, otherwise the simplification could result in a different schema
		newExpression.SetType(expression.Type())
	}
	return newExpression
}

// simplifyFlatten mirrors simplify.flatten:
//
//	A AND (B AND C) -> A AND B AND C
//	A OR (B OR C) -> A OR B OR C
func simplifyFlatten(expression *Expr) *Expr {
	if expression.IsA(KConnector) {
		for _, a := range append([]arg(nil), expression.args...) {
			node, ok := a.val.(*Expr)
			if !ok {
				smpRaise("AttributeError", "object has no attribute 'unnest'")
			}
			child := unnestMethod(node)
			if child.IsA(expression.kind) {
				node.Replace(child)
			}
		}
	}
	return expression
}

// simplifyParens mirrors simplify.simplify_parens (d == nil means the default dialect).
func simplifyParens(expression *Expr, d *Dialect) *Expr {
	if !expression.IsA(KParen) {
		return expression
	}

	this := expression.This()
	parent := expression.Parent()
	parentIsPredicate := parent.IsA(KPredicate)

	if this.IsA(KSelect) {
		return expression
	}

	if parent.IsA(KSubqueryPredicate, KBracket) {
		return expression
	}

	if d == nil {
		d = prototype("")
	}
	if d.S.REQUIRES_PARENTHESIZED_STRUCT_ACCESS && parent.IsA(KDot) && parent.Right().IsA(KIdentifier, KStar) {
		return expression
	}

	if this.IsA(KPredicate) &&
		!(parentIsPredicate || parent.IsA(KNeg) || (parent.IsA(KBinary) && !parent.IsA(KConnector))) {
		return this
	}

	if !parent.IsA(KCondition, KBinary) ||
		parent.IsA(KParen) ||
		(!this.IsA(KBinary) && !(this.IsA(KNot, KIs) && parentIsPredicate)) ||
		(this.IsA(KAdd) && parent.IsA(KAdd)) ||
		(this.IsA(KMul) && parent.IsA(KMul)) ||
		(this.IsA(KMul) && parent.IsA(KAdd, KSub)) {
		return this
	}

	return expression
}

// smpPropagateConstants mirrors simplify.propagate_constants: propagate constants for
// conjunctions in DNF:
//
//	SELECT * FROM t WHERE a = b AND b = 5 becomes
//	SELECT * FROM t WHERE a = 5 AND b = 5
func smpPropagateConstants(expression *Expr, root bool) *Expr {
	if expression.IsA(KAnd) && (root || !expression.SameParent()) && normalizedCNF(expression, true) {
		type constant struct {
			id    *Expr
			value *Expr
		}
		constantMapping := newSmpExprMap[constant]()
		for expr := range WalkInScope(expression, func(node *Expr) bool { return node.IsA(KIf) }) {
			if expr.IsA(KEQ) {
				l, r := expr.Left(), expr.Right()

				// TODO: create a helper that can be used to detect nested literal expressions such
				// as CAST(123456 AS BIGINT), since we usually want to treat those as literals too
				if l.IsA(KColumn) && r.IsA(KLiteral) {
					constantMapping.set(l, constant{l, r})
				}
			}
		}

		if constantMapping.len() > 0 {
			for column := range FindAllInScope(expression, KColumn) {
				parent := column.Parent()
				c, ok := constantMapping.get(column)
				if ok && column != c.id && !(parent.IsA(KIs) && parent.Expression().IsA(KNull)) {
					column.Replace(c.value.Copy())
				}
			}
		}
	}

	return expression
}

func smpIsNumber(expression *Expr) bool { return expression.IsNumber() }

func smpIsInterval(expression *Expr) bool {
	if !expression.IsA(KInterval) {
		return false
	}
	_, ok := smpExtractInterval(expression)
	return ok
}

func smpIsNonnullConstant(expression *Expr) bool {
	return expression.IsA(KLiteral, KBoolean) || smpIsDateLiteral(expression)
}

func smpIsConstant(expression *Expr) bool {
	expr := expression
	if expression.IsA(KNeg) {
		expr = expression.This()
	}
	return expr.IsA(KLiteral, KBoolean, KNull) || smpIsDateLiteral(expr)
}

// smpDateRange mirrors DateRange = tuple[date, date].
type smpDateRange struct{ lo, hi smpDT }

// smpDatetruncRange mirrors simplify._datetrunc_range: the [min, max) date range for a DATE_TRUNC
// equality comparison, or ok=false if a value can never be equal to `date` for `unit`.
func smpDatetruncRange(date smpDT, unit string, d *Dialect) (smpDateRange, bool) {
	floor := smpDatetimeFloor(date, unit, d)

	if !smpDTEq(date, floor) {
		// This will always be False, except for NULL values.
		return smpDateRange{}, false
	}

	return smpDateRange{floor, smpIntervalOne(unit).addTo(floor)}, true
}

// smpDatetruncEqExpression mirrors simplify._datetrunc_eq_expression.
func smpDatetruncEqExpression(left *Expr, drange smpDateRange, targetType *Expr) *Expr {
	return AndExprOpts([]*Expr{
		smpBinop(KGTE, left, smpDateLiteral(drange.lo, targetType)),
		smpBinop(KLT, left, smpDateLiteral(drange.hi, targetType)),
	}, false, true)
}

// smpDatetruncEq mirrors simplify._datetrunc_eq.
func smpDatetruncEq(left *Expr, date smpDT, unit string, d *Dialect, targetType *Expr) *Expr {
	drange, ok := smpDatetruncRange(date, unit, d)
	if !ok {
		return nil
	}

	return smpDatetruncEqExpression(left, drange, targetType)
}

// smpDatetruncNeq mirrors simplify._datetrunc_neq.
func smpDatetruncNeq(left *Expr, date smpDT, unit string, d *Dialect, targetType *Expr) *Expr {
	drange, ok := smpDatetruncRange(date, unit, d)
	if !ok {
		return nil
	}

	return AndExprOpts([]*Expr{
		smpBinop(KLT, left, smpDateLiteral(drange.lo, targetType)),
		smpBinop(KGTE, left, smpDateLiteral(drange.hi, targetType)),
	}, false, true)
}

// smpAlwaysTrue mirrors simplify.always_true.
func smpAlwaysTrue(expression *Expr) bool {
	return (expression.IsA(KBoolean) && expression.ArgB("this")) ||
		(expression.IsA(KLiteral) && expression.IsNumber() && !smpIsZero(expression))
}

// smpAlwaysFalse mirrors simplify.always_false.
func smpAlwaysFalse(expression *Expr) bool {
	return smpIsFalse(expression) || smpIsNull(expression) || smpIsZero(expression)
}

// smpIsZero mirrors simplify.is_zero.
func smpIsZero(expression *Expr) bool {
	if !expression.IsA(KLiteral) {
		return false
	}
	v, ok := smpToPy(expression).(smpNum)
	return ok && smpNumEq(v, smpIntNum(new(big.Int)))
}

// smpIsFalse mirrors simplify.is_false.
func smpIsFalse(a *Expr) bool {
	return a.Is(KBoolean) && !a.ArgB("this")
}

// smpIsNull mirrors simplify.is_null.
func smpIsNull(a *Expr) bool { return a.Is(KNull) }

// smpEvalBoolean mirrors simplify.eval_boolean for Python values a, b (numbers, strings, dates).
func smpEvalBoolean(expression *Expr, a, b any) *Expr {
	switch {
	case expression.IsA(KEQ, KIs):
		return smpBooleanLiteral(smpPyEq(a, b))
	case expression.IsA(KNEQ):
		return smpBooleanLiteral(!smpPyEq(a, b))
	case expression.IsA(KGT):
		return smpBooleanLiteral(smpPyGt(a, b))
	case expression.IsA(KGTE):
		return smpBooleanLiteral(smpPyGe(a, b))
	case expression.IsA(KLT):
		return smpBooleanLiteral(smpPyLt(a, b))
	case expression.IsA(KLTE):
		return smpBooleanLiteral(smpPyLe(a, b))
	}
	return nil
}

// smpCastAsDate mirrors simplify.cast_as_date for a date/datetime (dt != nil) or a string.
func smpCastAsDate(dt *smpDT, s string) (smpDT, bool) {
	if dt != nil {
		if dt.isDatetime {
			return dt.date(), true
		}
		return *dt, true
	}
	v, ok := smpFromISOFormat(s)
	if !ok {
		return smpDT{}, false
	}
	return v.date(), true
}

// smpCastAsDatetime mirrors simplify.cast_as_datetime for a date/datetime (dt != nil) or a string.
func smpCastAsDatetime(dt *smpDT, s string) (smpDT, bool) {
	if dt != nil {
		if dt.isDatetime {
			return *dt, true
		}
		return smpDatetimeFromDate(*dt), true
	}
	return smpFromISOFormat(s)
}

// smpCastValue mirrors simplify.cast_value. value is a *smpDT, a string or nil (None).
func smpCastValue(dt *smpDT, s string, isNone bool, to *Expr) (smpDT, bool) {
	if isNone || (dt == nil && s == "") {
		return smpDT{}, false
	}
	if smpDataTypeIsType(to, DT_DATE) {
		return smpCastAsDate(dt, s)
	}
	if smpDataTypeIsType(to, smpTemporalTypes...) {
		return smpCastAsDatetime(dt, s)
	}
	return smpDT{}, false
}

// smpExtractDate mirrors simplify.extract_date.
func smpExtractDate(cast *Expr) (smpDT, bool) {
	var to *Expr
	if cast.IsA(KCast) {
		to = cast.ArgE("to")
	} else if cast.IsA(KTsOrDsToDate) && !cast.ArgB("format") {
		to = NewDataType(DT_DATE)
	} else {
		return smpDT{}, false
	}

	this := cast.This()
	if this.IsA(KLiteral) {
		return smpCastValue(nil, this.Name(), false, to)
	} else if this.IsA(KCast, KTsOrDsToDate) {
		v, ok := smpExtractDate(this)
		if !ok {
			return smpCastValue(nil, "", true, to)
		}
		return smpCastValue(&v, "", false, to)
	}
	return smpDT{}, false
}

func smpIsDateLiteral(expression *Expr) bool {
	_, ok := smpExtractDate(expression)
	return ok
}

// smpExtractInterval mirrors simplify.extract_interval (ok=false means None).
func smpExtractInterval(expression *Expr) (rd smpRelDelta, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			switch r.(type) {
			case *ValueError, *smpUnsupportedUnit:
				rd, ok = smpRelDelta{}, false
			default:
				panic(r)
			}
		}
	}()
	this := expression.This()
	if this == nil {
		smpRaise("AttributeError", "'NoneType' object has no attribute 'to_py'")
	}
	n := smpPyInt(smpToPy(this))
	unit := pyLower(expression.Text("unit"))
	return smpInterval(unit, n), true
}

// smpExtractType mirrors simplify.extract_type.
func smpExtractType(expressions ...*Expr) *Expr {
	var targetType *Expr
	for _, expression := range expressions {
		if expression.IsA(KCast) {
			targetType = expression.ArgE("to")
		} else {
			targetType = expression.Type()
		}
		if targetType != nil {
			break
		}
	}

	return targetType
}

// smpDateLiteral mirrors simplify.date_literal.
func smpDateLiteral(date smpDT, targetType *Expr) *Expr {
	var to any = targetType
	if targetType == nil || !smpDataTypeIsType(targetType, smpTemporalTypes...) {
		if date.isDatetime {
			to = DT_DATETIME
		} else {
			to = DT_DATE
		}
	}

	return CastExpr(LiteralString(date.String()), to, true, nil)
}

// smpDatetimeFloor mirrors simplify.datetime_floor.
func smpDatetimeFloor(d smpDT, unit string, dialect *Dialect) smpDT {
	// Truncate sub-day units — only valid for datetime inputs
	if d.isDatetime {
		switch unit {
		case "hour":
			d.minute, d.second, d.usec = 0, 0, 0
			return d
		case "minute":
			d.second, d.usec = 0, 0
			return d
		case "second":
			d.usec = 0
			return d
		case "millisecond":
			d.usec = (d.usec / 1000) * 1000
			return d
		case "microsecond":
			return d
		}
	}

	// Truncate date-level units, shared for both date and datetime
	var result smpDT
	switch unit {
	case "year":
		result = d.replaceYMD(int64(d.year), 1, 1)
	case "quarter":
		switch {
		case d.month <= 3:
			result = d.replaceYMD(int64(d.year), 1, 1)
		case d.month <= 6:
			result = d.replaceYMD(int64(d.year), 4, 1)
		case d.month <= 9:
			result = d.replaceYMD(int64(d.year), 7, 1)
		default:
			result = d.replaceYMD(int64(d.year), 10, 1)
		}
	case "month":
		result = d.replaceYMD(int64(d.year), d.month, 1)
	case "week":
		// Assuming week starts on Monday (0) and ends on Sunday (6)
		days := int64(d.weekday() - dialect.S.WEEK_OFFSET)
		result = smpAddTimedelta(d, smpNewTimedelta(days, 0, 0, 0, 0).neg())
	case "day":
		result = d
	default:
		panic(&smpUnsupportedUnit{msg: "Unsupported unit: " + unit})
	}

	// For datetime inputs, zero out the time component after date-level truncation
	if result.isDatetime {
		result.hour, result.minute, result.second, result.usec = 0, 0, 0, 0
	}
	return result
}

// smpDateCeil mirrors simplify.date_ceil.
func smpDateCeil(d smpDT, unit string, dialect *Dialect) smpDT {
	floor := smpDatetimeFloor(d, unit, dialect)

	if smpDTEq(floor, d) {
		return d
	}

	return smpIntervalOne(unit).addTo(floor)
}

// smpBooleanLiteral mirrors simplify.boolean_literal.
func smpBooleanLiteral(condition bool) *Expr {
	if condition {
		return Boolean(true)
	}
	return Boolean(false)
}

// ---------------------------------------------------------------------------------------------
// Small helpers (not part of simplify.py)
// ---------------------------------------------------------------------------------------------

var smpTemporalTypes = DataType_TEMPORAL_TYPES.Items()

// smpDataTypeIsType mirrors DataType.is_type(*dtypes) for plain DType arguments.
func smpDataTypeIsType(dt *Expr, dtypes ...DType) bool {
	if dt == nil {
		return false
	}
	this := dt.Arg("this")
	for _, d := range dtypes {
		var matches bool
		if this == any(DT_USERDEFINED) || d == DT_USERDEFINED {
			matches = dt.Equal(NewDataType(d))
		} else {
			matches = this == any(d)
		}
		if matches {
			return true
		}
	}
	return false
}

// smpIsType mirrors Expr.is_type(*dtypes) (DataType.is_type / Cast.is_type overrides included).
func smpIsType(e *Expr, dtypes ...DType) bool {
	if e == nil {
		return false
	}
	if e.kind.isDataType() {
		return smpDataTypeIsType(e, dtypes...)
	}
	if e.IsA(KCast) {
		return smpDataTypeIsType(e.ArgE("to"), dtypes...)
	}
	return e.typ != nil && smpDataTypeIsType(e.typ, dtypes...)
}

// smpBinop mirrors Expr._binop(klass, other) (used by the Python operators <, >=, .eq(), .is_()).
func smpBinop(kind Kind, this, other *Expr) *Expr {
	this = this.Copy()
	other = other.Copy()
	if !this.IsA(kind) && !other.IsA(kind) {
		this = wrapIfKind(this, KBinary)
		other = wrapIfKind(other, KBinary)
	}
	return New(kind, "this", this, "expression", other)
}

// smpWhileChanging mirrors sqlglot.helper.while_changing.
func smpWhileChanging(expression *Expr, fn func(*Expr) *Expr) *Expr {
	for {
		startHash := smpHash(expression)
		expression = fn(expression)
		endHash := smpHash(expression)

		if startHash == endHash {
			break
		}
	}
	return expression
}

func smpHash(e *Expr) uint64 {
	if e == nil {
		return 0
	}
	return e.Hash()
}

// smpSetEntry is an element of a Python set/dict keyed by expressions: the hash is the one
// computed when the element was inserted (Python never rehashes stored keys).
type smpSetEntry struct {
	e *Expr
	h uint64
}

// smpExprSet mirrors a Python set of expressions (structural equality, stored hashes).
type smpExprSet struct {
	entries []smpSetEntry
	index   map[uint64][]int
}

func newSmpExprSet(exprs ...*Expr) *smpExprSet {
	s := &smpExprSet{index: map[uint64][]int{}}
	for _, e := range exprs {
		s.add(e)
	}
	return s
}

// lookup mirrors set_lookkey: an entry with the same stored hash that is the same object or
// compares equal.
func (s *smpExprSet) lookup(e *Expr, h uint64) int {
	for _, i := range s.index[h] {
		if s.entries[i].e == e || s.entries[i].e.Equal(e) {
			return i
		}
	}
	return -1
}

func (s *smpExprSet) add(e *Expr) {
	h := e.Hash()
	if s.lookup(e, h) >= 0 {
		return
	}
	s.index[h] = append(s.index[h], len(s.entries))
	s.entries = append(s.entries, smpSetEntry{e, h})
}

func (s *smpExprSet) has(e *Expr) bool { return s.lookup(e, e.Hash()) >= 0 }

func (s *smpExprSet) len() int { return len(s.entries) }

// properSubsetOf mirrors `s < o`.
func (s *smpExprSet) properSubsetOf(o *smpExprSet) bool {
	if s.len() >= o.len() {
		return false
	}
	for _, en := range s.entries {
		if o.lookup(en.e, en.h) < 0 {
			return false
		}
	}
	return true
}

// smpExprMap mirrors a Python dict keyed by expressions (structural equality, stored hashes).
type smpExprMap[V any] struct {
	keys   *smpExprSet
	values []V
}

func newSmpExprMap[V any]() *smpExprMap[V] { return &smpExprMap[V]{keys: newSmpExprSet()} }

func (m *smpExprMap[V]) get(e *Expr) (V, bool) {
	if i := m.keys.lookup(e, e.Hash()); i >= 0 {
		return m.values[i], true
	}
	var zero V
	return zero, false
}

func (m *smpExprMap[V]) set(e *Expr, v V) {
	if i := m.keys.lookup(e, e.Hash()); i >= 0 {
		m.values[i] = v
		return
	}
	m.keys.add(e)
	m.values = append(m.values, v)
}

func (m *smpExprMap[V]) len() int { return len(m.values) }
