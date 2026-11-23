package sqlengine

import (
	"math/big"
)

// smpFINAL means that an expression should not be simplified.
const smpFINAL = "final"

var smpSIMPLIFIABLE = []Kind{KBinary, KFunc, KLambda, KPredicate, KUnary}

type SimplifyOptions struct {
	// ConstantPropagation enables propagating column = literal equalities within conjunctions.
	ConstantPropagation bool
	Coalesce            bool
	// Dialect is the dialect used by the rules (nil means the default dialect).
	Dialect *Dialect
	// MaxDepth is accepted for API compatibility only; it is ignored.
	MaxDepth int
}

// Simplify rewrites an AST to simplify expressions, e.g. "TRUE AND TRUE" -> "TRUE". The
// expression is mutated in place.
//
// Errors the rules let escape (e.g. decimal division by zero for `1.0 / 0`, overflow in
// out-of-range date arithmetic) are raised as panics.
func Simplify(e *Expr, o SimplifyOptions) *Expr {
	return NewSimplifier(o.Dialect, true).Simplify(e, o.ConstantPropagation, o.Coalesce)
}

func simplifyExpr(e *Expr, d *Dialect) *Expr {
	return Simplify(e, SimplifyOptions{Dialect: d})
}

// smpUnsupportedUnit is raised (as a panic) for a date unit the date helpers do not support.
type smpUnsupportedUnit struct{ msg string }

func (e *smpUnsupportedUnit) Error() string { return e.msg }

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

// smpTypeAnnotator is the part of TypeAnnotator the Simplifier uses to annotate the
// expressions its rules produce.
type smpTypeAnnotator interface {
	// Clear resets the annotator's per-run caches.
	Clear()
	Annotate(e *Expr, annotateScope bool) *Expr
}

// smpNewTypeAnnotator builds the Simplifier's annotator: an empty schema in dialect d, without
// overwriting existing types. It is set by opt_annotate.go; while it is nil, the Simplifier does
// not annotate new expressions.
var smpNewTypeAnnotator func(d *Dialect) smpTypeAnnotator

type Simplifier struct {
	dialect                *Dialect
	annotateNewExpressions bool
	annotator              smpTypeAnnotator
}

// NewSimplifier returns a Simplifier for dialect d (nil means the default dialect).
// annotateNewExpressions makes rewritten expressions carry types.
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

func (s *Simplifier) Dialect() *Dialect { return s.dialect }

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

// simplifyFlatten merges nested connectors of the same kind:
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

// smpPropagateConstants propagates constants for conjunctions in DNF:
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

// smpDateRange is a [lo, hi) range of dates.
type smpDateRange struct{ lo, hi smpDT }

// smpDatetruncRange returns the [min, max) date range for a DATE_TRUNC equality comparison, or
// ok=false if a value can never be equal to `date` for `unit`.
func smpDatetruncRange(date smpDT, unit string, d *Dialect) (smpDateRange, bool) {
	floor := smpDatetimeFloor(date, unit, d)

	if !smpDTEq(date, floor) {
		// This will always be FALSE, except for NULL values.
		return smpDateRange{}, false
	}

	return smpDateRange{floor, smpIntervalOne(unit).addTo(floor)}, true
}

func smpDatetruncEqExpression(left *Expr, drange smpDateRange, targetType *Expr) *Expr {
	return AndExprOpts([]*Expr{
		smpBinop(KGTE, left, smpDateLiteral(drange.lo, targetType)),
		smpBinop(KLT, left, smpDateLiteral(drange.hi, targetType)),
	}, false, true)
}

// smpDatetruncEq rewrites DATE_TRUNC(unit, left) = date as a range check on left, or returns
// nil when date is not aligned to unit.
func smpDatetruncEq(left *Expr, date smpDT, unit string, d *Dialect, targetType *Expr) *Expr {
	drange, ok := smpDatetruncRange(date, unit, d)
	if !ok {
		return nil
	}

	return smpDatetruncEqExpression(left, drange, targetType)
}

// smpDatetruncNeq rewrites DATE_TRUNC(unit, left) <> date as `left < lo AND left >= hi`, or
// returns nil when date is not aligned to unit.
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

func smpAlwaysTrue(expression *Expr) bool {
	return (expression.IsA(KBoolean) && expression.ArgB("this")) ||
		(expression.IsA(KLiteral) && expression.IsNumber() && !smpIsZero(expression))
}

func smpAlwaysFalse(expression *Expr) bool {
	return smpIsFalse(expression) || smpIsNull(expression) || smpIsZero(expression)
}

func smpIsZero(expression *Expr) bool {
	if !expression.IsA(KLiteral) {
		return false
	}
	v, ok := smpToPy(expression).(smpNum)
	return ok && smpNumEq(v, smpIntNum(new(big.Int)))
}

func smpIsFalse(a *Expr) bool {
	return a.Is(KBoolean) && !a.ArgB("this")
}

func smpIsNull(a *Expr) bool { return a.Is(KNull) }

func smpEvalBoolean(expression *Expr, a, b any) *Expr {
	switch {
	case expression.IsA(KEQ, KIs):
		if expression.IsA(KIs) && expression.ArgB("negate") {
			return smpBooleanLiteral(!smpPyEq(a, b))
		}
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

// smpCastAsDate converts a date/datetime (dt != nil) or an ISO-format string to a date.
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

// smpCastAsDatetime converts a date/datetime (dt != nil) or an ISO-format string to a datetime.
func smpCastAsDatetime(dt *smpDT, s string) (smpDT, bool) {
	if dt != nil {
		if dt.isDatetime {
			return *dt, true
		}
		return smpDatetimeFromDate(*dt), true
	}
	return smpFromISOFormat(s)
}

// smpCastValue casts a date/datetime (dt != nil) or a string to the temporal type `to`. isNone
// marks a missing value; ok is false for missing values, empty strings and non-temporal types.
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

// smpDatetimeFloor truncates d to the start of unit. Weeks start at the dialect's WEEK_OFFSET;
// unsupported units panic with smpUnsupportedUnit.
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

	if result.isDatetime {
		result.hour, result.minute, result.second, result.usec = 0, 0, 0, 0
	}
	return result
}

func smpDateCeil(d smpDT, unit string, dialect *Dialect) smpDT {
	floor := smpDatetimeFloor(d, unit, dialect)

	if smpDTEq(floor, d) {
		return d
	}

	return smpIntervalOne(unit).addTo(floor)
}

func smpBooleanLiteral(condition bool) *Expr {
	if condition {
		return Boolean(true)
	}
	return Boolean(false)
}

var smpTemporalTypes = DataType_TEMPORAL_TYPES.Items()

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

func smpBinop(kind Kind, this, other *Expr) *Expr {
	this = this.Copy()
	other = other.Copy()
	if !this.IsA(kind) && !other.IsA(kind) {
		this = wrapIfKind(this, KBinary)
		other = wrapIfKind(other, KBinary)
	}
	return New(kind, "this", this, "expression", other)
}

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

// smpSetEntry is an element of an expression set or map: the hash is the one computed when the
// element was inserted (stored keys are never rehashed).
type smpSetEntry struct {
	e *Expr
	h uint64
}

// smpExprSet is an insertion-ordered set of expressions (structural equality, stored hashes).
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

// smpExprMap is an insertion-ordered map keyed by expressions (structural equality, stored
// hashes).
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
