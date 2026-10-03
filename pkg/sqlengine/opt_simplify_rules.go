package sqlengine

// Port of the Simplifier class of sqlglot/optimizer/simplify.py: the driver (simplify,
// _simplify) and every rewrite rule. Rules decorated with @annotate_types_on_change in Python
// are exported wrappers calling s.onChange around the unexported implementation.

import "fmt"

// Value ranges for byte-sized signed/unsigned integers
const (
	smpTINYINT_MIN  = -128
	smpTINYINT_MAX  = 127
	smpUTINYINT_MIN = 0
	smpUTINYINT_MAX = 255
)

var smpCOMPLEMENT_COMPARISONS = map[Kind]Kind{
	KLT:  KGTE,
	KGT:  KLTE,
	KLTE: KGT,
	KGTE: KLT,
	KEQ:  KNEQ,
	KNEQ: KEQ,
}

var smpCOMPLEMENT_SUBQUERY_PREDICATES = map[Kind]Kind{
	KAll: KAny,
	KAny: KAll,
}

var (
	smpLT_LTE       = []Kind{KLT, KLTE}
	smpGT_GTE       = []Kind{KGT, KGTE}
	smpCOMPARISONS  = []Kind{KLT, KLTE, KGT, KGTE, KEQ, KNEQ, KIs}
	smpCONNECTOR_OK = []Kind{KBoolean, KLiteral, KNull, KLT, KLTE, KGT, KGTE, KEQ, KNEQ, KIs}
)

var smpINVERSE_COMPARISONS = map[Kind]Kind{
	KLT:  KGT,
	KGT:  KLT,
	KLTE: KGTE,
	KGTE: KLTE,
}

var (
	smpNONDETERMINISTIC = []Kind{KRand, KRandn}
	smpAND_OR           = []Kind{KAnd, KOr}
)

var smpINVERSE_DATE_OPS = map[Kind]Kind{
	KDateAdd:     KSub,
	KDateSub:     KAdd,
	KDatetimeAdd: KSub,
	KDatetimeSub: KAdd,
}

var smpINVERSE_OPS = map[Kind]Kind{
	KDateAdd:     KSub,
	KDateSub:     KAdd,
	KDatetimeAdd: KSub,
	KDatetimeSub: KAdd,
	KAdd:         KSub,
	KSub:         KAdd,
}

var (
	smpNULL_OK = []Kind{KNullSafeEQ, KNullSafeNEQ, KPropertyEQ}
	smpCONCATS = []Kind{KConcat, KDPipe}
)

// smpDatetruncBinaryTransform mirrors DateTruncBinaryTransform.
type smpDatetruncBinaryTransform func(l *Expr, dt smpDT, u string, d *Dialect, t *Expr) *Expr

var smpDATETRUNC_BINARY_COMPARISONS = map[Kind]smpDatetruncBinaryTransform{
	KLT: func(l *Expr, dt smpDT, u string, d *Dialect, t *Expr) *Expr {
		var v smpDT
		if floor := smpDatetimeFloor(dt, u, d); smpDTEq(dt, floor) {
			v = dt
		} else {
			v = smpIntervalOne(u).addTo(smpDatetimeFloor(dt, u, d))
		}
		return smpBinop(KLT, l, smpDateLiteral(v, t))
	},
	KGT: func(l *Expr, dt smpDT, u string, d *Dialect, t *Expr) *Expr {
		return smpBinop(KGTE, l, smpDateLiteral(smpIntervalOne(u).addTo(smpDatetimeFloor(dt, u, d)), t))
	},
	KLTE: func(l *Expr, dt smpDT, u string, d *Dialect, t *Expr) *Expr {
		return smpBinop(KLT, l, smpDateLiteral(smpIntervalOne(u).addTo(smpDatetimeFloor(dt, u, d)), t))
	},
	KGTE: func(l *Expr, dt smpDT, u string, d *Dialect, t *Expr) *Expr {
		return smpBinop(KGTE, l, smpDateLiteral(smpDateCeil(dt, u, d), t))
	},
	KEQ:  smpDatetruncEq,
	KNEQ: smpDatetruncNeq,
}

var smpDATETRUNCS = []Kind{KDateTrunc, KTimestampTrunc}

// CROSS joins result in an empty table if the right table is empty.
// So we can only simplify certain types of joins to CROSS.
// Or in other words, LEFT JOIN x ON TRUE != CROSS JOIN x
var smpJOINS = map[[2]string]bool{
	{"", ""}:           true,
	{"", "INNER"}:      true,
	{"RIGHT", ""}:      true,
	{"RIGHT", "OUTER"}: true,
}

func smpKindOf(e *Expr) Kind {
	if e == nil {
		return KNone
	}
	return e.kind
}

// Simplify mirrors Simplifier.simplify.
func (s *Simplifier) Simplify(expression *Expr, constantPropagation, coalesceSimplification bool) *Expr {
	var wheres, joins []*Expr

	prune := func(n *Expr) bool { return n.IsA(KCondition) || truthy(n.MetaGet(smpFINAL)) }
	for node := range expression.Walk(true, prune) {
		if truthy(node.MetaGet(smpFINAL)) {
			continue
		}

		// group by expressions cannot be simplified, for example
		// select x + 1 + 1 FROM y GROUP BY x + 1 + 1
		// the projection must exactly match the group by key
		group := node.Arg("group")

		if truthy(group) && propOwner_selects[node.kind] != KNone {
			g, ok := group.(*Expr)
			if !ok {
				smpRaise("AttributeError", "object has no attribute 'expressions'")
			}
			groups := newSmpExprSet(g.Expressions()...)
			g.Meta()[smpFINAL] = true

			for _, sel := range node.Selects() {
				for n := range sel.Walk(true, nil) {
					if groups.has(n) {
						sel.Meta()[smpFINAL] = true
						break
					}
				}
			}

			having := node.ArgE("having")

			if having != nil {
				for n := range having.Walk(true, nil) {
					if groups.has(n) {
						having.Meta()[smpFINAL] = true
						break
					}
				}
			}
		}

		if node.IsA(KCondition) {
			simplified := smpWhileChanging(node, func(e *Expr) *Expr {
				return s.simplifyOnce(e, constantPropagation, coalesceSimplification)
			})

			if node == expression {
				expression = simplified
			}
		} else if node.IsA(KWhere) {
			wheres = append(wheres, node)
		} else if node.IsA(KJoin) {
			// snowflake match_conditions have very strict ordering rules
			if match := node.ArgE("match_condition"); match != nil {
				match.Meta()[smpFINAL] = true
			}

			joins = append(joins, node)
		}
	}

	for _, where := range wheres {
		if smpAlwaysTrue(where.This()) {
			where.Pop()
		}
	}
	for _, join := range joins {
		if smpAlwaysTrue(join.ArgE("on")) &&
			!join.ArgB("using") &&
			!join.ArgB("method") &&
			smpJOINS[[2]string{join.SideText(), join.KindText()}] {
			join.ArgE("on").Pop()
			join.Set("side", nil)
			join.Set("kind", "CROSS")
		}
	}

	return expression
}

// simplifyOnce mirrors Simplifier._simplify.
func (s *Simplifier) simplifyOnce(expression *Expr, constantPropagation, coalesceSimplification bool) *Expr {
	type postItem struct{ node, parent *Expr }
	preTransformationStack := []*Expr{expression}
	var children []*Expr
	var postTransformationStack []postItem
	var node *Expr

	for len(preTransformationStack) > 0 {
		original := preTransformationStack[len(preTransformationStack)-1]
		preTransformationStack = preTransformationStack[:len(preTransformationStack)-1]
		node = original

		if !node.IsA(smpSIMPLIFIABLE...) {
			if node.IsA(KQuery) {
				s.Simplify(node, constantPropagation, coalesceSimplification)
			}
			continue
		}

		parent := node.Parent()
		root := node == expression

		node = s.RewriteBetween(node)
		node = s.UniqSort(node, root)
		node = s.AbsorbAndEliminate(node, root)
		node = s.SimplifyConcat(node)
		node = s.SimplifyConditionals(node)

		if constantPropagation {
			node = smpPropagateConstants(node, root)
		}

		if node != original {
			original.Replace(node)
		}

		children = node.appendChildren(children[:0], true)
		for _, n := range children {
			if !truthy(n.MetaGet(smpFINAL)) {
				preTransformationStack = append(preTransformationStack, n)
			}
		}
		postTransformationStack = append(postTransformationStack, postItem{node, parent})
	}

	for len(postTransformationStack) > 0 {
		item := postTransformationStack[len(postTransformationStack)-1]
		postTransformationStack = postTransformationStack[:len(postTransformationStack)-1]
		original, parent := item.node, item.parent
		root := original == expression

		// Resets parent, arg_key, index pointers– this is needed because some of the
		// previous transformations mutate the AST, leading to an inconsistent state.
		// We only fix pointers instead of calling `set` because the values are unchanged
		// (Python pops None-valued args from the dict directly, without invalidating hashes).
		kept := original.args[:0:0]
		for _, a := range original.args {
			if a.val == nil {
				continue
			}
			kept = append(kept, a)
			original.setParent(a.key, a.val, -1)
		}
		original.args = kept

		// Post-order transformations
		node = s.SimplifyNot(original)
		node = simplifyFlatten(node)
		node = s.SimplifyConnectors(node, root)
		node = s.RemoveComplements(node, root)

		if coalesceSimplification {
			node = s.SimplifyCoalesce(node)
		}
		node.parent = parent

		node = s.SimplifyLiterals(node, root)
		node = s.SimplifyEquality(node)
		node = simplifyParens(node, s.dialect)
		node = s.SimplifyDatetrunc(node)
		node = s.SortComparison(node)
		node = s.SimplifyStartswith(node)

		if node != original {
			original.Replace(node)
		}
	}

	return node
}

// RewriteBetween mirrors Simplifier.rewrite_between: rewrite x between y and z to
// x >= y AND x <= z. This is done because comparison simplification is only done on lt/lte/gt/gte.
func (s *Simplifier) RewriteBetween(expression *Expr) *Expr {
	return s.onChange(expression, s.rewriteBetween(expression))
}

func (s *Simplifier) rewriteBetween(expression *Expr) *Expr {
	if expression.IsA(KBetween) {
		negate := expression.Parent().IsA(KNot)

		expression = AndExprOpts([]*Expr{
			New(KGTE, "this", expression.This().Copy(), "expression", expression.ArgE("low")),
			New(KLTE, "this", expression.This().Copy(), "expression", expression.ArgE("high")),
		}, false, true)

		if negate {
			expression = ParenExpr(expression, false)
		}
	}

	return expression
}

// SimplifyNot mirrors Simplifier.simplify_not (Demorgan's Law):
//
//	NOT (x OR y) -> NOT x AND NOT y
//	NOT (x AND y) -> NOT x OR NOT y
func (s *Simplifier) SimplifyNot(expression *Expr) *Expr {
	return s.onChange(expression, s.simplifyNot(expression))
}

func (s *Simplifier) simplifyNot(expression *Expr) *Expr {
	if expression.IsA(KNot) {
		this := expression.This()
		if smpIsNull(this) {
			return AndExprOpts([]*Expr{Null(), Boolean(true)}, false, true)
		}
		if complement, ok := smpCOMPLEMENT_COMPARISONS[smpKindOf(this)]; ok {
			right := this.Expression()
			if complementSubqueryPredicate, ok := smpCOMPLEMENT_SUBQUERY_PREDICATES[smpKindOf(right)]; ok {
				right = New(complementSubqueryPredicate, "this", right.This())
			}

			return New(complement, "this", this.This(), "expression", right)
		}
		if this.IsA(KParen) {
			condition := this.Unnest()
			if condition.IsA(KAnd) {
				return ParenExpr(
					OrExprOpts([]*Expr{
						NotExpr(condition.Left(), false),
						NotExpr(condition.Right(), false),
					}, false, true),
					false,
				)
			}
			if condition.IsA(KOr) {
				return ParenExpr(
					AndExprOpts([]*Expr{
						NotExpr(condition.Left(), false),
						NotExpr(condition.Right(), false),
					}, false, true),
					false,
				)
			}
			if smpIsNull(condition) {
				return AndExprOpts([]*Expr{Null(), Boolean(true)}, false, true)
			}
		}
		if smpAlwaysTrue(this) {
			return Boolean(false)
		}
		if smpIsFalse(this) {
			return Boolean(true)
		}
		if this.IsA(KNot) && s.dialect.S.SAFE_TO_ELIMINATE_DOUBLE_NEGATION {
			inner := this.This()
			if smpIsType(inner, DT_BOOLEAN) {
				// double negation
				// NOT NOT x -> x, if x is BOOLEAN type
				return inner
			}
		}
	}
	return expression
}

// SimplifyConnectors mirrors Simplifier.simplify_connectors.
func (s *Simplifier) SimplifyConnectors(expression *Expr, root bool) *Expr {
	return s.onChange(expression, s.simplifyConnectors(expression, root))
}

func (s *Simplifier) simplifyConnectors(expression *Expr, root bool) *Expr {
	simplifyConnectors := func(expression, left, right *Expr) *Expr {
		if expression.IsA(KAnd) {
			if smpIsFalse(left) || smpIsFalse(right) {
				return Boolean(false)
			}
			if smpIsZero(left) || smpIsZero(right) {
				return Boolean(false)
			}
			if (smpIsNull(left) && smpIsNull(right)) ||
				(smpIsNull(left) && smpAlwaysTrue(right)) ||
				(smpAlwaysTrue(left) && smpIsNull(right)) {
				return Null()
			}
			if smpAlwaysTrue(left) && smpAlwaysTrue(right) {
				return Boolean(true)
			}
			if smpAlwaysTrue(left) {
				return right
			}
			if smpAlwaysTrue(right) {
				return left
			}
			return s.SimplifyComparison(expression, left, right, false)
		} else if expression.IsA(KOr) {
			if smpAlwaysTrue(left) || smpAlwaysTrue(right) {
				return Boolean(true)
			}
			if (smpIsNull(left) && smpIsNull(right)) ||
				(smpIsNull(left) && smpAlwaysFalse(right)) ||
				(smpAlwaysFalse(left) && smpIsNull(right)) {
				return Null()
			}
			if smpIsFalse(left) {
				return right
			}
			if smpIsFalse(right) {
				return left
			}
			return s.SimplifyComparison(expression, left, right, true)
		}
		return nil
	}

	if expression.IsA(KConnector) {
		originalParent := expression.Parent()
		expression = s.flatSimplify(expression, simplifyConnectors, root)

		// If we reduced a connector to, e.g., a column (t1 AND ... AND tn -> Tk), then we need
		// to ensure that the resulting type is boolean. We know this is true only for connectors,
		// boolean values and columns that are essentially operands to a connector:
		//
		// A AND (((B)))
		//          ~ this is safe to keep because it will eventually be part of another connector
		if !expression.IsA(KConnector, KBoolean) && !smpIsType(expression, DT_BOOLEAN) {
			for {
				if originalParent.IsA(KConnector) {
					break
				}
				if !originalParent.IsA(KParen) {
					expression = expression.ExprAnd([]*Expr{Boolean(true)}, false, true)
					break
				}

				originalParent = originalParent.Parent()
			}
		}
	}

	return expression
}

// smpTwoArgs mirrors `a, b = expression.args.values()`.
func smpTwoArgs(e *Expr) (*Expr, *Expr) {
	if len(e.args) != 2 {
		panic(&ValueError{Msg: fmt.Sprintf("expected 2 values to unpack, got %d", len(e.args))})
	}
	a, ok1 := e.args[0].val.(*Expr)
	b, ok2 := e.args[1].val.(*Expr)
	if !ok1 || !ok2 {
		smpRaise("AttributeError", "comparison operand is not an expression")
	}
	return a, b
}

// SimplifyComparison mirrors Simplifier._simplify_comparison.
func (s *Simplifier) SimplifyComparison(expression, left, right *Expr, or bool) *Expr {
	return s.onChange(expression, s.simplifyComparison(expression, left, right, or))
}

func (s *Simplifier) simplifyComparison(expression, left, right *Expr, or bool) *Expr {
	if left.IsA(smpCOMPARISONS...) && right.IsA(smpCOMPARISONS...) {
		ll, lr := smpTwoArgs(left)
		rl, rr := smpTwoArgs(right)

		largs := newSmpExprSet(ll, lr)
		rargs := newSmpExprSet(rl, rr)

		// matching = largs & rargs (elements are taken from the smaller set)
		so, other := largs, rargs
		if other.len() > so.len() {
			so, other = other, so
		}
		matching := newSmpExprSet()
		for _, en := range other.entries {
			if so.lookup(en.e, en.h) >= 0 {
				matching.add(en.e)
			}
		}
		columns := newSmpExprSet()
		for _, en := range matching.entries {
			if !smpIsConstant(en.e) && en.e.Find(smpNONDETERMINISTIC...) == nil {
				columns.add(en.e)
			}
		}

		if matching.len() > 0 && columns.len() > 0 {
			first := func(set *smpExprSet) *Expr {
				for _, en := range set.entries {
					if columns.lookup(en.e, en.h) < 0 {
						return en.e
					}
				}
				return nil
			}
			lExpr := first(largs)
			if lExpr == nil {
				return expression
			}
			rExpr := first(rargs)
			if rExpr == nil {
				return expression
			}

			var l, r any
			if lExpr.IsNumber() && rExpr.IsNumber() {
				l = smpToPyNum(lExpr)
				r = smpToPyNum(rExpr)
			} else if lExpr.IsString() && rExpr.IsString() {
				l = lExpr.Name()
				r = rExpr.Name()
			} else {
				ld, ok := smpExtractDate(lExpr)
				if !ok {
					return nil
				}
				rd, ok := smpExtractDate(rExpr)
				if !ok {
					return nil
				}
				// python won't compare date and datetime, but many engines will upcast
				ld, _ = smpCastAsDatetime(&ld, "")
				rd, _ = smpCastAsDatetime(&rd, "")
				l, r = ld, rd
			}

			type operand struct {
				e *Expr
				v any
			}
			permutations := [][2]operand{
				{{left, l}, {right, r}},
				{{right, r}, {left, l}},
			}
			for _, p := range permutations {
				a, av, b, bv := p[0].e, p[0].v, p[1].e, p[1].v
				if a.IsA(smpLT_LTE...) && b.IsA(smpLT_LTE...) {
					var c bool
					if or {
						c = smpPyGt(av, bv)
					} else {
						c = smpPyLe(av, bv)
					}
					if c {
						return left
					}
					return right
				}
				if a.IsA(smpGT_GTE...) && b.IsA(smpGT_GTE...) {
					var c bool
					if or {
						c = smpPyLt(av, bv)
					} else {
						c = smpPyGe(av, bv)
					}
					if c {
						return left
					}
					return right
				}

				// we can't ever shortcut to true because the column could be null
				if !or {
					if a.IsA(KLT) && b.IsA(smpGT_GTE...) {
						if smpPyLe(av, bv) {
							return Boolean(false)
						}
					} else if a.IsA(KGT) && b.IsA(smpLT_LTE...) {
						if smpPyGe(av, bv) {
							return Boolean(false)
						}
					} else if a.IsA(KEQ) {
						if b.IsA(KLT) {
							if smpPyGe(av, bv) {
								return Boolean(false)
							}
							return a
						}
						if b.IsA(KLTE) {
							if smpPyGt(av, bv) {
								return Boolean(false)
							}
							return a
						}
						if b.IsA(KGT) {
							if smpPyLe(av, bv) {
								return Boolean(false)
							}
							return a
						}
						if b.IsA(KGTE) {
							if smpPyLt(av, bv) {
								return Boolean(false)
							}
							return a
						}
						if b.IsA(KNEQ) {
							if smpPyEq(av, bv) {
								return Boolean(false)
							}
							return a
						}
					}
				}
			}
		}
	}
	return nil
}

// RemoveComplements mirrors Simplifier.remove_complements:
//
//	A AND NOT A -> FALSE (only for non-NULL A)
//	A OR NOT A -> TRUE (only for non-NULL A)
func (s *Simplifier) RemoveComplements(expression *Expr, root bool) *Expr {
	return s.onChange(expression, s.removeComplements(expression, root))
}

func (s *Simplifier) removeComplements(expression *Expr, root bool) *Expr {
	if expression.IsA(smpAND_OR...) && (root || !expression.SameParent()) {
		ops := newSmpExprSet(expression.Flatten(true)...)
		for _, en := range ops.entries {
			op := en.e
			if op.IsA(KNot) && ops.has(op.This()) {
				if v, ok := expression.MetaGet("nonnull").(bool); ok && v {
					if expression.IsA(KAnd) {
						return Boolean(false)
					}
					return Boolean(true)
				}
			}
		}
	}

	return expression
}

// UniqSort mirrors Simplifier.uniq_sort: uniq and sort a connector.
//
//	C AND A AND B AND B -> A AND B AND C
func (s *Simplifier) UniqSort(expression *Expr, root bool) *Expr {
	return s.onChange(expression, s.uniqSort(expression, root))
}

type smpSQLItem struct {
	sql string
	e   *Expr
}

func (s *Simplifier) uniqSort(expression *Expr, root bool) *Expr {
	if expression.IsA(KConnector) && (root || !expression.SameParent()) {
		flattened := expression.Flatten(true)

		var resultFunc func([]*Expr) *Expr
		var deduped *omap[*Expr]
		var arr []smpSQLItem
		if expression.IsA(KXor) {
			resultFunc = func(es []*Expr) *Expr { return XorExprOpts(es, false, true) }
			// Do not deduplicate XOR as A XOR A != A if A == True
			for _, e := range flattened {
				arr = append(arr, smpSQLItem{smpGen(e, false), e})
			}
		} else {
			if expression.IsA(KAnd) {
				resultFunc = func(es []*Expr) *Expr { return AndExprOpts(es, false, true) }
			} else {
				resultFunc = func(es []*Expr) *Expr { return OrExprOpts(es, false, true) }
			}
			deduped = newOMap[*Expr]()
			for _, e := range flattened {
				deduped.Set(smpGen(e, false), e)
			}
			for _, k := range deduped.Keys() {
				v, _ := deduped.Get(k)
				arr = append(arr, smpSQLItem{k, v})
			}
		}

		// check if the operands are already sorted, if not sort them
		// A AND C AND B -> A AND B AND C
		isSorted := true
		for i := 1; i < len(arr); i++ {
			if arr[i].sql < arr[i-1].sql {
				sorted := append([]smpSQLItem(nil), arr...)
				smpPySort(sorted, smpSQLItemLess)
				es := make([]*Expr, len(sorted))
				for j, it := range sorted {
					es[j] = it.e
				}
				expression = resultFunc(es)
				isSorted = false
				break
			}
		}
		if isSorted {
			// we didn't have to sort but maybe we need to dedup
			if deduped != nil && deduped.Len() > 0 && deduped.Len() < len(flattened) {
				uniqueOperand := flattened[0]
				if deduped.Len() == 1 {
					expression = uniqueOperand.ExprAnd([]*Expr{Boolean(true)}, false, true)
				} else {
					expression = resultFunc(deduped.Values())
				}
			}
		}
	}

	return expression
}

// smpSQLItemLess mirrors `(sql_a, a) < (sql_b, b)` for tuples of (str, Expr): ties on the SQL
// fall back to Expr.__lt__, which builds an (always truthy) LT expression.
func smpSQLItemLess(x, y smpSQLItem) bool {
	if x.sql != y.sql {
		return x.sql < y.sql
	}
	if x.e == y.e || x.e.Equal(y.e) {
		return false
	}
	return true
}

// smpPySort mirrors CPython 3.13's list.sort for runs shorter than MAX_MINRUN (count_run followed
// by a binary insertion sort); this matters only for inconsistent comparators.
func smpPySort[T any](a []T, lt func(x, y T) bool) {
	n := len(a)
	if n < 2 {
		return
	}
	run := smpCountRun(a, lt)
	if run < n {
		smpBinarySort(a, run, lt)
	}
}

func smpReverse[T any](a []T) {
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
}

func smpCountRun[T any](lo []T, lt func(x, y T) bool) int {
	nremaining := len(lo)
	n := 1
	// try ascending run first
	for ; n < nremaining; n++ {
		if lt(lo[n], lo[n-1]) {
			break
		}
	}
	if n == nremaining {
		return n
	}
	// lo[n] is strictly less
	if n > 1 {
		if lt(lo[0], lo[n-1]) {
			return n
		}
		smpReverse(lo[:n])
	}
	n++

	neq := 0
	reverseLastNeq := func() {
		if neq > 0 {
			neq++
			smpReverse(lo[n-neq : n])
			neq = 0
		}
	}
	for ; n < nremaining; n++ {
		if lt(lo[n], lo[n-1]) {
			reverseLastNeq()
		} else {
			if lt(lo[n-1], lo[n]) {
				break
			}
			neq++
		}
	}
	reverseLastNeq()
	smpReverse(lo[:n])

	for ; n < nremaining; n++ {
		if lt(lo[n], lo[n-1]) {
			break
		}
	}
	return n
}

func smpBinarySort[T any](a []T, ok int, lt func(x, y T) bool) {
	if ok == 0 {
		ok++
	}
	for ; ok < len(a); ok++ {
		l, r := 0, ok
		pivot := a[ok]
		for l < r {
			m := (l + r) >> 1
			if lt(pivot, a[m]) {
				r = m
			} else {
				l = m + 1
			}
		}
		copy(a[l+1:ok+1], a[l:ok])
		a[l] = pivot
	}
}

// AbsorbAndEliminate mirrors Simplifier.absorb_and_eliminate:
//
//	absorption:
//	    A AND (A OR B) -> A
//	    A OR (A AND B) -> A
//	    A AND (NOT A OR B) -> A AND B
//	    A OR (NOT A AND B) -> A OR B
//	elimination:
//	    (A AND B) OR (A AND NOT B) -> A
//	    (A OR B) AND (A OR NOT B) -> A
func (s *Simplifier) AbsorbAndEliminate(expression *Expr, root bool) *Expr {
	return s.onChange(expression, s.absorbAndEliminate(expression, root))
}

// smpFrozenset mirrors a frozenset of expressions used as a dict key.
type smpFrozenset struct{ entries []smpSetEntry }

func newSmpFrozenset(exprs ...*Expr) smpFrozenset {
	set := newSmpExprSet(exprs...)
	return smpFrozenset{entries: set.entries}
}

func (f smpFrozenset) hashKey() [3]uint64 {
	k := [3]uint64{uint64(len(f.entries))}
	for i, en := range f.entries {
		if i < 2 {
			k[i+1] = en.h
		}
	}
	if k[2] < k[1] {
		k[1], k[2] = k[2], k[1]
	}
	return k
}

// equal mirrors stored == probe for frozensets (stored entry hashes are used for the lookups).
func (f smpFrozenset) equal(probe smpFrozenset) bool {
	if len(f.entries) != len(probe.entries) || f.hashKey() != probe.hashKey() {
		return false
	}
	for _, en := range f.entries {
		found := false
		for _, p := range probe.entries {
			if p.h == en.h && (p.e == en.e || p.e.Equal(en.e)) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

type smpComplementPair struct{ other, complement *Expr }

type smpPairsMap struct {
	keys   []smpFrozenset
	values [][]smpComplementPair
	index  map[[3]uint64][]int
}

func (m *smpPairsMap) find(k smpFrozenset) int {
	for _, i := range m.index[k.hashKey()] {
		if m.keys[i].equal(k) {
			return i
		}
	}
	return -1
}

func (m *smpPairsMap) append(k smpFrozenset, v smpComplementPair) {
	if i := m.find(k); i >= 0 {
		m.values[i] = append(m.values[i], v)
		return
	}
	m.index[k.hashKey()] = append(m.index[k.hashKey()], len(m.keys))
	m.keys = append(m.keys, k)
	m.values = append(m.values, []smpComplementPair{v})
}

func (m *smpPairsMap) get(k smpFrozenset) []smpComplementPair {
	if i := m.find(k); i >= 0 {
		return m.values[i]
	}
	return nil
}

// smpTwoOperands mirrors `a, b = op.unnest_operands()`.
func smpTwoOperands(op *Expr) (*Expr, *Expr) {
	ops := optxUnnestOperands(op)
	if len(ops) != 2 {
		panic(&ValueError{Msg: fmt.Sprintf("expected 2 values to unpack, got %d", len(ops))})
	}
	return ops[0], ops[1]
}

func (s *Simplifier) absorbAndEliminate(expression *Expr, root bool) *Expr {
	if expression.IsA(smpAND_OR...) && (root || !expression.SameParent()) {
		kind := KAnd
		if expression.IsA(KAnd) {
			kind = KOr
		}

		ops := expression.Flatten(true)

		// Initialize lookup tables:
		// Set of all operands, used to find complements for absorption.
		opSet := newSmpExprSet()
		// Sub-operands, used to find subsets for absorption.
		subops := newSmpExprMap[[]*smpExprSet]()
		// Pairs of complements, used for elimination.
		pairs := &smpPairsMap{index: map[[3]uint64][]int{}}

		subopsAppend := func(key *Expr, set *smpExprSet) {
			list, _ := subops.get(key)
			subops.set(key, append(list, set))
		}

		// Populate the lookup tables
		for _, op := range ops {
			opSet.add(op)

			if !op.IsA(kind) {
				// In cases like: A OR (A AND B)
				// Subop will be: ^
				subopsAppend(op, newSmpExprSet(op))
				continue
			}

			// In cases like: (A AND B) OR (A AND B AND C)
			// Subops will be: ^     ^
			subset := newSmpExprSet(op.Flatten(true)...)
			for _, en := range subset.entries {
				subopsAppend(en.e, subset)
			}

			a, b := smpTwoOperands(op)
			if a.IsA(KNot) {
				pairs.append(newSmpFrozenset(a.This(), b), smpComplementPair{op, b})
			}
			if b.IsA(KNot) {
				pairs.append(newSmpFrozenset(a, b.This()), smpComplementPair{op, a})
			}
		}

		for _, op := range ops {
			if !op.IsA(kind) {
				continue
			}

			a, b := smpTwoOperands(op)

			// Absorb
			if a.IsA(KNot) && opSet.has(a.This()) {
				if kind == KAnd {
					a.Replace(Boolean(true))
				} else {
					a.Replace(Boolean(false))
				}
				continue
			}
			if b.IsA(KNot) && opSet.has(b.This()) {
				if kind == KAnd {
					b.Replace(Boolean(true))
				} else {
					b.Replace(Boolean(false))
				}
				continue
			}
			superset := newSmpExprSet(op.Flatten(true)...)
			absorbed := false
			for _, en := range superset.entries {
				subsets, _ := subops.get(en.e)
				for _, subset := range subsets {
					if subset.properSubsetOf(superset) {
						absorbed = true
						break
					}
				}
				if absorbed {
					break
				}
			}
			if absorbed {
				if kind == KAnd {
					op.Replace(Boolean(false))
				} else {
					op.Replace(Boolean(true))
				}
				continue
			}

			// Eliminate
			for _, pc := range pairs.get(newSmpFrozenset(a, b)) {
				op.Replace(pc.complement)
				pc.other.Replace(pc.complement)
			}
		}
	}

	return expression
}

// SimplifyEquality mirrors Simplifier.simplify_equality: use the subtraction and addition
// properties of equality to simplify expressions:
//
//	x + 1 = 3 becomes x = 2
//
// Subtraction is not commutative, so when the variable is the subtrahend the operands can't
// simply be swapped; the comparison is inverted instead:
//
//	5 - x = 2 becomes x = 3
//	5 - x < 2 becomes x > 3
func (s *Simplifier) SimplifyEquality(expression *Expr) *Expr {
	return s.onChange(expression, smpCatch(expression, func() *Expr { return s.simplifyEquality(expression) }))
}

func (s *Simplifier) simplifyEquality(expression *Expr) *Expr {
	if expression.IsA(smpCOMPARISONS...) {
		l, r := expression.Left(), expression.Right()

		if _, ok := smpINVERSE_OPS[smpKindOf(l)]; !ok {
			return expression
		}

		var aPredicate, bPredicate func(*Expr) bool
		if r.IsNumber() {
			aPredicate = smpIsNumber
			bPredicate = smpIsNumber
		} else if smpIsDateLiteral(r) {
			aPredicate = smpIsDateLiteral
			bPredicate = smpIsInterval
		} else {
			return expression
		}

		var a, b *Expr
		if _, ok := smpINVERSE_DATE_OPS[l.kind]; ok {
			a = l.This()
			b = smpIntervalOpInterval(l)
		} else {
			a, b = l.Left(), l.Right()
		}

		if !aPredicate(a) && bPredicate(b) {
			// pass
		} else if !aPredicate(b) && bPredicate(a) {
			if l.IsA(KSub) {
				cls := expression.kind
				if inv, ok := smpINVERSE_COMPARISONS[expression.kind]; ok {
					cls = inv
				}
				return New(cls, "this", b, "expression", New(KSub, "this", a, "expression", r))
			}
			a, b = b, a
		} else {
			return expression
		}

		return New(expression.kind, "this", a, "expression", New(smpINVERSE_OPS[l.kind], "this", r, "expression", b))
	}
	return expression
}

// smpIntervalOpInterval mirrors IntervalOp.interval().
func smpIntervalOpInterval(e *Expr) *Expr {
	expr := e.Expression()
	var this, unit *Expr
	if expr != nil {
		this = expr.Copy()
	}
	if u := e.ArgE("unit"); u != nil {
		unit = u.Copy()
	}
	return New(KInterval, "this", this, "unit", unit)
}

func (s *Simplifier) isInverseDateOp(expression *Expr) bool {
	_, ok := smpINVERSE_DATE_OPS[smpKindOf(expression)]
	return ok
}

// SimplifyLiterals mirrors Simplifier.simplify_literals.
func (s *Simplifier) SimplifyLiterals(expression *Expr, root bool) *Expr {
	return s.onChange(expression, s.simplifyLiterals(expression, root))
}

func (s *Simplifier) simplifyLiterals(expression *Expr, root bool) *Expr {
	if expression.IsA(KBinary) && !expression.IsA(KConnector) {
		return s.flatSimplify(expression, s.simplifyBinary, root)
	}

	if expression.IsA(KNeg) && expression.This().IsA(KNeg) {
		return expression.This().This()
	}

	if s.isInverseDateOp(expression) {
		if r := s.simplifyBinary(expression, expression.This(), smpIntervalOpInterval(expression)); r != nil {
			return r
		}
		return expression
	}

	return expression
}

// simplifyIntegerCast mirrors Simplifier._simplify_integer_cast.
func (s *Simplifier) simplifyIntegerCast(expr *Expr) *Expr {
	var this *Expr
	if expr.IsA(KCast) && expr.This().IsA(KCast) {
		this = s.simplifyIntegerCast(expr.This())
	} else {
		this = expr.This()
	}

	if expr.IsA(KCast) && this.IsInt() {
		num := smpToPyNum(this).i

		// Remove the (up)cast from small (byte-sized) integers in predicates which is side-effect free. Downcasts on any
		// integer type might cause overflow, thus the cast cannot be eliminated and the behavior is
		// engine-dependent
		to, _ := expr.ArgE("to").Arg("this").(DType)
		_, isDType := expr.ArgE("to").Arg("this").(DType)
		if (num.IsInt64() && num.Int64() >= smpTINYINT_MIN && num.Int64() <= smpTINYINT_MAX &&
			isDType && DataType_SIGNED_INTEGER_TYPES.Has(to)) ||
			(num.IsInt64() && num.Int64() >= smpUTINYINT_MIN && num.Int64() <= smpUTINYINT_MAX &&
				isDType && DataType_UNSIGNED_INTEGER_TYPES.Has(to)) {
			return this
		}
	}

	return expr
}

// simplifyBinary mirrors Simplifier._simplify_binary.
func (s *Simplifier) simplifyBinary(expression, a, b *Expr) *Expr {
	if expression.IsA(smpCOMPARISONS...) {
		a = s.simplifyIntegerCast(a)
		b = s.simplifyIntegerCast(b)
	}

	if expression.IsA(KIs) {
		var c *Expr
		var not bool
		if b.IsA(KNot) {
			c = b.This()
			not = true
		} else {
			c = b
			not = false
		}

		if smpIsNull(c) {
			if a.IsA(KLiteral) {
				return smpBooleanLiteral(not)
			}
			if smpIsNull(a) {
				return smpBooleanLiteral(!not)
			}
		}
	} else if expression.IsA(smpNULL_OK...) {
		return nil
	} else if (smpIsNull(a) || smpIsNull(b)) && expression.Parent().IsA(KIf) {
		return Null()
	}

	if a.IsNumber() && b.IsNumber() {
		numA := smpToPyNum(a)
		numB := smpToPyNum(b)

		if expression.IsA(KAdd) {
			return smpLiteralNumber(smpNumAdd(numA, numB))
		}
		if expression.IsA(KMul) {
			return smpLiteralNumber(smpNumMul(numA, numB))
		}

		// We only simplify Sub, Div if a and b have the same parent because they're not associative
		if expression.IsA(KSub) {
			if a.Parent() == b.Parent() {
				return smpLiteralNumber(smpNumSub(numA, numB))
			}
			return nil
		}
		if expression.IsA(KDiv) {
			// engines have differing int div behavior so intdiv is not safe
			if (numA.isInt && numB.isInt) || a.Parent() != b.Parent() {
				return nil
			}
			return smpLiteralNumber(smpNumDiv(numA, numB))
		}

		if boolean := smpEvalBoolean(expression, numA, numB); boolean != nil {
			return boolean
		}
	} else if a.IsString() && b.IsString() {
		if boolean := smpEvalBoolean(expression, a.Arg("this"), b.Arg("this")); boolean != nil {
			return boolean
		}
	} else if smpIsDateLiteral(a) && b.IsA(KInterval) {
		date, _ := smpExtractDate(a)
		rd, ok := smpExtractInterval(b)
		if ok && rd.truthy() {
			if expression.IsA(KAdd, KDateAdd, KDatetimeAdd) {
				return smpDateLiteral(rd.addTo(date), smpExtractType(a))
			}
			if expression.IsA(KSub, KDateSub, KDatetimeSub) {
				return smpDateLiteral(rd.subFrom(date), smpExtractType(a))
			}
		}
	} else if a.IsA(KInterval) && smpIsDateLiteral(b) {
		rd, ok := smpExtractInterval(a)
		date, _ := smpExtractDate(b)
		// you cannot subtract a date from an interval
		if ok && rd.truthy() && expression.IsA(KAdd) {
			return smpDateLiteral(rd.addTo(date), smpExtractType(b))
		}
	} else if smpIsDateLiteral(a) && smpIsDateLiteral(b) {
		if expression.IsA(KPredicate) {
			da, _ := smpExtractDate(a)
			db, _ := smpExtractDate(b)
			if boolean := smpEvalBoolean(expression, da, db); boolean != nil {
				return boolean
			}
		}
	}

	return nil
}

// SimplifyCoalesce mirrors Simplifier.simplify_coalesce.
func (s *Simplifier) SimplifyCoalesce(expression *Expr) *Expr {
	return s.onChange(expression, s.simplifyCoalesce(expression))
}

func (s *Simplifier) simplifyCoalesce(expression *Expr) *Expr {
	// COALESCE(x) -> x
	if expression.IsA(KCoalesce) &&
		(len(expression.Expressions()) == 0 || smpIsNonnullConstant(expression.This())) &&
		// COALESCE is also used as a Spark partitioning hint
		!expression.Parent().IsA(KHint) {
		return expression.This()
	}

	if s.dialect.S.COALESCE_COMPARISON_NON_STANDARD {
		return expression
	}

	if !expression.IsA(smpCOMPARISONS...) {
		return expression
	}

	var coalesce, other *Expr
	if expression.Left().IsA(KCoalesce) {
		coalesce = expression.Left()
		other = expression.Right()
	} else if expression.Right().IsA(KCoalesce) {
		coalesce = expression.Right()
		other = expression.Left()
	} else {
		return expression
	}

	// This transformation is valid for non-constants,
	// but it really only does anything if they are both constants.
	if !smpIsConstant(other) {
		return expression
	}

	// Find the first constant arg
	argIndex := -1
	var arg *Expr
	for i, x := range coalesce.Expressions() {
		if smpIsConstant(x) {
			argIndex, arg = i, x
			break
		}
	}
	if argIndex < 0 {
		return expression
	}

	coalesce.Set("expressions", append([]*Expr{}, coalesce.Expressions()[:argIndex]...))

	// Remove the COALESCE function. This is an optimization, skipping a simplify iteration,
	// since we already remove COALESCE at the top of this function.
	this := coalesce
	if len(coalesce.Expressions()) == 0 {
		this = coalesce.This()
	}

	// This expression is more complex than when we started, but it will get simplified further
	return ParenExpr(
		OrExprOpts([]*Expr{
			AndExprOpts([]*Expr{
				smpBinop(KIs, this, Null()).ExprNot(false),
				expression.Copy(),
			}, false, true),
			AndExprOpts([]*Expr{
				smpBinop(KIs, this, Null()),
				New(expression.kind, "this", arg.Copy(), "expression", other.Copy()),
			}, false, true),
		}, false, true),
		false,
	)
}

// SimplifyConcat mirrors Simplifier.simplify_concat: reduces all groups that contain string
// literals by concatenating them.
func (s *Simplifier) SimplifyConcat(expression *Expr) *Expr {
	return s.onChange(expression, s.simplifyConcat(expression))
}

func (s *Simplifier) simplifyConcat(expression *Expr) *Expr {
	if !expression.IsA(smpCONCATS...) {
		return expression
	}
	if expression.IsA(KConcatWs) {
		exprs := expression.Expressions()
		if len(exprs) == 0 {
			smpRaise("IndexError", "list index out of range")
		}
		// We can't reduce a CONCAT_WS call if we don't statically know the separator
		if !exprs[0].IsString() {
			return expression
		}
	}

	var sepExpr *Expr
	var expressions []*Expr
	var sep string
	var concatType Kind
	if expression.IsA(KConcatWs) {
		all := expression.Expressions()
		sepExpr, expressions = all[0], all[1:]
		sep = sepExpr.Name()
		concatType = KConcatWs
	} else {
		expressions = expression.Expressions()
		sep = ""
		concatType = KConcat
	}

	safe := expression.Arg("safe")
	coalesce := expression.Arg("coalesce")

	operands := expressions
	if len(operands) == 0 {
		operands = expression.Flatten(true)
	}

	newArgs := []*Expr{}
	for i := 0; i < len(operands); {
		isStringGroup := operands[i].IsString()
		j := i
		for j < len(operands) && operands[j].IsString() == isStringGroup {
			j++
		}
		group := operands[i:j]
		if isStringGroup {
			names := make([]string, len(group))
			for k, str := range group {
				names[k] = str.Name()
			}
			joined := ""
			for k, n := range names {
				if k > 0 {
					joined += sep
				}
				joined += n
			}
			newArgs = append(newArgs, LiteralString(joined))
		} else {
			newArgs = append(newArgs, group...)
		}
		i = j
	}

	if len(newArgs) == 1 && newArgs[0].IsString() {
		return newArgs[0]
	}

	if concatType == KConcatWs {
		newArgs = append([]*Expr{sepExpr}, newArgs...)
	} else if expression.IsA(KDPipe) {
		if len(newArgs) == 0 {
			smpRaise("TypeError", "reduce() of empty iterable with no initial value")
		}
		acc := newArgs[0]
		for _, y := range newArgs[1:] {
			acc = New(KDPipe, "this", acc, "expression", y)
		}
		return acc
	}

	return New(concatType, "expressions", newArgs, "safe", safe, "coalesce", coalesce)
}

// SimplifyConditionals mirrors Simplifier.simplify_conditionals: simplifies expressions like
// IF, CASE if their condition is statically known.
func (s *Simplifier) SimplifyConditionals(expression *Expr) *Expr {
	return s.onChange(expression, s.simplifyConditionals(expression))
}

func (s *Simplifier) simplifyConditionals(expression *Expr) *Expr {
	if expression.IsA(KCase) {
		this := expression.This()
		// Python iterates the live `ifs` list while popping from it (popped elements shift the
		// following ones, which are then skipped).
		for idx := 0; idx < len(expression.ArgL("ifs")); idx++ {
			c := expression.ArgL("ifs")[idx]
			cond := c.This()
			if this != nil {
				cond = cond.Replace(smpBinop(KEQ, this.Pop(), cond))
			}

			if smpAlwaysTrue(cond) {
				return c.ArgE("true")
			}

			if smpAlwaysFalse(cond) {
				c.Pop()
				if len(expression.ArgL("ifs")) == 0 {
					if d := expression.ArgE("default"); d != nil {
						return d
					}
					return Null()
				}
			}
		}
	} else if expression.IsA(KIf) && !expression.Parent().IsA(KCase) {
		if smpAlwaysTrue(expression.This()) {
			return expression.ArgE("true")
		}
		if smpAlwaysFalse(expression.This()) {
			if f := expression.ArgE("false"); f != nil {
				return f
			}
			return Null()
		}
	}

	return expression
}

// SimplifyStartswith mirrors Simplifier.simplify_startswith: reduces a prefix check to either
// TRUE or FALSE if both the string and the prefix are statically known.
func (s *Simplifier) SimplifyStartswith(expression *Expr) *Expr {
	return s.onChange(expression, s.simplifyStartswith(expression))
}

func (s *Simplifier) simplifyStartswith(expression *Expr) *Expr {
	if expression.IsA(KStartsWith) && expression.This().IsString() && expression.Expression().IsString() {
		name, prefix := expression.Name(), expression.Expression().Name()
		return Boolean(len(name) >= len(prefix) && name[:len(prefix)] == prefix)
	}

	return expression
}

func (s *Simplifier) isDatetruncPredicate(left, right *Expr) bool {
	return left.IsA(smpDATETRUNCS...) && smpIsDateLiteral(right)
}

// SimplifyDatetrunc mirrors Simplifier.simplify_datetrunc: simplify expressions like
// `DATE_TRUNC('year', x) >= CAST('2021-01-01' AS DATE)`.
func (s *Simplifier) SimplifyDatetrunc(expression *Expr) *Expr {
	return s.onChange(expression, smpCatch(expression, func() *Expr { return s.simplifyDatetrunc(expression) }))
}

func (s *Simplifier) simplifyDatetrunc(expression *Expr) *Expr {
	comparison := expression.kind

	_, isDatetruncComparison := smpDATETRUNC_BINARY_COMPARISONS[comparison]
	if expression.IsA(smpDATETRUNCS...) {
		this := expression.This()
		truncType := smpExtractType(this)
		date, ok := smpExtractDate(this)
		if ok && expression.ArgE("unit") != nil {
			return smpDateLiteral(
				smpDatetimeFloor(date, pyLower(expression.ArgE("unit").Name()), s.dialect),
				truncType,
			)
		}
	} else if comparison != KIn && !isDatetruncComparison {
		return expression
	}

	if expression.IsA(KBinary) {
		l, r := expression.Left(), expression.Right()

		if !s.isDatetruncPredicate(l, r) || !l.IsA(KDateTrunc, KTimestampTrunc) {
			return expression
		}

		truncArg := l.This()
		unit := pyLower(l.ArgE("unit").Name())
		date, ok := smpExtractDate(r)

		if !ok {
			return expression
		}

		if res := smpDATETRUNC_BINARY_COMPARISONS[comparison](truncArg, date, unit, s.dialect, smpExtractType(r)); res != nil {
			return res
		}
		return expression
	}

	if expression.IsA(KIn) {
		l := expression.This()
		rs := expression.Expressions()

		allPredicates := true
		for _, r := range rs {
			if !s.isDatetruncPredicate(l, r) {
				allPredicates = false
				break
			}
		}
		if len(rs) > 0 && allPredicates && l.IsA(KDateTrunc, KTimestampTrunc) {
			unit := pyLower(l.ArgE("unit").Name())

			var ranges []smpDateRange
			for _, r := range rs {
				date, ok := smpExtractDate(r)
				if !ok {
					return expression
				}
				if drange, ok := smpDatetruncRange(date, unit, s.dialect); ok {
					ranges = append(ranges, drange)
				}
			}

			if len(ranges) == 0 {
				return expression
			}

			ranges = smpMergeRanges(ranges)
			targetType := smpExtractType(rs...)

			eqs := make([]*Expr, len(ranges))
			for i, drange := range ranges {
				eqs[i] = smpDatetruncEqExpression(l, drange, targetType)
			}
			return OrExprOpts(eqs, false, true)
		}
	}

	return expression
}

// smpMergeRanges mirrors sqlglot.helper.merge_ranges for date ranges.
func smpMergeRanges(ranges []smpDateRange) []smpDateRange {
	if len(ranges) == 0 {
		return nil
	}

	sorted := append([]smpDateRange(nil), ranges...)
	smpStableSort(sorted, func(x, y smpDateRange) bool {
		// tuple comparison: first differing element decides
		if !smpPyEq(x.lo, y.lo) {
			return smpPyLt(x.lo, y.lo)
		}
		if !smpPyEq(x.hi, y.hi) {
			return smpPyLt(x.hi, y.hi)
		}
		return false
	})

	merged := []smpDateRange{sorted[0]}

	for _, rg := range sorted[1:] {
		last := merged[len(merged)-1]

		if smpPyLe(rg.lo, last.hi) {
			end := last.hi
			if smpPyGt(rg.hi, last.hi) {
				end = rg.hi
			}
			merged[len(merged)-1] = smpDateRange{last.lo, end}
		} else {
			merged = append(merged, rg)
		}
	}

	return merged
}

// smpStableSort is a stable insertion sort (inputs are tiny).
func smpStableSort[T any](a []T, lt func(x, y T) bool) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && lt(a[j], a[j-1]); j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// SortComparison mirrors Simplifier.sort_comparison.
func (s *Simplifier) SortComparison(expression *Expr) *Expr {
	return s.onChange(expression, s.sortComparison(expression))
}

func (s *Simplifier) sortComparison(expression *Expr) *Expr {
	if _, ok := smpCOMPLEMENT_COMPARISONS[expression.kind]; ok {
		l, r := expression.This(), expression.Expression()
		lColumn := l.IsA(KColumn)
		rColumn := r.IsA(KColumn)
		lConst := smpIsConstant(l)
		rConst := smpIsConstant(r)

		if (lColumn && !rColumn) || (rConst && !lConst) || r.IsA(KSubqueryPredicate) {
			return expression
		}
		if (rColumn && !lColumn) || (lConst && !rConst) || smpGen(l, false) > smpGen(r, false) {
			cls := expression.kind
			if inv, ok := smpINVERSE_COMPARISONS[expression.kind]; ok {
				cls = inv
			}
			return New(cls, "this", r, "expression", l)
		}
	}
	return expression
}

// flatSimplify mirrors Simplifier._flat_simplify.
func (s *Simplifier) flatSimplify(expression *Expr, simplifier func(expression, a, b *Expr) *Expr, root bool) *Expr {
	if root || !expression.SameParent() {
		var operands []*Expr
		queue := expression.Flatten(false)
		size := len(queue)

		// The pairwise scan below is O(n^2). For connectors, a pair only combines if one side
		// is a constant or both are comparisons (see _simplify_connectors / _simplify_comparison);
		// if no operand is combinable the scan is a guaranteed no-op, so return early. This
		// avoids the quadratic blowup on large connectors of inert operands (e.g. a 1000-way OR
		// of ANDs). Non-connector callers (simplify_equality) are unaffected by the type guard.
		if expression.IsA(KConnector) {
			combinable := false
			for _, op := range queue {
				if op.IsA(smpCONNECTOR_OK...) {
					combinable = true
					break
				}
			}
			if !combinable {
				return expression
			}
		}

		for len(queue) > 0 {
			a := queue[0]
			queue = queue[1:]

			combined := false
			for _, b := range queue {
				result := simplifier(expression, a, b)

				if result != nil && result != expression {
					// deque.remove(b): removes the first element that is b or equal to b
					for i, x := range queue {
						if x == b || x.Equal(b) {
							queue = append(queue[:i:i], queue[i+1:]...)
							break
						}
					}
					queue = append([]*Expr{result}, queue...)
					combined = true
					break
				}
			}
			if !combined {
				operands = append(operands, a)
			}
		}

		if len(operands) < size {
			acc := operands[0]
			for _, b := range operands[1:] {
				acc = New(expression.kind, "this", acc, "expression", b)
			}
			return acc
		}
	}
	return expression
}
