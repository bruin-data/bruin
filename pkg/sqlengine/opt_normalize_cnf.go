package sqlengine

import (
	"fmt"
	"iter"
	"math"
)

// Port of sqlglot/optimizer/normalize.py.

// normalizationDistanceInf stands for Python's float("inf") as a maxDistance.
const normalizationDistanceInf = math.MaxInt

// NormalizeCNF mirrors optimizer.normalize.normalize: rewrites a sqlglot AST into conjunctive
// normal form (or disjunctive normal form when dnf is true). Python defaults: dnf=False,
// max_distance=128.
//
// Example: "(x AND y) OR z" -> "(x OR z) AND (y OR z)"
func NormalizeCNF(expression *Expr, dnf bool, maxDistance int) *Expr {
	simplifier := normalizeNewSimplifier()

	var nodes []*Expr
	for node := range expression.Walk(true, func(e *Expr) bool { return e.IsA(KConnector) }) {
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		if !node.IsA(KConnector) {
			continue
		}
		if normalizedCNF(node, dnf) {
			continue
		}
		root := node == expression
		original := node.Copy()

		node.Transform(simplifier.RewriteBetween, false)
		distance := normalizationDistance(node, dnf, maxDistance)

		if distance > maxDistance {
			// Skipping normalization because distance exceeds max (logged in Python)
			return expression
		}

		failed := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					if _, ok := r.(*OptimizeError); !ok {
						panic(r)
					}
					failed = true
				}
			}()
			node = node.Replace(optxWhileChanging(node, func(e *Expr) *Expr {
				return normalizeDistributiveLaw(e, dnf, maxDistance, simplifier)
			}))
		}()
		if failed {
			node.Replace(original)
			if root {
				return original
			}
			return expression
		}

		if root {
			expression = node
		}
	}

	return expression
}

// normalizedCNF mirrors optimizer.normalize.normalized: checks whether a given expression is in
// conjunctive normal form (or disjunctive normal form when dnf is true).
//
// Examples:
//
//	normalized("(a AND b) OR c OR (d AND e)", dnf=True) -> True
//	normalized("(a OR b) AND c") -> True
//	normalized("a AND (b OR c)", dnf=True) -> False
func normalizedCNF(expression *Expr, dnf bool) bool {
	ancestor, root := KOr, KAnd
	if dnf {
		ancestor, root = KAnd, KOr
	}
	for connector := range FindAllInScope(expression, root) {
		if connector.FindAncestor(ancestor) != nil {
			return false
		}
	}
	return true
}

// Normalized is the exported form of normalizedCNF (optimizer.normalize.normalized).
func Normalized(expression *Expr, dnf bool) bool { return normalizedCNF(expression, dnf) }

// normalizationDistance mirrors optimizer.normalize.normalization_distance: the difference in the
// number of predicates between a given expression and its normalized form. Pass
// normalizationDistanceInf for Python's default max_=float("inf").
//
// Example: normalization_distance("(a AND b) OR (c AND d)") -> 4
func normalizationDistance(expression *Expr, dnf bool, maxDistance int) int {
	count := 0
	for range expression.FindAll(KConnector) {
		count++
	}
	total := -(count + 1)

	for length := range normalizePredicateLengths(expression, dnf, maxDistance, 0) {
		total += length
		if total > maxDistance {
			return total
		}
	}

	return total
}

// NormalizationDistance is the exported form of normalizationDistance.
func NormalizationDistance(expression *Expr, dnf bool, maxDistance int) int {
	return normalizationDistance(expression, dnf, maxDistance)
}

// normalizePredicateLengths mirrors normalize._predicate_lengths: the predicate lengths when
// expanded to normalized form, e.g. (A AND B) OR C -> [2, 2] because len(A OR C), len(B OR C).
func normalizePredicateLengths(expression *Expr, dnf bool, maxDistance int, depth int) iter.Seq[int] {
	return func(yield func(int) bool) {
		normalizePredicateLengthsImpl(expression, dnf, maxDistance, depth, yield)
	}
}

func normalizePredicateLengthsImpl(expression *Expr, dnf bool, maxDistance int, depth int, yield func(int) bool) bool {
	if depth > maxDistance {
		return yield(depth)
	}

	expression = unnestMethod(expression)

	if !expression.IsA(KConnector) {
		return yield(1)
	}

	depth++
	left, right := expression.Left(), expression.Right()

	kind := KOr
	if dnf {
		kind = KAnd
	}
	if expression.IsA(kind) {
		cont := true
		normalizePredicateLengthsImpl(left, dnf, maxDistance, depth, func(a int) bool {
			normalizePredicateLengthsImpl(right, dnf, maxDistance, depth, func(b int) bool {
				cont = yield(a + b)
				return cont
			})
			return cont
		})
		return cont
	}
	if !normalizePredicateLengthsImpl(left, dnf, maxDistance, depth, yield) {
		return false
	}
	return normalizePredicateLengthsImpl(right, dnf, maxDistance, depth, yield)
}

// normalizeDistributiveLaw mirrors normalize.distributive_law:
//
//	x OR (y AND z) -> (x OR y) AND (x OR z)
//	(x AND y) OR (y AND z) -> (x OR y) AND (x OR z) AND (y OR y) AND (y OR z)
//
// simplifier may be nil (a new Simplifier(annotate_new_expressions=False) is then created).
func normalizeDistributiveLaw(expression *Expr, dnf bool, maxDistance int, simplifier *Simplifier) *Expr {
	if normalizedCNF(expression, dnf) {
		return expression
	}

	distance := normalizationDistance(expression, dnf, maxDistance)

	if distance > maxDistance {
		panic(&OptimizeError{Msg: fmt.Sprintf("Normalization distance %d exceeds max %d", distance, maxDistance)})
	}

	ReplaceChildren(expression, func(e *Expr) any {
		return normalizeDistributiveLaw(e, dnf, maxDistance, nil)
	})
	toExp, fromExp := KAnd, KOr
	if dnf {
		toExp, fromExp = KOr, KAnd
	}

	if expression.IsA(fromExp) {
		operands := optxUnnestOperands(expression)
		if len(operands) != 2 {
			panic(&ValueError{Msg: fmt.Sprintf("too many values to unpack (expected 2, got %d)", len(operands))})
		}
		a, b := operands[0], operands[1]

		fromFunc := func(x, y *Expr, copy bool) *Expr {
			return combineConditions([]*Expr{x, y}, fromExp, copy, true)
		}
		toFunc := func(x, y *Expr, copy bool) *Expr {
			return combineConditions([]*Expr{x, y}, toExp, copy, true)
		}

		if simplifier == nil {
			simplifier = normalizeNewSimplifier()
		}

		if a.IsA(toExp) && b.IsA(toExp) {
			if len(a.FindAllList(KConnector)) > len(b.FindAllList(KConnector)) {
				return normalizeDistribute(a, b, fromFunc, toFunc, simplifier)
			}
			return normalizeDistribute(b, a, fromFunc, toFunc, simplifier)
		}
		if a.IsA(toExp) {
			return normalizeDistribute(b, a, fromFunc, toFunc, simplifier)
		}
		if b.IsA(toExp) {
			return normalizeDistribute(a, b, fromFunc, toFunc, simplifier)
		}
	}

	return expression
}

// normalizeDistribute mirrors normalize._distribute.
func normalizeDistribute(a, b *Expr, fromFunc, toFunc func(x, y *Expr, copy bool) *Expr, simplifier *Simplifier) *Expr {
	// When `a` and `b` are connectors of the SAME polarity (e.g. both AND in
	// CNF mode), `b` is distributed across `a`'s children so the existing
	// cross-product handling can simplify them.
	if a.IsA(KConnector) && a.IsA(b.Kind()) {
		ReplaceChildren(a, func(c *Expr) any {
			return toFunc(
				simplifier.UniqSort(simplifyFlatten(fromFunc(c, b.Left(), true)), true),
				simplifier.UniqSort(simplifyFlatten(fromFunc(c, b.Right(), true)), true),
				false,
			)
		})
		return a
	}

	// Otherwise apply the textbook rule
	// ``a OR (b.left AND b.right) -> (a OR b.left) AND (a OR b.right)`` (or
	// the AND/OR dual).
	return toFunc(
		simplifier.UniqSort(simplifyFlatten(fromFunc(a, b.Left(), true)), true),
		simplifier.UniqSort(simplifyFlatten(fromFunc(a, b.Right(), true)), true),
		false,
	)
}

// normalizeNewSimplifier mirrors Simplifier(annotate_new_expressions=False) (default dialect).
func normalizeNewSimplifier() *Simplifier {
	return NewSimplifier(nil, false)
}

// ---------------------------------------------------------------------------------------------
// Shared helpers for the optimizer rules ported in the opt_* files of this chunk.
// ---------------------------------------------------------------------------------------------

// optxWhileChanging mirrors sqlglot.helper.while_changing: applies fn until the expression's
// hash stops changing.
func optxWhileChanging(expression *Expr, fn func(*Expr) *Expr) *Expr {
	for {
		startHash := expression.Hash()
		expression = fn(expression)
		endHash := expression.Hash()

		if startHash == endHash {
			break
		}
	}
	return expression
}

// optxFlattenSeq mirrors Expr.flatten(unnest) as a lazy generator: like Python, the DFS prune
// check runs after the consumer handles each node, so mutations made while iterating are seen.
func optxFlattenSeq(e *Expr, unnest bool) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		kind := e.kind
		prune := func(n *Expr) bool { return n.parent != nil && n.kind != kind }
		for node := range e.DFS(prune) {
			if node.kind != kind {
				out := node
				if unnest && !node.kind.isSubquery() {
					out = node.Unnest()
				}
				if !yield(out) {
					return
				}
			}
		}
	}
}

// optxUnnestOperands mirrors Expr.unnest_operands with Python's dynamic dispatch of `unnest`
// (Subquery.unnest for subqueries, Paren stripping otherwise).
func optxUnnestOperands(e *Expr) []*Expr {
	children := e.IterExpressions(false)
	out := make([]*Expr, len(children))
	for i, c := range children {
		out[i] = unnestMethod(c)
	}
	return out
}
