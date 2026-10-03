package sqlengine

// Port of sqlglot/optimizer/optimizer.py.

// OptimizeContext carries the arguments of sqlglot.optimizer.optimize that are forwarded to the
// rules: Python passes each rule the keyword arguments its signature accepts among
// db, catalog, schema, dialect, sql, isolate_tables=True, quote_identifiers=False and **kwargs.
type OptimizeContext struct {
	Schema  *MappingSchema
	Dialect *Dialect
	// Db and Catalog are the default database/catalog ("" means None).
	Db      string
	Catalog string
	// SQL is the original SQL string for error highlighting ("" means None).
	SQL string
	// Kwargs mirrors **kwargs (keys are the Python parameter names, e.g. "expand_stars",
	// "leave_tables_isolated"). Values must have the Go type of the matching option.
	Kwargs map[string]any
}

// OptimizerRule is an optimizer rule: it takes an expression and returns the rewritten expression.
type OptimizerRule func(e *Expr, ctx *OptimizeContext) *Expr

// DefaultOptimizerRules mirrors sqlglot.optimizer.optimizer.RULES.
var DefaultOptimizerRules = []OptimizerRule{
	RuleQualify,
	RulePushdownProjections,
	RuleNormalize,
	RuleUnnestSubqueries,
	RulePushdownPredicates,
	RuleOptimizeJoins,
	RuleEliminateSubqueries,
	RuleMergeSubqueries,
	RuleEliminateJoins,
	RuleEliminateCTEs,
	RuleQuoteIdentifiers,
	RuleAnnotateTypes,
	RuleCanonicalize,
	RuleSimplify,
}

// Optimize mirrors sqlglot.optimizer.optimize(expression, schema, dialect=dialect, rules=rules).
// The expression is copied first. nil rules means DefaultOptimizerRules (Python's RULES).
// Errors are raised as panics, like the Python exceptions.
func Optimize(e *Expr, schema *MappingSchema, d *Dialect, rules []OptimizerRule) *Expr {
	return OptimizeWith(e, &OptimizeContext{Schema: schema, Dialect: d}, rules)
}

// OptimizeWith mirrors sqlglot.optimizer.optimize with every argument (db, catalog, sql, **kwargs) in ctx.
// nil rules means DefaultOptimizerRules.
func OptimizeWith(e *Expr, ctx *OptimizeContext, rules []OptimizerRule) *Expr {
	if rules == nil {
		rules = DefaultOptimizerRules
	}
	c := *ctx
	if c.Schema == nil {
		c.Schema = NewMappingSchema(nil, nil, c.Dialect, true, nil)
	}

	optimized := e.Copy()
	for _, rule := range rules {
		optimized = rule(optimized, &c)
	}

	return optimized
}

// kwarg returns ctx.Kwargs[key] if it is present with type T.
func optKwarg[T any](ctx *OptimizeContext, key string) (T, bool) {
	var zero T
	if ctx.Kwargs == nil {
		return zero, false
	}
	v, ok := ctx.Kwargs[key]
	if !ok {
		return zero, false
	}
	t, ok := v.(T)
	return t, ok
}

// RuleQualify mirrors the `qualify` rule: qualify receives dialect, db, catalog, schema, sql,
// isolate_tables=True and quote_identifiers=False, plus any matching kwargs.
var RuleQualify OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	o := DefaultQualifyOptions()
	o.Dialect = ctx.Dialect
	o.Db = ctx.Db
	o.Catalog = ctx.Catalog
	o.Schema = ctx.Schema
	o.SQL = ctx.SQL
	o.IsolateTables = true
	o.QuoteIdentifiers = false

	for key, dst := range map[string]*bool{
		"expand_alias_refs":           &o.ExpandAliasRefs,
		"expand_stars":                &o.ExpandStars,
		"isolate_tables":              &o.IsolateTables,
		"qualify_columns":             &o.QualifyColumns,
		"allow_partial_qualification": &o.AllowPartialQualification,
		"validate_qualify_columns":    &o.ValidateQualifyColumns,
		"quote_identifiers":           &o.QuoteIdentifiers,
		"identify":                    &o.Identify,
		"canonicalize_table_aliases":  &o.CanonicalizeTableAliases,
	} {
		if v, ok := optKwarg[bool](ctx, key); ok {
			*dst = v
		}
	}
	if v, ok := optKwarg[Tri](ctx, "infer_schema"); ok {
		o.InferSchema = v
	} else if v, ok := optKwarg[bool](ctx, "infer_schema"); ok {
		o.InferSchema = TriFalse
		if v {
			o.InferSchema = TriTrue
		}
	}
	if v, ok := optKwarg[func(*Expr)](ctx, "on_qualify"); ok {
		o.OnQualify = v
	}

	return Qualify(e, o)
}

// RulePushdownProjections mirrors the `pushdown_projections` rule: it receives schema and dialect
// (remove_unused_selections from kwargs, default True).
var RulePushdownProjections OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	removeUnusedSelections := true
	if v, ok := optKwarg[bool](ctx, "remove_unused_selections"); ok {
		removeUnusedSelections = v
	}
	return PushdownProjections(e, ctx.Schema, removeUnusedSelections, ctx.Dialect)
}

// RuleNormalize mirrors the `normalize` rule (dnf and max_distance from kwargs, defaults False and 128).
var RuleNormalize OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	dnf, _ := optKwarg[bool](ctx, "dnf")
	maxDistance := 128
	if v, ok := optKwarg[int](ctx, "max_distance"); ok {
		maxDistance = v
	}
	return NormalizeCNF(e, dnf, maxDistance)
}

// RuleUnnestSubqueries mirrors the `unnest_subqueries` rule (it only takes the expression).
var RuleUnnestSubqueries OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return UnnestSubqueries(e)
}

// RulePushdownPredicates mirrors the `pushdown_predicates` rule: it receives the dialect.
var RulePushdownPredicates OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return PushdownPredicates(e, ctx.Dialect)
}

// RuleOptimizeJoins mirrors the `optimize_joins` rule (it only takes the expression).
var RuleOptimizeJoins OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return OptimizeJoins(e)
}

// RuleEliminateSubqueries mirrors the `eliminate_subqueries` rule (it only takes the expression).
var RuleEliminateSubqueries OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return EliminateSubqueries(e)
}

// RuleMergeSubqueries mirrors the `merge_subqueries` rule (leave_tables_isolated from kwargs, default False).
var RuleMergeSubqueries OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	leaveTablesIsolated, _ := optKwarg[bool](ctx, "leave_tables_isolated")
	return MergeSubqueries(e, leaveTablesIsolated)
}

// RuleEliminateJoins mirrors the `eliminate_joins` rule (it only takes the expression).
var RuleEliminateJoins OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return EliminateJoins(e)
}

// RuleEliminateCTEs mirrors the `eliminate_ctes` rule (it only takes the expression).
var RuleEliminateCTEs OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return EliminateCTEs(e)
}

// RuleQuoteIdentifiers mirrors the `quote_identifiers` rule: it receives the dialect (identify from
// kwargs, default True; optimize's quote_identifiers=False is not one of its parameters).
var RuleQuoteIdentifiers OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	identify := true
	if v, ok := optKwarg[bool](ctx, "identify"); ok {
		identify = v
	}
	return QuoteIdentifiers(e, ctx.Dialect, identify)
}

// RuleAnnotateTypes mirrors the `annotate_types` rule: it receives schema and dialect.
var RuleAnnotateTypes OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return AnnotateTypes(e, AnnotateOptions{Schema: ctx.Schema, Dialect: ctx.Dialect})
}

// RuleCanonicalize mirrors the `canonicalize` rule: it receives the dialect.
var RuleCanonicalize OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return Canonicalize(e, ctx.Dialect)
}

// RuleSimplify mirrors the `simplify` rule: it receives the dialect.
var RuleSimplify OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return Simplify(e, SimplifyOptions{Dialect: ctx.Dialect})
}

// SelectReplaceProjections mirrors select_expr.select(projection, append=False) (copy=True): a copy
// of sel whose projections are replaced by the given projection. Like Python, the projection is
// not copied, so it is re-parented under the returned select.
func SelectReplaceProjections(sel *Expr, projection *Expr) *Expr {
	return sel.QuerySelect([]*Expr{projection}, false, true)
}
