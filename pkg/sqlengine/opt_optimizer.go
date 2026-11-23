package sqlengine

// OptimizeContext carries the optimizer arguments that are forwarded to the rules. Each rule
// picks the fields it needs and reads its extra options from Kwargs.
type OptimizeContext struct {
	Schema  *MappingSchema
	Dialect *Dialect
	// Db and Catalog are the default database/catalog ("" means unset).
	Db      string
	Catalog string
	// SQL is the original SQL string for error highlighting ("" means unset).
	SQL string
	// Kwargs holds extra rule options keyed by snake_case option name (e.g. "expand_stars",
	// "leave_tables_isolated"). Values must have the Go type of the matching option; values
	// of another type are ignored.
	Kwargs map[string]any
}

// OptimizerRule is an optimizer rule: it takes an expression and returns the rewritten expression.
type OptimizerRule func(e *Expr, ctx *OptimizeContext) *Expr

// DefaultOptimizerRules is the rule list Optimize applies, in order, when none is given.
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

// Optimize applies rules to a copy of e using the given schema and dialect. nil rules means
// DefaultOptimizerRules. Rule errors are raised as panics.
func Optimize(e *Expr, schema *MappingSchema, d *Dialect, rules []OptimizerRule) *Expr {
	return OptimizeWith(e, &OptimizeContext{Schema: schema, Dialect: d}, rules)
}

// OptimizeWith is Optimize with every argument (db, catalog, sql, extra options) taken from
// ctx. nil rules means DefaultOptimizerRules; a nil schema is replaced by an empty one.
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

// optKwarg returns ctx.Kwargs[key] if it is present with type T.
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

// RuleQualify runs Qualify with the context's dialect, db, catalog, schema and SQL, table
// isolation on and identifier quoting off. Kwargs can override the boolean options below,
// infer_schema (a Tri or a bool) and on_qualify.
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

// RulePushdownProjections runs PushdownProjections with the context's schema and dialect
// (remove_unused_selections from Kwargs, default true).
var RulePushdownProjections OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	removeUnusedSelections := true
	if v, ok := optKwarg[bool](ctx, "remove_unused_selections"); ok {
		removeUnusedSelections = v
	}
	return PushdownProjections(e, ctx.Schema, removeUnusedSelections, ctx.Dialect)
}

// RuleNormalize runs NormalizeCNF (dnf and max_distance from Kwargs, defaults false and 128).
var RuleNormalize OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	dnf, _ := optKwarg[bool](ctx, "dnf")
	maxDistance := 128
	if v, ok := optKwarg[int](ctx, "max_distance"); ok {
		maxDistance = v
	}
	return NormalizeCNF(e, dnf, maxDistance)
}

// RuleUnnestSubqueries runs UnnestSubqueries (it only takes the expression).
var RuleUnnestSubqueries OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return UnnestSubqueries(e)
}

// RulePushdownPredicates runs PushdownPredicates with the context's dialect.
var RulePushdownPredicates OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return PushdownPredicates(e, ctx.Dialect)
}

// RuleOptimizeJoins runs OptimizeJoins (it only takes the expression).
var RuleOptimizeJoins OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return OptimizeJoins(e)
}

// RuleEliminateSubqueries runs EliminateSubqueries (it only takes the expression).
var RuleEliminateSubqueries OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return EliminateSubqueries(e)
}

// RuleMergeSubqueries runs MergeSubqueries (leave_tables_isolated from Kwargs, default false).
var RuleMergeSubqueries OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	leaveTablesIsolated, _ := optKwarg[bool](ctx, "leave_tables_isolated")
	return MergeSubqueries(e, leaveTablesIsolated)
}

// RuleEliminateJoins runs EliminateJoins (it only takes the expression).
var RuleEliminateJoins OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return EliminateJoins(e)
}

// RuleEliminateCTEs runs EliminateCTEs (it only takes the expression).
var RuleEliminateCTEs OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return EliminateCTEs(e)
}

// RuleQuoteIdentifiers runs QuoteIdentifiers with the context's dialect (identify from Kwargs,
// default true). The qualify rule's quote_identifiers=false does not apply here.
var RuleQuoteIdentifiers OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	identify := true
	if v, ok := optKwarg[bool](ctx, "identify"); ok {
		identify = v
	}
	return QuoteIdentifiers(e, ctx.Dialect, identify)
}

// RuleAnnotateTypes runs AnnotateTypes with the context's schema and dialect.
var RuleAnnotateTypes OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return AnnotateTypes(e, AnnotateOptions{Schema: ctx.Schema, Dialect: ctx.Dialect})
}

// RuleCanonicalize runs Canonicalize with the context's dialect.
var RuleCanonicalize OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return Canonicalize(e, ctx.Dialect)
}

// RuleSimplify runs Simplify with the context's dialect.
var RuleSimplify OptimizerRule = func(e *Expr, ctx *OptimizeContext) *Expr {
	return Simplify(e, SimplifyOptions{Dialect: ctx.Dialect})
}

// SelectReplaceProjections returns a copy of sel whose projections are replaced by projection.
// The projection itself is not copied, so it is re-parented under the returned select.
func SelectReplaceProjections(sel *Expr, projection *Expr) *Expr {
	return sel.QuerySelect([]*Expr{projection}, false, true)
}
