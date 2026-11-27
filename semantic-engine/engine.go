package semantic

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var refPattern = regexp.MustCompile(`\{([^}]+)\}`)

var plainColumnPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)

var aggregateFuncPattern = regexp.MustCompile(`(?i)\b(sum|count|avg|min|max|stddev|stddev_samp|stddev_pop|variance|var_samp|var_pop|median|percentile_cont|percentile_disc|array_agg|string_agg|listagg|approx_count_distinct|approx_quantile)\s*\(`)

func maskTemplateDelimiters(expr string) string {
	replacer := strings.NewReplacer("{{", "@@", "}}", "##")
	return replacer.Replace(expr)
}

// containsAggregateOutsideRefs reports whether expr contains an aggregate
// function call that is not inside a {ref} placeholder or quoted text.
func (e *Engine) containsAggregateOutsideRefs(expr string) bool {
	stripped := refPattern.ReplaceAllString(e.maskSQL(expr), "")
	return aggregateFuncPattern.MatchString(stripped)
}

var dollarQuoteTag = regexp.MustCompile(`^\$([A-Za-z_][A-Za-z0-9_]*)?\$`)

// maskSQL blanks quoted text, comments, and {{ }} template delimiters in expr,
// keeping byte offsets, so parentheses and {refs} found in it are SQL.
// backslashEscapes says whether a backslash escapes a quote in '...' strings.
func maskSQL(expr string, backslashEscapes bool) string {
	s := maskTemplateDelimiters(expr)
	b := []byte(s)
	for i := 0; i < len(s); {
		var end int
		switch {
		case s[i] == '\'' || s[i] == '"' || s[i] == '`':
			end = scanQuoted(s, i, s[i], backslashEscapes)
		case strings.HasPrefix(s[i:], "--"):
			end = spanEnd(s, i+2, "\n")
		case strings.HasPrefix(s[i:], "/*"):
			end = spanEnd(s, i+2, "*/")
		case s[i] == '$' && (i == 0 || !isIdentifierPart(s[i-1]) && s[i-1] != '$') && dollarQuoteTag.MatchString(s[i:]):
			tag := dollarQuoteTag.FindString(s[i:])
			end = spanEnd(s, i+len(tag), tag)
		default:
			i++
			continue
		}
		for k := i; k < end; k++ {
			b[k] = ' '
		}
		i = end
	}
	return string(b)
}

// spanEnd returns the index after the first closer at or after from, or len(s).
func spanEnd(s string, from int, closer string) int {
	if j := strings.Index(s[from:], closer); j >= 0 {
		return from + j + len(closer)
	}
	return len(s)
}

// maskSQLAnyDialect blanks text that either backslash reading treats as quoted.
// One reading is the dialect's, so text a viewer typed is always blanked.
func maskSQLAnyDialect(expr string) string {
	a, b := []byte(maskSQL(expr, true)), maskSQL(expr, false)
	for i := range a {
		if b[i] == ' ' {
			a[i] = ' '
		}
	}
	return string(a)
}

// maskSQL masks expr for finding {refs} and aggregates. Without a dialect, as
// when a model loads, text either reading quotes is blanked, so a viewer value
// can never add a ref; with one, the dialect's own reading is used.
func (e *Engine) maskSQL(expr string) string {
	if e.Dialect == "" {
		return maskSQLAnyDialect(expr)
	}
	return maskSQL(expr, backslashEscapes(e.Dialect))
}

func derivedFilterError(name string) error {
	return fmt.Errorf("metric %q: filter is not supported on derived metrics; put it on the base metrics it references", name)
}

// standardStringDialects read a backslash in '...' as an ordinary character.
var standardStringDialects = map[string]bool{
	"postgres": true, "duckdb": true, "redshift": true, "sqlite": true, "mssql": true,
	"tsql": true, "fabric": true, "oracle": true, "trino": true, "athena": true,
}

// backslashEscapes is false only for dialects known to treat a backslash as
// text; elsewhere it is true, which keeps DAC's \' escaped values in quotes.
func backslashEscapes(dialect string) bool { return !standardStringDialects[dialect] }

// Engine generates SQL from a Model and a Query.
type Engine struct {
	// Dialect selects warehouse-specific string escaping; empty uses SQL quote doubling.
	Dialect string

	model    *Model
	models   map[string]*Model
	metrics  map[string]*Metric
	dims     map[string]*Dimension
	segments map[string]*Segment
}

func NewEngine(m *Model) (*Engine, error) {
	return newEngine(m, nil)
}

func NewEngineWithModels(m *Model, models map[string]*Model) (*Engine, error) {
	return newEngine(m, models)
}

func newEngine(m *Model, models map[string]*Model) (*Engine, error) {
	if m == nil {
		return nil, errors.New("model is required")
	}

	modelSet := map[string]*Model{m.Name: m}
	for name, model := range models {
		if model != nil {
			modelSet[name] = model
		}
	}

	e := &Engine{
		model:    m,
		models:   modelSet,
		metrics:  make(map[string]*Metric),
		dims:     make(map[string]*Dimension),
		segments: make(map[string]*Segment),
	}
	for i := range m.Dimensions {
		e.dims[m.Dimensions[i].Name] = &m.Dimensions[i]
	}
	for i := range m.Metrics {
		e.metrics[m.Metrics[i].Name] = &m.Metrics[i]
	}
	for i := range m.Segments {
		e.segments[m.Segments[i].Name] = &m.Segments[i]
	}
	if err := e.validate(); err != nil {
		return nil, err
	}
	for _, model := range modelSet {
		if err := validateJoins(model); err != nil {
			return nil, err
		}
	}
	if err := validateJoinTargets(modelSet); err != nil {
		return nil, err
	}
	if err := e.validateChecks(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) validate() error {
	if e.model == nil {
		return errors.New("model is required")
	}
	if e.model.Name == "" {
		return errors.New("model name is required")
	}
	if err := e.model.Source.validate(); err != nil {
		return err
	}
	if err := validateJoins(e.model); err != nil {
		return err
	}

	names := make(map[string]bool)
	for _, d := range e.model.Dimensions {
		if d.Name == "" {
			return errors.New("dimension name is required")
		}
		if names[d.Name] {
			return fmt.Errorf("duplicate name: %s", d.Name)
		}
		names[d.Name] = true
	}
	for _, m := range e.model.Metrics {
		if m.Name == "" {
			return errors.New("metric name is required")
		}
		if m.Expression == "" {
			return fmt.Errorf("metric %q: expression is required", m.Name)
		}
		if names[m.Name] {
			return fmt.Errorf("duplicate name: %s", m.Name)
		}
		names[m.Name] = true
		if m.Filter != "" && e.isDerived(&m) {
			return derivedFilterError(m.Name)
		}
		// The dialect is not known yet, so accept either reading of backslashes;
		// SQL generation checks again with the dialect's rules.
		if m.Filter != "" {
			_, errEsc := applyMetricFilter(m.Expression, "true", true)
			if _, err := applyMetricFilter(m.Expression, "true", false); err != nil && errEsc != nil {
				return fmt.Errorf("metric %q: %w", m.Name, err)
			}
		}
	}
	for _, s := range e.model.Segments {
		if s.Name == "" {
			return errors.New("segment name is required")
		}
		if s.Filter == "" {
			return fmt.Errorf("segment %q: filter is required", s.Name)
		}
		if names[s.Name] {
			return fmt.Errorf("duplicate name: %s", s.Name)
		}
		names[s.Name] = true
	}

	noteIDs := make(map[string]bool, len(e.model.Notes))
	for _, note := range e.model.Notes {
		if note.ID == "" {
			return errors.New("note id is required")
		}
		if noteIDs[note.ID] {
			return fmt.Errorf("duplicate note id: %s", note.ID)
		}
		noteIDs[note.ID] = true
		if note.Dimensions == nil {
			return fmt.Errorf("note %q: dimensions is required", note.ID)
		}

		dimensionNames := make(map[string]bool, len(note.Dimensions))
		for _, dimension := range note.Dimensions {
			if dimension.Name == "" {
				return fmt.Errorf("note %q: dimension name is required", note.ID)
			}
			if dimensionNames[dimension.Name] {
				return fmt.Errorf("note %q: duplicate dimension %q", note.ID, dimension.Name)
			}
			dimensionNames[dimension.Name] = true
			// An empty type defaults to select.
			switch dimension.Type {
			case "", "select", "date", "date-range", "number", "boolean", "text":
			default:
				return fmt.Errorf("note %q: dimension %q has invalid type %q", note.ID, dimension.Name, dimension.Type)
			}
			if dimension.Multiselect && (dimension.Type == "boolean" || dimension.Type == "date-range") {
				return fmt.Errorf("note %q: dimension %q: multiselect is not supported for type %s", note.ID, dimension.Name, dimension.Type)
			}
		}
	}

	for _, m := range e.model.Metrics {
		if e.isDerived(&m) && !isWindow(&m) {
			if err := e.validateRefs(m.Name, map[string]bool{}); err != nil {
				return fmt.Errorf("metric %q: %w", m.Name, err)
			}
		}
	}

	for i := range e.model.Metrics {
		m := &e.model.Metrics[i]
		if isWindow(m) {
			if err := e.validateWindowMetric(m); err != nil {
				return err
			}
			if dep, ok := e.findMixedAggregationDep(m.Name); ok {
				return fmt.Errorf("window metric %q depends on metric %q which mixes {refs} with raw aggregation; split %q into a named base metric and a derived metric", m.Name, dep, dep)
			}
		}
	}
	return nil
}

// ValidateNote resolves the dimension names of the note with the given ID the
// same way a semantic query dimension is resolved, so joined names need an
// engine built with NewEngineWithModels.
func (e *Engine) ValidateNote(id string) error {
	for _, note := range e.model.Notes {
		if note.ID != id {
			continue
		}

		type resolvedKey struct {
			model     *Model
			relation  string
			dimension string
		}
		resolved := make(map[resolvedKey]bool, len(note.Dimensions))
		for _, dimension := range note.Dimensions {
			binding, err := e.resolveDimension(DimensionRef{Name: dimension.Name})
			if err != nil {
				return fmt.Errorf("note %q: %w", note.ID, err)
			}
			if dimension.Type == "date-range" && binding.dimension.Type != "time" {
				return fmt.Errorf("note %q: dimension %q: date-range requires a time dimension", note.ID, dimension.Name)
			}
			key := resolvedKey{model: binding.model, relation: binding.relationName, dimension: binding.dimension.Name}
			if resolved[key] {
				return fmt.Errorf("note %q: duplicate dimension %q", note.ID, dimension.Name)
			}
			resolved[key] = true
		}
		return nil
	}
	return fmt.Errorf("note not found: %s", id)
}

// isMixedExpression reports whether a metric expression contains both {refs}
// and a raw aggregation function call. Such metrics work in simple queries but
// cannot be hoisted into the inner subquery of a window-wrapped query.
func (e *Engine) isMixedExpression(m *Metric) bool {
	return e.isDerived(m) && e.containsAggregateOutsideRefs(m.Expression)
}

// findMixedAggregationDep walks the {ref} chain of a window metric and returns
// the name of the first transitive dependency that mixes {refs} with raw
// aggregation. The window metric itself is exempt — its expression must be a
// single {ref} (validated separately).
func (e *Engine) findMixedAggregationDep(rootName string) (string, bool) {
	visited := map[string]bool{}
	var walk func(string) (string, bool)
	walk = func(name string) (string, bool) {
		if visited[name] {
			return "", false
		}
		visited[name] = true
		m := e.metrics[name]
		if m == nil {
			return "", false
		}
		if name != rootName && e.isMixedExpression(m) {
			return name, true
		}
		for _, ref := range e.extractRefs(m.Expression) {
			if dep, ok := walk(ref); ok {
				return dep, true
			}
		}
		return "", false
	}
	return walk(rootName)
}

func (e *Engine) validateWindowMetric(m *Metric) error {
	refs := e.extractRefs(m.Expression)
	if len(refs) != 1 || strings.TrimSpace(m.Expression) != "{"+refs[0]+"}" {
		return fmt.Errorf("window metric %q: expression must be exactly a single {ref}, got %q", m.Name, m.Expression)
	}
	if e.metrics[refs[0]] == nil {
		return fmt.Errorf("window metric %q: references unknown metric {%s}", m.Name, refs[0])
	}

	switch m.Window.Type {
	case "running_total", "lag", "lead", "rank":
		if m.Window.OrderBy == "" {
			return fmt.Errorf("window metric %q: window.order_by is required for type %q", m.Name, m.Window.Type)
		}
		if e.dims[m.Window.OrderBy] == nil {
			return fmt.Errorf("window metric %q: window.order_by references unknown dimension %q", m.Name, m.Window.OrderBy)
		}
		for _, p := range m.Window.PartitionBy {
			if e.dims[p] == nil {
				return fmt.Errorf("window metric %q: window.partition_by references unknown dimension %q", m.Name, p)
			}
		}
	case "percent_of_total":
		for _, p := range m.Window.PartitionBy {
			if e.dims[p] == nil {
				return fmt.Errorf("window metric %q: window.partition_by references unknown dimension %q", m.Name, p)
			}
		}
	case "":
		return fmt.Errorf("window metric %q: window.type is required", m.Name)
	default:
		return fmt.Errorf("window metric %q: unknown window.type %q", m.Name, m.Window.Type)
	}
	return nil
}

// validateRefs checks that all {refs} in a metric resolve and there are no cycles.
func (e *Engine) validateRefs(name string, visited map[string]bool) error {
	if visited[name] {
		return fmt.Errorf("circular dependency: %s", name)
	}
	visited[name] = true
	m := e.metrics[name]
	if m == nil {
		return fmt.Errorf("metric not found: %s", name)
	}
	for _, ref := range e.extractRefs(m.Expression) {
		if e.metrics[ref] == nil {
			return fmt.Errorf("references unknown metric {%s}", ref)
		}
		if err := e.validateRefs(ref, copyVisited(visited)); err != nil {
			return err
		}
	}
	return nil
}

// GenerateSQL produces a SQL query string for the given Query.
func (e *Engine) GenerateSQL(q *Query) (string, error) {
	sql, _, err := e.GenerateSQLWithColumns(q)
	return sql, err
}

// GenerateGrandTotalSQL recalculates metrics over the source rows behind the
// rows the query returns, so limits and aggregate filters/segments bound the
// totals. Window metrics and their dependents return NULL. The input query and
// engine are not modified.
func (e *Engine) GenerateGrandTotalSQL(q *Query) (string, error) {
	return e.totalSQL(q, q.Metrics, nil, true)
}

// totalSQL computes metrics over q's rows grouped by dims; restrict keeps only the rows q shows.
func (e *Engine) totalSQL(q *Query, metrics []string, dims []DimensionRef, restrict bool) (string, error) {
	if len(metrics) == 0 {
		return "", errors.New("grand totals require at least one metric")
	}
	plan, err := e.planQuery(q)
	if err != nil {
		return "", err
	}
	if err := e.validateQuery(q, plan); err != nil {
		return "", err
	}

	grandTotal := *q
	grandTotal.Dimensions, grandTotal.Sort, grandTotal.Limit = dims, nil, 0
	grandTotal.Filters = nil
	for _, filter := range q.Filters {
		if filter.Expression != "" {
			_, having, err := e.expandFilterExpr(filter.Expression, plan)
			if err != nil {
				return "", err
			}
			if having {
				continue
			}
		}
		grandTotal.Filters = append(grandTotal.Filters, filter)
	}
	grandTotal.Segments = nil
	for _, name := range q.Segments {
		_, having, err := e.expandFilterExpr(e.segments[name].Filter, plan)
		if err != nil {
			return "", err
		}
		if !having {
			grandTotal.Segments = append(grandTotal.Segments, name)
		}
	}
	grandTotalEngine := *e
	grandTotalEngine.metrics = maps.Clone(e.metrics)
	for _, name := range metrics {
		if e.needsWindowWrap([]string{name}) {
			metric := *e.metrics[name]
			// An aggregate NULL also returns one row for window-only queries.
			metric.Expression, metric.Filter, metric.Window = "SUM(NULLIF(0, 0))", "", nil
			grandTotalEngine.metrics[name] = &metric
		}
	}
	// Keep an aggregate grain even when every requested metric is a constant.
	// The outer projection hides the helper metric from the result.
	rowCount := "__grand_total_row_count"
	for {
		if _, err := e.resolveDimension(DimensionRef{Name: rowCount}); e.metrics[rowCount] == nil && err != nil {
			break
		}
		rowCount += "_"
	}
	grandTotalEngine.metrics[rowCount] = &Metric{Name: rowCount, Expression: "COUNT(*)"}
	// Compute each metric once; the outer projection repeats it by name.
	unique := func(names []string) []string {
		var out []string
		for _, name := range names {
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
		return out
	}
	grandTotal.Metrics = append(unique(metrics), rowCount)
	// Replan without excluded filters so HAVING-only joins cannot affect totals.
	// When a limit or aggregate filter can hide rows, join to the rows the
	// detail query returns. Window queries return one row per combination of
	// their inner dimensions, so those key the rows.
	shown := *q
	shown.Metrics = unique(q.Metrics) // SQL Server rejects repeated derived-table columns
	if e.needsWindowWrap(q.Metrics) {
		shown.Dimensions = e.collectInnerDimensions(q)
	}
	// Identical refs return the same rows; sorting matters only with a limit.
	seen := map[DimensionRef]bool{}
	shown.Dimensions = slices.DeleteFunc(slices.Clone(shown.Dimensions), func(d DimensionRef) bool {
		if seen[d] {
			return true
		}
		seen[d] = true
		return false
	})
	if shown.Limit == 0 {
		shown.Sort = nil
	}
	restrict = restrict && (len(grandTotal.Filters) < len(q.Filters) || len(grandTotal.Segments) < len(q.Segments) || (len(shown.Dimensions) > 0 && q.Limit > 0))
	planned := grandTotal
	if restrict {
		planned.Dimensions = slices.Clone(shown.Dimensions)
		for _, d := range dims {
			if !slices.Contains(planned.Dimensions, d) {
				planned.Dimensions = append(planned.Dimensions, d)
			}
		}
	}
	totalPlan, err := grandTotalEngine.planQuery(&planned)
	if err != nil {
		return "", err
	}
	var cols []string
	for _, d := range dims {
		cols = append(cols, totalPlan.dimensionBinding(d).outputAlias)
	}
	cols = append(cols, metrics...)
	if restrict {
		shownSQL, columns, err := e.GenerateSQLWithColumns(&shown)
		if err != nil {
			return "", err
		}
		keys := make([]string, len(shown.Dimensions))
		conditions := make([]string, len(shown.Dimensions))
		names := map[string]bool{}
		for i, d := range shown.Dimensions {
			if names[strings.ToLower(columns[i].Name)] {
				// Two dimensions share a column, so the shown rows can't be matched.
				return "SELECT NULL AS " + strings.Join(cols, ", NULL AS "), nil
			}
			names[strings.ToLower(columns[i].Name)] = true
			key := fmt.Sprintf("__grand_total_shown_%d", i)
			keys[i] = columns[i].Name + " AS " + key
			shown := "__grand_total_shown." + key
			source := totalPlan.dimensionSQL(totalPlan.dimensionBinding(d))
			conditions[i] = fmt.Sprintf("(%s = (%s) OR (%s IS NULL AND (%s) IS NULL))", shown, source, shown, source)
		}
		if len(keys) == 0 {
			// A metric-only query's one row may be hidden by an aggregate filter.
			keys, conditions = []string{"1 AS __grand_total_shown_0"}, []string{"1 = 1"}
		}
		totalPlan.shownJoin = " INNER JOIN (SELECT DISTINCT " + strings.Join(keys, ", ") + " FROM (" + shownSQL + ") __grand_total_detail) __grand_total_shown ON " + strings.Join(conditions, " AND ")
	}
	sql, err := grandTotalEngine.generateSimple(&grandTotal, totalPlan)
	if err != nil {
		return "", err
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM (" + sql + ") grand_total", nil
}

// GenerateSeriesSQL returns metrics per q's dimensions and by (by alone with
// total), ordered by by: one point per row and x, such as for a sparkline. It
// covers only the rows q returns and, with limit > 0, each row's last limit
// points. A by among the dimensions that key q's rows keeps every point, since
// each row needs the ones before its own x.
func (e *Engine) GenerateSeriesSQL(q *Query, by DimensionRef, metrics []string, limit int, total bool) (string, error) {
	keys := q.Dimensions
	if e.needsWindowWrap(q.Metrics) {
		keys = e.collectInnerDimensions(q) // as totalSQL keys the shown rows
	}
	restrict := !slices.ContainsFunc(keys, func(d DimensionRef) bool { return d.Name == by.Name })
	sql, x, err := e.seriesSQL(q, by, metrics, limit, restrict, total)
	if err != nil {
		return "", err
	}
	return sql + " ORDER BY " + x, nil
}

func (e *Engine) seriesSQL(q *Query, by DimensionRef, metrics []string, limit int, restrict, total bool) (sql, x string, err error) {
	dims := []DimensionRef{by}
	if !total {
		dims = q.Dimensions
		if !slices.Contains(dims, by) {
			dims = append(slices.Clone(dims), by)
		}
	}
	points := &Query{Dimensions: dims, Metrics: metrics}
	plan, err := e.planQuery(points)
	if err != nil {
		return "", "", err
	}
	if err := e.validateQuery(points, plan); err != nil {
		return "", "", err
	}
	if e.needsWindowWrap(metrics) {
		return "", "", errors.New("window metrics are not supported in series")
	}
	x = plan.dimensionBinding(by).outputAlias
	var cols, keys []string
	for _, d := range dims {
		alias := plan.dimensionBinding(d).outputAlias
		if d != by && alias == x {
			return "", "", fmt.Errorf("%s and dimension %s both return column %s", by.Name, d.Name, x)
		}
		cols = append(cols, alias)
		if d != by {
			keys = append(keys, alias)
		}
	}
	if slices.Contains(metrics, x) {
		return "", "", fmt.Errorf("%s and metric %s both return column %s", by.Name, x, x)
	}
	if sql, err = e.totalSQL(q, metrics, dims, restrict || total); err != nil {
		return "", "", err
	}
	if limit <= 0 || (!total && slices.Contains(q.Dimensions, by)) {
		return "SELECT * FROM (" + sql + ") points WHERE " + x + " IS NOT NULL", x, nil
	}
	partition := ""
	if len(keys) > 0 {
		partition = "PARTITION BY " + strings.Join(keys, ", ") + " "
	}
	cols = append(cols, metrics...)
	// Each metric keeps its own last points that have a value, so a NULL y isn't counted.
	ranks, kept := make([]string, len(metrics)), make([]string, len(metrics))
	for i, m := range metrics {
		rank := fmt.Sprintf("__series_rank_%d", i)
		for slices.ContainsFunc(cols, func(c string) bool { return strings.EqualFold(c, rank) }) {
			rank += "_"
		}
		ranks[i] = fmt.Sprintf("CASE WHEN %[1]s IS NOT NULL THEN ROW_NUMBER() OVER (%[2]sORDER BY CASE WHEN %[1]s IS NULL THEN 1 ELSE 0 END, %[3]s DESC) END AS %[4]s", m, partition, x, rank)
		kept[i] = fmt.Sprintf("%s <= %d", rank, limit)
	}
	return fmt.Sprintf("SELECT %s FROM (SELECT points.*, %s FROM (%s) points WHERE %s IS NOT NULL) ranked WHERE %s",
		strings.Join(cols, ", "), strings.Join(ranks, ", "), sql, x, strings.Join(kept, " OR ")), x, nil
}

// Series is one list column for AddSeriesSQL.
type Series struct {
	By     DimensionRef `json:"by"`              // x
	Metric string       `json:"metric"`          // y
	Limit  int          `json:"limit,omitempty"` // last N points; 0 keeps all
}

// listDialects build a list of {x, y} points newest first, skipping a NULL y, from
// x, y and the limit suffix (empty, or limit with N).
var listDialects = map[string]struct{ list, limit string }{
	"bigquery":              {"ARRAY_AGG(IF(%[2]s IS NOT NULL, STRUCT(%[1]s AS x, CAST(%[2]s AS FLOAT64) AS y), NULL) IGNORE NULLS ORDER BY %[1]s DESC%[3]s)", " LIMIT %d"},
	"google_cloud_platform": {"ARRAY_AGG(IF(%[2]s IS NOT NULL, STRUCT(%[1]s AS x, CAST(%[2]s AS FLOAT64) AS y), NULL) IGNORE NULLS ORDER BY %[1]s DESC%[3]s)", " LIMIT %d"},
	"duckdb":                {"(ARRAY_AGG({'x': %[1]s, 'y': CAST(%[2]s AS DOUBLE)} ORDER BY %[1]s DESC) FILTER (WHERE %[2]s IS NOT NULL))%[3]s", "[1:%d]"},
}

// AddSeriesSQL joins each series' points, newest first, into sql (q's detail or,
// with total, grand-total SQL) as column __series_<i>. ok is false when it
// can't; callers then run GenerateSeriesSQL.
func (e *Engine) AddSeriesSQL(q *Query, sql string, series []Series, total bool) (string, bool, error) {
	dialect, ok := listDialects[e.Dialect]
	// Drivers may not read a DECIMAL x inside a list, so a number x isn't merged.
	if !ok || slices.ContainsFunc(series, func(s Series) bool {
		b, err := e.resolveDimension(s.By)
		return slices.Contains(q.Dimensions, s.By) || err != nil || b.dimension.Type == "number"
	}) {
		return "", false, nil
	}
	plan, err := e.planQuery(q)
	if err != nil {
		return "", false, err
	}
	var keys, order []string
	if !total {
		for _, d := range q.Dimensions {
			key := plan.dimensionBinding(d).outputAlias
			if slices.Contains(keys, key) {
				return "", false, nil
			}
			keys = append(keys, key)
		}
		// The join may not keep the rows' order, so repeat q's sort and tie-breakers.
		for _, s := range q.Sort {
			name := s.Name
			if binding := plan.sortBinding(s.Name); binding != nil {
				name = binding.outputAlias
			}
			if !slices.Contains(keys, name) && !slices.Contains(q.Metrics, name) {
				return "", false, nil
			}
			order = append(order, "__rows."+name+" "+cmp.Or(strings.ToUpper(s.Direction), "ASC"))
		}
		if len(order) > 0 && q.Limit > 0 {
			for _, key := range keys {
				order = append(order, "__rows."+key+" ASC")
			}
		}
	}
	cols, joins := []string{"__rows.*"}, []string{}
	var groups []DimensionRef
	for _, s := range series {
		if !slices.Contains(groups, s.By) {
			groups = append(groups, s.By)
		}
	}
	for g, by := range groups {
		var metrics []string
		for _, s := range series {
			if s.By == by && !slices.Contains(metrics, s.Metric) {
				metrics = append(metrics, s.Metric)
			}
		}
		points, x, err := e.seriesSQL(q, by, metrics, 0, false, total)
		if err != nil {
			return "", false, err
		}
		name := fmt.Sprintf("__series_join_%d", g)
		selects := slices.Clone(keys)
		for i, s := range series {
			if s.By != by {
				continue
			}
			limit := ""
			if s.Limit > 0 {
				limit = fmt.Sprintf(dialect.limit, s.Limit)
			}
			list := fmt.Sprintf(dialect.list, x, s.Metric, limit)
			col := fmt.Sprintf("__series_%d", i)
			selects = append(selects, list+" AS "+col)
			cols = append(cols, name+"."+col)
		}
		groupBy, conditions := "", []string{"TRUE"}
		if len(keys) > 0 {
			// Not IS NOT DISTINCT FROM: BigQuery can't hash-join on it.
			groupBy, conditions = " GROUP BY "+strings.Join(keys, ", "), nil
			for _, key := range keys {
				conditions = append(conditions, fmt.Sprintf("(__rows.%[1]s = %[2]s.%[1]s OR (__rows.%[1]s IS NULL AND %[2]s.%[1]s IS NULL))", key, name))
			}
		}
		joins = append(joins, fmt.Sprintf(" LEFT JOIN (SELECT %s FROM (%s) __points%s) %s ON %s", strings.Join(selects, ", "), points, groupBy, name, strings.Join(conditions, " AND ")))
	}
	sql = "SELECT " + strings.Join(cols, ", ") + " FROM (" + sql + ") __rows" + strings.Join(joins, "")
	if len(order) > 0 {
		sql += " ORDER BY " + strings.Join(order, ", ")
	}
	return sql, true, nil
}

// GenerateSQLWithColumns compiles the query and also returns the result set's
// columns in SELECT order. Each column carries both the name as it appears in
// the SQL (Name) and the dimension/metric the query referenced (Field), so
// callers can map results back to the request without re-deriving the engine's
// column aliasing (qualified joined dimensions are sanitized, e.g.
// "customers.country" -> "customers_country").
func (e *Engine) GenerateSQLWithColumns(q *Query) (string, []QueryColumn, error) {
	plan, err := e.planQuery(q)
	if err != nil {
		return "", nil, err
	}
	if err := e.validateQuery(q, plan); err != nil {
		return "", nil, err
	}

	var sql string
	if e.needsWindowWrap(q.Metrics) {
		sql, err = e.generateWrapped(q, plan)
	} else {
		sql, err = e.generateSimple(q, plan)
	}
	if err != nil {
		return "", nil, err
	}
	return sql, e.outputColumns(q, plan), nil
}

// outputColumns reports the result columns in the same order the SELECT emits
// them: dimensions (in query order) followed by metrics (in query order).
func (e *Engine) outputColumns(q *Query, plan *queryPlan) []QueryColumn {
	cols := make([]QueryColumn, 0, len(q.Dimensions)+len(q.Metrics))
	for _, d := range q.Dimensions {
		name := d.Name
		if binding := plan.dimensionBinding(d); binding != nil {
			name = binding.outputAlias
		}
		cols = append(cols, QueryColumn{Name: name, Field: d.Name})
	}
	for _, name := range q.Metrics {
		cols = append(cols, QueryColumn{Name: name, Field: name})
	}
	return cols
}

func (e *Engine) validateQuery(q *Query, plan *queryPlan) error {
	if len(q.Dimensions) == 0 && len(q.Metrics) == 0 {
		return errors.New("query must include at least one dimension or metric")
	}
	for _, d := range q.Dimensions {
		binding := plan.dimensionBinding(d)
		dim := binding.dimension
		if d.Granularity != "" {
			if dim.Type != "time" {
				return fmt.Errorf("granularity on non-time dimension: %s", d.Name)
			}
			if _, ok := dim.Granularities[d.Granularity]; !ok {
				return fmt.Errorf("invalid granularity %q for dimension %s", d.Granularity, d.Name)
			}
		}
	}
	for _, name := range q.Metrics {
		if e.metrics[name] == nil {
			return fmt.Errorf("metric not found: %s", name)
		}
	}
	for _, name := range q.Segments {
		if e.segments[name] == nil {
			return fmt.Errorf("segment not found: %s", name)
		}
	}
	for _, f := range q.Filters {
		if f.Expression != "" {
			continue
		}
		if f.Dimension == "" {
			return errors.New("filter dimension is required")
		}
		if err := validateStructuredFilter(f); err != nil {
			return err
		}
	}
	for _, sort := range q.Sort {
		if e.metrics[sort.Name] == nil && plan.sortBinding(sort.Name) == nil {
			return fmt.Errorf("sort field not found: %s", sort.Name)
		}
	}
	return nil
}

func (e *Engine) generateSimple(q *Query, plan *queryPlan) (string, error) {
	var exprs, aliases []string
	groupBy := make([]string, 0, len(q.Dimensions))

	for i, d := range q.Dimensions {
		binding := plan.dimensionBinding(d)
		exprs = append(exprs, plan.dimensionSQL(binding))
		aliases = append(aliases, binding.outputAlias)
		groupBy = append(groupBy, strconv.Itoa(i+1))
	}

	for _, name := range q.Metrics {
		expanded, err := e.expandSimple(name, map[string]bool{}, plan)
		if err != nil {
			return "", err
		}
		exprs = append(exprs, expanded)
		aliases = append(aliases, name)
	}

	rest := plan.fromSQL()
	where, having, err := e.buildWhereHaving(q, plan)
	if err != nil {
		return "", err
	}
	if where != "" {
		rest += " WHERE " + where
	}
	if len(groupBy) > 0 {
		rest += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if having != "" {
		rest += " HAVING " + having
	}
	return e.selectSQL(exprs, aliases, rest) + e.buildOrderAndLimit(q, plan, false), nil
}

// selectSQL returns SELECT exprs AS aliases followed by rest.
func (e *Engine) selectSQL(exprs, aliases []string, rest string) string {
	sel := make([]string, 0, len(exprs))
	for i := range exprs {
		sel = append(sel, exprs[i]+" AS "+aliases[i])
	}
	return "SELECT " + strings.Join(sel, ", ") + rest
}

func (e *Engine) expandSimple(name string, visited map[string]bool, plan *queryPlan) (string, error) {
	if visited[name] {
		return "", fmt.Errorf("circular dependency: %s", name)
	}
	visited[name] = true

	m := e.metrics[name]
	if m == nil {
		return "", fmt.Errorf("metric not found: %s", name)
	}
	if m.Filter != "" && e.isDerived(m) {
		return "", derivedFilterError(name)
	}

	if isWindow(m) {
		return "", fmt.Errorf("window metric %q in simple query path", name)
	}

	if !e.isDerived(m) {
		expr := m.Expression
		if plan != nil && plan.hasJoins() {
			expr = qualifySQLIdentifiers(expr, "base", backslashEscapes(e.Dialect))
		}
		if m.Filter != "" {
			expandedFilter, _, err := e.expandFilterExpr(m.Filter, plan)
			if err != nil {
				return "", err
			}
			filtered, err := applyMetricFilter(expr, expandedFilter, backslashEscapes(e.Dialect))
			if err != nil {
				return "", fmt.Errorf("metric %q: %w", name, err)
			}
			return filtered, nil
		}
		return expr, nil
	}

	return e.expandRefs(m.Expression, func(refName string) (string, error) {
		expanded, err := e.expandSimple(refName, copyVisited(visited), plan)
		if err != nil {
			return "", err
		}
		if containsOperator(expanded) {
			return "(" + expanded + ")", nil
		}
		return expanded, nil
	})
}

func (e *Engine) generateWrapped(q *Query, plan *queryPlan) (string, error) {
	innerMetrics := e.collectInnerMetrics(q.Metrics)
	innerDimensions := e.collectInnerDimensions(q)

	var innerExprs, innerAliases []string
	groupBy := make([]string, 0, len(innerDimensions))
	for i, d := range innerDimensions {
		binding := plan.dimensionBinding(d)
		if binding == nil {
			var err error
			binding, err = e.resolveDimension(d)
			if err != nil {
				return "", err
			}
			if err := plan.addDimension(binding); err != nil {
				return "", err
			}
		}
		innerExprs = append(innerExprs, plan.dimensionSQL(binding))
		innerAliases = append(innerAliases, binding.outputAlias)
		groupBy = append(groupBy, strconv.Itoa(i+1))
	}
	for _, name := range innerMetrics {
		if err := e.validateMetricFiltersForWrapped(name, map[string]bool{}); err != nil {
			return "", err
		}
		expr, err := e.expandSimple(name, map[string]bool{}, plan)
		if err != nil {
			return "", err
		}
		innerExprs = append(innerExprs, expr)
		innerAliases = append(innerAliases, name)
	}

	inner := plan.fromSQL()

	where, having, err := e.buildWhereHaving(q, plan)
	if err != nil {
		return "", err
	}
	if where != "" {
		inner += " WHERE " + where
	}
	if len(groupBy) > 0 {
		inner += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if having != "" {
		inner += " HAVING " + having
	}
	inner = e.selectSQL(innerExprs, innerAliases, inner)

	var outerSel []string
	for _, d := range q.Dimensions {
		binding := plan.dimensionBinding(d)
		outerSel = append(outerSel, "base."+binding.outputAlias)
	}
	for _, name := range q.Metrics {
		expanded, err := e.expandOuter(name, map[string]bool{})
		if err != nil {
			return "", err
		}
		outerSel = append(outerSel, expanded+" AS "+name)
	}

	sql := "SELECT " + strings.Join(outerSel, ", ")
	sql += " FROM (" + inner + ") base"
	sql += e.buildOrderAndLimit(q, plan, true)
	return sql, nil
}

func (e *Engine) expandOuter(name string, visited map[string]bool) (string, error) {
	if visited[name] {
		return "", fmt.Errorf("circular dependency: %s", name)
	}
	visited[name] = true

	m := e.metrics[name]
	if m == nil {
		return "", fmt.Errorf("metric not found: %s", name)
	}
	if m.Filter != "" && e.isDerived(m) {
		return "", derivedFilterError(name)
	}

	if !e.isDerived(m) && !isWindow(m) {
		return "base." + name, nil
	}

	if isWindow(m) {
		return e.windowSQL(m), nil
	}

	return e.expandRefs(m.Expression, func(refName string) (string, error) {
		expanded, err := e.expandOuter(refName, copyVisited(visited))
		if err != nil {
			return "", err
		}
		if containsOperator(expanded) {
			return "(" + expanded + ")", nil
		}
		return expanded, nil
	})
}

func (e *Engine) windowSQL(m *Metric) string {
	refs := e.extractRefs(m.Expression)
	if len(refs) == 0 {
		return m.Expression
	}
	refName := refs[0]

	var parts []string
	for _, p := range m.Window.PartitionBy {
		parts = append(parts, "base."+p)
	}
	orderBy := "base." + m.Window.OrderBy

	partitionClause := ""
	if len(parts) > 0 {
		partitionClause = "PARTITION BY " + strings.Join(parts, ", ") + " "
	}

	switch m.Window.Type {
	case "running_total":
		return fmt.Sprintf("SUM(base.%s) OVER (%sORDER BY %s ROWS UNBOUNDED PRECEDING)", refName, partitionClause, orderBy)
	case "lag":
		offset := m.Window.Offset
		if offset == 0 {
			offset = 1
		}
		return fmt.Sprintf("LAG(base.%s, %d) OVER (%sORDER BY %s)", refName, offset, partitionClause, orderBy)
	case "lead":
		offset := m.Window.Offset
		if offset == 0 {
			offset = 1
		}
		return fmt.Sprintf("LEAD(base.%s, %d) OVER (%sORDER BY %s)", refName, offset, partitionClause, orderBy)
	case "rank":
		return fmt.Sprintf("RANK() OVER (%sORDER BY %s)", partitionClause, orderBy)
	case "percent_of_total":
		return fmt.Sprintf("base.%s / NULLIF(SUM(base.%s) OVER (%s), 0)", refName, refName, strings.TrimSpace(partitionClause))
	default:
		return m.Expression
	}
}

func (e *Engine) expandRefs(expr string, resolve func(string) (string, error)) (string, error) {
	masked := e.maskSQL(expr)
	indices := refPattern.FindAllStringSubmatchIndex(masked, -1)
	if len(indices) == 0 {
		return expr, nil
	}

	var b strings.Builder
	prev := 0

	for _, loc := range indices {
		fullStart, fullEnd := loc[0], loc[1]
		nameStart, nameEnd := loc[2], loc[3]
		refName := expr[nameStart:nameEnd]

		b.WriteString(expr[prev:fullStart])

		expanded, err := resolve(refName)
		if err != nil {
			return "", err
		}

		before := strings.TrimRight(expr[:fullStart], " \t")
		if len(before) > 0 && before[len(before)-1] == '/' {
			b.WriteString("NULLIF(")
			b.WriteString(expanded)
			b.WriteString(", 0)")
		} else {
			b.WriteString(expanded)
		}

		prev = fullEnd
	}
	b.WriteString(expr[prev:])
	return b.String(), nil
}

var distinctPrefix = regexp.MustCompile(`(?i)^DISTINCT\b`)

// aggregateModifierPattern matches clauses inside an aggregate's parentheses
// that cannot go inside the CASE, such as array_agg(x ORDER BY y).
var aggregateModifierPattern = regexp.MustCompile(`(?i)\b(ORDER\s+BY|IGNORE\s+NULLS|RESPECT\s+NULLS|SEPARATOR|LIMIT)\b`)

// applyMetricFilter puts filter inside a metric that is one aggregate call
// with one argument, such as sum(x). Like Cube and Looker, other shapes are
// rejected: filter a base metric and wrap it in a derived metric instead.
func applyMetricFilter(expr, filter string, backslashEscapes bool) (string, error) {
	shapeErr := fmt.Errorf("a metric with filter must be one aggregate call with one argument, such as sum(amount), got %q; to round or combine it, reference it from a derived metric such as round({metric}, 2)", expr)
	masked := maskSQL(expr, backslashEscapes)
	end := len(strings.TrimRight(masked, " \t\r\n;"))
	start := end - len(strings.TrimLeft(masked[:end], " \t\r\n"))
	open := strings.IndexByte(masked[start:end], '(') + start
	if open <= start || findMatchingParen(masked, open) != end-1 {
		return "", shapeErr
	}
	name := strings.TrimSpace(expr[start:open])
	inner, maskedInner := expr[open+1:end-1], masked[open+1:end-1]
	top := topLevelSQL(maskedInner)
	if !plainColumnPattern.MatchString(name) || aggregateFuncPattern.MatchString(maskedInner) || strings.Contains(top, ",") || aggregateModifierPattern.MatchString(top) {
		return "", shapeErr
	}

	arg, distinct := strings.TrimSpace(inner), ""
	if distinctPrefix.MatchString(arg) {
		arg, distinct = strings.TrimSpace(arg[len("DISTINCT"):]), "DISTINCT "
	}
	if arg == "*" {
		arg = "1"
	}
	if arg == "" {
		return "", shapeErr
	}
	return fmt.Sprintf("%s(%sCASE WHEN %s THEN %s ELSE NULL END)", name, distinct, endLineComment(filter), endLineComment(arg)), nil
}

// topLevelSQL blanks everything inside parentheses in masked SQL.
func topLevelSQL(masked string) string {
	b := []byte(masked)
	depth := 0
	for i, c := range b {
		switch {
		case c == '(':
			depth++
			b[i] = ' '
		case c == ')':
			depth--
			b[i] = ' '
		case depth > 0:
			b[i] = ' '
		}
	}
	return string(b)
}

// endLineComment ends a trailing -- comment so the SQL after it still runs.
func endLineComment(s string) string {
	if strings.Contains(s, "--") {
		return s + "\n"
	}
	return s
}

func findMatchingParen(s string, openIdx int) int {
	depth := 1
	for i := openIdx + 1; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func (e *Engine) dimExpr(d *Dimension, granularity string) string {
	if granularity != "" {
		if expr, ok := d.Granularities[granularity]; ok {
			return expr
		}
	}
	if d.Expression != "" {
		return d.Expression
	}
	return d.Name
}

func (e *Engine) buildWhereHaving(q *Query, plan *queryPlan) (where, having string, err error) {
	var whereParts, havingParts []string

	for _, f := range q.Filters {
		if f.Expression == "" {
			// The plan resolves dimensions; keep literal values out of expression rewriting.
			whereParts = append(whereParts, e.filterToSQL(f, plan))
			continue
		}
		expanded, needsHaving, err := e.expandFilterExpr(f.Expression, plan)
		if err != nil {
			return "", "", err
		}
		if needsHaving {
			havingParts = append(havingParts, expanded)
		} else {
			whereParts = append(whereParts, expanded)
		}
	}

	for _, name := range q.Segments {
		expanded, needsHaving, err := e.expandFilterExpr(e.segments[name].Filter, plan)
		if err != nil {
			return "", "", err
		}
		if needsHaving {
			havingParts = append(havingParts, expanded)
		} else {
			whereParts = append(whereParts, expanded)
		}
	}

	return strings.Join(whereParts, " AND "), strings.Join(havingParts, " AND "), nil
}

func (e *Engine) expandFilterExpr(expr string, plan *queryPlan) (string, bool, error) {
	masked := e.maskSQL(expr)
	hasAggregate := e.containsAggregateOutsideRefs(expr)
	if !refPattern.MatchString(masked) {
		if plan != nil && plan.hasJoins() {
			expr = qualifySQLIdentifiers(expr, "base", backslashEscapes(e.Dialect))
		}
		return expr, hasAggregate, nil
	}

	hasMetricRef := false
	var expandErr error

	expand := func(match string) string {
		refName := match[1 : len(match)-1]

		if dim, ok := e.dims[refName]; ok {
			expr := e.dimExpr(dim, "")
			if plan != nil && plan.hasJoins() {
				expr = qualifySQLIdentifiers(expr, "base", backslashEscapes(e.Dialect))
			}
			return expr
		}

		if plan != nil {
			if binding, err := e.resolveDimension(DimensionRef{Name: refName}); err == nil && binding.model != e.model {
				if err := plan.addDimension(binding); err != nil {
					expandErr = err
					return match
				}
				return plan.dimensionSQL(binding)
			}
		}

		if _, ok := e.metrics[refName]; ok {
			hasMetricRef = true
			expanded, err := e.expandSimple(refName, map[string]bool{}, plan)
			if err != nil {
				expandErr = err
				return match
			}
			return expanded
		}

		expandErr = fmt.Errorf("unknown reference {%s} in filter", refName)
		return match
	}

	// Refs are found in the masked text, so a {name} inside quotes stays text.
	var b strings.Builder
	prev := 0
	for _, loc := range refPattern.FindAllStringIndex(masked, -1) {
		b.WriteString(expr[prev:loc[0]])
		b.WriteString(expand(expr[loc[0]:loc[1]]))
		prev = loc[1]
	}
	b.WriteString(expr[prev:])
	return b.String(), hasMetricRef || hasAggregate, expandErr
}

func (e *Engine) buildOrderAndLimit(q *Query, plan *queryPlan, outer bool) string {
	var s string
	if len(q.Sort) > 0 {
		parts := make([]string, 0, len(q.Sort))
		sorted := map[string]bool{}
		for _, sort := range q.Sort {
			dir := strings.ToUpper(sort.Direction)
			if dir == "" {
				dir = "ASC"
			}
			name := sort.Name
			if binding := plan.sortBinding(sort.Name); binding != nil {
				name = binding.outputAlias
				if outer {
					name = "base." + name
				}
			}
			sorted[name] = true
			parts = append(parts, name+" "+dir)
		}
		// Break ties by the grouping key so reruns of a limited query, such as
		// the grand total's copy, pick the same rows when sort values tie
		// exactly. Float aggregates can still differ in their last bits.
		keys := q.Dimensions
		if outer {
			keys = e.collectInnerDimensions(q)
		}
		for i, d := range keys {
			binding := plan.dimensionBinding(d)
			if q.Limit == 0 || binding == nil {
				continue
			}
			// Dimension i is select column i+1; positions stay unambiguous when
			// two dimensions share an alias.
			name, ref := binding.outputAlias, strconv.Itoa(i+1)
			if outer {
				name = "base." + name
				ref = name
			}
			if !sorted[name] && !sorted[ref] {
				sorted[ref] = true
				parts = append(parts, ref+" ASC")
			}
		}
		s += " ORDER BY " + strings.Join(parts, ", ")
	}
	if q.Limit > 0 {
		s += fmt.Sprintf(" LIMIT %d", q.Limit)
	}
	return s
}

func validateStructuredFilter(f Filter) error {
	switch f.Operator {
	case "equals", "not_equals", "gt", "gte", "lt", "lte", "in", "not_in":
		if f.Value == nil {
			return fmt.Errorf("filter %s on dimension %s needs a value; use is_null or is_not_null to match null", f.Operator, f.Dimension)
		}
		return nil
	case "is_null", "is_not_null":
		return nil
	case "between":
		if _, _, ok := betweenValues(f.Value); !ok {
			return fmt.Errorf("invalid between filter value for dimension %s", f.Dimension)
		}
		return nil
	default:
		return fmt.Errorf("invalid filter operator %q for dimension %s", f.Operator, f.Dimension)
	}
}

func (e *Engine) filterToSQL(f Filter, plan *queryPlan) string {
	if f.Expression != "" {
		return f.Expression
	}
	dim := f.Dimension
	if binding := plan.dimensionBinding(DimensionRef{Name: f.Dimension}); binding != nil {
		dim = plan.dimensionSQL(binding)
	}
	// Group expression dimensions such as "NOT active" so operators like IS NULL
	// apply to the whole expression rather than binding inside it.
	if !plainColumnPattern.MatchString(dim) {
		dim = "(" + dim + ")"
	}
	switch f.Operator {
	case "equals":
		return fmt.Sprintf("%s = %s", dim, formatValue(f.Value, e.Dialect))
	case "not_equals":
		// Keep null rows: "everything except X" should still count rows where the dimension is null.
		return fmt.Sprintf("(%s != %s OR %s IS NULL)", dim, formatValue(f.Value, e.Dialect), dim)
	case "gt":
		return fmt.Sprintf("%s > %s", dim, formatValue(f.Value, e.Dialect))
	case "gte":
		return fmt.Sprintf("%s >= %s", dim, formatValue(f.Value, e.Dialect))
	case "lt":
		return fmt.Sprintf("%s < %s", dim, formatValue(f.Value, e.Dialect))
	case "lte":
		return fmt.Sprintf("%s <= %s", dim, formatValue(f.Value, e.Dialect))
	case "in", "not_in":
		return membershipSQL(dim, f.Operator == "not_in", f.Value, e.Dialect)
	case "between":
		start, end, ok := betweenValues(f.Value)
		if !ok {
			return ""
		}
		return fmt.Sprintf("%s BETWEEN %s AND %s", dim, formatValue(start, e.Dialect), formatValue(end, e.Dialect))
	case "is_null":
		return dim + " IS NULL"
	case "is_not_null":
		return dim + " IS NOT NULL"
	default:
		return ""
	}
}

// membershipSQL compiles in/not_in. A null in the list selects (in) or excludes
// (not_in) null rows; without one, not_in still keeps null rows, matching not_equals.
func membershipSQL(dim string, negate bool, value interface{}, dialect string) string {
	values, hasNull := value, false
	if list, ok := value.([]interface{}); ok {
		var kept []interface{}
		for _, item := range list {
			if item == nil {
				hasNull = true
				continue
			}
			kept = append(kept, item)
		}
		if hasNull && len(kept) == 0 {
			if negate {
				return dim + " IS NOT NULL"
			}
			return dim + " IS NULL"
		}
		values = kept
	}

	op := "IN"
	if negate {
		op = "NOT IN"
	}
	predicate := fmt.Sprintf("%s %s (%s)", dim, op, formatList(values, dialect))
	switch {
	case negate && hasNull:
		return "(" + predicate + " AND " + dim + " IS NOT NULL)"
	case negate || hasNull:
		return "(" + predicate + " OR " + dim + " IS NULL)"
	}
	return predicate
}

func betweenValues(v interface{}) (interface{}, interface{}, bool) {
	switch val := v.(type) {
	case []interface{}:
		if len(val) != 2 {
			return nil, nil, false
		}
		return val[0], val[1], true
	case []string:
		if len(val) != 2 {
			return nil, nil, false
		}
		return val[0], val[1], true
	case map[string]interface{}:
		start, okStart := val["start"]
		end, okEnd := val["end"]
		return start, end, okStart && okEnd
	default:
		return nil, nil, false
	}
}

func formatValue(v interface{}, dialect ...string) string {
	switch val := v.(type) {
	case string:
		if len(dialect) > 0 && (dialect[0] == "bigquery" || dialect[0] == "google_cloud_platform") {
			// GoogleSQL accepts double-quoted strings and Go's escape sequences.
			return strconv.Quote(val)
		}
		if len(dialect) > 0 && dialect[0] != "" && backslashEscapes(dialect[0]) {
			val = strings.ReplaceAll(val, `\`, `\\`)
		}
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func formatList(v interface{}, dialect ...string) string {
	switch val := v.(type) {
	case []string:
		quoted := make([]string, len(val))
		for i, s := range val {
			quoted[i] = formatValue(s, dialect...)
		}
		return strings.Join(quoted, ", ")
	case []interface{}:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = formatValue(item, dialect...)
		}
		return strings.Join(parts, ", ")
	default:
		return formatValue(v, dialect...)
	}
}

func (e *Engine) needsWindowWrap(metricNames []string) bool {
	visited := map[string]bool{}
	var check func(string) bool
	check = func(name string) bool {
		if visited[name] {
			return false
		}
		visited[name] = true
		m := e.metrics[name]
		if m == nil {
			return false
		}
		if isWindow(m) {
			return true
		}
		for _, ref := range e.extractRefs(m.Expression) {
			if check(ref) {
				return true
			}
		}
		return false
	}
	for _, name := range metricNames {
		if check(name) {
			return true
		}
	}
	return false
}

func (e *Engine) collectInnerMetrics(metricNames []string) []string {
	include := map[string]bool{}
	visited := map[string]bool{}
	depsSeen := map[string]bool{}
	var collectDeps func(string)
	var collectForQuery func(string)

	collectDeps = func(name string) {
		if depsSeen[name] {
			return
		}
		depsSeen[name] = true
		m := e.metrics[name]
		if m == nil || isWindow(m) {
			return
		}
		if !e.isDerived(m) {
			include[name] = true
			return
		}
		for _, ref := range e.extractRefs(m.Expression) {
			collectDeps(ref)
		}
	}

	collectForQuery = func(name string) {
		if visited[name] {
			return
		}
		visited[name] = true

		m := e.metrics[name]
		if m == nil {
			return
		}
		if isWindow(m) {
			for _, ref := range e.extractRefs(m.Expression) {
				if refMetric := e.metrics[ref]; refMetric != nil && !isWindow(refMetric) {
					include[ref] = true
				}
				collectDeps(ref)
			}
			return
		}
		if !e.isDerived(m) {
			include[name] = true
			return
		}
		for _, ref := range e.extractRefs(m.Expression) {
			collectForQuery(ref)
		}
	}

	for _, name := range metricNames {
		collectForQuery(name)
	}

	var result []string
	for _, m := range e.model.Metrics {
		if include[m.Name] {
			result = append(result, m.Name)
		}
	}
	return result
}

func (e *Engine) collectInnerDimensions(q *Query) []DimensionRef {
	result := append([]DimensionRef(nil), q.Dimensions...)
	seen := make(map[string]bool, len(result))
	visited := map[string]bool{}
	for _, d := range result {
		seen[d.Name] = true
	}
	for _, sort := range q.Sort {
		if e.metrics[sort.Name] != nil || seen[sort.Name] {
			continue
		}
		result = append(result, DimensionRef{Name: sort.Name})
		seen[sort.Name] = true
	}

	var collect func(string)
	collect = func(name string) {
		if visited[name] {
			return
		}
		visited[name] = true

		m := e.metrics[name]
		if m == nil {
			return
		}
		if isWindow(m) {
			for _, p := range m.Window.PartitionBy {
				if !seen[p] {
					result = append(result, DimensionRef{Name: p})
					seen[p] = true
				}
			}
			if m.Window.OrderBy != "" && !seen[m.Window.OrderBy] {
				result = append(result, DimensionRef{Name: m.Window.OrderBy})
				seen[m.Window.OrderBy] = true
			}
		}
		for _, ref := range e.extractRefs(m.Expression) {
			collect(ref)
		}
	}

	for _, name := range q.Metrics {
		collect(name)
	}
	return result
}

func (e *Engine) validateMetricFiltersForWrapped(name string, visited map[string]bool) error {
	if visited[name] {
		return nil
	}
	visited[name] = true

	m := e.metrics[name]
	if m == nil || isWindow(m) {
		return nil
	}
	if m.Filter != "" {
		_, filterNeedsHaving, err := e.expandFilterExpr(m.Filter, nil)
		if err != nil {
			return err
		}
		if filterNeedsHaving {
			return fmt.Errorf("metric %q filter cannot reference aggregates in wrapped queries", name)
		}
	}
	for _, ref := range e.extractRefs(m.Expression) {
		if err := e.validateMetricFiltersForWrapped(ref, copyVisited(visited)); err != nil {
			return err
		}
	}
	return nil
}

func containsOperator(s string) bool {
	depth := 0
	inSingle := false
	inDouble := false
	for i := range len(s) {
		c := s[i]
		switch {
		case c == '\'' && !inDouble:
			inSingle = !inSingle
			continue
		case c == '"' && !inSingle:
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case '+', '-', '*', '/', '=', '<', '>', '!':
			if depth == 0 && i > 0 {
				return true
			}
		}
	}
	return false
}

func (e *Engine) isDerived(m *Metric) bool { return refPattern.MatchString(e.maskSQL(m.Expression)) }
func isWindow(m *Metric) bool              { return m.Window != nil }

func (e *Engine) extractRefs(expr string) []string {
	matches := refPattern.FindAllStringSubmatch(e.maskSQL(expr), -1)
	refs := make([]string, len(matches))
	for i, m := range matches {
		refs[i] = m[1]
	}
	return refs
}

func copyVisited(m map[string]bool) map[string]bool {
	c := make(map[string]bool, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}
