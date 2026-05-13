package semantic

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var refPattern = regexp.MustCompile(`\{([^}]+)\}`)

var aggregateFuncPattern = regexp.MustCompile(`(?i)\b(sum|count|avg|min|max|stddev|stddev_samp|stddev_pop|variance|var_samp|var_pop|median|percentile_cont|percentile_disc|array_agg|string_agg|listagg|approx_count_distinct|approx_quantile)\s*\(`)

func maskTemplateDelimiters(expr string) string {
	replacer := strings.NewReplacer("{{", "@@", "}}", "##")
	return replacer.Replace(expr)
}

// containsAggregateOutsideRefs reports whether expr contains an aggregate
// function call that is not inside a {ref} placeholder.
func containsAggregateOutsideRefs(expr string) bool {
	stripped := refPattern.ReplaceAllString(maskTemplateDelimiters(expr), "")
	return aggregateFuncPattern.MatchString(stripped)
}

// Engine generates SQL from a Model and a Query.
type Engine struct {
	model    *Model
	metrics  map[string]*Metric
	dims     map[string]*Dimension
	segments map[string]*Segment
}

func NewEngine(m *Model) (*Engine, error) {
	if m == nil {
		return nil, errors.New("model is required")
	}

	e := &Engine{
		model:    m,
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
	return e, nil
}

func (e *Engine) validate() error {
	if e.model == nil {
		return errors.New("model is required")
	}
	if e.model.Name == "" {
		return errors.New("model name is required")
	}
	if strings.TrimSpace(e.model.Source.Table) == "" {
		return errors.New("source.table is required")
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

	for _, m := range e.model.Metrics {
		if isDerived(&m) && !isWindow(&m) {
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

// isMixedExpression reports whether a metric expression contains both {refs}
// and a raw aggregation function call. Such metrics work in simple queries but
// cannot be hoisted into the inner subquery of a window-wrapped query.
func isMixedExpression(m *Metric) bool {
	return isDerived(m) && containsAggregateOutsideRefs(m.Expression)
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
		if name != rootName && isMixedExpression(m) {
			return name, true
		}
		for _, ref := range extractRefs(m.Expression) {
			if dep, ok := walk(ref); ok {
				return dep, true
			}
		}
		return "", false
	}
	return walk(rootName)
}

func (e *Engine) validateWindowMetric(m *Metric) error {
	refs := extractRefs(m.Expression)
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
	for _, ref := range extractRefs(m.Expression) {
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
	if err := e.validateQuery(q); err != nil {
		return "", err
	}
	if e.needsWindowWrap(q.Metrics) {
		return e.generateWrapped(q)
	}
	return e.generateSimple(q)
}

func (e *Engine) validateQuery(q *Query) error {
	if len(q.Dimensions) == 0 && len(q.Metrics) == 0 {
		return errors.New("query must include at least one dimension or metric")
	}
	for _, d := range q.Dimensions {
		dim := e.dims[d.Name]
		if dim == nil {
			return fmt.Errorf("dimension not found: %s", d.Name)
		}
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
		if e.dims[f.Dimension] == nil {
			return fmt.Errorf("filter dimension not found: %s", f.Dimension)
		}
		if err := validateStructuredFilter(f); err != nil {
			return err
		}
	}
	for _, sort := range q.Sort {
		if e.dims[sort.Name] == nil && e.metrics[sort.Name] == nil {
			return fmt.Errorf("sort field not found: %s", sort.Name)
		}
	}
	return nil
}

func (e *Engine) generateSimple(q *Query) (string, error) {
	var sel []string
	groupBy := make([]string, 0, len(q.Dimensions))

	for i, d := range q.Dimensions {
		expr := e.dimExpr(e.dims[d.Name], d.Granularity)
		sel = append(sel, expr+" AS "+d.Name)
		groupBy = append(groupBy, strconv.Itoa(i+1))
	}

	for _, name := range q.Metrics {
		expanded, err := e.expandSimple(name, map[string]bool{})
		if err != nil {
			return "", err
		}
		sel = append(sel, expanded+" AS "+name)
	}

	sql := "SELECT " + strings.Join(sel, ", ")
	sql += " FROM " + e.model.Source.Table

	where, having, err := e.buildWhereHaving(q)
	if err != nil {
		return "", err
	}
	if where != "" {
		sql += " WHERE " + where
	}
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if having != "" {
		sql += " HAVING " + having
	}
	sql += e.buildOrderAndLimit(q)
	return sql, nil
}

func (e *Engine) expandSimple(name string, visited map[string]bool) (string, error) {
	if visited[name] {
		return "", fmt.Errorf("circular dependency: %s", name)
	}
	visited[name] = true

	m := e.metrics[name]
	if m == nil {
		return "", fmt.Errorf("metric not found: %s", name)
	}

	if isWindow(m) {
		return "", fmt.Errorf("window metric %q in simple query path", name)
	}

	if !isDerived(m) {
		if m.Filter != "" {
			expandedFilter, _, err := e.expandFilterExpr(m.Filter)
			if err != nil {
				return "", err
			}
			return applyMetricFilter(m.Expression, expandedFilter), nil
		}
		return m.Expression, nil
	}

	return expandRefs(m.Expression, func(refName string) (string, error) {
		expanded, err := e.expandSimple(refName, copyVisited(visited))
		if err != nil {
			return "", err
		}
		if containsOperator(expanded) {
			return "(" + expanded + ")", nil
		}
		return expanded, nil
	})
}

func (e *Engine) generateWrapped(q *Query) (string, error) {
	innerMetrics := e.collectInnerMetrics(q.Metrics)
	innerDimensions := e.collectInnerDimensions(q)

	var innerSel []string
	groupBy := make([]string, 0, len(innerDimensions))
	for i, d := range innerDimensions {
		expr := e.dimExpr(e.dims[d.Name], d.Granularity)
		innerSel = append(innerSel, expr+" AS "+d.Name)
		groupBy = append(groupBy, strconv.Itoa(i+1))
	}
	for _, name := range innerMetrics {
		if err := e.validateMetricFiltersForWrapped(name, map[string]bool{}); err != nil {
			return "", err
		}
		expr, err := e.expandSimple(name, map[string]bool{})
		if err != nil {
			return "", err
		}
		innerSel = append(innerSel, expr+" AS "+name)
	}

	inner := "SELECT " + strings.Join(innerSel, ", ")
	inner += " FROM " + e.model.Source.Table

	where, having, err := e.buildWhereHaving(q)
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

	var outerSel []string
	for _, d := range q.Dimensions {
		outerSel = append(outerSel, "base."+d.Name)
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
	sql += e.buildOrderAndLimit(q)
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

	if !isDerived(m) && !isWindow(m) {
		return "base." + name, nil
	}

	if isWindow(m) {
		return e.windowSQL(m), nil
	}

	return expandRefs(m.Expression, func(refName string) (string, error) {
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
	refs := extractRefs(m.Expression)
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

func expandRefs(expr string, resolve func(string) (string, error)) (string, error) {
	masked := maskTemplateDelimiters(expr)
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

func applyMetricFilter(expr, filter string) string {
	expr = strings.TrimSpace(expr)
	parenIdx := strings.Index(expr, "(")
	if parenIdx == -1 {
		return expr
	}

	funcName := strings.TrimSpace(expr[:parenIdx])
	closeIdx := findMatchingParen(expr, parenIdx)
	if closeIdx == -1 {
		return expr
	}

	inner := expr[parenIdx+1 : closeIdx]
	innerTrim := strings.TrimSpace(inner)

	if innerTrim == "*" {
		return fmt.Sprintf("%s(CASE WHEN %s THEN 1 ELSE NULL END)", funcName, filter)
	}

	upperInner := strings.ToUpper(innerTrim)
	if strings.HasPrefix(upperInner, "DISTINCT ") {
		arg := strings.TrimSpace(innerTrim[len("DISTINCT "):])
		return fmt.Sprintf("%s(DISTINCT CASE WHEN %s THEN %s ELSE NULL END)", funcName, filter, arg)
	}

	return fmt.Sprintf("%s(CASE WHEN %s THEN %s ELSE NULL END)", funcName, filter, inner)
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

func (e *Engine) buildWhereHaving(q *Query) (where, having string, err error) {
	var whereParts, havingParts []string

	for _, f := range q.Filters {
		var raw string
		if f.Expression != "" {
			raw = f.Expression
		} else {
			raw = e.filterToSQL(f)
		}
		expanded, needsHaving, err := e.expandFilterExpr(raw)
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
		expanded, needsHaving, err := e.expandFilterExpr(e.segments[name].Filter)
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

func (e *Engine) expandFilterExpr(expr string) (string, bool, error) {
	masked := maskTemplateDelimiters(expr)
	hasAggregate := containsAggregateOutsideRefs(expr)
	if !refPattern.MatchString(masked) {
		return expr, hasAggregate, nil
	}

	hasMetricRef := false
	var expandErr error

	result := refPattern.ReplaceAllStringFunc(masked, func(match string) string {
		refName := match[1 : len(match)-1]

		if dim, ok := e.dims[refName]; ok {
			return e.dimExpr(dim, "")
		}

		if _, ok := e.metrics[refName]; ok {
			hasMetricRef = true
			expanded, err := e.expandSimple(refName, map[string]bool{})
			if err != nil {
				expandErr = err
				return match
			}
			return expanded
		}

		expandErr = fmt.Errorf("unknown reference {%s} in filter", refName)
		return match
	})

	result = strings.NewReplacer("@@", "{{", "##", "}}").Replace(result)
	return result, hasMetricRef || hasAggregate, expandErr
}

func (e *Engine) buildOrderAndLimit(q *Query) string {
	var s string
	if len(q.Sort) > 0 {
		parts := make([]string, 0, len(q.Sort))
		for _, sort := range q.Sort {
			dir := strings.ToUpper(sort.Direction)
			if dir == "" {
				dir = "ASC"
			}
			parts = append(parts, sort.Name+" "+dir)
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
	case "equals", "not_equals", "gt", "gte", "lt", "lte", "in", "not_in", "is_null", "is_not_null":
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

func (e *Engine) filterToSQL(f Filter) string {
	if f.Expression != "" {
		return f.Expression
	}
	dim := f.Dimension
	if d := e.dims[f.Dimension]; d != nil {
		dim = e.dimExpr(d, "")
	}
	switch f.Operator {
	case "equals":
		return fmt.Sprintf("%s = %s", dim, formatValue(f.Value))
	case "not_equals":
		return fmt.Sprintf("%s != %s", dim, formatValue(f.Value))
	case "gt":
		return fmt.Sprintf("%s > %s", dim, formatValue(f.Value))
	case "gte":
		return fmt.Sprintf("%s >= %s", dim, formatValue(f.Value))
	case "lt":
		return fmt.Sprintf("%s < %s", dim, formatValue(f.Value))
	case "lte":
		return fmt.Sprintf("%s <= %s", dim, formatValue(f.Value))
	case "in":
		return fmt.Sprintf("%s IN (%s)", dim, formatList(f.Value))
	case "not_in":
		return fmt.Sprintf("%s NOT IN (%s)", dim, formatList(f.Value))
	case "between":
		start, end, ok := betweenValues(f.Value)
		if !ok {
			return ""
		}
		return fmt.Sprintf("%s BETWEEN %s AND %s", dim, formatValue(start), formatValue(end))
	case "is_null":
		return dim + " IS NULL"
	case "is_not_null":
		return dim + " IS NOT NULL"
	default:
		return ""
	}
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

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
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

func formatList(v interface{}) string {
	switch val := v.(type) {
	case []string:
		quoted := make([]string, len(val))
		for i, s := range val {
			quoted[i] = "'" + strings.ReplaceAll(s, "'", "''") + "'"
		}
		return strings.Join(quoted, ", ")
	case []interface{}:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = formatValue(item)
		}
		return strings.Join(parts, ", ")
	default:
		return formatValue(v)
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
		for _, ref := range extractRefs(m.Expression) {
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
	var collectDeps func(string)
	var collectForQuery func(string)

	collectDeps = func(name string) {
		m := e.metrics[name]
		if m == nil || isWindow(m) {
			return
		}
		if !isDerived(m) {
			include[name] = true
			return
		}
		for _, ref := range extractRefs(m.Expression) {
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
			for _, ref := range extractRefs(m.Expression) {
				if refMetric := e.metrics[ref]; refMetric != nil && !isWindow(refMetric) {
					include[ref] = true
				}
				collectDeps(ref)
			}
			return
		}
		if !isDerived(m) {
			include[name] = true
			return
		}
		for _, ref := range extractRefs(m.Expression) {
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
		for _, ref := range extractRefs(m.Expression) {
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
		_, filterNeedsHaving, err := e.expandFilterExpr(m.Filter)
		if err != nil {
			return err
		}
		if filterNeedsHaving {
			return fmt.Errorf("metric %q filter cannot reference aggregates in wrapped queries", name)
		}
	}
	for _, ref := range extractRefs(m.Expression) {
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

func isDerived(m *Metric) bool { return refPattern.MatchString(maskTemplateDelimiters(m.Expression)) }
func isWindow(m *Metric) bool  { return m.Window != nil }

func extractRefs(expr string) []string {
	matches := refPattern.FindAllStringSubmatch(maskTemplateDelimiters(expr), -1)
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
