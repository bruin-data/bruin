package semantic

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Check is a built-in quality check attached to a dimension or a metric.
//
// Dimension checks run against the model source row by row and mirror Bruin's
// column checks: not_null, unique, positive, non_negative, negative, min, max,
// accepted_values, and pattern.
//
// Metric checks evaluate the metric over the whole model (no grouping) and
// assert on the single aggregated value: not_null, positive, non_negative,
// negative, min, max, and equals.
type Check struct {
	Name        string      `yaml:"name" json:"name"`
	Description string      `yaml:"description,omitempty" json:"description,omitempty"`
	Value       interface{} `yaml:"value,omitempty" json:"value,omitempty"`
}

// ModelCheck is a custom, model-level quality check expressed as a semantic
// query in YAML. The query is compiled through the engine like any other
// semantic query, so it can use dimensions, metrics, filters, segments, sort,
// limit, and joined dimensions.
//
// When Count is set the query is wrapped in SELECT count(*) and the row count
// is compared to Count. Otherwise the result is compared to Value:
//   - a scalar or boolean expects a single row with a single column;
//   - a mapping expects exactly one row whose listed columns match;
//   - a list expects the full result set, one entry per row, where each entry
//     is a mapping keyed by dimension or metric name, or a scalar when the
//     query has a single column.
//
// Rows are compared in order when the query has a sort, and as a multiset
// otherwise. A missing Value defaults to integer zero, like Bruin custom checks;
// an explicit `value: null` expects a single NULL cell.
type ModelCheck struct {
	Name        string      `yaml:"name" json:"name"`
	Description string      `yaml:"description,omitempty" json:"description,omitempty"`
	Query       *Query      `yaml:"query" json:"query"`
	Value       interface{} `yaml:"value,omitempty" json:"value,omitempty"`
	Count       *int64      `yaml:"count,omitempty" json:"count,omitempty"`

	// valueNull records an explicit `value: null`, which decodes to the same
	// nil Value as an omitted key.
	valueNull bool
}

// CheckScope identifies what a compiled check is attached to.
type CheckScope string

const (
	CheckScopeModel     CheckScope = "model"
	CheckScopeDimension CheckScope = "dimension"
	CheckScopeMetric    CheckScope = "metric"
)

const (
	checkNotNull        = "not_null"
	checkUnique         = "unique"
	checkPositive       = "positive"
	checkNonNegative    = "non_negative"
	checkNegative       = "negative"
	checkMin            = "min"
	checkMax            = "max"
	checkEquals         = "equals"
	checkAcceptedValues = "accepted_values"
	checkPattern        = "pattern"
)

// checkValueColumn names the single output column of the scalar check and
// validation queries. Platforms such as SQL Server reject a derived table with
// an unnamed column, and the validation fallback wraps these queries in one.
const checkValueColumn = "bruin_check_value"

var dimensionCheckNames = map[string]bool{
	checkNotNull: true, checkUnique: true, checkPositive: true, checkNonNegative: true, checkNegative: true,
	checkMin: true, checkMax: true, checkAcceptedValues: true, checkPattern: true,
}

var metricCheckNames = map[string]bool{
	checkNotNull: true, checkPositive: true, checkNonNegative: true, checkNegative: true,
	checkMin: true, checkMax: true, checkEquals: true,
}

// CompiledCheck is a quality check turned into SQL plus an evaluation rule.
// Run SQL against the model's connection and pass the rows to Evaluate.
type CompiledCheck struct {
	Model       string     `json:"model"`
	Scope       CheckScope `json:"scope"`
	Target      string     `json:"target,omitempty"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	SQL         string     `json:"sql"`
	// ValidationSQL is an equivalent form of SQL that survives being nested in
	// a derived table, which validation falls back to on platforms without a
	// dry run. It is empty when SQL itself can be nested as is.
	ValidationSQL string `json:"-"`

	evaluate func(rows [][]interface{}) error
}

// ID returns a stable, human-readable identifier such as
// "orders.metric.revenue.positive" or "orders.check.no_negative_amounts".
func (c *CompiledCheck) ID() string {
	switch c.Scope {
	case CheckScopeDimension, CheckScopeMetric:
		return c.Model + "." + string(c.Scope) + "." + c.Target + "." + c.Name
	case CheckScopeModel:
		return c.Model + ".check." + c.Name
	default:
		return c.Model + "." + c.Name
	}
}

// Evaluate inspects the rows returned by SQL and returns nil when the check
// passes, or an error describing the failure.
func (c *CompiledCheck) Evaluate(rows [][]interface{}) error {
	if c.evaluate == nil {
		return errors.New("check has no evaluator")
	}
	return c.evaluate(rows)
}

// CompileChecksOptions tunes SQL generation for checks.
type CompileChecksOptions struct {
	// Dialect selects dialect-specific SQL where ANSI SQL is not enough (the
	// pattern check). Values follow the Bruin SQL parser dialect names, e.g.
	// "bigquery", "snowflake", "postgres", "duckdb". Unknown or empty dialects
	// fall back to REGEXP_LIKE.
	Dialect string
}

// CountChecks returns the number of quality checks defined on a model.
func CountChecks(m *Model) int {
	if m == nil {
		return 0
	}
	count := len(m.Checks)
	for _, d := range m.Dimensions {
		count += len(d.Checks)
	}
	for _, metric := range m.Metrics {
		count += len(metric.Checks)
	}
	return count
}

// CompileChecks compiles every quality check of every model in the catalog,
// ordered by model name. Each model is compiled with the full catalog so that
// custom check queries can use joined dimensions.
func CompileChecks(models map[string]*Model, opts CompileChecksOptions) ([]CompiledCheck, error) {
	var compiled []CompiledCheck
	for _, name := range Names(models) {
		engine, err := NewEngineWithModels(models[name], models)
		if err != nil {
			return nil, fmt.Errorf("model %q: %w", name, err)
		}
		checks, err := engine.CompileChecks(opts)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, checks...)
	}
	return compiled, nil
}

// CompileChecks compiles the quality checks of the engine's model in
// definition order: dimension checks, metric checks, then model checks.
func (e *Engine) CompileChecks(opts CompileChecksOptions) ([]CompiledCheck, error) {
	m := e.model
	compiled := make([]CompiledCheck, 0, CountChecks(m))

	for i := range m.Dimensions {
		d := &m.Dimensions[i]
		for _, check := range d.Checks {
			cc, err := e.compileDimensionCheck(d, check, opts)
			if err != nil {
				return nil, fmt.Errorf("model %q: dimension %q: check %q: %w", m.Name, d.Name, check.Name, err)
			}
			compiled = append(compiled, cc)
		}
	}

	for i := range m.Metrics {
		metric := &m.Metrics[i]
		for _, check := range metric.Checks {
			cc, err := e.compileMetricCheck(metric, check)
			if err != nil {
				return nil, fmt.Errorf("model %q: metric %q: check %q: %w", m.Name, metric.Name, check.Name, err)
			}
			compiled = append(compiled, cc)
		}
	}

	for _, check := range m.Checks {
		cc, err := e.compileModelCheck(check)
		if err != nil {
			return nil, fmt.Errorf("model %q: check %q: %w", m.Name, check.Name, err)
		}
		compiled = append(compiled, cc)
	}

	return compiled, nil
}

// ValidationQueries returns independent queries covering every dimension,
// granularity, metric, join, and segment. Metrics are compiled independently so
// unrelated window metrics cannot change the SQL required by another metric.
func (e *Engine) ValidationQueries() ([]string, error) {
	var queries []*Query
	// Two join paths can reach a model through joins with the same name; the
	// qualified dimension names then collide and would repeat a column alias.
	seen := map[string]bool{}
	addDimension := func(name string, d Dimension) {
		if seen[name] {
			return
		}
		seen[name] = true
		queries = append(queries, &Query{Dimensions: []DimensionRef{{Name: name}}})
		granularities := make([]string, 0, len(d.Granularities))
		for granularity := range d.Granularities {
			granularities = append(granularities, granularity)
		}
		sort.Strings(granularities)
		for _, granularity := range granularities {
			queries = append(queries, &Query{Dimensions: []DimensionRef{{Name: name, Granularity: granularity}}})
		}
	}
	for _, d := range e.model.Dimensions {
		addDimension(d.Name, d)
	}
	for _, relation := range e.reachableRelations() {
		for _, d := range relation.model.Dimensions {
			addDimension(relation.relationName+"."+d.Name, d)
		}
	}
	for _, m := range e.model.Metrics {
		queries = append(queries, &Query{Metrics: []string{m.Name}})
	}
	var statements []string
	var dimensions []DimensionRef
	for _, q := range queries {
		if len(q.Dimensions) == 1 && q.Dimensions[0].Granularity == "" {
			dimensions = append(dimensions, q.Dimensions[0])
		}
		sql, err := e.GenerateSQL(q)
		if err != nil {
			return nil, err
		}
		statements = append(statements, sql)
	}
	for _, segment := range e.model.Segments {
		q := &Query{Dimensions: dimensions, Segments: []string{segment.Name}}
		if len(dimensions) > 0 {
			sql, err := e.GenerateSQL(q)
			if err != nil {
				return nil, fmt.Errorf("segment %q: %w", segment.Name, err)
			}
			statements = append(statements, sql)
			continue
		}
		plan, err := e.planQuery(q)
		if err != nil {
			return nil, fmt.Errorf("segment %q: %w", segment.Name, err)
		}
		where, having, err := e.buildWhereHaving(q, plan)
		if err != nil {
			return nil, fmt.Errorf("segment %q: %w", segment.Name, err)
		}
		sql := "SELECT count(*) AS " + checkValueColumn + plan.fromSQL()
		if where != "" {
			sql += " WHERE " + where
		}
		if having != "" {
			sql += " HAVING " + having
		}
		statements = append(statements, sql)
	}
	if len(statements) == 0 {
		statements = append(statements, "SELECT 1 AS "+checkValueColumn+" FROM "+e.model.Source.Table)
	}
	return statements, nil
}

// --- structural validation ---

func (e *Engine) validateChecks() error {
	m := e.model
	for _, d := range m.Dimensions {
		seen := make(map[string]bool, len(d.Checks))
		for _, check := range d.Checks {
			if err := validateBuiltinCheck(check, dimensionCheckNames, seen); err != nil {
				return fmt.Errorf("dimension %q: %w", d.Name, err)
			}
		}
	}
	for i := range m.Metrics {
		metric := &m.Metrics[i]
		// Window metrics, and metrics derived from one, produce a row per
		// order_by group, so there is no single value for a check to assert on.
		if len(metric.Checks) > 0 && e.needsWindowWrap([]string{metric.Name}) {
			return fmt.Errorf("metric %q: checks are not supported on window metrics", metric.Name)
		}
		seen := make(map[string]bool, len(metric.Checks))
		for _, check := range metric.Checks {
			if err := validateBuiltinCheck(check, metricCheckNames, seen); err != nil {
				return fmt.Errorf("metric %q: %w", metric.Name, err)
			}
		}
	}
	seen := make(map[string]bool, len(m.Checks))
	for _, check := range m.Checks {
		if err := validateModelCheck(check, seen); err != nil {
			return err
		}
	}
	return nil
}

func validateBuiltinCheck(check Check, allowed map[string]bool, seen map[string]bool) error {
	name := strings.TrimSpace(check.Name)
	if name == "" {
		return errors.New("check name is required")
	}
	if !allowed[name] {
		return fmt.Errorf("unknown check %q; supported checks: %s", name, strings.Join(sortedKeys(allowed), ", "))
	}
	if seen[name] {
		return fmt.Errorf("duplicate check %q", name)
	}
	seen[name] = true

	switch name {
	case checkMin, checkMax, checkEquals:
		if check.Value == nil {
			return fmt.Errorf("check %q requires a value", name)
		}
		if !isScalar(check.Value) && (name != checkEquals || !isBool(check.Value)) {
			return fmt.Errorf("check %q value must be a number or a string", name)
		}
	case checkAcceptedValues:
		values, ok := check.Value.([]interface{})
		if !ok || len(values) == 0 {
			return fmt.Errorf("check %q requires a non-empty list of values", name)
		}
		for _, v := range values {
			if !isScalar(v) {
				return fmt.Errorf("check %q values must be numbers or strings", name)
			}
		}
	case checkPattern:
		pattern, ok := check.Value.(string)
		if !ok || strings.TrimSpace(pattern) == "" {
			return fmt.Errorf("check %q requires a string pattern value", name)
		}
	default:
		if check.Value != nil {
			return fmt.Errorf("check %q does not take a value", name)
		}
	}
	return nil
}

func validateModelCheck(check ModelCheck, seen map[string]bool) error {
	name := strings.TrimSpace(check.Name)
	if name == "" {
		return errors.New("model check name is required")
	}
	if seen[name] {
		return fmt.Errorf("duplicate model check %q", name)
	}
	seen[name] = true

	if check.Query == nil {
		return fmt.Errorf("model check %q requires a query", name)
	}
	if check.Count != nil && (check.Value != nil || check.valueNull) {
		return fmt.Errorf("model check %q cannot set both value and count", name)
	}
	if check.Count != nil && *check.Count < 0 {
		return fmt.Errorf("model check %q count must not be negative", name)
	}
	if err := validateModelCheckValue(check.Value); err != nil {
		return fmt.Errorf("model check %q: %w", name, err)
	}
	return nil
}

func validateModelCheckValue(value interface{}) error {
	switch v := value.(type) {
	case nil:
		return nil
	case map[string]interface{}:
		return validateExpectedRow(v)
	case []interface{}:
		for i, item := range v {
			switch row := item.(type) {
			case nil:
				continue
			case map[string]interface{}:
				if err := validateExpectedRow(row); err != nil {
					return fmt.Errorf("value[%d]: %w", i, err)
				}
			default:
				if !isScalar(item) && !isBool(item) {
					return fmt.Errorf("value[%d] must be a number, string, boolean, null, or a mapping of column names to values", i)
				}
			}
		}
		return nil
	default:
		if !isScalar(value) && !isBool(value) {
			return errors.New("value must be a number, string, boolean, a mapping of column names to values, or a list of rows")
		}
		return nil
	}
}

func validateExpectedRow(row map[string]interface{}) error {
	if len(row) == 0 {
		return errors.New("expected row must list at least one column")
	}
	for column, cell := range row {
		if cell != nil && !isScalar(cell) && !isBool(cell) {
			return fmt.Errorf("column %q must be a number, string, boolean, or null", column)
		}
	}
	return nil
}

// --- compilation ---

func (e *Engine) compileDimensionCheck(d *Dimension, check Check, opts CompileChecksOptions) (CompiledCheck, error) {
	expr := e.dimExpr(d, "")
	if d.Expression != "" {
		expr = "(" + expr + ")"
	}
	table := e.model.Source.Table
	name := strings.TrimSpace(check.Name)

	var sql string
	var failure func(count int64) string
	switch name {
	case checkNotNull:
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s IS NULL", table, expr)
		failure = func(count int64) string { return fmt.Sprintf("dimension '%s' has %d null values", d.Name, count) }
	case checkUnique:
		sql = fmt.Sprintf("SELECT count(%s) - count(DISTINCT %s) AS "+checkValueColumn+" FROM %s", expr, expr, table)
		failure = func(count int64) string { return fmt.Sprintf("dimension '%s' has %d non-unique values", d.Name, count) }
	case checkPositive:
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s <= 0", table, expr)
		failure = func(count int64) string {
			return fmt.Sprintf("dimension '%s' has %d non-positive values", d.Name, count)
		}
	case checkNonNegative:
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s < 0", table, expr)
		failure = func(count int64) string { return fmt.Sprintf("dimension '%s' has %d negative values", d.Name, count) }
	case checkNegative:
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s >= 0", table, expr)
		failure = func(count int64) string {
			return fmt.Sprintf("dimension '%s' has %d non-negative values", d.Name, count)
		}
	case checkMin:
		threshold := formatValue(check.Value)
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s < %s", table, expr, threshold)
		failure = func(count int64) string {
			return fmt.Sprintf("dimension '%s' has %d values below minimum %s", d.Name, count, threshold)
		}
	case checkMax:
		threshold := formatValue(check.Value)
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s > %s", table, expr, threshold)
		failure = func(count int64) string {
			return fmt.Sprintf("dimension '%s' has %d values above maximum %s", d.Name, count, threshold)
		}
	case checkAcceptedValues:
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s NOT IN (%s)", table, expr, formatList(check.Value))
		failure = func(count int64) string {
			return fmt.Sprintf("dimension '%s' has %d values that are not in the accepted values", d.Name, count)
		}
	case checkPattern:
		pattern, _ := check.Value.(string)
		sql = fmt.Sprintf("SELECT count(*) AS "+checkValueColumn+" FROM %s WHERE %s", table, patternMismatchSQL(expr, pattern, opts.Dialect))
		failure = func(count int64) string {
			return fmt.Sprintf("dimension '%s' has %d values that do not match the pattern", d.Name, count)
		}
	default:
		return CompiledCheck{}, fmt.Errorf("unknown dimension check %q", name)
	}

	return CompiledCheck{
		Model:       e.model.Name,
		Scope:       CheckScopeDimension,
		Target:      d.Name,
		Name:        name,
		Description: check.Description,
		SQL:         sql,
		evaluate:    expectZeroCount(failure),
	}, nil
}

func (e *Engine) compileMetricCheck(metric *Metric, check Check) (CompiledCheck, error) {
	sql, err := e.GenerateSQL(&Query{Metrics: []string{metric.Name}})
	if err != nil {
		return CompiledCheck{}, err
	}

	name := strings.TrimSpace(check.Name)
	var evaluate func(value interface{}) error
	switch name {
	case checkNotNull:
		evaluate = func(value interface{}) error {
			if value == nil {
				return fmt.Errorf("metric '%s' is NULL", metric.Name)
			}
			return nil
		}
	case checkPositive:
		evaluate = numericMetricCondition(metric.Name, "positive", func(v float64) bool { return v > 0 })
	case checkNonNegative:
		evaluate = numericMetricCondition(metric.Name, "non-negative", func(v float64) bool { return v >= 0 })
	case checkNegative:
		evaluate = numericMetricCondition(metric.Name, "negative", func(v float64) bool { return v < 0 })
	case checkMin:
		expected := check.Value
		evaluate = func(value interface{}) error {
			cmp, err := compareOrdered(value, expected)
			if err != nil {
				return fmt.Errorf("metric '%s': %w", metric.Name, err)
			}
			if cmp < 0 {
				return fmt.Errorf("metric '%s' is %s, below minimum %s", metric.Name, displayValue(value), displayValue(expected))
			}
			return nil
		}
	case checkMax:
		expected := check.Value
		evaluate = func(value interface{}) error {
			cmp, err := compareOrdered(value, expected)
			if err != nil {
				return fmt.Errorf("metric '%s': %w", metric.Name, err)
			}
			if cmp > 0 {
				return fmt.Errorf("metric '%s' is %s, above maximum %s", metric.Name, displayValue(value), displayValue(expected))
			}
			return nil
		}
	case checkEquals:
		expected := check.Value
		evaluate = func(value interface{}) error {
			if !valuesEqual(expected, value) {
				return fmt.Errorf("metric '%s' is %s, expected %s", metric.Name, displayValue(value), displayValue(expected))
			}
			return nil
		}
	default:
		return CompiledCheck{}, fmt.Errorf("unknown metric check %q", name)
	}

	return CompiledCheck{
		Model:       e.model.Name,
		Scope:       CheckScopeMetric,
		Target:      metric.Name,
		Name:        name,
		Description: check.Description,
		SQL:         sql,
		evaluate: func(rows [][]interface{}) error {
			value, err := singleCell(rows)
			if err != nil {
				return err
			}
			return evaluate(value)
		},
	}, nil
}

func (e *Engine) compileModelCheck(check ModelCheck) (CompiledCheck, error) {
	if check.Query == nil {
		return CompiledCheck{}, errors.New("requires a query")
	}
	sql, columns, err := e.GenerateSQLWithColumns(check.Query)
	if err != nil {
		return CompiledCheck{}, err
	}

	// SQL Server and its relatives reject ORDER BY inside a derived table, and
	// both the count wrapper below and the validation probe wrap the query in
	// one. Dropping the sort avoids that, and normally changes neither the rows
	// returned nor whether the query is valid.
	unsorted := sql
	if len(check.Query.Sort) > 0 && !e.sortChangesGrouping(check.Query) {
		withoutSort := *check.Query
		withoutSort.Sort = nil
		if unsorted, err = e.GenerateSQL(&withoutSort); err != nil {
			return CompiledCheck{}, err
		}
	}

	compiled := CompiledCheck{
		Model:       e.model.Name,
		Scope:       CheckScopeModel,
		Name:        strings.TrimSpace(check.Name),
		Description: check.Description,
	}

	if check.Count != nil {
		expected := *check.Count
		compiled.SQL = "SELECT count(*) AS " + checkValueColumn + " FROM (" + unsorted + ") bruin_check"
		compiled.evaluate = func(rows [][]interface{}) error {
			value, err := singleCell(rows)
			if err != nil {
				return err
			}
			actual, err := toNumber(value)
			if err != nil {
				return fmt.Errorf("check result is not a row count: %w", err)
			}
			if actual.Cmp(new(big.Rat).SetInt64(expected)) != 0 {
				return fmt.Errorf("check returned %s rows, expected %d", displayValue(value), expected)
			}
			return nil
		}
		return compiled, nil
	}

	compiled.SQL = sql
	if unsorted != sql {
		compiled.ValidationSQL = unsorted
	}
	switch expected := check.Value.(type) {
	case []interface{}:
		evaluate, err := expectRows(columns, expected, len(check.Query.Sort) > 0)
		if err != nil {
			return CompiledCheck{}, err
		}
		compiled.evaluate = evaluate
	case map[string]interface{}:
		evaluate, err := expectRows(columns, []interface{}{expected}, true)
		if err != nil {
			return CompiledCheck{}, err
		}
		compiled.evaluate = evaluate
	default:
		if len(columns) != 1 {
			return CompiledCheck{}, fmt.Errorf("a scalar value needs a single-column query, got %d columns; use a mapping keyed by column name", len(columns))
		}
		scalar := expected
		if scalar == nil && !check.valueNull {
			scalar = 0
		}
		compiled.evaluate = func(rows [][]interface{}) error {
			value, err := singleCell(rows)
			if err != nil {
				return err
			}
			if !valuesEqual(scalar, value) {
				return fmt.Errorf("check returned %s, expected %s", displayValue(value), displayValue(scalar))
			}
			return nil
		}
	}
	return compiled, nil
}

// sortChangesGrouping reports whether dropping a query's sort would change the
// rows it returns rather than just their order. A window-wrapped query promotes
// each sort entry that is not a metric and not already selected into the inner
// GROUP BY (see collectInnerDimensions), so there the sort is part of the
// grouping.
func (e *Engine) sortChangesGrouping(q *Query) bool {
	if len(q.Sort) == 0 || !e.needsWindowWrap(q.Metrics) {
		return false
	}
	selected := make(map[string]bool, len(q.Dimensions))
	for _, d := range q.Dimensions {
		selected[d.Name] = true
	}
	for _, s := range q.Sort {
		if e.metrics[s.Name] == nil && !selected[s.Name] {
			return true
		}
	}
	return false
}

// expectedCell is one column expectation inside an expected row.
type expectedCell struct {
	column string
	index  int
	value  interface{}
}

// expectRows builds an evaluator that compares the full result set against
// the expected rows. Each expected row is either a mapping keyed by dimension
// or metric name (the query field name or the SQL output name) or, for
// single-column queries, a bare value. Only the listed columns are compared.
func expectRows(columns []QueryColumn, expected []interface{}, ordered bool) (func(rows [][]interface{}) error, error) {
	indexes := make(map[string]int, 2*len(columns))
	names := make([]string, 0, len(columns))
	for i, col := range columns {
		indexes[col.Field] = i
		indexes[col.Name] = i
		names = append(names, col.Field)
	}

	expectedRows := make([][]expectedCell, 0, len(expected))
	for i, item := range expected {
		row, ok := item.(map[string]interface{})
		if !ok {
			if len(columns) != 1 {
				return nil, fmt.Errorf("value[%d] is a bare value but the query returns %d columns; use a mapping keyed by column name", i, len(columns))
			}
			expectedRows = append(expectedRows, []expectedCell{{column: columns[0].Field, index: 0, value: item}})
			continue
		}
		cells := make([]expectedCell, 0, len(row))
		for _, column := range sortedStringKeys(row) {
			index, ok := indexes[column]
			if !ok {
				return nil, fmt.Errorf("value[%d] references unknown column %q; query columns: %s", i, column, strings.Join(names, ", "))
			}
			cells = append(cells, expectedCell{column: column, index: index, value: row[column]})
		}
		expectedRows = append(expectedRows, cells)
	}

	return func(rows [][]interface{}) error {
		if len(rows) != len(expectedRows) {
			return fmt.Errorf("check returned %d rows, expected %d", len(rows), len(expectedRows))
		}
		if ordered {
			for i, cells := range expectedRows {
				if mismatch := rowMismatch(rows[i], cells); mismatch != "" {
					return fmt.Errorf("row %d: %s", i+1, mismatch)
				}
			}
			return nil
		}

		if i, ok := matchRows(rows, expectedRows); !ok {
			return fmt.Errorf("the expected rows cannot all be matched to distinct returned rows; no unused row is left for expected row %d (%s)", i+1, displayExpectedRow(expectedRows[i]))
		}
		return nil
	}, nil
}

// matchRows pairs every expected row with a distinct returned row. Expected
// rows may list only some of the columns, so a less specific expectation can
// match a row that a more specific one also needs; a greedy assignment would
// then fail depending on the order the warehouse returned the rows in. The
// augmenting path search below finds a one-to-one assignment whenever one
// exists. It returns the index of the first expected row that cannot be
// matched, if any.
func matchRows(rows [][]interface{}, expectedRows [][]expectedCell) (int, bool) {
	assignedTo := make([]int, len(rows))
	for j := range assignedTo {
		assignedTo[j] = -1
	}
	var assign func(i int, visited []bool) bool
	assign = func(i int, visited []bool) bool {
		for j, row := range rows {
			if visited[j] || rowMismatch(row, expectedRows[i]) != "" {
				continue
			}
			visited[j] = true
			if assignedTo[j] == -1 || assign(assignedTo[j], visited) {
				assignedTo[j] = i
				return true
			}
		}
		return false
	}
	for i := range expectedRows {
		if !assign(i, make([]bool, len(rows))) {
			return i, false
		}
	}
	return 0, true
}

func rowMismatch(row []interface{}, cells []expectedCell) string {
	for _, cell := range cells {
		if cell.index >= len(row) {
			return fmt.Sprintf("column '%s' is missing from the result", cell.column)
		}
		actual := normalizeCell(row[cell.index])
		if !valuesEqual(cell.value, actual) {
			return fmt.Sprintf("column '%s' is %s, expected %s", cell.column, displayValue(actual), displayValue(cell.value))
		}
	}
	return ""
}

// normalizeCell unwraps the driver.Valuer wrappers some drivers return, such as
// pgx's pgtype.Numeric for Postgres NUMERIC columns, so that comparisons see
// the underlying value instead of the wrapper struct. A wrapper that fails to
// decode is left as is and simply compares unequal.
func normalizeCell(value interface{}) interface{} {
	valuer, ok := value.(driver.Valuer)
	if !ok {
		return value
	}
	normalized, err := valuer.Value()
	if err != nil {
		return value
	}
	return normalized
}

func displayExpectedRow(cells []expectedCell) string {
	parts := make([]string, 0, len(cells))
	for _, cell := range cells {
		parts = append(parts, cell.column+": "+displayValue(cell.value))
	}
	return strings.Join(parts, ", ")
}

func sortedStringKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func patternMismatchSQL(expr, pattern, dialect string) string {
	escaped := strings.ReplaceAll(pattern, "'", "''")
	switch dialect {
	case "bigquery":
		return fmt.Sprintf("NOT REGEXP_CONTAINS(%s, r'%s')", expr, escaped)
	case "duckdb", "postgres", "redshift":
		return fmt.Sprintf("%s !~ '%s'", expr, escaped)
	case "snowflake":
		return fmt.Sprintf("%s NOT REGEXP '%s'", expr, escaped)
	case "mysql", "doris", "starrocks":
		return fmt.Sprintf("NOT (%s REGEXP '%s')", expr, escaped)
	case "clickhouse":
		return fmt.Sprintf("NOT match(%s, '%s')", expr, escaped)
	case "spark", "databricks":
		return fmt.Sprintf("NOT (%s RLIKE '%s')", expr, escaped)
	case "tsql", "fabric":
		return fmt.Sprintf("%s NOT LIKE '%s'", expr, escaped)
	default:
		return fmt.Sprintf("NOT REGEXP_LIKE(%s, '%s')", expr, escaped)
	}
}

// --- evaluation helpers ---

func expectZeroCount(failure func(count int64) string) func(rows [][]interface{}) error {
	return func(rows [][]interface{}) error {
		value, err := singleCell(rows)
		if err != nil {
			return err
		}
		count, err := toFloat(value)
		if err != nil {
			return fmt.Errorf("check result is not a count: %w", err)
		}
		if count != 0 {
			return errors.New(failure(int64(count)))
		}
		return nil
	}
}

func numericMetricCondition(metricName, description string, ok func(v float64) bool) func(value interface{}) error {
	return func(value interface{}) error {
		if value == nil {
			return fmt.Errorf("metric '%s' is NULL, expected a %s value", metricName, description)
		}
		v, err := toFloat(value)
		if err != nil {
			return fmt.Errorf("metric '%s': %w", metricName, err)
		}
		if !ok(v) {
			return fmt.Errorf("metric '%s' is %s, expected a %s value", metricName, displayValue(value), description)
		}
		return nil
	}
}

func singleCell(rows [][]interface{}) (interface{}, error) {
	if len(rows) != 1 || len(rows[0]) != 1 {
		columns := 0
		if len(rows) > 0 {
			columns = len(rows[0])
		}
		return nil, fmt.Errorf("check query must return exactly one row with one column, got %d rows with %d columns", len(rows), columns)
	}
	value := rows[0][0]
	if valuer, ok := value.(driver.Valuer); ok {
		normalized, err := valuer.Value()
		if err != nil {
			return nil, fmt.Errorf("cannot decode check result: %w", err)
		}
		return normalized, nil
	}
	return value, nil
}

// valuesEqual compares an expected value from YAML/JSON with a value returned
// by a database driver. Numbers compare numerically, booleans compare by
// truthiness, and everything else compares by string form.
func valuesEqual(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == nil && actual == nil
	}
	if b, ok := expected.(bool); ok {
		actualBool, err := toBool(actual)
		return err == nil && actualBool == b
	}
	if cmp, ok := compareTemporal(actual, expected); ok {
		return cmp == 0
	}
	if expectedNum, err := toNumber(expected); err == nil {
		actualNum, err := toNumber(actual)
		if err != nil {
			return false
		}
		return withinFloatTolerance(expectedNum, actualNum, actual) || expectedNum.Cmp(actualNum) == 0
	}
	expectedStr := fmt.Sprint(expected)
	for _, candidate := range stringForms(actual) {
		if candidate == expectedStr {
			return true
		}
	}
	return false
}

// compareOrdered returns -1, 0, or 1 comparing actual against expected,
// numerically when both parse as numbers and lexicographically otherwise.
func compareOrdered(actual, expected interface{}) (int, error) {
	if actual == nil {
		return 0, errors.New("value is NULL")
	}
	if cmp, ok := compareTemporal(actual, expected); ok {
		return cmp, nil
	}
	expectedNum, expectedErr := toNumber(expected)
	if expectedErr == nil {
		actualNum, err := toNumber(actual)
		if err != nil {
			return 0, err
		}
		if withinFloatTolerance(expectedNum, actualNum, actual) {
			return 0, nil
		}
		return actualNum.Cmp(expectedNum), nil
	}
	return strings.Compare(stringForms(actual)[0], fmt.Sprint(expected)), nil
}

// compareTemporal accepts the date and timestamp forms returned by database
// drivers. Date-only expectations compare calendar dates; timestamps compare
// instants, including their fractional seconds and time zones.
func compareTemporal(actual, expected interface{}) (int, bool) {
	expectedText, ok := expected.(string)
	if !ok {
		return 0, false
	}
	parse := func(text string) (time.Time, bool) {
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
			if value, err := time.Parse(layout, text); err == nil {
				return value, true
			}
		}
		return time.Time{}, false
	}
	expectedTime, ok := parse(expectedText)
	if !ok {
		return 0, false
	}
	actualTime, ok := actual.(time.Time)
	if !ok {
		actualTime, ok = parse(stringForms(actual)[0])
		if !ok {
			return 0, false
		}
	}
	if len(expectedText) == len("2006-01-02") {
		return strings.Compare(actualTime.Format("2006-01-02"), expectedText), true
	}
	return actualTime.Compare(expectedTime), true
}

// withinFloatTolerance reports whether a float driver value is close enough to
// the expected number to count as equal. Float results carry summation-order
// rounding, so they get a relative tolerance sized to their precision, with an
// absolute floor for values near zero. Exact driver types never match here.
func withinFloatTolerance(expected, actual *big.Rat, raw interface{}) bool {
	var relative float64
	switch raw.(type) {
	case float64:
		relative = 1e-12
	case float32:
		relative = 1e-6
	default:
		return false
	}
	a, _ := expected.Float64()
	b, _ := actual.Float64()
	return math.Abs(a-b) <= math.Max(1e-9, relative*math.Max(math.Abs(a), math.Abs(b)))
}

// toNumber preserves exact integers and decimal driver values during comparison.
func toNumber(v interface{}) (*big.Rat, error) {
	if v == nil {
		return nil, errors.New("value is NULL")
	}
	if rat, ok := v.(*big.Rat); ok {
		if rat == nil {
			return nil, errors.New("value is NULL")
		}
		return rat, nil
	}
	if b, ok := v.(bool); ok {
		if b {
			return big.NewRat(1, 1), nil
		}
		return new(big.Rat), nil
	}
	s := strings.TrimSpace(stringForms(v)[0])
	if number, ok := new(big.Rat).SetString(s); ok {
		return number, nil
	}
	return nil, fmt.Errorf("value %q is not numeric", s)
}

func toFloat(v interface{}) (float64, error) {
	number, err := toNumber(v)
	if err != nil {
		return 0, err
	}
	f, _ := number.Float64()
	if math.IsInf(f, 0) {
		return 0, errors.New("numeric value is out of range")
	}
	return f, nil
}

func toBool(v interface{}) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case string:
		return strconv.ParseBool(strings.TrimSpace(val))
	default:
		f, err := toFloat(v)
		if err != nil {
			return false, err
		}
		return f != 0, nil
	}
}

// stringForms returns the textual representations a driver value may take, so
// string expectations match dates and timestamps in common formats.
func stringForms(v interface{}) []string {
	switch val := v.(type) {
	case time.Time:
		return []string{val.Format(time.RFC3339), val.Format("2006-01-02 15:04:05"), val.Format("2006-01-02")}
	case []byte:
		return []string{string(val)}
	default:
		return []string{fmt.Sprint(val)}
	}
}

func displayValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	return stringForms(v)[0]
}

func isScalar(v interface{}) bool {
	switch v.(type) {
	case string, int, int64, float64, float32, int32:
		return true
	default:
		return false
	}
}

func isBool(v interface{}) bool {
	_, ok := v.(bool)
	return ok
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
