package semanticcheck

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeConn struct {
	mu      sync.Mutex
	queries []string
	rows    map[string][][]interface{}
	errs    map[string]error
}

func (f *fakeConn) Select(_ context.Context, q *query.Query) ([][]interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q.Query)
	if err, ok := f.errs[q.Query]; ok {
		return nil, err
	}
	if rows, ok := f.rows[q.Query]; ok {
		return rows, nil
	}
	return [][]interface{}{{int64(0)}}, nil
}

type fakeValidator struct {
	fakeConn
	invalid map[string]bool
	dryRun  bool
}

func (f *fakeValidator) IsValid(ctx context.Context, q *query.Query) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dryRun = query.QueryTypeFromContext(ctx) == query.QueryTypeDryRun
	f.queries = append(f.queries, q.Query)
	if err, ok := f.errs[q.Query]; ok {
		return false, err
	}
	return !f.invalid[q.Query], nil
}

type fakeDryRunner struct {
	fakeConn
	invalid map[string]string
}

func (f *fakeDryRunner) DryRunQuery(_ context.Context, q *query.Query) (*query.DryRunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q.Query)
	if err, ok := f.errs[q.Query]; ok {
		return nil, err
	}
	if msg, ok := f.invalid[q.Query]; ok {
		_ = msg
		return &query.DryRunResult{Valid: false}, nil
	}
	return &query.DryRunResult{Valid: true}, nil
}

func TestRunnerValidatePrefersDryRunQuery(t *testing.T) {
	t.Parallel()
	checks := testChecks(t)
	conn := &fakeDryRunner{
		fakeConn: fakeConn{errs: map[string]error{"SELECT sum(amount) AS revenue FROM analytics.orders": errors.New("EXPLAIN failed: relation missing")}},
		invalid:  map[string]string{"SELECT count(*) AS order_count FROM analytics.orders": "bad plan"},
	}

	results := (&Runner{}).Validate(context.Background(), conn, checks)
	require.Len(t, results, 3)
	assert.Equal(t, StatusPassed, results[0].Status)
	assert.Equal(t, StatusFailed, results[1].Status)
	assert.Equal(t, "EXPLAIN failed: relation missing", results[1].Message)
	assert.Equal(t, StatusFailed, results[2].Status)
	assert.Equal(t, "query is invalid", results[2].Message)
	for _, q := range conn.queries {
		assert.NotContains(t, q, "bruin_dry_run")
	}
}

// A sorted model check carries a nestable form of its SQL for the wrapping
// fallback. The dry-run and EXPLAIN paths must still validate the real
// statement, so that a bad ORDER BY does not pass validation and then fail the
// run.
func TestRunnerValidateChecksTheRealSQLWhenTheConnectionCanDryRun(t *testing.T) {
	t.Parallel()
	sorted := sortedModelCheck(t)
	require.NotEmpty(t, sorted.ValidationSQL)
	require.NotEqual(t, sorted.SQL, sorted.ValidationSQL)
	checks := []semantic.CompiledCheck{sorted}

	dryRunner := &fakeDryRunner{invalid: map[string]string{sorted.SQL: "bad order by"}}
	results := (&Runner{}).Validate(context.Background(), dryRunner, checks)
	require.Len(t, results, 1)
	assert.Equal(t, StatusFailed, results[0].Status)
	assert.Equal(t, []string{sorted.SQL}, dryRunner.queries)

	validator := &fakeValidator{invalid: map[string]bool{sorted.SQL: true}}
	results = (&Runner{}).Validate(context.Background(), validator, checks)
	require.Len(t, results, 1)
	assert.Equal(t, StatusFailed, results[0].Status)
	assert.Equal(t, []string{sorted.SQL}, validator.queries)

	// Only the fallback, which nests the statement, uses the nestable form.
	plain := &fakeConn{}
	results = (&Runner{}).Validate(context.Background(), plain, checks)
	require.Len(t, results, 1)
	assert.Equal(t, StatusPassed, results[0].Status)
	require.Len(t, plain.queries, 1)
	assert.Contains(t, plain.queries[0], sorted.ValidationSQL)
	assert.NotContains(t, plain.queries[0], "ORDER BY")
}

func sortedModelCheck(t *testing.T) semantic.CompiledCheck {
	t.Helper()
	model := &semantic.Model{
		Name:       "orders",
		Source:     semantic.Source{Table: "analytics.orders"},
		Dimensions: []semantic.Dimension{{Name: "country"}},
		Metrics:    []semantic.Metric{{Name: "revenue", Expression: "sum(amount)"}},
		Checks: []semantic.ModelCheck{{
			Name: "sorted",
			Query: &semantic.Query{
				Dimensions: []semantic.DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Sort:       []semantic.SortSpec{{Name: "revenue", Direction: "desc"}},
			},
			Value: []interface{}{map[string]interface{}{"country": "US"}},
		}},
	}
	engine, err := semantic.NewEngine(model)
	require.NoError(t, err)
	checks, err := engine.CompileChecks(semantic.CompileChecksOptions{})
	require.NoError(t, err)
	require.Len(t, checks, 1)
	return checks[0]
}

func testChecks(t *testing.T) []semantic.CompiledCheck {
	t.Helper()
	model := &semantic.Model{
		Name:   "orders",
		Source: semantic.Source{Table: "analytics.orders"},
		Dimensions: []semantic.Dimension{
			{Name: "country", Checks: []semantic.Check{{Name: "not_null"}}},
		},
		Metrics: []semantic.Metric{
			{Name: "revenue", Expression: "sum(amount)", Checks: []semantic.Check{{Name: "positive"}}},
			{Name: "order_count", Expression: "count(*)"},
		},
		Checks: []semantic.ModelCheck{{Name: "custom", Query: &semantic.Query{Metrics: []string{"order_count"}}, Value: 7}},
	}
	engine, err := semantic.NewEngine(model)
	require.NoError(t, err)
	checks, err := engine.CompileChecks(semantic.CompileChecksOptions{})
	require.NoError(t, err)
	return checks
}

func TestRunnerRun(t *testing.T) {
	t.Parallel()
	checks := testChecks(t)
	conn := &fakeConn{
		rows: map[string][][]interface{}{
			"SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE country IS NULL": {{int64(2)}},
			"SELECT sum(amount) AS revenue FROM analytics.orders":                              {{123.4}},
		},
		errs: map[string]error{
			"SELECT count(*) AS order_count FROM analytics.orders": errors.New("boom"),
		},
	}

	results := (&Runner{Concurrency: 2}).Run(context.Background(), conn, checks)
	require.Len(t, results, 3)

	assert.Equal(t, "orders.dimension.country.not_null", results[0].ID)
	assert.Equal(t, StatusFailed, results[0].Status)
	assert.Equal(t, "dimension 'country' has 2 null values", results[0].Message)

	assert.Equal(t, "orders.metric.revenue.positive", results[1].ID)
	assert.Equal(t, StatusPassed, results[1].Status)
	assert.Empty(t, results[1].Message)

	assert.Equal(t, "orders.check.custom", results[2].ID)
	assert.Equal(t, StatusError, results[2].Status)
	assert.Equal(t, "boom", results[2].Message)

	summary := Summarize(results)
	assert.Equal(t, Summary{Passed: 1, Failed: 1, Errors: 1}, summary)
	assert.False(t, summary.OK())
	assert.Len(t, conn.queries, 3)
}

func TestRunnerRunDecimalDriverValues(t *testing.T) {
	t.Parallel()
	model := &semantic.Model{
		Name:   "orders",
		Source: semantic.Source{Table: "analytics.orders"},
		Metrics: []semantic.Metric{{
			Name:       "revenue",
			Expression: "sum(amount)",
			Checks: []semantic.Check{
				{Name: "positive"},
				{Name: "min", Value: 12},
				{Name: "max", Value: 13},
				{Name: "equals", Value: 12.3},
			},
		}},
	}
	engine, err := semantic.NewEngine(model)
	require.NoError(t, err)
	checks, err := engine.CompileChecks(semantic.CompileChecksOptions{})
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{name: "BigQuery NUMERIC", value: big.NewRat(123, 10)},
		{name: "Postgres NUMERIC", value: pgtype.Numeric{Int: big.NewInt(123), Exp: -1, Valid: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := &fakeConn{rows: map[string][][]interface{}{
				checks[0].SQL: {{tc.value}},
			}}
			results := (&Runner{}).Run(t.Context(), conn, checks)
			require.Len(t, results, 4)
			for _, result := range results {
				assert.Equal(t, StatusPassed, result.Status, "%s: %s", result.ID, result.Message)
			}
		})
	}
}

func TestRunnerRunUnsupportedConnection(t *testing.T) {
	t.Parallel()
	results := (&Runner{}).Run(context.Background(), struct{}{}, testChecks(t))
	require.Len(t, results, 3)
	for _, r := range results {
		assert.Equal(t, StatusError, r.Status)
		assert.Contains(t, r.Message, "does not support running queries")
	}
	assert.False(t, SupportsQueries(struct{}{}))
	assert.True(t, SupportsQueries(&fakeConn{}))
}

func TestRunnerValidateUsesDryRunWhenAvailable(t *testing.T) {
	t.Parallel()
	checks := testChecks(t)
	conn := &fakeValidator{invalid: map[string]bool{"SELECT count(*) AS order_count FROM analytics.orders": true}}

	results := (&Runner{}).Validate(context.Background(), conn, checks)
	require.Len(t, results, 3)
	assert.Equal(t, StatusPassed, results[0].Status)
	assert.Equal(t, StatusPassed, results[1].Status)
	assert.Equal(t, StatusFailed, results[2].Status)
	assert.Equal(t, "query is invalid", results[2].Message)
	assert.True(t, conn.dryRun)
	for _, q := range conn.queries {
		assert.NotContains(t, q, "bruin_dry_run")
	}
}

func TestRunnerValidateFallsBackToFalsePredicate(t *testing.T) {
	t.Parallel()
	checks := testChecks(t)
	conn := &fakeConn{errs: map[string]error{
		"SELECT * FROM (SELECT count(*) AS order_count FROM analytics.orders) bruin_dry_run WHERE 1 = 0": errors.New("no such table"),
	}}

	results := (&Runner{}).Validate(context.Background(), conn, checks)
	require.Len(t, results, 3)
	assert.Equal(t, StatusPassed, results[0].Status)
	assert.Equal(t, StatusFailed, results[2].Status)
	assert.Equal(t, "no such table", results[2].Message)
	for _, q := range conn.queries {
		assert.True(t, strings.HasPrefix(q, "SELECT * FROM ("), q)
		assert.True(t, strings.HasSuffix(q, ") bruin_dry_run WHERE 1 = 0"), q)
	}
}

func TestValidateSQLFallbackWithTerminator(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{}
	require.NoError(t, ValidateSQL(t.Context(), conn, "SELECT 1;\n"))
	assert.Equal(t, []string{"SELECT * FROM (SELECT 1) bruin_dry_run WHERE 1 = 0"}, conn.queries)
}

func TestValidateSQLUnsupportedConnection(t *testing.T) {
	t.Parallel()
	err := ValidateSQL(context.Background(), struct{}{}, "select 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support running queries")
}

func TestSortResults(t *testing.T) {
	t.Parallel()
	results := []Result{
		{Model: "orders", Scope: semantic.CheckScopeModel, Name: "b"},
		{Model: "orders", Scope: semantic.CheckScopeMetric, Target: "revenue", Name: "positive"},
		{Model: "customers", Scope: semantic.CheckScopeDimension, Target: "country", Name: "not_null"},
		{Model: "orders", Scope: semantic.CheckScopeDimension, Target: "country", Name: "unique"},
		{Model: "orders", Scope: semantic.CheckScopeDimension, Target: "country", Name: "not_null"},
		{Model: "orders", Scope: semantic.CheckScopeModel, Name: "a"},
	}
	SortResults(results)
	got := make([]string, len(results))
	for i, r := range results {
		got[i] = r.Model + "/" + string(r.Scope) + "/" + r.Target + "/" + r.Name
	}
	assert.Equal(t, []string{
		"customers/dimension/country/not_null",
		"orders/dimension/country/not_null",
		"orders/dimension/country/unique",
		"orders/metric/revenue/positive",
		"orders/model//a",
		"orders/model//b",
	}, got)
}
