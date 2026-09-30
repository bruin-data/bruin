// Package semanticcheck executes semantic layer quality checks against Bruin
// connections. SQL generation and pass/fail evaluation live in the semantic
// engine; this package only deals with connections, concurrency, and results.
package semanticcheck

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/query"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/pkg/errors"
)

type selector interface {
	Select(ctx context.Context, q *query.Query) ([][]interface{}, error)
}

type validator interface {
	IsValid(ctx context.Context, q *query.Query) (bool, error)
}

type dryRunner interface {
	DryRunQuery(ctx context.Context, q *query.Query) (*query.DryRunResult, error)
}

// Status is the outcome of running or validating a single check.
type Status string

const (
	StatusPassed  Status = "passed"
	StatusFailed  Status = "failed"
	StatusError   Status = "error"
	StatusSkipped Status = "skipped"
)

// Result describes the outcome of one check.
type Result struct {
	ID          string              `json:"id"`
	Model       string              `json:"model"`
	Scope       semantic.CheckScope `json:"scope"`
	Target      string              `json:"target,omitempty"`
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	SQL         string              `json:"sql"`
	Status      Status              `json:"status"`
	Message     string              `json:"message,omitempty"`
	DurationMS  int64               `json:"duration_ms"`
}

// Summary counts results by status.
type Summary struct {
	Passed  int `json:"passed"`
	Failed  int `json:"failed"`
	Errors  int `json:"errors"`
	Skipped int `json:"skipped"`
}

// Summarize tallies the results by status.
func Summarize(results []Result) Summary {
	var s Summary
	for _, r := range results {
		switch r.Status {
		case StatusPassed:
			s.Passed++
		case StatusFailed:
			s.Failed++
		case StatusError:
			s.Errors++
		case StatusSkipped:
			s.Skipped++
		}
	}
	return s
}

// OK reports whether every result passed or was skipped.
func (s Summary) OK() bool {
	return s.Failed == 0 && s.Errors == 0
}

// DefaultConcurrency is the number of checks run in parallel per Run call.
const DefaultConcurrency = 8

// Runner executes compiled checks.
type Runner struct {
	// Concurrency limits how many checks run at the same time. Zero or
	// negative values use DefaultConcurrency.
	Concurrency int
}

// SupportsQueries reports whether the connection can run the check queries.
func SupportsQueries(conn interface{}) bool {
	_, ok := conn.(selector)
	return ok
}

// Run executes every check against conn and evaluates the results. The order
// of the returned results matches the order of the checks.
func (r *Runner) Run(ctx context.Context, conn interface{}, checks []semantic.CompiledCheck) []Result {
	return r.each(ctx, checks, func(ctx context.Context, check *semantic.CompiledCheck) (Status, string) {
		return runCheck(ctx, conn, check)
	})
}

// Validate dry-runs every check against conn without evaluating data. It uses
// the connection's dry-run or EXPLAIN support when available and falls back to
// executing the query with a false predicate so that it is planned but returns
// no rows.
func (r *Runner) Validate(ctx context.Context, conn interface{}, checks []semantic.CompiledCheck) []Result {
	return r.each(ctx, checks, func(ctx context.Context, check *semantic.CompiledCheck) (Status, string) {
		nestable := check.SQL
		if check.ValidationSQL != "" {
			nestable = check.ValidationSQL
		}
		if err := validateSQL(ctx, conn, check.SQL, nestable); err != nil {
			return StatusFailed, err.Error()
		}
		return StatusPassed, ""
	})
}

// ValidateSQL checks that a single SELECT statement is valid on conn without
// materializing its result. It prefers the platform dry run or EXPLAIN, which
// resolves tables and columns, over syntax-only validation.
func ValidateSQL(ctx context.Context, conn interface{}, sql string) error {
	return validateSQL(ctx, conn, sql, sql)
}

// validateSQL validates sql, falling back to nesting it in a derived table when
// the platform has no dry run. Only that fallback uses nestable, an equivalent
// form of sql that survives nesting; the dry-run and EXPLAIN paths validate sql
// itself so that clauses the nestable form drops are still checked.
func validateSQL(ctx context.Context, conn interface{}, sql, nestable string) error {
	dryRunCtx := query.WithQueryType(ctx, query.QueryTypeDryRun)
	if d, ok := conn.(dryRunner); ok {
		result, err := d.DryRunQuery(dryRunCtx, &query.Query{Query: sql})
		if err != nil {
			return err
		}
		if result != nil && !result.Valid {
			return errors.New("query is invalid")
		}
		return nil
	}

	if v, ok := conn.(validator); ok {
		valid, err := v.IsValid(dryRunCtx, &query.Query{Query: sql})
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("query is invalid")
		}
		return nil
	}

	s, ok := conn.(selector)
	if !ok {
		return errors.New("connection does not support running queries")
	}
	// Oracle rejects AS before a table alias, so the derived table is aliased
	// without it. The wrapped queries name their output columns because SQL
	// Server rejects a derived table with an unnamed column.
	probe := "SELECT * FROM (" + strings.TrimRight(nestable, "; \r\n\t") + ") bruin_dry_run WHERE 1 = 0"
	if _, err := s.Select(ctx, &query.Query{Query: probe}); err != nil {
		return err
	}
	return nil
}

func runCheck(ctx context.Context, conn interface{}, check *semantic.CompiledCheck) (Status, string) {
	s, ok := conn.(selector)
	if !ok {
		return StatusError, "connection does not support running queries"
	}
	rows, err := s.Select(ctx, &query.Query{Query: check.SQL})
	if err != nil {
		return StatusError, err.Error()
	}
	if err := check.Evaluate(rows); err != nil {
		return StatusFailed, err.Error()
	}
	return StatusPassed, ""
}

func (r *Runner) each(ctx context.Context, checks []semantic.CompiledCheck, fn func(context.Context, *semantic.CompiledCheck) (Status, string)) []Result {
	concurrency := DefaultConcurrency
	if r != nil && r.Concurrency > 0 {
		concurrency = r.Concurrency
	}

	results := make([]Result, len(checks))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range checks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			check := &checks[i]
			started := time.Now()
			status, message := fn(ctx, check)
			results[i] = Result{
				ID:          check.ID(),
				Model:       check.Model,
				Scope:       check.Scope,
				Target:      check.Target,
				Name:        check.Name,
				Description: check.Description,
				SQL:         check.SQL,
				Status:      status,
				Message:     message,
				DurationMS:  time.Since(started).Milliseconds(),
			}
		}(i)
	}
	wg.Wait()
	return results
}

// SortResults orders results by model, scope, target, and name so that output
// is deterministic regardless of execution order.
func SortResults(results []Result) {
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Model != results[j].Model {
			return results[i].Model < results[j].Model
		}
		if results[i].Scope != results[j].Scope {
			return scopeOrder(results[i].Scope) < scopeOrder(results[j].Scope)
		}
		if results[i].Target != results[j].Target {
			return results[i].Target < results[j].Target
		}
		return results[i].Name < results[j].Name
	})
}

func scopeOrder(scope semantic.CheckScope) int {
	switch scope {
	case semantic.CheckScopeDimension:
		return 0
	case semantic.CheckScopeMetric:
		return 1
	case semantic.CheckScopeModel:
		return 2
	default:
		return 3
	}
}
