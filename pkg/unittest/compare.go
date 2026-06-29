package unittest

import (
	"database/sql/driver"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// UnitTestResult is the outcome of running a single unit test.
type UnitTestResult struct {
	Passed  bool
	Message string // human-readable explanation when Passed is false
}

// CompareResult compares a query result (columns + rows, as returned by a
// connection via SelectWithSchema) against a unit test's expectation.
func CompareResult(expected pipeline.UnitTestExpected, columns []string, rows [][]interface{}) *UnitTestResult {
	return compareExpectation(expected, rowsToMaps(columns, rows))
}

// CompareCTEResult compares the rows produced by a named intermediate CTE
// against its expectation, with the same semantics as the final output.
func CompareCTEResult(expected pipeline.UnitTestCTEExpected, columns []string, rows [][]interface{}) *UnitTestResult {
	return compareRowSet(expected.Rows, expected.Count, expected.Match, expected.Order, rowsToMaps(columns, rows))
}

// compareExpectation checks the actual query output against a test's top-level
// expectation (the final query's rows/count under the chosen match/order mode).
func compareExpectation(expected pipeline.UnitTestExpected, actual []map[string]interface{}) *UnitTestResult {
	return compareRowSet(expected.Rows, expected.Count, expected.Match, expected.Order, actual)
}

// compareRowSet checks actual rows against an expectation. Count and Rows are
// independent assertions: a test may set either or both, and when both are set
// both must hold. Count fixes the total row count, while Rows asserts specific
// rows under the chosen match/order mode.
func compareRowSet(expectedRows []map[string]interface{}, count *int64, match, order string, actual []map[string]interface{}) *UnitTestResult {
	if count != nil && int64(len(actual)) != *count {
		return fail(fmt.Sprintf("expected %d row(s), got %d\n%s", *count, len(actual), describeRows("actual rows", actual)))
	}

	ordered := strings.EqualFold(order, "strict")
	switch m := strings.ToLower(match); m {
	case "", "subset", "exact":
		return compareRows(expectedRows, actual, ordered, m == "exact")
	default:
		return fail(fmt.Sprintf("unknown match mode %q (use \"subset\" or \"exact\")", match))
	}
}

// compareRows checks that every expected row appears in actual; in exact mode it
// additionally requires the row counts to be equal. In unordered mode it binds
// expected rows to distinct actual rows with a maximum bipartite matching, so a
// broad partial-column expected row never claims the only actual row that a
// narrower expected row needs.
func compareRows(expected, actual []map[string]interface{}, ordered, exact bool) *UnitTestResult {
	if exact && len(expected) != len(actual) {
		return fail(fmt.Sprintf(
			"expected exactly %d row(s), got %d\n%s\n%s",
			len(expected), len(actual),
			describeRows("expected rows", expected),
			describeRows("actual rows", actual),
		))
	}
	if ordered {
		if len(actual) < len(expected) {
			return fail(fmt.Sprintf("expected at least %d row(s), got %d\n%s", len(expected), len(actual), describeRows("actual rows", actual)))
		}
		for i, exp := range expected {
			if !rowMatches(exp, actual[i]) {
				return fail(fmt.Sprintf("row %d mismatch\n  expected: %s\n  actual:   %s", i, formatRow(exp), formatRow(actual[i])))
			}
		}
		return pass()
	}

	// Bind each expected row to a distinct actual row via a maximum bipartite
	// matching (augmenting paths). A greedy first-match bind can fail when a
	// broad expected row claims the only actual row a narrower expected row needs.
	matchedBy := make([]int, len(actual)) // actual index -> expected index, or -1
	for i := range matchedBy {
		matchedBy[i] = -1
	}
	for ei := range expected {
		seen := make([]bool, len(actual))
		if !augmentMatch(ei, expected, actual, matchedBy, seen) {
			return fail(fmt.Sprintf("expected row not found: %s\n%s", formatRow(expected[ei]), describeRows("actual rows", actual)))
		}
	}
	return pass()
}

// augmentMatch tries to bind expected row ei to an actual row, possibly bumping
// an already-bound expected row onto a different match (an augmenting path).
// It returns false when no actual row can be freed for ei.
func augmentMatch(ei int, expected, actual []map[string]interface{}, matchedBy []int, seen []bool) bool {
	for ai := range actual {
		if seen[ai] || !rowMatches(expected[ei], actual[ai]) {
			continue
		}
		seen[ai] = true
		if matchedBy[ai] == -1 || augmentMatch(matchedBy[ai], expected, actual, matchedBy, seen) {
			matchedBy[ai] = ei
			return true
		}
	}
	return false
}

// rowMatches reports whether every column asserted in expected matches actual,
// allowing actual to carry extra columns (partial assertions). Column names are
// matched case-insensitively (actual keys are already lower-cased by rowsToMaps).
func rowMatches(expected, actual map[string]interface{}) bool {
	for k, want := range expected {
		got, ok := actual[strings.ToLower(k)]
		if !ok || !valuesEqual(want, got) {
			return false
		}
	}
	return true
}

// valuesEqual compares fixture values to query output tolerantly: numbers are
// compared numerically (YAML decodes ints as int; warehouses may return numerics
// as float64 or as strings), dates/timestamps are compared by instant (engines
// return them as RFC3339 strings while YAML may decode them as time.Time or a
// plain date string), and everything else compares by string.
func valuesEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			return floatsEqual(af, bf)
		}
	}
	if at, aok := toTime(a); aok {
		if bt, bok := toTime(b); bok {
			return at.Equal(bt)
		}
	}
	// Everything else (bools, strings) compares by string form, which is exact.
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// toTime reports whether v is a date/timestamp, as a time.Time or a string in a
// recognized layout, normalized to UTC. Strings that are not dates (the common
// case) return false so they fall through to the string comparison.
func toTime(v interface{}) (time.Time, bool) {
	switch val := v.(type) {
	case time.Time:
		return val.UTC(), true
	case string:
		// RFC3339(Nano) covers tz-suffixed values; the space- and T-separated
		// layouts cover warehouses that return a datetime without a zone
		// (Postgres uses a space, BigQuery uses "T"), with or without fractions.
		for _, layout := range []string{
			time.RFC3339Nano, time.RFC3339,
			"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05",
			"2006-01-02T15:04:05.999999", "2006-01-02T15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, val); err == nil {
				return t.UTC(), true
			}
		}
	}
	return time.Time{}, false
}

// floatsEqual compares with a small relative tolerance so computed and decimal
// values don't fail on representation noise such as 0.1 + 0.2 != 0.3.
func floatsEqual(a, b float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		// Warehouses often return numerics as strings (e.g. Snowflake NUMBER as
		// "5.000000"); compare those numerically so they match an int/float
		// expectation. Non-numeric strings fall through to the string comparison.
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// rowsToMaps zips column names with positional row values. Column names are
// lower-cased so comparison is case-insensitive (warehouses such as Snowflake
// return unquoted identifiers upper-cased, while fixtures use the written case),
// and each value is normalized so driver-specific wrappers compare cleanly.
func rowsToMaps(columns []string, rows [][]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		m := make(map[string]interface{}, len(columns))
		for i, c := range columns {
			if i < len(r) {
				m[strings.ToLower(c)] = normalizeCell(r[i])
			}
		}
		out = append(out, m)
	}
	return out
}

// normalizeCell unwraps driver-specific value types so comparison sees plain Go
// values. Postgres' pgx returns NUMERIC as pgtype.Numeric (a driver.Valuer whose
// Value() is the numeric string), and some drivers return text as []byte.
func normalizeCell(v interface{}) interface{} {
	if valuer, ok := v.(driver.Valuer); ok {
		if dv, err := valuer.Value(); err == nil {
			v = dv
		}
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func formatRow(row map[string]interface{}) string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%s", k, formatValue(row[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func formatValue(v interface{}) string {
	if v == nil {
		return "null"
	}
	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}
	return fmt.Sprintf("%v", v)
}

// describeRows formats up to a fixed number of rows under a label for the diff
// shown on a failed unit test.
func describeRows(label string, rows []map[string]interface{}) string {
	const limit = 20
	var b strings.Builder
	fmt.Fprintf(&b, "%s (%d):", label, len(rows))
	if len(rows) == 0 {
		b.WriteString(" <none>")
		return b.String()
	}
	shown := rows
	if len(rows) > limit {
		shown = rows[:limit]
	}
	for _, r := range shown {
		fmt.Fprintf(&b, "\n  %s", formatRow(r))
	}
	if len(rows) > limit {
		fmt.Fprintf(&b, "\n  ... and %d more", len(rows)-limit)
	}
	return b.String()
}

func pass() *UnitTestResult { return &UnitTestResult{Passed: true} }

func fail(msg string) *UnitTestResult { return &UnitTestResult{Passed: false, Message: msg} }
