package unittest

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func i64(v int64) *int64 { return &v }

func TestCompareExpectation(t *testing.T) {
	t.Parallel()

	oneRow := []map[string]interface{}{{"revenue": int64(100)}}
	twoRows := []map[string]interface{}{{"x": int64(1)}, {"x": int64(2)}}

	t.Run("subset passes with int(yaml) vs int64(engine)", func(t *testing.T) {
		t.Parallel()
		res := compareExpectation(pipeline.UnitTestExpected{
			Rows: []map[string]interface{}{{"revenue": 100}},
		}, oneRow)
		require.True(t, res.Passed, res.Message)
	})

	t.Run("subset fails and shows expected + actual rows", func(t *testing.T) {
		t.Parallel()
		res := compareExpectation(pipeline.UnitTestExpected{
			Rows: []map[string]interface{}{{"revenue": 999}},
		}, oneRow)
		require.False(t, res.Passed)
		require.Contains(t, res.Message, "999")
		require.Contains(t, res.Message, "100")
		require.Contains(t, res.Message, "actual rows")
	})

	t.Run("count matches", func(t *testing.T) {
		t.Parallel()
		require.True(t, compareExpectation(pipeline.UnitTestExpected{Count: i64(1)}, oneRow).Passed)
		require.False(t, compareExpectation(pipeline.UnitTestExpected{Count: i64(2)}, oneRow).Passed)
	})

	t.Run("count and rows are both enforced when both set", func(t *testing.T) {
		t.Parallel()
		require.True(t, compareExpectation(pipeline.UnitTestExpected{
			Count: i64(2),
			Rows:  []map[string]interface{}{{"x": 1}},
		}, twoRows).Passed)

		countWrong := compareExpectation(pipeline.UnitTestExpected{
			Count: i64(1),
			Rows:  []map[string]interface{}{{"x": 1}},
		}, twoRows)
		require.False(t, countWrong.Passed)
		require.Contains(t, countWrong.Message, "expected 1 row(s), got 2")

		rowWrong := compareExpectation(pipeline.UnitTestExpected{
			Count: i64(2),
			Rows:  []map[string]interface{}{{"x": 99}},
		}, twoRows)
		require.False(t, rowWrong.Passed)
		require.Contains(t, rowWrong.Message, "expected row not found")
	})

	t.Run("dates compare by instant across forms", func(t *testing.T) {
		t.Parallel()
		actual := []map[string]interface{}{{"d": "2024-01-15T00:00:00Z"}}
		require.True(t, compareExpectation(pipeline.UnitTestExpected{
			Rows: []map[string]interface{}{{"d": "2024-01-15"}},
		}, actual).Passed)
		require.True(t, compareExpectation(pipeline.UnitTestExpected{
			Rows: []map[string]interface{}{{"d": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)}},
		}, actual).Passed)
		require.False(t, compareExpectation(pipeline.UnitTestExpected{
			Rows: []map[string]interface{}{{"d": "2024-01-16"}},
		}, actual).Passed)

		// BigQuery returns a datetime T-separated with no zone; it must still
		// match a space-separated expected value by instant.
		require.True(t, compareExpectation(pipeline.UnitTestExpected{
			Rows: []map[string]interface{}{{"ts": "2024-01-15 09:30:00"}},
		}, []map[string]interface{}{{"ts": "2024-01-15T09:30:00"}}).Passed)
	})

	t.Run("float comparison tolerates representation noise", func(t *testing.T) {
		t.Parallel()
		actual := []map[string]interface{}{{"v": 0.1 + 0.2}} // 0.30000000000000004
		res := compareExpectation(pipeline.UnitTestExpected{
			Rows: []map[string]interface{}{{"v": 0.3}},
		}, actual)
		require.True(t, res.Passed, res.Message)
	})

	t.Run("subset ignores extra actual rows; exact does not", func(t *testing.T) {
		t.Parallel()
		expected := pipeline.UnitTestExpected{Rows: []map[string]interface{}{{"x": 1}}}
		require.True(t, compareExpectation(expected, twoRows).Passed)

		exact := pipeline.UnitTestExpected{Match: "exact", Rows: []map[string]interface{}{{"x": 1}}}
		require.False(t, compareExpectation(exact, twoRows).Passed)
	})

	t.Run("ordered strict respects row order", func(t *testing.T) {
		t.Parallel()
		expected := pipeline.UnitTestExpected{
			Order: "strict",
			Rows:  []map[string]interface{}{{"x": 2}, {"x": 1}},
		}
		require.False(t, compareExpectation(expected, twoRows).Passed)

		ok := pipeline.UnitTestExpected{
			Order: "strict",
			Rows:  []map[string]interface{}{{"x": 1}, {"x": 2}},
		}
		require.True(t, compareExpectation(ok, twoRows).Passed)
	})
}

// stringValuer mimics a driver type (like pgtype.Numeric) whose Value() returns
// a numeric string.
type stringValuer struct{ s string }

func (v stringValuer) Value() (driver.Value, error) { return v.s, nil }

func TestCompareResult(t *testing.T) {
	t.Parallel()

	t.Run("case-insensitive columns + numeric string from the engine", func(t *testing.T) {
		t.Parallel()
		// Engine returns upper-cased column and a numeric-as-string value.
		res := CompareResult(
			pipeline.UnitTestExpected{Rows: []map[string]interface{}{{"revenue": 5}}},
			[]string{"REVENUE"},
			[][]interface{}{{"5.000000"}},
		)
		require.True(t, res.Passed, res.Message)
	})

	t.Run("driver.Valuer values are unwrapped and compared numerically", func(t *testing.T) {
		t.Parallel()
		res := CompareResult(
			pipeline.UnitTestExpected{Rows: []map[string]interface{}{{"avg_unit": 12.48}}},
			[]string{"avg_unit"},
			[][]interface{}{{stringValuer{"12.48"}}},
		)
		require.True(t, res.Passed, res.Message)
	})

	t.Run("[]byte text is decoded to string", func(t *testing.T) {
		t.Parallel()
		res := CompareResult(
			pipeline.UnitTestExpected{Rows: []map[string]interface{}{{"name": "alice"}}},
			[]string{"name"},
			[][]interface{}{{[]byte("alice")}},
		)
		require.True(t, res.Passed, res.Message)
	})
}
