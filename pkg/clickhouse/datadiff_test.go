package clickhouse

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSummaryColumnType(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input    string
		kind     diff.CommonDataType
		nullable bool
	}{
		{"UInt64", diff.CommonTypeNumeric, false},
		{"Nullable(Decimal(18, 2))", diff.CommonTypeNumeric, true},
		{"LowCardinality(Nullable(String))", diff.CommonTypeString, true},
		{"FixedString(16)", diff.CommonTypeString, false},
		{"Enum8('a' = 1)", diff.CommonTypeString, false},
		{"UUID", diff.CommonTypeString, false},
		{"Bool", diff.CommonTypeBoolean, false},
		{"DateTime64(3, 'UTC')", diff.CommonTypeDateTime, false},
		{"Date32", diff.CommonTypeDateTime, false},
		{"JSON", diff.CommonTypeJSON, false},
		{"Array(Nullable(Int32))", diff.CommonTypeUnknown, false},
		{"Tuple(String, Int32)", diff.CommonTypeUnknown, false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			kind, nullable := summaryColumnType(tc.input)
			require.Equal(t, tc.kind, kind)
			require.Equal(t, tc.nullable, nullable)
		})
	}
}

func TestGetTableSummary(t *testing.T) {
	t.Parallel()
	for _, schemaOnly := range []bool{true, false} {
		t.Run(map[bool]string{true: "schema only", false: "full"}[schemaOnly], func(t *testing.T) {
			conn := &MockConn{}
			defer conn.AssertExpectations(t)
			conn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool { return strings.Contains(q, "FROM system.columns") })).Return(MockRows{index: new(int), rows: [][]any{
				{"id", "UInt64", uint8(1)}, {"label", "LowCardinality(Nullable(String))", uint8(0)},
			}}, nil).Once()
			if !schemaOnly {
				conn.On("Query", mock.Anything, "SELECT count() FROM `analytics`.`events`").Return(MockRows{index: new(int), rows: [][]any{{uint64(3)}}}, nil).Once()
				conn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool { return strings.Contains(q, "stddevSampOrNull(toFloat64(`id`))") })).Return(MockRows{index: new(int), rows: [][]any{{uint64(3), uint64(0), 1.0, 3.0, 2.0, 6.0, 1.0}}}, nil).Once()
				conn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool { return strings.Contains(q, "uniqExact(`label`)") })).Return(MockRows{index: new(int), rows: [][]any{{uint64(3), uint64(1), uint64(2), uint64(0), uint64(4), 2.0, uint64(1)}}}, nil).Once()
			}
			client := &Client{connection: conn, config: &Config{Database: "analytics"}}
			result, err := client.GetTableSummary(context.Background(), "events", schemaOnly)
			require.NoError(t, err)
			require.Len(t, result.Table.Columns, 2)
			require.True(t, result.Table.Columns[0].PrimaryKey)
			require.False(t, result.Table.Columns[0].Unique)
			require.False(t, result.Table.Columns[0].Nullable)
			require.True(t, result.Table.Columns[1].Nullable)
			if schemaOnly {
				require.Zero(t, result.RowCount)
				require.Nil(t, result.Table.Columns[0].Stats)
			} else {
				require.EqualValues(t, 3, result.RowCount)
				stats := result.Table.Columns[0].Stats.(*diff.NumericalStatistics)
				require.Equal(t, 6.0, *stats.Sum)
				require.EqualValues(t, 3, stats.Count)
				require.Equal(t, &diff.StringStatistics{Count: 3, NullCount: 1, DistinctCount: 2, MinLength: 0, MaxLength: 4, AvgLength: 2, EmptyCount: 1}, result.Table.Columns[1].Stats)
			}
		})
	}
}

func TestFetchSummaryStats(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		kind diff.CommonDataType
		row  []any
		want diff.ColumnStatistics
	}{
		{"empty numeric", diff.CommonTypeNumeric, []any{uint64(0), uint64(0), nil, nil, nil, nil, nil}, &diff.NumericalStatistics{}},
		{"all null numeric", diff.CommonTypeNumeric, []any{uint64(2), uint64(2), nil, nil, nil, nil, math.NaN()}, &diff.NumericalStatistics{NullCount: 2}},
		{"empty string", diff.CommonTypeString, []any{uint64(0), uint64(0), uint64(0), nil, nil, nil, uint64(0)}, &diff.StringStatistics{}},
		{"boolean", diff.CommonTypeBoolean, []any{uint64(4), uint64(1), uint64(2), uint64(1)}, &diff.BooleanStatistics{Count: 4, NullCount: 1, TrueCount: 2, FalseCount: 1}},
		{"datetime", diff.CommonTypeDateTime, []any{uint64(2), uint64(1), now, now, uint64(1)}, &diff.DateTimeStatistics{Count: 2, NullCount: 1, UniqueCount: 1, EarliestDate: &now, LatestDate: &now}},
		{"empty datetime", diff.CommonTypeDateTime, []any{uint64(0), uint64(0), nil, nil, uint64(0)}, &diff.DateTimeStatistics{}},
		{"json", diff.CommonTypeJSON, []any{uint64(2), uint64(0)}, &diff.JSONStatistics{Count: 2}},
		{"unknown", diff.CommonTypeUnknown, nil, &diff.UnknownStatistics{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &MockConn{}
			defer conn.AssertExpectations(t)
			if tc.row != nil {
				conn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
					return strings.HasPrefix(q, "SELECT count(), countIf(isNull(`a\\`b`))") && strings.HasSuffix(q, "FROM `db`.`table`")
				})).Return(MockRows{index: new(int), rows: [][]any{tc.row}}, nil).Once()
			}
			client := &Client{connection: conn}
			stats, err := client.fetchSummaryStats(context.Background(), "`db`.`table`", &diff.Column{Name: "a`b", NormalizedType: tc.kind})
			require.NoError(t, err)
			require.Equal(t, tc.want, stats)
		})
	}
}

func TestGetTableSummaryErrors(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"", ".events", "db.", "a.b.c"} {
		_, err := (&Client{}).GetTableSummary(context.Background(), table, true)
		require.ErrorContains(t, err, "table name must be")
	}
	for _, tc := range []struct {
		name string
		rows *MockRows
		err  error
		want string
	}{
		{"query", nil, errors.New("query failed"), "query failed"},
		{"missing", &MockRows{index: new(int)}, nil, "does not exist"},
		{"scan", &MockRows{index: new(int), rows: [][]any{{"id"}}, scanError: errors.New("scan failed")}, nil, "scan failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &MockConn{}
			conn.On("Query", mock.Anything, mock.Anything).Return(tc.rows, tc.err).Once()
			client := &Client{connection: conn, config: &Config{}}
			_, err := client.GetTableSummary(context.Background(), "db.events", true)
			require.ErrorContains(t, err, tc.want)
			conn.AssertExpectations(t)
		})
	}
}
