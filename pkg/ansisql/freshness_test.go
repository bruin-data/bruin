package ansisql

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestFreshnessCheck(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name     string
		result   [][]interface{}
		queryErr error
		wantErr  string
		failed   bool
	}{
		{name: "fresh", result: [][]interface{}{{now.Add(-time.Hour)}}},
		{name: "boundary", result: [][]interface{}{{now.Add(-30 * time.Hour)}}},
		{name: "future", result: [][]interface{}{{now.Add(time.Hour)}}},
		{name: "stale", result: [][]interface{}{{now.Add(-30*time.Hour - time.Nanosecond)}}, wantErr: "exceeding maximum age 30h0m0s", failed: true},
		{name: "empty or all null", result: [][]interface{}{{nil}}, wantErr: "has no timestamp values", failed: true},
		{name: "SQL text", result: [][]interface{}{{"2026-03-29 11:00:00.123456"}}},
		{name: "bytes", result: [][]interface{}{{[]byte("2026-03-29 11:00:00")}}},
		{name: "offset across DST", result: [][]interface{}{{"2026-03-28T07:00:00+01:00"}}},
		{name: "invalid timestamp", result: [][]interface{}{{"not a timestamp"}}, wantErr: "failed to parse"},
		{name: "numeric column", result: [][]interface{}{{int64(123)}}, wantErr: "expected a timestamp"},
		{name: "missing result", wantErr: "expected a single"},
		{name: "extra result", result: [][]interface{}{{now}, {now}}, wantErr: "expected a single"},
		{name: "timeout", queryErr: context.DeadlineExceeded, wantErr: "context deadline exceeded"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			asset, err := pipeline.ConvertYamlToTask([]byte(`name: dataset.events
type: duckdb.sql
connection: test
columns:
  - name: retrieved_at
    type: timestamp
    checks:
      - name: freshness
        value: 30h
        blocking: false
        retries: 2
`))
			require.NoError(t, err)
			require.Len(t, asset.Columns[0].Checks, 1)
			check := &asset.Columns[0].Checks[0]
			require.False(t, check.Blocking.Bool())
			require.Equal(t, 2, *check.Retries)
			ti := &scheduler.ColumnCheckInstance{
				AssetInstance: &scheduler.AssetInstance{Asset: asset, Pipeline: &pipeline.Pipeline{Name: "test"}},
				Column:        &asset.Columns[0], Check: check,
			}
			q := new(mockQuerierWithResult)
			q.On("Select", mock.Anything, &query.Query{Query: `SELECT MAX("retrieved_at") FROM "dataset"."events"`}).Return(tt.result, tt.queryErr).Once()
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "test").Return(q).Once()
			runner := NewFreshnessCheck(conn, QuoteIdentifierWithDoubleQuotes)
			runner.now = func() time.Time { return now }
			err = NewColumnCheckOperator(map[string]CheckRunner{"freshness": runner}).Run(t.Context(), ti)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			if tt.failed {
				var failure *CheckError
				require.ErrorAs(t, err, &failure)
				require.Equal(t, ti.ExecutedQuery, failure.Query)
			}
			if tt.queryErr != nil {
				require.ErrorIs(t, err, tt.queryErr)
			}
			q.AssertExpectations(t)
			conn.AssertExpectations(t)
		})
	}
}

func TestFreshnessInvalidDuration(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "0s", "-1h", "1d", "invalid", "999999999999999999999h"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			check := &pipeline.ColumnCheck{Value: pipeline.ColumnCheckValue{String: &value}}
			err := NewFreshnessCheck(nil, QuoteIdentifierWithDoubleQuotes).Check(t.Context(), &scheduler.ColumnCheckInstance{Check: check})
			require.ErrorContains(t, err, "positive duration string")
		})
	}
	err := NewFreshnessCheck(nil, QuoteIdentifierWithDoubleQuotes).Check(t.Context(), &scheduler.ColumnCheckInstance{Check: &pipeline.ColumnCheck{}})
	require.ErrorContains(t, err, "positive duration string")
}
