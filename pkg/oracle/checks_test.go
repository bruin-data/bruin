package oracle

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAcceptedValuesCheck_Check(t *testing.T) {
	t.Parallel()

	runTestsForCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) ansisql.CheckRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "oracle-default").Return(q, nil)
			return &AcceptedValuesCheck{conn: conn}
		},
		"SELECT COUNT(*) FROM dataset.test_asset WHERE CAST(test_column as VARCHAR2(4000)) NOT IN ('test','test2')",
		"column 'test_column' has 5 rows that are not in the accepted values",
		&pipeline.ColumnCheck{
			Name: "accepted_values",
			Value: pipeline.ColumnCheckValue{
				StringArray: &[]string{"test", "test2"},
			},
		},
	)

	runTestsForCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) ansisql.CheckRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "oracle-default").Return(q, nil)
			return &AcceptedValuesCheck{conn: conn}
		},
		"SELECT COUNT(*) FROM dataset.test_asset WHERE CAST(test_column as VARCHAR2(4000)) NOT IN ('1','2')",
		"column 'test_column' has 5 rows that are not in the accepted values",
		&pipeline.ColumnCheck{
			Name: "accepted_values",
			Value: pipeline.ColumnCheckValue{
				IntArray: &[]int{1, 2},
			},
		},
	)

	runTestsForCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) ansisql.CheckRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "oracle-default").Return(q, nil)
			return &AcceptedValuesCheck{conn: conn}
		},
		"SELECT COUNT(*) FROM dataset.test_asset WHERE CAST(test_column as VARCHAR2(4000)) NOT IN ('it''s','they''re')",
		"column 'test_column' has 5 rows that are not in the accepted values",
		&pipeline.ColumnCheck{
			Name: "accepted_values",
			Value: pipeline.ColumnCheckValue{
				StringArray: &[]string{"it's", "they're"},
			},
		},
	)
}

func runTestsForCountZeroCheck(t *testing.T, instanceBuilder func(q *mockQuerierWithResult) ansisql.CheckRunner, expectedQueryString string, expectedErrorMessage string, checkInstance *pipeline.ColumnCheck) {
	expectedQuery := &query.Query{Query: expectedQueryString}
	setupFunc := func(val [][]interface{}, err error) func(n *mockQuerierWithResult) {
		return func(q *mockQuerierWithResult) {
			q.On("Select", mock.Anything, expectedQuery).
				Return(val, err).
				Once()
		}
	}

	checkError := func(message string) assert.ErrorAssertionFunc {
		return func(t assert.TestingT, err error, i ...interface{}) bool {
			return assert.EqualError(t, err, message)
		}
	}

	tests := []struct {
		name    string
		setup   func(n *mockQuerierWithResult)
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "failed to run query",
			setup:   setupFunc(nil, assert.AnError),
			wantErr: assert.Error,
		},
		{
			name:    "multiple results are returned",
			setup:   setupFunc([][]interface{}{{1}, {2}}, nil),
			wantErr: assert.Error,
		},
		{
			name:    "null values found",
			setup:   setupFunc([][]interface{}{{5}}, nil),
			wantErr: checkError(expectedErrorMessage),
		},
		{
			name:    "null values found with int64 results",
			setup:   setupFunc([][]interface{}{{int64(5)}}, nil),
			wantErr: checkError(expectedErrorMessage),
		},
		{
			name:    "no null values found, test passed",
			setup:   setupFunc([][]interface{}{{0}}, nil),
			wantErr: assert.NoError,
		},
		{
			name:    "no null values found, result is a string, test passed",
			setup:   setupFunc([][]interface{}{{"0"}}, nil),
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := new(mockQuerierWithResult)
			tt.setup(q)

			n := instanceBuilder(q)

			testInstance := &scheduler.ColumnCheckInstance{
				AssetInstance: &scheduler.AssetInstance{
					Asset: &pipeline.Asset{
						Name: "dataset.test_asset",
						Type: pipeline.AssetTypeOracleQuery,
					},
					Pipeline: &pipeline.Pipeline{
						Name: "test",
					},
				},
				Column: &pipeline.Column{
					Name: "test_column",
					Checks: []pipeline.ColumnCheck{
						{
							Name: "not_null",
						},
					},
				},
				Check: checkInstance,
			}

			tt.wantErr(t, n.Check(t.Context(), testInstance))
			defer q.AssertExpectations(t)
		})
	}
}

func TestPatternCheck_Check(t *testing.T) {
	t.Parallel()

	pattern := "(a|b)"

	runTestsForCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) ansisql.CheckRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "oracle-default").Return(q, nil)
			return &PatternCheck{conn: conn}
		},
		"SELECT count(*) FROM dataset.test_asset WHERE NOT REGEXP_LIKE(test_column, '(a|b)')",
		"column test_column has 5 values that don't satisfy the pattern (a|b)",
		&pipeline.ColumnCheck{
			Name: "pattern",
			Value: pipeline.ColumnCheckValue{
				String: &pattern,
			},
		},
	)
}
