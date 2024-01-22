package snowflake

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockQuerierWithResult struct {
	mock.Mock
}

func (m *mockQuerierWithResult) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	args := m.Called(ctx, q)
	get := args.Get(0)
	if get == nil {
		return nil, args.Error(1)
	}

	return get.([][]interface{}), args.Error(1)
}

func (m *mockQuerierWithResult) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	args := m.Called(ctx, query)
	return args.Error(0)
}

type mockConnectionFetcher struct {
	mock.Mock
}

func (m *mockConnectionFetcher) GetSfConnection(name string) (SfClient, error) {
	args := m.Called(name)
	get := args.Get(0)
	if get == nil {
		return nil, args.Error(1)
	}

	return get.(SfClient), args.Error(1)
}

func TestNotNullCheck_Check(t *testing.T) {
	t.Parallel()

	runTestsFoCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) testRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetSfConnection", "test").Return(q, nil)
			return &NotNullCheck{conn: conn}
		},
		"SELECT count(*) FROM dataset.test_asset WHERE test_column IS NULL",
		"column 'test_column' has 5 null values",
		&pipeline.ColumnCheck{
			Name: "not_null",
		},
	)
}

func TestPositiveCheck_Check(t *testing.T) {
	t.Parallel()

	runTestsFoCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) testRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetSfConnection", "test").Return(q, nil)
			return &PositiveCheck{conn: conn}
		},
		"SELECT count(*) FROM dataset.test_asset WHERE test_column <= 0",
		"column 'test_column' has 5 non-positive values",
		&pipeline.ColumnCheck{
			Name: "positive",
		},
	)
}

func TestUniqueCheck_Check(t *testing.T) {
	t.Parallel()

	runTestsFoCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) testRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetSfConnection", "test").Return(q, nil)
			return &UniqueCheck{conn: conn}
		},
		"SELECT COUNT(test_column) - COUNT(DISTINCT test_column) FROM dataset.test_asset",
		"column 'test_column' has 5 non-unique values",
		&pipeline.ColumnCheck{
			Name: "unique",
		},
	)
}

func TestAcceptedValuesCheck_Check(t *testing.T) {
	t.Parallel()

	runTestsFoCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) testRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetSfConnection", "test").Return(q, nil)
			return &AcceptedValuesCheck{conn: conn}
		},
		"SELECT COUNT(*) FROM dataset.test_asset WHERE CAST(test_column as STRING) NOT IN ('test','test2')",
		"column 'test_column' has 5 rows that are not in the accepted values",
		&pipeline.ColumnCheck{
			Name: "accepted_values",
			Value: pipeline.ColumnCheckValue{
				StringArray: &[]string{"test", "test2"},
			},
		},
	)

	runTestsFoCountZeroCheck(
		t,
		func(q *mockQuerierWithResult) testRunner {
			conn := new(mockConnectionFetcher)
			conn.On("GetSfConnection", "test").Return(q, nil)
			return &AcceptedValuesCheck{conn: conn}
		},
		"SELECT COUNT(*) FROM dataset.test_asset WHERE CAST(test_column as STRING) NOT IN ('1','2')",
		"column 'test_column' has 5 rows that are not in the accepted values",
		&pipeline.ColumnCheck{
			Name: "accepted_values",
			Value: pipeline.ColumnCheckValue{
				IntArray: &[]int{1, 2},
			},
		},
	)
}

func runTestsFoCountZeroCheck(t *testing.T, instanceBuilder func(q *mockQuerierWithResult) testRunner, expectedQueryString string, expectedErrorMessage string, checkInstance *pipeline.ColumnCheck) {
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := new(mockQuerierWithResult)
			tt.setup(q)

			n := instanceBuilder(q)

			testInstance := &scheduler.ColumnCheckInstance{
				AssetInstance: &scheduler.AssetInstance{
					Asset: &pipeline.Asset{
						Name: "dataset.test_asset",
						Type: pipeline.AssetTypeBigqueryQuery,
					},
					Pipeline: &pipeline.Pipeline{
						Name: "test",
						DefaultConnections: map[string]string{
							"google_cloud_platform": "test",
						},
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

			tt.wantErr(t, n.Check(context.Background(), testInstance))
			defer q.AssertExpectations(t)
		})
	}
}
