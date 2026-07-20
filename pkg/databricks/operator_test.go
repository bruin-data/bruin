package databricks

import (
	"context"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func (m *mockExtractor) CloneForAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) (query.QueryExtractor, error) {
	return m, nil
}

func (m *mockExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	res := m.Called(content)
	return res.Get(0).([]string), res.Error(1)
}

type mockMaterializer struct {
	mock.Mock
}

func (m *mockMaterializer) Render(t *pipeline.Asset, query string) ([]string, error) {
	res := m.Called(t, query)
	return res.Get(0).([]string), res.Error(1)
}

func (m *mockMaterializer) LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error {
	return nil
}

func TestBasicOperator_RunTask(t *testing.T) {
	t.Parallel()

	type args struct {
		t *pipeline.Asset
	}

	type fields struct {
		q *mockQuerierWithResult
		e *mockExtractor
		m *mockMaterializer
	}

	tests := []struct {
		name              string
		setup             func(f *fields)
		setupQueries      func(m *mockQuerierWithResult)
		setupExtractor    func(m *mockExtractor)
		setupMaterializer func(m *mockMaterializer)
		args              args
		wantErr           bool
	}{
		{
			name: "failed to extract queries",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{}, errors.New("failed to extract queries"))
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no queries found in file",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple queries found but materialization is enabled, should fail",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "query 1"},
						{Query: "query 2"},
					}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query returned an error",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)

				f.m.On("Render", mock.Anything, "select * from users").
					Return([]string{"select * from users"}, nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeDatabricksQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query successfully executed",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)

				f.m.On("Render", mock.Anything, "select * from users").
					Return([]string{"select * from users"}, nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeDatabricksQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query successfully executed with materialization",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)

				f.m.On("Render", mock.Anything, "select * from users").
					Return([]string{"CREATE TABLE x AS select * from users"}, nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeDatabricksQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := new(mockQuerierWithResult)
			extractor := new(mockExtractor)
			mat := new(mockMaterializer)
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "databricks-default").Return(client)

			if tt.setup != nil {
				tt.setup(&fields{
					q: client,
					e: extractor,
					m: mat,
				})
			}

			o := BasicOperator{
				connection:   conn,
				extractor:    extractor,
				materializer: mat,
			}

			err := o.RunTask(t.Context(), &pipeline.Pipeline{}, tt.args.t)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBasicOperator_QueryAnnotations_Default(t *testing.T) {
	t.Parallel()

	client := new(mockQuerierWithResult)
	extractor := new(mockExtractor)
	mat := new(mockMaterializer)

	extractor.On("ExtractQueriesFromString", "SELECT * FROM users").
		Return([]*query.Query{{Query: "SELECT * FROM users"}}, nil)
	mat.On("Render", mock.Anything, "SELECT * FROM users").
		Return([]string{"SELECT * FROM users"}, nil)

	var executedQuery *query.Query
	client.On("RunQueryWithoutResult", mock.Anything, mock.AnythingOfType("*query.Query")).
		Run(func(args mock.Arguments) {
			executedQuery = args.Get(1).(*query.Query)
		}).
		Return(nil)

	conn := new(mockConnectionFetcher)
	conn.On("GetConnection", "databricks-default").Return(client)

	o := BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: mat,
	}
	asset := &pipeline.Asset{
		Name: "test_asset",
		Type: pipeline.AssetTypeDatabricksQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "SELECT * FROM users",
		},
	}
	ctx := context.WithValue(t.Context(), pipeline.RunConfigQueryAnnotations, ansisql.DefaultQueryAnnotations)

	err := o.RunTask(ctx, &pipeline.Pipeline{Name: "test_pipeline"}, asset)

	require.NoError(t, err)
	require.NotNil(t, executedQuery)
	expectedComment := `-- @bruin.config: {"asset":"test_asset","pipeline":"test_pipeline","type":"main"}`
	assert.True(t, strings.HasPrefix(executedQuery.Query, expectedComment))
	assert.Contains(t, executedQuery.Query, "SELECT * FROM users")
	assert.Equal(t, map[string]string{"asset": "test_asset", "pipeline": "test_pipeline", "type": "main"}, executedQuery.Annotations)
}

func TestBasicOperator_QueryAnnotations_CustomJSON(t *testing.T) {
	t.Parallel()

	client := new(mockQuerierWithResult)
	extractor := new(mockExtractor)
	mat := new(mockMaterializer)

	extractor.On("ExtractQueriesFromString", "SELECT * FROM orders").
		Return([]*query.Query{{Query: "SELECT * FROM orders"}}, nil)
	mat.On("Render", mock.Anything, "SELECT * FROM orders").
		Return([]string{"SELECT * FROM orders"}, nil)

	var executedQuery *query.Query
	client.On("RunQueryWithoutResult", mock.Anything, mock.AnythingOfType("*query.Query")).
		Run(func(args mock.Arguments) {
			executedQuery = args.Get(1).(*query.Query)
		}).
		Return(nil)

	conn := new(mockConnectionFetcher)
	conn.On("GetConnection", "databricks-default").Return(client)

	o := BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: mat,
	}
	asset := &pipeline.Asset{
		Name: "orders_asset",
		Type: pipeline.AssetTypeDatabricksQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "orders.sql",
			Content: "SELECT * FROM orders",
		},
	}
	customAnnotations := `{"environment":"test","owner":"data_team","version":"1.0"}`
	ctx := context.WithValue(t.Context(), pipeline.RunConfigQueryAnnotations, customAnnotations)

	err := o.RunTask(ctx, &pipeline.Pipeline{Name: "orders_pipeline"}, asset)

	require.NoError(t, err)
	require.NotNil(t, executedQuery)
	assert.True(t, strings.HasPrefix(executedQuery.Query, "-- @bruin.config:"))
	assert.Contains(t, executedQuery.Query, `"asset":"orders_asset"`)
	assert.Contains(t, executedQuery.Query, `"pipeline":"orders_pipeline"`)
	assert.Contains(t, executedQuery.Query, `"type":"main"`)
	assert.Contains(t, executedQuery.Query, `"environment":"test"`)
	assert.Contains(t, executedQuery.Query, `"owner":"data_team"`)
	assert.Contains(t, executedQuery.Query, `"version":"1.0"`)
	assert.Contains(t, executedQuery.Query, "SELECT * FROM orders")
	assert.Equal(t, map[string]string{
		"asset":       "orders_asset",
		"environment": "test",
		"owner":       "data_team",
		"pipeline":    "orders_pipeline",
		"type":        "main",
		"version":     "1.0",
	}, executedQuery.Annotations)
}

func TestBasicOperator_QueryAnnotations_EachRenderedQuery(t *testing.T) {
	t.Parallel()

	client := new(mockQuerierWithResult)
	extractor := new(mockExtractor)
	mat := new(mockMaterializer)

	extractor.On("ExtractQueriesFromString", "SELECT * FROM users").
		Return([]*query.Query{{Query: "SELECT * FROM users"}}, nil)
	mat.On("Render", mock.Anything, "SELECT * FROM users").
		Return([]string{"SET spark.sql.shuffle.partitions = 8", "SELECT * FROM users"}, nil)

	executedQueries := make([]*query.Query, 0, 2)
	client.On("RunQueryWithoutResult", mock.Anything, mock.AnythingOfType("*query.Query")).
		Run(func(args mock.Arguments) {
			executedQueries = append(executedQueries, args.Get(1).(*query.Query))
		}).
		Return(nil).
		Twice()

	conn := new(mockConnectionFetcher)
	conn.On("GetConnection", "databricks-default").Return(client)
	o := BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: mat,
	}
	asset := &pipeline.Asset{
		Name: "test_asset",
		Type: pipeline.AssetTypeDatabricksQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Content: "SELECT * FROM users",
		},
	}
	ctx := context.WithValue(t.Context(), pipeline.RunConfigQueryAnnotations, ansisql.DefaultQueryAnnotations)

	err := o.RunTask(ctx, &pipeline.Pipeline{Name: "test_pipeline"}, asset)

	require.NoError(t, err)
	require.Len(t, executedQueries, 2)
	for _, executedQuery := range executedQueries {
		assert.True(t, strings.HasPrefix(executedQuery.Query, `-- @bruin.config: {"asset":"test_asset","pipeline":"test_pipeline","type":"main"}`))
		assert.Equal(t, map[string]string{"asset": "test_asset", "pipeline": "test_pipeline", "type": "main"}, executedQuery.Annotations)
	}
	assert.Contains(t, executedQueries[0].Query, "SET spark.sql.shuffle.partitions = 8")
	assert.Contains(t, executedQueries[1].Query, "SELECT * FROM users")
}

func TestBasicOperator_SetsMainQueryType(t *testing.T) {
	t.Parallel()

	client := new(mockQuerierWithResult)
	extractor := new(mockExtractor)
	mat := new(mockMaterializer)

	extractor.On("ExtractQueriesFromString", "SELECT 1").
		Return([]*query.Query{{Query: "SELECT 1"}}, nil)
	mat.On("Render", mock.Anything, "SELECT 1").
		Return([]string{"SELECT 1"}, nil)
	client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "SELECT 1"}).
		Run(func(args mock.Arguments) {
			ctx := args.Get(0).(context.Context)
			assert.Equal(t, query.QueryTypeMain, query.QueryTypeFromContext(ctx))
		}).
		Return(nil)

	conn := new(mockConnectionFetcher)
	conn.On("GetConnection", "databricks-default").Return(client)
	o := BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: mat,
	}
	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeDatabricksQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Content: "SELECT 1",
		},
	}

	err := o.RunTask(t.Context(), &pipeline.Pipeline{}, asset)

	require.NoError(t, err)
	client.AssertExpectations(t)
}
