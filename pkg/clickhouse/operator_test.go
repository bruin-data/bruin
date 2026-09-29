package clickhouse

import (
	"context"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type clusterQuerier struct {
	mockQuerierWithResult
	cluster string
}

func (c *clusterQuerier) GetCluster() string {
	return c.cluster
}

func TestBasicOperatorClusterPerConnection(t *testing.T) {
	t.Parallel()
	extractor := new(mockExtractor)
	extractor.On("ExtractQueriesFromString", "SELECT 1 AS id").
		Return([]*query.Query{{Query: "SELECT 1 AS id"}}, nil)
	connections := new(mockConnectionFetcher)
	operator := NewBasicOperator(connections, extractor, false, nil, nil)
	operator.devEnv = nil
	pl := &pipeline.Pipeline{}
	for _, cluster := range []string{"analytics", "", "replicas"} {
		client := &clusterQuerier{cluster: cluster}
		connections.On("GetConnection", cluster+"_connection").Return(client).Once()
		pre := client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "SELECT 'pre';"}).Return(nil).Once()
		main := client.On("RunQueryWithoutResult", mock.Anything, mock.MatchedBy(func(q *query.Query) bool {
			if !strings.HasPrefix(q.Query, "CREATE OR REPLACE VIEW warehouse.events") || !strings.Contains(q.Query, "SELECT 1 AS id") {
				return false
			}
			if cluster == "" {
				return !strings.Contains(q.Query, "ON CLUSTER")
			}
			return strings.Contains(q.Query, "ON CLUSTER") && strings.Contains(q.Query, cluster)
		})).Return(nil).Once().NotBefore(pre)
		client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "SELECT 'post';"}).Return(nil).Once().NotBefore(main)
		asset := &pipeline.Asset{
			Name:       "warehouse.events",
			Type:       pipeline.AssetTypeClickHouse,
			Connection: cluster + "_connection",
			Materialization: pipeline.Materialization{
				Type: pipeline.MaterializationTypeView,
			},
			ExecutableFile: pipeline.ExecutableFile{Content: "SELECT 1 AS id"},
			Hooks: pipeline.Hooks{
				Pre:  []pipeline.Hook{{Query: "SELECT 'pre'"}},
				Post: []pipeline.Hook{{Query: "SELECT 'post'"}},
			},
		}
		require.NoError(t, operator.RunTask(t.Context(), pl, asset))
		client.AssertExpectations(t)
	}
	connections.AssertExpectations(t)
	extractor.AssertExpectations(t)
}

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

type mockMaterializerWithCleanup struct {
	mock.Mock
}

func (m *mockMaterializerWithCleanup) Render(t *pipeline.Asset, query string) ([]string, error) {
	res := m.Called(t, query)
	return res.Get(0).([]string), res.Error(1)
}

func (m *mockMaterializerWithCleanup) RenderWithCleanup(t *pipeline.Asset, query string) ([]string, []string, error) {
	res := m.Called(t, query)
	return res.Get(0).([]string), res.Get(1).([]string), res.Error(2)
}

func (m *mockMaterializerWithCleanup) LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error {
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
				f.e.On("ExtractQueriesFromString", "some query").
					Return([]*query.Query{}, errors.New("failed to extract queries"))
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no queries found in file",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some query").
					Return([]*query.Query{}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple queries found but materialization is enabled, should fail",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some query").
					Return([]*query.Query{
						{Query: "query 1"},
						{Query: "query 2"},
					}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
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
				f.e.On("ExtractQueriesFromString", "some query").
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
					Type: pipeline.AssetTypePostgresQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query successfully executed",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some query").
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
					Type: pipeline.AssetTypePostgresQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query successfully executed with materialization",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some query").
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
					Type: pipeline.AssetTypePostgresQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
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
			conn.On("GetConnection", mock.Anything).Return(client)

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

func TestBasicOperator_RunTaskCleansUpAfterQueryFailure(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeClickHouse,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some query",
		},
	}
	client := new(mockQuerierWithResult)
	extractor := new(mockExtractor)
	mat := new(mockMaterializerWithCleanup)
	conn := new(mockConnectionFetcher)

	extractor.On("ExtractQueriesFromString", "some query").
		Return([]*query.Query{{Query: "select * from users"}}, nil)
	mat.On("RenderWithCleanup", asset, "select * from users").
		Return(
			[]string{"CREATE TABLE temp", "INSERT INTO target SELECT * FROM temp"},
			[]string{"DROP TABLE IF EXISTS temp"},
			nil,
		)
	conn.On("GetConnection", mock.Anything).Return(client)
	createCall := client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE temp"}).
		Return(nil).
		Once()
	insertCall := client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "INSERT INTO target SELECT * FROM temp"}).
		Return(errors.New("insert failed")).
		Once().
		NotBefore(createCall)
	client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "DROP TABLE IF EXISTS temp"}).
		Return(nil).
		Once().
		NotBefore(insertCall)

	o := BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: mat,
	}

	err := o.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.EqualError(t, err, "insert failed")
	client.AssertExpectations(t)
}
