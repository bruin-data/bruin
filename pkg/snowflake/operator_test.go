package snowflake

import (
	"context"
	"testing"
	"time"

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
	res := m.Called(ctx, pipeline, asset)
	return res.Get(0).(query.QueryExtractor), res.Error(1)
}

func (m *mockExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	res := m.Called(content)
	return res.Get(0).([]string), res.Error(1)
}

type mockMaterializer struct {
	mock.Mock
}

func (m *mockMaterializer) Render(t *pipeline.Asset, query string) (string, error) {
	res := m.Called(t, query)
	return res.Get(0).(string), res.Error(1)
}

func (m *mockMaterializer) IsFullRefresh() bool {
	res := m.Called()
	return res.Bool(0)
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
					Return("select * from users", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeSnowflakeQuery,
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
					Return("select * from users", nil)
				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeSnowflakeQuery,
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
					Return("CREATE TABLE x AS select * from users", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeSnowflakeQuery,
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
			preExtractor := new(mockExtractor)
			preExtractor.On("ExtractQueriesFromString", mock.Anything).Return([]*query.Query{}, errors.New("should not be called"))
			preExtractor.On("CloneForAsset", mock.Anything, mock.Anything, mock.Anything).
				Return(extractor, nil)
			mat := new(mockMaterializer)
			conn := new(mockConnectionFetcher)
			mat.On("IsFullRefresh").Return(false)
			conn.On("GetConnection", mock.Anything).Return(client)
			client.On("CreateSchemaIfNotExist", mock.AnythingOfType("*pipeline.Asset"), mock.Anything).Return(nil)
			client.On("PushColumnDescriptions", mock.AnythingOfType("*pipeline.Asset"), mock.Anything).Return(nil)
			client.On("RecreateTableOnMaterializationTypeMismatch", mock.AnythingOfType("*pipeline.Asset"), mock.Anything).Return(nil)
			if tt.setup != nil {
				tt.setup(&fields{
					q: client,
					e: extractor,
					m: mat,
				})
			}

			o := BasicOperator{
				connection:   conn,
				extractor:    preExtractor,
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

func TestBasicOperator_RunTask_WarehouseOverride(t *testing.T) {
	t.Parallel()

	newAsset := func() *pipeline.Asset {
		return &pipeline.Asset{
			Type: pipeline.AssetTypeSnowflakeQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Path:    "test-file.sql",
				Content: "some content",
			},
			Parameters: pipeline.ParameterMap{"warehouse": "BIG_WH"},
		}
	}

	setupExtractorAndMaterializer := func(e *mockExtractor, m *mockMaterializer) {
		e.On("ExtractQueriesFromString", "some content").
			Return([]*query.Query{{Query: "select * from users"}}, nil)
		m.On("Render", mock.Anything, "select * from users").
			Return("select * from users", nil)
		m.On("IsFullRefresh").Return(false)
	}

	newExtractors := func() (*mockExtractor, *mockExtractor, *mockMaterializer) {
		extractor := new(mockExtractor)
		preExtractor := new(mockExtractor)
		preExtractor.On("CloneForAsset", mock.Anything, mock.Anything, mock.Anything).Return(extractor, nil)
		mat := new(mockMaterializer)
		setupExtractorAndMaterializer(extractor, mat)
		return preExtractor, extractor, mat
	}

	t.Run("runs on the overridden warehouse when reachable", func(t *testing.T) {
		t.Parallel()

		defaultClient := new(mockQuerierWithResult)
		overrideClient := new(mockQuerierWithResult)
		overrideClient.On("Ping", mock.Anything).Return(nil)
		overrideClient.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).Return(nil)

		preExtractor, _, mat := newExtractors()

		conn := new(mockConnectionFetcher)
		conn.On("GetConnection", mock.Anything).Return(defaultClient)
		conn.On("GetSfConnectionWithWarehouse", mock.Anything, "BIG_WH").Return(overrideClient, nil)

		o := BasicOperator{connection: conn, extractor: preExtractor, materializer: mat}
		require.NoError(t, o.RunTask(t.Context(), &pipeline.Pipeline{}, newAsset()))

		overrideClient.AssertExpectations(t)
		defaultClient.AssertNotCalled(t, "RunQueryWithoutResult", mock.Anything, mock.Anything)
	})

	t.Run("falls back to the default warehouse when the override is unreachable", func(t *testing.T) {
		t.Parallel()

		defaultClient := new(mockQuerierWithResult)
		defaultClient.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).Return(nil)
		overrideClient := new(mockQuerierWithResult)
		overrideClient.On("Ping", mock.Anything).Return(errors.New("account suspended"))

		preExtractor, _, mat := newExtractors()

		conn := new(mockConnectionFetcher)
		conn.On("GetConnection", mock.Anything).Return(defaultClient)
		conn.On("GetSfConnectionWithWarehouse", mock.Anything, "BIG_WH").Return(overrideClient, nil)

		o := BasicOperator{connection: conn, extractor: preExtractor, materializer: mat}
		require.NoError(t, o.RunTask(t.Context(), &pipeline.Pipeline{}, newAsset()))

		overrideClient.AssertCalled(t, "Ping", mock.Anything)
		overrideClient.AssertNotCalled(t, "RunQueryWithoutResult", mock.Anything, mock.Anything)
		defaultClient.AssertExpectations(t)
	})
}

func TestQuerySensorTimesOutWhenConfigured(t *testing.T) {
	t.Parallel()

	client := new(mockQuerierWithResult)
	extractor := new(mockExtractor)
	preExtractor := new(mockExtractor)
	conn := new(mockConnectionFetcher)

	preExtractor.On("CloneForAsset", mock.Anything, mock.Anything, mock.Anything).
		Return(extractor, nil)
	extractor.On("ExtractQueriesFromString", "select 1").
		Return([]*query.Query{{Query: "select 1"}}, nil)
	conn.On("GetConnection", mock.Anything).Return(client)
	// Always return 0 so the sensor never succeeds.
	client.On("SelectOnlyLastResult", mock.Anything, mock.Anything).Return([][]interface{}{{int64(0)}}, nil)

	sensor := NewQuerySensor(conn, preExtractor, 0)

	start := time.Now()
	err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeSnowflakeQuerySensor,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "select 1",
		},
		Parameters: pipeline.ParameterMap{
			"query":   "select 1",
			"timeout": "100ms",
		},
	})
	elapsed := time.Since(start)

	require.ErrorContains(t, err, "Sensor timed out after")
	assert.Less(t, elapsed, 5*time.Second, "timeout should fire promptly")
}
