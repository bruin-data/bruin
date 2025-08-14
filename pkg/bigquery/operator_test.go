package bigquery

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockExtractor struct {
	mock.Mock
	renderer *jinja.Renderer
}

func (m *mockExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	// Check if the renderer is set
	if m.renderer != nil {
		// Use the renderer to render the content
		renderedQuery, err := m.renderer.Render(content)
		if err != nil {
			return nil, err
		}
		// Return the rendered query wrapped in a query.Query struct
		return []*query.Query{
			{Query: renderedQuery},
		}, nil
	}

	// Fallback to the mock behavior if renderer is not set
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

				f.m.On("LogIfFullRefreshAndDDL", mock.Anything, mock.Anything).
					Return(nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
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
				f.m.On("LogIfFullRefreshAndDDL", mock.Anything, mock.Anything).
					Return(nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
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
				f.m.On("IsFullRefresh").Return(false)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
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
			mat.On("IsFullRefresh").Return(false)
			client.On("CreateDataSetIfNotExist", mock.AnythingOfType("*pipeline.Asset"), mock.Anything).Return(nil)
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "gcp-default").Return(client)
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

			err := o.RunTask(context.Background(), &pipeline.Pipeline{}, tt.args.t)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMetadataPushOperator_Run(t *testing.T) {
	t.Parallel()

	type fields struct {
		q *mockQuerierWithResult
	}

	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
	}

	tests := []struct {
		name    string
		setup   func(f *fields)
		t       *pipeline.Asset
		wantErr bool
	}{
		{
			name: "no metadata to push",
			setup: func(f *fields) {
				f.q.On("UpdateTableMetadataIfNotExist", mock.Anything, asset).
					Return(NoMetadataUpdatedError{})
			},
			t:       asset,
			wantErr: false,
		},
		{
			name: "other errors are propagated",
			setup: func(f *fields) {
				f.q.On("UpdateTableMetadataIfNotExist", mock.Anything, asset).
					Return(errors.New("something failed"))
			},
			t:       asset,
			wantErr: true,
		},
		{
			name: "metadata is pushed successfully",
			setup: func(f *fields) {
				f.q.On("UpdateTableMetadataIfNotExist", mock.Anything, asset).
					Return(nil)
			},
			t:       asset,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := new(mockQuerierWithResult)
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "gcp-default").Return(client)

			if tt.setup != nil {
				tt.setup(&fields{
					q: client,
				})
			}

			o := MetadataPushOperator{
				connection: conn,
			}

			taskInstance := scheduler.AssetInstance{Asset: tt.t, Pipeline: &pipeline.Pipeline{}}

			ctx := context.WithValue(context.Background(), executor.KeyPrinter, io.Discard)

			err := o.Run(ctx, &taskInstance)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBasicOperator_RunTask_WithRenderer(t *testing.T) {
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
			name: "query successfully executed with rendering",
			setup: func(f *fields) {
				f.m.On("Render", mock.Anything, "SELECT 1").
					Return("CREATE TABLE x AS SELECT 1", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS SELECT 1"}).
					Return(nil)
				f.m.On("IsFullRefresh").Return(false)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "{%- set sinks = [\n  'instant_cash',\n] -%}\n\nSELECT 1",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query successfully executed with rendering 2",
			setup: func(f *fields) {
				f.m.On("Render", mock.Anything, "SELECT 1").
					Return("CREATE TABLE x AS SELECT 1", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS SELECT 1"}).
					Return(nil)
				f.m.On("IsFullRefresh").Return(false)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "{%- set sinks = [\n  'instant_cash',\n] -%}\n\nSELECT 1",
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
			extractor.renderer = jinja.NewRenderer(jinja.Context{})
			mat := new(mockMaterializer)
			mat.On("IsFullRefresh").Return(false)
			client.On("CreateDataSetIfNotExist", mock.AnythingOfType("*pipeline.Asset"), mock.Anything).Return(nil)
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "gcp-default").Return(client)
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

			err := o.RunTask(context.Background(), &pipeline.Pipeline{}, tt.args.t)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

type MockConnectionGetter struct {
	mock.Mock
}

func (m *MockConnectionGetter) GetConnection(name string) any {
	res := m.Called(name)
	return res.Get(0)
}

type MockQueryExtractor struct {
	mock.Mock
}

type MockSensorDB struct {
	mock.Mock
}

func (m *MockSensorDB) BuildTableExistsQuery(tableName string) (string, error) {
	res := m.Called(tableName)
	return res.Get(0).(string), res.Error(1)
}

func (m *MockSensorDB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	res := m.Called(ctx, query)
	return res.Get(0).([][]interface{}), res.Error(1)
}

func (m *MockQueryExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func (m *MockQueryExtractor) CloneForAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) (query.QueryExtractor, error) {
	res := m.Called(ctx, pipeline, asset)
	return res.Get(0).(query.QueryExtractor), res.Error(1)
}

func (m *MockQueryExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	res := m.Called(content)
	return res.Get(0).([]string), res.Error(1)
}

func TestNewTableSensorModeSkip(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "skip",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
	})

	assert.Nil(t, err)
}

func TestNewTableSensorModeNoTable(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "wait",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
	})

	assert.ErrorContains(t, err, "table sensor requires a parameter named 'table'")
}

func TestNewTableSensorNoConnectionForAsset(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "wait",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeIngestr,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test_table",
		},
	})

	assert.ErrorContains(t, err, "connection type could not be inferred for destination")
}

func TestNewTableSensorConnectionNotFound(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	// Return nil to simulate connection not found
	mockConn.On("GetConnection", "gcp-default").Return(nil)

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "wait",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test_table",
		},
	})

	assert.ErrorContains(t, err, "either does not exist or is not a bigquery connection")
}

func TestNewTableSensorBuildTableExistsQueryError(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}
	mockDb := &MockSensorDB{}

	mockConn.On("GetConnection", "gcp-default").Return(mockDb)

	mockDb.On("BuildTableExistsQuery", "test.table.extraneous").Return("", fmt.Errorf("table name must be in dataset.table or project.dataset.table format, 'test.table.extraneous' given"))

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "wait",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test.table.extraneous",
		},
	})

	assert.ErrorContains(t, err, "table name must be in dataset.table or project.dataset.table format")
}

func TestNewTableSensorExtractQueriesFromStringError(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}
	mockDb := &MockSensorDB{}

	mockConn.On("GetConnection", "gcp-default").Return(mockDb)

	// BuildTableExistsQuery succeeds
	mockDb.On("BuildTableExistsQuery", "test.table").Return("SELECT", nil)

	// ExtractQueriesFromString fails
	mockExtractor.On("ExtractQueriesFromString", "SELECT").Return([]*query.Query{}, errors.New("could not render file while extracting the queries with the split query extractor"))

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "wait",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "SELECT",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test.table",
		},
	})

	assert.ErrorContains(t, err, "could not render file while extracting")
}

func TestNewTableSensorSelectError(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}
	mockDb := &MockSensorDB{}

	mockConn.On("GetConnection", "gcp-default").Return(mockDb)

	// BuildTableExistsQuery succeeds
	mockDb.On("BuildTableExistsQuery", "test.table").Return("SELECT * FROM test.data", nil)

	// ExtractQueriesFromString succeeds
	mockExtractor.On("ExtractQueriesFromString", "SELECT * FROM test.data").Return([]*query.Query{
		{Query: "SELECT * FROM test.data"},
	}, nil)

	// Select fails
	mockDb.On("Select", mock.Anything, mock.MatchedBy(func(q *query.Query) bool {
		return q.Query == "SELECT * FROM test.data"
	})).Return([][]interface{}{{1}}, nil)

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "wait",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "SELECT * FROM test.data",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test.table",
		},
	})

	assert.Nil(t, err, "")
}


func TestNewTableSensorTestPoking(t *testing.T) {
	t.Parallel()
	var output strings.Builder
    ctx := context.WithValue(context.Background(), executor.KeyPrinter, &output)

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}
	mockDb := &MockSensorDB{}

	mockConn.On("GetConnection", "gcp-default").Return(mockDb)

	// BuildTableExistsQuery succeeds
	mockDb.On("BuildTableExistsQuery", "test.table").Return("SELECT * FROM test.data", nil)

	// ExtractQueriesFromString succeeds
	mockExtractor.On("ExtractQueriesFromString", "SELECT * FROM test.data").Return([]*query.Query{
		{Query: "SELECT * FROM test.data"},
	}, nil)

	// Select fails
	mockDb.On("Select", mock.Anything, mock.MatchedBy(func(q *query.Query) bool {
		return q.Query == "SELECT * FROM test.data"
	})).Return([][]interface{}{{1}}, nil)

	ts := TableSensor{
		connection: mockConn,
		sensorMode: "wait",
		extractor:  mockExtractor,
	}

	err := ts.RunTask(ctx, &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "SELECT * FROM test.data",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test.table",
		},
	})

	assert.Nil(t, err)
	assert.Contains(t, output.String(), "Poking: test.table")
}

func TestNewTableSensor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sensorMode     string
		assetType      pipeline.AssetType
		parameters     pipeline.EmptyStringMap
		setupMocks     func(*MockConnectionGetter, *MockQueryExtractor, *MockSensorDB)
		expectedError  string
		expectedOutput string
	}{
		{
			name:       "skip mode should return nil",
			sensorMode: "skip",
			assetType:  pipeline.AssetTypeBigqueryQuery,
			setupMocks: func(mockConn *MockConnectionGetter, mockExtractor *MockQueryExtractor, mockDb *MockSensorDB) {
				// No mocks needed for skip mode
			},
			expectedError: "",
		},
		{
			name:       "missing table parameter should error",
			sensorMode: "wait",
			assetType:  pipeline.AssetTypeBigqueryQuery,
			setupMocks: func(mockConn *MockConnectionGetter, mockExtractor *MockQueryExtractor, mockDb *MockSensorDB) {
				// No mocks needed - fails before any DB calls
			},
			expectedError: "table sensor requires a parameter named 'table'",
		},
		{
			name:       "no connection for asset should error",
			sensorMode: "wait",
			assetType:  pipeline.AssetTypeIngestr,
			parameters: pipeline.EmptyStringMap{"table": "test_table"},
			setupMocks: func(mockConn *MockConnectionGetter, mockExtractor *MockQueryExtractor, mockDb *MockSensorDB) {
				// No mocks needed - fails at pipeline level
			},
			expectedError: "connection type could not be inferred for destination",
		},
		{
			name:       "connection not found should error",
			sensorMode: "wait",
			assetType:  pipeline.AssetTypeBigqueryQuery,
			parameters: pipeline.EmptyStringMap{"table": "test_table"},
			setupMocks: func(mockConn *MockConnectionGetter, mockExtractor *MockQueryExtractor, mockDb *MockSensorDB) {
				mockConn.On("GetConnection", "gcp-default").Return(nil)
			},
			expectedError: "either does not exist or is not a bigquery connection",
		},
		{
			name:       "BuildTableExistsQuery error should propagate",
			sensorMode: "wait",
			assetType:  pipeline.AssetTypeBigqueryQuery,
			parameters: pipeline.EmptyStringMap{"table": "test.table.extraneous"},
			setupMocks: func(mockConn *MockConnectionGetter, mockExtractor *MockQueryExtractor, mockDb *MockSensorDB) {
				mockConn.On("GetConnection", "gcp-default").Return(mockDb)
				mockDb.On("BuildTableExistsQuery", "test.table.extraneous").Return("", 
					fmt.Errorf("table name must be in dataset.table or project.dataset.table format, 'test.table.extraneous' given"))
			},
			expectedError: "table name must be in dataset.table or project.dataset.table format",
		},
		{
			name:       "ExtractQueriesFromString error should propagate",
			sensorMode: "wait",
			assetType:  pipeline.AssetTypeBigqueryQuery,
			parameters: pipeline.EmptyStringMap{"table": "test.table"},
			setupMocks: func(mockConn *MockConnectionGetter, mockExtractor *MockQueryExtractor, mockDb *MockSensorDB) {
				mockConn.On("GetConnection", "gcp-default").Return(mockDb)
				mockDb.On("BuildTableExistsQuery", "test.table").Return("SELECT", nil)
				mockExtractor.On("ExtractQueriesFromString", "SELECT").Return([]*query.Query{}, 
					errors.New("could not render file while extracting the queries with the split query extractor"))
			},
			expectedError: "could not render file while extracting",
		},
		{
			name:       "successful table sensor should print poking and return nil",
			sensorMode: "wait",
			assetType:  pipeline.AssetTypeBigqueryQuery,
			parameters: pipeline.EmptyStringMap{"table": "test.table"},
			setupMocks: func(mockConn *MockConnectionGetter, mockExtractor *MockQueryExtractor, mockDb *MockSensorDB) {
				mockConn.On("GetConnection", "gcp-default").Return(mockDb)
				mockDb.On("BuildTableExistsQuery", "test.table").Return("SELECT * FROM test.data", nil)
				mockExtractor.On("ExtractQueriesFromString", "SELECT * FROM test.data").Return([]*query.Query{
					{Query: "SELECT * FROM test.data"},
				}, nil)
				mockDb.On("Select", mock.Anything, mock.MatchedBy(func(q *query.Query) bool {
					return q.Query == "SELECT * FROM test.data"
				})).Return([][]interface{}{{1}}, nil)
			},
			expectedError:  "",
			expectedOutput: "Poking: test.table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup mocks
			mockConn := &MockConnectionGetter{}
			mockExtractor := &MockQueryExtractor{}
			mockDb := &MockSensorDB{}

			// Setup context with output capture if needed
			var ctx context.Context
			if tt.expectedOutput != "" {
				var output strings.Builder
				ctx = context.WithValue(context.Background(), executor.KeyPrinter, &output)
			} else {
				ctx = context.Background()
			}

			// Setup mocks based on test case
			tt.setupMocks(mockConn, mockExtractor, mockDb)

			// Create TableSensor
			ts := TableSensor{
				connection: mockConn,
				sensorMode: tt.sensorMode,
				extractor:  mockExtractor,
			}

			// Create asset
			asset := &pipeline.Asset{
				Type: tt.assetType,
				ExecutableFile: pipeline.ExecutableFile{
					Path:    "test-file.sql",
					Content: "some content",
				},
			}

			// Add parameters if specified
			if tt.parameters != nil {
				asset.Parameters = tt.parameters
			}

			// Run the test
			err := ts.RunTask(ctx, &pipeline.Pipeline{}, asset)

			// Assertions
			if tt.expectedError != "" {
				assert.ErrorContains(t, err, tt.expectedError)
			} else {
				assert.Nil(t, err)
			}

			// Check output if expected
			if tt.expectedOutput != "" {
				if output, ok := ctx.Value(executor.KeyPrinter).(*strings.Builder); ok {
					assert.Contains(t, output.String(), tt.expectedOutput)
				}
			}
		})
	}
}