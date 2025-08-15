package ansisql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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

type MockTableExistsChecker struct {
	mock.Mock
}

func (m *MockTableExistsChecker) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	res := m.Called(ctx, query)
	return res.Get(0).([][]interface{}), res.Error(1)
}

func (m *MockTableExistsChecker) BuildTableExistsQuery(tableName string) (string, error) {
	res := m.Called(tableName)
	return res.Get(0).(string), res.Error(1)
}

func TestNewTableSensorModeSkip(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	ts := NewTableSensor(mockConn, "skip", mockExtractor)

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

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

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

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

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

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

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

	assert.ErrorContains(t, err, "does not exist")
}

func TestNewTableSensorNoTableExistsChecker(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return("")

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test.table",
		},
	})

	assert.ErrorContains(t, err, "does not implement TableExistsChecker interface")
}

func TestNewTableSensorBuildTableExistsQueryError(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockTableExistsChecker := &MockTableExistsChecker{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return(mockTableExistsChecker)
	mockTableExistsChecker.On("BuildTableExistsQuery", "test.table").Return("", fmt.Errorf("failed to build table exists query"))

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "test.table",
		},
	})

	assert.ErrorContains(t, err, "failed to build table exists query")
}

func TestNewTableSensorExtractQueriesFromStringError(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockTableExistsChecker := &MockTableExistsChecker{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return(mockTableExistsChecker)
	mockTableExistsChecker.On("BuildTableExistsQuery", "database.test.table").Return("SELECT 1", nil)
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{}, fmt.Errorf("failed to extract queries from string"))

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "database.test.table",
		},
	})

	assert.ErrorContains(t, err, "failed to extract queries from string")
}

func TestNewTableSensorExtractQueriesFromStringNoQueries(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockTableExistsChecker := &MockTableExistsChecker{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return(mockTableExistsChecker)
	mockTableExistsChecker.On("BuildTableExistsQuery", "database.test.table").Return("SELECT 1", nil)
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{}, nil)

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.EmptyStringMap{
			"table": "database.test.table",
		},
	})

	assert.ErrorContains(t, err, "no queries extracted from table exists query")
}

// func TestNewTableSensorExtractQueriesFromStringError(t *testing.T) {
// 	t.Parallel()

// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockDb := &MockSensorDB{}

// 	mockConn.On("GetConnection", "gcp-default").Return(mockDb)

// 	// BuildTableExistsQuery succeeds
// 	mockDb.On("BuildTableExistsQuery", "test.table").Return("SELECT", nil)

// 	// ExtractQueriesFromString fails
// 	mockExtractor.On("ExtractQueriesFromString", "SELECT").Return([]*query.Query{}, errors.New("could not render file while extracting the queries with the split query extractor"))

// 	ts := TableSensor{
// 		connection: mockConn,
// 		sensorMode: "wait",
// 		extractor:  mockExtractor,
// 	}

// 	err := ts.RunTask(context.Background(), &pipeline.Pipeline{}, &pipeline.Asset{
// 		Type: pipeline.AssetTypeBigqueryQuery,
// 		ExecutableFile: pipeline.ExecutableFile{
// 			Path:    "test-file.sql",
// 			Content: "SELECT",
// 		},
// 		Parameters: pipeline.EmptyStringMap{
// 			"table": "test.table",
// 		},
// 	})

// 	assert.ErrorContains(t, err, "could not render file while extracting")
// }

func TestNewTableSensorBuildTableExistsQueryInvalidFormat(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}
	mockDb := &MockSensorDB{}

	mockConn.On("GetConnection", "gcp-default").Return(mockDb)

	mockDb.On("BuildTableExistsQuery", "test.table.extraneous").Return("", fmt.Errorf("table name must be in dataset.table or project.dataset.table format, 'test.table.extraneous' given"))

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

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

func TestNewTableSensorTestPoking(t *testing.T) {
	t.Parallel()
	var output strings.Builder
	ctx := context.WithValue(context.Background(), executor.KeyPrinter, &output)

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}
	mockDb := &MockSensorDB{}

	mockConn.On("GetConnection", "gcp-default").Return(mockDb)

	// BuildTableExistsQuery succeeds
	mockDb.On("BuildTableExistsQuery", "test.table").Return("", nil)

	// ExtractQueriesFromString succeeds
	mockExtractor.On("ExtractQueriesFromString", "SELECT * FROM test.data").Return([]*query.Query{
		{Query: "SELECT * FROM test.data"},
	}, nil)

	// Select fails
	mockDb.On("Select", mock.Anything, mock.MatchedBy(func(q *query.Query) bool {
		return q.Query == "SELECT * FROM test.data"
	})).Return([][]interface{}{{1}}, nil)

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

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

// // Mock implementations for testing

// type MockTableExistsChecker struct {
// 	mock.Mock
// }

// func (m *MockTableExistsChecker) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
// 	args := m.Called(ctx, q)
// 	if args.Get(0) == nil {
// 		return nil, args.Error(1)
// 	}
// 	return args.Get(0).([][]interface{}), args.Error(1)
// }

// func (m *MockTableExistsChecker) BuildTableExistsQuery(tableName string) (string, error) {
// 	args := m.Called(tableName)
// 	return args.String(0), args.Error(1)
// }

// type MockTimeProvider struct {
// 	mock.Mock
// }

// func (m *MockTimeProvider) Now() time.Time {
// 	args := m.Called()
// 	return args.Get(0).(time.Time)
// }

// func (m *MockTimeProvider) Sleep(duration time.Duration) {
// 	m.Called(duration)
// }

// func (m *MockTimeProvider) After(duration time.Duration) <-chan time.Time {
// 	args := m.Called(duration)
// 	return args.Get(0).(<-chan time.Time)
// }

// type MockPrinter struct {
// 	mock.Mock
// 	output []string
// }

// func (m *MockPrinter) Println(a ...interface{}) (n int, err error) {
// 	args := m.Called(a...)
// 	output := fmt.Sprint(a...)
// 	m.output = append(m.output, output)
// 	return args.Int(0), args.Error(1)
// }

// func (m *MockPrinter) GetOutput() []string {
// 	return m.output
// }

// type MockConnectionGetter struct {
// 	mock.Mock
// }

// func (m *MockConnectionGetter) GetConnection(name string) interface{} {
// 	args := m.Called(name)
// 	return args.Get(0)
// }

// type MockPipeline struct {
// 	mock.Mock
// 	connectionMappings map[string]string
// }

// func NewMockPipeline() *MockPipeline {
// 	return &MockPipeline{
// 		connectionMappings: make(map[string]string),
// 	}
// }

// func (m *MockPipeline) GetConnectionNameForAsset(asset *pipeline.Asset) (string, error) {
// 	args := m.Called(asset)
// 	if args.Get(0) == nil {
// 		return "", args.Error(1)
// 	}
// 	return args.Get(0).(string), args.Error(1)
// }

// func (m *MockPipeline) SetConnectionMapping(assetType, connectionName string) {
// 	m.connectionMappings[assetType] = connectionName
// }

// type MockQueryExtractor struct {
// 	mock.Mock
// }

// func (m *MockQueryExtractor) CloneForAsset(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) (query.QueryExtractor, error) {
// 	args := m.Called(ctx, p, t)
// 	return args.Get(0).(query.QueryExtractor), args.Error(1)
// }

// func (m *MockQueryExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
// 	args := m.Called(content)
// 	if args.Get(0) == nil {
// 		return nil, args.Error(1)
// 	}
// 	return args.Get(0).([]*query.Query), args.Error(1)
// }

// func (m *MockQueryExtractor) ReextractQueriesFromSlice(queries []string) ([]string, error) {
// 	args := m.Called(queries)
// 	if args.Get(0) == nil {
// 		return nil, args.Error(1)
// 	}
// 	return args.Get(0).([]string), args.Error(1)
// }

// func TestNewTableSensor(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}

// 	sensor := NewTableSensor(mockConn, "once", mockExtractor)

// 	assert.NotNil(t, sensor)
// 	assert.Equal(t, mockConn, sensor.connection)
// 	assert.Equal(t, "once", sensor.sensorMode)
// 	assert.Equal(t, mockExtractor, sensor.extractor)
// 	assert.NotNil(t, sensor.timeProvider)
// 	assert.Nil(t, sensor.printer) // Should be nil by default
// }

// func TestNewTableSensorWithDependencies(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockPrinter := &MockPrinter{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, mockPrinter)

// 	assert.NotNil(t, sensor)
// 	assert.Equal(t, mockConn, sensor.connection)
// 	assert.Equal(t, "once", sensor.sensorMode)
// 	assert.Equal(t, mockExtractor, sensor.extractor)
// 	assert.Equal(t, mockTimeProvider, sensor.timeProvider)
// 	assert.Equal(t, mockPrinter, sensor.printer)
// }

// func TestTableSensor_RunTask_SkipMode(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "skip", mockExtractor, mockTimeProvider, nil)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{}

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.NoError(t, err)
// }

// func TestTableSensor_RunTask_MissingTableParameter(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, nil)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{}, // Empty parameters
// 	}

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "table sensor requires a parameter named 'table'")
// }

// func TestTableSensor_RunTask_ConnectionNotFound(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockPipeline := NewMockPipeline()

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, nil)

// 	ctx := context.Background()
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock pipeline to return connection name
// 	mockPipeline.On("GetConnectionNameForAsset", task).Return("test_connection", nil)

// 	// Mock connection not found
// 	mockConn.On("GetConnection", "test_connection").Return(nil)

// 	err := sensor.RunTask(ctx, mockPipeline, task)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "does not exist")
// }

// func TestTableSensor_RunTask_ConnectionNotTableExistsChecker(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, nil)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that doesn't implement TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return("not a table checker")

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "does not support table existence checking")
// }

// func TestTableSensor_RunTask_BuildTableExistsQueryError(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockTableChecker := &MockTableExistsChecker{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, nil)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that implements TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return(mockTableChecker)

// 	// Mock BuildTableExistsQuery to return error
// 	mockTableChecker.On("BuildTableExistsQuery", "test_table").Return("", errors.New("build query error"))

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "failed to build table exists query")
// }

// func TestTableSensor_RunTask_ExtractQueriesError(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockTableChecker := &MockTableExistsChecker{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, nil)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that implements TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return(mockTableChecker)

// 	// Mock BuildTableExistsQuery to return success
// 	mockTableChecker.On("BuildTableExistsQuery", "test_table").Return("SELECT 1", nil)

// 	// Mock ExtractQueriesFromString to return error
// 	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return(nil, errors.New("extract error"))

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "failed to extract table exists query")
// }

// func TestTableSensor_RunTask_NoQueriesExtracted(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockTableChecker := &MockTableExistsChecker{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, nil)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that implements TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return(mockTableChecker)

// 	// Mock BuildTableExistsQuery to return success
// 	mockTableChecker.On("BuildTableExistsQuery", "test_table").Return("SELECT 1", nil)

// 	// Mock ExtractQueriesFromString to return empty slice
// 	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{}, nil)

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "no queries extracted from table exists query")
// }

// func TestTableSensor_RunTask_Success_OnceMode(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockTableChecker := &MockTableExistsChecker{}
// 	mockPrinter := &MockPrinter{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "once", mockExtractor, mockTimeProvider, mockPrinter)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that implements TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return(mockTableChecker)

// 	// Mock BuildTableExistsQuery to return success
// 	mockTableChecker.On("BuildTableExistsQuery", "test_table").Return("SELECT 1", nil)

// 	// Mock ExtractQueriesFromString to return a query
// 	extractedQuery := &query.Query{Query: "SELECT 1"}
// 	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{extractedQuery}, nil)

// 	// Mock Select to return table exists
// 	mockTableChecker.On("Select", ctx, extractedQuery).Return([][]interface{}{{1}}, nil)

// 	// Mock printer
// 	mockPrinter.On("Println", "Poking:", "test_table").Return(0, nil)

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.NoError(t, err)

// 	// Verify the output
// 	assert.Contains(t, mockPrinter.GetOutput(), "Poking: test_table")
// }

// func TestTableSensor_RunTask_Success_PollingMode(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockTableChecker := &MockTableExistsChecker{}
// 	mockPrinter := &MockPrinter{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "polling", mockExtractor, mockTimeProvider, mockPrinter)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that implements TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return(mockTableChecker)

// 	// Mock BuildTableExistsQuery to return success
// 	mockTableChecker.On("BuildTableExistsQuery", "test_table").Return("SELECT 1", nil)

// 	// Mock ExtractQueriesFromString to return a query
// 	extractedQuery := &query.Query{Query: "SELECT 1"}
// 	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{extractedQuery}, nil)

// 	// Mock Select to return table doesn't exist first, then exists
// 	mockTableChecker.On("Select", ctx, extractedQuery).Return([][]interface{}{{0}}, nil).Once()
// 	mockTableChecker.On("Select", ctx, extractedQuery).Return([][]interface{}{{1}}, nil).Once()

// 	// Mock time provider
// 	timeoutChan := make(chan time.Time, 1)
// 	mockTimeProvider.On("After", 24*time.Hour).Return(timeoutChan)
// 	mockTimeProvider.On("Sleep", time.Duration(30)*time.Second).Return()

// 	// Mock printer
// 	mockPrinter.On("Println", "Poking:", "test_table").Return(0, nil)
// 	mockPrinter.On("Println", "Info: Sensor didn't return the expected result, waiting for", 30, "seconds").Return(0, nil)

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.NoError(t, err)

// 	// Verify the output
// 	output := mockPrinter.GetOutput()
// 	assert.Contains(t, output, "Poking: test_table")
// 	assert.Contains(t, output, "Info: Sensor didn't return the expected result, waiting for 30 seconds")
// }

// func TestTableSensor_RunTask_Timeout(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockTableChecker := &MockTableExistsChecker{}
// 	mockPrinter := &MockPrinter{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "polling", mockExtractor, mockTimeProvider, mockPrinter)

// 	ctx := context.Background()
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that implements TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return(mockTableChecker)

// 	// Mock BuildTableExistsQuery to return success
// 	mockTableChecker.On("BuildTableExistsQuery", "test_table").Return("SELECT 1", nil)

// 	// Mock ExtractQueriesFromString to return a query
// 	extractedQuery := &query.Query{Query: "SELECT 1"}
// 	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{extractedQuery}, nil)

// 	// Mock Select to always return table doesn't exist
// 	mockTableChecker.On("Select", ctx, extractedQuery).Return([][]interface{}{{0}}, nil)

// 	// Mock time provider to trigger timeout
// 	timeoutChan := make(chan time.Time, 1)
// 	timeoutChan <- time.Now() // Trigger timeout immediately
// 	mockTimeProvider.On("After", 24*time.Hour).Return(timeoutChan)
// 	mockTimeProvider.On("Sleep", time.Duration(30)*time.Second).Return()

// 	// Mock printer
// 	mockPrinter.On("Println", "Poking:", "test_table").Return(0, nil)

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.Error(t, err)
// 	assert.Contains(t, err.Error(), "Sensor timed out after 24 hours")
// }

// func TestTableSensor_RunTask_ContextCancelled(t *testing.T) {
// 	mockConn := &MockConnectionGetter{}
// 	mockExtractor := &MockQueryExtractor{}
// 	mockTimeProvider := &MockTimeProvider{}
// 	mockTableChecker := &MockTableExistsChecker{}
// 	mockPrinter := &MockPrinter{}

// 	sensor := NewTableSensorWithDependencies(mockConn, "polling", mockExtractor, mockTimeProvider, mockPrinter)

// 	ctx, cancel := context.WithCancel(context.Background())
// 	p := &pipeline.Pipeline{}
// 	task := &pipeline.Asset{
// 		Parameters: map[string]string{"table": "test_table"},
// 	}

// 	// Mock connection that implements TableExistsChecker
// 	mockConn.On("GetConnection", mock.AnythingOfType("string")).Return(mockTableChecker)

// 	// Mock BuildTableExistsQuery to return success
// 	mockTableChecker.On("BuildTableExistsQuery", "test_table").Return("SELECT 1", nil)

// 	// Mock ExtractQueriesFromString to return a query
// 	extractedQuery := &query.Query{Query: "SELECT 1"}
// 	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{extractedQuery}, nil)

// 	// Mock Select to always return table doesn't exist
// 	mockTableChecker.On("Select", ctx, extractedQuery).Return([][]interface{}{{0}}, nil)

// 	// Mock time provider
// 	timeoutChan := make(chan time.Time, 1)
// 	mockTimeProvider.On("After", 24*time.Hour).Return(timeoutChan)
// 	mockTimeProvider.On("Sleep", time.Duration(30)*time.Second).Return().Run(func(args mock.Arguments) {
// 		cancel() // Cancel context during sleep
// 	})

// 	// Mock printer
// 	mockPrinter.On("Println", "Poking:", "test_table").Return(0, nil)

// 	err := sensor.RunTask(ctx, p, task)
// 	assert.Error(t, err)
// 	assert.Equal(t, context.Canceled, err)
// }

// func TestContextPrinter(t *testing.T) {
// 	ctx := context.Background()
// 	printer := NewContextPrinter(ctx)

// 	// Test without writer in context
// 	n, err := printer.Println("test")
// 	assert.Equal(t, 0, n)
// 	assert.NoError(t, err)

// 	// Test with writer in context
// 	var output strings.Builder
// 	ctxWithWriter := context.WithValue(ctx, "printer", &output)
// 	printerWithWriter := NewContextPrinter(ctxWithWriter)

// 	n, err = printerWithWriter.Println("test")
// 	assert.Greater(t, n, 0)
// 	assert.NoError(t, err)
// }

// func TestDefaultTimeProvider(t *testing.T) {
// 	provider := &DefaultTimeProvider{}

// 	// Test Now
// 	now := provider.Now()
// 	assert.NotZero(t, now)

// 	// Test Sleep (should not panic)
// 	start := time.Now()
// 	provider.Sleep(1 * time.Millisecond)
// 	duration := time.Since(start)
// 	assert.GreaterOrEqual(t, duration, time.Millisecond)

// 	// Test After
// 	ch := provider.After(1 * time.Millisecond)
// 	select {
// 	case <-ch:
// 		// Expected
// 	case <-time.After(100 * time.Millisecond):
// 		t.Fatal("After channel did not receive value")
// 	}
// }
