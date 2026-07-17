package ansisql

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

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
	})

	assert.NoError(t, err)
}

func TestNewTableSensorModeNoTable(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
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

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeIngestr,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.ParameterMap{
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

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.ParameterMap{
			"table": "test_table",
		},
	})

	assert.ErrorContains(t, err, "connection 'gcp-default' not found in config file '.bruin.yml' under environment 'default'")
}

func TestNewTableSensorNoTableExistsChecker(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return("")

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.ParameterMap{
			"table": "test.table",
		},
	})

	assert.ErrorContains(t, err, "Connection 'gcp-default' cannot be used for sensor on 'test.table'")
}

func TestNewTableSensorBuildTableExistsQueryError(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockTableExistsChecker := &MockTableExistsChecker{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return(mockTableExistsChecker)
	mockTableExistsChecker.On("BuildTableExistsQuery", "test.table").Return("", errors.New("failed to build table exists query"))

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.ParameterMap{
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
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{}, errors.New("failed to extract table exists query"))

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.ParameterMap{
			"table": "database.test.table",
		},
	})

	assert.ErrorContains(t, err, "failed to extract table exists query")
}

func TestTableSensorTimesOutWhenConfigured(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockTableExistsChecker := &MockTableExistsChecker{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return(mockTableExistsChecker)
	mockTableExistsChecker.On("BuildTableExistsQuery", "database.test.table").Return("SELECT 1", nil)
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{{Query: "SELECT 1"}}, nil)
	// Always return 0 so the sensor never succeeds.
	mockTableExistsChecker.On("Select", mock.Anything, mock.Anything).Return([][]interface{}{{int64(0)}}, nil)

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	start := time.Now()
	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.ParameterMap{
			"table":         "database.test.table",
			"timeout":       "100ms",
			"poke_interval": "0",
		},
	})
	elapsed := time.Since(start)

	require.ErrorContains(t, err, "Sensor timed out after")
	assert.Less(t, elapsed, 5*time.Second, "timeout should fire promptly")
}

func TestNewTableSensorgNoQueries(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockTableExistsChecker := &MockTableExistsChecker{}
	mockExtractor := &MockQueryExtractor{}

	mockConn.On("GetConnection", "gcp-default").Return(mockTableExistsChecker)
	mockTableExistsChecker.On("BuildTableExistsQuery", "database.test.table").Return("SELECT 1", nil)
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{}, nil)

	ts := NewTableSensor(mockConn, "wait", mockExtractor)

	err := ts.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
		Parameters: pipeline.ParameterMap{
			"table": "database.test.table",
		},
	})

	assert.ErrorContains(t, err, "no queries extracted from table exists query")
}

func TestQuerySensorAddsAnnotations(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockDB := &MockSensorDB{}
	mockExtractor := &MockQueryExtractor{}
	p := &pipeline.Pipeline{Name: "sensor_pipeline"}
	asset := &pipeline.Asset{
		Name: "query_sensor_asset",
		Type: pipeline.AssetTypeDatabricksQuerySensor,
		Parameters: pipeline.ParameterMap{
			"query": "SELECT 1",
		},
	}
	expectedQuery := &query.Query{
		Query:       "-- @bruin.config: {\"asset\":\"query_sensor_asset\",\"pipeline\":\"sensor_pipeline\",\"sensor_type\":\"query\",\"type\":\"sensor\"}\nSELECT 1",
		Annotations: map[string]string{"asset": "query_sensor_asset", "pipeline": "sensor_pipeline", "sensor_type": "query", "type": "sensor"},
	}

	mockExtractor.On("CloneForAsset", mock.Anything, p, asset).Return(mockExtractor, nil)
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{{Query: "SELECT 1"}}, nil)
	mockConn.On("GetConnection", "databricks-default").Return(mockDB)
	mockDB.On("Select", mock.Anything, expectedQuery).Return([][]interface{}{{int64(1)}}, nil).Once()

	ctx := context.WithValue(t.Context(), pipeline.RunConfigQueryAnnotations, DefaultQueryAnnotations)
	err := NewQuerySensor(mockConn, mockExtractor, "once").RunTask(ctx, p, asset)

	require.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestTableSensorAddsAnnotations(t *testing.T) {
	t.Parallel()

	mockConn := &MockConnectionGetter{}
	mockDB := &MockTableExistsChecker{}
	mockExtractor := &MockQueryExtractor{}
	p := &pipeline.Pipeline{Name: "sensor_pipeline"}
	asset := &pipeline.Asset{
		Name: "table_sensor_asset",
		Type: pipeline.AssetTypeDatabricksTableSensor,
		Parameters: pipeline.ParameterMap{
			"table": "catalog.schema.table",
		},
	}
	expectedQuery := &query.Query{
		Query:       "-- @bruin.config: {\"asset\":\"table_sensor_asset\",\"pipeline\":\"sensor_pipeline\",\"sensor_type\":\"table\",\"type\":\"sensor\"}\nSELECT 1",
		Annotations: map[string]string{"asset": "table_sensor_asset", "pipeline": "sensor_pipeline", "sensor_type": "table", "type": "sensor"},
	}

	mockConn.On("GetConnection", "databricks-default").Return(mockDB)
	mockDB.On("BuildTableExistsQuery", "catalog.schema.table").Return("SELECT 1", nil)
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{{Query: "SELECT 1"}}, nil)
	mockDB.On("Select", mock.Anything, expectedQuery).Return([][]interface{}{{int64(1)}}, nil).Once()

	ctx := context.WithValue(t.Context(), pipeline.RunConfigQueryAnnotations, DefaultQueryAnnotations)
	err := NewTableSensor(mockConn, "once", mockExtractor).RunTask(ctx, p, asset)

	require.NoError(t, err)
	mockDB.AssertExpectations(t)
}
