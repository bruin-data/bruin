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
	mockExtractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{}, fmt.Errorf("failed to extract table exists query"))

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

	assert.ErrorContains(t, err, "failed to extract table exists query")
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




///////////////////

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
