package cmd

import (
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockBuilder struct {
	mock.Mock
}

func (m *mockBuilder) CreateAssetFromFile(path string, foundPipeline *pipeline.Pipeline) (*pipeline.Asset, error) {
	called := m.Called(path, foundPipeline)
	if called.Get(0) == nil {
		return nil, called.Error(1)
	}

	return called.Get(0).(*pipeline.Asset), called.Error(1)
}

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	res := m.Called(content)
	if res.Get(0) == nil {
		return nil, res.Error(1)
	}

	return res.Get(0).([]*query.Query), res.Error(1)
}

type mockMaterializer struct {
	mock.Mock
}

func (m *mockMaterializer) Render(task *pipeline.Asset, query string) (string, error) {
	res := m.Called(task, query)
	return res.String(0), res.Error(1)
}

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) Write(p []byte) (int, error) {
	res := m.Called(p)
	return res.Int(0), res.Error(1)
}

func TestRenderCommand_Run(t *testing.T) {
	t.Parallel()

	bqAsset := &pipeline.Asset{
		Name: "asset1",
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/executable",
		},
	}

	nonBqAsset := &pipeline.Asset{
		Name: "non-bq",
		Type: pipeline.AssetTypeEmpty,
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/executable2",
		},
	}

	type fields struct {
		extractor      *mockExtractor
		bqMaterializer *mockMaterializer
		builder        *mockBuilder
		writer         *mockWriter
	}
	type args struct {
		task *pipeline.Asset
	}
	tests := []struct {
		name    string
		setup   func(*fields)
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "should return error if task path is empty",
			args: args{
				task: nil,
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if failed to extract queries from file",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable",
					},
					Name: "asset1",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", bqAsset.ExecutableFile.Content).
					Return(nil, assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if materialization fails",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable",
					},
					Name: "asset1",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", bqAsset.ExecutableFile.Content).
					Return([]*query.Query{{Query: "some query"}}, nil)

				f.bqMaterializer.On("Render", mock.Anything, "some query").
					Return("", assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should materialize if asset is a bigquery query",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable",
					},
					Name: "asset1",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", bqAsset.ExecutableFile.Content).
					Return([]*query.Query{{Query: "extracted query"}}, nil)
				f.bqMaterializer.On("Render", mock.Anything, "extracted query").
					Return("some-materialized-query", nil)

				f.writer.On("Write", []byte("some-materialized-query\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should skip materialization if asset is a not bigquery query",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeSnowflakeQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable",
					},
					Name: "asset1",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", nonBqAsset.ExecutableFile.Content).
					Return([]*query.Query{{Query: "SELECT * FROM nonbq.table1"}}, nil)

				f.writer.On("Write", []byte("SELECT * FROM nonbq.table1\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fields{
				extractor:      new(mockExtractor),
				bqMaterializer: new(mockMaterializer),
				builder:        new(mockBuilder),
				writer:         new(mockWriter),
			}

			if tt.setup != nil {
				tt.setup(f)
			}

			render := &RenderCommand{
				extractor: f.extractor,
				materializers: map[pipeline.AssetType]queryMaterializer{
					pipeline.AssetTypeBigqueryQuery: f.bqMaterializer,
				},
				builder: f.builder,
				writer:  f.writer,
			}

			// Create an instance of ExecutionParameters
			params := ModifierInfo{
				StartDate:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				EndDate:        time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
				ApplyModifiers: false,
			}

			tt.wantErr(t, render.Run(&pipeline.Pipeline{}, tt.args.task, params))
			f.extractor.AssertExpectations(t)
			f.bqMaterializer.AssertExpectations(t)
			f.builder.AssertExpectations(t)
			f.writer.AssertExpectations(t)
		})
	}
}

func TestRenderCommand_Run_QuerySensors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		task    *pipeline.Asset
		setup   func(*mockExtractor, *mockMaterializer, *mockWriter)
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "should extract query from parameters for BigQuery query sensor",
			task: &pipeline.Asset{
				Name: "bq-sensor",
				Type: pipeline.AssetTypeBigqueryQuerySensor,
				Parameters: map[string]string{
					"query": "SELECT COUNT(*) FROM `project.dataset.table` WHERE created_at > '{{ start_date }}'",
				},
				ExecutableFile: pipeline.ExecutableFile{
					Path:    "/path/to/sensor.task.yml",
					Content: "sensor:\ntype: bq.sensor.query\nparameters:\n  query: SELECT...",
				},
			},
			setup: func(extractor *mockExtractor, materializer *mockMaterializer, writer *mockWriter) {
				extractor.On("ExtractQueriesFromString", "SELECT COUNT(*) FROM `project.dataset.table` WHERE created_at > '{{ start_date }}'").
					Return([]*query.Query{{Query: "SELECT COUNT(*) FROM `project.dataset.table` WHERE created_at > '2024-01-01'"}}, nil)
				materializer.On("Render", mock.Anything, "SELECT COUNT(*) FROM `project.dataset.table` WHERE created_at > '2024-01-01'").
					Return("SELECT COUNT(*) FROM `project.dataset.table` WHERE created_at > '2024-01-01'", nil)
				writer.On("Write", []byte("SELECT COUNT(*) FROM `project.dataset.table` WHERE created_at > '2024-01-01'\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should extract query from parameters for Snowflake query sensor",
			task: &pipeline.Asset{
				Name: "sf-sensor",
				Type: pipeline.AssetTypeSnowflakeQuerySensor,
				Parameters: map[string]string{
					"query": "SELECT * FROM sensor_table WHERE timestamp > '{{ start_date }}'",
				},
				ExecutableFile: pipeline.ExecutableFile{
					Path:    "/path/to/sensor.task.yml",
					Content: "sensor:\ntype: sf.sensor.query\nparameters:\n  query: SELECT...",
				},
			},
			setup: func(extractor *mockExtractor, materializer *mockMaterializer, writer *mockWriter) {
				extractor.On("ExtractQueriesFromString", "SELECT * FROM sensor_table WHERE timestamp > '{{ start_date }}'").
					Return([]*query.Query{{Query: "SELECT * FROM sensor_table WHERE timestamp > '2024-01-01'"}}, nil)
				materializer.On("Render", mock.Anything, "SELECT * FROM sensor_table WHERE timestamp > '2024-01-01'").
					Return("SELECT * FROM sensor_table WHERE timestamp > '2024-01-01'", nil)
				writer.On("Write", []byte("SELECT * FROM sensor_table WHERE timestamp > '2024-01-01'\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should return error if query sensor missing query parameter",
			task: &pipeline.Asset{
				Name: "bq-sensor",
				Type: pipeline.AssetTypeBigqueryQuerySensor,
				Parameters: map[string]string{
					"project_id": "my-project",
				},
				ExecutableFile: pipeline.ExecutableFile{
					Path:    "/path/to/sensor.task.yml",
					Content: "sensor:\ntype: bq.sensor.query\nparameters:\n  project_id: my-project",
				},
			},
			setup: func(extractor *mockExtractor, materializer *mockMaterializer, writer *mockWriter) {
				// No expectations set since it should fail early
			},
			wantErr: assert.Error,
		},
		{
			name: "should use ExecutableFile.Content for regular query assets",
			task: &pipeline.Asset{
				Name: "regular-query",
				Type: pipeline.AssetTypeBigqueryQuery,
				ExecutableFile: pipeline.ExecutableFile{
					Path:    "/path/to/query.sql",
					Content: "SELECT * FROM regular_table WHERE date = '{{ start_date }}'",
				},
			},
			setup: func(extractor *mockExtractor, materializer *mockMaterializer, writer *mockWriter) {
				extractor.On("ExtractQueriesFromString", "SELECT * FROM regular_table WHERE date = '{{ start_date }}'").
					Return([]*query.Query{{Query: "SELECT * FROM regular_table WHERE date = '2024-01-01'"}}, nil)
				materializer.On("Render", mock.Anything, "SELECT * FROM regular_table WHERE date = '2024-01-01'").
					Return("SELECT * FROM regular_table WHERE date = '2024-01-01'", nil)
				writer.On("Write", []byte("SELECT * FROM regular_table WHERE date = '2024-01-01'\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			extractor := new(mockExtractor)
			materializer := new(mockMaterializer)
			writer := new(mockWriter)

			if tt.setup != nil {
				tt.setup(extractor, materializer, writer)
			}

			render := &RenderCommand{
				extractor: extractor,
				materializers: map[pipeline.AssetType]queryMaterializer{
					pipeline.AssetTypeBigqueryQuery:        materializer,
					pipeline.AssetTypeBigqueryQuerySensor:  materializer,
					pipeline.AssetTypeSnowflakeQuerySensor: materializer,
				},
				writer: writer,
			}

			params := ModifierInfo{
				StartDate:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				EndDate:        time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
				ApplyModifiers: false,
			}

			tt.wantErr(t, render.Run(&pipeline.Pipeline{}, tt.task, params))
			extractor.AssertExpectations(t)
			materializer.AssertExpectations(t)
			writer.AssertExpectations(t)
		})
	}
}

func TestIsQuerySensorAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		assetType pipeline.AssetType
		expected  bool
	}{
		{
			name:      "BigQuery query sensor should be detected",
			assetType: pipeline.AssetTypeBigqueryQuerySensor,
			expected:  true,
		},
		{
			name:      "Snowflake query sensor should be detected",
			assetType: pipeline.AssetTypeSnowflakeQuerySensor,
			expected:  true,
		},
		{
			name:      "PostgreSQL query sensor should be detected",
			assetType: pipeline.AssetTypePostgresQuerySensor,
			expected:  true,
		},
		{
			name:      "Athena query sensor should be detected",
			assetType: pipeline.AssetTypeAthenaSQLSensor,
			expected:  true,
		},
		{
			name:      "BigQuery table sensor should NOT be detected",
			assetType: pipeline.AssetTypeBigqueryTableSensor,
			expected:  false,
		},
		{
			name:      "Snowflake table sensor should NOT be detected",
			assetType: pipeline.AssetTypeSnowflakeTableSensor,
			expected:  false,
		},
		{
			name:      "Regular BigQuery query should NOT be detected",
			assetType: pipeline.AssetTypeBigqueryQuery,
			expected:  false,
		},
		{
			name:      "S3 key sensor should NOT be detected",
			assetType: pipeline.AssetTypeS3KeySensor,
			expected:  false,
		},
		{
			name:      "Custom query sensor with .sensor.query suffix should be detected",
			assetType: pipeline.AssetType("custom.sensor.query"),
			expected:  true,
		},
		{
			name:      "Empty asset type should NOT be detected",
			assetType: pipeline.AssetType(""),
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isQuerySensorAsset(tt.assetType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestModifyExtractor(t *testing.T) {
	t.Parallel()
	type args struct {
		task   *pipeline.Asset
		params ModifierInfo
		query  string
	}

	tests := []struct {
		name      string
		args      args
		wantErr   assert.ErrorAssertionFunc
		wantQuery string
	}{
		{
			name: "test modifying extractor",
			args: args{
				task: &pipeline.Asset{
					Name:           "Asset1",
					Type:           pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{},
					IntervalModifiers: pipeline.IntervalModifiers{
						Start: pipeline.TimeModifier{Days: 1},
						End:   pipeline.TimeModifier{Days: 0},
					},
				},
				params: ModifierInfo{
					StartDate:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					EndDate:        time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
					ApplyModifiers: true,
				},
				query: "SELECT * FROM asset1 WHERE date(timestamp_col) = '{{ start_date }}'",
			},
			wantErr:   assert.NoError,
			wantQuery: "SELECT * FROM asset1 WHERE date(timestamp_col) = '2024-01-02'",
		},
		{
			name: "test modifying extractor with no modifiers",
			args: args{
				task: &pipeline.Asset{
					Name:           "Asset1",
					Type:           pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{},
					IntervalModifiers: pipeline.IntervalModifiers{
						Start: pipeline.TimeModifier{Days: 0},
						End:   pipeline.TimeModifier{Days: 0},
					},
				},
				params: ModifierInfo{
					StartDate:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					EndDate:        time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
					ApplyModifiers: true,
				},
				query: "SELECT * FROM asset1 WHERE date(timestamp_col) = '{{ start_date }}'",
			},
			wantErr:   assert.NoError,
			wantQuery: "SELECT * FROM asset1 WHERE date(timestamp_col) = '2024-01-01'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			extractor, err := modifyExtractor(tt.args.params, &pipeline.Pipeline{}, tt.args.task)
			require.NoError(t, err)
			qry, err := extractor.ExtractQueriesFromString(tt.args.query)
			require.NoError(t, err)
			assert.Equal(t, tt.wantQuery, qry[0].Query)
		})
	}
}
