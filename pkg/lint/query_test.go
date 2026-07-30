package lint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockValidator struct {
	mock.Mock
}

func (m *mockValidator) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	res := m.Called(ctx, query)
	return res.Bool(0), res.Error(1)
}

type mockConnectionManager struct {
	mock.Mock
}

func (m *mockConnectionManager) GetConnection(conn string) any {
	res := m.Called(conn)
	return res.Get(0)
}

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func (m *mockExtractor) CloneForAsset(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) (query.QueryExtractor, error) {
	res := m.Called(ctx, p, t)
	return res.Get(0).(query.QueryExtractor), res.Error(1)
}

func (m *mockExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	res := m.Called(content)
	return res.Get(0).([]string), res.Error(1)
}

type mockMaterializer struct {
	mock.Mock
}

func (m *mockMaterializer) Render(task *pipeline.Asset, query string) (string, error) {
	res := m.Called(task, query)
	return res.Get(0).(string), res.Error(1)
}

func TestQueryValidatorRule_ValidateAssetWithHookScope(t *testing.T) {
	t.Parallel()

	startDate := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2024, time.January, 31, 0, 0, 0, 0, time.UTC)
	executionDate := endDate
	ctx := context.WithValue(t.Context(), pipeline.RunConfigStartDate, startDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, endDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, executionDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run-id")

	asset := &pipeline.Asset{
		Name: "dashboard.repro1",
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Content: "asset query",
		},
		Hooks: pipeline.Hooks{
			Pre: []pipeline.Hook{{
				Query: "DECLARE window_start DATE DEFAULT date_sub(date '{{ end_date }}', INTERVAL 30 DAY)",
			}},
		},
	}
	p := &pipeline.Pipeline{
		Name:   "test-pipeline",
		Assets: []*pipeline.Asset{asset},
		DefaultConnections: map[string]string{
			"google_cloud_platform": "gcp-conn",
		},
	}

	extractor := new(mockExtractor)
	extractor.On("CloneForAsset", mock.Anything, p, asset).Return(extractor, nil)
	extractor.On("ExtractQueriesFromString", "asset query").Return(
		[]*query.Query{{Query: "select window_start, date '2024-01-31' as window_end"}},
		nil,
	)

	materializer := new(mockMaterializer)
	materializer.On(
		"Render",
		asset,
		"select window_start, date '2024-01-31' as window_end",
	).Return(
		"CREATE OR REPLACE TABLE dashboard.repro1 AS\nselect window_start, date '2024-01-31' as window_end",
		nil,
	)

	expectedQuery := &query.Query{Query: "DECLARE window_start DATE DEFAULT date_sub(date '2024-01-31', INTERVAL 30 DAY);\n" +
		"CREATE OR REPLACE TABLE dashboard.repro1 AS\nselect window_start, date '2024-01-31' as window_end;"}
	validator := new(mockValidator)
	validator.On("IsValid", mock.Anything, expectedQuery).Return(true, nil)

	connections := new(mockConnectionManager)
	connections.On("GetConnection", "gcp-conn").Return(validator)

	rule := &QueryValidatorRule{
		TaskType:     pipeline.AssetTypeBigqueryQuery,
		Connections:  connections,
		Extractor:    extractor,
		Materializer: materializer,
		HookRenderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, &executionDate, "test-pipeline", "test-run-id", nil),
		Logger:       zap.NewNop().Sugar(),
	}

	issues, err := rule.ValidateAsset(ctx, p, asset)
	require.NoError(t, err)
	assert.Empty(t, issues)
	validator.AssertExpectations(t)
	extractor.AssertExpectations(t)
	materializer.AssertExpectations(t)
	connections.AssertExpectations(t)
}

func TestQueryValidatorRule_Validate(t *testing.T) {
	t.Parallel()

	noIssues := make([]*Issue, 0)
	taskType := pipeline.AssetType("bq.sql")

	type fields struct {
		validator         *mockValidator
		extractor         *mockExtractor
		connectionManager *mockConnectionManager
		materializer      *mockMaterializer
	}

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		setup   func(f *fields)
		want    []*Issue
		wantErr bool
	}{
		{
			name: "no tasks to execute",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{},
			},
			want:    noIssues,
			wantErr: false,
		},
		{
			name: "no tasks from task type to execute",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: "yet another task type",
					},
				},
			},
			want:    noIssues,
			wantErr: false,
		},
		{
			name: "a task to extract, but query extractor fails",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file-with-no-queries.sql",
							Content: "some content",
						},
					},
				},
			},
			setup: func(f *fields) {
				// Mock CloneForAsset to return the extractor itself
				f.extractor.On("CloneForAsset", mock.Anything, mock.Anything, mock.Anything).Return(f.extractor, nil)
				f.extractor.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{}, errors.New("something failed"))
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file-with-no-queries.sql",
							Content: "some content",
						},
					},
					Description: "Cannot read executable file 'path/to/file-with-no-queries.sql'",
					Context:     []string{"something failed"},
				},
			},
		},
		{
			name: "a task to extract, but no queries in it",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file-with-no-queries.sql",
							Content: "some content",
						},
					},
				},
			},
			setup: func(f *fields) {
				f.extractor.On("CloneForAsset", mock.Anything, mock.Anything, mock.Anything).Return(f.extractor, nil)
				f.extractor.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{}, nil)
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file-with-no-queries.sql",
							Content: "some content",
						},
					},
					Description: "No queries found in executable file 'path/to/file-with-no-queries.sql'",
				},
			},
		},
		{
			name: "two tasks to extract, 3 queries in each, one invalid",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file1.sql",
							Content: "content1",
						},
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file2.sql",
							Content: "content2",
						},
					},
				},
				DefaultConnections: map[string]string{
					"google_cloud_platform": "gcp-conn",
				},
			},
			setup: func(f *fields) {
				// Mock CloneForAsset for both assets
				f.extractor.On("CloneForAsset", mock.Anything, mock.Anything, mock.Anything).Return(f.extractor, nil)

				f.extractor.On("ExtractQueriesFromString", "content1").
					Return(
						[]*query.Query{
							{Query: "query11"},
							{Query: "query12"},
							{Query: "query13"},
						},
						nil,
					)
				f.extractor.On("ExtractQueriesFromString", "content2").
					Return(
						[]*query.Query{
							{Query: "query21"},
							{Query: "query22"},
							{Query: "query23"},
						},
						nil,
					)
			},
			want: []*Issue{},
		},
		{
			name: "two tasks to extract, all materialized",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file1.sql",
							Content: "content1",
						},
						Materialization: pipeline.Materialization{
							Type: "table",
						},
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path:    "path/to/file2.sql",
							Content: "content2",
						},
						Materialization: pipeline.Materialization{
							Type: "table",
						},
					},
				},
				DefaultConnections: map[string]string{
					"google_cloud_platform": "gcp-conn",
				},
			},
			setup: func(f *fields) {
				// Mock CloneForAsset for both assets
				f.extractor.On("CloneForAsset", mock.Anything, mock.Anything, mock.Anything).Return(f.extractor, nil)

				f.extractor.On("ExtractQueriesFromString", "content1").
					Return(
						[]*query.Query{
							{Query: "query11"},
						},
						nil,
					)
				f.extractor.On("ExtractQueriesFromString", "content2").
					Return(
						[]*query.Query{
							{Query: "query21"},
						},
						nil,
					)

				f.connectionManager.On("GetConnection", "gcp-conn").Return(f.validator, nil)

				f.materializer.On("Render", &pipeline.Asset{
					Type: taskType,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "path/to/file1.sql",
						Content: "content1",
					},
					Materialization: pipeline.Materialization{
						Type: "table",
					},
				}, "query11").Return("materialized-query11", nil)

				f.materializer.On("Render", &pipeline.Asset{
					Type: taskType,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "path/to/file2.sql",
						Content: "content2",
					},
					Materialization: pipeline.Materialization{
						Type: "table",
					},
				}, "query21").Return("materialized-query21", nil)

				f.validator.On("IsValid", mock.Anything, &query.Query{Query: "materialized-query11"}).Return(true, nil)
				f.validator.On("IsValid", mock.Anything, &query.Query{Query: "materialized-query21"}).Return(true, nil)
			},
			want: []*Issue{},
		},
	}
	for _, tt := range tests {
		ctx := t.Context()
		// Add required context values for CloneForAsset to work with interval modifiers
		ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Now().AddDate(0, 0, -1))
		ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Now())
		ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, time.Now())
		ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run-id")

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			validator := new(mockValidator)
			extractor := new(mockExtractor)
			conn := new(mockConnectionManager)
			mat := new(mockMaterializer)

			if tt.setup != nil {
				tt.setup(&fields{
					validator:         validator,
					extractor:         extractor,
					connectionManager: conn,
					materializer:      mat,
				})
			}

			q := &QueryValidatorRule{
				TaskType:     taskType,
				Extractor:    extractor,
				Connections:  conn,
				Logger:       zap.NewNop().Sugar(),
				WorkerCount:  1,
				Materializer: mat,
			}

			got, err := q.Validate(ctx, tt.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.ElementsMatch(t, tt.want, got)
			validator.AssertExpectations(t)
			extractor.AssertExpectations(t)
			conn.AssertExpectations(t)
		})
	}
}
