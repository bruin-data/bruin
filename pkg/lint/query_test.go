package lint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
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
		ctx := context.Background()
		// Add required context values for CloneForAsset to work with interval modifiers
		ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Now().AddDate(0, 0, -1))
		ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Now())
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
