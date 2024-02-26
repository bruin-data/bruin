package lint

import (
	"context"
	"errors"
	"testing"

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

func (m *mockConnectionManager) GetConnection(conn string) (interface{}, error) {
	res := m.Called(conn)
	return res.Get(0), res.Error(1)
}

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromFile(filepath string) ([]*query.Query, error) {
	res := m.Called(filepath)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func TestQueryValidatorRule_Validate(t *testing.T) {
	t.Parallel()

	noIssues := make([]*Issue, 0)
	taskType := pipeline.AssetType("bq.sql")

	type fields struct {
		validator         *mockValidator
		extractor         *mockExtractor
		connectionManager *mockConnectionManager
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
							Path: "path/to/file-with-no-queries.sql",
						},
					},
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromFile", "path/to/file-with-no-queries.sql").
					Return([]*query.Query{}, errors.New("something failed"))
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file-with-no-queries.sql",
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
							Path: "path/to/file-with-no-queries.sql",
						},
					},
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromFile", "path/to/file-with-no-queries.sql").
					Return([]*query.Query{}, nil)
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file-with-no-queries.sql",
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
							Path: "path/to/file1.sql",
						},
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file2.sql",
						},
					},
				},
				DefaultConnections: map[string]string{
					"google_cloud_platform": "gcp-conn",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromFile", "path/to/file1.sql").
					Return(
						[]*query.Query{
							{Query: "query11"},
							{Query: "query12"},
							{Query: "query13"},
						},
						nil,
					)
				f.extractor.On("ExtractQueriesFromFile", "path/to/file2.sql").
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
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			validator := new(mockValidator)
			extractor := new(mockExtractor)
			conn := new(mockConnectionManager)

			if tt.setup != nil {
				tt.setup(&fields{
					validator:         validator,
					extractor:         extractor,
					connectionManager: conn,
				})
			}

			q := &QueryValidatorRule{
				TaskType:    taskType,
				Extractor:   extractor,
				Connections: conn,
				Logger:      zap.NewNop().Sugar(),
				WorkerCount: 1,
			}

			got, err := q.Validate(tt.p)
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
