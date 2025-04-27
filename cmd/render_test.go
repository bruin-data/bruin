package cmd

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
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
			ctx := make(map[string]any, 3)
			ctx["startDate"] = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			ctx["endDate"] = time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
			ctx["applyModifiers"] = false

			tt.wantErr(t, render.Run(tt.args.task, ctx))
			f.extractor.AssertExpectations(t)
			f.bqMaterializer.AssertExpectations(t)
			f.builder.AssertExpectations(t)
			f.writer.AssertExpectations(t)
		})
	}
}
