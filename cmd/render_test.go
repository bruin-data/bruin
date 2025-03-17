package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		taskPath string
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
				taskPath: "",
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if asset fails to be built",
			args: args{
				taskPath: "/path/to/asset",
			},
			setup: func(f *fields) {
				f.builder.On("CreateAssetFromFile", "/path/to/asset", mock.Anything).
					Return(nil, assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if asset building doesnt fail but returns an empty asset",
			args: args{
				taskPath: "/path/to/asset",
			},
			setup: func(f *fields) {
				f.builder.On("CreateAssetFromFile", "/path/to/asset", mock.Anything).
					Return(nil, nil)
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if failed to extract queries from file",
			args: args{
				taskPath: "/path/to/asset",
			},
			setup: func(f *fields) {
				f.builder.On("CreateAssetFromFile", "/path/to/asset", mock.Anything).
					Return(bqAsset, nil)

				f.bqMaterializer.On("Render", bqAsset, bqAsset.ExecutableFile.Content).
					Return("some materialized query", nil)

				f.extractor.On("ExtractQueriesFromString", "some materialized query").
					Return(nil, assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if materialization fails",
			args: args{
				taskPath: "/path/to/asset",
			},
			setup: func(f *fields) {
				f.builder.On("CreateAssetFromFile", "/path/to/asset", mock.Anything).
					Return(bqAsset, nil)

				f.bqMaterializer.On("Render", bqAsset, bqAsset.ExecutableFile.Content).
					Return("", assert.AnError)

			},
			wantErr: assert.Error,
		},
		{
			name: "should materialize if asset is a bigquery query",
			args: args{
				taskPath: "/path/to/asset",
			},
			setup: func(f *fields) {
				f.builder.On("CreateAssetFromFile", "/path/to/asset", mock.Anything).
					Return(bqAsset, nil)

				f.bqMaterializer.On("Render", bqAsset, bqAsset.ExecutableFile.Content).
					Return("some-materialized-query", nil)

				f.extractor.On("ExtractQueriesFromString", "some-materialized-query").
					Return([]*query.Query{{Query: "some materialized and rendered query"}}, nil)

				f.writer.On("Write", []byte("some materialized and rendered query\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should skip materialization if asset is a not bigquery query",
			args: args{
				taskPath: "/path/to/asset",
			},
			setup: func(f *fields) {
				f.builder.On("CreateAssetFromFile", "/path/to/asset", mock.Anything).
					Return(nonBqAsset, nil)

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

			tt.wantErr(t, render.Run(tt.args.taskPath, nil))
			f.extractor.AssertExpectations(t)
			f.bqMaterializer.AssertExpectations(t)
			f.builder.AssertExpectations(t)
			f.writer.AssertExpectations(t)
		})
	}
}
