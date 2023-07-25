package lint

import (
	"errors"
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockPipelineBuilder struct {
	mock.Mock
}

func (p *mockPipelineBuilder) CreatePipelineFromPath(pathToPipeline string) (*pipeline.Pipeline, error) {
	args := p.Called(pathToPipeline)
	return args.Get(0).(*pipeline.Pipeline), args.Error(1)
}

func TestLinter_Lint(t *testing.T) {
	t.Parallel()

	errorRule := &SimpleRule{
		Identifier: "errorRule",
		Validator:  func(p *pipeline.Pipeline) ([]*Issue, error) { return nil, errors.New("first rule failed") },
	}

	successRule := &SimpleRule{
		Identifier: "successRule",
		Validator:  func(p *pipeline.Pipeline) ([]*Issue, error) { return nil, nil },
	}

	type fields struct {
		pipelineFinder   func(rootPath string, pipelineDefinitionFileName string) ([]string, error)
		setupBuilderMock func(m *mockPipelineBuilder)
		rules            []Rule
	}

	type args struct {
		rootPath                   string
		pipelineDefinitionFileName string
	}
	tests := []struct {
		name          string
		pipelinePaths []string
		fields        fields
		args          args
		wantErr       bool
	}{
		{
			name: "pipeline finder returned an error",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return nil, errors.New("cannot find pipelines")
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "pipeline finder returned a file not found error",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return nil, os.ErrNotExist
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "empty file list returned",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "found nested pipelines",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline1/some-other-pipeline/under-here", "path/to/pipeline2"}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, first rule fails",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline2", "path/to/pipeline2_some_other_name"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1").
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2").
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2_some_other_name").
						Return(&pipeline.Pipeline{}, nil)
				},
				rules: []Rule{errorRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, second rule fails",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline2"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1").
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2").
						Return(&pipeline.Pipeline{}, nil)
				},
				rules: []Rule{successRule, errorRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, second rule fails",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline2"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1").
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2").
						Return(&pipeline.Pipeline{}, nil)
				},
				rules: []Rule{successRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := new(mockPipelineBuilder)
			if tt.fields.setupBuilderMock != nil {
				tt.fields.setupBuilderMock(m)
			}

			logger := zap.NewNop()
			l := &Linter{
				findPipelines: tt.fields.pipelineFinder,
				builder:       m,
				rules:         tt.fields.rules,
				logger:        logger.Sugar(),
			}

			_, err := l.Lint(tt.args.rootPath, tt.args.pipelineDefinitionFileName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			m.AssertExpectations(t)
		})
	}
}

func TestPipelineAnalysisResult_ErrorCount(t *testing.T) {
	t.Parallel()

	rule1 := &SimpleRule{Identifier: "rule1"}
	rule2 := &SimpleRule{Identifier: "rule2"}
	tests := []struct {
		name      string
		pipelines []*PipelineIssues
		want      int
	}{
		{
			pipelines: []*PipelineIssues{
				{
					Pipeline: &pipeline.Pipeline{
						Name: "pipeline1",
					},
					Issues: map[Rule][]*Issue{
						rule1: {
							{
								Description: "issue1",
							},
							{
								Description: "issue2",
							},
						},
						rule2: {
							{
								Description: "issue1",
							},
							{
								Description: "issue2",
							},
						},
					},
				},
				{
					Pipeline: &pipeline.Pipeline{
						Name: "pipeline2",
					},
					Issues: map[Rule][]*Issue{
						rule1: {
							{
								Description: "issue1",
							},
							{
								Description: "issue2",
							},
						},
					},
				},
			},
			want: 6,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &PipelineAnalysisResult{
				Pipelines: tt.pipelines,
			}
			assert.Equal(t, tt.want, p.ErrorCount())
		})
	}
}
