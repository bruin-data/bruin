package lint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

type mockPipelineBuilder struct {
	mock.Mock
}

func (p *mockPipelineBuilder) CreatePipelineFromPath(pathToPipeline string, opts ...pipeline.CreatePipelineOption) (*pipeline.Pipeline, error) {
	args := p.Called(pathToPipeline, opts)
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
		pipelineFinder   func(rootPath string, pipelineDefinitionFileName []string) ([]string, error)
		setupBuilderMock func(m *mockPipelineBuilder)
		rules            []Rule
	}

	type args struct {
		rootPath                   string
		pipelineDefinitionFileName []string
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
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return nil, errors.New("cannot find pipelines")
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: true,
		},
		{
			name: "pipeline finder returned a file not found error",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return nil, os.ErrNotExist
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: true,
		},
		{
			name: "empty file list returned",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: true,
		},
		{
			name: "found nested pipelines",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1", "path/to/pipeline1/some-other-pipeline/under-here", "path/to/pipeline2"}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, first rule fails",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1", "path/to/pipeline2", "path/to/pipeline2_some_other_name"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2_some_other_name", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
				},
				rules: []Rule{errorRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, second rule fails",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1", "path/to/pipeline2"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
				},
				rules: []Rule{successRule, errorRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, second rule fails",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1", "path/to/pipeline2"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
				},
				rules: []Rule{successRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
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

			_, err := l.Lint(tt.args.rootPath, tt.args.pipelineDefinitionFileName, cli.NewContext(nil, nil, nil))
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			m.AssertExpectations(t)
		})
	}
}

func TestLinter_LintAsset(t *testing.T) {
	t.Parallel()

	errorRule := &SimpleRule{
		Identifier: "errorRule",
		AssetValidator: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			return nil, errors.New("first rule failed")
		},
	}

	successRule := &SimpleRule{
		Identifier: "successRule",
		AssetValidator: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			return nil, nil
		},
	}

	type fields struct {
		pipelineFinder   func(rootPath string, pipelineDefinitionFileName []string) ([]string, error)
		setupBuilderMock func(m *mockPipelineBuilder)
		rules            []Rule
	}

	type args struct {
		rootPath                   string
		pipelineDefinitionFileName []string
	}
	tests := []struct {
		name          string
		pipelinePaths []string
		fields        fields
		args          args
		wantErr       bool
		errorMessage  string
	}{
		{
			name: "pipeline finder returned an error",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return nil, errors.New("cannot find pipelines")
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr:      true,
			errorMessage: "error getting pipeline paths: cannot find pipelines",
		},
		{
			name: "pipeline finder returned a file not found error",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return nil, os.ErrNotExist
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr:      true,
			errorMessage: "the given pipeline path does not exist, please make sure you gave the right path",
		},
		{
			name: "empty file list returned",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr:      true,
			errorMessage: "no pipelines found in path 'some-root-path'",
		},
		{
			name: "found nested pipelines",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1", "path/to/pipeline1/some-other-pipeline/under-here", "path/to/pipeline2"}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr:      true,
			errorMessage: "nested pipelines are not allowed: seems like 'path/to/pipeline1' is already a parent pipeline for 'path/to/pipeline1/some-other-pipeline/under-here'",
		},
		{
			name: "there's no asset with the name or path",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1", "path/to/pipeline2", "path/to/pipeline2_some_other_name"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", "path/to/pipeline2_some_other_name", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
				},
				rules: []Rule{errorRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr:      true,
			errorMessage: "failed to find an asset with the path or name 'my-asset' under the path 'some-root-path'",
		},
		{
			name: "rules are properly applied, first rule fails",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{
							Assets: []*pipeline.Asset{
								{Name: "my-asset"},
							},
						}, nil)
				},
				rules: []Rule{errorRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr:      true,
			errorMessage: "first rule failed",
		},
		{
			name: "rules are properly applied, second rule fails",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{
							Assets: []*pipeline.Asset{
								{Name: "my-asset"},
							},
						}, nil)
				},
				rules: []Rule{successRule, errorRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr:      true,
			errorMessage: "first rule failed",
		},
		{
			name: "rules are properly applied, they all pass when asset name is used",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{
							Assets: []*pipeline.Asset{
								{Name: "my-asset"},
							},
						}, nil)
				},
				rules: []Rule{successRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: false,
		},
		{
			name: "rules are properly applied, they all pass when asset path is used",
			fields: fields{
				pipelineFinder: func(root string, fileName []string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, []string{"some-file-name"}, fileName)

					return []string{"path/to/pipeline1"}, nil
				},
				setupBuilderMock: func(m *mockPipelineBuilder) {
					abspath, err := filepath.Abs("./my-asset")
					require.NoError(t, err)

					m.On("CreatePipelineFromPath", "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{
							Assets: []*pipeline.Asset{
								{
									DefinitionFile: pipeline.TaskDefinitionFile{Path: abspath},
								},
							},
						}, nil)
				},
				rules: []Rule{successRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: []string{"some-file-name"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
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

			_, err := l.LintAsset(tt.args.rootPath, tt.args.pipelineDefinitionFileName, "my-asset", cli.NewContext(nil, nil, nil))
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			} else {
				require.NoError(t, err)
			}

			m.AssertExpectations(t)
		})
	}
}

func TestPipelineAnalysisResult_ErrorCount(t *testing.T) {
	t.Parallel()

	rule1 := &SimpleRule{Identifier: "rule1", Severity: ValidatorSeverityCritical}
	rule2 := &SimpleRule{Identifier: "rule2", Severity: ValidatorSeverityCritical}
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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &PipelineAnalysisResult{
				Pipelines: tt.pipelines,
			}
			assert.Equal(t, tt.want, p.ErrorCount())
		})
	}
}
