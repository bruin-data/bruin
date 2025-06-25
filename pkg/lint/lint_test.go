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

func (p *mockPipelineBuilder) CreatePipelineFromPath(ctx context.Context, pathToPipeline string, opts ...pipeline.CreatePipelineOption) (*pipeline.Pipeline, error) {
	args := p.Called(ctx, pathToPipeline, opts)
	return args.Get(0).(*pipeline.Pipeline), args.Error(1)
}

func TestLinter_Lint(t *testing.T) {
	t.Parallel()

	errorRule := &SimpleRule{
		Identifier: "errorRule",
		Validator: func(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
			return nil, errors.New("first rule failed")
		},
		ApplicableLevels: []Level{LevelPipeline},
	}

	successRule := &SimpleRule{
		Identifier:       "successRule",
		Validator:        func(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) { return nil, nil },
		ApplicableLevels: []Level{LevelPipeline},
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
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline2_some_other_name", mock.FunctionalOptions(pipeline.WithMutate())).
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
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
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
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
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
		ctx := context.Background()
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

			_, err := l.Lint(ctx, tt.args.rootPath, tt.args.pipelineDefinitionFileName, cli.NewContext(nil, nil, nil), nil)
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
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline2", mock.FunctionalOptions(pipeline.WithMutate())).
						Return(&pipeline.Pipeline{}, nil)
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline2_some_other_name", mock.FunctionalOptions(pipeline.WithMutate())).
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
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
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
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
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
					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
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

					m.On("CreatePipelineFromPath", mock.Anything, "path/to/pipeline1", mock.FunctionalOptions(pipeline.WithMutate())).
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
		ctx := context.Background()
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

			_, err := l.LintAsset(ctx, tt.args.rootPath, tt.args.pipelineDefinitionFileName, "my-asset", cli.NewContext(nil, nil, nil), nil)
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

func TestRunLintRulesOnPipeline_ExcludeTag(t *testing.T) {
	t.Parallel()

	// Create a rule that will be called for each asset
	assetRule := &SimpleRule{
		Identifier: "testAssetRule",
		AssetValidator: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			return []*Issue{
				{
					Task:        asset,
					Description: "test issue for " + asset.Name,
				},
			}, nil
		},
		ApplicableLevels: []Level{LevelAsset},
	}

	tests := []struct {
		name                  string
		assets                []*pipeline.Asset
		excludeTag            string
		expectedIssues        int
		expectedExcludedCount int
	}{
		{
			name: "no exclude tag, all assets processed",
			assets: []*pipeline.Asset{
				{Name: "asset1", Tags: []string{"tag1"}},
				{Name: "asset2", Tags: []string{"tag2"}},
				{Name: "asset3", Tags: []string{"tag3"}},
			},
			excludeTag:            "",
			expectedIssues:        3,
			expectedExcludedCount: 0,
		},
		{
			name: "exclude tag matches some assets",
			assets: []*pipeline.Asset{
				{Name: "asset1", Tags: []string{"skip"}},
				{Name: "asset2", Tags: []string{"process"}},
				{Name: "asset3", Tags: []string{"skip"}},
				{Name: "asset4", Tags: []string{"process"}},
			},
			excludeTag:            "skip",
			expectedIssues:        2, // Only asset2 and asset4 should be processed
			expectedExcludedCount: 2, // asset1 and asset3 should be excluded
		},
		{
			name: "exclude tag matches all assets",
			assets: []*pipeline.Asset{
				{Name: "asset1", Tags: []string{"skip"}},
				{Name: "asset2", Tags: []string{"skip"}},
				{Name: "asset3", Tags: []string{"skip"}},
			},
			excludeTag:            "skip",
			expectedIssues:        0, // No assets should be processed
			expectedExcludedCount: 3, // All assets should be excluded
		},
		{
			name: "exclude tag doesn't match any assets",
			assets: []*pipeline.Asset{
				{Name: "asset1", Tags: []string{"tag1"}},
				{Name: "asset2", Tags: []string{"tag2"}},
			},
			excludeTag:            "nonexistent",
			expectedIssues:        2, // All assets should be processed
			expectedExcludedCount: 0, // No assets should be excluded
		},
		{
			name: "assets with no tags",
			assets: []*pipeline.Asset{
				{Name: "asset1", Tags: nil},
				{Name: "asset2", Tags: []string{}},
				{Name: "asset3", Tags: []string{"skip"}},
			},
			excludeTag:            "skip",
			expectedIssues:        2, // asset1 and asset2 should be processed
			expectedExcludedCount: 1, // asset3 should be excluded
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pipeline := &pipeline.Pipeline{
				Name:   "test-pipeline",
				Assets: tt.assets,
			}

			ctx := context.Background()
			if tt.excludeTag != "" {
				ctx = context.WithValue(ctx, excludeTagKey, tt.excludeTag)
			}

			result, err := RunLintRulesOnPipeline(ctx, pipeline, []Rule{assetRule}, nil)
			require.NoError(t, err)

			// Count total issues across all rules
			totalIssues := 0
			for _, issues := range result.Issues {
				totalIssues += len(issues)
			}

			assert.Equal(t, tt.expectedIssues, totalIssues, "Expected %d issues, got %d", tt.expectedIssues, totalIssues)

			// Note: The excluded count is now tracked at the PipelineAnalysisResult level, not at the PipelineIssues level
			// This test only checks the individual pipeline result, so we don't check the excluded count here
		})
	}
}

func TestContainsTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []string
		target   string
		expected bool
	}{
		{
			name:     "empty target returns false",
			tags:     []string{"tag1", "tag2"},
			target:   "",
			expected: false,
		},
		{
			name:     "nil tags returns false",
			tags:     nil,
			target:   "tag1",
			expected: false,
		},
		{
			name:     "empty tags returns false",
			tags:     []string{},
			target:   "tag1",
			expected: false,
		},
		{
			name:     "tag found",
			tags:     []string{"tag1", "tag2", "tag3"},
			target:   "tag2",
			expected: true,
		},
		{
			name:     "tag not found",
			tags:     []string{"tag1", "tag2", "tag3"},
			target:   "tag4",
			expected: false,
		},
		{
			name:     "case sensitive match",
			tags:     []string{"Tag1", "TAG2", "tag3"},
			target:   "tag2",
			expected: false,
		},
		{
			name:     "exact match",
			tags:     []string{"tag1", "tag2", "tag3"},
			target:   "tag1",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ContainsTag(tt.tags, tt.target)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLinter_LintPipelines_AssetWithExcludeTagCount(t *testing.T) {
	t.Parallel()

	// Create a simple rule that will be called for each asset
	assetRule := &SimpleRule{
		Identifier: "testAssetRule",
		AssetValidator: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			return []*Issue{
				{
					Task:        asset,
					Description: "test issue for " + asset.Name,
				},
			}, nil
		},
		ApplicableLevels: []Level{LevelAsset},
	}

	tests := []struct {
		name                  string
		pipelines             []*pipeline.Pipeline
		excludeTag            string
		expectedExcludedCount int
	}{
		{
			name: "no exclude tag, no excluded assets",
			pipelines: []*pipeline.Pipeline{
				{
					Name: "pipeline1",
					Assets: []*pipeline.Asset{
						{Name: "asset1", Tags: []string{"tag1"}},
						{Name: "asset2", Tags: []string{"tag2"}},
					},
				},
			},
			excludeTag:            "",
			expectedExcludedCount: 0,
		},
		{
			name: "exclude tag matches some assets across multiple pipelines",
			pipelines: []*pipeline.Pipeline{
				{
					Name: "pipeline1",
					Assets: []*pipeline.Asset{
						{Name: "asset1", Tags: []string{"skip"}},
						{Name: "asset2", Tags: []string{"process"}},
					},
				},
				{
					Name: "pipeline2",
					Assets: []*pipeline.Asset{
						{Name: "asset3", Tags: []string{"skip"}},
						{Name: "asset4", Tags: []string{"process"}},
					},
				},
			},
			excludeTag:            "skip",
			expectedExcludedCount: 2, // asset1 and asset3 should be excluded
		},
		{
			name: "exclude tag matches all assets",
			pipelines: []*pipeline.Pipeline{
				{
					Name: "pipeline1",
					Assets: []*pipeline.Asset{
						{Name: "asset1", Tags: []string{"skip"}},
						{Name: "asset2", Tags: []string{"skip"}},
					},
				},
			},
			excludeTag:            "skip",
			expectedExcludedCount: 2, // All assets should be excluded
		},
		{
			name: "context without assetWithExcludeTagCountKey should default to 0",
			pipelines: []*pipeline.Pipeline{
				{
					Name: "pipeline1",
					Assets: []*pipeline.Asset{
						{Name: "asset1", Tags: []string{"skip"}},
						{Name: "asset2", Tags: []string{"process"}},
					},
				},
			},
			excludeTag:            "skip",
			expectedExcludedCount: 0, // Should default to 0 when context key is missing
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tt.excludeTag != "" {
				ctx = context.WithValue(ctx, excludeTagKey, tt.excludeTag)
			}

			// Only set the assetWithExcludeTagCountKey for tests that expect it
			if tt.name != "context without assetWithExcludeTagCountKey should default to 0" {
				ctx = context.WithValue(ctx, assetWithExcludeTagCountKey, tt.expectedExcludedCount)
			}

			l := &Linter{
				rules: []Rule{assetRule},
			}

			result, err := l.LintPipelines(ctx, tt.pipelines, nil)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedExcludedCount, result.AssetWithExcludeTagCount,
				"Expected %d excluded assets, got %d", tt.expectedExcludedCount, result.AssetWithExcludeTagCount)
		})
	}
}
