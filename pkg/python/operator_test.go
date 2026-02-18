package python

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRepoFinder struct {
	mock.Mock
}

func (m *mockRepoFinder) Repo(path string) (*git.Repo, error) {
	args := m.Called(path)
	return args.Get(0).(*git.Repo), args.Error(1)
}

type mockModuleFinder struct {
	mock.Mock
}

func (m *mockModuleFinder) FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error) {
	args := m.Called(repo, executable)
	return args.Get(0).(string), args.Error(1)
}

func (m *mockModuleFinder) FindRequirementsTxtInPath(path string, executable *pipeline.ExecutableFile) (string, error) {
	args := m.Called(path, executable)
	return args.Get(0).(string), args.Error(1)
}

func (m *mockModuleFinder) FindDependencyConfig(path string, executable *pipeline.ExecutableFile) (*DependencyConfig, error) {
	args := m.Called(path, executable)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*DependencyConfig), args.Error(1)
}

type mockRunner struct {
	mock.Mock
}

func (m *mockRunner) Run(ctx context.Context, ec *executionContext) error {
	args := m.Called(ctx, ec)
	return args.Error(0)
}

type mockSecretFinder struct {
	mock.Mock
}

func (m *mockSecretFinder) GetConnection(name string) any {
	args := m.Called(name)
	return args.Get(0)
}

func (m *mockSecretFinder) GetConnectionDetails(name string) any {
	args := m.Called(name)
	return args.Get(0)
}

func (m *mockSecretFinder) GetConnectionType(name string) string {
	args := m.Called(name)
	return args.String(0)
}

func TestLocalOperator_RunTask(t *testing.T) {
	t.Parallel()

	task := &pipeline.Asset{
		Name: "my-asset",
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/file.py",
		},
	}

	assetWithConnection := &pipeline.Asset{
		Name: "my-asset",
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/file.py",
		},
		Connection: "my_bigquery",
	}

	assetWithSecrets := &pipeline.Asset{
		Name: "my-asset",
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/file.py",
		},
		Secrets: []pipeline.SecretMapping{
			{
				SecretKey:   "key1",
				InjectedKey: "key1_injected",
			},
			{
				SecretKey:   "key2",
				InjectedKey: "key2",
			},
		},
	}
	tests := []struct {
		name     string
		task     *pipeline.Asset
		pipeline *pipeline.Pipeline
		ctx      func(t *testing.T) context.Context
		setup    func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder)
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name: "should return an error if the repo finder fails",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				rf.On("Repo", "/path/to/file.py").
					Return(&git.Repo{}, assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should return an error if the module path finder fails",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("", assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should continue without dependencies if FindDependencyConfig fails",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(nil, assert.AnError)

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.repo == repo &&
						ec.module == testModule &&
						ec.requirementsTxt == "" &&
						ec.dependencyConfig != nil &&
						ec.dependencyConfig.Type == DependencyTypeNone
				})).Return(assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should call runner if there is no dependency configuration",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.repo == repo &&
						ec.module == testModule &&
						ec.requirementsTxt == "" &&
						ec.dependencyConfig.Type == DependencyTypeNone &&
						ec.envVariables["BRUIN_ASSET"] == "my-asset"
				})).Return(assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should call runner with the secrets properly injected",
			task: assetWithSecrets,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)

				msf.On("GetConnectionDetails", "key1").Return(&config.GenericConnection{
					Value: "value1",
				})
				msf.On("GetConnectionDetails", "key2").Return(&config.GenericConnection{
					Value: "value2",
				})
				msf.On("GetConnectionType", "key1").Return("generic")
				msf.On("GetConnectionType", "key2").Return("generic")

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.repo == repo &&
						ec.module == testModule &&
						ec.requirementsTxt == "" &&
						ec.dependencyConfig.Type == DependencyTypeNone &&
						ec.envVariables["key1_injected"] == "value1" &&
						ec.envVariables["key2"] == "value2" &&
						ec.envVariables["BRUIN_CONNECTION_TYPES"] == `{"key1_injected":"generic","key2":"generic"}`
				})).Return(assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name:     "should inject BRUIN_CONNECTION when asset has a connection",
			task:     assetWithConnection,
			pipeline: &pipeline.Pipeline{Name: "test-pipeline"},
			ctx: func(t *testing.T) context.Context {
				ctx := t.Context()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run")
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)
				return ctx
			},
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("path.to.module", nil)

				mf.On("FindRequirementsTxtInPath", repo.Path, mock.Anything).
					Return("", &NoRequirementsFoundError{})

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.module == "path.to.module" &&
						ec.envVariables["BRUIN_CONNECTION"] == "my_bigquery" &&
						ec.envVariables["BRUIN_ASSET"] == "my-asset" &&
						ec.envVariables["BRUIN_THIS"] == "my-asset" &&
						ec.envVariables["BRUIN_PIPELINE"] == "test-pipeline"
				})).
					Return(nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should call runner with the found requirements.txt file",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{
						Type:            DependencyTypeRequirementsTxt,
						RequirementsTxt: "/path/to/requirements.txt",
						ProjectRoot:     "/path/to",
					}, nil)

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.repo == repo &&
						ec.module == testModule &&
						ec.requirementsTxt == "/path/to/requirements.txt" &&
						ec.dependencyConfig.Type == DependencyTypeRequirementsTxt &&
						ec.envVariables["BRUIN_ASSET"] == "my-asset"
				})).Return(assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should call runner with pyproject.toml configuration",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{
						Type:          DependencyTypePyproject,
						PyprojectPath: "/path/to/pyproject.toml",
						ProjectRoot:   "/path/to",
					}, nil)

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.repo == repo &&
						ec.module == testModule &&
						ec.requirementsTxt == "" &&
						ec.dependencyConfig.Type == DependencyTypePyproject &&
						ec.dependencyConfig.PyprojectPath == "/path/to/pyproject.toml"
				})).Return(assert.AnError)
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mockRepoFinder{}
			module := &mockModuleFinder{}
			runner := &mockRunner{}
			secret := &mockSecretFinder{}
			if tt.setup != nil {
				tt.setup(repo, module, runner, secret)
			}

			o := &LocalOperator{
				repoFinder: repo,
				module:     module,
				runner:     runner,
				config:     secret,
			}

			var testCtx context.Context
			if tt.ctx != nil {
				testCtx = tt.ctx(t)
			} else {
				testCtx = t.Context()
			}
			p := tt.pipeline
			tt.wantErr(t, o.RunTask(testCtx, p, tt.task))
		})
	}
}
