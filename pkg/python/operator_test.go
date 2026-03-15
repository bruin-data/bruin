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

const testModule = "path.to.module"

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
						ec.envVariables["key1"] == "value1" &&
						ec.envVariables["key2"] == "value2" &&
						ec.envVariables["BRUIN_CONNECTION_TYPES"] == `{"key1":"generic","key1_injected":"generic","key2":"generic"}`
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

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)

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
		{
			name: "should return error when default inject_as conflicts with explicit inject_as of another secret",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "key1",
						InjectedKey: "key1", // defaulted from key
					},
					{
						SecretKey:   "key2",
						InjectedKey: "key1", // explicit inject_as collides
					},
				},
			},
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)
			},
			wantErr: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorContains(t, err, "duplicate env var name 'key1'")
			},
		},
		{
			name: "should return error when same key defined twice without inject_as",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "key1",
						InjectedKey: "key1", // defaulted from key
					},
					{
						SecretKey:   "key1",
						InjectedKey: "key1", // defaulted from key, duplicate
					},
				},
			},
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)
			},
			wantErr: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorContains(t, err, "duplicate secret mapping: secret 'key1' is injected as 'key1' more than once")
			},
		},
		{
			name: "should return error for duplicate inject_as across secrets",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "key1",
						InjectedKey: "MY_VAR",
					},
					{
						SecretKey:   "key2",
						InjectedKey: "MY_VAR",
					},
				},
			},
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)
			},
			wantErr: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorContains(t, err, "duplicate env var name 'MY_VAR'")
			},
		},
		{
			name: "should return error when inject_as conflicts with another secret key",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "key1",
						InjectedKey: "key1",
					},
					{
						SecretKey:   "key2",
						InjectedKey: "key1",
					},
				},
			},
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)
			},
			wantErr: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorContains(t, err, "duplicate env var name 'key1'")
			},
		},
		{
			name: "should return error when secret key conflicts with inject_as of another secret",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "key1",
						InjectedKey: "INJECTED_NAME",
					},
					{
						SecretKey:   "INJECTED_NAME",
						InjectedKey: "INJECTED_NAME",
					},
				},
			},
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)
			},
			wantErr: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorContains(t, err, "duplicate env var name 'INJECTED_NAME'")
			},
		},
		{
			name: "should return error when secret key collides with another secrets inject_as expansion",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "conn-a",
						InjectedKey: "conn-b",
					},
					{
						SecretKey:   "conn-b",
						InjectedKey: "SOME_OTHER_VAR",
					},
				},
			},
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)
			},
			wantErr: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorContains(t, err, "secret key 'conn-b' conflicts with env var already claimed by secret 'conn-a'")
			},
		},
		{
			name: "should support same secret key with different inject_as values",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "gcp-default",
						InjectedKey: "GOOGLE_APPLICATION_CREDENTIALS",
					},
					{
						SecretKey:   "gcp-default",
						InjectedKey: "GCP_CREDS",
					},
				},
			},
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
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)

				msf.On("GetConnectionDetails", "gcp-default").Return(&config.GenericConnection{
					Value: "cred-value",
				})
				msf.On("GetConnectionType", "gcp-default").Return("google_cloud_platform")

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.envVariables["GOOGLE_APPLICATION_CREDENTIALS"] == "cred-value" &&
						ec.envVariables["GCP_CREDS"] == "cred-value" &&
						ec.envVariables["gcp-default"] == "cred-value"
				})).Return(nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should return error when secret connection does not exist",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "nonexistent",
						InjectedKey: "MY_SECRET",
					},
				},
			},
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
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)

				msf.On("GetConnectionDetails", "nonexistent").Return(nil)
			},
			wantErr: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorContains(t, err, "there's no secret with the name 'nonexistent'")
			},
		},
		{
			name: "should inject non-generic connection as JSON with both keys",
			task: &pipeline.Asset{
				Name: "my-asset",
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "my-bq",
						InjectedKey: "BQ_CONN",
					},
				},
			},
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
					Return(testModule, nil)

				mf.On("FindDependencyConfig", repo.Path, mock.Anything).
					Return(&DependencyConfig{Type: DependencyTypeNone}, nil)

				// Return a non-GenericConnection (a map will be JSON-marshaled)
				connDetails := map[string]string{"project": "my-project", "dataset": "my-dataset"}
				msf.On("GetConnectionDetails", "my-bq").Return(connDetails)
				msf.On("GetConnectionType", "my-bq").Return("bigquery")

				runner.On("Run", mock.Anything, mock.MatchedBy(func(ec *executionContext) bool {
					return ec.envVariables["BQ_CONN"] != "" &&
						ec.envVariables["my-bq"] != "" &&
						ec.envVariables["BQ_CONN"] == ec.envVariables["my-bq"]
				})).Return(nil)
			},
			wantErr: assert.NoError,
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
