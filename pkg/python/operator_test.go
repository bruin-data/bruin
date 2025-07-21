package python

import (
	"context"
	"testing"

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
		name    string
		task    *pipeline.Asset
		setup   func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder)
		wantErr assert.ErrorAssertionFunc
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
			name: "should fail if requirement finding fails",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("path.to.module", nil)

				mf.On("FindRequirementsTxtInPath", repo.Path, mock.Anything).
					Return("", assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should call runner if there is no requirements.txt file",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("path.to.module", nil)

				mf.On("FindRequirementsTxtInPath", repo.Path, mock.Anything).
					Return("", &NoRequirementsFoundError{})

				runner.On("Run", mock.Anything, &executionContext{
					repo:            repo,
					module:          "path.to.module",
					requirementsTxt: "",
					asset:           task,
					envVariables: map[string]string{
						"BRUIN_ASSET":           "my-asset",
						"BRUIN_START_DATE":      "2024-01-01",
						"BRUIN_START_DATETIME":  "2024-01-01T00:00:00",
						"BRUIN_START_TIMESTAMP": "2024-01-01T00:00:00.000000Z",
						"BRUIN_END_DATE":        "2024-01-01",
						"BRUIN_END_DATETIME":    "2024-01-01T00:00:00",
						"BRUIN_END_TIMESTAMP":   "2024-01-01T00:00:00.000000Z",
						"BRUIN_PIPELINE":        "test-pipeline",
						"BRUIN_RUN_ID":          "test-run",
					},
				}).
					Return(assert.AnError)
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
					Return("path.to.module", nil)

				mf.On("FindRequirementsTxtInPath", repo.Path, mock.Anything).
					Return("", &NoRequirementsFoundError{})

				msf.On("GetConnection", "key1").Return(config.GenericConnection{
					Value: "value1",
				})
				msf.On("GetConnection", "key2").Return(config.GenericConnection{
					Value: "value2",
				})

				runner.On("Run", mock.Anything, &executionContext{
					repo:            repo,
					module:          "path.to.module",
					requirementsTxt: "",
					asset:           assetWithSecrets,
					envVariables: map[string]string{
						"key1_injected":         "value1",
						"key2":                  "value2",
						"BRUIN_ASSET":           "my-asset",
						"BRUIN_START_DATE":      "2024-01-01",
						"BRUIN_START_DATETIME":  "2024-01-01T00:00:00",
						"BRUIN_START_TIMESTAMP": "2024-01-01T00:00:00.000000Z",
						"BRUIN_END_DATE":        "2024-01-01",
						"BRUIN_END_DATETIME":    "2024-01-01T00:00:00",
						"BRUIN_END_TIMESTAMP":   "2024-01-01T00:00:00.000000Z",
					},
				}).
					Return(assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should call runner with the found requirements.txt file",
			task: task,
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner, msf *mockSecretFinder) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("path.to.module", nil)

				mf.On("FindRequirementsTxtInPath", repo.Path, mock.Anything).
					Return("/path/to/requirements.txt", nil)

				runner.On("Run", mock.Anything, &executionContext{
					repo:            repo,
					module:          "path.to.module",
					requirementsTxt: "/path/to/requirements.txt",
					asset:           task,
					envVariables: map[string]string{
						"BRUIN_ASSET":           "my-asset",
						"BRUIN_START_DATE":      "2024-01-01",
						"BRUIN_START_DATETIME":  "2024-01-01T00:00:00",
						"BRUIN_START_TIMESTAMP": "2024-01-01T00:00:00.000000Z",
						"BRUIN_END_DATE":        "2024-01-01",
						"BRUIN_END_DATETIME":    "2024-01-01T00:00:00",
						"BRUIN_END_TIMESTAMP":   "2024-01-01T00:00:00.000000Z",
					},
				}).Return(assert.AnError)
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

			tt.wantErr(t, o.RunTask(context.Background(), nil, tt.task))
		})
	}
}
