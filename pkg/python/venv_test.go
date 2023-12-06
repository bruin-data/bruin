package python

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockConfigManager struct {
	mock.Mock
}

func (m *mockConfigManager) EnsureVirtualenvDirExists() error {
	return m.Called().Error(0)
}

func (m *mockConfigManager) MakeVirtualenvPath(dir string) string {
	return m.Called(dir).String(0)
}

type mockCmd struct {
	mock.Mock
}

func (m *mockCmd) Run(ctx context.Context, repo *git.Repo, cmd *command) error {
	return m.Called(ctx, repo, cmd).Error(0)
}

func Test_installReqsToHomeDir_EnsureVirtualEnvExists(t *testing.T) {
	t.Parallel()

	type fields struct {
		fs     afero.Fs
		config *mockConfigManager
		cmd    *mockCmd
	}
	repo := &git.Repo{}
	requirementsTxt := "/path1/requirements.txt"

	// this is the hash for the content 'req1\nreq2' below
	fileHash := "147ed061038b695a06605f70c4e966c602effd55ab6cc50315787fe7f5633d81"
	validReqsContent := "req1\nreq2"

	createRequirementsFile := func(fs afero.Fs, content string) {
		file, err := fs.Create(requirementsTxt)
		require.NoError(t, err)
		defer func() { require.NoError(t, file.Close()) }()
		_, err = file.Write([]byte(content))
		require.NoError(t, err)
	}

	tests := []struct {
		name    string
		fields  func() *fields
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "should return error if EnsureVirtualenvDirExists fails",
			fields: func() *fields {
				fs := afero.NewMemMapFs()
				config := new(mockConfigManager)
				config.On("EnsureVirtualenvDirExists").
					Return(assert.AnError)

				return &fields{
					fs:     fs,
					config: config,
					cmd:    &mockCmd{},
				}
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if cannot read the reqs file",
			fields: func() *fields {
				fs := afero.NewMemMapFs()
				config := new(mockConfigManager)
				config.On("EnsureVirtualenvDirExists").Return(nil)

				return &fields{
					fs:     fs,
					config: config,
					cmd:    &mockCmd{},
				}
			},
			wantErr: assert.Error,
		},
		{
			name: "should return empty if the requirements file is empty",
			fields: func() *fields {
				config := new(mockConfigManager)
				config.On("EnsureVirtualenvDirExists").Return(nil)

				fs := afero.NewMemMapFs()
				createRequirementsFile(fs, "")

				return &fields{
					fs:     fs,
					config: config,
					cmd:    &mockCmd{},
				}
			},
			want:    "",
			wantErr: assert.NoError,
		},
		{
			name: "should return empty if the requirements file has some spaces in it",
			fields: func() *fields {
				config := new(mockConfigManager)
				config.On("EnsureVirtualenvDirExists").Return(nil)

				fs := afero.NewMemMapFs()
				createRequirementsFile(fs, "   \n\n  \n  \t\t  \n")

				return &fields{
					fs:     fs,
					config: config,
					cmd:    &mockCmd{},
				}
			},
			want:    "",
			wantErr: assert.NoError,
		},
		{
			name: "should skip creating a venv if the venv already exists",
			fields: func() *fields {
				fs := afero.NewMemMapFs()

				config := new(mockConfigManager)
				config.On("EnsureVirtualenvDirExists").Return(nil)

				config.On("MakeVirtualenvPath", fileHash).
					Return("/path/to/venv")

				createRequirementsFile(fs, "req1\nreq2")

				require.NoError(t, fs.MkdirAll("/path/to/venv", 0o755))
				return &fields{
					fs:     fs,
					config: config,
					cmd:    &mockCmd{},
				}
			},
			want:    "/path/to/venv",
			wantErr: assert.NoError,
		},
		{
			name: "should create the venv if it doesnt exist",
			fields: func() *fields {
				fs := afero.NewMemMapFs()

				config := new(mockConfigManager)
				config.On("EnsureVirtualenvDirExists").Return(nil)

				// this returns a path that doesn't exist in the filesystem,
				// that's how this test is validating that the venv is created
				config.On("MakeVirtualenvPath", fileHash).
					Return("/path/to/venv")

				createRequirementsFile(fs, validReqsContent)

				fakeCmd := new(mockCmd)
				fakeCmd.On("Run", mock.Anything, repo, &command{
					Name: "python3",
					Args: []string{"-m", "venv", "/path/to/venv"},
				}).Return(nil)
				fakeCmd.On("Run", mock.Anything, repo, &command{
					Name: "/bin/sh",
					Args: []string{"-c", "source /path/to/venv/bin/activate && pip3 install -r /path1/requirements.txt --quiet --quiet && echo 'installed all the dependencies'"},
				}).Return(nil)

				return &fields{
					fs:     fs,
					config: config,
					cmd:    fakeCmd,
				}
			},
			wantErr: assert.NoError,
			want:    "/path/to/venv",
		},
		{
			name: "the venv is created successfully",
			fields: func() *fields {
				fs := afero.NewMemMapFs()

				config := new(mockConfigManager)
				config.On("EnsureVirtualenvDirExists").Return(nil)

				// this returns a path that doesn't exist in the filesystem,
				// that's how this test is validating that the venv is created
				config.On("MakeVirtualenvPath", fileHash).
					Return("/path/to/venv")

				createRequirementsFile(fs, validReqsContent)

				fakeCmd := new(mockCmd)
				fakeCmd.On("Run", mock.Anything, repo, &command{
					Name: "python3",
					Args: []string{"-m", "venv", "/path/to/venv"},
				}).Return(assert.AnError)

				return &fields{
					fs:     fs,
					config: config,
					cmd:    fakeCmd,
				}
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := tt.fields()
			i := &installReqsToHomeDir{
				fs:     f.fs,
				config: f.config,
				cmd:    f.cmd,
			}

			got, err := i.EnsureVirtualEnvExists(context.Background(), repo, requirementsTxt)
			tt.wantErr(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
