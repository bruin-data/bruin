package python

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockUvInstaller struct {
	mock.Mock
}

func (m *mockUvInstaller) EnsureUvInstalled(ctx context.Context) (string, error) {
	called := m.Called(ctx)
	return called.String(0), called.Error(1)
}

func Test_uvPythonRunner_Run(t *testing.T) {
	t.Parallel()

	type fields struct {
		cmd         cmd
		uvInstaller uvInstaller
	}

	repo := &git.Repo{}

	module := "path.to.module"
	tests := []struct {
		name    string
		fields  func() *fields
		execCtx *executionContext
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "if no dependencies are found the basic CommandInstance should be executed, and error should be propagated",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &CommandInstance{
					Name: "~/.bruin/uv",
					Args: []string{"run", "--no-config", "--no-sync", "--python", "3.11", "--module", module},
				}).Return(assert.AnError)

				inst := new(mockUvInstaller)
				inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

				return &fields{
					cmd:         cmd,
					uvInstaller: inst,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "",
				asset: &pipeline.Asset{
					Image: "",
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "requirements should be installed",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &CommandInstance{
					Name: "~/.bruin/uv",
					Args: []string{"run", "--no-config", "--no-sync", "--python", "3.11", "--with-requirements", "/path/to/requirements.txt", "--module", module},
				}).Return(assert.AnError)

				inst := new(mockUvInstaller)
				inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

				return &fields{
					cmd:         cmd,
					uvInstaller: inst,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "/path/to/requirements.txt",
				asset: &pipeline.Asset{
					Image: "",
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "different python versions are supported",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &CommandInstance{
					Name: "~/.bruin/uv",
					Args: []string{"run", "--no-config", "--no-sync", "--python", "3.13", "--with-requirements", "/path/to/requirements.txt", "--module", module},
				}).Return(assert.AnError)

				inst := new(mockUvInstaller)
				inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

				return &fields{
					cmd:         cmd,
					uvInstaller: inst,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "/path/to/requirements.txt",
				asset: &pipeline.Asset{
					Image: "python:3.13",
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := tt.fields()
			l := &UvPythonRunner{
				Cmd:         f.cmd,
				UvInstaller: f.uvInstaller,
			}
			tt.wantErr(t, l.Run(t.Context(), tt.execCtx))
		})
	}
}

func Test_buildIngestrPackageKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		extraPackages []string
		expected      string
	}{
		{
			name:          "no extra packages",
			extraPackages: []string{},
			expected:      "ingestr@" + ingestrVersion + ":",
		},
		{
			name:          "single package",
			extraPackages: []string{"pyodbc"},
			expected:      "ingestr@" + ingestrVersion + ":pyodbc",
		},
		{
			name:          "multiple packages sorted",
			extraPackages: []string{"pyodbc", "duckdb"},
			expected:      "ingestr@" + ingestrVersion + ":duckdb,pyodbc",
		},
		{
			name:          "packages in different order produce same key",
			extraPackages: []string{"duckdb", "pyodbc"},
			expected:      "ingestr@" + ingestrVersion + ":duckdb,pyodbc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			u := &UvPythonRunner{}
			key := u.buildIngestrPackageKey(t.Context(), tt.extraPackages)
			assert.Equal(t, tt.expected, key)
		})
	}
}

func Test_ensureIngestrInstalled_OnlyInstallsOnce(t *testing.T) {
	t.Parallel()

	ResetIngestrInstallCache()
	defer ResetIngestrInstallCache()

	repo := &git.Repo{}
	var installCount atomic.Int32

	cmd := new(mockCmd)
	cmd.On("Run", mock.Anything, repo, mock.MatchedBy(func(c *CommandInstance) bool {
		return len(c.Args) > 0 && c.Args[0] == "tool" && c.Args[1] == "install"
	})).Run(func(args mock.Arguments) {
		installCount.Add(1)
	}).Return(nil)

	inst := new(mockUvInstaller)
	inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

	u := &UvPythonRunner{
		Cmd:            cmd,
		UvInstaller:    inst,
		binaryFullPath: "~/.bruin/uv",
	}

	ctx := t.Context()
	extraPackages := []string{"pyodbc"}

	err := u.ensureIngestrInstalled(ctx, extraPackages, repo)
	require.NoError(t, err)

	err = u.ensureIngestrInstalled(ctx, extraPackages, repo)
	require.NoError(t, err)

	err = u.ensureIngestrInstalled(ctx, extraPackages, repo)
	require.NoError(t, err)

	assert.Equal(t, int32(1), installCount.Load(), "ingestr should only be installed once for the same package combination")
}

func Test_ensureIngestrInstalled_InstallsForDifferentPackages(t *testing.T) {
	t.Parallel()

	ResetIngestrInstallCache()
	defer ResetIngestrInstallCache()

	repo := &git.Repo{}
	var installCount atomic.Int32

	cmd := new(mockCmd)
	cmd.On("Run", mock.Anything, repo, mock.MatchedBy(func(c *CommandInstance) bool {
		return len(c.Args) > 0 && c.Args[0] == "tool" && c.Args[1] == "install"
	})).Run(func(args mock.Arguments) {
		installCount.Add(1)
	}).Return(nil)

	inst := new(mockUvInstaller)
	inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

	u := &UvPythonRunner{
		Cmd:            cmd,
		UvInstaller:    inst,
		binaryFullPath: "~/.bruin/uv",
	}

	ctx := t.Context()

	err := u.ensureIngestrInstalled(ctx, []string{"pyodbc"}, repo)
	require.NoError(t, err)

	err = u.ensureIngestrInstalled(ctx, []string{"duckdb"}, repo)
	require.NoError(t, err)

	err = u.ensureIngestrInstalled(ctx, []string{"pyodbc", "duckdb"}, repo)
	require.NoError(t, err)

	assert.Equal(t, int32(3), installCount.Load(), "ingestr should be installed once for each unique package combination")
}

func Test_ensureIngestrInstalled_ConcurrentCalls(t *testing.T) {
	t.Parallel()

	ResetIngestrInstallCache()
	defer ResetIngestrInstallCache()

	repo := &git.Repo{}
	var installCount atomic.Int32

	cmd := new(mockCmd)
	cmd.On("Run", mock.Anything, repo, mock.MatchedBy(func(c *CommandInstance) bool {
		return len(c.Args) > 0 && c.Args[0] == "tool" && c.Args[1] == "install"
	})).Run(func(args mock.Arguments) {
		installCount.Add(1)
	}).Return(nil)

	inst := new(mockUvInstaller)
	inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

	u := &UvPythonRunner{
		Cmd:            cmd,
		UvInstaller:    inst,
		binaryFullPath: "~/.bruin/uv",
	}

	ctx := t.Context()
	extraPackages := []string{"pyodbc"}

	var wg sync.WaitGroup
	numGoroutines := 10

	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := u.ensureIngestrInstalled(ctx, extraPackages, repo)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(1), installCount.Load(), "ingestr should only be installed once even with concurrent calls")
}
