package python

import (
	"context"
	"os"
	"regexp"
	"strings"
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

type mockCmd struct {
	mock.Mock
}

func (m *mockCmd) Run(ctx context.Context, repo *git.Repo, command *CommandInstance) error {
	args := m.Called(ctx, repo, command)
	return args.Error(0)
}

type fakeConnectionGetter map[string]any

func (f fakeConnectionGetter) GetConnection(name string) any {
	return f[name]
}

type fakeIngestrConnection struct {
	uri string
}

func (f fakeIngestrConnection) GetIngestrURI() (string, error) {
	return f.uri, nil
}

func Test_ingestrEnvVars_DisablesTelemetry(t *testing.T) {
	t.Parallel()

	assert.Equal(t, map[string]string{
		"INGESTR_DISABLE_TELEMETRY": "true",
		"PYTHONUNBUFFERED":          "1",
	}, ingestrEnvVars())
}

func Test_uvPythonRunner_RunIngestr_DisablesTelemetry(t *testing.T) {
	t.Parallel()

	repo := &git.Repo{}
	cmd := new(mockCmd)
	cmd.On("Run", mock.Anything, repo, &CommandInstance{
		Name: "~/.bruin/uv",
		Args: []string{
			"tool",
			"run",
			"--no-config",
			"--prerelease",
			"allow",
			"--python",
			"3.11",
			"--from",
			"ingestr@" + IngestrVersionV1,
			"ingestr",
			"ingest",
		},
		EnvVars: map[string]string{
			"INGESTR_DISABLE_TELEMETRY": "true",
			"PYTHONUNBUFFERED":          "1",
		},
	}).Return(nil)

	inst := new(mockUvInstaller)
	inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

	runner := &UvPythonRunner{
		Cmd:         cmd,
		UvInstaller: inst,
	}

	err := runner.RunIngestr(t.Context(), []string{"ingest"}, nil, repo)

	require.NoError(t, err)
	cmd.AssertExpectations(t)
}

func Test_uvPythonRunner_RunWithMaterialization_DisablesTelemetryForIngestrUpload(t *testing.T) {
	t.Parallel()

	repo := &git.Repo{Path: t.TempDir()}
	cmd := new(mockCmd)
	cmd.On("Run", mock.Anything, repo, mock.MatchedBy(func(c *CommandInstance) bool {
		return c.Name == "~/.bruin/uv" &&
			len(c.Args) > 0 &&
			c.Args[0] == "run"
	})).Run(func(args mock.Arguments) {
		command := args.Get(2).(*CommandInstance)
		scriptPath := command.Args[len(command.Args)-1]
		script, err := os.ReadFile(scriptPath)
		require.NoError(t, err)

		arrowPath := extractArrowPathFromScript(t, string(script))
		require.NoError(t, os.WriteFile(arrowPath, []byte("arrow"), 0o600))
	}).Return(nil)

	cmd.On("Run", mock.Anything, repo, mock.MatchedBy(func(c *CommandInstance) bool {
		return c.Name == "~/.bruin/uv" &&
			len(c.Args) > 0 &&
			c.Args[0] == "tool" &&
			c.EnvVars["INGESTR_DISABLE_TELEMETRY"] == "true" &&
			c.EnvVars["PYTHONUNBUFFERED"] == "1" &&
			containsArg(c.Args, "mmap://")
	})).Return(nil)

	inst := new(mockUvInstaller)
	inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

	runner := &UvPythonRunner{
		Cmd:         cmd,
		UvInstaller: inst,
		conn: fakeConnectionGetter{
			"dest": fakeIngestrConnection{uri: "postgres://user:pass@localhost:5432/db"},
		},
	}

	err := runner.Run(t.Context(), &executionContext{
		repo:     repo,
		module:   "path.to.module",
		pipeline: &pipeline.Pipeline{},
		asset: &pipeline.Asset{
			Name:       "public.asset_data",
			Type:       pipeline.AssetTypePython,
			Connection: "dest",
			Materialization: pipeline.Materialization{
				Type: "table",
			},
		},
	})

	require.NoError(t, err)
	cmd.AssertExpectations(t)
}

func Test_uvPythonRunner_RunWithMaterialization_RejectsIncrementalPredicate(t *testing.T) {
	t.Parallel()

	runner := &UvPythonRunner{}

	err := runner.runWithMaterialization(t.Context(), &executionContext{
		asset: &pipeline.Asset{
			Name:       "public.asset_data",
			Type:       pipeline.AssetTypePython,
			Connection: "dest",
			Materialization: pipeline.Materialization{
				Type:                 "table",
				Strategy:             pipeline.MaterializationStrategyMerge,
				IncrementalPredicate: "target.event_date >= DATE '2026-07-01'",
			},
		},
	}, "")

	require.ErrorContains(t, err, "incremental_predicate is not supported for Python assets")
}

func extractArrowPathFromScript(t *testing.T, script string) string {
	t.Helper()

	matches := regexp.MustCompile(`pa\.OSFile\("([^"]+)", 'wb'\)`).FindStringSubmatch(script)
	require.Len(t, matches, 2)
	return strings.ReplaceAll(matches[1], `\\`, `\`)
}

func containsArg(args []string, value string) bool {
	for _, arg := range args {
		if strings.Contains(arg, value) {
			return true
		}
	}
	return false
}

func TestPythonArrowTemplate_UsesNativePolarsIPC(t *testing.T) {
	t.Parallel()

	polarsCheckIndex := strings.Index(PythonArrowTemplate, "if is_polars_dataframe(df):")
	polarsWriteIndex := strings.Index(PythonArrowTemplate, `df.write_ipc("$ARROW_FILE_PATH")`)
	pandasCheckIndex := strings.Index(PythonArrowTemplate, "if is_pandas_dataframe(df):")
	arrowTableIndex := strings.Index(PythonArrowTemplate, "elif isinstance(df, pa.Table):")
	arrowTablesWriterIndex := strings.Index(PythonArrowTemplate, "def write_arrow_tables(tables):")
	pyarrowWriteIndex := strings.Index(PythonArrowTemplate, `with pa.OSFile("$ARROW_FILE_PATH", 'wb') as f:`)

	require.NotEqual(t, -1, polarsCheckIndex)
	require.NotEqual(t, -1, polarsWriteIndex)
	require.NotEqual(t, -1, pandasCheckIndex)
	require.NotEqual(t, -1, arrowTableIndex)
	require.NotEqual(t, -1, arrowTablesWriterIndex)
	require.NotEqual(t, -1, pyarrowWriteIndex)

	assert.Less(t, arrowTablesWriterIndex, polarsCheckIndex)
	assert.Less(t, arrowTablesWriterIndex, pyarrowWriteIndex)
	assert.Less(t, pyarrowWriteIndex, polarsCheckIndex)
	assert.Less(t, polarsCheckIndex, polarsWriteIndex)
	assert.Less(t, polarsWriteIndex, pandasCheckIndex)
	assert.Less(t, pandasCheckIndex, arrowTableIndex)
	assert.NotContains(t, PythonArrowTemplate, "df.to_arrow()")
	assert.Contains(t, PythonArrowTemplate, "write_arrow_tables(rows_to_tables(df))")
}

func TestPythonArrowTemplate_PreservesNoDataAndIterableHandling(t *testing.T) {
	t.Parallel()

	assert.Contains(t, PythonArrowTemplate, "if df is None:")
	assert.Contains(t, PythonArrowTemplate, "if not df:")
	assert.Contains(t, PythonArrowTemplate, "except StopIteration:")

	// Generators are streamed: each yielded value becomes its own Arrow batch
	// without buffering the whole dataset first.
	assert.Contains(t, PythonArrowTemplate, "def rows_to_tables(items):")
	// Leading rows are buffered until a null-free schema is found, then locked so
	// nullable fields and missing optional keys in later yields conform instead of
	// raising "All yielded pyarrow Tables must have the same schema."
	assert.Contains(t, PythonArrowTemplate, "locked_schema = None")
	assert.Contains(t, PythonArrowTemplate, "pa.types.is_null(field.type)")
	assert.Contains(t, PythonArrowTemplate, "pa.Table.from_pylist(list(rows), schema=locked_schema)")
	// A yielded pa.Table's explicit schema flushes pending rows and locks the
	// schema, so a table next to nullable dict rows agrees in either order.
	assert.Contains(t, PythonArrowTemplate, "pa.Table.from_pylist(pending, schema=item.schema)")
	// The old buffer-everything path must be gone.
	assert.NotContains(t, PythonArrowTemplate, "rows = [first_row, *iterator]")
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
					Args: []string{"run", "--no-config", "--no-project", "--python", "3.11", "--module", module},
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
					Args: []string{"run", "--no-config", "--no-project", "--python", "3.11", "--with-requirements", "/path/to/requirements.txt", "--module", module},
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
					Args: []string{"run", "--no-config", "--no-project", "--python", "3.13", "--with-requirements", "/path/to/requirements.txt", "--module", module},
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

func Test_ingestrPackage_HonorsCtxIngestrVersion(t *testing.T) {
	t.Parallel()

	u := &UvPythonRunner{}

	defaultPkg, _ := u.ingestrPackage(t.Context())
	assert.Equal(t, "ingestr@"+IngestrVersionV1, defaultPkg)

	pinnedCtx := context.WithValue(t.Context(), CtxIngestrVersion, "0.14.2")
	pinnedPkg, isLocal := u.ingestrPackage(pinnedCtx)
	assert.Equal(t, "ingestr@0.14.2", pinnedPkg)
	assert.False(t, isLocal)

	// LocalIngestr (debug-ingestr-src) takes precedence over CtxIngestrVersion.
	localCtx := context.WithValue(pinnedCtx, LocalIngestr, "/local/path/to/ingestr")
	localPkg, isLocal := u.ingestrPackage(localCtx)
	assert.Equal(t, "/local/path/to/ingestr", localPkg)
	assert.True(t, isLocal)
}

func Test_ingestrRunCmd_UsesResolvedPackage(t *testing.T) {
	t.Parallel()

	u := &UvPythonRunner{}
	ctx := context.WithValue(t.Context(), CtxIngestrVersion, "0.14.155")

	args := u.ingestrRunCmd(ctx, []string{"pyodbc", "duckdb"}, []string{"ingest", "--source-uri", "csv:///tmp/seed.csv"})

	assert.Equal(t, []string{
		"tool",
		"run",
		"--no-config",
		"--prerelease",
		"allow",
		"--python",
		"3.11",
		"--from",
		"ingestr@0.14.155",
		"--with",
		"pyodbc",
		"--with",
		"duckdb",
		"ingestr",
		"ingest",
		"--source-uri",
		"csv:///tmp/seed.csv",
	}, args)
}

func Test_ingestrRunCmd_LocalPathForcesReinstall(t *testing.T) {
	t.Parallel()

	u := &UvPythonRunner{}
	ctx := context.WithValue(t.Context(), LocalIngestr, "/local/ingestr")

	args := u.ingestrRunCmd(ctx, nil, []string{"ingest"})

	assert.Equal(t, []string{
		"tool",
		"run",
		"--no-config",
		"--prerelease",
		"allow",
		"--python",
		"3.11",
		"--reinstall-package",
		"ingestr",
		"--from",
		"/local/ingestr",
		"ingestr",
		"ingest",
	}, args)
}

func TestResolvePythonVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		requiresPython string
		defaultVersion string
		want           string
		wantErr        bool
	}{
		{
			name:           "empty string returns default",
			requiresPython: "",
			defaultVersion: "3.11",
			want:           "3.11",
		},
		{
			name:           "whitespace only returns default",
			requiresPython: "   ",
			defaultVersion: "3.11",
			want:           "3.11",
		},
		{
			name:           ">=3.13 returns 3.13",
			requiresPython: ">=3.13",
			defaultVersion: "3.11",
			want:           "3.13",
		},
		{
			name:           ">=3.11 returns 3.11",
			requiresPython: ">=3.11",
			defaultVersion: "3.11",
			want:           "3.11",
		},
		{
			name:           ">=3.12,<3.14 returns 3.12",
			requiresPython: ">=3.12,<3.14",
			defaultVersion: "3.11",
			want:           "3.12",
		},
		{
			name:           "==3.12 returns 3.12",
			requiresPython: "==3.12",
			defaultVersion: "3.11",
			want:           "3.12",
		},
		{
			name:           ">3.11 returns 3.12",
			requiresPython: ">3.11",
			defaultVersion: "3.11",
			want:           "3.12",
		},
		{
			name:           "~=3.10 returns 3.10",
			requiresPython: "~=3.10",
			defaultVersion: "3.11",
			want:           "3.10",
		},
		{
			name:           ">=3.8 returns lowest default 3.8",
			requiresPython: ">=3.8",
			defaultVersion: "3.11",
			want:           "3.8",
		},
		{
			name:           ">=3.14 returns 3.14",
			requiresPython: ">=3.14",
			defaultVersion: "3.11",
			want:           "3.14",
		},
		{
			name:           "==3.14 returns 3.14",
			requiresPython: "==3.14",
			defaultVersion: "3.11",
			want:           "3.14",
		},
		{
			name:           "handles spaces in specifier",
			requiresPython: ">= 3.12 , < 3.14",
			defaultVersion: "3.11",
			want:           "3.12",
		},
		{
			name:           "wildcard suffix is handled",
			requiresPython: "==3.12.*",
			defaultVersion: "3.11",
			want:           "3.12",
		},
		{
			name:           "patch version is stripped >=3.13.0",
			requiresPython: ">=3.13.0",
			defaultVersion: "3.11",
			want:           "3.13",
		},
		{
			name:           "patch version is stripped >=3.12.1",
			requiresPython: ">=3.12.1",
			defaultVersion: "3.11",
			want:           "3.12",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ResolvePythonVersion(tt.requiresPython, tt.defaultVersion)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_uvPythonRunner_Run_UsesRequiresPython(t *testing.T) {
	t.Parallel()

	repo := &git.Repo{}
	module := "path.to.module"

	tests := []struct {
		name             string
		image            string
		dependencyConfig *DependencyConfig
		expectedVersion  string
	}{
		{
			name:  "pyproject with requires-python >=3.13 uses 3.13",
			image: "",
			dependencyConfig: &DependencyConfig{
				Type:           DependencyTypePyproject,
				RequiresPython: ">=3.13",
				ProjectRoot:    "/tmp/project",
				PyprojectPath:  "/tmp/project/pyproject.toml",
			},
			expectedVersion: "3.13",
		},
		{
			name:  "explicit image overrides requires-python",
			image: "python:3.12",
			dependencyConfig: &DependencyConfig{
				Type:           DependencyTypePyproject,
				RequiresPython: ">=3.13",
				ProjectRoot:    "/tmp/project",
				PyprojectPath:  "/tmp/project/pyproject.toml",
			},
			expectedVersion: "3.12",
		},
		{
			name:  "no image and no requires-python uses default",
			image: "",
			dependencyConfig: &DependencyConfig{
				Type:          DependencyTypePyproject,
				ProjectRoot:   "/tmp/project",
				PyprojectPath: "/tmp/project/pyproject.toml",
			},
			expectedVersion: "3.11",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := new(mockCmd)

			// For pyproject-based runs, expect lock then run calls
			projectRepo := &git.Repo{Path: tt.dependencyConfig.ProjectRoot}
			cmd.On("Run", mock.Anything, projectRepo, &CommandInstance{
				Name: "~/.bruin/uv",
				Args: []string{"lock", "--python", tt.expectedVersion},
			}).Return(nil)
			cmd.On("Run", mock.Anything, projectRepo, mock.MatchedBy(func(c *CommandInstance) bool {
				return len(c.Args) > 0 && c.Args[0] == "run" &&
					c.Args[2] == tt.expectedVersion
			})).Return(nil)

			inst := new(mockUvInstaller)
			inst.On("EnsureUvInstalled", mock.Anything).Return("~/.bruin/uv", nil)

			runner := &UvPythonRunner{
				Cmd:         cmd,
				UvInstaller: inst,
			}

			err := runner.Run(t.Context(), &executionContext{
				repo:             repo,
				module:           module,
				dependencyConfig: tt.dependencyConfig,
				asset: &pipeline.Asset{
					Image: tt.image,
				},
			})
			require.NoError(t, err)
			cmd.AssertExpectations(t)
		})
	}
}
