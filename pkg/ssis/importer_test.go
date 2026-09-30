package ssis

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func fixtureFS(t *testing.T) *afero.MemMapFs {
	t.Helper()
	fs := &afero.MemMapFs{}
	require.NoError(t, fs.MkdirAll("input", 0o755))
	for _, file := range []string{"education.dtsx", "transform.py", "jobs.json"} {
		data, err := os.ReadFile("testdata/" + file)
		require.NoError(t, err)
		require.NoError(t, afero.WriteFile(fs, "input/"+file, data, 0o600))
	}
	return fs
}

func read(t *testing.T, fs afero.Fs, path string) string {
	t.Helper()
	data, err := afero.ReadFile(fs, path)
	require.NoError(t, err)
	return string(data)
}

func TestImport(t *testing.T) {
	t.Parallel()
	fs := fixtureFS(t)
	opts := ImportOptions{SourcePath: "input", PipelinePath: "output", Connection: "school-db"}
	result, err := Import(t.Context(), fs, opts)
	require.NoError(t, err)
	require.Equal(t, &ImportResult{SQLAssets: 4, PythonAssets: 1, PipelineCreated: true}, result)
	prepare := read(t, fs, "output/assets/education.prepare.sql")
	require.Contains(t, prepare, "type: ms.sql")
	require.Contains(t, prepare, "connection: school-db")
	require.NotContains(t, prepare, "depends:")
	require.True(t, strings.HasSuffix(prepare, "TRUNCATE TABLE dbo.pupils;\n"))
	pythonCode := read(t, fs, "output/assets/education/transform.py")
	require.True(t, strings.HasPrefix(pythonCode, `"""@bruin`)) // Python is inferred from the extension.
	require.Contains(t, pythonCode, "education.prepare")
	require.True(t, strings.HasSuffix(pythonCode, "print(\"transform pupils\")\n"))
	publish := read(t, fs, "output/assets/education.publish.sql")
	require.Contains(t, publish, "education.transform")
	require.Contains(t, publish, "grade > 3;")
	extract := read(t, fs, "output/assets/mexico_extract.extract_pupil.sql")
	require.Contains(t, extract, "mexico_extract.truncate")
	require.Contains(t, extract, "USE [source];\nINSERT INTO dbo.pupils EXEC sp_execute_external_script")
	require.Contains(t, extract, `O''Brien`)

	edited := prepare + "-- user edit\n"
	require.NoError(t, afero.WriteFile(fs, "output/assets/education.prepare.sql", []byte(edited), 0o600))
	require.NoError(t, afero.WriteFile(fs, "output/pipeline.yml", []byte("name: existing\n"), 0o600))
	result, err = Import(t.Context(), fs, opts)
	require.NoError(t, err)
	require.Equal(t, 5, result.SkippedAssets)
	require.Equal(t, edited, read(t, fs, "output/assets/education.prepare.sql"))
	opts.Overwrite = true
	_, err = Import(t.Context(), fs, opts)
	require.NoError(t, err)
	require.Equal(t, prepare, read(t, fs, "output/assets/education.prepare.sql"))
	require.Equal(t, "name: existing\n", read(t, fs, "output/pipeline.yml"))
}

func TestRejectPackageBeforeWriting(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, old, replacement, message string }{
		{"failure", `DTS:LogicalAnd="True"`, `DTS:Value="1"`, "success precedence"},
		{"expression", `DTS:LogicalAnd="True"`, `DTS:EvalOp="2"`, "expression precedence"},
		{"or", `DTS:LogicalAnd="True"`, `DTS:LogicalAnd="False"`, "OR precedence"},
		{"missing from", `DTS:From="Package\Prepare"`, `DTS:From="missing"`, "unresolved precedence"},
		{"missing to", `DTS:To="Package\Transform"`, `DTS:To="missing"`, "unresolved precedence"},
		{"cycle", `DTS:From="Package\Prepare"`, `DTS:From="Package\Publish"`, "cycle"},
		{"collision", `DTS:ObjectName="Publish"`, `DTS:ObjectName="Prepare!"`, "duplicate generated"},
		{"data flow", `DTS:ExecutableType="Microsoft.ExecuteSQLTask"`, `DTS:ExecutableType="Microsoft.Pipeline"`, "unsupported executable"},
		{"variable SQL", `SQLTask:ResultType="ResultSetType_None"`, `SQLTask:SqlStatementSourceType="Variable"`, "direct-input"},
		{"disabled", `DTS:ObjectName="Prepare"`, `DTS:ObjectName="Prepare" DTS:Disabled="True"`, "disabled"},
		{"traversal", `&quot;transform.py&quot;`, `../transform.py`, "relative .py"},
		{"missing script", `&quot;transform.py&quot;`, `missing.py`, "read Python"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fs := fixtureFS(t)
			data := strings.Replace(read(t, fs, "input/education.dtsx"), tc.old, tc.replacement, 1)
			require.NoError(t, afero.WriteFile(fs, "input/education.dtsx", []byte(data), 0o600))
			_, err := Import(t.Context(), fs, ImportOptions{SourcePath: "input", PipelinePath: "output"})
			require.ErrorContains(t, err, tc.message)
			exists, err := afero.Exists(fs, "output")
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestRejectJobControlFlow(t *testing.T) {
	t.Parallel()
	for _, replacement := range []string{`"on_fail_action": 3`, `"on_fail_action": 1`, `"on_fail_action": 0`} {
		fs := fixtureFS(t)
		data := strings.ReplaceAll(read(t, fs, "input/jobs.json"), `"on_fail_action": 2`, replacement)
		_, err := parseJobs(fs, "input/jobs.json", []byte(data))
		require.ErrorContains(t, err, "quit-on-failure")
	}
}

func TestCanceledImport(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Import(ctx, fixtureFS(t), ImportOptions{SourcePath: "input", PipelinePath: "output"})
	require.ErrorIs(t, err, context.Canceled)
}

func TestPackageParallelRootsAndJoin(t *testing.T) {
	t.Parallel()
	fs := fixtureFS(t)
	data := read(t, fs, "input/education.dtsx")
	// Prepare and Transform are independent, and Publish waits for both.
	data = strings.Replace(data, `DTS:To="Package\Transform"`, `DTS:To="Package\Publish"`, 1)
	w, err := parsePackage(fs, "input/education.dtsx", []byte(data))
	require.NoError(t, err)
	require.Empty(t, w.tasks[0].upstream)
	require.Empty(t, w.tasks[1].upstream)
	require.Equal(t, []string{`Package\Prepare`, `Package\Transform`}, w.tasks[2].upstream)
}

func TestAgentPythonAndDatabaseEscaping(t *testing.T) {
	t.Parallel()
	fs := fixtureFS(t)
	data := `[{
	  "name":"job", "enabled":1, "start_step_id":4,
	  "steps":[
	    {"step_id":4,"step_name":"python","subsystem":"CmdExec","command":"python3 transform.py","on_success_action":3,"on_fail_action":2},
	    {"step_id":8,"step_name":"sql","subsystem":"TSQL","command":"SELECT 7;","database_name":"school]archive","on_success_action":1,"on_fail_action":2}
	  ]
	}]`
	workflows, err := parseJobs(fs, "input/jobs.json", []byte(data))
	require.NoError(t, err)
	require.True(t, workflows[0].tasks[0].python)
	require.Equal(t, "print(\"transform pupils\")\n", workflows[0].tasks[0].code)
	require.Equal(t, "USE [school]]archive];\nSELECT 7;", workflows[0].tasks[1].code)
	require.Equal(t, []string{"4"}, workflows[0].tasks[1].upstream)
}

func TestGeneratedPythonRunsAsModule(t *testing.T) {
	t.Parallel()
	interpreter, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	directory := t.TempDir()
	_, err = Import(t.Context(), afero.NewOsFs(), ImportOptions{SourcePath: "testdata/education.dtsx", PipelinePath: directory})
	require.NoError(t, err)
	finder := &python.ModulePathFinder{}
	module, err := finder.FindModulePath(&git.Repo{Path: directory}, &pipeline.ExecutableFile{Path: filepath.Join(directory, "assets", "education", "transform.py")})
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), interpreter, "-m", module)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Equal(t, "transform pupils", strings.TrimSpace(string(output)))
}

func TestDestinationConflictsFailBeforeWriting(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, code, message string
		overwrite                 bool
	}{
		{"renamed target", "education.prepare.sql", "/* @bruin\nname: manually.renamed\n@bruin */\nSELECT 1;", "expected", false},
		{"non-asset target", "education.prepare.sql", "SELECT 1;", "expected", false},
		{"other SQL path", "other.sql", "/* @bruin\nname: education.prepare\n@bruin */\nSELECT 1;", "conflicts", false},
		{"other YAML path", "other.asset.yml", "name: education.prepare\ntype: ms.sql\n", "conflicts", true},
		{"bare YAML filename", "asset.yml", "name: education.prepare\ntype: ms.sql\n", "conflicts", false},
		{"inferred name", "education/prepare.py", "\"\"\"@bruin\ndescription: inferred name\n@bruin\"\"\"\nprint(1)", "conflicts", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fs := fixtureFS(t)
			path := filepath.Join("output", "assets", tc.path)
			require.NoError(t, fs.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, afero.WriteFile(fs, path, []byte(tc.code), 0o600))
			_, err := Import(t.Context(), fs, ImportOptions{SourcePath: "input", PipelinePath: "output", Overwrite: tc.overwrite})
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, tc.code, read(t, fs, path))
			exists, err := afero.Exists(fs, "output/pipeline.yml")
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestStagedScriptSymlinks(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"file", "directory", "contained"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			staging := filepath.Join(root, "staging")
			require.NoError(t, os.Mkdir(staging, 0o755))
			target := filepath.Join(root, "outside.py")
			require.NoError(t, os.WriteFile(target, []byte("outside sentinel"), 0o600))
			link, argument := filepath.Join(staging, "script.py"), "script.py"
			switch kind {
			case "directory":
				target, link, argument = root, filepath.Join(staging, "linked"), "linked/outside.py"
			case "contained":
				target = "inside.py"
				require.NoError(t, os.WriteFile(filepath.Join(staging, target), []byte("inside sentinel"), 0o600))
			}
			if err := os.Symlink(target, link); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("symlinks unavailable: %v", err)
				}
				require.NoError(t, err)
			}
			code, err := pythonScript(afero.NewOsFs(), filepath.Join(staging, "package.dtsx"), "python", argument)
			if kind == "contained" {
				require.NoError(t, err)
				require.Equal(t, "inside sentinel", code)
			} else {
				require.Error(t, err)
				require.Empty(t, code)
			}
		})
	}
}

func TestDestinationSymlinksDoNotWriteOutsidePipeline(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"file", "python directory", "assets directory", "dangling assets directory"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel.sql")
			require.NoError(t, os.WriteFile(sentinel, []byte("must not change"), 0o600))
			assets := filepath.Join(directory, "assets")
			target, link := outside, assets
			switch kind {
			case "file":
				require.NoError(t, os.Mkdir(assets, 0o755))
				target, link = sentinel, filepath.Join(assets, "education.prepare.sql")
			case "python directory":
				require.NoError(t, os.Mkdir(assets, 0o755))
				link = filepath.Join(assets, "education")
			case "dangling assets directory":
				target = filepath.Join(outside, "missing")
			}
			if err := os.Symlink(target, link); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("symlinks unavailable: %v", err)
				}
				require.NoError(t, err)
			}
			_, err := Import(t.Context(), afero.NewOsFs(), ImportOptions{
				SourcePath: "testdata/education.dtsx", PipelinePath: directory, Overwrite: kind == "file",
			})
			require.ErrorContains(t, err, "destination symlink")
			require.Equal(t, "must not change", read(t, afero.NewOsFs(), sentinel))
			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			_, err = os.Stat(filepath.Join(directory, "pipeline.yml"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
