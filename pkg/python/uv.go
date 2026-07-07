package python

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bruin-data/bruin/pkg/config"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/uv"
	"github.com/pkg/errors"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/afero"
)

var AvailablePythonVersions = map[string]bool{
	"3.8":  true,
	"3.9":  true,
	"3.10": true,
	"3.11": true,
	"3.12": true,
	"3.13": true,
	"3.14": true,
}

// SortedPythonVersions is the list of available Python versions sorted ascending.
var SortedPythonVersions = []string{"3.8", "3.9", "3.10", "3.11", "3.12", "3.13", "3.14"}

const (
	pythonVersionForIngestr = "3.11"
	defaultPythonVersion    = "3.11"
	// IngestrVersionV0 is the legacy ingestr release pinned for parameters.version=v0.
	IngestrVersionV0 = "0.14.155"
	// IngestrVersionV1 is the current ingestr release used by default and for parameters.version=v1.
	IngestrVersionV1 = "1.0.69"
	sqlfluffVersion  = "3.4.1"
)

func ingestrEnvVars() map[string]string {
	return map[string]string{
		"INGESTR_DISABLE_TELEMETRY": "true",
		"PYTHONUNBUFFERED":          "1",
	}
}

// parsePythonVersion parses a "X.Y" version string into (major, minor).
func parsePythonVersion(v string) (int, int, bool) {
	parts := strings.Split(v, ".")
	if len(parts) != 2 {
		return 0, 0, false
	}
	var major, minor int
	if _, err := fmt.Sscanf(parts[0], "%d", &major); err != nil {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &minor); err != nil {
		return 0, 0, false
	}
	return major, minor, true
}

// versionSatisfies checks if version v satisfies a single constraint like ">=3.12".
func versionSatisfies(vMajor, vMinor int, op string, cMajor, cMinor int) bool {
	switch op {
	case ">=":
		return vMajor > cMajor || (vMajor == cMajor && vMinor >= cMinor)
	case ">":
		return vMajor > cMajor || (vMajor == cMajor && vMinor > cMinor)
	case "<=":
		return vMajor < cMajor || (vMajor == cMajor && vMinor <= cMinor)
	case "<":
		return vMajor < cMajor || (vMajor == cMajor && vMinor < cMinor)
	case "==":
		return vMajor == cMajor && vMinor == cMinor
	case "!=":
		return vMajor != cMajor || vMinor != cMinor
	case "~=":
		// ~=3.12 means >=3.12, <4.0 (compatible release)
		return (vMajor > cMajor || (vMajor == cMajor && vMinor >= cMinor)) && vMajor == cMajor
	default:
		return false
	}
}

// ResolvePythonVersion resolves the minimum available Python version that satisfies
// the given requires-python specifier (PEP 440). Returns defaultVersion if the
// specifier is empty. Returns an error if no available version satisfies the constraint.
func ResolvePythonVersion(requiresPython string, defaultVersion string) (string, error) {
	requiresPython = strings.TrimSpace(requiresPython)
	if requiresPython == "" {
		return defaultVersion, nil
	}

	// Parse all constraints (comma-separated)
	type constraint struct {
		op    string
		major int
		minor int
	}
	parts := strings.Split(requiresPython, ",")
	constraints := make([]constraint, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var op, ver string
		for _, prefix := range []string{">=", "<=", "!=", "~=", "==", ">", "<"} {
			if strings.HasPrefix(part, prefix) {
				op = prefix
				ver = strings.TrimSpace(part[len(prefix):])
				break
			}
		}
		if op == "" {
			// bare version like "3.12" — treat as ==
			op = "=="
			ver = part
		}

		// Strip wildcard suffix (e.g., "3.12.*" -> "3.12")
		ver = strings.TrimSuffix(ver, ".*")

		// Strip patch version (e.g., "3.13.0" -> "3.13")
		if verParts := strings.Split(ver, "."); len(verParts) > 2 {
			ver = verParts[0] + "." + verParts[1]
		}

		major, minor, ok := parsePythonVersion(ver)
		if !ok {
			return defaultVersion, nil
		}
		constraints = append(constraints, constraint{op: op, major: major, minor: minor})
	}

	if len(constraints) == 0 {
		return defaultVersion, nil
	}

	// Find the minimum available version that satisfies all constraints
	for _, v := range SortedPythonVersions {
		vMajor, vMinor, ok := parsePythonVersion(v)
		if !ok {
			continue
		}

		satisfiesAll := true
		for _, c := range constraints {
			if !versionSatisfies(vMajor, vMinor, c.op, c.major, c.minor) {
				satisfiesAll = false
				break
			}
		}
		if satisfiesAll {
			return v, nil
		}
	}

	return "", fmt.Errorf("no available Python version satisfies the constraint %q (available: %s)", requiresPython, strings.Join(SortedPythonVersions, ", "))
}

var DatabasePrefixToSqlfluffDialect = map[string]string{
	"bq":         "bigquery",
	"sf":         "snowflake",
	"pg":         "postgres",
	"rs":         "redshift",
	"athena":     "athena",
	"ms":         "tsql",
	"databricks": "sparksql",
	"synapse":    "tsql",
	"duckdb":     "duckdb",
	"clickhouse": "clickhouse",
}

// CtxUseWingetForUv is a context key for enabling winget-based uv installation on Windows.
const CtxUseWingetForUv = "use_winget_for_uv"

// UvChecker handles checking and installing the uv package manager.
type UvChecker struct {
	uv.Checker
}

type uvInstaller interface {
	EnsureUvInstalled(ctx context.Context) (string, error)
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

type UvPythonRunner struct {
	Cmd            cmd
	UvInstaller    uvInstaller
	conn           config.ConnectionGetter
	binaryFullPath string
}

func (u *UvPythonRunner) Run(ctx context.Context, execCtx *executionContext) error {
	binaryFullPath, err := u.UvInstaller.EnsureUvInstalled(ctx)
	if err != nil {
		return err
	}

	u.binaryFullPath = binaryFullPath

	pythonVersion := defaultPythonVersion
	if execCtx.asset.Image != "" {
		image := execCtx.asset.Image
		parts := strings.Split(image, ":")
		if len(parts) > 1 && parts[0] == "python" && AvailablePythonVersions[parts[1]] {
			pythonVersion = parts[1]
		}
	} else if execCtx.dependencyConfig != nil && execCtx.dependencyConfig.RequiresPython != "" {
		resolved, err := ResolvePythonVersion(execCtx.dependencyConfig.RequiresPython, defaultPythonVersion)
		if err != nil {
			return err
		}
		pythonVersion = resolved
	}

	if execCtx.asset.Materialization.Type == "" {
		return u.runWithNoMaterialization(ctx, execCtx, pythonVersion)
	}

	return u.runWithMaterialization(ctx, execCtx, pythonVersion)
}

func (u *UvPythonRunner) RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error {
	binaryFullPath, err := u.UvInstaller.EnsureUvInstalled(ctx)
	if err != nil {
		return err
	}
	u.binaryFullPath = binaryFullPath

	noDependencyCommand := &CommandInstance{
		Name:    u.binaryFullPath,
		Args:    u.ingestrRunCmd(ctx, extraPackages, args),
		EnvVars: ingestrEnvVars(),
	}

	return u.Cmd.Run(ctx, repo, noDependencyCommand)
}

func (u *UvPythonRunner) runWithNoMaterialization(ctx context.Context, execCtx *executionContext, pythonVersion string) error {
	// Check for pyproject.toml-based dependencies first
	if execCtx.dependencyConfig != nil && execCtx.dependencyConfig.Type == DependencyTypePyproject {
		return u.runWithPyproject(ctx, execCtx, pythonVersion)
	}

	// Fall back to requirements.txt or no dependencies (existing behavior)
	flags := []string{"run", "--no-config", "--no-project", "--python", pythonVersion}
	if execCtx.requirementsTxt != "" {
		flags = append(flags, "--with-requirements", execCtx.requirementsTxt)
	}

	flags = append(flags, "--module", execCtx.module)

	noDependencyCommand := &CommandInstance{
		Name:    u.binaryFullPath,
		Args:    flags,
		EnvVars: execCtx.envVariables,
	}

	return u.Cmd.Run(ctx, execCtx.repo, noDependencyCommand)
}

// lockPyprojectDeps runs `uv lock` in the project directory to ensure uv.lock is up to date.
func (u *UvPythonRunner) lockPyprojectDeps(ctx context.Context, projectRepo *git.Repo, pythonVersion string) error {
	lockCmd := &CommandInstance{
		Name: u.binaryFullPath,
		Args: []string{"lock", "--python", pythonVersion},
	}

	return u.Cmd.Run(ctx, projectRepo, lockCmd)
}

// runWithPyproject runs a Python module with dependencies from pyproject.toml.
// It first locks the dependencies, then runs the module.
func (u *UvPythonRunner) runWithPyproject(ctx context.Context, execCtx *executionContext, pythonVersion string) error {
	depConfig := execCtx.dependencyConfig

	// When using pyproject.toml, UV expects to run from the project directory
	projectRepo := &git.Repo{
		Path: depConfig.ProjectRoot,
	}

	// Lock dependencies before running
	if err := u.lockPyprojectDeps(ctx, projectRepo, pythonVersion); err != nil {
		return errors.Wrap(err, "failed to lock pyproject.toml dependencies")
	}

	// Calculate module path relative to project root
	modulePath := u.calculateModuleFromProjectRoot(execCtx, depConfig.ProjectRoot)

	// Build command: uv run --python <version> --module <module>
	flags := []string{"run", "--python", pythonVersion, "--module", modulePath}

	command := &CommandInstance{
		Name:    u.binaryFullPath,
		Args:    flags,
		EnvVars: execCtx.envVariables,
	}

	return u.Cmd.Run(ctx, projectRepo, command)
}

// calculateModuleFromProjectRoot calculates the module path relative to the project root.
// This handles cases where pyproject.toml is in a subdirectory of the repo.
func (u *UvPythonRunner) calculateModuleFromProjectRoot(execCtx *executionContext, projectRoot string) string {
	// If the project root is the same as repo root, use existing module
	if projectRoot == execCtx.repo.Path {
		return execCtx.module
	}

	// Otherwise, the module path needs to be relative to project root
	// Example:
	//   repo: /path/to/repo
	//   projectRoot: /path/to/repo/python_project
	//   original module: python_project.src.main
	//   new module: src.main
	relPath, err := filepath.Rel(execCtx.repo.Path, projectRoot)
	if err != nil {
		return execCtx.module // fallback
	}

	prefix := strings.ReplaceAll(relPath, string(os.PathSeparator), ".") + "."
	if strings.HasPrefix(strings.ToLower(execCtx.module), strings.ToLower(prefix)) {
		return execCtx.module[len(prefix):]
	}

	return execCtx.module
}

func (u *UvPythonRunner) runWithMaterialization(ctx context.Context, execCtx *executionContext, pythonVersion string) error {
	asset := execCtx.asset
	mat := asset.Materialization

	if mat.Type != "table" {
		return errors.New("only table materialization is supported for Python assets")
	}

	arrowFilePath := filepath.Join(os.TempDir(), fmt.Sprintf("asset_data_%d.arrow", time.Now().UnixNano()))
	defer func(name string) {
		_ = os.Remove(name)
	}(arrowFilePath)

	tempPyScript, err := os.CreateTemp("", "bruin-arrow-*.py")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func(name string) {
		_ = os.Remove(name)
	}(tempPyScript.Name())
	defer func(tempPyScript *os.File) {
		_ = tempPyScript.Close()
	}(tempPyScript)

	// Use adjusted module path and root for pyproject-based execution.
	// When pyproject.toml is in a subdirectory, the module path is relative to the project
	// root, so sys.path must also point to the project root (not the git repo root).
	modulePath := execCtx.module
	rootPath := execCtx.repo.Path
	isPyproject := execCtx.dependencyConfig != nil && execCtx.dependencyConfig.Type == DependencyTypePyproject
	if isPyproject {
		modulePath = u.calculateModuleFromProjectRoot(execCtx, execCtx.dependencyConfig.ProjectRoot)
		rootPath = execCtx.dependencyConfig.ProjectRoot
	}
	arrowScript := strings.ReplaceAll(PythonArrowTemplate, "$REPO_ROOT", strings.ReplaceAll(rootPath, "\\", "\\\\"))
	arrowScript = strings.ReplaceAll(arrowScript, "$MODULE_PATH", modulePath)
	arrowScript = strings.ReplaceAll(arrowScript, "$ARROW_FILE_PATH", strings.ReplaceAll(arrowFilePath, "\\", "\\\\"))

	// For pyproject-based execution, strip inline script metadata (PEP 723) so that uv
	// stays in project mode and uses pyproject.toml dependencies. Without this, uv enters
	// script mode which creates an isolated env with only pyarrow, ignoring project deps.
	// pyarrow is then provided via --with flag instead.
	if isPyproject {
		arrowScript = strings.ReplaceAll(arrowScript, pythonArrowInlineMetadata, "")
	}

	if _, err := io.WriteString(tempPyScript, arrowScript); err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	// Determine which dependency method to use
	var flags []string
	var runRepo *git.Repo

	if isPyproject {
		// Use pyproject.toml - lock dependencies first, then run.
		// pyarrow is added via --with since inline script metadata was stripped above.
		runRepo = &git.Repo{Path: execCtx.dependencyConfig.ProjectRoot}
		if err := u.lockPyprojectDeps(ctx, runRepo, pythonVersion); err != nil {
			return errors.Wrap(err, "failed to lock pyproject.toml dependencies")
		}
		flags = []string{"run", "--python", pythonVersion, "--with", "pyarrow==18.0.0", tempPyScript.Name()}
	} else {
		// Fall back to requirements.txt or no dependencies
		runRepo = execCtx.repo
		flags = []string{"run", "--no-config", "--no-project", "--python", pythonVersion}
		if execCtx.requirementsTxt != "" {
			flags = append(flags, "--with-requirements", execCtx.requirementsTxt)
		}
		flags = append(flags, tempPyScript.Name())
	}

	err = u.Cmd.Run(ctx, runRepo, &CommandInstance{
		Name:    u.binaryFullPath,
		Args:    flags,
		EnvVars: execCtx.envVariables,
	})
	if err != nil {
		return errors.Wrap(err, "failed to run asset code")
	}

	var output io.Writer = os.Stdout
	if ctx.Value(executor.KeyPrinter) != nil {
		output = ctx.Value(executor.KeyPrinter).(io.Writer)
	}

	// Check if the arrow file was created (materialize() may return None)
	if _, err := os.Stat(arrowFilePath); os.IsNotExist(err) {
		_, _ = output.Write([]byte("WARNING: materialize() returned None, skipping materialization\n"))
		return nil
	}

	_, _ = output.Write([]byte("Successfully collected the data from the asset, uploading to the destination...\n"))

	if len(asset.Parameters) == 0 {
		asset.Parameters = make(pipeline.ParameterMap)
	}

	if mat.Strategy != "" {
		ingestrStrategy, ok := TranslateBruinStrategyToIngestr(mat.Strategy)
		if ok {
			asset.Parameters["incremental_strategy"] = ingestrStrategy
		}
	}

	if mat.IncrementalKey != "" {
		asset.Parameters["incremental_key"] = mat.IncrementalKey
	}

	// build ingestr flags
	cmdArgs, err := ConsolidatedParameters(ctx, asset, []string{
		"ingest",
		"--source-uri",
		"mmap://" + arrowFilePath,
		"--source-table",
		"asset_data",
		"--dest-table",
		execCtx.asset.Name,
		"--yes",
		"--progress",
		"log",
	}, &ColumnHintOptions{
		NormalizeColumnNames:   false,
		EnforceSchemaByDefault: false,
	})
	if err != nil {
		return err
	}

	destConnectionName, err := execCtx.pipeline.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection := u.conn.GetConnection(destConnectionName)
	if destConnection == nil {
		return config.NewConnectionNotFoundError(ctx, "destination", destConnectionName)
	}

	destConnectionInst, ok := destConnection.(pipelineConnection)
	if !ok {
		return errors.Errorf("destination connection '%s' is not supported by ingestr", destConnectionName)
	}

	destURI, err := destConnectionInst.GetIngestrURI()
	if err != nil {
		return errors.Wrap(err, "could not get the destination uri")
	}

	if destURI == "" {
		return errors.New("destination uri is empty, which means the destination connection is not configured correctly")
	}

	cmdArgs = append(cmdArgs, "--dest-uri", destURI)

	// Compute extra packages based on destination URI (e.g., pyodbc for MSSQL)
	var extraPackages []string
	extraPackages = AddExtraPackages(destURI, "", extraPackages)

	if strings.HasPrefix(destURI, "duckdb://") {
		if dbURIGetter, ok := destConnectionInst.(interface{ GetDBConnectionURI() string }); ok {
			duck.LockDatabase(dbURIGetter.GetDBConnectionURI())
			defer duck.UnlockDatabase(dbURIGetter.GetDBConnectionURI())
		}
	}

	ingestrCtx := ctx
	showLogsVal, _ := asset.Parameters.GetString("show_ingestr_logs")
	showLogs := showLogsVal == "true"
	var logBuffer *tailBuffer
	if !showLogs {
		logBuffer = newTailBuffer(1 << 20) // 1MB cap
		ingestrCtx = context.WithValue(ctx, executor.KeyPrinter, io.Writer(logBuffer))
	}

	runArgs := u.ingestrRunCmd(ctx, extraPackages, cmdArgs)

	if executor.IsDebugMode(ctx) {
		_, _ = output.Write([]byte("Running CommandInstance: uv " + strings.Join(runArgs, " ") + "\n"))
	}

	err = u.Cmd.Run(ingestrCtx, execCtx.repo, &CommandInstance{
		Name:    u.binaryFullPath,
		Args:    runArgs,
		EnvVars: ingestrEnvVars(),
	})
	if err != nil {
		if logBuffer != nil {
			logBuffer.flushTo(output)
		}
		return errors.Wrap(err, "failed to load the data into the destination")
	}

	_, _ = output.Write([]byte("Successfully loaded the data from the asset into the destination.\n"))

	return nil
}

func (u *UvPythonRunner) ingestrPackage(ctx context.Context) (string, bool) {
	localIngestr := ctx.Value(LocalIngestr)
	if localIngestr != nil {
		ingestrPath, ok := localIngestr.(string)
		if ok && ingestrPath != "" {
			// maybe verify that the destination exists?
			return ingestrPath, true
		}
	}
	if v := ctx.Value(CtxIngestrVersion); v != nil {
		if version, ok := v.(string); ok && version != "" {
			return "ingestr@" + version, false
		}
	}
	return "ingestr@" + IngestrVersionV1, false
}

// ingestrRunCmd builds the `uv tool run` invocation for ingestr. Each call resolves
// a per-version environment via `--from ingestr@<ver>` (or a local path), so v0 and
// v1 assets in the same pipeline never share — and never clobber — an installation.
func (u *UvPythonRunner) ingestrRunCmd(ctx context.Context, extraPackages, args []string) []string {
	ingestrPackageName, isLocal := u.ingestrPackage(ctx)
	cmdline := make([]string, 0, 12+(2*len(extraPackages))+len(args))
	cmdline = append(cmdline, "tool", "run", "--no-config", "--prerelease", "allow", "--python", pythonVersionForIngestr)
	if isLocal {
		// Force uv to rebuild the local source on every run so edits in
		// debug-ingestr-src are picked up.
		cmdline = append(cmdline, "--reinstall-package", "ingestr")
	}
	cmdline = append(cmdline, "--from", ingestrPackageName)
	for _, pkg := range extraPackages {
		cmdline = append(cmdline, "--with", pkg)
	}
	cmdline = append(cmdline, "ingestr")
	return append(cmdline, args...)
}

// tailBuffer is a bounded io.Writer that retains at most maxBytes of the most
// recently written data. When the buffer is flushed after an error, a notice
// is prepended if earlier output was dropped.
type tailBuffer struct {
	mu        sync.Mutex
	data      []byte
	maxBytes  int
	truncated bool
}

func newTailBuffer(maxBytes int) *tailBuffer {
	return &tailBuffer{maxBytes: maxBytes}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(p) >= t.maxBytes {
		// Single write larger than cap: keep only the tail.
		t.data = append(t.data[:0], p[len(p)-t.maxBytes:]...)
		t.truncated = true
		return len(p), nil
	}
	t.data = append(t.data, p...)
	if len(t.data) > t.maxBytes {
		excess := len(t.data) - t.maxBytes
		copy(t.data, t.data[excess:])
		t.data = t.data[:t.maxBytes]
		t.truncated = true
	}
	return len(p), nil
}

func (t *tailBuffer) flushTo(w io.Writer) {
	if t.truncated {
		_, _ = w.Write([]byte("[earlier ingestr output omitted]\n"))
	}
	_, _ = w.Write(t.data)
}

// pythonArrowInlineMetadata is the PEP 723 inline script metadata header in the arrow template.
// When running in pyproject mode, this must be stripped so uv stays in project mode
// (inline metadata forces uv into script mode, ignoring pyproject.toml dependencies).
const pythonArrowInlineMetadata = `# /// script
# dependencies = [
#   "pyarrow==18.0.0"
# ]
# ///`

const PythonArrowTemplate = `
# /// script
# dependencies = [
#   "pyarrow==18.0.0"
# ]
# ///

import sys
import importlib.util
from pathlib import Path

def import_module_from_path(module_path: str, module_name: str):
    project_root = str(Path(module_path))
    sys.path.insert(0, project_root)

    return importlib.import_module(module_name)

def convert_and_write(df):
    if df is None:
        return  # Go-side will detect missing arrow file and log a warning

    import pyarrow as pa
    import pyarrow.ipc as ipc

    # Try importing pandas and polars for isinstance checks
    try:
        import pandas as pd
    except ImportError:
        pd = None

    try:
        import polars as pl
    except ImportError:
        pl = None

    def is_polars_dataframe(obj):
        if pl is not None and isinstance(obj, pl.DataFrame):
            return True
        type_module = type(obj).__module__
        type_name = type(obj).__name__
        return 'polars' in type_module and type_name == 'DataFrame'

    def is_pandas_dataframe(obj):
        if pd is not None and isinstance(obj, pd.DataFrame):
            return True
        type_module = type(obj).__module__
        type_name = type(obj).__name__
        return 'pandas' in type_module and type_name == 'DataFrame'

    def write_arrow_tables(tables):
        iterator = iter(tables)
        try:
            first_table = next(iterator)
        except StopIteration:
            return

        if not isinstance(first_table, pa.Table):
            raise TypeError(f"Unsupported return type: {type(first_table)}")

        with pa.OSFile("$ARROW_FILE_PATH", 'wb') as f:
            writer = ipc.new_file(f, first_table.schema)
            writer.write_table(first_table)
            for next_table in iterator:
                if not isinstance(next_table, pa.Table):
                    raise TypeError(f"Unsupported yielded type: {type(next_table)}. expected pyarrow.Table.")
                if not next_table.schema.equals(first_table.schema):
                    raise TypeError("All yielded pyarrow Tables must have the same schema.")
                writer.write_table(next_table)
            writer.close()

    # Polars can write Arrow IPC directly, avoiding the extra PyArrow writer
    # memory peak seen when converting through a PyArrow Table first.
    if is_polars_dataframe(df):
        if pl is None:
            raise TypeError(f"Unsupported return type: {type(df)}. polars DataFrame detected but polars cannot be imported.")
        df.write_ipc("$ARROW_FILE_PATH")
        return

    if is_pandas_dataframe(df):
        if pd is None:
            raise TypeError(f"Unsupported return type: {type(df)}. pandas DataFrame detected but pandas cannot be imported.")
        table = pa.Table.from_pandas(df)
    elif isinstance(df, pa.Table):
        write_arrow_tables([df])
        return
    elif isinstance(df, (list, tuple)):
        if not df:
            return
        if isinstance(df[0], pa.Table):
            write_arrow_tables(df)
            return
        table = pa.Table.from_pylist(list(df))
    elif hasattr(df, '__iter__') and not isinstance(df, (str, bytes)):
        # Handle generators and other iterables (but not strings/bytes).
        # Each yielded value is written as its own Arrow batch, as-is, so a
        # generator never has to hold the full dataset in memory. A value can be:
        #   1. an individual dict:        yield {"col": val}         -> a one-row batch
        #   2. a batch (list of dicts):   yield [{"col": val}, ...]  -> one batch per page
        #   3. a pyarrow Table:           yield pa.table(...)         -> written directly
        # The batch granularity is whatever materialize() yields; we do not
        # re-chunk. This mirrors how yielded pyarrow Tables are handled.
        #
        # The Arrow IPC file has a single schema fixed when the first batch is
        # written, so every yielded batch must share one schema. If each yield
        # inferred its own schema, a None in one yield would produce a 'null'-typed
        # column that mismatches a typed value in the next yield (a common
        # database-cursor pattern: "for row in cursor: yield row"). To handle this
        # we buffer only the leading rows until their inferred schema has no
        # null-typed columns, lock that schema, then stream every later batch
        # against it so nullable values and missing optional keys conform instead
        # of raising. The buffer is just the warm-up window; if a column stays
        # entirely None we cannot infer its type and fall back to buffering it.
        def rows_to_tables(items):
            locked_schema = None
            pending = []
            for item in items:
                if isinstance(item, pa.Table):
                    # A yielded pa.Table carries an explicit schema, so use it to
                    # flush any buffered rows (whose own inference may have left
                    # null-typed columns) and to lock the schema for later rows.
                    # This keeps the table and the surrounding dict batches in
                    # agreement, whether the table comes before or after them.
                    if pending:
                        yield pa.Table.from_pylist(pending, schema=item.schema)
                        pending = []
                    if locked_schema is None:
                        locked_schema = item.schema
                    yield item
                    continue
                rows = item if isinstance(item, (list, tuple)) else [item]
                if not rows:  # skip empty pages so we never emit a zero-row batch
                    continue
                if locked_schema is not None:
                    yield pa.Table.from_pylist(list(rows), schema=locked_schema)
                    continue
                pending.extend(rows)
                table = pa.Table.from_pylist(pending)
                if not any(pa.types.is_null(field.type) for field in table.schema):
                    locked_schema = table.schema
                    yield table
                    pending = []
            if pending:  # a column stayed all-None through the end of the stream
                yield pa.Table.from_pylist(pending)

        write_arrow_tables(rows_to_tables(df))
        return
    else:
        raise TypeError(f"Unsupported return type: {type(df)}")

    write_arrow_tables([table])

module = import_module_from_path("$REPO_ROOT", "$MODULE_PATH")
convert_and_write(module.materialize())
`

type SqlfluffRunner struct {
	UvInstaller    uvInstaller
	binaryFullPath string
}

type SQLFileInfo struct {
	FilePath string
	Dialect  string
}

// DetectDialectFromAssetType extracts the dialect from an asset's type field
// by splitting on the first dot and mapping the prefix.
func DetectDialectFromAssetType(assetType string) string {
	parts := strings.Split(assetType, ".")
	if len(parts) == 0 {
		return "ansi" // fallback to ANSI SQL
	}

	prefix := parts[0]
	if dialect, exists := DatabasePrefixToSqlfluffDialect[prefix]; exists {
		return dialect
	}
	return "ansi" // fallback to ANSI SQL
}

func (s *SqlfluffRunner) RunSqlfluffWithDialects(ctx context.Context, sqlFiles []SQLFileInfo, repo *git.Repo) error {
	binaryFullPath, err := s.UvInstaller.EnsureUvInstalled(ctx)
	if err != nil {
		return err
	}
	s.binaryFullPath = binaryFullPath

	// Install sqlfluff
	err = s.installSqlfluff(ctx, repo)
	if err != nil {
		return errors.Wrap(err, "failed to install sqlfluff")
	}

	if len(sqlFiles) == 0 {
		return nil
	}

	// For single asset, use majority dialect detection logic
	if len(sqlFiles) == 1 {
		// For single asset, use its specific dialect
		sqlFileInfo := sqlFiles[0]
		err = s.formatSQLFileWithDialect(ctx, sqlFileInfo.FilePath, sqlFileInfo.Dialect, repo)
		if err != nil {
			// Check if it's a non-critical exit code (1 = unfixable violations)
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
				// Exit code 1 means there were unfixable violations but fixes were applied
				// This is acceptable, so we continue
				return nil
			}
			return errors.Wrapf(err, "failed to format SQL file %s", sqlFileInfo.FilePath)
		}
		return nil
	}

	// Use conc pool to process SQL files in parallel with max 30 goroutines
	sqlPool := pool.New().WithMaxGoroutines(30)
	errorList := make([]error, 0)
	var errorMutex sync.Mutex

	for _, sqlFileInfo := range sqlFiles {
		sqlPool.Go(func() {
			fileInfo := sqlFileInfo

			err := s.formatSQLFileWithDialect(ctx, fileInfo.FilePath, fileInfo.Dialect, repo)
			if err != nil {
				// Check if it's a non-critical exit code (1 = unfixable violations)
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
					// Exit code 1 means there were unfixable violations but fixes were applied
					// This is acceptable, so we continue
					return
				}

				// Add error to the list in a thread-safe manner
				errorMutex.Lock()
				errorList = append(errorList, errors.Wrapf(err, "failed to format SQL file %s", fileInfo.FilePath))
				errorMutex.Unlock()
			}
		})
	}

	sqlPool.Wait()

	// Return the first error if any occurred
	if len(errorList) > 0 {
		return errorList[0]
	}

	return nil
}

func (s *SqlfluffRunner) installSqlfluff(ctx context.Context, repo *git.Repo) error {
	cmdArgs := []string{
		"tool",
		"install",
		"--no-config",
		"--force",
		"--quiet",
		"--python",
		pythonVersionForIngestr,
		"sqlfluff==" + sqlfluffVersion,
	}

	cmd := &CommandInstance{
		Name: s.binaryFullPath,
		Args: cmdArgs,
	}

	cmdRunner := &CommandRunner{}
	return cmdRunner.Run(ctx, repo, cmd)
}

var rendererFs = afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)

type PyprojectToml struct {
	Tool struct {
		Sqlfluff map[string]any `toml:"sqlfluff"`
	} `toml:"tool"`
}

func parsePyprojectToml(filePath string) (map[string]any, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil // No pyproject.toml file found
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read pyproject.toml: %w", err)
	}

	var pyproject PyprojectToml
	if err := toml.Unmarshal(content, &pyproject); err != nil {
		return nil, fmt.Errorf("failed to parse pyproject.toml: %w", err)
	}

	return pyproject.Tool.Sqlfluff, nil
}

func convertTomlConfigToIni(config map[string]any, prefix string) string {
	var result strings.Builder

	// Sort keys to ensure deterministic output
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := config[key]
		sectionName := key
		if prefix != "" {
			sectionName = prefix + ":" + key
		}

		switch v := value.(type) {
		case map[string]any:
			// Nested section
			fmt.Fprintf(&result, "[sqlfluff:%s]\n", sectionName)
			result.WriteString(convertTomlConfigToIni(v, sectionName))
			result.WriteString("\n")
		case string:
			fmt.Fprintf(&result, "%s = %s\n", key, v)
		case bool:
			fmt.Fprintf(&result, "%s = %t\n", key, v)
		case int, int64, float64:
			fmt.Fprintf(&result, "%s = %v\n", key, v)
		case []any:
			// Handle arrays like exclude_rules
			items := make([]string, 0, len(v))
			for _, item := range v {
				items = append(items, fmt.Sprintf("%v", item))
			}
			fmt.Fprintf(&result, "%s = %s\n", key, strings.Join(items, ","))
		default:
			fmt.Fprintf(&result, "%s = %v\n", key, v)
		}
	}

	return result.String()
}

func findPipelineFile(basePath string) (string, error) {
	pipelineFileNames := []string{"pipeline.yml", "pipeline.yaml"}
	for _, fileName := range pipelineFileNames {
		candidatePath := filepath.Join(basePath, fileName)
		if _, err := os.Stat(candidatePath); err == nil {
			return candidatePath, nil
		}
	}
	return "", fmt.Errorf("no pipeline file found in '%s'. Supported files: %v", basePath, pipelineFileNames)
}

func (s *SqlfluffRunner) createSqlfluffConfigWithJinjaContext(sqlFilePath string, repo *git.Repo) string {
	// Try to find and parse pyproject.toml for existing sqlfluff configuration
	pyprojectPath := filepath.Join(repo.Path, "pyproject.toml")
	userSqlfluffConfig, err := parsePyprojectToml(pyprojectPath)
	if err != nil {
		// Log the error but continue - we'll use default config
		fmt.Printf("Warning: failed to parse pyproject.toml: %v\n", err)
		userSqlfluffConfig = nil
	}

	// Try to find pipeline.yml to extract user-defined variables
	var pipelineVars map[string]any

	// Look for pipeline.yml or pipeline.yaml in the repo using the same logic as CreatePipelineFromPath
	pipelinePath, err := findPipelineFile(repo.Path)
	if err == nil {
		if pipelineInstance, err := pipeline.PipelineFromPath(pipelinePath, rendererFs); err == nil && pipelineInstance != nil {
			// Use Variables.Value() to get the default values
			pipelineVars = pipelineInstance.Variables.Value()
		}
	}

	if pipelineVars == nil {
		pipelineVars = map[string]any{
			"test1": "my_test1",
			"test2": "placeholder",
		}
	}

	// Start with basic sqlfluff config
	var configBuilder strings.Builder
	configBuilder.WriteString("[sqlfluff]\ntemplater = jinja\n\n")

	// Merge user's existing sqlfluff configuration if available
	if userSqlfluffConfig != nil {
		// Convert user's TOML config to INI format and append
		userConfigSection := convertTomlConfigToIni(userSqlfluffConfig, "")
		if userConfigSection != "" {
			configBuilder.WriteString(userConfigSection)
			configBuilder.WriteString("\n")
		}
	}

	// Add Jinja context section
	configBuilder.WriteString("[sqlfluff:templater:jinja:context]\n")

	// Add pipeline variables under var namespace
	for varName, varValue := range pipelineVars {
		if strValue, ok := varValue.(string); ok {
			fmt.Fprintf(&configBuilder, "var.%s = %s\n", varName, strValue)
		} else {
			fmt.Fprintf(&configBuilder, "var.%s = placeholder\n", varName)
		}
	}

	// Add standard Bruin variables
	configBuilder.WriteString("end_date = 2024-01-01\n")
	configBuilder.WriteString("start_date = 2024-01-01\n")
	configBuilder.WriteString("start_datetime = 2024-01-01 00:00:00\n")
	configBuilder.WriteString("end_datetime = 2024-01-01 00:00:00\n")
	configBuilder.WriteString("start_timestamp = 2024-01-01 00:00:00\n")
	configBuilder.WriteString("end_timestamp = 2024-01-01 00:00:00\n")
	configBuilder.WriteString("pipeline = placeholder\n")
	configBuilder.WriteString("run_id = placeholder\n")
	fmt.Fprintf(&configBuilder, "this = %s\n", filepath.Base(strings.TrimSuffix(sqlFilePath, filepath.Ext(sqlFilePath))))

	return configBuilder.String()
}

func (s *SqlfluffRunner) formatSQLFileWithDialect(ctx context.Context, sqlFile, dialect string, repo *git.Repo) error {
	// Create a temporary .sqlfluff config file with Jinja context
	configContent := s.createSqlfluffConfigWithJinjaContext(sqlFile, repo)

	// Create temporary config file
	configFile, err := os.CreateTemp("", ".sqlfluff-*.cfg")
	if err != nil {
		return s.formatSQLFileWithoutTemplating(ctx, sqlFile, dialect, repo)
	}
	defer os.Remove(configFile.Name())
	defer configFile.Close()

	if _, err := configFile.WriteString(configContent); err != nil {
		return s.formatSQLFileWithoutTemplating(ctx, sqlFile, dialect, repo)
	}
	configFile.Close()

	cmdArgs := []string{
		"tool",
		"run",
		"--no-config",
		"--python",
		pythonVersionForIngestr,
		"sqlfluff",
		"fix",
		"--quiet",
		"--dialect",
		dialect,
		"--config",
		configFile.Name(),
		sqlFile,
	}

	cmd := &CommandInstance{
		Name: s.binaryFullPath,
		Args: cmdArgs,
	}

	cmdRunner := &CommandRunner{}
	return cmdRunner.Run(ctx, repo, cmd)
}

func (s *SqlfluffRunner) formatSQLFileWithoutTemplating(ctx context.Context, sqlFile, dialect string, repo *git.Repo) error {
	cmdArgs := []string{
		"tool",
		"run",
		"--no-config",
		"--python",
		pythonVersionForIngestr,
		"sqlfluff",
		"fix",
		"--quiet",
		"--dialect",
		dialect,
		sqlFile,
	}

	cmd := &CommandInstance{
		Name: s.binaryFullPath,
		Args: cmdArgs,
	}

	cmdRunner := &CommandRunner{}
	return cmdRunner.Run(ctx, repo, cmd)
}
