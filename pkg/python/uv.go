package python

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
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
	"github.com/bruin-data/bruin/pkg/user"
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
}

const (
	UvVersion               = "0.6.16"
	pythonVersionForIngestr = "3.11"
	ingestrVersion          = "0.14.2"
	sqlfluffVersion         = "3.4.1"
)

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

// UvChecker handles checking and installing the uv package manager.
type UvChecker struct {
	mut sync.Mutex
	cmd CommandRunner
}

// EnsureUvInstalled checks if uv is installed and installs it if not present, then returns the full path of the binary.
func (u *UvChecker) EnsureUvInstalled(ctx context.Context) (string, error) {
	u.mut.Lock()
	defer u.mut.Unlock()

	// Check if uv is already installed
	m := user.NewConfigManager(afero.NewOsFs())
	bruinHomeDirAbsPath, err := m.EnsureAndGetBruinHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get bruin home directory")
	}
	var binaryName string
	if runtime.GOOS == "windows" {
		binaryName = "uv.exe"
	} else {
		binaryName = "uv"
	}
	uvBinaryPath := filepath.Join(bruinHomeDirAbsPath, binaryName)
	if _, err := os.Stat(uvBinaryPath); errors.Is(err, os.ErrNotExist) {
		err = u.installUvCommand(ctx, bruinHomeDirAbsPath)
		if err != nil {
			return "", err
		}
		return uvBinaryPath, nil
	}

	cmd := exec.Command(uvBinaryPath, "version", "--no-config", "--output-format", "json")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to check uv version: %w -- Output: %s", err, output)
	}

	var uvVersion struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(output, &uvVersion); err != nil {
		return "", fmt.Errorf("failed to parse uv version: %w", err)
	}

	if uvVersion.Version != UvVersion {
		err = u.installUvCommand(ctx, bruinHomeDirAbsPath)
		if err != nil {
			return "", err
		}
		return uvBinaryPath, nil
	}

	return uvBinaryPath, nil
}

const CtxUseWingetForUv = "use_winget_for_uv"

func (u *UvChecker) installUvCommand(ctx context.Context, dest string) error {
	var output io.Writer = os.Stdout
	if ctx.Value(executor.KeyPrinter) != nil {
		output = ctx.Value(executor.KeyPrinter).(io.Writer)
	}

	_, _ = output.Write([]byte("===============================\n"))
	_, _ = output.Write([]byte(fmt.Sprintf("Installing uv v%s...\n", UvVersion)))
	_, _ = output.Write([]byte("This is a one-time operation.\n"))
	_, _ = output.Write([]byte("\n"))

	var commandInstance *exec.Cmd
	if runtime.GOOS == "windows" {
		// this conditional part is to test the powershell stuff safely.
		// once we confirm this on different systems we should remove winget altogether.
		useWinget := false
		if ctx.Value(CtxUseWingetForUv) != nil {
			useWinget = ctx.Value(CtxUseWingetForUv).(bool)
		}

		if useWinget {
			commandInstance = exec.Command(Shell, ShellSubcommandFlag, fmt.Sprintf("winget install --accept-package-agreements --accept-source-agreements --silent --id=astral-sh.uv --version %s --location %s -e", UvVersion, dest)) //nolint:gosec
		} else {
			commandInstance = exec.Command("powershell", "-ExecutionPolicy", "ByPass", "-c", fmt.Sprintf("$env:NO_MODIFY_PATH=1 ; $env:UV_INSTALL_DIR='~/.bruin' ; irm https://astral.sh/uv/%s/install.ps1 | iex", UvVersion)) //nolint:gosec
		}
	} else {
		commandInstance = exec.Command(Shell, ShellSubcommandFlag, fmt.Sprintf("set -e; curl -LsSf https://astral.sh/uv/%s/install.sh | UV_INSTALL_DIR=\"%s\" NO_MODIFY_PATH=1 sh", UvVersion, dest)) //nolint:gosec
	}

	err := u.cmd.RunAnyCommand(ctx, commandInstance)
	if err != nil {
		return fmt.Errorf("failed to install uv: %w", err)
	}

	_, _ = output.Write([]byte("\n"))
	_, _ = output.Write([]byte(fmt.Sprintf("Installed uv v%s, continuing...\n", UvVersion)))
	_, _ = output.Write([]byte("===============================\n"))
	_, _ = output.Write([]byte("\n"))

	return nil
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

	pythonVersion := "3.11"
	if execCtx.asset.Image != "" {
		image := execCtx.asset.Image
		parts := strings.Split(image, ":")
		if len(parts) > 1 && parts[0] == "python" && AvailablePythonVersions[parts[1]] {
			pythonVersion = parts[1]
		}
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

	err = u.Cmd.Run(ctx, repo, &CommandInstance{
		Name: u.binaryFullPath,
		Args: u.ingestrInstallCmd(ctx, extraPackages),
	})
	if err != nil {
		return errors.Wrap(err, "failed to install ingestr")
	}

	flags := []string{"tool", "run", "--no-config", "--prerelease", "allow", "--python", pythonVersionForIngestr, "ingestr"}
	flags = append(flags, args...)

	noDependencyCommand := &CommandInstance{
		Name:    u.binaryFullPath,
		Args:    flags,
		EnvVars: map[string]string{},
	}

	return u.Cmd.Run(ctx, repo, noDependencyCommand)
}

func (u *UvPythonRunner) runWithNoMaterialization(ctx context.Context, execCtx *executionContext, pythonVersion string) error {
	flags := []string{"run", "--no-config", "--no-sync", "--python", pythonVersion}
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

	arrowScript := strings.ReplaceAll(PythonArrowTemplate, "$REPO_ROOT", strings.ReplaceAll(execCtx.repo.Path, "\\", "\\\\"))
	arrowScript = strings.ReplaceAll(arrowScript, "$MODULE_PATH", execCtx.module)
	arrowScript = strings.ReplaceAll(arrowScript, "$ARROW_FILE_PATH", strings.ReplaceAll(arrowFilePath, "\\", "\\\\"))

	if _, err := io.WriteString(tempPyScript, arrowScript); err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	flags := []string{"run", "--no-config", "--no-sync", "--python", pythonVersion}
	if execCtx.requirementsTxt != "" {
		flags = append(flags, "--with-requirements", execCtx.requirementsTxt)
	}

	flags = append(flags, tempPyScript.Name())

	err = u.Cmd.Run(ctx, execCtx.repo, &CommandInstance{
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

	_, _ = output.Write([]byte("Successfully collected the data from the asset, uploading to the destination...\n"))

	if len(asset.Parameters) == 0 {
		asset.Parameters = make(map[string]string)
	}

	if mat.Strategy != "" {
		asset.Parameters["incremental_strategy"] = string(mat.Strategy)
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
	})
	if err != nil {
		return err
	}

	destConnectionName, err := execCtx.pipeline.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection := u.conn.GetConnection(destConnectionName)

	destConnectionInst, ok := destConnection.(pipelineConnection)
	if !ok {
		return errors.Errorf("destination connection %s is not supported by ingestr or doesn't exist", destConnectionName)
	}

	destURI, err := destConnectionInst.GetIngestrURI()
	if err != nil {
		return errors.New("could not get the source uri")
	}

	if destURI == "" {
		return errors.New("could not determine the destination, please set the `connection` value in the asset definition.")
	}

	cmdArgs = append(cmdArgs, "--dest-uri", destURI)

	if strings.HasPrefix(destURI, "duckdb://") {
		if dbURIGetter, ok := destConnectionInst.(interface{ GetDBConnectionURI() string }); ok {
			duck.LockDatabase(dbURIGetter.GetDBConnectionURI())
			defer duck.UnlockDatabase(dbURIGetter.GetDBConnectionURI())
		}
	}

	err = u.Cmd.Run(ctx, execCtx.repo, &CommandInstance{
		Name: u.binaryFullPath,
		Args: u.ingestrInstallCmd(ctx, nil),
	})
	if err != nil {
		return errors.Wrap(err, "failed to install ingestr")
	}

	runArgs := slices.Concat([]string{"tool", "run", "--no-config", "--prerelease", "allow", "--python", pythonVersionForIngestr, "ingestr"}, cmdArgs)

	if debug := ctx.Value(executor.KeyIsDebug); debug != nil {
		boolVal := debug.(*bool)
		if *boolVal {
			_, _ = output.Write([]byte("Running CommandInstance: uv " + strings.Join(runArgs, " ") + "\n"))
		}
	}

	err = u.Cmd.Run(ctx, execCtx.repo, &CommandInstance{
		Name: u.binaryFullPath,
		Args: runArgs,
	})
	if err != nil {
		return errors.Wrap(err, "failed to run load the data into the destination")
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
	return "ingestr@" + ingestrVersion, false
}

// ingestrInstallCmd returns the uv tool commandline
// args necessary for installing ingestr.
func (u *UvPythonRunner) ingestrInstallCmd(ctx context.Context, pkgs []string) []string {
	ingestrPackageName, isLocal := u.ingestrPackage(ctx)
	cmdline := []string{
		"tool",
		"install",
		"--no-config",
		"--force",
		"--quiet",
		"--prerelease",
		"allow",
		"--python",
		pythonVersionForIngestr,
	}
	for _, pkg := range pkgs {
		cmdline = append(cmdline, "--with", pkg)
	}
	if isLocal {
		cmdline = append(cmdline, "--reinstall")
	}
	cmdline = append(cmdline, ingestrPackageName)
	return cmdline
}

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

module = import_module_from_path("$REPO_ROOT", "$MODULE_PATH")
df = module.materialize()

import pyarrow as pa
import pyarrow.ipc as ipc

if str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
    table = pa.Table.from_pandas(df)
elif str(type(df)) == "<class 'polars.dataframe.frame.DataFrame'>":
    table = df.to_arrow()
elif str(type(df)) == "<class 'generator'>":
    table = pa.Table.from_pylist(list(df)) # TODO: Terrible implementation, but works for now
elif str(type(df)) == "<class 'list'>":
    table = pa.Table.from_pylist(df)
else:
    raise TypeError(f"Unsupported return type: {type(df)}")

# Write to memory mapped file
with pa.OSFile("$ARROW_FILE_PATH", 'wb') as f:
	writer = ipc.new_file(f, table.schema)
	writer.write_table(table)
	writer.close()
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
			result.WriteString(fmt.Sprintf("[sqlfluff:%s]\n", sectionName))
			result.WriteString(convertTomlConfigToIni(v, sectionName))
			result.WriteString("\n")
		case string:
			result.WriteString(fmt.Sprintf("%s = %s\n", key, v))
		case bool:
			result.WriteString(fmt.Sprintf("%s = %t\n", key, v))
		case int, int64, float64:
			result.WriteString(fmt.Sprintf("%s = %v\n", key, v))
		case []any:
			// Handle arrays like exclude_rules
			var items []string
			for _, item := range v {
				items = append(items, fmt.Sprintf("%v", item))
			}
			result.WriteString(fmt.Sprintf("%s = %s\n", key, strings.Join(items, ",")))
		default:
			result.WriteString(fmt.Sprintf("%s = %v\n", key, v))
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
	configContent := "[sqlfluff]\ntemplater = jinja\n\n"

	// Merge user's existing sqlfluff configuration if available
	if userSqlfluffConfig != nil {
		// Convert user's TOML config to INI format and append
		userConfigSection := convertTomlConfigToIni(userSqlfluffConfig, "")
		if userConfigSection != "" {
			configContent += userConfigSection + "\n"
		}
	}

	// Add Jinja context section
	configContent += "[sqlfluff:templater:jinja:context]\n"

	// Add pipeline variables under var namespace
	for varName, varValue := range pipelineVars {
		if strValue, ok := varValue.(string); ok {
			configContent += fmt.Sprintf("var.%s = %s\n", varName, strValue)
		} else {
			configContent += fmt.Sprintf("var.%s = placeholder\n", varName)
		}
	}

	// Add standard Bruin variables
	configContent += "end_date = 2024-01-01\n"
	configContent += "start_date = 2024-01-01\n"
	configContent += "start_datetime = 2024-01-01 00:00:00\n"
	configContent += "end_datetime = 2024-01-01 00:00:00\n"
	configContent += "start_timestamp = 2024-01-01 00:00:00\n"
	configContent += "end_timestamp = 2024-01-01 00:00:00\n"
	configContent += "pipeline = placeholder\n"
	configContent += "run_id = placeholder\n"
	configContent += fmt.Sprintf("this = %s\n", filepath.Base(strings.TrimSuffix(sqlFilePath, filepath.Ext(sqlFilePath))))

	return configContent
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
