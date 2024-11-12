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
	"strings"
	"sync"
	"time"

	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/ingestr"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
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
	UvVersion               = "0.5.1"
	pythonVersionForIngestr = "3.11"
)

// UvChecker handles checking and installing the uv package manager.
type UvChecker struct {
	mut sync.Mutex
	cmd commandRunner
}

// EnsureUvInstalled checks if uv is installed and installs it if not present.
func (u *UvChecker) EnsureUvInstalled(ctx context.Context) error {
	u.mut.Lock()
	defer u.mut.Unlock()

	// Check if uv is already installed
	_, err := exec.LookPath("uv")
	if err != nil {
		err = u.installUvCommand(ctx)
		if err != nil {
			return err
		}
		return nil
	}

	cmd := exec.Command("uv", "version", "--output-format", "json")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to check uv version: %w\nOutput: %s\n\nPlease install uv v%s yourself: https://docs.astral.sh/uv/getting-started/installation/", err, output, UvVersion)
	}

	var uvVersion struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(output, &uvVersion); err != nil {
		return fmt.Errorf("failed to parse uv version: %w", err)
	}

	if uvVersion.Version != UvVersion {
		err = u.installUvCommand(ctx)
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (u *UvChecker) installUvCommand(ctx context.Context) error {
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
		commandInstance = exec.Command(Shell, ShellSubcommandFlag, fmt.Sprintf("winget install --silent --id=astral-sh.uv --version %s -e", UvVersion)) //nolint:gosec
	} else {
		commandInstance = exec.Command(Shell, ShellSubcommandFlag, fmt.Sprintf(" set -o pipefail; curl -LsSf https://astral.sh/uv/%s/install.sh | sh", UvVersion)) //nolint:gosec
	}

	err := u.cmd.RunAnyCommand(ctx, commandInstance)
	if err != nil {
		return fmt.Errorf("failed to install uv: %w\nPlease install uv v%s yourself: https://docs.astral.sh/uv/getting-started/installation/", err, UvVersion)
	}

	_, _ = output.Write([]byte("\n"))
	_, _ = output.Write([]byte(fmt.Sprintf("Installed uv v%s, continuing...\n", UvVersion)))
	_, _ = output.Write([]byte("===============================\n"))
	_, _ = output.Write([]byte("\n"))

	return nil
}

type uvInstaller interface {
	EnsureUvInstalled(ctx context.Context) error
}

type connectionFetcher interface {
	GetConnection(name string) (interface{}, error)
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

type uvPythonRunner struct {
	cmd         cmd
	uvInstaller uvInstaller
	conn        connectionFetcher
}

func (u *uvPythonRunner) Run(ctx context.Context, execCtx *executionContext) error {
	err := u.uvInstaller.EnsureUvInstalled(ctx)
	if err != nil {
		return err
	}

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

func (u *uvPythonRunner) runWithNoMaterialization(ctx context.Context, execCtx *executionContext, pythonVersion string) error {
	flags := []string{"run", "--python", pythonVersion}
	if execCtx.requirementsTxt != "" {
		flags = append(flags, "--with-requirements", execCtx.requirementsTxt)
	}

	flags = append(flags, "--module", execCtx.module)

	noDependencyCommand := &command{
		Name:    "uv",
		Args:    flags,
		EnvVars: execCtx.envVariables,
	}

	return u.cmd.Run(ctx, execCtx.repo, noDependencyCommand)
}

func (u *uvPythonRunner) runWithMaterialization(ctx context.Context, execCtx *executionContext, pythonVersion string) error {
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

	arrowScript := strings.ReplaceAll(PythonArrowTemplate, "$REPO_ROOT", execCtx.repo.Path)
	arrowScript = strings.ReplaceAll(arrowScript, "$MODULE_PATH", execCtx.module)
	arrowScript = strings.ReplaceAll(arrowScript, "$ARROW_FILE_PATH", arrowFilePath)

	if _, err := io.WriteString(tempPyScript, arrowScript); err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	flags := []string{"run", "--python", pythonVersion}
	if execCtx.requirementsTxt != "" {
		flags = append(flags, "--with-requirements", execCtx.requirementsTxt)
	}

	flags = append(flags, tempPyScript.Name())

	err = u.cmd.Run(ctx, execCtx.repo, &command{
		Name:    "uv",
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

	// build ingestr flags
	cmdArgs := []string{
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
	}

	destConnectionName, err := execCtx.pipeline.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection, err := u.conn.GetConnection(destConnectionName)
	if err != nil {
		return fmt.Errorf("destination connection %s not found", destConnectionName)
	}

	destConnectionInst, ok := destConnection.(pipelineConnection)
	if !ok {
		return errors.New("destination connection does not implement pipelineConnection, please create an issue in Bruin repo: https://github.com/bruin-data/bruin")
	}

	destURI, err := destConnectionInst.GetIngestrURI()
	if err != nil {
		return errors.New("could not get the source uri")
	}

	if destURI == "" {
		return errors.New("could not determine the destination, please set the `connection` value in the asset definition.")
	}

	cmdArgs = append(cmdArgs, "--dest-uri", destURI)

	fullRefresh := ctx.Value(pipeline.RunConfigFullRefresh)
	if fullRefresh != nil && fullRefresh.(bool) {
		cmdArgs = append(cmdArgs, "--full-refresh")
	}

	if mat.Strategy != "" {
		cmdArgs = append(cmdArgs, "--incremental-strategy", string(mat.Strategy))
	}

	if mat.IncrementalKey != "" {
		cmdArgs = append(cmdArgs, "--incremental-key", mat.IncrementalKey)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) > 0 {
		for _, pk := range primaryKeys {
			cmdArgs = append(cmdArgs, "--primary-key", pk)
		}
	}

	if strings.HasPrefix(destURI, "duckdb://") {
		if dbURIGetter, ok := destConnectionInst.(interface{ GetDBConnectionURI() string }); ok {
			duck.LockDatabase(dbURIGetter.GetDBConnectionURI())
			defer duck.UnlockDatabase(dbURIGetter.GetDBConnectionURI())
		}
	}

	ingestrPackageName := "ingestr@" + ingestr.IngestrVersion
	err = u.cmd.Run(ctx, execCtx.repo, &command{
		Name: "uv",
		Args: []string{"tool", "install", "--quiet", "--python", pythonVersionForIngestr, ingestrPackageName},
	})
	if err != nil {
		return errors.Wrap(err, "failed to install ingestr")
	}

	runArgs := slices.Concat([]string{"tool", "run", "--python", pythonVersionForIngestr, ingestrPackageName}, cmdArgs)

	if debug := ctx.Value(executor.KeyIsDebug); debug != nil {
		boolVal := debug.(*bool)
		if *boolVal {
			_, _ = output.Write([]byte("Running command: uv " + strings.Join(runArgs, " ") + "\n"))
		}

	}

	err = u.cmd.Run(ctx, execCtx.repo, &command{
		Name: "uv",
		Args: runArgs,
	})
	if err != nil {
		return errors.Wrap(err, "failed to run load the data into the destination")
	}

	_, _ = output.Write([]byte("Successfully loaded the data from the asset into the destination.\n"))

	return nil
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
