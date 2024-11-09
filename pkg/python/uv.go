package python

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/executor"
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
	UvVersion = "0.5.1"
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

type uvPythonRunner struct {
	cmd         cmd
	uvInstaller uvInstaller
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

	// uv run python --python <py-version> --with-requirements <requirements.txt> --module <module>
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
