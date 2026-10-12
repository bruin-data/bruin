package uv

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	Version = "0.10.8"

	// Shell is the shell to use for running commands.
	Shell = "/bin/sh"
	// ShellSubcommandFlag is the flag to pass to the shell to run a command.
	ShellSubcommandFlag = "-c"
)

// Checker handles checking and installing the uv package manager.
type Checker struct {
	mut sync.Mutex
}

// Environment keeps uv-managed caches and Python installations in BRUIN_HOME
// when configured. Explicit UV_* settings continue to take precedence.
func Environment() ([]string, error) {
	env := os.Environ()
	if os.Getenv("BRUIN_HOME") == "" {
		return env, nil
	}
	home, err := user.NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	if err != nil {
		return nil, err
	}
	for key, dir := range map[string]string{
		"UV_CACHE_DIR":          "uv-cache",
		"UV_PYTHON_INSTALL_DIR": "python",
		"UV_TOOL_DIR":           "uv-tools",
		"UV_TOOL_BIN_DIR":       "bin",
	} {
		if os.Getenv(key) == "" {
			env = append(env, key+"="+filepath.Join(home, dir))
		}
	}
	return env, nil
}

// EnsureUvInstalled checks if uv is installed and installs it if not present, then returns the full path of the binary.
func (u *Checker) EnsureUvInstalled(ctx context.Context) (string, error) {
	u.mut.Lock()
	defer u.mut.Unlock()

	m := user.NewConfigManager(afero.NewOsFs())
	bruinHomeDirAbsPath, err := m.EnsureAndGetBruinHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get bruin home directory")
	}

	binaryName := "uv"
	if runtime.GOOS == "windows" {
		binaryName = "uv.exe"
	}

	uvBinaryPath := filepath.Join(bruinHomeDirAbsPath, binaryName)
	if _, err := os.Stat(uvBinaryPath); errors.Is(err, os.ErrNotExist) {
		err = u.installUvCommand(ctx, bruinHomeDirAbsPath)
		if err != nil {
			return "", err
		}
		return uvBinaryPath, nil
	}

	var uvVersion struct {
		Version string `json:"version"`
	}
	cmd := exec.CommandContext(ctx, uvBinaryPath, "self", "version", "--output-format", "json")
	output, err := cmd.CombinedOutput()
	if err == nil {
		_ = json.Unmarshal(output, &uvVersion)
	}

	if uvVersion.Version != Version {
		err = u.installUvCommand(ctx, bruinHomeDirAbsPath)
		if err != nil {
			return "", err
		}
		return uvBinaryPath, nil
	}

	return uvBinaryPath, nil
}

func (u *Checker) installUvCommand(ctx context.Context, dest string) error {
	var output io.Writer = os.Stdout
	if ctx.Value(executor.KeyPrinter) != nil {
		output = ctx.Value(executor.KeyPrinter).(io.Writer)
	}

	_, _ = output.Write([]byte("===============================\n"))
	_, _ = fmt.Fprintf(output, "Installing uv v%s...\n", Version)
	_, _ = output.Write([]byte("This is a one-time operation.\n"))
	_, _ = output.Write([]byte("\n"))

	var commandInstance *exec.Cmd
	if runtime.GOOS == "windows" {
		shell := windowsShell()
		commandInstance = exec.CommandContext(ctx, shell, "-ExecutionPolicy", "ByPass", "-c", fmt.Sprintf("irm https://astral.sh/uv/%s/install.ps1 | iex", Version)) //nolint:gosec
	} else {
		commandInstance = exec.CommandContext(ctx, Shell, ShellSubcommandFlag, fmt.Sprintf("set -e; curl -LsSf https://astral.sh/uv/%s/install.sh | sh", Version)) //nolint:gosec
	}
	// Bruin manages this pinned binary; uv must not write shell profiles or
	// installation receipts elsewhere in the user's (possibly read-only) home.
	commandInstance.Env = append(os.Environ(), "UV_UNMANAGED_INSTALL="+dest)

	commandInstance.Stdout = output
	commandInstance.Stderr = output
	if err := commandInstance.Run(); err != nil {
		return fmt.Errorf("failed to install uv: %w", err)
	}

	_, _ = output.Write([]byte("\n"))
	_, _ = fmt.Fprintf(output, "Installed uv v%s, continuing...\n", Version)
	_, _ = output.Write([]byte("===============================\n"))
	_, _ = output.Write([]byte("\n"))

	return nil
}

// windowsShell returns "pwsh" if available, otherwise falls back to "powershell".
func windowsShell() string {
	if _, err := exec.LookPath("pwsh"); err == nil {
		return "pwsh"
	}
	return "powershell"
}
