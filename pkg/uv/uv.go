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
	Version = "0.9.13"

	// Shell is the shell to use for running commands.
	Shell = "/bin/sh"
	// ShellSubcommandFlag is the flag to pass to the shell to run a command.
	ShellSubcommandFlag = "-c"
)

// Checker handles checking and installing the uv package manager.
type Checker struct {
	mut sync.Mutex
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

	cmd := exec.Command(uvBinaryPath, "self", "version", "--output-format", "json")
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
	_, _ = output.Write([]byte(fmt.Sprintf("Installing uv v%s...\n", Version)))
	_, _ = output.Write([]byte("This is a one-time operation.\n"))
	_, _ = output.Write([]byte("\n"))

	var commandInstance *exec.Cmd
	if runtime.GOOS == "windows" {
		commandInstance = exec.Command("powershell", "-ExecutionPolicy", "ByPass", "-c", fmt.Sprintf("$env:NO_MODIFY_PATH=1 ; $env:UV_INSTALL_DIR='~/.bruin' ; irm https://astral.sh/uv/%s/install.ps1 | iex", Version)) //nolint:gosec
	} else {
		commandInstance = exec.Command(Shell, ShellSubcommandFlag, fmt.Sprintf("set -e; curl -LsSf https://astral.sh/uv/%s/install.sh | UV_INSTALL_DIR=\"%s\" NO_MODIFY_PATH=1 sh", Version, dest)) //nolint:gosec
	}

	commandInstance.Stdout = output
	commandInstance.Stderr = output
	if err := commandInstance.Run(); err != nil {
		return fmt.Errorf("failed to install uv: %w", err)
	}

	_, _ = output.Write([]byte("\n"))
	_, _ = output.Write([]byte(fmt.Sprintf("Installed uv v%s, continuing...\n", Version)))
	_, _ = output.Write([]byte("===============================\n"))
	_, _ = output.Write([]byte("\n"))

	return nil
}
