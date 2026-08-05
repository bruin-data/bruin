package python

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"golang.org/x/mod/semver"
)

const (
	ingestrInstallerURL          = "https://getbruin.com/install/ingestr"
	ingestrInstallerShellCommand = `curl -LsSf "$1" | sh -s -- -b "$2" "$3"`
)

// ingestrInstallMu prevents concurrent installations within the same Bruin
// process. Separate processes are safe because each download goes to a temporary
// directory and the completed binary is moved into place atomically.
var ingestrInstallMu sync.Mutex

type ingestrInstallFunc func(ctx context.Context, output io.Writer, installDir, version string) error

// IngestrChecker installs and locates standalone ingestr releases.
type IngestrChecker struct {
	install ingestrInstallFunc
}

// EnsureIngestrInstalled returns the path to an exact ingestr release, installing
// it with the official curl-based installer when it is not already present.
func (c *IngestrChecker) EnsureIngestrInstalled(ctx context.Context, version string) (string, error) {
	if !semver.IsValid("v" + version) {
		return "", fmt.Errorf("invalid ingestr version %q", version)
	}

	manager := user.NewConfigManager(afero.NewOsFs())
	bruinHomeDir, err := manager.EnsureAndGetBruinHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get bruin home directory")
	}

	var output io.Writer = os.Stdout
	if printer := ctx.Value(executor.KeyPrinter); printer != nil {
		output = printer.(io.Writer)
	}

	return c.ensureIngestrInstalled(ctx, bruinHomeDir, version, output)
}

func (c *IngestrChecker) ensureIngestrInstalled(
	ctx context.Context,
	bruinHomeDir string,
	version string,
	output io.Writer,
) (string, error) {
	ingestrInstallMu.Lock()
	defer ingestrInstallMu.Unlock()

	binaryName := ingestrBinaryName(runtime.GOOS)
	versionDir := filepath.Join(bruinHomeDir, "ingestr", version)
	binaryPath := filepath.Join(versionDir, binaryName)
	if isIngestrBinary(binaryPath) {
		return binaryPath, nil
	}

	installRoot := filepath.Join(bruinHomeDir, "ingestr")
	if err := os.MkdirAll(installRoot, 0o755); err != nil {
		return "", errors.Wrap(err, "failed to create ingestr installation directory")
	}

	tempDir, err := os.MkdirTemp(installRoot, ".install-"+version+"-")
	if err != nil {
		return "", errors.Wrap(err, "failed to create temporary ingestr installation directory")
	}
	defer os.RemoveAll(tempDir)

	_, _ = fmt.Fprintf(output, "Installing ingestr v%s...\n", version)
	install := c.install
	if install == nil {
		install = runIngestrInstaller
	}
	if err := install(ctx, output, tempDir, version); err != nil {
		return "", err
	}

	tempBinaryPath := filepath.Join(tempDir, binaryName)
	if !isIngestrBinary(tempBinaryPath) {
		return "", fmt.Errorf("ingestr installer did not produce %s", binaryName)
	}

	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		return "", errors.Wrap(err, "failed to create versioned ingestr installation directory")
	}
	if err := os.Rename(tempBinaryPath, binaryPath); err != nil {
		// Another Bruin process may have completed the same installation first.
		if isIngestrBinary(binaryPath) {
			return binaryPath, nil
		}
		return "", errors.Wrap(err, "failed to activate ingestr installation")
	}

	return binaryPath, nil
}

func isIngestrBinary(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func ingestrBinaryName(goos string) string {
	if goos == "windows" {
		return "ingestr.exe"
	}
	return "ingestr"
}

func runIngestrInstaller(ctx context.Context, output io.Writer, installDir, version string) error {
	shell, err := exec.LookPath("sh")
	if err != nil {
		return errors.Wrap(err, "the ingestr installer requires sh")
	}

	cmd := exec.CommandContext(ctx, shell, ingestrInstallerCommandArgs(installDir, version, runtime.GOOS)...) //nolint:gosec
	cmd.Env = environmentWithOverride(os.Environ(), "SHELL", "bruin-installer")
	cmd.Stdout = output
	cmd.Stderr = output

	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to install ingestr v%s", version)
	}
	return nil
}

func ingestrInstallerCommandArgs(installDir, version, goos string) []string {
	if goos == "windows" {
		// Git Bash/MSYS understands drive-letter paths in slash form.
		installDir = strings.ReplaceAll(installDir, `\`, "/")
	}
	return []string{
		"-c",
		ingestrInstallerShellCommand,
		"ingestr-installer",
		ingestrInstallerURL,
		installDir,
		"v" + version,
	}
}

func environmentWithOverride(environment []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if strings.HasPrefix(entry, prefix) {
			continue
		}
		result = append(result, entry)
	}
	return append(result, prefix+value)
}
