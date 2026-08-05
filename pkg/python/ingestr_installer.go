package python

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"golang.org/x/mod/semver"
)

const (
	ingestrInstallerURL          = "https://getbruin.com/install/ingestr"
	ingestrInstallerShellCommand = `curl -LsSf "$1" | sh -s -- -b "$2" "$3"`
	ingestrReleaseDownloadURL    = "https://github.com/bruin-data/ingestr/releases/download"
	maxIngestrBinarySize         = 512 * 1024 * 1024
)

// ingestrInstallMu prevents concurrent installations within the same Bruin
// process. Separate processes are safe because each download goes to a temporary
// directory and the completed binary is moved into place atomically.
var ingestrInstallMu sync.Mutex

type ingestrInstallFunc func(ctx context.Context, output io.Writer, installDir, version string) error

type ingestrInstallerRuntime struct {
	goos               string
	goarch             string
	findShell          func(string) (string, error)
	runShell           func(context.Context, io.Writer, string, []string) error
	httpClient         *http.Client
	releaseDownloadURL string
}

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
	installer := ingestrInstallerRuntime{
		goos:               runtime.GOOS,
		goarch:             runtime.GOARCH,
		findShell:          exec.LookPath,
		runShell:           runIngestrShellInstaller,
		httpClient:         &http.Client{Timeout: 5 * time.Minute},
		releaseDownloadURL: ingestrReleaseDownloadURL,
	}
	return installer.install(ctx, output, installDir, version)
}

func (r ingestrInstallerRuntime) install(ctx context.Context, output io.Writer, installDir, version string) error {
	shell, err := r.findShell("sh")
	if err != nil {
		if r.goos == "windows" {
			return r.installWindowsRelease(ctx, output, installDir, version)
		}
		return errors.Wrap(err, "the ingestr installer requires sh")
	}

	if err := r.runShell(ctx, output, shell, ingestrInstallerCommandArgs(installDir, version, r.goos)); err != nil {
		if r.goos == "windows" && ctx.Err() == nil {
			if fallbackErr := r.installWindowsRelease(ctx, output, installDir, version); fallbackErr != nil {
				return fmt.Errorf(
					"failed to install ingestr v%s with the shell installer: %w; native Windows fallback failed: %w",
					version,
					err,
					fallbackErr,
				)
			}
			return nil
		}
		return errors.Wrapf(err, "failed to install ingestr v%s", version)
	}
	return nil
}

func runIngestrShellInstaller(ctx context.Context, output io.Writer, shell string, args []string) error {
	cmd := exec.CommandContext(ctx, shell, args...) //nolint:gosec
	cmd.Env = environmentWithOverride(os.Environ(), "SHELL", "bruin-installer")
	cmd.Stdout = output
	cmd.Stderr = output
	return cmd.Run()
}

func (r ingestrInstallerRuntime) installWindowsRelease(
	ctx context.Context,
	output io.Writer,
	installDir string,
	version string,
) error {
	archiveName, err := windowsIngestrArchiveName(r.goarch)
	if err != nil {
		return err
	}

	downloadURL := strings.TrimRight(r.releaseDownloadURL, "/") + "/" + url.PathEscape("v"+version) + "/" + archiveName
	_, _ = fmt.Fprintf(output, "Downloading ingestr v%s for Windows...\n", version)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create ingestr download request")
	}
	response, err := r.httpClient.Do(request)
	if err != nil {
		return errors.Wrapf(err, "failed to download ingestr v%s", version)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("failed to download ingestr v%s: server returned %s", version, response.Status)
	}

	archiveFile, err := os.CreateTemp(installDir, ".ingestr-*.zip")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary ingestr archive")
	}
	archivePath := archiveFile.Name()
	defer os.Remove(archivePath)

	_, copyErr := io.Copy(archiveFile, response.Body)
	closeErr := archiveFile.Close()
	if copyErr != nil {
		return errors.Wrap(copyErr, "failed to save ingestr release archive")
	}
	if closeErr != nil {
		return errors.Wrap(closeErr, "failed to close ingestr release archive")
	}

	return extractWindowsIngestrArchive(archivePath, installDir)
}

func windowsIngestrArchiveName(goarch string) (string, error) {
	if goarch != "amd64" {
		return "", fmt.Errorf("ingestr standalone releases do not support windows/%s", goarch)
	}
	return "ingestr_Windows_x86_64.zip", nil
}

func extractWindowsIngestrArchive(archivePath, installDir string) error {
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		return errors.Wrap(err, "failed to open ingestr release archive")
	}
	defer archive.Close()

	const binaryName = "ingestr.exe"
	for _, file := range archive.File {
		if file.FileInfo().IsDir() || filepath.Base(filepath.ToSlash(file.Name)) != binaryName {
			continue
		}

		return extractFileFromZip(file, filepath.Join(installDir, binaryName))
	}

	return fmt.Errorf("ingestr release archive did not contain %s", binaryName)
}

func extractFileFromZip(source *zip.File, destination string) error {
	reader, err := source.Open()
	if err != nil {
		return errors.Wrap(err, "failed to open ingestr binary in release archive")
	}

	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		_ = reader.Close()
		return errors.Wrap(err, "failed to create ingestr binary")
	}

	written, copyErr := io.CopyN(file, reader, maxIngestrBinarySize+1)
	readerCloseErr := reader.Close()
	fileCloseErr := file.Close()
	if copyErr != nil && !errors.Is(copyErr, io.EOF) {
		return errors.Wrap(copyErr, "failed to extract ingestr binary")
	}
	if written > maxIngestrBinarySize {
		return fmt.Errorf("ingestr binary exceeds the %d-byte extraction limit", maxIngestrBinarySize)
	}
	if readerCloseErr != nil {
		return errors.Wrap(readerCloseErr, "failed to close ingestr binary in release archive")
	}
	if fileCloseErr != nil {
		return errors.Wrap(fileCloseErr, "failed to close ingestr binary")
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
