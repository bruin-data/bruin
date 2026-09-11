package python

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
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
	ingestrReleaseDownloadURL = "https://github.com/bruin-data/ingestr/releases/download"
	ingestrScriptDownloadURL  = "https://raw.githubusercontent.com/bruin-data/ingestr"
	maxIngestrBinarySize      = 512 * 1024 * 1024
	maxIngestrScriptSize      = 1024 * 1024
)

// ingestrInstallMu prevents concurrent installations within the same Bruin
// process. Separate processes are safe because each download goes to a temporary
// directory and the completed binary is moved into place atomically.
var ingestrInstallMu sync.Mutex

//go:embed ingestr_hashes.json
var ingestrHashesJSON []byte

type ingestrInstallFunc func(ctx context.Context, output io.Writer, installDir, version string) error

type ingestrReleaseHashes struct {
	Archives        map[string]string `json:"archives"`
	InstallerCommit string            `json:"installer_commit"`
	InstallerSHA256 string            `json:"installer_sha256"`
}

type ingestrInstallerRuntime struct {
	goos               string
	goarch             string
	hashes             map[string]ingestrReleaseHashes
	httpClient         *http.Client
	releaseDownloadURL string
	scriptDownloadURL  string
	findShell          func(string) (string, error)
	runScript          func(context.Context, io.Writer, string, []byte, []string) error
}

// IngestrChecker installs and locates standalone ingestr releases.
type IngestrChecker struct {
	install ingestrInstallFunc
}

// EnsureIngestrInstalled returns the path to an exact ingestr release, installing
// it from an archive verified against embedded hashes when not already present.
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
	var hashes map[string]ingestrReleaseHashes
	if err := json.Unmarshal(ingestrHashesJSON, &hashes); err != nil {
		return errors.Wrap(err, "invalid embedded ingestr hashes")
	}
	installer := ingestrInstallerRuntime{
		goos:               runtime.GOOS,
		goarch:             runtime.GOARCH,
		hashes:             hashes,
		httpClient:         &http.Client{Timeout: 5 * time.Minute},
		releaseDownloadURL: ingestrReleaseDownloadURL,
		scriptDownloadURL:  ingestrScriptDownloadURL,
		findShell:          exec.LookPath,
		runScript:          runVerifiedIngestrScript,
	}
	return installer.install(ctx, output, installDir, version)
}

func (r ingestrInstallerRuntime) install(ctx context.Context, output io.Writer, installDir, version string) error {
	archiveName, err := ingestrArchiveName(r.goos, r.goarch)
	if err != nil {
		return err
	}
	release := r.hashes[version]
	expected := release.Archives[archiveName]
	if len(expected) != sha256.Size*2 {
		return fmt.Errorf("no trusted embedded ingestr hash for v%s %s/%s; upgrade Bruin or use its pinned ingestr version", version, r.goos, r.goarch)
	}

	shell, err := r.findShell("sh")
	if err != nil {
		if r.goos == "windows" {
			return r.installWindowsRelease(ctx, output, installDir, version, archiveName, expected)
		}
		return errors.Wrap(err, "the ingestr installer requires sh")
	}
	if len(release.InstallerCommit) != 40 || len(release.InstallerSHA256) != sha256.Size*2 {
		return fmt.Errorf("no trusted embedded ingestr installer hash for v%s", version)
	}
	scriptURL := strings.TrimRight(r.scriptDownloadURL, "/") + "/" + url.PathEscape(release.InstallerCommit) + "/install.sh"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, scriptURL, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create ingestr installer request")
	}
	response, err := r.httpClient.Do(request)
	if err != nil {
		return errors.Wrap(err, "failed to download ingestr installer")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download ingestr installer: server returned %s", response.Status)
	}
	script, err := io.ReadAll(io.LimitReader(response.Body, maxIngestrScriptSize+1))
	if err != nil {
		return errors.Wrap(err, "failed to read ingestr installer")
	}
	if len(script) > maxIngestrScriptSize || fmt.Sprintf("%x", sha256.Sum256(script)) != release.InstallerSHA256 {
		return fmt.Errorf("ingestr installer SHA-256 verification failed")
	}
	if r.goos == "windows" {
		// Git Bash/MSYS accepts drive-letter paths in slash form.
		installDir = strings.ReplaceAll(installDir, `\`, "/")
	}
	args := []string{"-s", "--", "-b", installDir, "-s", expected, "v" + version}
	if err := r.runScript(ctx, output, shell, script, args); err != nil {
		return errors.Wrapf(err, "verified ingestr v%s installer failed", version)
	}
	return nil
}

func runVerifiedIngestrScript(ctx context.Context, output io.Writer, shell string, script []byte, args []string) error {
	cmd := exec.CommandContext(ctx, shell, args...) //nolint:gosec
	if runtime.GOOS == "windows" {
		configureCommandCancellation(cmd)
	} else {
		// Give the installer's TERM trap time to stop downloads and clean up.
		configureManagedCmd(cmd)
	}
	// Execute exactly the bytes verified in memory, never re-fetch the script.
	cmd.Stdin = bytes.NewReader(script)
	cmd.Stdout = output
	cmd.Stderr = output
	cmd.Env = append(os.Environ(), "SHELL=bruin-installer")
	return cmd.Run()
}

func (r ingestrInstallerRuntime) installWindowsRelease(ctx context.Context, output io.Writer, installDir, version, archiveName, expected string) error {
	downloadURL := strings.TrimRight(r.releaseDownloadURL, "/") + "/" + url.PathEscape("v"+version) + "/" + archiveName
	_, _ = fmt.Fprintf(output, "Downloading ingestr v%s for %s/%s...\n", version, r.goos, r.goarch)

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

	archiveFile, err := os.CreateTemp(installDir, ".ingestr-*")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary ingestr archive")
	}
	archivePath := archiveFile.Name()
	defer os.Remove(archivePath)

	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(archiveFile, hash), io.LimitReader(response.Body, maxIngestrBinarySize+1))
	closeErr := archiveFile.Close()
	if copyErr != nil {
		return errors.Wrap(copyErr, "failed to save ingestr release archive")
	}
	if closeErr != nil {
		return errors.Wrap(closeErr, "failed to close ingestr release archive")
	}
	if written > maxIngestrBinarySize || fmt.Sprintf("%x", hash.Sum(nil)) != expected {
		return fmt.Errorf("ingestr v%s %s archive SHA-256 verification failed", version, archiveName)
	}

	return extractWindowsIngestrArchive(archivePath, installDir)
}

func ingestrArchiveName(goos, goarch string) (string, error) {
	osName := map[string]string{"linux": "Linux", "darwin": "Darwin", "windows": "Windows"}[goos]
	archName := map[string]string{"amd64": "x86_64", "arm64": "arm64"}[goarch]
	if osName == "" || archName == "" || (goos == "windows" && goarch != "amd64") {
		return "", fmt.Errorf("ingestr standalone releases do not support %s/%s", goos, goarch)
	}
	ext := ".tar.gz"
	if goos == "windows" {
		ext = ".zip"
	}
	return "ingestr_" + osName + "_" + archName + ext, nil
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
