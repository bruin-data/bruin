package gong

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	_ "embed"
	"fmt"
	"io"
	"net/http"
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
)

//go:embed version.txt
var Version string

const (
	BaseURL = "https://storage.googleapis.com/gong-release"

	binDir          = "bin"
	httpTimeout     = 5 * time.Minute
	filePermissions = 0o755
)

// Checker handles checking and installing the gong binary.
type Checker struct {
	mut sync.Mutex
}

// EnsureGongInstalled checks if gong is installed and installs it if not present, then returns the full path of the binary.
func (g *Checker) EnsureGongInstalled(ctx context.Context) (string, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	Version = strings.TrimSpace(Version)

	m := user.NewConfigManager(afero.NewOsFs())
	bruinHomeDirAbsPath, err := m.EnsureAndGetBruinHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get bruin home directory")
	}

	binDirPath := filepath.Join(bruinHomeDirAbsPath, binDir)
	if err := os.MkdirAll(binDirPath, filePermissions); err != nil {
		return "", errors.Wrap(err, "failed to create bin directory")
	}

	binaryName := "gong"
	if runtime.GOOS == "windows" {
		binaryName = "gong.exe"
	}

	gongBinaryPath := filepath.Join(binDirPath, binaryName)
	if _, err := os.Stat(gongBinaryPath); errors.Is(err, os.ErrNotExist) {
		err = g.downloadGong(ctx, gongBinaryPath)
		if err != nil {
			return "", err
		}
		return gongBinaryPath, nil
	}

	installedVersion := ""
	cmd := exec.CommandContext(ctx, gongBinaryPath, "--version")
	output, err := cmd.CombinedOutput()
	if err == nil {
		installedVersion = parseVersionOutput(strings.TrimSpace(string(output)))
	}

	if installedVersion != Version {
		err = g.downloadGong(ctx, gongBinaryPath)
		if err != nil {
			return "", err
		}
	}

	return gongBinaryPath, nil
}

// buildDownloadURL constructs the download URL for the gong binary based on OS and architecture.
func buildDownloadURL() string {
	osName := getOSName()
	archName := getArchName()
	return fmt.Sprintf("%s/releases/%s/%s/gong_%s", BaseURL, strings.TrimSpace(Version), osName, archName)
}

const ibmCliDriverBaseURL = "https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli"

// buildCliDriverDownloadURL returns the IBM public download URL for the DB2 clidriver
// based on the current OS and architecture.
func buildCliDriverDownloadURL() (string, error) {
	switch runtime.GOOS {
	case "linux":
		if runtime.GOARCH == "amd64" {
			return ibmCliDriverBaseURL + "/v11.5.9/linuxx64_odbc_cli.tar.gz", nil
		}
		return "", fmt.Errorf("DB2 clidriver is not available for linux/%s", runtime.GOARCH)
	case "darwin":
		switch runtime.GOARCH {
		case "arm64":
			return ibmCliDriverBaseURL + "/macarm64_odbc_cli.tar.gz", nil
		case "amd64":
			return ibmCliDriverBaseURL + "/macos64_odbc_cli.tar.gz", nil
		default:
			return "", fmt.Errorf("DB2 clidriver is not available for darwin/%s", runtime.GOARCH)
		}
	case "windows":
		if runtime.GOARCH == "amd64" {
			return ibmCliDriverBaseURL + "/ntx64_odbc_cli.zip", nil
		}
		return "", fmt.Errorf("DB2 clidriver is not available for windows/%s", runtime.GOARCH)
	default:
		return "", fmt.Errorf("DB2 clidriver is not available for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
}

// isDB2Supported returns true if the DB2 clidriver is available for the current platform.
// IBM does not provide a DB2 clidriver for Linux ARM64.
func isDB2Supported() bool {
	return !(runtime.GOOS == "linux" && runtime.GOARCH == "arm64")
}

// ErrDB2NotSupported is returned when DB2 is not supported on the current platform.
var ErrDB2NotSupported = fmt.Errorf("DB2 is not supported on Linux ARM64 — IBM does not provide a clidriver for this platform")

func getOSName() string {
	switch runtime.GOOS {
	case "darwin":
		return "darwin"
	case "linux":
		return "linux"
	case "windows":
		return "windows"
	default:
		return runtime.GOOS
	}
}

func getArchName() string {
	switch runtime.GOARCH {
	case "amd64":
		return "amd64"
	case "arm64":
		return "arm64"
	default:
		return runtime.GOARCH
	}
}

// parseVersionOutput extracts the version from "gong version X.Y.Z" output
// and normalizes it to "vX.Y.Z" to match the Version constant.
func parseVersionOutput(output string) string {
	// Expected format: "gong version X.Y.Z"
	parts := strings.Fields(output)
	if len(parts) < 3 {
		return output
	}

	ver := parts[len(parts)-1]
	if !strings.HasPrefix(ver, "v") {
		ver = "v" + ver
	}
	return ver
}

func (g *Checker) downloadGong(ctx context.Context, destPath string) error {
	var output io.Writer = os.Stdout
	if printer, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		output = printer
	}

	_, _ = fmt.Fprintf(output, "===============================\n")
	_, _ = fmt.Fprintf(output, "Installing gong %s...\n", Version)
	_, _ = fmt.Fprintf(output, "This is a one-time operation.\n")
	_, _ = fmt.Fprintf(output, "\n")

	downloadURL := buildDownloadURL()
	_, _ = fmt.Fprintf(output, "Downloading from %s\n", downloadURL)

	client := &http.Client{
		Timeout: httpTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create download request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download gong: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download gong: server returned status %d", resp.StatusCode)
	}

	// Create a temporary file in the same directory to ensure atomic write
	destDir := filepath.Dir(destPath)
	tmpFile, err := os.CreateTemp(destDir, "gong-download-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Clean up temp file on error
	defer func() {
		if tmpPath != "" {
			os.Remove(tmpPath)
		}
	}()

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write gong binary: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// Set executable permissions (on Windows this is a no-op effectively)
	if err := os.Chmod(tmpPath, filePermissions); err != nil {
		return fmt.Errorf("failed to set executable permissions: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("failed to move gong binary to destination: %w", err)
	}
	tmpPath = "" // Prevent cleanup of the renamed file

	// Remove macOS quarantine flag from the gong binary
	removeQuarantine(destPath)

	// Download and extract DB2 clidriver alongside the binary.
	// The gong binary is compiled with libdb2 linked, so it needs the library
	// at startup even for non-DB2 sources.
	if isDB2Supported() {
		destDir = filepath.Dir(destPath)
		if err := g.downloadAndExtractCliDriver(ctx, destDir, output); err != nil {
			_, _ = fmt.Fprintf(output, "Warning: failed to download DB2 clidriver: %v\n", err)
			_, _ = fmt.Fprintf(output, "DB2 sources may not work without the clidriver.\n")
		}
	} else {
		_, _ = fmt.Fprintf(output, "Skipping DB2 clidriver: not available for %s/%s\n", runtime.GOOS, runtime.GOARCH)
	}

	_, _ = fmt.Fprintf(output, "\n")
	_, _ = fmt.Fprintf(output, "Installed gong %s, continuing...\n", Version)
	_, _ = fmt.Fprintf(output, "===============================\n")
	_, _ = fmt.Fprintf(output, "\n")

	return nil
}

// CliDriverDir is the name of the clidriver directory extracted next to the gong binary.
const CliDriverDir = "clidriver"

// EnsureDB2CliDriverInstalled checks if the DB2 clidriver is present next to the gong binary and downloads it if missing.
// This should only be called when the user is running a DB2 source.
func (g *Checker) EnsureDB2CliDriverInstalled(ctx context.Context, gongBinaryPath string) (string, error) {
	if !isDB2Supported() {
		return "", ErrDB2NotSupported
	}

	g.mut.Lock()
	defer g.mut.Unlock()

	Version = strings.TrimSpace(Version)

	binDirPath := filepath.Dir(gongBinaryPath)
	cliDriverPath := filepath.Join(binDirPath, CliDriverDir)

	// Already exists — return the path
	if info, err := os.Stat(cliDriverPath); err == nil && info.IsDir() {
		return cliDriverPath, nil
	}

	var output io.Writer = os.Stdout
	if printer, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		output = printer
	}

	if err := g.downloadAndExtractCliDriver(ctx, binDirPath, output); err != nil {
		return "", err
	}

	// Remove macOS quarantine from the entire clidriver directory
	removeQuarantineRecursive(cliDriverPath)

	return cliDriverPath, nil
}

func (g *Checker) downloadAndExtractCliDriver(ctx context.Context, destDir string, output io.Writer) error {
	downloadURL, err := buildCliDriverDownloadURL()
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(output, "Downloading DB2 clidriver from %s\n", downloadURL)

	client := &http.Client{
		Timeout: httpTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create download request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download DB2 clidriver: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DB2 clidriver not available: server returned status %d", resp.StatusCode)
	}

	if strings.HasSuffix(downloadURL, ".zip") {
		err = extractZip(resp.Body, destDir)
	} else {
		err = extractTarGz(resp.Body, destDir)
	}
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(output, "DB2 clidriver installed successfully\n")
	return nil
}

func extractTarGz(r io.Reader, destDir string) error {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar entry: %w", err)
		}

		cleanName := filepath.Clean(header.Name)
		if strings.Contains(cleanName, "..") {
			continue
		}

		target := filepath.Join(destDir, cleanName)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, filePermissions); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", target, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), filePermissions); err != nil {
				return fmt.Errorf("failed to create parent directory for %s: %w", target, err)
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode)) //nolint:gosec
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil { //nolint:gosec
				f.Close()
				return fmt.Errorf("failed to write file %s: %w", target, err)
			}
			f.Close()
		}
	}
	return nil
}

func extractZip(r io.Reader, destDir string) error {
	// zip requires random access, so we need to save to a temp file first
	tmpFile, err := os.CreateTemp("", "clidriver-*.zip")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := io.Copy(tmpFile, r); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write zip to temp file: %w", err)
	}
	tmpFile.Close()

	zr, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return fmt.Errorf("failed to open zip: %w", err)
	}
	defer zr.Close()

	for _, f := range zr.File {
		cleanName := filepath.Clean(f.Name)
		if strings.Contains(cleanName, "..") {
			continue
		}

		target := filepath.Join(destDir, cleanName)

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, filePermissions); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", target, err)
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), filePermissions); err != nil {
			return fmt.Errorf("failed to create parent directory for %s: %w", target, err)
		}

		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("failed to open zip entry %s: %w", f.Name, err)
		}

		outFile, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, f.Mode()) //nolint:gosec
		if err != nil {
			rc.Close()
			return fmt.Errorf("failed to create file %s: %w", target, err)
		}

		if _, err := io.Copy(outFile, rc); err != nil { //nolint:gosec
			outFile.Close()
			rc.Close()
			return fmt.Errorf("failed to write file %s: %w", target, err)
		}
		outFile.Close()
		rc.Close()
	}
	return nil
}

// removeQuarantineRecursive removes macOS quarantine from all files in a directory.
func removeQuarantineRecursive(path string) {
	if runtime.GOOS != "darwin" {
		return
	}
	_ = exec.Command("xattr", "-r", "-d", "com.apple.quarantine", path).Run() //nolint:gosec
}

// removeQuarantine removes the macOS quarantine extended attribute from a file.
func removeQuarantine(path string) {
	if runtime.GOOS != "darwin" {
		return
	}
	// Best-effort: ignore errors since xattr may not be available or the attribute may not exist.
	_ = exec.Command("xattr", "-d", "com.apple.quarantine", path).Run() //nolint:gosec
}
