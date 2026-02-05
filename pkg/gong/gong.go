package gong

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	Version = "v0.1.0"
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
	}

	return gongBinaryPath, nil
}

// BuildDownloadURL constructs the download URL for the gong binary based on OS and architecture.
func BuildDownloadURL() string {
	osName := getOSName()
	archName := getArchName()
	return fmt.Sprintf("%s/releases/%s/%s/gong_%s", BaseURL, Version, osName, archName)
}

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

	if printer, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		output = printer
	}

	_, _ = fmt.Fprintf(output, "===============================\n")
	_, _ = fmt.Fprintf(output, "Installing gong %s...\n", Version)
	_, _ = fmt.Fprintf(output, "This is a one-time operation.\n")
	_, _ = fmt.Fprintf(output, "\n")

	downloadURL := BuildDownloadURL()
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

	_, _ = fmt.Fprintf(output, "\n")
	_, _ = fmt.Fprintf(output, "Installed gong %s, continuing...\n", Version)
	_, _ = fmt.Fprintf(output, "===============================\n")
	_, _ = fmt.Fprintf(output, "\n")

	return nil
}
