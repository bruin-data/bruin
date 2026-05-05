package gong

import (
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
// It supports installing multiple versions side-by-side; concurrent installs of
// the same version are deduplicated, while different versions install in parallel.
type Checker struct {
	mu    sync.Mutex
	slots map[string]*installSlot
}

type installSlot struct {
	once sync.Once
	path string
	err  error
}

// EnsureGongInstalled checks if gong is installed and installs it if not present, then returns the full path of the binary.
// An empty version uses the bundled default from version.txt.
func (g *Checker) EnsureGongInstalled(ctx context.Context, version string) (string, error) {
	resolvedVersion := strings.TrimSpace(version)
	if resolvedVersion == "" {
		resolvedVersion = strings.TrimSpace(Version)
	}

	slot := g.slotFor(resolvedVersion)
	slot.once.Do(func() {
		slot.path, slot.err = g.ensureInstalled(ctx, resolvedVersion)
		if slot.err != nil {
			// Drop the slot so a subsequent caller retries the install.
			g.dropSlot(resolvedVersion, slot)
		}
	})
	return slot.path, slot.err
}

func (g *Checker) slotFor(version string) *installSlot {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.slots == nil {
		g.slots = make(map[string]*installSlot)
	}
	slot, ok := g.slots[version]
	if !ok {
		slot = &installSlot{}
		g.slots[version] = slot
	}
	return slot
}

func (g *Checker) dropSlot(version string, slot *installSlot) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if current, ok := g.slots[version]; ok && current == slot {
		delete(g.slots, version)
	}
}

func (g *Checker) binaryPath(version string) (string, error) {
	m := user.NewConfigManager(afero.NewOsFs())
	bruinHomeDirAbsPath, err := m.EnsureAndGetBruinHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get bruin home directory")
	}

	binDirPath := filepath.Join(bruinHomeDirAbsPath, binDir)
	if err := os.MkdirAll(binDirPath, filePermissions); err != nil {
		return "", errors.Wrap(err, "failed to create bin directory")
	}

	binaryName := "gong-" + version
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	return filepath.Join(binDirPath, binaryName), nil
}

func (g *Checker) ensureInstalled(ctx context.Context, version string) (string, error) {
	gongBinaryPath, err := g.binaryPath(version)
	if err != nil {
		return "", err
	}

	if _, err := os.Stat(gongBinaryPath); errors.Is(err, os.ErrNotExist) {
		if err := g.downloadGong(ctx, version, gongBinaryPath); err != nil {
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

	if installedVersion != version {
		if err := g.downloadGong(ctx, version, gongBinaryPath); err != nil {
			return "", err
		}
	}

	return gongBinaryPath, nil
}

// buildDownloadURL constructs the download URL for the gong binary based on OS and architecture.
func buildDownloadURL(version string) string {
	osName := getOSName()
	archName := getArchName()
	return fmt.Sprintf("%s/releases/%s/%s/gong_%s", BaseURL, version, osName, archName)
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

func (g *Checker) downloadGong(ctx context.Context, version, destPath string) error {
	var output io.Writer = os.Stdout
	if printer, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		output = printer
	}

	_, _ = fmt.Fprintf(output, "===============================\n")
	_, _ = fmt.Fprintf(output, "Installing gong %s...\n", version)
	_, _ = fmt.Fprintf(output, "This is a one-time operation.\n")
	_, _ = fmt.Fprintf(output, "\n")

	downloadURL := buildDownloadURL(version)
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
		return fmt.Errorf("failed to download gong %s: server returned status %d", version, resp.StatusCode)
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
	_, _ = fmt.Fprintf(output, "Installed gong %s, continuing...\n", version)
	_, _ = fmt.Fprintf(output, "===============================\n")
	_, _ = fmt.Fprintf(output, "\n")

	return nil
}
