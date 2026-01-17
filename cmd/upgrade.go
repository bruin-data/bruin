package cmd

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/charmbracelet/lipgloss"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

const (
	binaryName     = "bruin"
	githubReleases = "https://github.com/bruin-data/bruin/releases"
	githubDownload = "https://github.com/bruin-data/bruin/releases/download"
)

var (
	// Colors and styles for the upgrade UI - coral/salmon to match bruin logo.
	coral     = lipgloss.NewStyle().Foreground(lipgloss.Color("#F96E5B"))
	green     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	bold      = lipgloss.NewStyle().Bold(true)
	faintText = lipgloss.NewStyle().Faint(true)

	// Box drawing characters.
	boxTopLeft    = "┌"
	boxBottomLeft = "└"
	boxVertical   = "│"
	bullet        = "●"
	diamond       = "◇"
)

// bruinBanner returns the ASCII art banner for bruin.
func bruinBanner() string {
	banner := `
  _               _
 | |__  _ __ _   _ _ _ __
 | '_ \| '__| | | | | '_ \
 | |_) | |  | |_| | | | | |
 |_.__/|_|   \__,_|_|_| |_|
`
	return coral.Render(banner)
}

// printHeader prints the styled header.
func printHeader(title string) {
	fmt.Println()
	fmt.Printf("%s  %s\n", boxTopLeft, bold.Render(title))
	fmt.Println(boxVertical)
}

// printStep prints a step with a bullet.
func printStep(message string) {
	fmt.Printf("%s  %s\n", coral.Render(bullet), message)
	fmt.Println(boxVertical)
}

// printSuccess prints a success step with a diamond.
func printSuccess(message string) {
	fmt.Printf("%s  %s\n", green.Render(diamond), green.Render(message))
	fmt.Println(boxVertical)
}

// printFooter prints the footer.
func printFooter(message string) {
	fmt.Printf("%s  %s\n", boxBottomLeft, message)
	fmt.Println()
}

// getDownloadMethod returns the method used for downloading (curl/wget/native).
func getDownloadMethod() string {
	return "native Go HTTP"
}

// getOSName returns the OS name formatted for download URL.
func getOSName() string {
	switch runtime.GOOS {
	case "darwin":
		return "Darwin"
	case "linux":
		return "Linux"
	case "windows":
		return "Windows"
	default:
		return runtime.GOOS
	}
}

// getArchName returns the architecture name formatted for download URL.
func getArchName() string {
	switch runtime.GOARCH {
	case "amd64":
		return "x86_64"
	case "arm64":
		return "arm64"
	case "386":
		return "i386"
	default:
		return runtime.GOARCH
	}
}

// getFormat returns the archive format based on OS.
func getFormat() string {
	if runtime.GOOS == "windows" {
		return "zip"
	}
	return "tar.gz"
}

// buildDownloadURL builds the download URL for the release.
func buildDownloadURL(version string) string {
	osName := getOSName()
	archName := getArchName()
	format := getFormat()
	tarball := fmt.Sprintf("%s_%s_%s.%s", binaryName, osName, archName, format)
	return fmt.Sprintf("%s/%s/%s", githubDownload, version, tarball)
}

// fetchLatestVersionForUpgrade fetches the latest version from GitHub.
func fetchLatestVersionForUpgrade(ctx context.Context, timeout time.Duration) (string, error) {
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubReleases+"/latest", nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to fetch latest version")
	}
	defer resp.Body.Close()

	// GitHub redirects /releases/latest to /releases/tag/<version>
	if resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently {
		location := resp.Header.Get("Location")
		// Extract version from redirect URL (e.g., .../releases/tag/v0.1.0)
		parts := strings.Split(location, "/tag/")
		if len(parts) == 2 {
			return parts[1], nil
		}
		return "", errors.New("unexpected redirect URL format: " + location)
	}

	return "", fmt.Errorf("unexpected response status: %s", resp.Status)
}

// progressWriter wraps an io.Writer to track download progress.
type progressWriter struct {
	total      int64
	downloaded int64
	lastPrint  time.Time
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n := len(p)
	pw.downloaded += int64(n)

	// Update progress every 100ms
	if time.Since(pw.lastPrint) > 100*time.Millisecond {
		pw.printProgress()
		pw.lastPrint = time.Now()
	}
	return n, nil
}

func (pw *progressWriter) printProgress() {
	if pw.total > 0 {
		pct := float64(pw.downloaded) / float64(pw.total) * 100
		downloaded := float64(pw.downloaded) / 1024 / 1024
		total := float64(pw.total) / 1024 / 1024
		fmt.Printf("\r%s  Downloading: %.1f MB / %.1f MB (%.0f%%)", boxVertical, downloaded, total, pct)
	} else {
		downloaded := float64(pw.downloaded) / 1024 / 1024
		fmt.Printf("\r%s  Downloading: %.1f MB", boxVertical, downloaded)
	}
}

func (pw *progressWriter) finish() {
	if pw.total > 0 {
		downloaded := float64(pw.downloaded) / 1024 / 1024
		fmt.Printf("\r%s  Downloaded: %.1f MB                    \n", boxVertical, downloaded)
	} else {
		downloaded := float64(pw.downloaded) / 1024 / 1024
		fmt.Printf("\r%s  Downloaded: %.1f MB\n", boxVertical, downloaded)
	}
}

// downloadFile downloads a file from URL to a local path with progress.
func downloadFile(ctx context.Context, url, destPath string, timeout time.Duration) error {
	client := &http.Client{
		Timeout: timeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to download file")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed with status: %s", resp.Status)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return errors.Wrap(err, "failed to create file")
	}
	defer out.Close()

	pw := &progressWriter{
		total:     resp.ContentLength,
		lastPrint: time.Now(),
	}

	_, err = io.Copy(out, io.TeeReader(resp.Body, pw))
	pw.finish()

	if err != nil {
		return errors.Wrap(err, "failed to write file")
	}

	return nil
}

// extractTarGz extracts a tar.gz archive.
func extractTarGz(archivePath, destDir string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	// Get the absolute path of destDir to check against
	absDestDir, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}

	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name) //nolint:gosec
		// Validate that target is within destDir (prevent path traversal).
		absTarget, err := filepath.Abs(target)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(absTarget, absDestDir+string(filepath.Separator)) && absTarget != absDestDir {
			return errors.New("path traversal detected in archive")
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil { //nolint:gosec
				f.Close()
				return err
			}
			f.Close()
		}
	}

	return nil
}

// extractZip extracts a zip archive.
func extractZip(archivePath, destDir string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	// Get the absolute path of destDir to check against
	absDestDir, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}

	for _, f := range r.File {
		target := filepath.Join(destDir, f.Name) //nolint:gosec
		// Validate that target is within destDir (prevent path traversal).
		absTarget, err := filepath.Abs(target)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(absTarget, absDestDir+string(filepath.Separator)) && absTarget != absDestDir {
			return errors.New("path traversal detected in archive")
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}

		outFile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc) //nolint:gosec
		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

// getBinaryInstallPath returns the path where the binary should be installed.
func getBinaryInstallPath() (string, error) {
	// Try to find the current binary location
	execPath, err := os.Executable()
	if err == nil {
		// Resolve symlinks
		realPath, err := filepath.EvalSymlinks(execPath)
		if err == nil {
			return filepath.Dir(realPath), nil
		}
		return filepath.Dir(execPath), nil
	}

	// Fall back to default location
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get home directory")
	}

	binDir := filepath.Join(homeDir, ".local", "bin")
	return binDir, nil
}

// installBinary installs the binary from extracted archive to the bin directory.
// Returns (deferred, error) where deferred=true means the upgrade will complete
// after the current process exits (used on Windows due to file locking).
func installBinary(srcDir, binDir string) (bool, error) {
	// Ensure bin directory exists
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return false, errors.Wrap(err, "failed to create bin directory")
	}

	srcBinary := filepath.Join(srcDir, binaryName)
	if runtime.GOOS == "windows" {
		srcBinary += ".exe"
	}

	dstBinary := filepath.Join(binDir, binaryName)
	if runtime.GOOS == "windows" {
		dstBinary += ".exe"
	}

	// Read source binary
	data, err := os.ReadFile(srcBinary)
	if err != nil {
		return false, errors.Wrap(err, "failed to read binary")
	}

	// On Windows, we cannot overwrite a running executable due to file locking.
	// Use a batch script to perform the replacement after this process exits.
	if runtime.GOOS == "windows" {
		return installBinaryWindows(data, dstBinary)
	}

	// Non-Windows: direct write
	if err := os.WriteFile(dstBinary, data, 0o755); err != nil { //nolint:gosec
		return false, errors.Wrap(err, "failed to write binary")
	}

	return false, nil
}

// installBinaryWindows handles Windows-specific binary installation.
// On Windows, we cannot overwrite a running executable, so we write the new
// binary to a temp file and use a batch script to replace it after exit.
func installBinaryWindows(data []byte, dstBinary string) (bool, error) {
	// Write new binary to temporary location next to destination
	tempBinary := dstBinary + ".new"
	if err := os.WriteFile(tempBinary, data, 0o755); err != nil { //nolint:gosec
		return false, errors.Wrap(err, "failed to write temp binary")
	}

	// Create a batch script that waits for this process to exit, then replaces the binary.
	// The script retries the move operation until it succeeds (when the lock is released).
	scriptContent := fmt.Sprintf(`@echo off
:retry
ping 127.0.0.1 -n 2 > nul
move /Y "%s" "%s" > nul 2>&1
if errorlevel 1 goto retry
del "%%~f0"
`, tempBinary, dstBinary)

	scriptPath := filepath.Join(os.TempDir(), fmt.Sprintf("bruin-upgrade-%d.bat", time.Now().UnixNano()))
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0o755); err != nil { //nolint:gosec
		os.Remove(tempBinary)
		return false, errors.Wrap(err, "failed to create upgrade script")
	}

	// Start the script in background using 'start /b'
	cmd := exec.Command("cmd", "/C", "start", "/b", "", scriptPath)
	if err := cmd.Start(); err != nil {
		os.Remove(tempBinary)
		os.Remove(scriptPath)
		return false, errors.Wrap(err, "failed to start upgrade process")
	}

	return true, nil
}

// Upgrade returns the upgrade command.
func Upgrade() *cli.Command {
	return &cli.Command{
		Name:      "upgrade",
		Aliases:   []string{"update"},
		Usage:     "Upgrade Bruin CLI to the latest version or a specific version",
		ArgsUsage: "[version]",
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:  "timeout",
				Usage: "timeout for fetching version info",
				Value: 30 * time.Second,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			timeout := c.Duration("timeout")
			currentVersion := c.Root().Version

			// Print banner
			fmt.Print(bruinBanner())

			// Print header
			printHeader("Upgrade")

			// Show download method
			printStep("Using method: " + bold.Render(getDownloadMethod()))

			// Check if a specific version was provided
			var targetVersion string
			if c.Args().Present() {
				targetVersion = c.Args().First()
				// Normalize version (add v prefix if missing)
				if !strings.HasPrefix(targetVersion, "v") {
					targetVersion = "v" + targetVersion
				}
			} else {
				// Fetch latest version
				latestVersion, err := fetchLatestVersionForUpgrade(ctx, timeout)
				if err != nil {
					return errors.Wrap(err, "failed to fetch latest version")
				}
				targetVersion = latestVersion
			}

			// Compare versions - string equality is sufficient here since we only
			// check if versions are identical (to skip re-downloading the same version).
			// We don't need semver comparison because we're not determining which is newer.
			currentClean := strings.TrimPrefix(currentVersion, "v")
			targetClean := strings.TrimPrefix(targetVersion, "v")

			if currentClean == targetClean { //nolint:govet
				printSuccess("Already at version: " + bold.Render(currentVersion))
				printFooter("Done")
				return nil
			}

			// Show version transition
			versionTransition := "From " + faintText.Render(currentVersion) + " " + coral.Render("→") + " " + green.Render(targetVersion)
			printStep(versionTransition)

			// Create temp directory
			tmpDir, err := os.MkdirTemp("", "bruin-upgrade-*")
			if err != nil {
				return errors.Wrap(err, "failed to create temp directory")
			}
			defer os.RemoveAll(tmpDir)

			// Build download URL
			downloadURL := buildDownloadURL(targetVersion)
			format := getFormat()
			archivePath := filepath.Join(tmpDir, "bruin."+format)

			// Download
			fmt.Printf("%s  Downloading from GitHub...\n", coral.Render(bullet))
			if err := downloadFile(ctx, downloadURL, archivePath, timeout); err != nil {
				return errors.Wrap(err, "failed to download release")
			}

			// Extract
			fmt.Printf("%s  Extracting archive...\n", boxVertical)
			if format == "zip" {
				if err := extractZip(archivePath, tmpDir); err != nil {
					return errors.Wrap(err, "failed to extract zip")
				}
			} else {
				if err := extractTarGz(archivePath, tmpDir); err != nil {
					return errors.Wrap(err, "failed to extract tarball")
				}
			}

			// Get install path
			binDir, err := getBinaryInstallPath()
			if err != nil {
				return errors.Wrap(err, "failed to determine install path")
			}

			// Install
			fmt.Printf("%s  Installing to %s...\n", boxVertical, faintText.Render(binDir))
			deferred, err := installBinary(tmpDir, binDir)
			if err != nil {
				return errors.Wrap(err, "failed to install binary")
			}

			fmt.Println(boxVertical)

			// Success message - on Windows the upgrade completes after exit
			if deferred {
				printSuccess("Upgrade prepared")
				printFooter("The upgrade will complete when this process exits")
			} else {
				printSuccess("Upgrade complete")
				printFooter("Done")
			}

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}
