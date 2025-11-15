package r

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
)

type RenvInstaller struct {
	cmd cmd
}

// EnsureRenvExists checks if renv dependencies are installed and installs them if necessary.
func (r *RenvInstaller) EnsureRenvExists(ctx context.Context, repo *git.Repo, renvLockPath string) error {
	if renvLockPath == "" {
		return nil
	}

	// Get the directory containing the renv.lock file
	renvDir := filepath.Dir(renvLockPath)

	// Check if renv library directory exists
	renvLibDir := filepath.Join(renvDir, "renv", "library")
	if _, err := os.Stat(renvLibDir); err == nil {
		// renv library exists, check if it's up to date by comparing lock file hash
		if r.isRenvUpToDate(renvLockPath, renvLibDir) {
			log(ctx, "renv dependencies are up to date, skipping installation")
			return nil
		}
	}

	log(ctx, "Installing renv dependencies...")

	// Create an R script to initialize and restore renv
	restoreScript := fmt.Sprintf(`
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv", repos = "https://cloud.r-project.org")
}
setwd("%s")
renv::restore(prompt = FALSE)
`, strings.ReplaceAll(renvDir, "\\", "/"))

	// Write the script to a temporary file
	tmpFile, err := os.CreateTemp("", "renv_restore_*.R")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary R script")
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(restoreScript); err != nil {
		return errors.Wrap(err, "failed to write R script")
	}
	tmpFile.Close()

	// Execute the restore script
	pathToRscript, err := findPathToExecutable([]string{"Rscript"})
	if err != nil {
		return errors.Wrap(err, "Rscript not found")
	}

	err = r.cmd.Run(ctx, repo, &CommandInstance{
		Name:    pathToRscript,
		Args:    []string{tmpFile.Name()},
		EnvVars: map[string]string{},
	})
	if err != nil {
		return errors.Wrap(err, "failed to restore renv dependencies")
	}

	// Save the lock file hash for future checks
	r.saveLockFileHash(renvLockPath, renvLibDir)

	log(ctx, "renv dependencies installed successfully")
	return nil
}

// isRenvUpToDate checks if the renv library is synchronized with the lock file.
func (r *RenvInstaller) isRenvUpToDate(renvLockPath, renvLibDir string) bool {
	// Calculate current lock file hash
	currentHash, err := calculateFileHash(renvLockPath)
	if err != nil {
		return false
	}

	// Read stored hash
	hashFile := filepath.Join(renvLibDir, ".bruin_lock_hash")
	storedHash, err := os.ReadFile(hashFile)
	if err != nil {
		return false
	}

	return string(storedHash) == currentHash
}

// saveLockFileHash saves the hash of the lock file for future comparison.
func (r *RenvInstaller) saveLockFileHash(renvLockPath, renvLibDir string) {
	hash, err := calculateFileHash(renvLockPath)
	if err != nil {
		return
	}

	hashFile := filepath.Join(renvLibDir, ".bruin_lock_hash")
	_ = os.WriteFile(hashFile, []byte(hash), 0o600)
}

// calculateFileHash calculates SHA256 hash of a file.
func calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// GetRenvCachePath returns the path where renv cache should be stored.
func GetRenvCachePath() (string, error) {
	configManager := user.NewConfigManager(nil)
	bruinHome, err := configManager.EnsureAndGetBruinHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get bruin home directory")
	}

	return filepath.Join(bruinHome, "renv-cache"), nil
}
