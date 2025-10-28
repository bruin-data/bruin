package r

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

type NoRenvFoundError struct{}

func (m *NoRenvFoundError) Error() string {
	return "no renv.lock file found for the given R script"
}

type RenvPathFinder struct{}

func (r *RenvPathFinder) FindRenvLockInPath(path string, executable *pipeline.ExecutableFile) (string, error) {
	lowerExecutable := filepath.Clean(strings.ToLower(executable.Path))
	if !strings.HasPrefix(lowerExecutable, strings.ToLower(path)) {
		return "", errors.New("executable is not in the repository to find the renv.lock")
	}

	executablePath := filepath.Clean(executable.Path)

	renvLock := findFileUntilParent("renv.lock", filepath.Dir(executablePath), path)
	if renvLock == "" {
		return "", &NoRenvFoundError{}
	}

	return renvLock, nil
}

func findFileUntilParent(file, startDir, stopDir string) string {
	stopDir = filepath.Clean(stopDir)
	for {
		potentialPath := filepath.Join(startDir, file)
		if _, err := os.Stat(potentialPath); err == nil {
			return potentialPath
		}

		if startDir == stopDir {
			break
		}

		if startDir == "/" {
			break
		}

		// On Windows, check for root directories like C:\
		if runtime := filepath.VolumeName(startDir); runtime != "" && startDir == runtime+string(filepath.Separator) {
			break
		}

		startDir = filepath.Dir(startDir)
	}

	return ""
}

// ValidateRScript checks if the given path is a valid R script.
func ValidateRScript(path string) error {
	if !strings.HasSuffix(strings.ToLower(path), ".r") {
		return fmt.Errorf("file %s is not an R script (must have .r extension)", path)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("r script %s does not exist", path)
	}

	return nil
}
