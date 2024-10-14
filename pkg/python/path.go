package python

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

type NoRequirementsFoundError struct{}

func (m *NoRequirementsFoundError) Error() string {
	return "no requirements.txt file found for the given module"
}

type ModulePathFinder struct {
	PathSeparatorOverride int32
}

func (m *ModulePathFinder) FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error) {
	// Normalize paths by replacing OS-specific separators with a slash
	if !strings.HasPrefix(filepath.Clean(strings.ToLower(executable.Path)), strings.ToLower(repo.Path)) {
		return "", errors.New("executable is not in the repository")
	}

	relativePath, err := filepath.Rel(repo.Path, filepath.Clean(executable.Path))
	if err != nil {
		return "", err
	}

	moduleName := strings.ReplaceAll(relativePath, string(os.PathSeparator), ".")
	moduleName = strings.TrimSuffix(moduleName, ".py")
	return moduleName, nil
}

func (m *ModulePathFinder) FindRequirementsTxtInPath(path string, executable *pipeline.ExecutableFile) (string, error) {
	executablePath := filepath.Clean(executable.Path)
	if !strings.HasPrefix(executablePath, path) {
		return "", errors.New("executable is not in the repository to find the requirements")
	}

	requirementsTxt := findFileUntilParent("requirements.txt", filepath.Dir(executablePath), path)
	if requirementsTxt == "" {
		return "", &NoRequirementsFoundError{}
	}

	return requirementsTxt, nil
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

		startDir = filepath.Dir(startDir)
	}

	return ""
}
