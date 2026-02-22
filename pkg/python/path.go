package python

import (
	"fmt"
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

type NoPyprojectFoundError struct{}

func (m *NoPyprojectFoundError) Error() string {
	return "no pyproject.toml file found for the given module"
}

// DependencyType represents the type of dependency configuration found.
type DependencyType int

const (
	DependencyTypeNone DependencyType = iota
	DependencyTypeRequirementsTxt
	DependencyTypePyproject
)

// DependencyConfig holds the discovered dependency configuration for a Python asset.
type DependencyConfig struct {
	Type            DependencyType
	PyprojectPath   string // Path to pyproject.toml (empty if not found)
	RequirementsTxt string // Path to requirements.txt (empty if not found)
	ProjectRoot     string // Directory containing the dependency file
}

type ModulePathFinder struct {
	PathSeparatorOverride int32
}

func (m *ModulePathFinder) FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error) {
	// Normalize paths by replacing OS-specific separators with a slash
	if !strings.HasPrefix(filepath.Clean(strings.ToLower(executable.Path)), strings.ToLower(repo.Path)) {
		return "", errors.New(fmt.Sprintf("executable (%s) is not in the repository (%s)", executable.Path, repo.Path))
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
	lowerExecutable := filepath.Clean(strings.ToLower(executable.Path))
	if !strings.HasPrefix(lowerExecutable, strings.ToLower(path)) {
		return "", errors.New("executable is not in the repository to find the requirements")
	}

	executablePath := filepath.Clean(executable.Path)

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

// FindPyprojectTomlInPath searches for pyproject.toml starting from the executable's
// directory and walking up the directory tree until reaching the repository root.
func (m *ModulePathFinder) FindPyprojectTomlInPath(path string, executable *pipeline.ExecutableFile) (string, error) {
	lowerExecutable := filepath.Clean(strings.ToLower(executable.Path))
	if !strings.HasPrefix(lowerExecutable, strings.ToLower(path)) {
		return "", errors.New("executable is not in the repository to find pyproject.toml")
	}

	executablePath := filepath.Clean(executable.Path)

	pyprojectToml := findFileUntilParent("pyproject.toml", filepath.Dir(executablePath), path)
	if pyprojectToml == "" {
		return "", &NoPyprojectFoundError{}
	}

	return pyprojectToml, nil
}

// FindDependencyConfig searches for dependency configuration files and returns
// the best option based on priority: requirements.txt > pyproject.toml.
// This ensures backward compatibility while supporting modern uv-based workflows.
// When pyproject.toml is found, UV will automatically handle lockfile detection.
func (m *ModulePathFinder) FindDependencyConfig(path string, executable *pipeline.ExecutableFile) (*DependencyConfig, error) {
	lowerExecutable := filepath.Clean(strings.ToLower(executable.Path))
	if !strings.HasPrefix(lowerExecutable, strings.ToLower(path)) {
		return nil, errors.New("executable is not in the repository to find dependency config")
	}

	config := &DependencyConfig{
		Type: DependencyTypeNone,
	}

	// Priority 1: Check for requirements.txt first (backward compatibility)
	requirementsTxt, err := m.FindRequirementsTxtInPath(path, executable)
	if err == nil && requirementsTxt != "" {
		config.Type = DependencyTypeRequirementsTxt
		config.RequirementsTxt = requirementsTxt
		config.ProjectRoot = filepath.Dir(requirementsTxt)
		return config, nil
	}

	// Priority 2: Check for pyproject.toml
	// UV will automatically detect and use uv.lock if present
	pyprojectToml, err := m.FindPyprojectTomlInPath(path, executable)
	if err == nil && pyprojectToml != "" {
		config.Type = DependencyTypePyproject
		config.PyprojectPath = pyprojectToml
		config.ProjectRoot = filepath.Dir(pyprojectToml)
		return config, nil
	}

	// No dependency configuration found
	return config, nil
}
