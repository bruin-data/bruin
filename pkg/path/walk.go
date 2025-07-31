package path

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pkg/errors"
)

var SkipDirs = []string{".git", ".github", ".vscode", "node_modules", "dist", "build", "target", "vendor", ".venv", ".env", "env", "venv", "dbt_packages"}

// shouldExcludePath checks if a given path should be excluded based on the exclude patterns
func shouldExcludePath(path string, excludePaths []string) bool {
	if len(excludePaths) == 0 {
		return false
	}

	// Convert path to absolute path for consistent comparison
	absPath, err := filepath.Abs(path)
	if err != nil {
		// If we can't get absolute path, fall back to original path
		absPath = path
	}

	for _, excludePattern := range excludePaths {
		// Convert exclude pattern to absolute path
		absExcludePattern, err := filepath.Abs(excludePattern)
		if err != nil {
			// If we can't get absolute path, use original pattern
			absExcludePattern = excludePattern
		}

		// Check if the path starts with the exclude pattern (prefix match)
		// This allows excluding entire directory trees
		if strings.HasPrefix(absPath, absExcludePattern) {
			// Ensure we're matching complete path components to avoid false positives
			// e.g., "/foo/bar" should not match "/foo/barbaz"
			if absPath == absExcludePattern || strings.HasPrefix(absPath, absExcludePattern+string(filepath.Separator)) {
				return true
			}
		}

		// Also check relative path matching for convenience
		if strings.HasPrefix(path, excludePattern) {
			if path == excludePattern || strings.HasPrefix(path, excludePattern+string(filepath.Separator)) {
				return true
			}
		}
	}

	return false
}

func GetPipelinePaths(root string, pipelineDefinitionFile []string) ([]string, error) {
	return GetPipelinePathsWithExclusions(root, pipelineDefinitionFile, nil)
}

func GetPipelinePathsWithExclusions(root string, pipelineDefinitionFile []string, excludePaths []string) ([]string, error) {
	pipelinePaths := make([]string, 0)

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if slices.Contains(SkipDirs, d.Name()) {
				return filepath.SkipDir
			}

			// Check if this directory path should be excluded
			if shouldExcludePath(path, excludePaths) {
				return filepath.SkipDir
			}

			return nil
		}

		// Check if the parent directory should be excluded before processing files
		if shouldExcludePath(filepath.Dir(path), excludePaths) {
			return nil
		}

		// Check if the file matches any of the pipeline definition file names
		for _, pipelineDefinition := range pipelineDefinitionFile {
			if strings.HasSuffix(path, pipelineDefinition) {
				abs, err := filepath.Abs(path)
				if err != nil {
					return errors.Wrapf(err, "failed to get absolute path for %s", path)
				}

				pipelineDir := filepath.Dir(abs)
				// Double-check that the pipeline directory itself isn't excluded
				if !shouldExcludePath(pipelineDir, excludePaths) {
					pipelinePaths = append(pipelinePaths, pipelineDir)
				}
				break // Stop checking other file names if one matches
			}
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error walking directory")
	}

	return pipelinePaths, nil
}

func GetPipelineRootFromTask(taskPath string, pipelineDefinitionFile []string) (string, error) {
	absoluteTaskPath, err := filepath.Abs(taskPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to convert task path to absolute path")
	}

	currentFolder := absoluteTaskPath
	rootPath := filepath.VolumeName(currentFolder) + string(os.PathSeparator)
	for currentFolder != rootPath && currentFolder != "/" {
		for _, pipelineDefinition := range pipelineDefinitionFile {
			tryPath := filepath.Join(currentFolder, pipelineDefinition)
			if _, err := os.Stat(tryPath); err == nil {
				return currentFolder, nil
			}
		}

		currentFolder = filepath.Dir(currentFolder)
	}

	return "", errors.New("cannot find a pipeline the given task belongs to. Are you sure this task is in an actual pipeline?")
}

func GetAllFilesRecursive(root string, suffixes []string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if slices.Contains(SkipDirs, d.Name()) {
				return filepath.SkipDir
			}

			return nil
		}

		abs, err := filepath.Abs(path)
		if err != nil {
			return errors.Wrapf(err, "failed to get absolute path for %s", path)
		}

		for _, s := range suffixes {
			if strings.HasSuffix(abs, s) {
				paths = append(paths, abs)
			}
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error walking directory")
	}

	return paths, nil
}

func GetAllPossibleAssetPaths(pipelinePath string, assetsDirectoryNames []string, supportedFileSuffixes []string) []string {
	taskFiles := make([]string, 0)

	files, err := GetAllFilesRecursive(pipelinePath, supportedFileSuffixes)
	if err != nil {
		return taskFiles
	}

	for _, file := range files {
		for _, dir := range assetsDirectoryNames {
			if strings.Contains(file, "/"+dir+"/") {
				taskFiles = append(taskFiles, file)
				break
			}
		}
	}

	return taskFiles
}
