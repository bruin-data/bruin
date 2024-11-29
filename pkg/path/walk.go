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

func GetPipelinePaths(root string, pipelineDefinitionFiles []string) ([]string, error) {
	var pipelinePaths []string

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

		// Check if the file matches any of the pipeline definition file names
		for _, pipelineDefinitionFile := range pipelineDefinitionFiles {
			if strings.HasSuffix(path, pipelineDefinitionFile) {
				abs, err := filepath.Abs(path)
				if err != nil {
					return errors.Wrapf(err, "failed to get absolute path for %s", path)
				}

				pipelinePaths = append(pipelinePaths, filepath.Dir(abs))
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

func GetPipelineRootFromTask(taskPath string, pipelineDefinitionFiles []string) (string, error) {
	absoluteTaskPath, err := filepath.Abs(taskPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to convert task path to absolute path")
	}

	currentFolder := absoluteTaskPath
	rootPath := filepath.VolumeName(currentFolder) + string(os.PathSeparator)
	for currentFolder != rootPath && currentFolder != "/" {
		for _, pipelineDefinitionFile := range pipelineDefinitionFiles {
			tryPath := filepath.Join(currentFolder, pipelineDefinitionFile)
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
