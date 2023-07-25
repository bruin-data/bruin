package path

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

func GetPipelinePaths(root, pipelineDefinitionFile string) ([]string, error) {
	var pipelinePaths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if !strings.HasSuffix(path, pipelineDefinitionFile) {
			return nil
		}

		abs, err := filepath.Abs(path)
		if err != nil {
			return errors.Wrapf(err, "failed to get absolute path for %s", path)
		}

		pipelinePaths = append(pipelinePaths, filepath.Dir(abs))

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error walking directory")
	}

	return pipelinePaths, nil
}

func GetPipelineRootFromTask(taskPath, pipelineDefinitionFile string) (string, error) {
	absoluteTaskPath, err := filepath.Abs(taskPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to convert task path to absolute path")
	}

	currentFolder := filepath.Dir(absoluteTaskPath)
	for currentFolder != "/" {
		tryPath := filepath.Join(currentFolder, pipelineDefinitionFile)
		if _, err := os.Stat(tryPath); err == nil {
			return currentFolder, nil
		}

		currentFolder = filepath.Dir(currentFolder)
	}

	return "", errors.New("cannot find a pipeline the given task belongs to, are you sure this task is in an actual pipeline?")
}

func GetAllFilesRecursive(root string, suffixes []string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
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
