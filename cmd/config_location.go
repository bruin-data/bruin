package cmd

import (
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/spf13/afero"
)

const defaultConfigFileName = ".bruin.yml"

func validateDefaultConfigFileLocation(fs afero.Fs, repoRootPath, inputPath string) error {
	repoRootAbs, err := filepath.Abs(repoRootPath)
	if err != nil {
		return fmt.Errorf("failed to resolve repository root '%s': %w", repoRootPath, err)
	}

	inputAbs, err := filepath.Abs(inputPath)
	if err != nil {
		return fmt.Errorf("failed to resolve input path '%s': %w", inputPath, err)
	}

	// Make sure searching starts from a directory:
	// - if input is a directory, use it directly
	// - if input is a file or missing path, use its parent directory
	startPath := inputAbs
	info, statErr := fs.Stat(startPath)
	if statErr != nil || !info.IsDir() {
		startPath = filepath.Dir(startPath)
	}

	// Collect all default config files between the start location and repo root.
	foundConfigPaths, err := findConfigFilesBetween(fs, startPath, repoRootAbs, defaultConfigFileName)
	if err != nil {
		return err
	}

	if len(foundConfigPaths) == 0 {
		return nil
	}

	// A single config in repo root is the expected default layout.
	rootConfigPath := filepath.Join(repoRootAbs, defaultConfigFileName)
	rootConfigDisplay := displayPathFromRepoRoot(repoRootAbs, rootConfigPath)
	hasRootConfig := slices.Contains(foundConfigPaths, rootConfigPath)
	if len(foundConfigPaths) == 1 && hasRootConfig {
		return nil
	}

	if len(foundConfigPaths) == 1 && !hasRootConfig {
		foundPath := foundConfigPaths[0]
		foundDisplay := displayPathFromRepoRoot(repoRootAbs, foundPath)
		return fmt.Errorf(
			"could not find '%s' in repository root (looked for '%s'). found '%s' instead.\nmove it to '%s', or pass --config-file '%s'",
			defaultConfigFileName,
			rootConfigDisplay,
			foundDisplay,
			rootConfigDisplay,
			foundDisplay,
		)
	}

	slices.Sort(foundConfigPaths)
	foundDisplayPaths := make([]string, 0, len(foundConfigPaths))
	for _, foundPath := range foundConfigPaths {
		foundDisplayPaths = append(foundDisplayPaths, displayPathFromRepoRoot(repoRootAbs, foundPath))
	}

	// Multiple matches are ambiguous unless user picks one via --config-file.
	return fmt.Errorf(
		"found multiple '%s' files for this run:\n  - %s\nBruin expects a single '%s' at repository root '%s'.\nkeep only '%s' at root, move/remove the others, or pass --config-file to choose one explicitly",
		defaultConfigFileName,
		strings.Join(foundDisplayPaths, "\n  - "),
		defaultConfigFileName,
		rootConfigDisplay,
		rootConfigDisplay,
	)
}

func findConfigFilesBetween(fs afero.Fs, startPath, repoRootPath, configFileName string) ([]string, error) {
	currentPath := startPath
	foundConfigPaths := make([]string, 0, 2)

	for {
		// Check this directory first, then move one level up.
		configPath := filepath.Join(currentPath, configFileName)
		exists, err := afero.Exists(fs, configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to check config path '%s': %w", configPath, err)
		}
		if exists {
			foundConfigPaths = append(foundConfigPaths, configPath)
		}

		if samePath(currentPath, repoRootPath) {
			break
		}

		// Stop once we reach filesystem root (parent == self).
		parentPath := filepath.Dir(currentPath)
		if samePath(parentPath, currentPath) {
			break
		}
		currentPath = parentPath
	}

	return foundConfigPaths, nil
}

func samePath(a, b string) bool {
	a = filepath.Clean(a)
	b = filepath.Clean(b)

	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}

	return a == b
}

func displayPathFromRepoRoot(repoRootAbs, targetPath string) string {
	rel, err := filepath.Rel(repoRootAbs, targetPath)
	if err != nil {
		return targetPath
	}

	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return targetPath
	}

	// Use forward slashes for consistent and concise CLI output across platforms.
	rel = filepath.ToSlash(rel)

	if rel == defaultConfigFileName {
		return "./" + rel
	}

	return rel
}
