package git

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/spf13/afero"
)

func EnsureGivenPatternIsInGitignore(fs afero.Fs, repoRoot string, pattern string) (err error) {
	// Check if .gitignore file exists in the root of the repository
	gitignorePath := path.Join(repoRoot, ".gitignore")
	exists, err := afero.Exists(fs, gitignorePath)
	if err != nil {
		return err
	}

	if !exists {
		// Create a new .gitignore file if it doesn't exist
		if err = afero.WriteFile(fs, gitignorePath, []byte(pattern), 0o644); err != nil {
			return err
		}
		return nil
	}

	// Check for the pattern read-only first, so an already-ignored pattern never
	// needs write access to .gitignore (which may be read-only for this user).
	content, err := afero.ReadFile(fs, gitignorePath)
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(string(content), "\n") {
		if strings.TrimSpace(line) == pattern {
			return nil
		}
	}

	file, err := fs.OpenFile(gitignorePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func(open afero.File) {
		tempErr := open.Close()
		if tempErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close file: %w", tempErr))
		}
	}(file)

	_, err = file.Write([]byte("\n" + pattern))
	return err
}
