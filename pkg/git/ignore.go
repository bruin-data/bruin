package git

import (
	"bufio"
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
		if err = afero.WriteFile(fs, gitignorePath, []byte(pattern+"\n"), 0o644); err != nil {
			return err
		}
		return nil
	}

	file, err := fs.OpenFile(gitignorePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer func(open afero.File) {
		tempErr := open.Close()
		if tempErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close file: %w", tempErr))
		}
	}(file)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == pattern {
			return nil
		}
	}

	// Start a new line only if the file doesn't end with one, and always end ours.
	info, err := file.Stat()
	if err != nil {
		return err
	}
	last := []byte{'\n'}
	if info.Size() > 0 {
		if _, err = file.ReadAt(last, info.Size()-1); err != nil {
			return err
		}
	}
	entry := pattern + "\n"
	if last[0] != '\n' {
		entry = "\n" + entry
	}
	_, err = file.Write([]byte(entry))
	return err
}
