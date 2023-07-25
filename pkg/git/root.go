package git

import (
	"os"
	"os/exec"
	"path"
	"strings"
)

// RepoFinder is a wrapper for finding the root path of a git repository.
type RepoFinder struct{}

// Repo represents the path of a given git repository.
type Repo struct {
	Path string
}

// Repo uses git by spawning a process to locate the top level directory.
func (*RepoFinder) Repo(path string) (*Repo, error) {
	res, err := rootPath(path)
	if err != nil {
		return nil, err
	}

	return &Repo{
		Path: res,
	}, nil
}

func rootPath(inputPath string) (string, error) {
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	command.Dir = inputPath
	if !isDirectory(inputPath) {
		command.Dir = path.Dir(inputPath)
	}

	res, err := command.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(res)), nil
}

// isDirectory determines if a file represented by `path` is a directory or not.
func isDirectory(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return fileInfo.IsDir()
}
