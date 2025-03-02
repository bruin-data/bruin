package git

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var rwLock = sync.RWMutex{}
var knownRepoRoots = make(map[string]bool)

// RepoFinder is a wrapper for finding the root path of a git repository.
type RepoFinder struct{}

// Repo represents the path of a given git repository.
type Repo struct {
	Path string `json:"path"`
}

// Repo uses git by spawning a process to locate the top level directory.
func (*RepoFinder) Repo(path string) (*Repo, error) {
	res, err := FindRepoFromPath(path)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// leaving this here temporarily in case we need to revert back to this version for some reason
//func FindRepoFromPath(path string) (*Repo, error) {
//	return (&RepoFinder{}).Repo(path)
//}

func FindRepoFromPath(path string) (*Repo, error) {
	rwLock.RLock()
	for knownPath, _ := range knownRepoRoots {
		if strings.HasPrefix(path, knownPath+"/") {
			rwLock.RUnlock()
			return &Repo{Path: knownPath}, nil
		}
	}
	rwLock.RUnlock()

	d, err := detectGitPath(path)
	if err != nil {
		return nil, err
	}

	if runtime.GOOS == "windows" {
		d = strings.Replace(d, "/", "\\", -1)
	}

	rwLock.Lock()
	knownRepoRoots[d] = true
	rwLock.Unlock()

	return &Repo{
		Path: d,
	}, nil
}

func detectGitPath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}

	for {
		gitPath := filepath.Join(path, ".git")
		fi, err := os.Stat(gitPath)
		if err == nil {
			if fi.IsDir() {
				return path, nil
			}

			return "", fmt.Errorf(".git exist but is not a directory")
		}

		parent := filepath.Dir(path)
		if parent == path {
			return "", fmt.Errorf("no git repository found")
		}
		path = parent
	}
}

func rootPath(inputPath string) (string, error) {
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	command.Dir = inputPath
	if !isDirectory(inputPath) {
		command.Dir = filepath.Dir(inputPath)
	}

	res, err := command.Output()
	if err != nil {
		return "", err
	}
	cleanPath := strings.TrimSpace(string(res))

	if runtime.GOOS == "windows" {
		cleanPath = strings.Replace(cleanPath, "/", "\\", -1)
	}

	return cleanPath, nil
}

// isDirectory determines if a file represented by `path` is a directory or not.
func isDirectory(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return fileInfo.IsDir()
}

func CurrentCommit(path string) (string, error) {
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = path
	res, err := command.Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(res)), nil
}
