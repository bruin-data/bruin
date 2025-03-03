package git

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

var (
	rwLock         = sync.RWMutex{}
	knownRepoRoots = make(map[string]bool)
)

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
	for knownPath := range knownRepoRoots {
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

type errDubiousOwnership struct {
	Path string
}

func (e errDubiousOwnership) Error() string {
	return fmt.Sprintf("detected dubious ownership in repository at '%s'", e.Path)
}

var dubiousOwnershipPattern = regexp.MustCompile("fatal: detected dubious ownership in repository at '(.*)'")

func parseGitError(err error, stderr string) error {
	matches := dubiousOwnershipPattern.FindStringSubmatch(stderr)
	if len(matches) == 2 {
		return errDubiousOwnership{Path: matches[1]}
	}

	return fmt.Errorf("%w: %s", err, stderr)
}

func addSafeDirectory(path string) error {
	cmd := exec.Command(
		"git",
		"config",
		"--global",
		"--add",
		"safe.directory",
		path,
	)
	var stderr = new(bytes.Buffer)
	cmd.Stderr = stderr

	err := cmd.Run()
	if err != nil {
		return parseGitError(err, stderr.String())
	}
	return nil
}

func runGitRevParse(path string) (string, error) {
	var (
		stdout = new(bytes.Buffer)
		stderr = new(bytes.Buffer)
	)
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = path
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if err != nil {
		return "", parseGitError(err, stderr.String())
	}

	return strings.TrimSpace(stdout.String()), nil
}

func CurrentCommit(path string) (string, error) {
	hash, err := runGitRevParse(path)
	if err != nil {
		// git refuses to parse a repository when the calling
		// user is not the owner of the repository. This can happen
		// when the repository is cloned from a different user or container.
		// This can be fixed by adding the repository to the safe.directory
		var dubiousOwnership errDubiousOwnership
		if errors.As(err, &dubiousOwnership) {
			updateErr := addSafeDirectory(dubiousOwnership.Path)
			if updateErr != nil {
				return "", fmt.Errorf("failed to add repository to safe.directory: %w", updateErr)
			}
			return runGitRevParse(path)
		}
		return "", err
	}

	return hash, nil
}
