package python

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"sync"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type configManager interface {
	EnsureVirtualenvDirExists() error
	MakeVirtualenvPath(dir string) string
}

type installReqsToHomeDir struct {
	fs           afero.Fs
	config       configManager
	cmd          cmd
	pathToPython string

	lock sync.Mutex
}

func (i *installReqsToHomeDir) EnsureVirtualEnvExists(ctx context.Context, repo *git.Repo, requirementsTxt string) (string, error) {
	err := i.config.EnsureVirtualenvDirExists()
	if err != nil {
		return "", err
	}

	reqContent, err := afero.ReadFile(i.fs, requirementsTxt)
	if err != nil {
		return "", errors.Wrap(err, "failed to read requirements.txt")
	}

	if len(reqContent) == 0 {
		return "", nil
	}

	cleanContent := bytes.TrimSpace(reqContent)
	if len(cleanContent) == 0 {
		return "", nil
	}

	sum := sha256.Sum256(cleanContent)
	venvPath := i.config.MakeVirtualenvPath(hex.EncodeToString(sum[:]))

	i.lock.Lock()
	defer i.lock.Unlock()

	reqsPathExists := path.DirExists(i.fs, venvPath)
	if reqsPathExists {
		activateFileExists := path.FileExists(i.fs, fmt.Sprintf("%s/%s/activate", venvPath, VirtualEnvBinaryFolder))
		if activateFileExists {
			return venvPath, nil
		}
	}

	err = i.cmd.Run(ctx, repo, &CommandInstance{
		Name: i.pathToPython,
		Args: []string{"-m", "venv", venvPath},
	})
	if err != nil {
		return "", errors.Wrap(err, "failed to create virtualenv")
	}

	pipVenvPath := fmt.Sprintf("%s/%s/pip3", venvPath, VirtualEnvBinaryFolder)
	fullCommand := fmt.Sprintf("%s/%s/activate && %s install -r %s --quiet --quiet && echo 'installed all the dependencies'", venvPath, VirtualEnvBinaryFolder, pipVenvPath, requirementsTxt)
	if runtime.GOOS != WINDOWS {
		fullCommand = ". " + fullCommand
	}

	err = i.cmd.Run(ctx, repo, &CommandInstance{
		Name: Shell,
		Args: []string{ShellSubcommandFlag, fullCommand},
	})
	if err != nil {
		return "", errors.Wrap(err, "failed to install dependencies in the new isolated environment")
	}

	return venvPath, nil
}
