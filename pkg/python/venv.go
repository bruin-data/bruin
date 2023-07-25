package python

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	fs     afero.Fs
	config configManager
	cmd    cmd

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
		return venvPath, nil
	}

	err = i.cmd.Run(ctx, repo, &command{
		Name: "python3",
		Args: []string{"-m", "venv", venvPath},
	})

	if err != nil {
		return "", err
	}

	return venvPath, nil
}
