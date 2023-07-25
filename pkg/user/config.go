package user

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	bruinHomeDir       = ".bruin"
	homeDirPermissions = 0o755
	virtualEnvsPath    = "virtualenvs"
)

type ConfigManager struct {
	fs afero.Fs

	lock sync.Mutex

	userHomeDir  string
	bruinHomeDir string
}

func NewConfigManager(fs afero.Fs) *ConfigManager {
	return &ConfigManager{
		fs: fs,
	}
}

func (c *ConfigManager) EnsureHomeDirExists() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	bruinConfigPath := filepath.Join(homeDir, bruinHomeDir)
	if !path.DirExists(c.fs, bruinConfigPath) {
		err = c.fs.MkdirAll(bruinConfigPath, homeDirPermissions)
		if err != nil {
			return errors.Wrap(err, "failed to create bruin home directory")
		}
	}

	c.userHomeDir = homeDir
	c.bruinHomeDir = bruinConfigPath

	return nil
}

func (c *ConfigManager) makePathUnderConfig(dirName string) string {
	return filepath.Join(c.bruinHomeDir, dirName)
}

func (c *ConfigManager) MakeVirtualenvPath(dirName string) string {
	return filepath.Join(c.bruinHomeDir, virtualEnvsPath, dirName)
}

func (c *ConfigManager) EnsureVirtualenvDirExists() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.EnsureHomeDirExists()
	if err != nil {
		return err
	}

	venvPath := c.makePathUnderConfig(virtualEnvsPath)
	if !path.DirExists(c.fs, venvPath) {
		err = c.fs.MkdirAll(venvPath, homeDirPermissions)
		if err != nil {
			return errors.Wrap(err, "failed to create virtualenvs directory under bruin home")
		}
	}

	return nil
}
