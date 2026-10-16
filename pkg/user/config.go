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
	bruinHomeDir        = ".bruin"
	homeDirPermissions  = 0o755
	virtualEnvsPath     = "virtualenvs"
	homeMigrationMarker = ".home-migrated"
)

var homeMigrationMu sync.Mutex

type ConfigManager struct {
	fs afero.Fs

	lock sync.Mutex

	userHomeDir  string
	bruinHomeDir string
}

func NewConfigManager(fs afero.Fs) *ConfigManager {
	if fs == nil {
		fs = afero.NewOsFs()
	}
	return &ConfigManager{
		fs: fs,
	}
}

func (c *ConfigManager) RecreateHomeDir() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	homeMigrationMu.Lock()
	defer homeMigrationMu.Unlock()

	err := c.ensureHomeDirSet()
	if err != nil {
		return err
	}

	if os.Getenv("BRUIN_HOME") == "" {
		return errors.Wrap(c.fs.RemoveAll(c.bruinHomeDir), "failed to remove bruin home directory")
	}
	err = c.fs.MkdirAll(c.bruinHomeDir, homeDirPermissions)
	if err != nil {
		return err
	}
	unlock, err := lockHomeMigration(c.fs, c.bruinHomeDir)
	if err != nil {
		return err
	}
	defer unlock()
	entries, err := afero.ReadDir(c.fs, c.bruinHomeDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		// Keep the same lockfile for processes waiting on this cleanup.
		if entry.Name() == homeMigrationLock {
			continue
		}
		if err := c.fs.RemoveAll(filepath.Join(c.bruinHomeDir, entry.Name())); err != nil {
			return errors.Wrap(err, "failed to remove bruin home contents")
		}
	}
	// Do not restore the legacy cache immediately after cleaning it.
	return afero.WriteFile(c.fs, filepath.Join(c.bruinHomeDir, homeMigrationMarker), nil, 0o600)
}

func (c *ConfigManager) EnsureAndGetBruinHomeDir() (string, error) {
	err := c.EnsureHomeDirExists()
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(c.bruinHomeDir)
	if err != nil {
		return "", err
	}
	return absPath, nil
}

func (c *ConfigManager) EnsureHomeDirExists() error {
	homeMigrationMu.Lock()
	defer homeMigrationMu.Unlock()

	err := c.ensureHomeDirSet()
	if err != nil {
		return err
	}

	if !path.DirExists(c.fs, c.bruinHomeDir) {
		err = c.fs.MkdirAll(c.bruinHomeDir, homeDirPermissions)
		if err != nil {
			return errors.Wrap(err, "failed to create bruin home directory")
		}
	}

	if os.Getenv("BRUIN_HOME") != "" {
		unlock, err := lockHomeMigration(c.fs, c.bruinHomeDir)
		if err != nil {
			return err
		}
		defer unlock()
		marker := filepath.Join(c.bruinHomeDir, homeMigrationMarker)
		if _, err := c.fs.Stat(marker); os.IsNotExist(err) {
			if c.userHomeDir != "" {
				if err := copyHomeFiles(c.fs, filepath.Join(c.userHomeDir, bruinHomeDir), c.bruinHomeDir, nil); err != nil {
					return errors.Wrap(err, "failed to copy previous bruin home")
				}
			}
			return afero.WriteFile(c.fs, marker, nil, 0o600)
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (c *ConfigManager) ensureHomeDirSet() error {
	homeDir, err := os.UserHomeDir()
	if homeDir != "" {
		homeDir, err = filepath.Abs(homeDir)
		if err != nil {
			return err
		}
	}
	c.userHomeDir = homeDir
	if override := os.Getenv("BRUIN_HOME"); override != "" {
		c.bruinHomeDir, err = filepath.Abs(override)
		if err != nil {
			return err
		}
		sameHome := c.bruinHomeDir == homeDir
		if !sameHome && homeDir != "" {
			homeInfo, homeErr := c.fs.Stat(homeDir)
			targetInfo, targetErr := c.fs.Stat(c.bruinHomeDir)
			sameHome = homeErr == nil && targetErr == nil && os.SameFile(homeInfo, targetInfo)
		}
		target := c.bruinHomeDir
		if c.fs.Name() == "OsFs" {
			if resolved, err := filepath.EvalSymlinks(target); err == nil {
				target = resolved
			}
		}
		if target == filepath.Dir(target) || sameHome {
			return errors.New("BRUIN_HOME must be a dedicated directory, not the filesystem root or user home")
		}
		return nil
	}
	if err != nil {
		return err
	}
	c.bruinHomeDir = filepath.Join(homeDir, bruinHomeDir)
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
