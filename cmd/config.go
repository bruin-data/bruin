package cmd

import (
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
)

func loadConfig(fs afero.Fs, configFilePath string, requireExisting bool) (*config.Config, error) {
	if requireExisting {
		return config.Load(fs, configFilePath)
	}
	return config.LoadOrCreate(fs, configFilePath)
}
