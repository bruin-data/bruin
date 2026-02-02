package telemetry

import (
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/bruin-data/bruin/pkg/user"
	"github.com/google/uuid"
	"github.com/spf13/afero"
)

const telemetryStateFileName = "telemetry.json"

type installState struct {
	InstallID      string `json:"install_id"`
	InstallAt      string `json:"install_at"`
	InstallVersion string `json:"install_version"`
}

func loadOrCreateInstallState(appVersion string) (installState, bool, error) {
	fs := afero.NewOsFs()
	cm := user.NewConfigManager(fs)
	bruinHomeDir, err := cm.EnsureAndGetBruinHomeDir()
	if err != nil {
		return installState{}, false, err
	}

	return loadOrCreateInstallStateWithFS(fs, bruinHomeDir, appVersion, time.Now)
}

func loadOrCreateInstallStateWithFS(fs afero.Fs, bruinHomeDir string, appVersion string, now func() time.Time) (installState, bool, error) {
	if err := fs.MkdirAll(bruinHomeDir, 0o755); err != nil {
		return installState{}, false, err
	}

	statePath := filepath.Join(bruinHomeDir, telemetryStateFileName)
	state, err := readInstallState(fs, statePath)
	if err == nil && state.InstallID != "" {
		return state, false, nil
	}

	newState := installState{
		InstallID:      uuid.NewString(),
		InstallAt:      now().UTC().Format(time.RFC3339),
		InstallVersion: appVersion,
	}

	if err := writeInstallState(fs, statePath, newState); err != nil {
		return newState, false, err
	}

	return newState, true, nil
}

func readInstallState(fs afero.Fs, path string) (installState, error) {
	buf, err := afero.ReadFile(fs, path)
	if err != nil {
		return installState{}, err
	}

	var state installState
	if err := json.Unmarshal(buf, &state); err != nil {
		return installState{}, err
	}

	return state, nil
}

func writeInstallState(fs afero.Fs, path string, state installState) error {
	buf, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return afero.WriteFile(fs, path, buf, 0o600)
}
