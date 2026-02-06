package telemetry

import (
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestLoadOrCreateInstallStateWithFS_CreatesAndPersists(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	homeDir := "/home/test/.bruin"
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	state, isNew, err := loadOrCreateInstallStateWithFS(fs, homeDir, "1.2.3", func() time.Time {
		return now
	})
	require.NoError(t, err)
	require.True(t, isNew)
	require.NotEmpty(t, state.InstallID)
	require.Equal(t, "1.2.3", state.InstallVersion)
	require.Equal(t, now.Format(time.RFC3339), state.InstallAt)

	state2, isNew2, err := loadOrCreateInstallStateWithFS(fs, homeDir, "9.9.9", time.Now)
	require.NoError(t, err)
	require.False(t, isNew2)
	require.Equal(t, state.InstallID, state2.InstallID)
	require.Equal(t, state.InstallAt, state2.InstallAt)
	require.Equal(t, state.InstallVersion, state2.InstallVersion)
}
