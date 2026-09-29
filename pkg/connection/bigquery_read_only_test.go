package connection

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestManagerBigQueryReadOnlyRejectsADC(t *testing.T) {
	t.Parallel()
	for _, readOnly := range []bool{false, true} {
		m := Manager{availableConnections: map[string]any{}, AllConnectionDetails: map[string]any{}}
		connection := &config.GoogleCloudPlatformConnection{
			ConnectionMetadata:               config.ConnectionMetadata{Name: "reader"},
			ProjectID:                        "test-project",
			UseApplicationDefaultCredentials: true,
			ReadOnly:                         readOnly,
		}
		err := m.AddBqConnectionFromConfig(connection)
		if readOnly {
			require.ErrorContains(t, err, "read_only requires service_account_json or service_account_file")
			require.Nil(t, m.GetConnection("reader"))
		} else {
			require.NoError(t, err)
			require.NotNil(t, m.GetConnection("reader"))
		}
	}
}
