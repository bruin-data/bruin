package connection

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestManagerMotherduckReadOnly(t *testing.T) {
	t.Parallel()
	for _, readOnly := range []bool{false, true} {
		m := Manager{availableConnections: map[string]any{}, AllConnectionDetails: map[string]any{}}
		connection := &config.MotherduckConnection{ConnectionMetadata: config.ConnectionMetadata{Name: "reader"}, Token: "test-token", ReadOnly: readOnly}
		require.NoError(t, m.AddMotherduckConnectionFromConfig(connection))
		client, ok := m.GetConnection("reader").(interface{ GetIngestrURI() (string, error) })
		require.True(t, ok)
		uri, err := client.GetIngestrURI()
		if readOnly {
			require.EqualError(t, err, "read_only MotherDuck connections cannot be used with ingestr")
			require.Empty(t, uri)
		} else {
			require.NoError(t, err)
			require.NotEmpty(t, uri)
		}
	}
}
