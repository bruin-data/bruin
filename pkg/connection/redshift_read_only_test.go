package connection

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestManagerRedshiftReadOnly(t *testing.T) {
	t.Parallel()
	for _, readOnly := range []bool{false, true} {
		m := Manager{availableConnections: map[string]any{}, AllConnectionDetails: map[string]any{}}
		connection := &config.RedshiftConnection{ConnectionMetadata: config.ConnectionMetadata{Name: "reader"}, Host: "localhost", Port: 5432, SslMode: "disable", ReadOnly: readOnly}
		require.NoError(t, m.AddRedshiftConnectionFromConfig(t.Context(), connection))
		client, ok := m.GetConnection("reader").(interface{ GetIngestrURI() (string, error) })
		require.True(t, ok)
		uri, err := client.GetIngestrURI()
		if readOnly {
			require.EqualError(t, err, "read_only connections cannot be used with ingestr")
			require.Empty(t, uri)
		} else {
			require.NoError(t, err)
			require.NotEmpty(t, uri)
		}
	}
}
