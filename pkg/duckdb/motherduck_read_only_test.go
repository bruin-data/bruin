//go:build !bruin_no_duckdb

package duck

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMotherDuckReadOnlyOptions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		config   DuckDBConfig
		readOnly bool
	}{
		{"motherduck default", MotherDuckConfig{Database: "analytics", Token: "token"}, false},
		{"motherduck readonly", MotherDuckConfig{Database: "analytics", Token: "token", ReadOnly: true}, true},
		{"motherduck pointer", &MotherDuckConfig{ReadOnly: true}, true},
		{"duckdb default", Config{Path: "test.db"}, false},
		{"duckdb readonly", Config{Path: "test.db", ReadOnly: true}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &EphemeralConnection{config: test.config}
			options := connection.databaseOptions()
			require.Equal(t, test.config.ToDBConnectionURI(), options["path"])
			if test.readOnly {
				require.Equal(t, "read_only", options["access_mode"])
			} else {
				require.NotContains(t, options, "access_mode")
			}
		})
	}
}

func TestMotherDuckReadOnlyClient(t *testing.T) {
	t.Parallel()
	for _, readOnly := range []bool{false, true} {
		client, err := NewClient(MotherDuckConfig{Token: "test-token", Database: "analytics", ReadOnly: readOnly})
		require.NoError(t, err)
		require.Equal(t, readOnly, client.readOnly)
		uri, err := client.GetIngestrURI()
		if readOnly {
			require.EqualError(t, err, "read_only MotherDuck connections cannot be used with ingestr")
			require.Empty(t, uri)
		} else {
			require.NoError(t, err)
			require.Equal(t, "motherduck://analytics?token=test-token", uri)
		}
		client.Close()
	}
}

func TestMotherDuckTokenEscaping(t *testing.T) {
	t.Parallel()
	config := MotherDuckConfig{Database: "analytics", Token: "a&access_mode=read_write"}
	require.Equal(t, "md:analytics?motherduck_token=a%26access_mode%3Dread_write", config.ToDBConnectionURI())
	require.Equal(t, "motherduck://analytics?token=a%26access_mode%3Dread_write", config.GetIngestrURI())
}
