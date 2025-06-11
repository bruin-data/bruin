package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnowflakeConnection_MarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		conn      SnowflakeConnection
		wantKey   string
		wantNoKey string
	}{
		{
			name: "inline private key",
			conn: SnowflakeConnection{
				Name:       "conn1",
				Account:    "acc",
				Username:   "usr",
				Region:     "region",
				PrivateKey: "some-key",
			},
			wantKey:   "private_key",
			wantNoKey: "private_key_path",
		},
		{
			name: "private key path",
			conn: SnowflakeConnection{
				Name:           "conn1",
				Account:        "acc",
				Username:       "usr",
				Region:         "region",
				PrivateKeyPath: "path/to/key",
			},
			wantKey:   "private_key_path",
			wantNoKey: "private_key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := tt.conn.MarshalJSON()
			require.NoError(t, err)

			var got map[string]string
			require.NoError(t, json.Unmarshal(data, &got))

			_, ok := got[tt.wantKey]
			require.True(t, ok)
			_, ok = got[tt.wantNoKey]
			require.False(t, ok)
		})
	}
}
