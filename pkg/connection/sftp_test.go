package connection

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/mask"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestManager_AddSftpConnectionFromConfig(t *testing.T) {
	t.Parallel()
	var conn config.SFTPConnection
	require.NoError(t, yaml.Unmarshal([]byte(`name: sftp-key
host: sftp.example.com
port: 22
username: user
password: userinfo-passphrase
key_file: /keys/id_ed25519
key_passphrase: legacy-passphrase
known_hosts_file: /keys/known_hosts
host_key_fingerprint:
  - SHA256:first
  - SHA256:second
insecure_skip_host_key_check: true
`), &conn))
	m := Manager{availableConnections: map[string]any{}, AllConnectionDetails: map[string]any{}}
	require.NoError(t, m.AddSftpConnectionFromConfig(&conn))
	client := m.Sftp[conn.Name]
	require.NotNil(t, client)
	assert.Same(t, client, m.GetConnection(conn.Name))
	assert.Same(t, &conn, m.AllConnectionDetails[conn.Name])
	raw, err := client.GetIngestrURI()
	require.NoError(t, err)
	uri, err := url.Parse(raw)
	require.NoError(t, err)
	assert.Equal(t, "sftp.example.com:22", uri.Host)
	assert.Equal(t, "user", uri.User.Username())
	password, _ := uri.User.Password()
	assert.Equal(t, "userinfo-passphrase", password)
	assert.Equal(t, url.Values{
		"key_file":                     {"/keys/id_ed25519"},
		"key_passphrase":               {"legacy-passphrase"},
		"known_hosts_file":             {"/keys/known_hosts"},
		"host_key_fingerprint":         {"SHA256:first", "SHA256:second"},
		"insecure_skip_host_key_check": {"true"},
	}, uri.Query())

	conn.KeyFile = filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(conn.KeyFile, []byte("private-key-content"), 0o600))
	values, unreadable := mask.SensitiveValues(conn)
	assert.Empty(t, unreadable)
	assert.Contains(t, values, "private-key-content")
	assert.Contains(t, values, conn.Password)
	assert.Contains(t, values, conn.KeyPassphrase)
}
