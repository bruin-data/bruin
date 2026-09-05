package sftp

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config Config
		query  url.Values
	}{
		{name: "password only"},
		{
			name:   "key with known hosts",
			config: Config{KeyFile: "/keys/my key&file", KnownHostsFile: "~/.ssh/known_hosts"},
			query:  url.Values{"key_file": {"/keys/my key&file"}, "known_hosts_file": {"~/.ssh/known_hosts"}},
		},
		{
			name:   "encrypted key with multiple fingerprints",
			config: Config{KeyFile: "/keys/id_ed25519", KeyPassphrase: "secret +&?#/", HostKeyFingerprint: []string{"SHA256:abc+123", "SHA256:def/456"}},
			query:  url.Values{"key_file": {"/keys/id_ed25519"}, "key_passphrase": {"secret +&?#/"}, "host_key_fingerprint": {"SHA256:abc+123", "SHA256:def/456"}},
		},
		{
			name:   "explicit insecure opt out",
			config: Config{InsecureSkipHostKeyCheck: true},
			query:  url.Values{"insecure_skip_host_key_check": {"true"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.config.Host = "sftp.example.com"
			tt.config.Port = 22
			tt.config.Username = "user@example.com"
			uri, err := url.Parse(tt.config.GetIngestrURI())
			require.NoError(t, err)
			assert.Equal(t, "sftp", uri.Scheme)
			assert.Equal(t, "sftp.example.com:22", uri.Host)
			assert.Equal(t, tt.config.Username, uri.User.Username())
			assert.Equal(t, tt.query.Encode(), uri.Query().Encode())
		})
	}

	c := Config{Host: "localhost", Port: 22, Username: "user_1", Password: "pass-1234"}
	assert.Equal(t, "sftp://user_1:pass-1234@localhost:22", c.GetIngestrURI())
	c.Password = "pass +&@:/?#"
	uri, err := url.Parse(c.GetIngestrURI())
	require.NoError(t, err)
	password, _ := uri.User.Password()
	assert.Equal(t, c.Password, password)
}
