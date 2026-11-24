package duck

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/mask"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_UsesDuckDB2DriverForDuckDB2Files(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// The first 20 bytes of files written by DuckDB 1.5.2 (version 64) and 2.0.0 (version 999).
	v1 := filepath.Join(dir, "v1.duckdb")
	v2 := filepath.Join(dir, "v2.duckdb")
	require.NoError(t, os.WriteFile(v1, []byte("R\xf6\r\xaa\xd0\x87\xa1\xadDUCK\x40\x00\x00\x00\x00\x00\x00\x00"), 0o600))
	require.NoError(t, os.WriteFile(v2, []byte("*]\x13\xb0O\x19!qDUCK\xe7\x03\x00\x00\x00\x00\x00\x00"), 0o600))

	assert.False(t, Config{Path: v1}.usesDuckDB2Driver())
	assert.True(t, Config{Path: v2}.usesDuckDB2Driver())
	assert.False(t, Config{Path: filepath.Join(dir, "new.duckdb")}.usesDuckDB2Driver())
	assert.True(t, Config{Quack: true}.usesDuckDB2Driver())
	assert.False(t, IsDuckDB2FileURI("duckdb:///"+v1))
	assert.True(t, IsDuckDB2FileURI("duckdb:///"+v2))
}

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()
	c := Config{
		Path: "/some/path/db.duckdb",
	}

	assert.Equal(t, "/some/path/db.duckdb", c.ToDBConnectionURI())
}

func TestConfig_ToIngestr(t *testing.T) {
	t.Parallel()
	c := Config{
		Path: "/some/path/db.duckdb",
	}

	assert.Equal(t, "duckdb:////some/path/db.duckdb", c.GetIngestrURI())
}

func TestConfig_ReadOnly_DoesNotAffectURI(t *testing.T) {
	t.Parallel()
	c := Config{
		Path:     "/some/path/db.duckdb",
		ReadOnly: true,
	}

	assert.Equal(t, "/some/path/db.duckdb", c.ToDBConnectionURI())
	assert.Equal(t, "duckdb:////some/path/db.duckdb", c.GetIngestrURI())
}

func TestConfig_QuackIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		c    Config
		want string
	}{
		{"host and port", Config{Quack: true, Host: "localhost", Port: 9494, Token: "abc"}, "duckdb://:abc@localhost:9494?quack=true"},
		{"default port", Config{Quack: true, Host: "my-server.com", Token: "abc"}, "duckdb://:abc@my-server.com?quack=true"},
		{"disable ssl", Config{Quack: true, Host: "10.0.0.5", Port: 9494, Token: "abc", DisableSSL: true}, "duckdb://:abc@10.0.0.5:9494?disable_ssl=true&quack=true"},
		{"special characters in token", Config{Quack: true, Host: "localhost", Token: "a@b:c/d"}, "duckdb://:a%40b%3Ac%2Fd@localhost?quack=true"},
		{"ipv6", Config{Quack: true, Host: "::1", Port: 9494, Token: "abc"}, "duckdb://:abc@[::1]:9494?quack=true"},
		{"quack wins over path", Config{Path: "/tmp/a.db", Quack: true, Host: "localhost", Token: "abc"}, "duckdb://:abc@localhost?quack=true"},
		{"host without quack is ignored", Config{Path: "/tmp/a.db", Host: "localhost", Token: "abc"}, "duckdb:////tmp/a.db"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.c.GetIngestrURI())
		})
	}
}

func TestConfig_QuackAttachStatements(t *testing.T) {
	t.Parallel()

	c := Config{Quack: true, Host: "localhost", Port: 9494, Token: "a'b"}
	assert.Equal(t, []string{
		"INSTALL quack",
		"LOAD quack",
		"CREATE OR REPLACE SECRET bruin_quack (TYPE quack, TOKEN 'a''b', SCOPE 'quack:localhost:9494')",
		"ATTACH 'quack:localhost:9494' AS quack_db",
		"USE quack_db",
	}, c.quackAttachStatements())

	c = Config{Quack: true, Host: "::1", Token: "abc", DisableSSL: true}
	assert.Contains(t, c.quackAttachStatements(), "ATTACH 'quack:[::1]' AS quack_db (DISABLE_SSL true)")
}

func TestConfig_QuackAttachStatementsMaskToken(t *testing.T) {
	t.Parallel()

	c := Config{Quack: true, Host: "localhost", Token: "to'ken"}
	m := mask.New([]string{c.Token})
	for _, stmt := range c.quackAttachStatements() {
		masked := m.Mask(stmt)
		assert.NotContains(t, masked, "to''ken")
		assert.NotContains(t, masked, "to'ken")
	}
	assert.Contains(t, m.Mask(c.quackAttachStatements()[2]), "TOKEN '****'")
}
