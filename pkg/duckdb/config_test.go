package duck

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
