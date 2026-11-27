package trino

import (
	"net"
	"net/url"
	"strconv"
	"testing"
)

func TestConfig_ToDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "username only",
			cfg:  Config{Username: "admin", Host: "localhost", Port: 8080},
			want: "http://admin@localhost:8080",
		},
		{
			name: "with password and catalog",
			cfg:  Config{Username: "user", Password: "secret", Host: "trino.example.com", Port: 8443, Catalog: "hive", Schema: "analytics"},
			want: "http://user:secret@trino.example.com:8443?catalog=hive&schema=analytics",
		},
		{
			name: "colon in username",
			cfg:  Config{Username: "user:name", Password: "secret", Host: "localhost", Port: 8080},
			want: "http://user%3Aname:secret@localhost:8080",
		},
		{
			name: "reserved characters in credentials",
			cfg:  Config{Username: "user:name@example.com", Password: "p@ss:/?#[]% word", Host: "localhost", Port: 8080},
			want: "http://user%3Aname%40example.com:p%40ss%3A%2F%3F%23%5B%5D%25%20word@localhost:8080",
		},
		{
			name: "reserved characters without password",
			cfg:  Config{Username: "user/name#tag", Host: "localhost", Port: 8080},
			want: "http://user%2Fname%23tag@localhost:8080",
		},
		{
			name: "literal percent escapes",
			cfg:  Config{Username: "user%40name", Password: "p%2Fss", Host: "localhost", Port: 8080},
			want: "http://user%2540name:p%252Fss@localhost:8080",
		},
		{
			name: "unicode credentials",
			cfg:  Config{Username: "usér", Password: "päss", Host: "localhost", Port: 8080},
			want: "http://us%C3%A9r:p%C3%A4ss@localhost:8080",
		},
		{
			name: "IPv6 host",
			cfg:  Config{Username: "admin", Host: "::1", Port: 8080},
			want: "http://admin@[::1]:8080",
		},
		{
			name: "escaped catalog and schema",
			cfg:  Config{Username: "admin", Host: "localhost", Port: 8080, Catalog: "my catalog", Schema: "a&b"},
			want: "http://admin@localhost:8080?catalog=my+catalog&schema=a%26b",
		},
		{
			name: "empty username",
			cfg:  Config{Host: "localhost", Port: 8080},
			want: "http://@localhost:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.cfg.ToDSN()
			if got != tt.want {
				t.Errorf("ToDSN() = %q, want %q", got, tt.want)
			}
			parsed, err := url.Parse(got)
			if err != nil {
				t.Fatalf("ToDSN() returned an invalid URL: %v", err)
			}
			if username := parsed.User.Username(); username != tt.cfg.Username {
				t.Errorf("parsed username = %q, want %q", username, tt.cfg.Username)
			}
			password, hasPassword := parsed.User.Password()
			if password != tt.cfg.Password || hasPassword != (tt.cfg.Password != "") {
				t.Errorf("parsed password = %q (present: %t), want %q (present: %t)", password, hasPassword, tt.cfg.Password, tt.cfg.Password != "")
			}
			if host := parsed.Host; host != net.JoinHostPort(tt.cfg.Host, strconv.Itoa(tt.cfg.Port)) {
				t.Errorf("parsed host = %q, want %s:%d", host, tt.cfg.Host, tt.cfg.Port)
			}
			if parsed.Path != "" || parsed.Fragment != "" {
				t.Errorf("credentials leaked into URL path %q or fragment %q", parsed.Path, parsed.Fragment)
			}
			for key, want := range map[string]string{"catalog": tt.cfg.Catalog, "schema": tt.cfg.Schema} {
				if got := parsed.Query().Get(key); got != want {
					t.Errorf("parsed %s = %q, want %q", key, got, want)
				}
			}
		})
	}
}

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "catalog only",
			cfg: Config{
				Username: "admin",
				Host:     "localhost",
				Port:     8080,
				Catalog:  "hive",
			},
			want: "trino://admin@localhost:8080/hive",
		},
		{
			name: "with password",
			cfg: Config{
				Username: "user",
				Password: "secret",
				Host:     "trino.example.com",
				Port:     8443,
				Catalog:  "iceberg",
			},
			want: "trino://user:secret@trino.example.com:8443/iceberg",
		},
		{
			name: "encodes credentials",
			cfg: Config{
				Username: "user@example.com",
				Password: "p@ss word",
				Host:     "trino.example.com",
				Port:     8443,
				Catalog:  "iceberg",
			},
			want: "trino://user%40example.com:p%40ss%20word@trino.example.com:8443/iceberg",
		},
		{
			name: "omits catalog",
			cfg: Config{
				Username: "user",
				Host:     "localhost",
				Port:     8080,
			},
			want: "trino://user@localhost:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.cfg.GetIngestrURI(); got != tt.want {
				t.Fatalf("GetIngestrURI() = %q, want %q", got, tt.want)
			}
		})
	}
}
