package trino

import "testing"

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
