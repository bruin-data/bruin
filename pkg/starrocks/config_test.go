package starrocks

import "testing"

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name:   "minimal with default port",
			config: Config{Username: "root", Host: "localhost"},
			want:   "starrocks://root@localhost:9030",
		},
		{
			name:   "user and password",
			config: Config{Username: "root", Password: "secret", Host: "fe", Port: 9030, Database: "analytics"},
			want:   "starrocks://root:secret@fe:9030/analytics",
		},
		{
			name:   "catalog and database",
			config: Config{Username: "root", Host: "fe", Port: 9030, Catalog: "iceberg_catalog", Database: "lake"},
			want:   "starrocks://root@fe:9030/iceberg_catalog/lake",
		},
		{
			name:   "ssl enabled",
			config: Config{Username: "root", Host: "fe", Port: 9030, Database: "db", SSL: "true"},
			want:   "starrocks://root@fe:9030/db?ssl=true",
		},
		{
			name:   "ssl skip-verify",
			config: Config{Username: "root", Host: "fe", Port: 9030, SSL: "skip-verify"},
			want:   "starrocks://root@fe:9030?ssl=skip-verify",
		},
		{
			name:   "catalog without database is omitted from the path",
			config: Config{Username: "root", Host: "fe", Port: 9030, Catalog: "iceberg_catalog"},
			want:   "starrocks://root@fe:9030",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.config.GetIngestrURI(); got != tt.want {
				t.Errorf("GetIngestrURI() = %q, want %q", got, tt.want)
			}
		})
	}
}
