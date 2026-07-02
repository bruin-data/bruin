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
		{
			name:   "destination params http_port and replication_num",
			config: Config{Username: "root", Host: "fe", Port: 9030, Database: "db", HTTPPort: 8030, ReplicationNum: 1},
			want:   "starrocks://root@fe:9030/db?http_port=8030&replication_num=1",
		},
		{
			name:   "destination params combine with ssl",
			config: Config{Username: "root", Host: "fe", Port: 9030, SSL: "true", HTTPPort: 8040},
			want:   "starrocks://root@fe:9030?http_port=8040&ssl=true",
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
