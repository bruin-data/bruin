package clickhouse

import "testing"

func TestConfig_ToClickHouseOptions(t *testing.T) {
	t.Parallel()
	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     8123,
		Database: "database",
	}

	options := c.ToClickHouseOptions()
	if options.Addr[0] != "localhost:8123" {
		t.Errorf("expected localhost:8123, got %s", options.Addr[0])
	}
	if options.Auth.Database != "database" {
		t.Errorf("expected database, got %s", options.Auth.Database)
	}
	if options.Auth.Username != "user" {
		t.Errorf("expected user, got %s", options.Auth.Username)
	}
	if options.Auth.Password != "password" {
		t.Errorf("expected password, got %s", options.Auth.Password)
	}
}
