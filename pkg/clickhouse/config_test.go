package clickhouse

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/version"
	"github.com/stretchr/testify/require"
)

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
	if userAgent := options.ClientInfo.String(); !strings.HasPrefix(userAgent, "bruin/"+version.Version+" clickhouse-go/") {
		t.Errorf("expected ClickHouse user agent to identify Bruin, got %s", userAgent)
	}
}

func TestConfigReadOnly(t *testing.T) {
	t.Parallel()
	for _, readOnly := range []bool{false, true} {
		c := Config{ReadOnly: readOnly}
		options := c.ToClickHouseOptions()
		if readOnly {
			if options.Settings["readonly"] != 1 {
				t.Fatalf("expected readonly=1, got %v", options.Settings)
			}
		} else if _, ok := options.Settings["readonly"]; ok {
			t.Fatal("readonly should be unset by default")
		}
	}
}

func TestConfigClusterSettings(t *testing.T) {
	t.Parallel()
	for _, cluster := range []string{"", "analytics"} {
		for _, readOnly := range []bool{false, true} {
			c := Config{Cluster: cluster, ReadOnly: readOnly}
			options := c.ToClickHouseOptions()
			require.Equal(t, cluster, c.GetCluster())
			if cluster == "" {
				require.NotContains(t, options.Settings, "distributed_ddl_output_mode")
				require.NotContains(t, options.Settings, "distributed_ddl_task_timeout")
			} else {
				require.Equal(t, "throw", options.Settings["distributed_ddl_output_mode"])
				require.Equal(t, 180, options.Settings["distributed_ddl_task_timeout"])
			}
			if readOnly {
				require.Equal(t, 1, options.Settings["readonly"])
			} else {
				require.NotContains(t, options.Settings, "readonly")
			}
		}
	}
}
