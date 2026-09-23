package clickhouse

import (
	"strings"
	"testing"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
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

func TestConfigSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		config   Config
		expected click_house.Settings
	}{
		{
			name: "absent settings",
		},
		{
			name:     "empty settings",
			config:   Config{Settings: map[string]any{}},
			expected: click_house.Settings{},
		},
		{
			name: "typed values",
			config: Config{Settings: map[string]any{
				"max_threads":                 4,
				"max_memory_usage":            uint64(10000000000),
				"max_execution_time":          1.5,
				"join_algorithm":              "grace_hash",
				"allow_experimental_analyzer": true,
			}},
			expected: click_house.Settings{
				"max_threads":                 4,
				"max_memory_usage":            uint64(10000000000),
				"max_execution_time":          1.5,
				"join_algorithm":              "grace_hash",
				"allow_experimental_analyzer": true,
			},
		},
		{
			name: "read only merges settings",
			config: Config{
				ReadOnly: true,
				Settings: map[string]any{"max_threads": 4},
			},
			expected: click_house.Settings{"readonly": 1, "max_threads": 4},
		},
		{
			name: "read only overrides conflicting setting",
			config: Config{
				ReadOnly: true,
				Settings: map[string]any{"readonly": 0, "max_threads": 4},
			},
			expected: click_house.Settings{"readonly": 1, "max_threads": 4},
		},
		{
			name:     "read only false preserves explicit setting",
			config:   Config{Settings: map[string]any{"readonly": 2}},
			expected: click_house.Settings{"readonly": 2},
		},
		{
			name: "cluster settings merge with read only and user settings",
			config: Config{
				Cluster:  "analytics",
				ReadOnly: true,
				Settings: map[string]any{
					"max_threads":                  4,
					"readonly":                     0,
					"distributed_ddl_output_mode":  "none",
					"distributed_ddl_task_timeout": 60,
				},
			},
			expected: click_house.Settings{
				"max_threads":                  4,
				"readonly":                     1,
				"distributed_ddl_output_mode":  "throw",
				"distributed_ddl_task_timeout": 180,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, tt.config.ToClickHouseOptions().Settings)
		})
	}
}

func TestConfigSettingsIndependentMaps(t *testing.T) {
	t.Parallel()
	settings := map[string]any{
		"max_threads":                  4,
		"readonly":                     0,
		"distributed_ddl_output_mode":  "none",
		"distributed_ddl_task_timeout": 60,
	}
	c := Config{ReadOnly: true, Cluster: "analytics", Settings: settings}
	first := c.ToClickHouseOptions()
	second := c.ToClickHouseOptions()
	require.Equal(t, map[string]any{
		"max_threads":                  4,
		"readonly":                     0,
		"distributed_ddl_output_mode":  "none",
		"distributed_ddl_task_timeout": 60,
	}, settings)

	first.Settings["max_threads"] = 8
	first.Settings["max_execution_time"] = 30
	require.Equal(t, 4, settings["max_threads"])
	require.NotContains(t, settings, "max_execution_time")
	require.Equal(t, 4, second.Settings["max_threads"])
	require.NotContains(t, second.Settings, "max_execution_time")

	settings["max_threads"] = 16
	require.Equal(t, 8, first.Settings["max_threads"])
	require.Equal(t, 4, second.Settings["max_threads"])
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
