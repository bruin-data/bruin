package clickhouse_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadOnlyConnection(t *testing.T) {
	host, port := startClickHouse(t)
	writableConfig := writeClickHouseConfig(t, host, port)
	binary := bruinBinary(t)
	data, err := os.ReadFile(writableConfig)
	require.NoError(t, err)
	readonlyConfig := filepath.Join(t.TempDir(), ".bruin.yml")
	data = append(data, []byte("          read_only: true\n")...)
	require.NoError(t, os.WriteFile(readonlyConfig, data, 0o600))

	runBruinQuery(t, binary, writableConfig, "CREATE TABLE readonly_rows (id Int32) ENGINE=Memory")
	runBruinQuery(t, binary, writableConfig, "INSERT INTO readonly_rows VALUES (1)")

	run := func(configPath, sql string) ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), binary, "query", "--config-file", configPath,
			"--connection", connectionName, "--output", "json", "--query", sql)
		cmd.Env = append(os.Environ(), "DISABLE_TELEMETRY=true")
		return cmd.CombinedOutput()
	}
	assertCount := func(t *testing.T, configPath string, count float64) {
		t.Helper()
		output, err := run(configPath, "SELECT count(*) AS count FROM readonly_rows")
		require.NoError(t, err, "%s", output)
		var result struct {
			Rows [][]float64 `json:"rows"`
		}
		require.NoError(t, json.Unmarshal(output, &result), "%s", output)
		require.Equal(t, [][]float64{{count}}, result.Rows)
	}
	assertCount(t, readonlyConfig, 1)
	for _, sql := range []string{
		"INSERT INTO readonly_rows VALUES (2)",
		"CREATE TABLE forbidden_rows (id Int32) ENGINE=Memory",
		"DROP TABLE readonly_rows",
		"TRUNCATE TABLE readonly_rows",
		"ALTER TABLE readonly_rows ADD COLUMN value Int32",
		"SET readonly=0",
		"INSERT INTO readonly_rows SETTINGS readonly=0 VALUES (3)",
	} {
		t.Run(sql, func(t *testing.T) {
			output, err := run(readonlyConfig, sql)
			require.Error(t, err, "%s", output)
			require.Contains(t, strings.ToLower(string(output)), "readonly mode")
			assertCount(t, readonlyConfig, 1)
			assertCount(t, writableConfig, 1)
		})
	}
	runBruinQuery(t, binary, writableConfig, "INSERT INTO readonly_rows VALUES (4)")
	assertCount(t, readonlyConfig, 2)
}
