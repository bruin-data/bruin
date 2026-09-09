package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestReadOnlyConnection(t *testing.T) {
	t.Parallel()
	binary, err := filepath.Abs("../../bin/bruin")
	require.NoError(t, err)
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	require.FileExists(t, binary, "run make build in the repository root first")
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:15",
			Env:          map[string]string{"POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test", "POSTGRES_DB": "testdb"},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor: wait.ForSQL("5432/tcp", "pgx", func(host, port string) string {
				return "postgres://test:test@" + net.JoinHostPort(host, strings.TrimSuffix(port, "/tcp")) + "/testdb?sslmode=disable"
			}).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if container != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cancel()
			require.NoError(t, container.Terminate(ctx))
		})
	}
	require.NoError(t, err)
	host, err := container.Host(t.Context())
	require.NoError(t, err)
	port, err := container.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), ".bruin.yml")
	config := fmt.Sprintf(`default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: writable
          host: %q
          port: %s
          username: test
          password: test
          database: testdb
          ssl_mode: disable
        - name: readonly
          host: %q
          port: %s
          username: test
          password: test
          database: testdb
          ssl_mode: disable
          read_only: true
`, host, port.Port(), host, port.Port())
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))
	run := func(connection, sql string) ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), binary, "query", "--config-file", configPath,
			"--connection", connection, "--output", "json", "--query", sql)
		cmd.Env = append(os.Environ(), "DISABLE_TELEMETRY=true")
		return cmd.CombinedOutput()
	}
	write := func(sql string) {
		output, err := run("writable", sql)
		require.NoError(t, err, "%s: %s", sql, output)
	}
	write("CREATE TABLE readonly_rows (id INT PRIMARY KEY)")
	write("INSERT INTO readonly_rows VALUES (1), (2)")
	assertRows := func(connection string, expected [][]int) {
		output, err := run(connection, "SELECT id FROM readonly_rows ORDER BY id")
		require.NoError(t, err, "%s", output)
		var result struct {
			Rows [][]int `json:"rows"`
		}
		require.NoError(t, json.Unmarshal(output, &result), "%s", output)
		require.Equal(t, expected, result.Rows)
	}
	assertRows("readonly", [][]int{{1}, {2}})
	for _, sql := range []string{
		"INSERT INTO readonly_rows VALUES (3)",
		"UPDATE readonly_rows SET id = 4 WHERE id = 1",
		"DELETE FROM readonly_rows",
		"CREATE TABLE forbidden_rows (id INT)",
		"DROP TABLE readonly_rows",
		"TRUNCATE TABLE readonly_rows",
		"ALTER TABLE readonly_rows ADD COLUMN value INT",
	} {
		output, err := run("readonly", sql)
		require.Error(t, err, "%s: %s", sql, output)
		require.Contains(t, strings.ToLower(string(output)), "read-only transaction", "%s", sql)
		assertRows("readonly", [][]int{{1}, {2}})
		assertRows("writable", [][]int{{1}, {2}})
	}
	for _, sql := range []string{
		"SELECT 1; SELECT 2",
		"COMMIT; INSERT INTO readonly_rows VALUES (3)",
		"ROLLBACK; INSERT INTO readonly_rows VALUES (3)",
		"SET TRANSACTION READ WRITE; INSERT INTO readonly_rows VALUES (3)",
		"SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE; INSERT INTO readonly_rows VALUES (3)",
	} {
		output, err := run("readonly", sql)
		require.Error(t, err, "%s: %s", sql, output)
		require.Contains(t, strings.ToLower(string(output)), "cannot insert multiple commands into a prepared statement", "%s", sql)
		assertRows("readonly", [][]int{{1}, {2}})
		assertRows("writable", [][]int{{1}, {2}})
	}
	write("INSERT INTO readonly_rows VALUES (3)")
	assertRows("readonly", [][]int{{1}, {2}, {3}})
}
