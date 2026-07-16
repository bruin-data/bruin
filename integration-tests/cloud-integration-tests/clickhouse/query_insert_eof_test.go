package clickhouse_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/query"
	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	clickHouseImage    = "clickhouse/clickhouse-server:26.2"
	clickHouseUser     = "default"
	clickHousePassword = "password"
	clickHouseDatabase = "default"
	connectionName     = "clickhouse-default"
)

// TestQueryInsertDoesNotReportEOF protects the regression from
// https://github.com/bruin-data/bruin/issues/2394. ClickHouse commits INSERT
// statements successfully, but bruin query previously reported a bare EOF.
func TestQueryInsertDoesNotReportEOF(t *testing.T) {
	host, port := startClickHouse(t)
	configPath := writeClickHouseConfig(t, host, port)
	binary := bruinBinary(t)

	createOutput := runBruinQuery(t, binary, configPath,
		"CREATE TABLE IF NOT EXISTS bug_test_seed (event_date Date, row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	)
	require.Contains(t, createOutput, "Statement executed successfully")
	require.NotContains(t, createOutput, "EOF")

	insertOutput := runBruinQuery(t, binary, configPath,
		"INSERT INTO bug_test_seed (event_date, row_id, amount) VALUES (toDate('2026-07-16'), 'row_7', 77)",
	)
	require.Contains(t, insertOutput, "Statement executed successfully")
	require.NotContains(t, insertOutput, "EOF")
	require.NotContains(t, insertOutput, "query execution failed")

	commentedInsertOutput := runBruinQuery(t, binary, configPath,
		"-- seed insert\nINSERT INTO bug_test_seed (event_date, row_id, amount) VALUES (toDate('2026-07-16'), 'row_8', 88)",
	)
	require.Contains(t, commentedInsertOutput, "Statement executed successfully")
	require.NotContains(t, commentedInsertOutput, "EOF")

	selectOutput := runBruinQuery(t, binary, configPath,
		"SELECT row_id, amount FROM bug_test_seed ORDER BY row_id",
	)
	require.Contains(t, selectOutput, "row_7")
	require.Contains(t, selectOutput, "77")
	require.Contains(t, selectOutput, "row_8")
	require.Contains(t, selectOutput, "88")

	client, err := clickhouse.NewClient(&clickhouse.Config{
		Username: clickHouseUser,
		Password: clickHousePassword,
		Host:     host,
		Port:     port,
		Database: clickHouseDatabase,
	})
	require.NoError(t, err)

	_, err = client.SelectWithSchema(t.Context(), &query.Query{
		Query: "CREATE TABLE IF NOT EXISTS client_insert_seed (row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	})
	require.NoError(t, err)

	result, err := client.SelectWithSchema(t.Context(), &query.Query{
		Query: "INSERT INTO client_insert_seed (row_id, amount) VALUES ('row_1', 11)",
	})
	require.NoError(t, err)
	require.NotNil(t, result.Execution)
	require.Equal(t, "INSERT", result.Execution.StatementType)

	rows, err := client.Select(t.Context(), &query.Query{
		Query: "SELECT row_id, amount FROM client_insert_seed WHERE row_id = 'row_1'",
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "row_1", rows[0][0])
}

func startClickHouse(t *testing.T) (string, int) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	image := os.Getenv("CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = clickHouseImage
	}

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			Env: map[string]string{
				"CLICKHOUSE_DB":                        clickHouseDatabase,
				"CLICKHOUSE_USER":                      clickHouseUser,
				"CLICKHOUSE_PASSWORD":                  clickHousePassword,
				"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
			},
			ExposedPorts: []string{"9000/tcp"},
			HostConfigModifier: func(hostConfig *dockercontainer.HostConfig) {
				hostConfig.Ulimits = []*dockercontainer.Ulimit{
					{Name: "nofile", Soft: 262144, Hard: 262144},
				}
			},
			WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err, "failed to start the ClickHouse container")
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(t.Context())
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(t.Context(), "9000/tcp")
	require.NoError(t, err)
	port, err := strconv.Atoi(mappedPort.Port())
	require.NoError(t, err)

	return host, port
}

func writeClickHouseConfig(t *testing.T, host string, port int) string {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), ".bruin.yml")
	configYAML := fmt.Sprintf(`default_environment: default
environments:
  default:
    connections:
      clickhouse:
        - name: %s
          username: %s
          password: %s
          host: %s
          port: %d
          database: %s
`, connectionName, clickHouseUser, clickHousePassword, host, port, clickHouseDatabase)
	require.NoError(t, os.WriteFile(configPath, []byte(configYAML), 0o600))
	return configPath
}

func bruinBinary(t *testing.T) string {
	t.Helper()

	if binary := os.Getenv("BRUIN_TEST_BINARY"); binary != "" {
		require.FileExists(t, binary)
		return binary
	}

	workingDirectory, err := os.Getwd()
	require.NoError(t, err)
	repositoryRoot, err := filepath.Abs(filepath.Join(workingDirectory, "../../.."))
	require.NoError(t, err)

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}

	binary := filepath.Join(repositoryRoot, "bin", executable)
	require.FileExists(t, binary)
	return binary
}

func runBruinQuery(t *testing.T, binary, configPath, sql string) string {
	t.Helper()

	args := []string{
		"query",
		"--config-file", configPath,
		"--connection", connectionName,
		"--output", "plain",
		"--query", sql,
	}
	cmd := exec.CommandContext(t.Context(), binary, args...)
	cmd.Env = append(os.Environ(), "DISABLE_TELEMETRY=true", "INGESTR_DISABLE_TELEMETRY=true")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "bruin query failed:\n%s", output)
	return string(output)
}
