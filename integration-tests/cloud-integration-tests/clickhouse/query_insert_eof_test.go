package clickhouse_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
	clickHouseImage              = "clickhouse/clickhouse-server:26.2"
	clickHouseDeduplicationImage = "clickhouse/clickhouse-server:25.8"
	clickHouseUser               = "default"
	clickHousePassword           = "password"
	clickHouseDatabase           = "default"
	connectionName               = "clickhouse-default"
	clickHouseKeeperConfig       = `<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <heart_beat_interval_ms>0</heart_beat_interval_ms>
        <election_timeout_lower_bound_ms>100</election_timeout_lower_bound_ms>
        <election_timeout_upper_bound_ms>200</election_timeout_upper_bound_ms>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>localhost</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
    <zookeeper>
        <node>
            <host>127.0.0.1</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>`
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

// TestTimeIntervalRerunKeepsRows protects the regression from
// https://github.com/bruin-data/bruin/issues/2396. ClickHouse's insert
// deduplication must not treat a time-interval refresh as a retry of the
// previous refresh after Bruin has deleted that interval.
func TestTimeIntervalRerunKeepsRows(t *testing.T) {
	// This ReplicatedMergeTree version reproduces SharedMergeTree's behavior:
	// the lightweight delete does not prevent the replacement block from being
	// deduplicated on an identical interval rerun.
	host, port := startClickHouseWithKeeper(t, clickHouseDeduplicationImage)
	configPath := writeClickHouseConfig(t, host, port)
	binary := bruinBinary(t)

	runBruinQuery(t, binary, configPath,
		"CREATE TABLE bug_test_seed (event_date Date, row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		"CREATE TABLE bug_test_incremental (event_date Date, row_id String, amount Int32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/bug_test_incremental', 'replica1') ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		"INSERT INTO bug_test_seed (event_date, row_id, amount) VALUES (toDate('2026-07-16'), 'row_7', 77)",
	)

	assetPath := writeTimeIntervalPipeline(t)
	for range 2 {
		runBruin(t, binary, configPath,
			"run",
			"--env", "default",
			"--start-date", "2026-07-16",
			"--end-date", "2026-07-16",
			assetPath,
		)
	}

	client, err := clickhouse.NewClient(&clickhouse.Config{
		Username: clickHouseUser,
		Password: clickHousePassword,
		Host:     host,
		Port:     port,
		Database: clickHouseDatabase,
	})
	require.NoError(t, err)

	rows, err := client.Select(t.Context(), &query.Query{
		Query: "SELECT row_id, amount FROM bug_test_incremental WHERE event_date = toDate('2026-07-16')",
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"row_7", int32(77)}}, rows)

	require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: "SYSTEM FLUSH LOGS"}))
	rows, err = client.Select(t.Context(), &query.Query{
		Query: `SELECT count()
FROM system.query_log
WHERE type = 'QueryFinish'
  AND position(query, 'INSERT INTO bug_test_incremental SETTINGS insert_deduplicate = 0') > 0`,
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{uint64(2)}}, rows)
}

func startClickHouse(t *testing.T) (string, int) {
	t.Helper()

	image := os.Getenv("CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = clickHouseImage
	}
	return startClickHouseContainer(t, image, false)
}

func startClickHouseWithKeeper(t *testing.T, image string) (string, int) {
	t.Helper()
	return startClickHouseContainer(t, image, true)
}

func startClickHouseContainer(t *testing.T, image string, withKeeper bool) (string, int) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	var files []testcontainers.ContainerFile
	if withKeeper {
		files = append(files, testcontainers.ContainerFile{
			Reader:            strings.NewReader(clickHouseKeeperConfig),
			ContainerFilePath: "/etc/clickhouse-server/config.d/keeper.xml",
			FileMode:          0o644,
		})
	}

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			Files: files,
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

	client, err := clickhouse.NewClient(&clickhouse.Config{
		Username: clickHouseUser,
		Password: clickHousePassword,
		Host:     host,
		Port:     port,
		Database: clickHouseDatabase,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := client.Select(t.Context(), &query.Query{Query: "SELECT 1"})
		return err == nil
	}, 30*time.Second, 500*time.Millisecond, "ClickHouse did not become query-ready")

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

func writeTimeIntervalPipeline(t *testing.T) string {
	t.Helper()

	pipelineDir, err := os.MkdirTemp(".", "time-interval-pipeline-")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(pipelineDir))
	})
	pipelineDir, err = filepath.Abs(pipelineDir)
	require.NoError(t, err)
	assetsDir := filepath.Join(pipelineDir, "assets")
	require.NoError(t, os.MkdirAll(assetsDir, 0o755))

	pipelineYAML := `name: clickhouse-time-interval-test
default_connections:
  clickhouse: clickhouse-default
`
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte(pipelineYAML), 0o600))

	assetSQL := `/* @bruin
name: bug_test_incremental
type: clickhouse.sql
materialization:
  type: table
  strategy: time_interval
  incremental_key: event_date
  time_granularity: date
@bruin */

SELECT event_date, row_id, amount
FROM bug_test_seed
WHERE event_date BETWEEN toDate('{{start_date}}') AND toDate('{{end_date}}')
`
	assetPath := filepath.Join(assetsDir, "bug_test_incremental.sql")
	require.NoError(t, os.WriteFile(assetPath, []byte(assetSQL), 0o600))
	return assetPath
}

func runBruin(t *testing.T, binary, configPath string, args ...string) string {
	t.Helper()

	commandArgs := []string{args[0], "--config-file", configPath}
	commandArgs = append(commandArgs, args[1:]...)
	cmd := exec.CommandContext(t.Context(), binary, commandArgs...)
	cmd.Env = append(os.Environ(), "DISABLE_TELEMETRY=true", "INGESTR_DISABLE_TELEMETRY=true")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "bruin %s failed:\n%s", commandArgs[0], output)
	return string(output)
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
