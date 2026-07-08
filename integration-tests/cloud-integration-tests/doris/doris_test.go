package doris

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/e2e"
	_ "github.com/go-sql-driver/mysql"
	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const connectionName = "doris-default"

func TestDorisWorkflows(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	if runtime.GOOS != "linux" && os.Getenv("DORIS_RUN_ON_NON_LINUX") == "" {
		t.Skip("Doris all-in-one container requires vm.max_map_count >= 2000000 and swap disabled in the Docker VM; on macOS run `docker run --rm --privileged alpine sysctl -w vm.max_map_count=2000000` and `docker run --rm --privileged alpine swapoff -a`, then set DORIS_RUN_ON_NON_LINUX=1 to force")
	}

	ctx := context.Background()
	image := os.Getenv("DORIS_TEST_IMAGE")
	if image == "" {
		image = "apache/doris:4.0.3-all-slim"
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			Entrypoint:   []string{"bash", "-c"},
			Cmd:          []string{"bash /usr/local/bin/entry_point.sh && tail -f /dev/null"},
			ExposedPorts: []string{"8030/tcp", "9030/tcp"},
			HostConfigModifier: func(hostConfig *dockercontainer.HostConfig) {
				if runtime.GOOS == "linux" {
					hostConfig.Sysctls = map[string]string{
						"vm.max_map_count": "2000000",
					}
				}
				hostConfig.Ulimits = []*dockercontainer.Ulimit{
					{Name: "nofile", Soft: 1000000, Hard: 1000000},
				}
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("8030/tcp"),
				wait.ForListeningPort("9030/tcp"),
			).WithStartupTimeout(8 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err, "failed to start the Doris container")
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(ctx, "9030/tcp")
	require.NoError(t, err)

	waitForDorisSQL(t, host, mappedPort.Port())

	currentFolder, err := os.Getwd()
	require.NoError(t, err)
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")
	pipelines := filepath.Join(currentFolder, "test-pipelines")

	configFile := filepath.Join(t.TempDir(), ".bruin.yml")
	configYAML := fmt.Sprintf(`default_environment: default
environments:
    default:
        connections:
            doris:
                - name: %s
                  username: root
                  password: ""
                  host: %s
                  port: %s
                  database: bruin_test
`, connectionName, host, mappedPort.Port())
	require.NoError(t, os.WriteFile(configFile, []byte(configYAML), 0o600))
	configFlags := []string{"--config-file", configFile}

	runAssetWithArgs := func(name, asset string, extraArgs ...string) e2e.Task {
		args := append([]string{"run"}, configFlags...)
		args = append(args, extraArgs...)
		args = append(args, "--env", "default", filepath.Join(pipelines, asset))

		return e2e.Task{
			Name:    name,
			Command: binary,
			Args:    args,
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		}
	}

	runAsset := func(name, asset string) e2e.Task {
		return runAssetWithArgs(name, asset, "--full-refresh")
	}

	queryContains := func(name, query string, contains ...string) e2e.Task {
		return e2e.Task{
			Name:    name,
			Command: binary,
			Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", query),
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: contains,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		}
	}

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "connection-test",
			workflow: e2e.Workflow{
				Name: "connection-test",
				Steps: []e2e.Task{
					{
						Name:    "ping the Doris connection",
						Command: binary,
						Args:    append(append([]string{"connections", "test"}, configFlags...), "--name", connectionName),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
				},
			},
		},
		{
			name: "checks-and-custom-checks",
			workflow: e2e.Workflow{
				Name: "checks-and-custom-checks",
				Steps: []e2e.Task{
					runAsset("create products with column and custom checks", "checks-pipeline/assets/products.sql"),
					queryContains(
						"query the products table",
						"SELECT product_id, product_name FROM `bruin_test`.`products` ORDER BY product_id",
						"Laptop", "Smartphone", "Headphones", "Monitor",
					),
				},
			},
		},
		{
			name: "failing-check-fails-the-run",
			workflow: e2e.Workflow{
				Name: "failing-check-fails-the-run",
				Steps: []e2e.Task{
					{
						Name:    "run asset whose positive check must fail",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, "failing-check-pipeline/assets/bad_price.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
				},
			},
		},
		{
			name: "view",
			workflow: e2e.Workflow{
				Name: "view",
				Steps: []e2e.Task{
					runAsset("create metrics source table", "view-pipeline/assets/metrics_src.sql"),
					runAsset("create metrics view", "view-pipeline/assets/metrics_view.sql"),
					queryContains(
						"query the view",
						"SELECT metric_name FROM `bruin_test`.`metrics_view`",
						"visits",
					),
				},
			},
		},
		{
			name: "append-materialization",
			workflow: e2e.Workflow{
				Name: "append-materialization",
				Steps: []e2e.Task{
					runAsset("append: create initial table with full refresh", "append-pipeline/assets/events.sql"),
					queryContains(
						"append: query initial row count",
						"SELECT COUNT(*) AS row_count FROM `bruin_test`.`append_events`",
						"2",
					),
					runAssetWithArgs("append: run append strategy", "append-pipeline/assets/events.sql"),
					queryContains(
						"append: query appended row count",
						"SELECT COUNT(*) AS row_count FROM `bruin_test`.`append_events`",
						"4",
					),
					queryContains(
						"append: verify duplicate row was appended",
						"SELECT COUNT(*) AS event_one_count FROM `bruin_test`.`append_events` WHERE event_id = 1",
						"2",
					),
				},
			},
		},
		{
			name: "delete-insert-materialization",
			workflow: e2e.Workflow{
				Name: "delete-insert-materialization",
				Steps: []e2e.Task{
					runAsset("delete+insert: create initial table with full refresh", "delete-insert-initial-pipeline/assets/orders.sql"),
					runAssetWithArgs("delete+insert: run incremental replacement", "delete-insert-updated-pipeline/assets/orders.sql"),
					queryContains(
						"delete+insert: query final row count",
						"SELECT COUNT(*) AS row_count FROM `bruin_test`.`delete_insert_orders`",
						"3",
					),
					queryContains(
						"delete+insert: verify updated row",
						"SELECT order_status FROM `bruin_test`.`delete_insert_orders` WHERE order_id = 2",
						"updated",
					),
					queryContains(
						"delete+insert: verify old row was deleted",
						"SELECT COUNT(*) AS old_status_count FROM `bruin_test`.`delete_insert_orders` WHERE order_status = 'will-update'",
						"0",
					),
				},
			},
		},
		{
			name: "merge-materialization",
			workflow: e2e.Workflow{
				Name: "merge-materialization",
				Steps: []e2e.Task{
					runAsset("merge: create unique key table with full refresh", "merge-initial-pipeline/assets/accounts.sql"),
					queryContains(
						"merge: verify unique key table model",
						"SHOW CREATE TABLE `bruin_test`.`merge_accounts`",
						"UNIQUE KEY(`account_id`)",
					),
					runAssetWithArgs("merge: run native merge strategy", "merge-updated-pipeline/assets/accounts.sql"),
					queryContains(
						"merge: query final row count",
						"SELECT COUNT(*) AS row_count FROM `bruin_test`.`merge_accounts`",
						"3",
					),
					queryContains(
						"merge: verify updated row",
						"SELECT account_status, balance, update_count FROM `bruin_test`.`merge_accounts` WHERE account_id = 2",
						"updated", "30", "2",
					),
					queryContains(
						"merge: verify unchanged row",
						"SELECT account_status, balance, update_count FROM `bruin_test`.`merge_accounts` WHERE account_id = 1",
						"kept", "10", "1",
					),
					queryContains(
						"merge: verify inserted row",
						"SELECT account_status, balance, update_count FROM `bruin_test`.`merge_accounts` WHERE account_id = 3",
						"new", "40", "1",
					),
				},
			},
		},
		{
			name: "truncate-insert-materialization",
			workflow: e2e.Workflow{
				Name: "truncate-insert-materialization",
				Steps: []e2e.Task{
					runAsset("truncate+insert: create initial table with full refresh", "truncate-insert-initial-pipeline/assets/snapshots.sql"),
					runAssetWithArgs("truncate+insert: replace table contents", "truncate-insert-updated-pipeline/assets/snapshots.sql"),
					queryContains(
						"truncate+insert: query final row count",
						"SELECT COUNT(*) AS row_count FROM `bruin_test`.`truncate_insert_snapshots`",
						"2",
					),
					queryContains(
						"truncate+insert: verify old rows were removed",
						"SELECT COUNT(*) AS old_snapshot_count FROM `bruin_test`.`truncate_insert_snapshots` WHERE snapshot_name LIKE 'old-%'",
						"0",
					),
					queryContains(
						"truncate+insert: verify replacement rows were inserted",
						"SELECT snapshot_name FROM `bruin_test`.`truncate_insert_snapshots` ORDER BY snapshot_id",
						"replacement-one", "replacement-two",
					),
				},
			},
		},
		{
			name: "time-interval-materialization",
			workflow: e2e.Workflow{
				Name: "time-interval-materialization",
				Steps: []e2e.Task{
					runAssetWithArgs(
						"time_interval: create initial table with full refresh",
						"time-interval-initial-pipeline/assets/events.sql",
						"--full-refresh", "--start-date", "2024-01-01", "--end-date", "2024-01-31",
					),
					runAssetWithArgs(
						"time_interval: replace the selected date range",
						"time-interval-updated-pipeline/assets/events.sql",
						"--start-date", "2024-01-15", "--end-date", "2024-01-18",
					),
					queryContains(
						"time_interval: query final row count",
						"SELECT COUNT(*) AS row_count FROM `bruin_test`.`time_interval_events`",
						"4",
					),
					queryContains(
						"time_interval: verify old interval rows were deleted",
						"SELECT COUNT(*) AS old_middle_count FROM `bruin_test`.`time_interval_events` WHERE event_name = 'old-middle'",
						"0",
					),
					queryContains(
						"time_interval: verify query respected interval variables",
						"SELECT COUNT(*) AS outside_filtered_count FROM `bruin_test`.`time_interval_events` WHERE event_name = 'outside-filtered'",
						"0",
					),
					queryContains(
						"time_interval: verify final rows",
						"SELECT event_name FROM `bruin_test`.`time_interval_events` ORDER BY dt, event_id",
						"old-before", "updated-middle", "new-middle", "old-after",
					),
				},
			},
		},
		{
			name: "ddl",
			workflow: e2e.Workflow{
				Name: "ddl",
				Steps: []e2e.Task{
					runAsset("create empty table from a column definition", "ddl-pipeline/assets/ddl_table.sql"),
				},
			},
		},
		{
			name: "seed",
			workflow: e2e.Workflow{
				Name: "seed",
				Steps: []e2e.Task{
					runAsset("load csv seed", "seed-pipeline/assets/seed.asset.yml"),
					queryContains(
						"query seeded table",
						"SELECT name FROM `bruin_test`.`seed_contacts` ORDER BY name",
						"Ada", "Grace",
					),
					queryContains(
						"query seeded backslash value",
						"SELECT note FROM `bruin_test`.`seed_contacts` WHERE name = 'Ada'",
						`C:\tmp`,
					),
					queryContains(
						"query seeded empty string value",
						"SELECT CASE WHEN note = '' THEN 'empty-string' ELSE note END AS note_value FROM `bruin_test`.`seed_contacts` WHERE name = 'Grace'",
						"empty-string",
					),
				},
			},
		},
		{
			name: "schema-auto-create",
			workflow: e2e.Workflow{
				Name: "schema-auto-create",
				Steps: []e2e.Task{
					runAsset("materialize into a non-existent schema", "schema-create-pipeline/assets/widget.sql"),
					queryContains(
						"query the auto-created table",
						"SELECT widget_name FROM `bruin_auto`.`widget`",
						"gizmo",
					),
				},
			},
		},
		{
			name: "query-sensor",
			workflow: e2e.Workflow{
				Name: "query-sensor",
				Steps: []e2e.Task{
					runAsset("seed the sensor table", "query-sensor-pipeline/assets/sensor_table.sql"),
					runAsset("run the query sensor", "query-sensor-pipeline/assets/sensor.sql"),
				},
			},
		},
		{
			name: "table-sensor",
			workflow: e2e.Workflow{
				Name: "table-sensor",
				Steps: []e2e.Task{
					runAsset("seed the table sensor table", "table-sensor-pipeline/assets/sensor_table.sql"),
					runAsset("run the table sensor", "table-sensor-pipeline/assets/table_sensor.sql"),
				},
			},
		},
		{
			name: "source-asset",
			workflow: e2e.Workflow{
				Name: "source-asset",
				Steps: []e2e.Task{
					runAsset("run Doris source metadata asset", "source-pipeline/assets/source.asset.yml"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.workflow.Run(); err != nil {
				t.Fatalf("workflow %q failed: %v", tt.name, err)
			}
		})
	}
}

func waitForDorisSQL(t *testing.T, host, port string) {
	t.Helper()

	dsn := fmt.Sprintf("root:@tcp(%s:%s)/?multiStatements=true&parseTime=true", host, port)
	deadline := time.Now().Add(8 * time.Minute)
	var lastErr error

	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var aliveBackend bool
			aliveBackend, err = hasAliveDorisBackend(ctx, db)
			if err == nil && aliveBackend {
				_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS bruin_test")
			}
			cancel()
			_ = db.Close()
			if err == nil && aliveBackend {
				return
			}
		}
		lastErr = err
		time.Sleep(3 * time.Second)
	}

	t.Fatalf("Doris SQL endpoint did not become ready: %v", lastErr)
}

func hasAliveDorisBackend(ctx context.Context, db *sql.DB) (bool, error) {
	rows, err := db.QueryContext(ctx, "SHOW BACKENDS")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return false, err
	}

	aliveColumn := -1
	for i, column := range columns {
		if strings.EqualFold(column, "Alive") {
			aliveColumn = i
			break
		}
	}
	if aliveColumn == -1 {
		return false, fmt.Errorf("SHOW BACKENDS did not include an Alive column")
	}

	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return false, err
		}

		alive := strings.TrimSpace(values[aliveColumn].String)
		if strings.EqualFold(alive, "true") || alive == "1" {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	return false, fmt.Errorf("no alive Doris backend reported by SHOW BACKENDS")
}
