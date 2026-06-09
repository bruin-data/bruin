package sail

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const connectionName = "sail-default"

// TestSailWorkflows boots a real Sail Arrow Flight SQL server (built from the
// Dockerfile in this directory) via testcontainers and runs core bruin
// workflows against it. It is skipped when no Docker provider is available.
func TestSailWorkflows(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    ".",
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			ExposedPorts: []string{"32010/tcp"},
			WaitingFor:   wait.ForListeningPort("32010/tcp").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err, "failed to start the Sail container")
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(ctx, "32010/tcp")
	require.NoError(t, err)

	currentFolder, err := os.Getwd()
	require.NoError(t, err)
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")
	pipelines := filepath.Join(currentFolder, "test-pipelines")

	// Point a temp .bruin.yml at the container's mapped Flight SQL port.
	configFile := filepath.Join(t.TempDir(), ".bruin.yml")
	configYAML := fmt.Sprintf(`default_environment: default
environments:
    default:
        connections:
            sail:
                - name: %s
                  host: %s
                  port: %s
`, connectionName, host, mappedPort.Port())
	require.NoError(t, os.WriteFile(configFile, []byte(configYAML), 0o600))
	configFlags := []string{"--config-file", configFile}

	runAsset := func(name, asset string) e2e.Task {
		return e2e.Task{
			Name:    name,
			Command: binary,
			Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, asset)),
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		}
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
						Name:    "ping the sail connection",
						Command: binary,
						// --config-file is a flag on the `test` subcommand, so it
						// must come after "test" (unlike run/query where it sits
						// right after the subcommand).
						Args: append(append([]string{"connections", "test"}, configFlags...), "--name", connectionName),
						Env:  []string{},
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
			name: "ddl",
			workflow: e2e.Workflow{
				Name: "ddl",
				Steps: []e2e.Task{
					runAsset("create empty table from a column definition", "ddl-pipeline/assets/ddl_table.sql"),
				},
			},
		},
		{
			name: "schema-auto-create",
			workflow: e2e.Workflow{
				Name: "schema-auto-create",
				Steps: []e2e.Task{
					// bruin_auto does not exist on a fresh server; the run must
					// create the schema before materializing the table.
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.workflow.Run(); err != nil {
				t.Fatalf("workflow %q failed: %v", tt.name, err)
			}
		})
	}
}
