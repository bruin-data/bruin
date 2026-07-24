package spark

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	connectionName = "spark-default"
	minioAccessKey = "admin"
	minioBucket    = "bruin-test"
	minioSecretKey = "password"
)

func TestSparkWorkflows(t *testing.T) {
	configureDockerHost(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	testNetwork, err := network.New(ctx)
	require.NoError(t, err, "failed to create the Spark test network")
	t.Cleanup(func() {
		_ = testNetwork.Remove(context.Background())
	})

	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "minio/minio:RELEASE.2025-09-07T16-13-09Z",
			Cmd:          []string{"server", "/data", "--console-address", ":9001"},
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     minioAccessKey,
				"MINIO_ROOT_PASSWORD": minioSecretKey,
			},
			Networks:       []string{testNetwork.Name},
			NetworkAliases: map[string][]string{testNetwork.Name: {"minio"}},
			WaitingFor: wait.ForHTTP("/minio/health/ready").
				WithPort("9000/tcp").
				WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err, "failed to start MinIO")
	t.Cleanup(func() {
		_ = minioContainer.Terminate(context.Background())
	})

	minioHost, err := minioContainer.Host(ctx)
	require.NoError(t, err)
	minioPort, err := minioContainer.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	minioEndpoint := fmt.Sprintf("http://%s:%s", minioHost, minioPort.Port())
	createBucket(t, ctx, minioEndpoint)

	sparkRequest := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    ".",
			Dockerfile: "Dockerfile",
			KeepImage:  true,
		},
		ExposedPorts: []string{"15002/tcp"},
		Networks:     []string{testNetwork.Name},
		WaitingFor:   wait.ForListeningPort("15002/tcp").WithStartupTimeout(6 * time.Minute),
	}
	if image := os.Getenv("SPARK_TEST_IMAGE"); image != "" {
		sparkRequest.Image = image
		sparkRequest.FromDockerfile = testcontainers.FromDockerfile{}
	}
	sparkContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: sparkRequest,
		Started:          true,
	})
	require.NoError(t, err, "failed to start Spark Connect")
	t.Cleanup(func() {
		_ = sparkContainer.Terminate(context.Background())
	})

	sparkHost, err := sparkContainer.Host(ctx)
	require.NoError(t, err)
	sparkPort, err := sparkContainer.MappedPort(ctx, "15002/tcp")
	require.NoError(t, err)

	currentFolder, err := os.Getwd()
	require.NoError(t, err)
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")
	pipelines := filepath.Join(currentFolder, "test-pipelines")
	require.FileExists(t, binary, "build Bruin before running the Spark integration suite")

	configFile := filepath.Join(t.TempDir(), ".bruin.yml")
	configYAML := fmt.Sprintf(`default_environment: default
environments:
    default:
        connections:
            spark:
                - name: %s
                  uri: "spark://%s:%s?auth_type=none&api=connect"
                  catalog: local
                  ingest_staging_area: "s3://%s/staging"
                  options:
                    spark.ingest.s3.base_endpoint: "%s"
                    spark.ingest.s3.use_path_style: "true"
`, connectionName, sparkHost, sparkPort.Port(), minioBucket, minioEndpoint)
	require.NoError(t, os.WriteFile(configFile, []byte(configYAML), 0o600))

	configFlags := []string{"--config-file", configFile}
	taskEnv := []string{
		"AWS_ACCESS_KEY_ID=" + minioAccessKey,
		"AWS_SECRET_ACCESS_KEY=" + minioSecretKey,
		"AWS_REGION=us-east-1",
		"AWS_EC2_METADATA_DISABLED=true",
	}

	runAssetWithArgs := func(name, asset string, extraArgs ...string) e2e.Task {
		args := append([]string{"run"}, configFlags...)
		args = append(args, extraArgs...)
		args = append(args, "--env", "default", filepath.Join(pipelines, asset))
		return successfulTask(name, binary, args, taskEnv)
	}
	runAsset := func(name, asset string) e2e.Task {
		return runAssetWithArgs(name, asset, "--full-refresh")
	}
	queryContains := func(name, query string, contains ...string) e2e.Task {
		task := successfulTask(
			name,
			binary,
			append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", query),
			taskEnv,
		)
		task.Expected.Contains = contains
		task.Asserts = append(task.Asserts, e2e.AssertByContains)
		return task
	}

	t.Run("connection", func(t *testing.T) {
		task := successfulTask(
			"test the Spark connection",
			binary,
			append(append([]string{"connections", "test"}, configFlags...), "--name", connectionName),
			taskEnv,
		)
		task.Retries = 5
		runTask(t, &task)
	})

	t.Run("query-command", func(t *testing.T) {
		task := queryContains("run an ad hoc Spark query", "SELECT 'spark-ready' AS status", "spark-ready")
		runTask(t, &task)
	})

	t.Run("checks-and-custom-checks", func(t *testing.T) {
		runTask(t, taskPointer(runAssetWithArgs(
			"create products and run annotated checks",
			"core-pipeline/assets/products.sql",
			"--full-refresh", "--query-annotations", "default",
		)))
		task := queryContains(
			"query checked products",
			"SELECT product_name FROM local.bruin_test.products ORDER BY product_id",
			"Laptop", "Smartphone", "Headphones", "Monitor",
		)
		runTask(t, &task)
		layoutTask := queryContains(
			"verify create-replace partitioning and clustering",
			"SHOW CREATE TABLE local.bruin_test.products",
			"PARTITIONED BY (category)", "sort-order", "product_id ASC",
		)
		runTask(t, &layoutTask)
	})

	t.Run("failing-check", func(t *testing.T) {
		task := e2e.Task{
			Name:    "fail an asset with an invalid positive check",
			Command: binary,
			Args: append(
				append([]string{"run"}, configFlags...),
				"--full-refresh",
				"--env",
				"default",
				filepath.Join(pipelines, "core-pipeline/assets/bad_price.sql"),
			),
			Env: taskEnv,
			Expected: e2e.Output{
				ExitCode: 1,
			},
			Asserts: []func(*e2e.Task) error{e2e.AssertByExitCode},
		}
		runTask(t, &task)
	})

	t.Run("view", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("create the view source", "core-pipeline/assets/metrics_src.sql")))
		runTask(t, taskPointer(runAsset("create the Spark view", "core-pipeline/assets/metrics_view.sql")))
		task := queryContains(
			"query the Spark view",
			"SELECT metric_name FROM spark_catalog.bruin_view.metrics_view ORDER BY metric_name",
			"visits",
		)
		runTask(t, &task)
	})

	t.Run("append-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("create the append target", "core-pipeline/assets/append_events.sql")))
		runTask(t, taskPointer(runAssetWithArgs("append another batch", "core-pipeline/assets/append_events.sql")))
		task := queryContains(
			"verify append rows",
			"SELECT COUNT(*) AS row_count FROM local.bruin_test.append_events",
			"4",
		)
		runTask(t, &task)
	})

	t.Run("delete-insert-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAsset(
			"create the delete-insert target",
			"delete-insert-initial-pipeline/assets/orders.sql",
		)))
		runTask(t, taskPointer(runAssetWithArgs(
			"replace matching incremental keys",
			"delete-insert-updated-pipeline/assets/orders.sql",
		)))
		task := queryContains(
			"verify delete-insert rows",
			"SELECT order_status FROM local.bruin_test.delete_insert_orders ORDER BY order_id",
			"kept", "updated", "new",
		)
		runTask(t, &task)
	})

	t.Run("truncate-insert-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAsset(
			"create the truncate-insert target",
			"truncate-insert-initial-pipeline/assets/snapshots.sql",
		)))
		runTask(t, taskPointer(runAssetWithArgs(
			"replace the truncate-insert target",
			"truncate-insert-updated-pipeline/assets/snapshots.sql",
		)))
		task := queryContains(
			"verify truncate-insert rows",
			"SELECT snapshot_name FROM local.bruin_test.truncate_insert_snapshots ORDER BY snapshot_id",
			"replacement-one", "replacement-two",
		)
		runTask(t, &task)
	})

	t.Run("time-interval-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAssetWithArgs(
			"create the time interval target",
			"time-interval-initial-pipeline/assets/events.sql",
			"--full-refresh", "--start-date", "2024-01-01", "--end-date", "2024-01-31",
		)))
		runTask(t, taskPointer(runAssetWithArgs(
			"replace a bounded time interval",
			"time-interval-updated-pipeline/assets/events.sql",
			"--start-date", "2024-01-15", "--end-date", "2024-01-18",
		)))
		task := queryContains(
			"verify time interval rows",
			"SELECT event_name FROM local.bruin_test.time_interval_events ORDER BY dt, event_id",
			"old-before", "updated-middle", "new-middle", "old-after",
		)
		runTask(t, &task)
	})

	t.Run("merge-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAsset(
			"create the merge target",
			"merge-initial-pipeline/assets/accounts.sql",
		)))
		runTask(t, taskPointer(runAssetWithArgs(
			"merge updates and inserts",
			"merge-updated-pipeline/assets/accounts.sql",
		)))
		task := queryContains(
			"verify merged rows and column update rules",
			`SELECT CASE WHEN
				(SELECT COUNT(*) FROM local.bruin_test.merge_accounts) = 3
				AND (SELECT COUNT(*) FROM local.bruin_test.merge_accounts
					WHERE account_id = 1 AND account_name = 'Alice' AND score = 10 AND note = 'initial-one') = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.merge_accounts
					WHERE account_id = 2 AND account_name = 'Bobby' AND score = 20 AND note = 'initial-two') = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.merge_accounts
					WHERE account_id = 3 AND account_name = 'Cara' AND score = 30 AND note = 'inserted-three') = 1
				THEN 'merge-ok' ELSE 'merge-failed' END AS status`,
			"merge-ok",
		)
		runTask(t, &task)
	})

	t.Run("scd2-by-column-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAsset(
			"create the SCD2-by-column target",
			"scd2-column-initial-pipeline/assets/customers.sql",
		)))
		runTask(t, taskPointer(runAssetWithArgs(
			"apply SCD2-by-column changes",
			"scd2-column-updated-pipeline/assets/customers.sql",
		)))
		task := queryContains(
			"verify SCD2-by-column history",
			`SELECT CASE WHEN
				(SELECT COUNT(*) FROM local.bruin_test.scd2_column_customers) = 5
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_column_customers
					WHERE customer_id = 1 AND tier = 'bronze' AND NOT _is_current
					AND _valid_until = TIMESTAMP '2026-02-01 00:00:00') = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_column_customers
					WHERE customer_id = 1 AND tier = 'gold' AND _is_current
					AND _valid_from = TIMESTAMP '2026-02-01 00:00:00') = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_column_customers
					WHERE customer_id = 2 AND tier = 'silver' AND _is_current) = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_column_customers
					WHERE customer_id = 3 AND NOT _is_current
					AND _valid_until < TIMESTAMP '9999-12-31 00:00:00') = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_column_customers
					WHERE customer_id = 4 AND _is_current) = 1
				THEN 'scd2-column-ok' ELSE 'scd2-column-failed' END AS status`,
			"scd2-column-ok",
		)
		runTask(t, &task)
		layoutTask := queryContains(
			"verify default SCD2 partitioning and clustering",
			"SHOW CREATE TABLE local.bruin_test.scd2_column_customers",
			"PARTITIONED BY (days(_valid_from))",
			"sort-order",
			"_is_current ASC NULLS FIRST, customer_id ASC",
		)
		runTask(t, &layoutTask)
	})

	t.Run("scd2-by-time-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAsset(
			"create the SCD2-by-time target",
			"scd2-time-initial-pipeline/assets/inventory.sql",
		)))
		runTask(t, taskPointer(runAssetWithArgs(
			"apply SCD2-by-time changes",
			"scd2-time-updated-pipeline/assets/inventory.sql",
		)))
		task := queryContains(
			"verify SCD2-by-time history",
			`SELECT CASE WHEN
				(SELECT COUNT(*) FROM local.bruin_test.scd2_time_inventory) = 5
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_time_inventory
					WHERE item_id = 1 AND quantity = 10 AND NOT _is_current
					AND _valid_until = TIMESTAMP '2026-04-01 00:00:00') = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_time_inventory
					WHERE item_id = 1 AND quantity = 15 AND _is_current
					AND _valid_from = TIMESTAMP '2026-04-01 00:00:00') = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_time_inventory
					WHERE item_id = 2 AND quantity = 20 AND _is_current) = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_time_inventory
					WHERE item_id = 2) = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_time_inventory
					WHERE item_id = 3 AND NOT _is_current) = 1
				AND (SELECT COUNT(*) FROM local.bruin_test.scd2_time_inventory
					WHERE item_id = 4 AND quantity = 40 AND _is_current) = 1
				THEN 'scd2-time-ok' ELSE 'scd2-time-failed' END AS status`,
			"scd2-time-ok",
		)
		runTask(t, &task)
		layoutTask := queryContains(
			"verify custom SCD2 partitioning and clustering",
			"SHOW CREATE TABLE local.bruin_test.scd2_time_inventory",
			"PARTITIONED BY (days(changed_at))",
			"sort-order",
			"warehouse ASC NULLS FIRST, item_id ASC",
		)
		runTask(t, &layoutTask)
	})

	t.Run("ddl-materialization", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("create a table from column DDL", "core-pipeline/assets/ddl_table.sql")))
		task := queryContains(
			"describe the DDL table",
			"DESCRIBE local.bruin_test.ddl_table",
			"id", "name", "created_at",
		)
		runTask(t, &task)
		layoutTask := queryContains(
			"verify DDL partitioning and clustering",
			"SHOW CREATE TABLE local.bruin_test.ddl_table",
			"PARTITIONED BY (days(created_at))",
			"sort-order",
			"name ASC NULLS FIRST, id ASC",
		)
		runTask(t, &layoutTask)
	})

	t.Run("seed", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("ingest a typed CSV seed", "core-pipeline/assets/seed.asset.yml")))
		task := queryContains(
			"query the seed",
			"SELECT name, age, joined_on FROM local.bruin_test.seed_contacts ORDER BY name",
			"Ada", "Grace", "36", "42", "2015-07-14", "2018-02-03",
		)
		runTask(t, &task)
	})

	t.Run("schema-auto-create", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("create a missing namespace", "core-pipeline/assets/widget.sql")))
		task := queryContains(
			"query the auto-created namespace",
			"SELECT widget_name FROM local.bruin_auto.widget",
			"gizmo",
		)
		runTask(t, &task)
	})

	t.Run("query-sensor", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("create query sensor input", "core-pipeline/assets/query_sensor_table.sql")))
		runTask(t, taskPointer(runAsset("run the query sensor", "core-pipeline/assets/query_sensor.sql")))
	})

	t.Run("table-sensor", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("create table sensor input", "core-pipeline/assets/table_sensor_table.sql")))
		runTask(t, taskPointer(runAsset("run the table sensor", "core-pipeline/assets/table_sensor.sql")))
	})

	t.Run("source-asset", func(t *testing.T) {
		runTask(t, taskPointer(runAsset("create an externally managed table", "core-pipeline/assets/external_source_builder.sql")))
		runTask(t, taskPointer(runAsset("check a Spark source asset", "core-pipeline/assets/external_source.asset.yml")))
	})

	t.Run("import-metadata", func(t *testing.T) {
		importPipeline := t.TempDir()
		require.NoError(t, os.WriteFile(
			filepath.Join(importPipeline, "pipeline.yml"),
			[]byte("name: spark-import-pipeline\n"),
			0o600,
		))
		gitInit := exec.CommandContext(ctx, "git", "init", "-q", importPipeline)
		require.NoError(t, gitInit.Run(), "failed to initialize the temporary import repository")
		task := successfulTask(
			"import Spark table metadata",
			binary,
			[]string{
				"import", "database",
				"--config-file", configFile,
				"--connection", connectionName,
				"--environment", "default",
				"--schema", "bruin_test",
				importPipeline,
			},
			taskEnv,
		)
		task.Asserts = append(task.Asserts, func(_ *e2e.Task) error {
			content, err := os.ReadFile(filepath.Join(
				importPipeline,
				"assets",
				"bruin_test",
				"products.asset.yml",
			))
			if err != nil {
				return fmt.Errorf("read imported Spark asset: %w", err)
			}
			imported := string(content)
			for _, expected := range []string{
				"name: bruin_test.products",
				"type: spark.source",
				"name: product_id",
				"name: product_name",
			} {
				if !strings.Contains(imported, expected) {
					return fmt.Errorf("imported Spark asset does not contain %q:\n%s", expected, imported)
				}
			}
			return nil
		})
		runTask(t, &task)
	})
}

func configureDockerHost(t *testing.T) {
	t.Helper()

	if os.Getenv("DOCKER_HOST") != "" {
		return
	}

	output, err := exec.CommandContext(
		context.Background(),
		"docker",
		"context",
		"inspect",
		"--format",
		"{{.Endpoints.docker.Host}}",
	).Output()
	if err != nil {
		return
	}

	if dockerHost := strings.TrimSpace(string(output)); dockerHost != "" {
		t.Setenv("DOCKER_HOST", dockerHost)
	}
}

func createBucket(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			minioAccessKey,
			minioSecretKey,
			"",
		)),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(minioBucket)})
	require.NoError(t, err, "failed to create the MinIO staging bucket")
}

func successfulTask(name, command string, args, env []string) e2e.Task {
	return e2e.Task{
		Name:    name,
		Command: command,
		Args:    args,
		Env:     env,
		Expected: e2e.Output{
			ExitCode: 0,
		},
		Asserts: []func(*e2e.Task) error{e2e.AssertByExitCode},
	}
}

func runTask(t *testing.T, task *e2e.Task) {
	t.Helper()
	if err := task.Run(); err != nil {
		t.Fatal(err)
	}
}

func taskPointer(task e2e.Task) *e2e.Task {
	return &task
}
