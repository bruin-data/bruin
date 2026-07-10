package mssql_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

const (
	mssqlImage    = "mcr.microsoft.com/mssql/server@sha256:49b45a911dc535e9345fbfd7101a1bd8a1e190a5f29b877ef75387a061e5fcf0"
	mssqlPassword = "BruinLocal2333!Pass"
)

// TestDeveloperEnvironmentCrossDatabaseReference protects the regression from
// https://github.com/bruin-data/bruin/issues/2333. The asset connects to Gold_WH
// but reads a three-part Silver_WH.schema.table reference.
//
//nolint:paralleltest,tparallel
func TestDeveloperEnvironmentCrossDatabaseReference(t *testing.T) {
	host, port := startSQLServer(t)

	master := openSQLServer(t, host, port, "master")
	execSQL(t, master, "CREATE DATABASE [Silver_WH]")
	execSQL(t, master, "CREATE DATABASE [Gold_WH]")

	silver := openSQLServer(t, host, port, "Silver_WH")
	gold := openSQLServer(t, host, port, "Gold_WH")
	seedCrossDatabaseScenario(t, silver, gold)

	configPath, assetPath := writeTestPipeline(t, host, port)
	binary := bruinBinary(t)

	t.Run("rewrites when the prefixed upstream table exists", func(t *testing.T) {
		output := runBruin(t, binary, configPath, assetPath)
		require.Contains(t, output, "Silver_WH.dev_myschema.upstream")
		require.Equal(t, "silver-development", queryMarker(t, gold))
	})

	t.Run("does not trust a matching table in the current database", func(t *testing.T) {
		execSQL(t, silver, "DROP TABLE dev_myschema.upstream")

		output := runBruin(t, binary, configPath, assetPath)
		require.Contains(t, output, "Silver_WH.myschema.upstream")
		require.NotContains(t, output, "Silver_WH.dev_myschema.upstream")
		require.Equal(t, "silver-production", queryMarker(t, gold))
		require.Equal(t, "gold-decoy", queryString(t, gold, "SELECT source_marker FROM dev_myschema.upstream"))
	})

	t.Run("does not rewrite when the prefixed upstream schema is absent", func(t *testing.T) {
		execSQL(t, silver, "DROP SCHEMA dev_myschema")

		output := runBruin(t, binary, configPath, assetPath)
		require.Contains(t, output, "Silver_WH.myschema.upstream")
		require.NotContains(t, output, "Silver_WH.dev_myschema.upstream")
		require.Equal(t, "silver-production", queryMarker(t, gold))
	})

	t.Run("resumes rewriting when the prefixed upstream table is restored", func(t *testing.T) {
		execSQL(t, silver, "CREATE SCHEMA dev_myschema")
		execSQL(t, silver, "CREATE TABLE dev_myschema.upstream (source_marker nvarchar(100) NOT NULL)")
		execSQL(t, silver, "INSERT INTO dev_myschema.upstream VALUES (N'silver-development-restored')")

		output := runBruin(t, binary, configPath, assetPath)
		require.Contains(t, output, "Silver_WH.dev_myschema.upstream")
		require.Equal(t, "silver-development-restored", queryMarker(t, gold))
	})
}

func startSQLServer(t *testing.T) (string, string) {
	t.Helper()

	_, err := exec.LookPath("docker")
	require.NoError(t, err, "Docker is required for the MSSQL integration test")

	containerName := fmt.Sprintf("bruin-mssql-devenv-%d", time.Now().UnixNano())
	image := os.Getenv("BRUIN_MSSQL_TEST_IMAGE")
	if image == "" {
		image = mssqlImage
	}

	args := []string{
		"run", "-d", "--rm",
		"--platform", "linux/amd64",
		"--name", containerName,
		"-e", "ACCEPT_EULA=Y",
		"-e", "MSSQL_SA_PASSWORD=" + mssqlPassword,
		"-p", "127.0.0.1::1433",
		image,
	}
	output, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput()
	require.NoError(t, err, "failed to start SQL Server container: %s", output)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupOutput, cleanupErr := exec.CommandContext(cleanupCtx, "docker", "rm", "-f", containerName).CombinedOutput()
		if cleanupErr != nil && !strings.Contains(string(cleanupOutput), "No such container") {
			t.Logf("failed to remove SQL Server container: %v: %s", cleanupErr, cleanupOutput)
		}
	})

	portOutput, err := exec.CommandContext(t.Context(), "docker", "port", containerName, "1433/tcp").CombinedOutput()
	require.NoError(t, err, "failed to read SQL Server container port: %s", portOutput)

	host, port, err := net.SplitHostPort(strings.TrimSpace(string(portOutput)))
	require.NoError(t, err)

	waitForSQLServer(t, host, port, containerName)

	return host, port
}

func waitForSQLServer(t *testing.T, host, port, containerName string) {
	t.Helper()

	db, err := sql.Open("sqlserver", sqlServerDSN(host, port, "master"))
	require.NoError(t, err)
	defer db.Close()

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(time.Second)
	}

	logs, logsErr := exec.CommandContext(t.Context(), "docker", "logs", containerName).CombinedOutput()
	if logsErr != nil {
		t.Logf("failed to read SQL Server logs: %v", logsErr)
	}
	t.Fatalf("SQL Server did not become ready: %v\n%s", err, logs)
}

func openSQLServer(t *testing.T, host, port, database string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlserver", sqlServerDSN(host, port, database))
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return db
}

func sqlServerDSN(host, port, database string) string {
	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword("sa", mssqlPassword),
		Host:   net.JoinHostPort(host, port),
	}
	query := u.Query()
	query.Set("database", database)
	query.Set("encrypt", "disable")
	query.Set("TrustServerCertificate", "true")
	u.RawQuery = query.Encode()

	return u.String()
}

func seedCrossDatabaseScenario(t *testing.T, silver, gold *sql.DB) {
	t.Helper()

	execSQL(t, silver, "CREATE SCHEMA myschema")
	execSQL(t, silver, "CREATE SCHEMA dev_myschema")
	execSQL(t, silver, "CREATE TABLE myschema.upstream (source_marker nvarchar(100) NOT NULL)")
	execSQL(t, silver, "INSERT INTO myschema.upstream VALUES (N'silver-production')")
	execSQL(t, silver, "CREATE TABLE dev_myschema.upstream (source_marker nvarchar(100) NOT NULL)")
	execSQL(t, silver, "INSERT INTO dev_myschema.upstream VALUES (N'silver-development')")

	execSQL(t, gold, "CREATE SCHEMA dev_gold")
	execSQL(t, gold, "CREATE SCHEMA dev_myschema")
	execSQL(t, gold, "CREATE TABLE dev_myschema.upstream (source_marker nvarchar(100) NOT NULL)")
	execSQL(t, gold, "INSERT INTO dev_myschema.upstream VALUES (N'gold-decoy')")
}

func writeTestPipeline(t *testing.T, host, port string) (string, string) {
	t.Helper()

	root := t.TempDir()
	assetsDir := filepath.Join(root, "assets")
	require.NoError(t, os.MkdirAll(assetsDir, 0o755))

	config := fmt.Sprintf(`default_environment: dev
environments:
  dev:
    schema_prefix: dev_
    connections:
      mssql:
        - name: mssql-gold
          username: sa
          password: %q
          host: %s
          port: %s
          database: Gold_WH
          options: "encrypt=disable&TrustServerCertificate=true"
`, mssqlPassword, host, port)
	pipeline := `name: issue-2333-mssql
default_connections:
  mssql: mssql-gold
`
	asset := `/* @bruin
name: gold.reader
type: ms.sql

materialization:
  type: table
@bruin */

SELECT source_marker
FROM Silver_WH.myschema.upstream;
`

	configPath := filepath.Join(root, ".bruin.yml")
	assetPath := filepath.Join(assetsDir, "reader.sql")
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "pipeline.yml"), []byte(pipeline), 0o600))
	require.NoError(t, os.WriteFile(assetPath, []byte(asset), 0o600))

	gitOutput, err := exec.CommandContext(t.Context(), "git", "init", root).CombinedOutput()
	require.NoError(t, err, "failed to initialize temporary pipeline repository: %s", gitOutput)

	return configPath, assetPath
}

func bruinBinary(t *testing.T) string {
	t.Helper()

	if binary := os.Getenv("BRUIN_TEST_BINARY"); binary != "" {
		require.FileExists(t, binary)
		return binary
	}

	workingDirectory, err := os.Getwd()
	require.NoError(t, err)
	repositoryRoot, err := filepath.Abs(filepath.Join(workingDirectory, "../.."))
	require.NoError(t, err)

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}

	binary := filepath.Join(repositoryRoot, "bin", executable)
	require.FileExists(t, binary)

	return binary
}

func runBruin(t *testing.T, binary, configPath, assetPath string) string {
	t.Helper()

	args := []string{
		"run",
		"--config-file", configPath,
		"--env", "dev",
		"--full-refresh",
		"--workers", "1",
		"--verbose",
		"--no-log-file",
		"--no-color",
		assetPath,
	}
	cmd := exec.CommandContext(t.Context(), binary, args...)
	cmd.Env = append(os.Environ(), "DISABLE_TELEMETRY=true", "INGESTR_DISABLE_TELEMETRY=true")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Bruin run failed:\n%s", output)

	return string(output)
}

func execSQL(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), statement)
	require.NoError(t, err, "SQL statement failed: %s", statement)
}

func queryMarker(t *testing.T, db *sql.DB) string {
	t.Helper()
	return queryString(t, db, "SELECT source_marker FROM dev_gold.reader")
}

func queryString(t *testing.T, db *sql.DB, statement string) string {
	t.Helper()
	var result string
	require.NoError(t, db.QueryRowContext(t.Context(), statement).Scan(&result))
	return result
}
