//go:build !bruin_no_duckdb

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestRenderCommandStdinRendersWithEnvironment(t *testing.T) { //nolint:paralleltest // uses t.Chdir
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, ".git"), 0o755))
	dbPath := filepath.ToSlash(filepath.Join(dir, "test.duckdb"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(fmt.Sprintf(`
environments:
  default:
    connections:
      duckdb:
        - name: local
          path: '%s'
  dev:
    schema_prefix: jane_
    connections:
      duckdb:
        - name: local
          path: '%s'
`, dbPath, dbPath)), 0o600))
	t.Chdir(dir)

	command := Render()
	command.Reader = bytes.NewBufferString("SELECT '{{ schema_prefix }}' AS prefix")
	output := &bytes.Buffer{}
	command.Writer = output
	command.ExitErrHandler = func(context.Context, *cli.Command, error) {}

	err := command.Run(context.Background(), []string{
		"render", "--stdin", "--connection", "local", "--raw-query", "--output", "json", "--environment", "dev",
	})
	require.NoError(t, err)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(output.Bytes(), &payload))
	assert.Equal(t, "SELECT 'jane_' AS prefix", payload["query"])
}

func TestRenderCommandStdinConnectionAndEnvironmentErrors(t *testing.T) { //nolint:paralleltest // uses t.Chdir
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, ".git"), 0o755))
	dbPath := filepath.ToSlash(filepath.Join(dir, "test.duckdb"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(fmt.Sprintf(`
environments:
  default:
    connections:
      duckdb:
        - name: local
          path: '%s'
`, dbPath)), 0o600))
	t.Chdir(dir)

	run := func(args ...string) error {
		command := Render()
		command.Reader = bytes.NewBufferString("SELECT 1")
		command.Writer = &bytes.Buffer{}
		command.ExitErrHandler = func(context.Context, *cli.Command, error) {}
		return command.Run(context.Background(), args)
	}

	require.Error(t, run("render", "--stdin", "--output", "json"))
	require.Error(t, run("render", "--stdin", "--connection", "missing", "--output", "json"))
	require.Error(t, run("render", "--stdin", "--connection", "local", "--environment", "missing", "--output", "json"))
}
