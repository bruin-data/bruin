package connection

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOnePasswordCLI puts a stand-in op on PATH that serves items keyed "<vault>/<item>" and logs each read.
func fakeOnePasswordCLI(t *testing.T, items map[string]string) func() []string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the stand-in op CLI is a shell script")
	}

	dir := t.TempDir()
	calls := filepath.Join(dir, "calls.log")
	var script strings.Builder
	script.WriteString("#!/bin/sh\n")
	script.WriteString(`echo "$5/$3" >> '` + calls + "'\n")
	script.WriteString(`case "$5/$3" in` + "\n")
	for key, item := range items {
		script.WriteString("  '" + key + "') echo '" + item + "' ;;\n")
	}
	script.WriteString(`  *) echo "\"$3\" isn't an item in the \"$5\" vault" >&2; exit 1 ;;` + "\n")
	script.WriteString("esac\n")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "op"), []byte(script.String()), 0o755)) //nolint:gosec // the stand-in CLI has to be executable

	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return func() []string {
		buf, err := os.ReadFile(calls)
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
		return strings.Fields(string(buf))
	}
}

func loadVaultConfig(t *testing.T, content string) *config.Config {
	t.Helper()
	fs := afero.NewMemMapFs()
	require.NoError(t, afero.WriteFile(fs, "/project/.bruin.yml", []byte(content), 0o600))
	cm, err := config.LoadFromFileOrEnv(fs, "/project/.bruin.yml")
	require.NoError(t, err)
	return cm
}

// The vault and item names are unique to each test because fetched items are cached for the whole process.
const lazyVaultConfig = `
default_environment: default
environments:
  default:
    connections:
      generic:
        - name: plain
          value: in-file
        - name: from_vault
          bruin_external_vault: op://lazyvault/item-a
        - name: broken
          bruin_external_vault: op://lazyvault/missing
  prod:
    connections:
      generic:
        - name: prod_only
          bruin_external_vault: op://lazyvault/prod-item
`

func TestManagerReadsTheVaultOnlyForTheConnectionsAskedFor(t *testing.T) {
	calls := fakeOnePasswordCLI(t, map[string]string{
		"lazyvault/item-a":    `{"fields":[{"label":"value","value":"from-vault"}]}`,
		"lazyvault/prod-item": `{"fields":[{"label":"value","value":"prod-secret"}]}`,
	})
	cm := loadVaultConfig(t, lazyVaultConfig)

	manager, errs := NewManagerFromConfig(cm)
	require.Empty(t, errs)
	assert.Empty(t, calls(), "building the manager must not read the vault")

	assert.Equal(t, "generic", manager.GetConnectionType("from_vault"))
	plain, ok := manager.GetConnection("plain").(*config.GenericConnection)
	require.True(t, ok)
	assert.Equal(t, "in-file", plain.Value)
	assert.Empty(t, calls(), "types and plain connections must not read the vault")

	conn, ok := manager.GetConnection("from_vault").(*config.GenericConnection)
	require.True(t, ok)
	assert.Equal(t, "from-vault", conn.Value)
	details, ok := manager.GetConnectionDetails("from_vault").(*config.GenericConnection)
	require.True(t, ok)
	assert.Equal(t, "from-vault", details.Value)
	assert.Equal(t, []string{"lazyvault/item-a"}, calls())

	assert.Nil(t, manager.GetConnection("prod_only"), "another environment's connection is not this manager's")
	assert.Equal(t, []string{"lazyvault/item-a"}, calls())

	inFile := cm.SelectedEnvironment.Connections.Generic[1]
	assert.Equal(t, "from_vault", inFile.Name)
	assert.Empty(t, inFile.Value, "the secret must not be written back into the loaded config")
}

func TestManagerReportsAVaultFailureOnce(t *testing.T) {
	calls := fakeOnePasswordCLI(t, map[string]string{})
	cm := loadVaultConfig(t, lazyVaultConfig)

	manager, errs := NewManagerFromConfig(cm)
	require.Empty(t, errs)

	resolver, ok := manager.(config.ConnectionResolver)
	require.True(t, ok)
	conn, err := resolver.ResolveConnection("broken")
	assert.Nil(t, conn)
	require.ErrorContains(t, err, "connection 'broken' (generic) in environment 'default'")
	require.ErrorContains(t, err, "op://lazyvault/missing")

	assert.Nil(t, manager.GetConnection("broken"))
	assert.Nil(t, manager.GetConnectionDetails("broken"))
	assert.Equal(t, []string{"lazyvault/missing"}, calls(), "a failed read is not retried within one command")

	plain, err := resolver.ResolveConnection("plain")
	require.NoError(t, err)
	assert.NotNil(t, plain)
}

func TestManagerReadsTheSelectedEnvironmentOnly(t *testing.T) {
	calls := fakeOnePasswordCLI(t, map[string]string{
		"lazyvault/prod-item": `{"fields":[{"label":"value","value":"prod-secret"}]}`,
	})
	cm := loadVaultConfig(t, lazyVaultConfig)
	require.NoError(t, cm.SelectEnvironment("prod"))

	manager, errs := NewManagerFromConfig(cm)
	require.Empty(t, errs)
	conn, ok := manager.GetConnection("prod_only").(*config.GenericConnection)
	require.True(t, ok)
	assert.Equal(t, "prod-secret", conn.Value)
	assert.Equal(t, []string{"lazyvault/prod-item"}, calls())
}
