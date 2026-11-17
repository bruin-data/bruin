package config

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOnePassword stands in for the op CLI; items are keyed by the full op:// reference.
type fakeOnePassword struct {
	items map[string]map[string]string
	calls map[string]int
}

func (f *fakeOnePassword) FetchItem(_ context.Context, path string) (map[string]string, error) {
	vault, item, err := parseOnePasswordPath(path)
	if err != nil {
		return nil, err
	}
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls["op://"+path]++
	fields, ok := f.items["op://"+path]
	if !ok {
		return nil, errors.New(`"` + item + `" isn't an item in the "` + vault + `" vault`)
	}
	return fields, nil
}

type fakeProvider map[string]map[string]string

func (f fakeProvider) FetchItem(_ context.Context, path string) (map[string]string, error) {
	fields, ok := f[path]
	if !ok {
		return nil, errors.New("not found")
	}
	return fields, nil
}

func fakeVaults(items map[string]map[string]string) (vaultProviders, *fakeOnePassword) {
	op := &fakeOnePassword{items: items}
	return vaultProviders{"op": op}, op
}

func loadWithVault(t *testing.T, config string, fetcher secretItemFetcher) *Config {
	t.Helper()
	var cfg Config
	require.NoError(t, decodeConfigYAML([]byte(config), &cfg))
	require.NoError(t, cfg.SelectEnvironment("default"))
	cfg.vaultFetcher = fetcher
	return &cfg
}

func vaultPostgres(t *testing.T, cfg *Config, name string) *PostgresConnection {
	t.Helper()
	resolved, err := cfg.ExternalVaultConnection(t.Context(), name)
	require.NoError(t, err)
	pg, ok := resolved.SelectedEnvironment.Connections.GetConnection(name).(*PostgresConnection)
	require.True(t, ok)
	return pg
}

func vaultPassword(t *testing.T, cfg *Config, env, name string) string {
	t.Helper()
	require.NoError(t, cfg.SelectEnvironment(env))
	return vaultPostgres(t, cfg, name).Password
}

const vaultConfig = `
environments:
  default:
    connections:
      postgres:
        - name: my_pg
          host: localhost
          port: 5432
          database: analytics
          username: me
          bruin_external_vault: op://Private/bruin-my-pg
`

func TestExternalVaultFillsConnectionKeys(t *testing.T) {
	t.Parallel()

	vault, _ := fakeVaults(map[string]map[string]string{
		"op://Private/bruin-my-pg": {
			"password":       "s3cret",
			"ssl_mode":       "require",
			"notesPlain":     "not a connection key",
			"pool_max_conns": "7",
		},
	})

	cfg := loadWithVault(t, vaultConfig, vault)

	pg := vaultPostgres(t, cfg, "my_pg")
	assert.Equal(t, "localhost", pg.Host)
	assert.Equal(t, "me", pg.Username)
	assert.Equal(t, 5432, pg.Port)
	assert.Equal(t, "s3cret", pg.Password)
	assert.Equal(t, "require", pg.SslMode)
	assert.Equal(t, 7, pg.PoolMaxConns)

	inFile := cfg.SelectedEnvironment.Connections.Postgres[0]
	assert.Empty(t, inFile.Password)
	assert.Empty(t, inFile.SslMode)
}

func TestExternalVaultFetchesEachItemOnce(t *testing.T) {
	t.Parallel()

	providers, op := fakeVaults(map[string]map[string]string{
		"op://Private/shared": {"password": "s3cret"},
	})
	fetcher := &cachingFetcher{next: providers}

	config := `
environments:
  default:
    connections:
      postgres:
        - name: a
          host: localhost
          bruin_external_vault: op://Private/shared
  prod:
    connections:
      postgres:
        - name: b
          host: localhost
          bruin_external_vault: op://Private/shared
`
	for range 2 {
		cfg := loadWithVault(t, config, fetcher)
		assert.Equal(t, "s3cret", vaultPostgres(t, cfg, "a").Password)
		require.NoError(t, cfg.SelectEnvironment("prod"))
		assert.Equal(t, "s3cret", vaultPostgres(t, cfg, "b").Password)
		assert.Equal(t, "s3cret", vaultPostgres(t, cfg, "b").Password)
	}
	assert.Equal(t, 1, op.calls["op://Private/shared"])
}

func TestExternalVaultErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  string
		items   map[string]map[string]string
		wantErr []string
	}{
		{
			name:    "key set in both places",
			config:  vaultConfig,
			items:   map[string]map[string]string{"op://Private/bruin-my-pg": {"username": "other-user"}},
			wantErr: []string{"connection 'my_pg'", "'username' is set both in the config file and in op://Private/bruin-my-pg"},
		},
		{
			name:    "missing item",
			config:  vaultConfig,
			items:   map[string]map[string]string{},
			wantErr: []string{"connection 'my_pg' (postgres) in environment 'default'", "op://Private/bruin-my-pg", "isn't an item"},
		},
		{
			name:    "value of the wrong type",
			config:  vaultConfig,
			items:   map[string]map[string]string{"op://Private/bruin-my-pg": {"pool_max_conns": "not-a-number"}},
			wantErr: []string{"field 'pool_max_conns' in op://Private/bruin-my-pg is not a valid int"},
		},
		{
			name: "unsupported scheme",
			config: `
environments:
  default:
    connections:
      postgres:
        - name: my_pg
          bruin_external_vault: doppler://project/config
`,
			wantErr: []string{"connection 'my_pg'", "doppler://project/config", `unsupported vault scheme "doppler", supported: op://`},
		},
		{
			name: "malformed op reference",
			config: `
environments:
  default:
    connections:
      postgres:
        - name: my_pg
          bruin_external_vault: op://Private/item/password
`,
			wantErr: []string{"expected op://<vault>/<item>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			providers, _ := fakeVaults(tt.items)
			_, err := loadWithVault(t, tt.config, providers).ExternalVaultConnection(t.Context(), "my_pg")
			require.Error(t, err)
			for _, want := range tt.wantErr {
				assert.Contains(t, err.Error(), want)
			}
			assert.NotContains(t, err.Error(), "not-a-number")
			assert.NotContains(t, err.Error(), "other-user")
		})
	}
}

func TestSelectingAnEnvironmentDoesNotReadTheVault(t *testing.T) {
	t.Parallel()

	cfg := loadWithVault(t, vaultConfig, unreachableVault{})

	pg := cfg.SelectedEnvironment.Connections.Postgres[0]
	assert.Equal(t, "localhost", pg.Host)
	assert.Empty(t, pg.Password)
	assert.True(t, cfg.HasExternalVaultConnections())
	assert.True(t, cfg.UsesExternalVault("my_pg"))
	assert.False(t, cfg.UsesExternalVault("other"))
}

func TestExternalVaultConnectionReadsOnlyTheAskedConnection(t *testing.T) {
	t.Parallel()

	config := `
environments:
  default:
    connections:
      postgres:
        - name: dev_pg
          bruin_external_vault: op://Private/dev
        - name: other_pg
          bruin_external_vault: op://Private/other
  prod:
    connections:
      postgres:
        - name: prod_pg
          bruin_external_vault: op://Private/prod-missing
`
	providers, op := fakeVaults(map[string]map[string]string{"op://Private/dev": {"password": "dev-secret"}})
	cfg := loadWithVault(t, config, providers)

	assert.Equal(t, "dev-secret", vaultPostgres(t, cfg, "dev_pg").Password)
	assert.Equal(t, map[string]int{"op://Private/dev": 1}, op.calls)

	_, err := cfg.ExternalVaultConnection(t.Context(), "prod_pg")
	require.ErrorContains(t, err, "connection 'prod_pg' does not use bruin_external_vault in environment 'default'")

	require.NoError(t, cfg.SelectEnvironment("prod"))
	assert.False(t, cfg.UsesExternalVault("dev_pg"))
	_, err = cfg.ExternalVaultConnection(t.Context(), "prod_pg")
	require.ErrorContains(t, err, "connection 'prod_pg' (postgres) in environment 'prod'")
}

func TestWithoutExternalVaultConnections(t *testing.T) {
	t.Parallel()

	cfg := loadWithVault(t, twoEnvironmentVaultConfig, unreachableVault{})
	filtered := cfg.WithoutExternalVaultConnections()

	names := make([]string, 0, len(filtered.SelectedEnvironment.Connections.Postgres))
	for _, pg := range filtered.SelectedEnvironment.Connections.Postgres {
		names = append(names, pg.Name)
	}
	assert.Equal(t, []string{"plain_pg"}, names)
	assert.False(t, filtered.HasExternalVaultConnections())
	assert.Equal(t, map[string]string{"plain_pg": "postgres"}, filtered.SelectedEnvironment.Connections.ConnectionsSummaryList())

	assert.Len(t, cfg.SelectedEnvironment.Connections.Postgres, 2)
	assert.True(t, cfg.SelectedEnvironment.Connections.Exists("my_pg"))
	assert.True(t, cfg.UsesExternalVault("my_pg"))
}

func TestExternalVaultReferenceIsCheckedOnLoad(t *testing.T) {
	t.Parallel()

	config := `
environments:
  default:
    connections:
      postgres:
        - name: my_pg
          bruin_external_vault: not-a-reference
`
	var cfg Config
	err := decodeConfigYAML([]byte(config), &cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection 'my_pg'")
}

func TestPersistAfterReadingTheVaultWritesNoSecrets(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	path := "/project/.bruin.yml"
	vault, _ := fakeVaults(map[string]map[string]string{
		"op://Private/bruin-my-pg": {"password": "s3cret"},
	})
	cfg := loadWithVault(t, vaultConfig, vault)
	cfg.fs = fs
	cfg.path = path
	assert.Equal(t, "s3cret", vaultPostgres(t, cfg, "my_pg").Password)

	require.NoError(t, cfg.Persist())
	content, err := afero.ReadFile(fs, path)
	require.NoError(t, err)
	assert.NotContains(t, string(content), "s3cret")
	assert.Contains(t, string(content), "bruin_external_vault: op://Private/bruin-my-pg")
}

const twoEnvironmentVaultConfig = `
default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: plain_pg
          host: localhost
          password: plain
        - name: my_pg
          host: localhost
          port: 5432
          bruin_external_vault: op://Private/bruin-my-pg
  prod:
    connections:
      postgres:
        - name: prod_pg
          host: prod.internal
          bruin_external_vault: op://Private/bruin-prod-pg
`

func loadForEditing(t *testing.T, content string) (*Config, func() *Config) {
	t.Helper()

	fs := afero.NewMemMapFs()
	path := "/project/.bruin.yml"
	var cfg Config
	require.NoError(t, decodeConfigYAML([]byte(content), &cfg))
	require.NoError(t, cfg.SelectEnvironment("default"))
	cfg.fs = fs
	cfg.path = path

	return &cfg, func() *Config {
		t.Helper()
		require.NoError(t, cfg.Persist())
		saved, err := afero.ReadFile(fs, path)
		require.NoError(t, err)

		vault, _ := fakeVaults(map[string]map[string]string{
			"op://Private/bruin-my-pg":   {"password": "s3cret"},
			"op://Private/bruin-prod-pg": {"password": "pr0d"},
		})
		var reloaded Config
		require.NoError(t, decodeConfigYAML(saved, &reloaded), string(saved))
		reloaded.vaultFetcher = vault
		assert.NotContains(t, string(saved), "s3cret")
		assert.NotContains(t, string(saved), "pr0d")
		return &reloaded
	}
}

func vaultReferences(cfg *Config) map[string]string {
	refs := map[string]string{}
	for _, ref := range cfg.ExternalVaultReferences() {
		refs[ref.Environment+"/"+ref.Name] = ref.Reference
	}
	return refs
}

func TestPersistKeepsVaultReferencesWhenOtherConnectionsChange(t *testing.T) {
	t.Parallel()

	cfg, saveAndReload := loadForEditing(t, twoEnvironmentVaultConfig)
	require.NoError(t, cfg.DeleteConnection("default", "plain_pg"))
	require.NoError(t, cfg.AddConnection("default", "new_pg", "postgres", map[string]any{"host": "db", "password": "typed"}))

	reloaded := saveAndReload()
	assert.Equal(t, map[string]string{
		"default/my_pg": "op://Private/bruin-my-pg",
		"prod/prod_pg":  "op://Private/bruin-prod-pg",
	}, vaultReferences(reloaded))

	pg := reloaded.Environments["default"].Connections.Postgres
	require.Len(t, pg, 2)
	assert.Equal(t, "my_pg", pg[0].Name)
	assert.Equal(t, 5432, pg[0].Port)
	assert.Equal(t, "new_pg", pg[1].Name)
	assert.Equal(t, "typed", pg[1].Password)
	assert.Equal(t, "s3cret", vaultPassword(t, reloaded, "default", "my_pg"))
	assert.Equal(t, "pr0d", vaultPassword(t, reloaded, "prod", "prod_pg"))
}

func TestPersistDropsTheReferenceOfADeletedConnection(t *testing.T) {
	t.Parallel()

	cfg, saveAndReload := loadForEditing(t, twoEnvironmentVaultConfig)
	require.NoError(t, cfg.DeleteConnection("default", "my_pg"))

	reloaded := saveAndReload()
	assert.Equal(t, map[string]string{"prod/prod_pg": "op://Private/bruin-prod-pg"}, vaultReferences(reloaded))
	assert.Len(t, reloaded.Environments["default"].Connections.Postgres, 1)
}

func TestPersistKeepsVaultReferencesThroughEnvironmentChanges(t *testing.T) {
	t.Parallel()

	cfg, saveAndReload := loadForEditing(t, twoEnvironmentVaultConfig)
	require.NoError(t, cfg.UpdateEnvironment("prod", "production", ""))
	require.NoError(t, cfg.CloneEnvironment("default", "staging", ""))

	reloaded := saveAndReload()
	assert.Equal(t, map[string]string{
		"default/my_pg":      "op://Private/bruin-my-pg",
		"staging/my_pg":      "op://Private/bruin-my-pg",
		"production/prod_pg": "op://Private/bruin-prod-pg",
	}, vaultReferences(reloaded))
	assert.Equal(t, "s3cret", vaultPassword(t, reloaded, "staging", "my_pg"))

	require.NoError(t, reloaded.DeleteEnvironment("production"))
	assert.NotContains(t, vaultReferences(reloaded), "production/prod_pg")
}

func TestExternalVaultRoutesBySchemeToProvider(t *testing.T) {
	t.Parallel()

	config := `
environments:
  default:
    connections:
      postgres:
        - name: from_op
          bruin_external_vault: op://Private/pg
        - name: from_other
          bruin_external_vault: other://project/prod
`
	providers, _ := fakeVaults(map[string]map[string]string{"op://Private/pg": {"password": "from-1password"}})
	providers["other"] = fakeProvider{"project/prod": {"password": "from-other-vault"}}

	cfg := loadWithVault(t, config, providers)

	assert.Equal(t, "from-1password", vaultPostgres(t, cfg, "from_op").Password)
	assert.Equal(t, "from-other-vault", vaultPostgres(t, cfg, "from_other").Password)
}

func TestParseExternalVaultRef(t *testing.T) {
	t.Parallel()

	ref, err := parseExternalVaultRef("doppler://my-project/prd")
	require.NoError(t, err)
	assert.Equal(t, "doppler", ref.scheme)
	assert.Equal(t, "my-project/prd", ref.path)

	for _, uri := range []string{"Private/item", "://item", "op://"} {
		_, err := parseExternalVaultRef(uri)
		assert.Error(t, err, uri)
	}
}

func TestParseOnePasswordPath(t *testing.T) {
	t.Parallel()

	vault, item, err := parseOnePasswordPath("Shared Vault/bruin my pg")
	require.NoError(t, err)
	assert.Equal(t, "Shared Vault", vault)
	assert.Equal(t, "bruin my pg", item)

	for _, path := range []string{"Private", "/item", "Private/", "Private/item/password"} {
		_, _, err := parseOnePasswordPath(path)
		assert.Error(t, err, path)
	}
}

type unreachableVault struct{}

func (unreachableVault) FetchItem(context.Context, externalVaultRef) (map[string]string, error) {
	return nil, errors.New("vault is locked")
}

func TestExternalVaultMatchesLabelsLoosely(t *testing.T) {
	t.Parallel()

	config := `
environments:
  default:
    connections:
      postgres:
        - name: my_pg
          database: analytics
          bruin_external_vault: op://Private/db-item
`
	vault, _ := fakeVaults(map[string]map[string]string{
		"op://Private/db-item": {
			"server":             "db.internal",
			"Port":               "5433",
			"Password":           "s3cret",
			"pool max conns":     "4",
			"name":               "not-the-connection-name",
			"type":               "postgresql",
			"connection options": "sslmode=require",
		},
	})
	cfg := loadWithVault(t, config, vault)

	pg := vaultPostgres(t, cfg, "my_pg")
	assert.Equal(t, "my_pg", pg.Name)
	assert.Equal(t, "db.internal", pg.Host)
	assert.Equal(t, 5433, pg.Port)
	assert.Equal(t, "s3cret", pg.Password)
	assert.Equal(t, 4, pg.PoolMaxConns)
	assert.Equal(t, "analytics", pg.Database)
}

func TestExternalVaultRejectsTwoLabelsForOneSetting(t *testing.T) {
	t.Parallel()

	vault, _ := fakeVaults(map[string]map[string]string{
		"op://Private/bruin-my-pg": {"password": "first", "Password": "second"},
	})
	_, err := loadWithVault(t, vaultConfig, vault).ExternalVaultConnection(t.Context(), "my_pg")

	require.ErrorContains(t, err, "the fields 'Password' and 'password' both set 'password'")
	assert.NotContains(t, err.Error(), "first")
	assert.NotContains(t, err.Error(), "second")
}

func TestExternalVaultResolvesRelativePathsAgainstTheConfigFile(t *testing.T) {
	t.Parallel()

	config := `
environments:
  default:
    connections:
      duckdb:
        - name: local
          bruin_external_vault: op://Private/duck
      snowflake:
        - name: warehouse
          private_key_path: /keys/already-absolute.p8
          bruin_external_vault: op://Private/snow
`
	vault, _ := fakeVaults(map[string]map[string]string{
		"op://Private/duck": {"path": "data/local.db"},
		"op://Private/snow": {"account": "acme"},
	})
	cfg := loadWithVault(t, config, vault)
	cfg.path = "/project/.bruin.yml"
	absoluteConfig, err := filepath.Abs(cfg.path)
	require.NoError(t, err)

	resolved, err := cfg.ExternalVaultConnection(t.Context(), "local")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(filepath.Dir(absoluteConfig), "data", "local.db"), resolved.SelectedEnvironment.Connections.DuckDB[0].Path)

	resolved, err = cfg.ExternalVaultConnection(t.Context(), "warehouse")
	require.NoError(t, err)
	assert.Equal(t, "/keys/already-absolute.p8", resolved.SelectedEnvironment.Connections.Snowflake[0].PrivateKeyPath)
}

func TestExternalVaultKeepsLinksOnConnectionsSharedThroughAnchors(t *testing.T) {
	t.Parallel()

	config := `
environments:
  default:
    connections:
      postgres:
        - &shared
          name: my_pg
          host: localhost
          bruin_external_vault: op://Private/bruin-my-pg
  prod:
    connections:
      postgres:
        - *shared
`
	vault, _ := fakeVaults(map[string]map[string]string{"op://Private/bruin-my-pg": {"password": "s3cret"}})
	cfg := loadWithVault(t, config, vault)

	assert.Equal(t, map[string]string{
		"default/my_pg": "op://Private/bruin-my-pg",
		"prod/my_pg":    "op://Private/bruin-my-pg",
	}, vaultReferences(cfg))
	assert.Equal(t, "s3cret", vaultPassword(t, cfg, "prod", "my_pg"))
}

func TestExternalVaultIgnoresSettingsTheLoaderFilledIn(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	config := `
environments:
  default:
    connections:
      duckdb:
        - name: remote
          bruin_external_vault: op://Private/quack
`
	require.NoError(t, afero.WriteFile(fs, "/project/.bruin.yml", []byte(config), 0o600))
	cfg, err := LoadFromFileOrEnv(fs, "/project/.bruin.yml")
	require.NoError(t, err)
	require.NotEmpty(t, cfg.SelectedEnvironment.Connections.DuckDB[0].Path, "the loader gives a DuckDB connection without a path a default one")

	vault, _ := fakeVaults(map[string]map[string]string{
		"op://Private/quack": {"quack": "true", "host": "quack.internal", "token": "t0ken"},
	})
	cfg.vaultFetcher = vault
	resolved, err := cfg.ExternalVaultConnection(t.Context(), "remote")
	require.NoError(t, err)

	duck := resolved.SelectedEnvironment.Connections.DuckDB[0]
	assert.True(t, duck.Quack)
	assert.Equal(t, "quack.internal", duck.Host)
	assert.Empty(t, duck.Path)
}
