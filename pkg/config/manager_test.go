package config

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromFile(t *testing.T) {
	t.Parallel()

	devEnv := Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
					ServiceAccountFile: "/path/to/service_account.json",
					ProjectID:          "my-project",
				},
			},
			Snowflake: []SnowflakeConnection{
				{
					Name:      "conn2",
					Username:  "user",
					Password:  "pass",
					Account:   "account",
					Region:    "region",
					Role:      "role",
					Database:  "db",
					Schema:    "schema",
					Warehouse: "wh",
				},
			},
			Postgres: []PostgresConnection{
				{
					Name:         "conn3",
					Host:         "somehost",
					Username:     "pguser",
					Password:     "pgpass",
					Database:     "pgdb",
					Schema:       "non_public_schema",
					Port:         5432,
					PoolMaxConns: 5,
					SslMode:      "require",
				},
			},
			RedShift: []PostgresConnection{
				{
					Name:         "conn4",
					Host:         "someredshift",
					Username:     "rsuser",
					Password:     "rspass",
					Database:     "rsdb",
					Port:         5433,
					PoolMaxConns: 4,
					SslMode:      "disable",
				},
			},
			MsSQL: []MsSQLConnection{
				{
					Name:     "conn5",
					Host:     "somemssql",
					Username: "msuser",
					Password: "mspass",
					Database: "mssqldb",
					Port:     1433,
				},
			},
			Databricks: []DatabricksConnection{
				{
					Name:  "conn55",
					Host:  "hostbricks",
					Path:  "sql",
					Token: "aaaaaaaa",
					Port:  443,
				},
			},
			Synapse: []MsSQLConnection{
				{
					Name:     "conn6",
					Host:     "somemsynapse",
					Username: "syuser",
					Password: "sypass",
					Database: "sydb",
					Port:     1434,
				},
			},
			Mongo: []MongoConnection{
				{
					Name:     "conn7",
					Host:     "mongohost",
					Username: "mongouser",
					Password: "mongopass",
					Database: "mongodb",
					Port:     27017,
				},
			},
			MySQL: []MySQLConnection{
				{
					Name:     "conn8",
					Host:     "mysqlhost",
					Username: "mysqluser",
					Password: "mysqlpass",
					Database: "mysqldb",
					Port:     3306,
				},
			},
			Notion: []NotionConnection{
				{
					Name:   "conn9",
					APIKey: "XXXXYYYYZZZZ",
				},
			},
			HANA: []HANAConnection{
				{
					Name:     "conn10",
					Host:     "hanahost",
					Username: "hanauser",
					Password: "hanapass",
					Database: "hanadb",
					Port:     39013,
				},
			},
			Shopify: []ShopifyConnection{
				{
					Name:   "conn11",
					APIKey: "shopifykey",
					URL:    "shopifyurl",
				},
			},
			Gorgias: []GorgiasConnection{
				{
					Name:   "conn12",
					APIKey: "gorgiaskey",
					Domain: "gorgiasurl",
					Email:  "gorgiasemail",
				},
			},
			Generic: []GenericConnection{
				{
					Name:  "key1",
					Value: "value1",
				},
				{
					Name:  "key2",
					Value: "value2",
				},
			},
		},
	}

	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "missing path should error",
			args: args{
				path: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: assert.Error,
		},
		{
			name: "read simple connection",
			args: args{
				path: "testdata/simple.yml",
			},
			want: &Config{
				DefaultEnvironmentName:  "dev",
				SelectedEnvironment:     &devEnv,
				SelectedEnvironmentName: "dev",
				Environments: map[string]Environment{
					"dev": devEnv,
					"prod": {
						Connections: Connections{
							GoogleCloudPlatform: []GoogleCloudPlatformConnection{
								{
									Name:               "conn1",
									ServiceAccountFile: "/path/to/service_account.json",
									ProjectID:          "my-project",
								},
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewReadOnlyFs(afero.NewOsFs())
			got, err := LoadFromFile(fs, tt.args.path)

			tt.wantErr(t, err)
			if tt.want != nil {
				tt.want.fs = fs
				tt.want.path = tt.args.path
			}

			if tt.want != nil {
				got.SelectedEnvironment.Connections.byKey = nil
				assert.EqualExportedValues(t, *tt.want, *got)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func TestLoadOrCreate(t *testing.T) {
	t.Parallel()

	configPath := "/some/path/to/config.yml"
	defaultEnv := &Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
			Postgres:   []PostgresConnection{},
			Snowflake:  []SnowflakeConnection{},
			Generic:    []GenericConnection{},
			RedShift:   []PostgresConnection{},
			MsSQL:      []MsSQLConnection{},
			Databricks: []DatabricksConnection{},
			Synapse:    []MsSQLConnection{},
			Mongo:      []MongoConnection{},
			MySQL:      []MySQLConnection{},
			Notion:     []NotionConnection{},
			HANA:       []HANAConnection{},
			Shopify:    []ShopifyConnection{},
			Gorgias:    []GorgiasConnection{},
		},
	}

	existingConfig := &Config{
		path:                    configPath,
		DefaultEnvironmentName:  "dev",
		SelectedEnvironmentName: "dev",
		SelectedEnvironment:     defaultEnv,
		Environments: map[string]Environment{
			"dev": *defaultEnv,
		},
	}

	type args struct {
		fs afero.Fs
	}
	tests := []struct {
		name    string
		setup   func(t *testing.T, args args)
		want    *Config
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "missing path should create",
			want: &Config{
				DefaultEnvironmentName:  "default",
				SelectedEnvironment:     &Environment{},
				SelectedEnvironmentName: "default",
				Environments: map[string]Environment{
					"default": {},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "if any other is returned from the fs then propagate the error",
			setup: func(t *testing.T, args args) {
				err := afero.WriteFile(args.fs, configPath, []byte("some content"), 0o644)
				assert.NoError(t, err)
			},
			wantErr: assert.Error,
		},
		{
			name: "return the config if it exists",
			setup: func(t *testing.T, args args) {
				err := existingConfig.PersistToFs(args.fs)
				require.NoError(t, err)

				err = afero.WriteFile(args.fs, "/some/path/to/.gitignore", []byte("file1"), 0o644)
				require.NoError(t, err)
			},
			want:    existingConfig,
			wantErr: assert.NoError,
		},

		{
			name: "return the config if it exists, add to the gitignore",
			setup: func(t *testing.T, args args) {
				err := existingConfig.PersistToFs(args.fs)
				assert.NoError(t, err)
			},
			want:    existingConfig,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := args{
				fs: afero.NewMemMapFs(),
			}

			if tt.setup != nil {
				tt.setup(t, a)
			}

			got, err := LoadOrCreate(a.fs, configPath)
			tt.wantErr(t, err)

			if tt.want != nil {
				assert.EqualExportedValues(t, *tt.want.SelectedEnvironment, *got.SelectedEnvironment)
				assert.Equal(t, tt.want.SelectedEnvironmentName, got.SelectedEnvironmentName)
			} else {
				assert.Equal(t, tt.want, got)
			}

			exists, err := afero.Exists(a.fs, configPath)
			require.NoError(t, err)
			assert.True(t, exists)

			if tt.want != nil {
				content, err := afero.ReadFile(a.fs, "/some/path/to/.gitignore")
				require.NoError(t, err)
				assert.Contains(t, string(content), "config.yml", "config file content: %s", content)
			}
		})
	}
}

func TestConfig_SelectEnvironment(t *testing.T) {
	t.Parallel()

	defaultEnv := &Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
		},
	}

	prodEnv := &Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/prod_service_account.json",
				},
			},
		},
	}

	conf := Config{
		DefaultEnvironmentName: "default",
		SelectedEnvironment:    defaultEnv,
		Environments:           map[string]Environment{"default": *defaultEnv, "prod": *prodEnv},
	}

	err := conf.SelectEnvironment("prod")
	require.NoError(t, err)
	assert.EqualExportedValues(t, prodEnv, conf.SelectedEnvironment)
	assert.Equal(t, "prod", conf.SelectedEnvironmentName)

	err = conf.SelectEnvironment("non-existing")
	require.Error(t, err)
	assert.EqualExportedValues(t, prodEnv, conf.SelectedEnvironment)
	assert.Equal(t, "prod", conf.SelectedEnvironmentName)

	err = conf.SelectEnvironment("default")
	require.NoError(t, err)
	assert.EqualExportedValues(t, defaultEnv, conf.SelectedEnvironment)
	assert.Equal(t, "default", conf.SelectedEnvironmentName)
}
