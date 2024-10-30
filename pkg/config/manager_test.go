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
		Connections: &Connections{
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
			Klaviyo: []KlaviyoConnection{
				{
					Name:   "conn15",
					APIKey: "klaviyokey",
				},
			},
			Adjust: []AdjustConnection{
				{
					Name:   "conn16",
					APIKey: "adjustokey",
				},
			},
			AthenaConnection: []AthenaConnection{
				{
					Name:             "conn14",
					AccessKey:        "athena_key",
					SecretKey:        "athena_secret",
					QueryResultsPath: "s3://bucket/prefix",
					Region:           "us-west-2",
					Database:         "athena_db",
				},
			},
			AwsConnection: []AwsConnection{
				{
					Name:      "conn13",
					SecretKey: "awssecret",
					AccessKey: "awskey",
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
			FacebookAds: []FacebookAdsConnection{
				{
					Name:        "conn17",
					AccessToken: "Facebookkey",
					AccountID:   "Id123",
				},
			},
			Stripe: []StripeConnection{
				{
					Name:   "conn18",
					APIKey: "stripekey",
				},
			},
			Appsflyer: []AppsflyerConnection{
				{
					Name:   "conn19",
					APIKey: "appsflyerkey",
				},
			},
			Kafka: []KafkaConnection{
				{
					Name:             "conn20",
					BootstrapServers: "localhost:9093",
					GroupID:          "kafka123",
				},
			},
			DuckDB: []DuckDBConnection{
				{
					Name: "conn20",
					Path: "/path/to/duck.db",
				},
			},
			Hubspot: []HubspotConnection{
				{
					Name:   "conn21",
					APIKey: "hubspotkey",
				},
			},
			GoogleSheets: []GoogleSheetsConnection{
				{
					Name:            "conn22",
					CredentialsPath: "/path/to/service_account.json",
				},
			},
			Chess: []ChessConnection{
				{
					Name:    "conn24",
					Players: []string{"Max", "Peter"},
				},
			},
			Airtable: []AirtableConnection{
				{
					Name:        "conn23",
					BaseId:      "123",
					AccessToken: "accessKey",
				},
			},
			Zendesk: []ZendeskConnection{
				{
					Name:      "conn25",
					ApiToken:  "zendeskKey",
					Email:     "zendeskemail",
					Subdomain: "zendeskUrl",
				},
				{
					Name:       "conn25-1",
					OAuthToken: "zendeskToken",
					Subdomain:  "zendeskUrl",
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
						Connections: &Connections{
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
		Connections: &Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
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
					"default": {
						Connections: &Connections{},
					},
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
				assert.Equal(t, tt.want.SelectedEnvironmentName, got.SelectedEnvironmentName)

				if tt.want.SelectedEnvironment != nil && tt.want.SelectedEnvironment.Connections != nil {
					assert.EqualExportedValues(t, *tt.want.SelectedEnvironment.Connections, *got.SelectedEnvironment.Connections)
				}
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
		Connections: &Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
		},
	}

	prodEnv := &Environment{
		Connections: &Connections{
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

func TestConfig_AddConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		envName     string
		connType    string
		connName    string
		creds       map[string]interface{}
		expectedErr bool
	}{
		{
			name:     "Add GCP connection",
			envName:  "default",
			connType: "google_cloud_platform",
			connName: "gcp-conn",
			creds: map[string]interface{}{
				"service_account_file": "/path/to/gcp_service_account.json",
				"project_id":           "gcp-project-123",
			},
			expectedErr: false,
		},
		{
			name:     "Add AWS connection",
			envName:  "prod",
			connType: "aws",
			connName: "aws-conn",
			creds: map[string]interface{}{
				"access_key": "AKIAIOSFODNN7EXAMPLE",
				"secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"region":     "us-west-2",
			},
			expectedErr: false,
		},
		{
			name:     "Add Generic connection",
			envName:  "staging",
			connType: "generic",
			connName: "generic-conn",
			creds: map[string]interface{}{
				"value": "some-secret-value",
			},
			expectedErr: false,
		},
		{
			name:        "Add Invalid connection",
			envName:     "default",
			connType:    "invalid",
			connName:    "invalid-conn",
			creds:       map[string]interface{}{},
			expectedErr: true,
		},
		{
			name:        "Add to non-existent environment",
			envName:     "non-existent",
			connType:    "generic",
			connName:    "generic-conn",
			creds:       map[string]interface{}{"value": "test"},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf := &Config{
				Environments: map[string]Environment{
					"default": {Connections: &Connections{}},
					"prod":    {Connections: &Connections{}},
					"staging": {Connections: &Connections{}},
				},
			}

			err := conf.AddConnection(tt.envName, tt.connName, tt.connType, tt.creds)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				env, exists := conf.Environments[tt.envName]
				assert.True(t, exists)

				switch tt.connType {
				case "google_cloud_platform":
					assert.Len(t, env.Connections.GoogleCloudPlatform, 1)
					assert.Equal(t, tt.connName, env.Connections.GoogleCloudPlatform[0].Name)
					assert.Equal(t, tt.creds["service_account_file"], env.Connections.GoogleCloudPlatform[0].ServiceAccountFile)
					assert.Equal(t, tt.creds["project_id"], env.Connections.GoogleCloudPlatform[0].ProjectID)
				case "aws":
					assert.Len(t, env.Connections.AwsConnection, 1)
					assert.Equal(t, tt.connName, env.Connections.AwsConnection[0].Name)
					assert.Equal(t, tt.creds["access_key"], env.Connections.AwsConnection[0].AccessKey)
					assert.Equal(t, tt.creds["secret_key"], env.Connections.AwsConnection[0].SecretKey)
				case "generic":
					assert.Len(t, env.Connections.Generic, 1)
					assert.Equal(t, tt.connName, env.Connections.Generic[0].Name)
					assert.Equal(t, tt.creds["value"], env.Connections.Generic[0].Value)
				}
			}
		})
	}
}

func TestDeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		envName     string
		connName    string
		setupConfig func() *Config
		expectedErr bool
	}{
		{
			name:     "Delete existing GCP connection",
			envName:  "default",
			connName: "gcp-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{
						"default": {
							Connections: &Connections{
								GoogleCloudPlatform: []GoogleCloudPlatformConnection{
									{Name: "gcp-conn", ServiceAccountFile: "file.json", ProjectID: "project"},
								},
							},
						},
					},
				}
			},
			expectedErr: false,
		},
		{
			name:     "Delete existing AWS connection",
			envName:  "prod",
			connName: "aws-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{
						"prod": {
							Connections: &Connections{
								AwsConnection: []AwsConnection{
									{Name: "aws-conn", AccessKey: "key", SecretKey: "secret"},
								},
							},
						},
					},
				}
			},
			expectedErr: false,
		},
		{
			name:     "Delete non-existent connection",
			envName:  "staging",
			connName: "non-existent-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{
						"staging": {Connections: &Connections{}},
					},
				}
			},
			expectedErr: true,
		},
		{
			name:     "Delete from non-existent environment",
			envName:  "non-existent",
			connName: "any-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{},
				}
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf := tt.setupConfig()

			err := conf.DeleteConnection(tt.envName, tt.connName)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				env, exists := conf.Environments[tt.envName]
				assert.True(t, exists)

				switch tt.connName {
				case "gcp-conn":
					assert.Empty(t, env.Connections.GoogleCloudPlatform)
				case "aws-conn":
					assert.Empty(t, env.Connections.AwsConnection)
				}

				assert.False(t, env.Connections.Exists(tt.connName))
			}
		})
	}
}
