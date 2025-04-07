package config

import (
	"runtime"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromFile(t *testing.T) {
	t.Parallel()

	var servicefile, duckPath, configFile string
	if runtime.GOOS == "windows" {
		configFile = "simple_win.yml"
		duckPath = "C:\\path\\to\\duck.db"
		servicefile = "D:\\path\\to\\service_account.json"
	} else {
		configFile = "simple.yml"
		duckPath = "/path/to/duck.db"
		servicefile = "/path/to/service_account.json"
	}

	clickhouseSecureValue := 0

	devEnv := Environment{
		Connections: &Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
					ServiceAccountFile: servicefile,
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
			RedShift: []RedshiftConnection{
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
			Synapse: []SynapseConnection{
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
					Path: duckPath,
				},
			},

			ClickHouse: []ClickHouseConnection{
				{
					Name:     "conn-clickhouse",
					Host:     "clickhousehost",
					Port:     8123,
					Username: "clickhouseuser",
					Password: "clickhousepass",
					Database: "clickhousedb",
					HTTPPort: 8124,
					Secure:   &clickhouseSecureValue,
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
					Name:               "conn22",
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
				},
				{
					Name:               "conn22-1",
					ServiceAccountFile: servicefile,
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
					BaseID:      "123",
					AccessToken: "accessKey",
				},
			},
			S3: []S3Connection{
				{
					Name:            "conn25",
					BucketName:      "my-bucket",
					PathToFile:      "/folder1/file.csv",
					AccessKeyID:     "123Key",
					SecretAccessKey: "secretKey123",
				},
			},
			Slack: []SlackConnection{
				{
					Name:   "conn26",
					APIKey: "slackkey",
				},
			},
			Asana: []AsanaConnection{
				{
					Name:        "asana-workspace-1337",
					AccessToken: "access_token_pawn3d",
					WorkspaceID: "1337",
				},
			},
			DynamoDB: []DynamoDBConnection{
				{
					Name:            "dynamodb-432123",
					AccessKeyID:     "access-key-786",
					SecretAccessKey: "shhh,secret",
					Region:          "ap-south-1",
				},
			},
			Zendesk: []ZendeskConnection{
				{
					Name:      "conn25",
					APIToken:  "zendeskKey",
					Email:     "zendeskemail",
					Subdomain: "zendeskUrl",
				},
				{
					Name:       "conn25-1",
					OAuthToken: "zendeskToken",
					Subdomain:  "zendeskUrl",
				},
			},
			GoogleAds: []GoogleAdsConnection{
				{
					Name:               "googleads-0",
					DeveloperToken:     "dev-0",
					CustomerID:         "1234567890",
					ServiceAccountJSON: `{"email": "no-reply@googleads-0.com"}`,
				},
			},
			TikTokAds: []TikTokAdsConnection{
				{
					Name:          "tiktokads-1",
					AccessToken:   "access-token-123",
					AdvertiserIDs: "advertiser-id-123,advertiser-id-456",
					Timezone:      "UTC",
				},
			},
			GitHub: []GitHubConnection{
				{
					Name:  "github-1",
					Owner: "owner-456",
					Repo:  "repo-456",
				},
				{
					Name:        "github-2",
					AccessToken: "token-123",
					Owner:       "owner-456",
					Repo:        "repo-456",
				},
			},
			AppStore: []AppStoreConnection{
				{
					Name:     "appstore-1",
					IssuerID: "issuer-id-123",
					KeyID:    "key-id-123",
					KeyPath:  "/path/to/key.pem",
				},
			},
			LinkedInAds: []LinkedInAdsConnection{
				{
					Name:        "linkedinads-1",
					AccessToken: "access-token-123",
					AccountIds:  "account-id-123,account-id-456",
				},
			},
			GCS: []GCSConnection{
				{
					Name:               "gcs-1",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
			ApplovinMax: []ApplovinMaxConnection{
				{
					Name:   "applovinmax-1",
					APIKey: "api-key-123",
				},
			},
			Personio: []PersonioConnection{
				{
					Name:         "personio-1",
					ClientID:     "client-id-123",
					ClientSecret: "client-secret-123",
				},
			},
			Kinesis: []KinesisConnection{
				{
					Name:            "kinesis-1",
					AccessKeyID:     "aws-access-key-id-123",
					SecretAccessKey: "aws-secret-access-key-123",
					Region:          "us-east-1",
				},
			},
			Pipedrive: []PipedriveConnection{
				{
					Name:     "pipedrive-1",
					APIToken: "token-123",
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
				path: "testdata/" + configFile,
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
									ServiceAccountFile: servicefile,
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

	var servicefile string
	if runtime.GOOS == "windows" {
		servicefile = "C:\\path\\to\\service_account.json\""
	} else {
		servicefile = "/path/to/service_account.json"
	}
	configPath := "/some/path/to/config.yml"
	defaultEnv := &Environment{
		Connections: &Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: servicefile,
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

func TestLoadOrCreateWithoutPathAbsolutization(t *testing.T) {
	t.Parallel()

	servicefile := "path/to/service_account.json"
	configPath := "some/path/to/config.yml"
	defaultEnv := &Environment{
		Connections: &Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: servicefile,
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
				err = afero.WriteFile(args.fs, "some/path/to/.gitignore", []byte("file1"), 0o644)
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

			got, err := LoadOrCreateWithoutPathAbsolutization(a.fs, configPath)
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
				content, err := afero.ReadFile(a.fs, "some/path/to/.gitignore")
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

func TestCanRunTaskInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		config         Config
		pipeline       pipeline.Pipeline
		expectedErr    bool
		expectedCanRun bool
	}{
		{
			name:   "no connections, no assets",
			config: Config{},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{},
			},
			expectedErr: false,
		},
		{
			name: "specific connection, not found",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "conn1",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeBigqueryQuery,
						Connection: "conn2",
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "specific connection, found",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "conn1",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeBigqueryQuery,
						Connection: "conn1",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "no specific connection, not found",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "gcp-default",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: pipeline.AssetType("sf.sql"),
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "no specific connection, found",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "gcp-default",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: pipeline.AssetTypeBigqueryQuery,
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "2 assets 2 conn missing",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "conn1",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeBigqueryQuery,
						Connection: "conn1",
					},
					{
						Type:       pipeline.AssetTypeIngestr,
						Connection: "conn2",
						Parameters: map[string]string{"source_connection": "conn3"},
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "2 assets 2 conns",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "conn1",
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								Name: "conn2",
							},
							{
								Name: "conn3",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				DefaultConnections: map[string]string{
					"snowflake": "conn2",
				},
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeBigqueryQuery,
						Connection: "conn1",
					},
					{
						Type: pipeline.AssetTypeIngestr,
						Parameters: map[string]string{
							"source_connection": "conn3",
							"destination":       "snowflake",
						},
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "3 assets 1 python secret missing",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "conn1",
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								Name: "conn2",
							},
							{
								Name: "conn3",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				DefaultConnections: map[string]string{
					"snowflake": "conn2",
				},
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeBigqueryQuery,
						Connection: "conn1",
					},
					{
						Type: pipeline.AssetTypeIngestr,
						Parameters: map[string]string{
							"source_connection": "conn3",
							"destination":       "snowflake",
						},
					},
					{
						Type: pipeline.AssetTypePython,
						Secrets: []pipeline.SecretMapping{
							{
								SecretKey: "some_key",
							},
						},
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "3 assets 1 python secret",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "conn1",
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								Name: "conn2",
							},
							{
								Name: "conn3",
							},
						},
						Generic: []GenericConnection{
							{
								Name: "some_key",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				DefaultConnections: map[string]string{
					"snowflake": "conn2",
				},
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeBigqueryQuery,
						Connection: "conn1",
					},
					{
						Type: pipeline.AssetTypeIngestr,
						Parameters: map[string]string{
							"source_connection": "conn3",
							"destination":       "snowflake",
						},
					},
					{
						Type: pipeline.AssetTypePython,
						Secrets: []pipeline.SecretMapping{
							{
								SecretKey: "some_key",
							},
						},
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "defaults",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						GoogleCloudPlatform: []GoogleCloudPlatformConnection{
							{
								Name: "gcp-default",
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								Name: "snowflake-default",
							},
							{
								Name: "conn3",
							},
						},
						Generic: []GenericConnection{
							{
								Name: "some_key",
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type: pipeline.AssetTypeBigqueryQuery,
					},
					{
						Type: pipeline.AssetTypeIngestr,
						Parameters: map[string]string{
							"source_connection": "conn3",
							"destination":       "snowflake",
						},
					},
					{
						Type: pipeline.AssetTypePython,
						Secrets: []pipeline.SecretMapping{
							{
								SecretKey: "some_key",
							},
						},
					},
				},
			},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tasks := make([]scheduler.TaskInstance, 0)

			for _, asset := range tt.pipeline.Assets {
				tasks = append(tasks, &scheduler.AssetInstance{Asset: asset})
			}

			err := tt.config.CanRunTaskInstances(&tt.pipeline, tasks)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConnections_MergeFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		target      *Connections
		source      *Connections
		want        *Connections
		expectedErr bool
	}{
		{
			name:        "nil source should error",
			target:      &Connections{},
			source:      nil,
			expectedErr: true,
		},
		{
			name: "merge empty source should not change target",
			target: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{Name: "existing-conn"},
				},
			},
			source: &Connections{},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{Name: "existing-conn"},
				},
			},
			expectedErr: false,
		},
		{
			name: "merge multiple connection types",
			target: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{Name: "gcp1"},
				},
				Snowflake: []SnowflakeConnection{
					{Name: "sf1"},
				},
			},
			source: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{Name: "gcp2"},
				},
				Postgres: []PostgresConnection{
					{Name: "pg1"},
				},
			},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{Name: "gcp1"},
					{Name: "gcp2"},
				},
				Snowflake: []SnowflakeConnection{
					{Name: "sf1"},
				},
				Postgres: []PostgresConnection{
					{Name: "pg1"},
				},
			},
			expectedErr: false,
		},
		{
			name:   "merge into empty target",
			target: &Connections{},
			source: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{Name: "gcp1"},
				},
				Snowflake: []SnowflakeConnection{
					{Name: "sf1"},
				},
			},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{Name: "gcp1"},
				},
				Snowflake: []SnowflakeConnection{
					{Name: "sf1"},
				},
			},
			expectedErr: false,
		},
		{
			name: "merge connections with same name but different types",
			target: &Connections{
				Snowflake: []SnowflakeConnection{
					{Name: "prod-db"},
				},
			},
			source: &Connections{
				Postgres: []PostgresConnection{
					{Name: "prod-db"},
				},
			},
			want: &Connections{
				Snowflake: []SnowflakeConnection{
					{Name: "prod-db"},
				},
				Postgres: []PostgresConnection{
					{Name: "prod-db"},
				},
			},
			expectedErr: false,
		},
		{
			name: "merge development and production environments",
			target: &Connections{
				Snowflake: []SnowflakeConnection{
					{
						Name:     "dev-db",
						Username: "dev_user",
						Password: "dev_pass",
						Database: "dev_db",
					},
				},
				Postgres: []PostgresConnection{
					{
						Name:     "dev-pg",
						Username: "dev_pg_user",
						Password: "dev_pg_pass",
						Database: "dev_pg_db",
					},
				},
			},
			source: &Connections{
				Snowflake: []SnowflakeConnection{
					{
						Name:     "prod-db",
						Username: "prod_user",
						Password: "prod_pass",
						Database: "prod_db",
					},
				},
				Postgres: []PostgresConnection{
					{
						Name:     "prod-pg",
						Username: "prod_pg_user",
						Password: "prod_pg_pass",
						Database: "prod_pg_db",
					},
				},
			},
			want: &Connections{
				Snowflake: []SnowflakeConnection{
					{
						Name:     "dev-db",
						Username: "dev_user",
						Password: "dev_pass",
						Database: "dev_db",
					},
					{
						Name:     "prod-db",
						Username: "prod_user",
						Password: "prod_pass",
						Database: "prod_db",
					},
				},
				Postgres: []PostgresConnection{
					{
						Name:     "dev-pg",
						Username: "dev_pg_user",
						Password: "dev_pg_pass",
						Database: "dev_pg_db",
					},
					{
						Name:     "prod-pg",
						Username: "prod_pg_user",
						Password: "prod_pg_pass",
						Database: "prod_pg_db",
					},
				},
			},
			expectedErr: false,
		},
		{
			name:   "merge with all connection types",
			target: &Connections{},
			source: &Connections{
				AwsConnection:       []AwsConnection{{Name: "aws1"}},
				AthenaConnection:    []AthenaConnection{{Name: "athena1"}},
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{{Name: "gcp1"}},
				Snowflake:           []SnowflakeConnection{{Name: "sf1"}},
				Postgres:            []PostgresConnection{{Name: "pg1"}},
				RedShift:            []RedshiftConnection{{Name: "rs1"}},
				MsSQL:               []MsSQLConnection{{Name: "mssql1"}},
				Databricks:          []DatabricksConnection{{Name: "db1"}},
				Synapse:             []SynapseConnection{{Name: "syn1"}},
				Mongo:               []MongoConnection{{Name: "mongo1"}},
				MySQL:               []MySQLConnection{{Name: "mysql1"}},
				Notion:              []NotionConnection{{Name: "notion1"}},
				HANA:                []HANAConnection{{Name: "hana1"}},
				Shopify:             []ShopifyConnection{{Name: "shopify1"}},
				Gorgias:             []GorgiasConnection{{Name: "gorgias1"}},
				Klaviyo:             []KlaviyoConnection{{Name: "klaviyo1"}},
				DuckDB:              []DuckDBConnection{{Name: "duckdb1"}},
				ClickHouse:          []ClickHouseConnection{{Name: "clickhouse1"}},
			},
			want: &Connections{
				AwsConnection:       []AwsConnection{{Name: "aws1"}},
				AthenaConnection:    []AthenaConnection{{Name: "athena1"}},
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{{Name: "gcp1"}},
				Snowflake:           []SnowflakeConnection{{Name: "sf1"}},
				Postgres:            []PostgresConnection{{Name: "pg1"}},
				RedShift:            []RedshiftConnection{{Name: "rs1"}},
				MsSQL:               []MsSQLConnection{{Name: "mssql1"}},
				Databricks:          []DatabricksConnection{{Name: "db1"}},
				Synapse:             []SynapseConnection{{Name: "syn1"}},
				Mongo:               []MongoConnection{{Name: "mongo1"}},
				MySQL:               []MySQLConnection{{Name: "mysql1"}},
				Notion:              []NotionConnection{{Name: "notion1"}},
				HANA:                []HANAConnection{{Name: "hana1"}},
				Shopify:             []ShopifyConnection{{Name: "shopify1"}},
				Gorgias:             []GorgiasConnection{{Name: "gorgias1"}},
				Klaviyo:             []KlaviyoConnection{{Name: "klaviyo1"}},
				DuckDB:              []DuckDBConnection{{Name: "duckdb1"}},
				ClickHouse:          []ClickHouseConnection{{Name: "clickhouse1"}},
			},
			expectedErr: false,
		},
		{
			name: "merge existing GCP connections",
			target: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{
						Name:               "gcp-dev",
						ServiceAccountJSON: "{\"key\": \"dev-value\"}",
						ProjectID:          "dev-project",
					},
					{
						Name:               "gcp-staging",
						ServiceAccountJSON: "{\"key\": \"staging-value\"}",
						ProjectID:          "staging-project",
					},
				},
			},
			source: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{
						Name:               "gcp-prod",
						ServiceAccountJSON: "{\"key\": \"prod-value\"}",
						ProjectID:          "prod-project",
					},
				},
			},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{
						Name:               "gcp-dev",
						ServiceAccountJSON: "{\"key\": \"dev-value\"}",
						ProjectID:          "dev-project",
					},
					{
						Name:               "gcp-staging",
						ServiceAccountJSON: "{\"key\": \"staging-value\"}",
						ProjectID:          "staging-project",
					},
					{
						Name:               "gcp-prod",
						ServiceAccountJSON: "{\"key\": \"prod-value\"}",
						ProjectID:          "prod-project",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "merge existing Postgres and MySQL connections",
			target: &Connections{
				Postgres: []PostgresConnection{
					{
						Name:     "pg-dev",
						Host:     "dev-host",
						Port:     5432,
						Username: "dev-user",
						Password: "dev-pass",
						Database: "dev-db",
					},
				},
				MySQL: []MySQLConnection{
					{
						Name:     "mysql-dev",
						Host:     "dev-host",
						Port:     3306,
						Username: "dev-user",
						Password: "dev-pass",
						Database: "dev-db",
					},
				},
			},
			source: &Connections{
				Postgres: []PostgresConnection{
					{
						Name:     "pg-prod",
						Host:     "prod-host",
						Port:     5432,
						Username: "prod-user",
						Password: "prod-pass",
						Database: "prod-db",
					},
				},
				MySQL: []MySQLConnection{
					{
						Name:     "mysql-prod",
						Host:     "prod-host",
						Port:     3306,
						Username: "prod-user",
						Password: "prod-pass",
						Database: "prod-db",
					},
				},
			},
			want: &Connections{
				Postgres: []PostgresConnection{
					{
						Name:     "pg-dev",
						Host:     "dev-host",
						Port:     5432,
						Username: "dev-user",
						Password: "dev-pass",
						Database: "dev-db",
					},
					{
						Name:     "pg-prod",
						Host:     "prod-host",
						Port:     5432,
						Username: "prod-user",
						Password: "prod-pass",
						Database: "prod-db",
					},
				},
				MySQL: []MySQLConnection{
					{
						Name:     "mysql-dev",
						Host:     "dev-host",
						Port:     3306,
						Username: "dev-user",
						Password: "dev-pass",
						Database: "dev-db",
					},
					{
						Name:     "mysql-prod",
						Host:     "prod-host",
						Port:     3306,
						Username: "prod-user",
						Password: "prod-pass",
						Database: "prod-db",
					},
				},
			},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.target.MergeFrom(tt.source)

			if tt.expectedErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.EqualExportedValues(t, tt.want, tt.target)
		})
	}
}
