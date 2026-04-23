package config

import (
	"runtime"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
					Name:     "conn4",
					Host:     "someredshift",
					Username: "rsuser",
					Password: "rspass",
					Database: "rsdb",
					Port:     5433,
					SslMode:  "disable",
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
			Couchbase: []CouchbaseConnection{
				{
					Name:     "couchbase1",
					Host:     "couchbasehost",
					Username: "couchbaseuser",
					Password: "couchbasepass",
					Bucket:   "mybucket",
					SSL:      false,
				},
			},
			Cursor: []CursorConnection{
				{
					Name:   "cursor1",
					APIKey: "cursorapikey",
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
			Allium: []AlliumConnection{
				{
					Name:   "allium-1",
					APIKey: "allium-api-key",
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
			G2: []G2Connection{
				{
					Name:     "conn-g2",
					APIToken: "g2token",
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
			SurveyMonkey: []SurveyMonkeyConnection{
				{
					Name:        "surveymonkey-default",
					AccessToken: "test-token-123",
				},
			},
			Anthropic: []AnthropicConnection{
				{
					Name:   "anthropic-1",
					APIKey: "test-api-key",
				},
			},
			Hostaway: []HostawayConnection{
				{
					Name:   "hostaway-1",
					APIKey: "test-api-key",
				},
			},
			Intercom: []IntercomConnection{
				{
					Name:        "intercom-1",
					AccessToken: "test-access-token",
					Region:      "us",
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
			Dune: []DuneConnection{
				{
					Name:   "dune-1",
					APIKey: "dune-api-key-123",
				},
			},
			InfluxDB: []InfluxDBConnection{
				{
					Name:   "influxdb-1",
					Host:   "influxdb-host",
					Token:  "influxdb-token",
					Org:    "influxdb-org",
					Bucket: "influxdb-bucket",
					Secure: "true",
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
			RabbitMQ: []RabbitMQConnection{
				{
					Name:     "conn21",
					Host:     "localhost",
					Port:     5672,
					Username: "guest",
					Password: "guest",
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
					Players: []string{"FabianoCaruana", "Hikaru", "MagnusCarlsen", "GothamChess", "DanielNaroditsky", "AnishGiri", "Firouzja2003", "LevonAronian", "WesleySo", "GarryKasparov"},
				},
			},
			Airtable: []AirtableConnection{
				{
					Name:        "conn23",
					BaseID:      "123",
					AccessToken: "accessKey",
				},
			},
			Mailchimp: []MailchimpConnection{
				{
					Name:   "conn26",
					APIKey: "mailchimpKey",
					Server: "us10",
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
			Socrata: []SocrataConnection{
				{
					Name:     "socrata-test",
					Domain:   "data.seattle.gov",
					AppToken: "test_app_token",
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
			Docebo: []DoceboConnection{
				{
					Name:         "docebo-test",
					BaseURL:      "https://mycompany.docebosaas.com",
					ClientID:     "test-client-id",
					ClientSecret: "test-client-secret",
					Username:     "admin",
					Password:     "admin-password",
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
			SnapchatAds: []SnapchatAdsConnection{
				{
					Name:           "snapchatads-1",
					RefreshToken:   "refresh-token-123",
					ClientID:       "client-id-123",
					ClientSecret:   "client-secret-123",
					OrganizationID: "org-id-123",
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
			AppleAds: []AppleAdsConnection{
				{
					Name:     "appleads-1",
					ClientID: "SEARCHADS.client-id-123",
					TeamID:   "SEARCHADS.team-id-123",
					KeyID:    "key-id-123",
					OrgID:    "19371590",
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
			RevenueCat: []RevenueCatConnection{
				{
					Name:      "revenuecat-1",
					APIKey:    "rc_api_key_123",
					ProjectID: "proj_123456789",
				},
			},
			Linear: []LinearConnection{
				{
					Name:   "linear-1",
					APIKey: "api-key-123",
				},
			},
			GCS: []GCSConnection{
				{
					Name:               "gcs-1",
					ServiceAccountFile: "/path/to/service_account.json",
					BucketName:         "my-bucket",
					PathToFile:         "/folder1/file.csv",
					Layout:             "my_layout",
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
			Clickup: []ClickupConnection{
				{
					Name:     "clickup-1",
					APIToken: "token_123",
				},
			},
			Jobtread: []JobtreadConnection{
				{
					Name:           "jobtread-1",
					GrantKey:       "grant_key_123",
					OrganizationID: "org_123",
				},
			},
			Posthog: []PosthogConnection{
				{
					Name:           "posthog-1",
					PersonalAPIKey: "phx_test123",
					ProjectID:      "12345",
				},
			},
			QuickBooks: []QuickBooksConnection{
				{
					Name:         "quickbooks-1",
					CompanyID:    "123456",
					ClientID:     "cid",
					ClientSecret: "csecret",
					RefreshToken: "rtoken",
				},
			},
			Pinterest: []PinterestConnection{
				{
					Name:        "pinterest-1",
					AccessToken: "token",
				},
			},
			Mixpanel: []MixpanelConnection{
				{
					Name:      "mixpanel-1",
					Username:  "user-123",
					Password:  "secret-123",
					ProjectID: "12345",
					Server:    "eu",
				},
			},
			Wise: []WiseConnection{
				{
					Name:   "wise-1",
					APIKey: "token-123",
				},
			},
			Trustpilot: []TrustpilotConnection{
				{
					Name:           "trustpilot-1",
					BusinessUnitID: "unit123",
					APIKey:         "apikey",
				},
			},
			EMRServerless: []EMRServerlessConnection{
				{
					Name:          "emr_serverless-test",
					AccessKey:     "AKIAEXAMPLE",
					SecretKey:     "SECRETKEYEXAMPLE",
					Region:        "us-west-2",
					ApplicationID: "emr-app_123",
					ExecutionRole: "arn:aws:iam::123456789012:role/example_role",
					Workspace:     "s3://amzn-test-bucket/bruin-workspace/",
				},
			},
			GoogleAnalytics: []GoogleAnalyticsConnection{
				{
					Name:               "googleanalytics-1",
					ServiceAccountFile: "path/to/service_account.json",
					PropertyID:         "12345",
				},
			},
			Frankfurter: []FrankfurterConnection{
				{
					Name: "frankfurter-1",
				},
			},
			AppLovin: []AppLovinConnection{
				{
					Name:   "applovin-1",
					APIKey: "key-123",
				},
			},
			Salesforce: []SalesforceConnection{
				{
					Name:     "salesforce-1",
					Username: "username-123",
					Password: "password-123",
					Token:    "token-123",
					Domain:   "mydomain.my.salesforce.com",
				},
			},
			SQLite: []SQLiteConnection{
				{
					Name: "sqlite-1",
					Path: "C:\\path\\to\\sqlite.db",
				},
			},
			DB2: []DB2Connection{
				{
					Name:     "db2-default",
					Username: "username-123",
					Password: "password-123",
					Host:     "host-123",
					Port:     "1234",
					Database: "dbname-123",
				},
			},
			Oracle: []OracleConnection{
				{
					Name:        "oracle-1",
					Username:    "username-123",
					Password:    "password-123",
					Host:        "host-123",
					Port:        "1234",
					ServiceName: "service-123",
				},
			},
			Phantombuster: []PhantombusterConnection{
				{
					Name:   "phantombuster-1",
					APIKey: "api-key-123",
				},
			},
			Elasticsearch: []ElasticsearchConnection{
				{
					Name:        "elasticsearch-1",
					Username:    "username-123",
					Password:    "password-123",
					Host:        "host-123",
					Port:        9200,
					Secure:      "true",
					VerifyCerts: "true",
				},
			},
			Spanner: []SpannerConnection{
				{
					Name:               "spanner-1",
					ProjectID:          "project-id-123",
					InstanceID:         "instance-id-123",
					Database:           "database-id-123",
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
				},
				{
					Name:               "spanner-2",
					ProjectID:          "project-id-123",
					InstanceID:         "instance-id-123",
					Database:           "database-id-123",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
			Solidgate: []SolidgateConnection{
				{
					Name:      "solidgate-1",
					SecretKey: "secret-key-123",
					PublicKey: "public-key-123",
				},
			},
			Smartsheet: []SmartsheetConnection{
				{
					Name:        "smartsheet-1",
					AccessToken: "access-token-123",
				},
			},
			Attio: []AttioConnection{
				{
					Name:   "attio-1",
					APIKey: "api-key-123",
				},
			},
			Sftp: []SFTPConnection{
				{
					Name:     "sftp-1",
					Host:     "sftp-host",
					Port:     22,
					Username: "sftp-user",
					Password: "sftp-password",
				},
			},
			ISOCPulse: []ISOCPulseConnection{
				{
					Name:  "isoc_pulse-1",
					Token: "isoc-pulse-token-123",
				},
			},
			Zoom: []ZoomConnection{
				{
					Name:         "zoom-1",
					ClientID:     "zid",
					ClientSecret: "zsecret",
					AccountID:    "accid",
				},
			},
			Fluxx: []FluxxConnection{
				{
					Name:         "fluxx-1",
					Instance:     "test-instance",
					ClientID:     "test-client-id",
					ClientSecret: "test-client-secret",
				},
			},
			FundraiseUp: []FundraiseUpConnection{
				{
					Name:   "fundraiseup-1",
					APIKey: "test-api-key",
				},
			},
			Fireflies: []FirefliesConnection{
				{
					Name:   "fireflies-1",
					APIKey: "test-api-key",
				},
			},
			Jira: []JiraConnection{
				{
					Name:     "jira-1",
					Domain:   "company.atlassian.net",
					Email:    "user@company.com",
					APIToken: "test-api-token",
				},
			},
			Monday: []MondayConnection{
				{
					Name:     "monday-1",
					APIToken: "test-api-token",
				},
			},
			PlusVibeAI: []PlusVibeAIConnection{
				{
					Name:        "plusvibeai-1",
					APIKey:      "test-api-key",
					WorkspaceID: "test-workspace-id",
				},
			},
			BruinCloud: []BruinCloudConnection{
				{
					Name:     "bruin-1",
					APIToken: "test-api-token",
				},
			},
			Primer: []PrimerConnection{
				{
					Name:   "primer-1",
					APIKey: "test-api-key",
				},
			},
			Indeed: []IndeedConnection{
				{
					Name:         "indeed-1",
					ClientID:     "test-client-id",
					ClientSecret: "test-client-secret",
					EmployerID:   "test-employer-id",
				},
			},
			CustomerIo: []CustomerIoConnection{
				{
					Name:   "customerio-1",
					APIKey: "test-api-key",
					Region: "us",
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
		{
			name: "environment with no connections should error",
			args: args{
				path: "testdata/no_connections.yml",
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewReadOnlyFs(afero.NewOsFs())
			got, err := LoadFromFileOrEnv(fs, tt.args.path)

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
			name:     "Add Fluxx connection",
			envName:  "default",
			connType: "fluxx",
			connName: "fluxx-conn",
			creds: map[string]interface{}{
				"instance":      "mycompany.preprod",
				"client_id":     "test-client-id",
				"client_secret": "test-client-secret",
			},
			expectedErr: false,
		},
		{
			name:     "Add Anthropic connection",
			envName:  "default",
			connType: "anthropic",
			connName: "anthropic-conn",
			creds: map[string]interface{}{
				"api_key": "test-api-key",
			},
			expectedErr: false,
		},
		{
			name:     "Add Intercom connection",
			envName:  "default",
			connType: "intercom",
			connName: "intercom-conn",
			creds: map[string]interface{}{
				"access_token": "test-access-token",
				"region":       "us",
			},
			expectedErr: false,
		},
		{
			name:     "Add FundraiseUp connection",
			envName:  "default",
			connType: "fundraiseup",
			connName: "fundraiseup-conn",
			creds: map[string]interface{}{
				"api_key": "test-api-key",
			},
			expectedErr: false,
		},
		{
			name:     "Add Fireflies connection",
			envName:  "default",
			connType: "fireflies",
			connName: "fireflies-conn",
			creds: map[string]interface{}{
				"api_key": "test-api-key",
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
				case "fluxx":
					assert.Len(t, env.Connections.Fluxx, 1)
					assert.Equal(t, tt.connName, env.Connections.Fluxx[0].Name)
					assert.Equal(t, tt.creds["instance"], env.Connections.Fluxx[0].Instance)
					assert.Equal(t, tt.creds["client_id"], env.Connections.Fluxx[0].ClientID)
					assert.Equal(t, tt.creds["client_secret"], env.Connections.Fluxx[0].ClientSecret)
				case "anthropic":
					assert.Len(t, env.Connections.Anthropic, 1)
					assert.Equal(t, tt.connName, env.Connections.Anthropic[0].Name)
					assert.Equal(t, tt.creds["api_key"], env.Connections.Anthropic[0].APIKey)
				case "hostaway":
					assert.Len(t, env.Connections.Hostaway, 1)
					assert.Equal(t, tt.connName, env.Connections.Hostaway[0].Name)
					assert.Equal(t, tt.creds["api_key"], env.Connections.Hostaway[0].APIKey)
				case "intercom":
					assert.Len(t, env.Connections.Intercom, 1)
					assert.Equal(t, tt.connName, env.Connections.Intercom[0].Name)
					assert.Equal(t, tt.creds["access_token"], env.Connections.Intercom[0].AccessToken)
					assert.Equal(t, tt.creds["region"], env.Connections.Intercom[0].Region)
				case "fundraiseup":
					assert.Len(t, env.Connections.FundraiseUp, 1)
					assert.Equal(t, tt.connName, env.Connections.FundraiseUp[0].Name)
					assert.Equal(t, tt.creds["api_key"], env.Connections.FundraiseUp[0].APIKey)
				case "fireflies":
					assert.Len(t, env.Connections.Fireflies, 1)
					assert.Equal(t, tt.connName, env.Connections.Fireflies[0].Name)
					assert.Equal(t, tt.creds["api_key"], env.Connections.Fireflies[0].APIKey)
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
				Fabric:              []FabricConnection{{Name: "fabric1"}},
				Mongo:               []MongoConnection{{Name: "mongo1"}},
				Couchbase:           []CouchbaseConnection{{Name: "couchbase1"}},
				Cursor:              []CursorConnection{{Name: "cursor1"}},
				MongoAtlas:          []MongoAtlasConnection{{Name: "mongoatlas1"}},
				MySQL:               []MySQLConnection{{Name: "mysql1"}},
				Notion:              []NotionConnection{{Name: "notion1"}},
				Allium:              []AlliumConnection{{Name: "allium1"}},
				HANA:                []HANAConnection{{Name: "hana1"}},
				Hostaway:            []HostawayConnection{{Name: "hostaway1"}},
				Shopify:             []ShopifyConnection{{Name: "shopify1"}},
				Gorgias:             []GorgiasConnection{{Name: "gorgias1"}},
				G2:                  []G2Connection{{Name: "g21"}},
				Klaviyo:             []KlaviyoConnection{{Name: "klaviyo1"}},
				Adjust:              []AdjustConnection{{Name: "adjust1"}},
				SurveyMonkey:        []SurveyMonkeyConnection{{Name: "surveymonkey1"}},
				Anthropic:           []AnthropicConnection{{Name: "anthropic1"}},
				Generic:             []GenericConnection{{Name: "generic1"}},
				FacebookAds:         []FacebookAdsConnection{{Name: "facebookads1"}},
				Stripe:              []StripeConnection{{Name: "stripe1"}},
				Appsflyer:           []AppsflyerConnection{{Name: "appsflyer1"}},
				Kafka:               []KafkaConnection{{Name: "kafka1"}},
				RabbitMQ:            []RabbitMQConnection{{Name: "rabbitmq1"}},
				DuckDB:              []DuckDBConnection{{Name: "duckdb1"}},
				MotherDuck:          []MotherduckConnection{{Name: "motherduck1"}},
				ClickHouse:          []ClickHouseConnection{{Name: "clickhouse1"}},
				Hubspot:             []HubspotConnection{{Name: "hubspot1"}},
				Intercom:            []IntercomConnection{{Name: "intercom1"}},
				GitHub:              []GitHubConnection{{Name: "github1"}},
				GoogleSheets:        []GoogleSheetsConnection{{Name: "googlesheets1"}},
				Chess:               []ChessConnection{{Name: "chess1"}},
				Airtable:            []AirtableConnection{{Name: "airtable1"}},
				Zendesk:             []ZendeskConnection{{Name: "zendesk1"}},
				TikTokAds:           []TikTokAdsConnection{{Name: "tiktokads1"}},
				SnapchatAds:         []SnapchatAdsConnection{{Name: "snapchatads1"}},
				S3:                  []S3Connection{{Name: "s31"}},
				Slack:               []SlackConnection{{Name: "slack1"}},
				Socrata:             []SocrataConnection{{Name: "socrata1"}},
				Asana:               []AsanaConnection{{Name: "asana1"}},
				DynamoDB:            []DynamoDBConnection{{Name: "dynamodb1"}},
				Docebo:              []DoceboConnection{{Name: "docebo1"}},
				GoogleAds:           []GoogleAdsConnection{{Name: "googleads1"}},
				AppStore:            []AppStoreConnection{{Name: "appstore1"}},
				AppleAds:            []AppleAdsConnection{{Name: "appleads1"}},
				LinkedInAds:         []LinkedInAdsConnection{{Name: "linkedinads1"}},
				Mailchimp:           []MailchimpConnection{{Name: "mailchimp1"}},
				RevenueCat:          []RevenueCatConnection{{Name: "revenuecat1"}},
				Linear:              []LinearConnection{{Name: "linear1"}},
				GCS:                 []GCSConnection{{Name: "gcs1"}},
				ApplovinMax:         []ApplovinMaxConnection{{Name: "applovinmax1"}},
				Personio:            []PersonioConnection{{Name: "personio1"}},
				Kinesis:             []KinesisConnection{{Name: "kinesis1"}},
				Pipedrive:           []PipedriveConnection{{Name: "pipedrive1"}},
				Mixpanel:            []MixpanelConnection{{Name: "mixpanel1"}},
				Clickup:             []ClickupConnection{{Name: "clickup1"}},
				Jobtread:            []JobtreadConnection{{Name: "jobtread1"}},
				Posthog:             []PosthogConnection{{Name: "posthog1"}},
				Pinterest:           []PinterestConnection{{Name: "pinterest1"}},
				Trustpilot:          []TrustpilotConnection{{Name: "trustpilot1"}},
				QuickBooks:          []QuickBooksConnection{{Name: "quickbooks1"}},
				Wise:                []WiseConnection{{Name: "wise1"}},
				Zoom:                []ZoomConnection{{Name: "zoom1"}},
				EMRServerless:       []EMRServerlessConnection{{Name: "emr1"}},
				DataprocServerless:  []DataprocServerlessConnection{{Name: "dataproc1"}},
				GoogleAnalytics:     []GoogleAnalyticsConnection{{Name: "googleanalytics1"}},
				AppLovin:            []AppLovinConnection{{Name: "applovin1"}},
				Frankfurter:         []FrankfurterConnection{{Name: "frankfurter1"}},
				Salesforce:          []SalesforceConnection{{Name: "salesforce1"}},
				SQLite:              []SQLiteConnection{{Name: "sqlite1"}},
				DB2:                 []DB2Connection{{Name: "db21"}},
				Oracle:              []OracleConnection{{Name: "oracle1"}},
				Phantombuster:       []PhantombusterConnection{{Name: "phantombuster1"}},
				Elasticsearch:       []ElasticsearchConnection{{Name: "elasticsearch1"}},
				Solidgate:           []SolidgateConnection{{Name: "solidgate1"}},
				Spanner:             []SpannerConnection{{Name: "spanner1"}},
				Smartsheet:          []SmartsheetConnection{{Name: "smartsheet1"}},
				Attio:               []AttioConnection{{Name: "attio1"}},
				Sftp:                []SFTPConnection{{Name: "sftp1"}},
				ISOCPulse:           []ISOCPulseConnection{{Name: "isocpulse1"}},
				InfluxDB:            []InfluxDBConnection{{Name: "influxdb1"}},
				Tableau:             []TableauConnection{{Name: "tableau1"}},
				QuickSight:          []QuickSightConnection{{Name: "quicksight1"}},
				Trino:               []TrinoConnection{{Name: "trino1"}},
				Fluxx:               []FluxxConnection{{Name: "fluxx1"}},
				Freshdesk:           []FreshdeskConnection{{Name: "freshdesk1"}},
				FundraiseUp:         []FundraiseUpConnection{{Name: "fundraiseup1"}},
				Fireflies:           []FirefliesConnection{{Name: "fireflies1"}},
				Jira:                []JiraConnection{{Name: "jira1"}},
				Monday:              []MondayConnection{{Name: "monday1"}},
				PlusVibeAI:          []PlusVibeAIConnection{{Name: "plusvibeai1"}},
				BruinCloud:          []BruinCloudConnection{{Name: "bruin1"}},
				Primer:              []PrimerConnection{{Name: "primer1"}},
				Indeed:              []IndeedConnection{{Name: "indeed1"}},
				CustomerIo:          []CustomerIoConnection{{Name: "customerio1"}},
				Vertica:             []VerticaConnection{{Name: "vertica1"}},
				Dune:                []DuneConnection{{Name: "dune1"}},
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
				Fabric:              []FabricConnection{{Name: "fabric1"}},
				Mongo:               []MongoConnection{{Name: "mongo1"}},
				Couchbase:           []CouchbaseConnection{{Name: "couchbase1"}},
				Cursor:              []CursorConnection{{Name: "cursor1"}},
				MongoAtlas:          []MongoAtlasConnection{{Name: "mongoatlas1"}},
				MySQL:               []MySQLConnection{{Name: "mysql1"}},
				Notion:              []NotionConnection{{Name: "notion1"}},
				Allium:              []AlliumConnection{{Name: "allium1"}},
				HANA:                []HANAConnection{{Name: "hana1"}},
				Hostaway:            []HostawayConnection{{Name: "hostaway1"}},
				Shopify:             []ShopifyConnection{{Name: "shopify1"}},
				Gorgias:             []GorgiasConnection{{Name: "gorgias1"}},
				G2:                  []G2Connection{{Name: "g21"}},
				Klaviyo:             []KlaviyoConnection{{Name: "klaviyo1"}},
				Adjust:              []AdjustConnection{{Name: "adjust1"}},
				SurveyMonkey:        []SurveyMonkeyConnection{{Name: "surveymonkey1"}},
				Anthropic:           []AnthropicConnection{{Name: "anthropic1"}},
				Generic:             []GenericConnection{{Name: "generic1"}},
				FacebookAds:         []FacebookAdsConnection{{Name: "facebookads1"}},
				Stripe:              []StripeConnection{{Name: "stripe1"}},
				Appsflyer:           []AppsflyerConnection{{Name: "appsflyer1"}},
				Kafka:               []KafkaConnection{{Name: "kafka1"}},
				RabbitMQ:            []RabbitMQConnection{{Name: "rabbitmq1"}},
				DuckDB:              []DuckDBConnection{{Name: "duckdb1"}},
				MotherDuck:          []MotherduckConnection{{Name: "motherduck1"}},
				ClickHouse:          []ClickHouseConnection{{Name: "clickhouse1"}},
				Hubspot:             []HubspotConnection{{Name: "hubspot1"}},
				Intercom:            []IntercomConnection{{Name: "intercom1"}},
				GitHub:              []GitHubConnection{{Name: "github1"}},
				GoogleSheets:        []GoogleSheetsConnection{{Name: "googlesheets1"}},
				Chess:               []ChessConnection{{Name: "chess1"}},
				Airtable:            []AirtableConnection{{Name: "airtable1"}},
				Zendesk:             []ZendeskConnection{{Name: "zendesk1"}},
				TikTokAds:           []TikTokAdsConnection{{Name: "tiktokads1"}},
				SnapchatAds:         []SnapchatAdsConnection{{Name: "snapchatads1"}},
				S3:                  []S3Connection{{Name: "s31"}},
				Slack:               []SlackConnection{{Name: "slack1"}},
				Socrata:             []SocrataConnection{{Name: "socrata1"}},
				Asana:               []AsanaConnection{{Name: "asana1"}},
				DynamoDB:            []DynamoDBConnection{{Name: "dynamodb1"}},
				Docebo:              []DoceboConnection{{Name: "docebo1"}},
				GoogleAds:           []GoogleAdsConnection{{Name: "googleads1"}},
				AppStore:            []AppStoreConnection{{Name: "appstore1"}},
				AppleAds:            []AppleAdsConnection{{Name: "appleads1"}},
				LinkedInAds:         []LinkedInAdsConnection{{Name: "linkedinads1"}},
				Mailchimp:           []MailchimpConnection{{Name: "mailchimp1"}},
				RevenueCat:          []RevenueCatConnection{{Name: "revenuecat1"}},
				Linear:              []LinearConnection{{Name: "linear1"}},
				GCS:                 []GCSConnection{{Name: "gcs1"}},
				ApplovinMax:         []ApplovinMaxConnection{{Name: "applovinmax1"}},
				Personio:            []PersonioConnection{{Name: "personio1"}},
				Kinesis:             []KinesisConnection{{Name: "kinesis1"}},
				Pipedrive:           []PipedriveConnection{{Name: "pipedrive1"}},
				Mixpanel:            []MixpanelConnection{{Name: "mixpanel1"}},
				Clickup:             []ClickupConnection{{Name: "clickup1"}},
				Jobtread:            []JobtreadConnection{{Name: "jobtread1"}},
				Posthog:             []PosthogConnection{{Name: "posthog1"}},
				Pinterest:           []PinterestConnection{{Name: "pinterest1"}},
				Trustpilot:          []TrustpilotConnection{{Name: "trustpilot1"}},
				QuickBooks:          []QuickBooksConnection{{Name: "quickbooks1"}},
				Wise:                []WiseConnection{{Name: "wise1"}},
				Zoom:                []ZoomConnection{{Name: "zoom1"}},
				EMRServerless:       []EMRServerlessConnection{{Name: "emr1"}},
				DataprocServerless:  []DataprocServerlessConnection{{Name: "dataproc1"}},
				GoogleAnalytics:     []GoogleAnalyticsConnection{{Name: "googleanalytics1"}},
				AppLovin:            []AppLovinConnection{{Name: "applovin1"}},
				Frankfurter:         []FrankfurterConnection{{Name: "frankfurter1"}},
				Salesforce:          []SalesforceConnection{{Name: "salesforce1"}},
				SQLite:              []SQLiteConnection{{Name: "sqlite1"}},
				DB2:                 []DB2Connection{{Name: "db21"}},
				Oracle:              []OracleConnection{{Name: "oracle1"}},
				Phantombuster:       []PhantombusterConnection{{Name: "phantombuster1"}},
				Elasticsearch:       []ElasticsearchConnection{{Name: "elasticsearch1"}},
				Solidgate:           []SolidgateConnection{{Name: "solidgate1"}},
				Spanner:             []SpannerConnection{{Name: "spanner1"}},
				Smartsheet:          []SmartsheetConnection{{Name: "smartsheet1"}},
				Attio:               []AttioConnection{{Name: "attio1"}},
				Sftp:                []SFTPConnection{{Name: "sftp1"}},
				ISOCPulse:           []ISOCPulseConnection{{Name: "isocpulse1"}},
				InfluxDB:            []InfluxDBConnection{{Name: "influxdb1"}},
				Tableau:             []TableauConnection{{Name: "tableau1"}},
				QuickSight:          []QuickSightConnection{{Name: "quicksight1"}},
				Trino:               []TrinoConnection{{Name: "trino1"}},
				Fluxx:               []FluxxConnection{{Name: "fluxx1"}},
				Freshdesk:           []FreshdeskConnection{{Name: "freshdesk1"}},
				FundraiseUp:         []FundraiseUpConnection{{Name: "fundraiseup1"}},
				Fireflies:           []FirefliesConnection{{Name: "fireflies1"}},
				Jira:                []JiraConnection{{Name: "jira1"}},
				Monday:              []MondayConnection{{Name: "monday1"}},
				PlusVibeAI:          []PlusVibeAIConnection{{Name: "plusvibeai1"}},
				BruinCloud:          []BruinCloudConnection{{Name: "bruin1"}},
				Primer:              []PrimerConnection{{Name: "primer1"}},
				Indeed:              []IndeedConnection{{Name: "indeed1"}},
				CustomerIo:          []CustomerIoConnection{{Name: "customerio1"}},
				Vertica:             []VerticaConnection{{Name: "vertica1"}},
				Dune:                []DuneConnection{{Name: "dune1"}},
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

func TestLoadFromFileOrEnv_EnvironmentVariable(t *testing.T) {
	// Test data from environ.yml
	envConfigContent := `default_environment: dev
environments:
  dev:
    connections:
      google_cloud_platform:
        - name: conn1
          service_account_json: "{\"key10\": \"value10\"}"
          project_id: "my-project"`

	envConfigContentMalformedConnections := `default_environment: default
environments:
    default:
    connections:
            duckdb:
                - name: duckdb-default
                  path: duckdb.db`

	expectedConfig := &Config{
		DefaultEnvironmentName:  "dev",
		SelectedEnvironmentName: "dev",
		SelectedEnvironment: &Environment{
			Connections: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{
						Name:               "conn1",
						ServiceAccountJSON: "{\"key10\": \"value10\"}",
						ProjectID:          "my-project",
					},
				},
			},
		},
		Environments: map[string]Environment{
			"dev": {
				Connections: &Connections{
					GoogleCloudPlatform: []GoogleCloudPlatformConnection{
						{
							Name:               "conn1",
							ServiceAccountJSON: "{\"key10\": \"value10\"}",
							ProjectID:          "my-project",
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name           string
		envConfig      string
		configFilePath string
		want           *Config
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name:           "load from environment variable when file doesn't exist",
			envConfig:      envConfigContent,
			configFilePath: "testdata/nonexistent.yml",
			want:           expectedConfig,
			wantErr:        assert.NoError,
		},
		{
			name:           "load from environment variable with invalid YAML",
			envConfig:      "invalid: yaml: content",
			configFilePath: "testdata/nonexistent.yml",
			want:           nil,
			wantErr:        assert.Error,
		},
		{
			name:           "load from environment variable with empty content",
			envConfig:      "",
			configFilePath: "testdata/nonexistent.yml",
			want:           nil,
			wantErr:        assert.Error,
		},
		{
			name:           "environment with no connections should error",
			envConfig:      envConfigContentMalformedConnections,
			configFilePath: "testdata/nonexistent.yml",
			want:           nil,
			wantErr:        assert.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variable
			if tt.envConfig != "" {
				t.Setenv("BRUIN_CONFIG_FILE_CONTENT", tt.envConfig)
			} else {
				t.Setenv("BRUIN_CONFIG_FILE_CONTENT", "")
			}

			fs := afero.NewReadOnlyFs(afero.NewOsFs())
			got, err := LoadFromFileOrEnv(fs, tt.configFilePath)

			tt.wantErr(t, err)
			if tt.want != nil {
				tt.want.fs = fs
				tt.want.path = tt.configFilePath
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

func TestSnowflakeConnection_MarshalYAML_PrivateKey(t *testing.T) {
	t.Parallel()

	privateKey := `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7ZtFJp7nR3sL9
Xy8kPq2jLmV9bN3oR6tUwVcXzA1bH2cY4eF5gH6jK7lM8nBvCxDsEwQrTyUiOp
-----END PRIVATE KEY-----`

	conn := SnowflakeConnection{
		Name:       "test-snowflake",
		Account:    "test-account",
		Username:   "test-user",
		Database:   "test-db",
		PrivateKey: privateKey,
	}

	// Marshal to YAML
	yamlData, err := yaml.Marshal(&conn)
	require.NoError(t, err)

	yamlStr := string(yamlData)

	// Verify that the YAML contains literal block style indicator (|)
	assert.Contains(t, yamlStr, "private_key: |", "private_key should use literal block scalar style")

	// Verify that the YAML contains the BEGIN marker on its own line (not inline)
	assert.Contains(t, yamlStr, "-----BEGIN PRIVATE KEY-----", "private_key should contain BEGIN marker")
	assert.Contains(t, yamlStr, "-----END PRIVATE KEY-----", "private_key should contain END marker")

	// Verify we can unmarshal it back correctly
	var unmarshaled SnowflakeConnection
	err = yaml.Unmarshal(yamlData, &unmarshaled)
	require.NoError(t, err)

	// The unmarshaled private key should have proper newlines
	assert.Contains(t, unmarshaled.PrivateKey, "\n", "unmarshaled private_key should contain newlines")
	assert.Contains(t, unmarshaled.PrivateKey, "-----BEGIN PRIVATE KEY-----", "unmarshaled private_key should contain BEGIN marker")
	assert.Contains(t, unmarshaled.PrivateKey, "-----END PRIVATE KEY-----", "unmarshaled private_key should contain END marker")
}

func TestSnowflakeConnection_MarshalYAML_WithoutPrivateKey(t *testing.T) {
	t.Parallel()

	conn := SnowflakeConnection{
		Name:     "test-snowflake",
		Account:  "test-account",
		Username: "test-user",
		Password: "test-pass",
		Database: "test-db",
	}

	// Marshal to YAML
	yamlData, err := yaml.Marshal(&conn)
	require.NoError(t, err)

	yamlStr := string(yamlData)

	// Verify that the YAML does not contain private_key field
	assert.NotContains(t, yamlStr, "private_key:", "YAML should not contain private_key when not set")

	// Verify it contains the other fields
	assert.Contains(t, yamlStr, "name: test-snowflake")
	assert.Contains(t, yamlStr, "account: test-account")
	assert.Contains(t, yamlStr, "username: test-user")
	assert.Contains(t, yamlStr, "password: test-pass")
}

func TestSnowflakeConnection_MarshalYAML_SingleLinePrivateKey(t *testing.T) {
	t.Parallel()

	// This is how a private key might come from the VS Code extension - all on one line with spaces
	singleLineKey := "-----BEGIN PRIVATE KEY----- MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7ZtFJp -----END PRIVATE KEY-----"

	conn := SnowflakeConnection{
		Name:       "test-snowflake",
		Account:    "test-account",
		Username:   "test-user",
		Database:   "test-db",
		PrivateKey: singleLineKey,
	}

	// Marshal to YAML
	yamlData, err := yaml.Marshal(&conn)
	require.NoError(t, err)

	yamlStr := string(yamlData)

	// Verify that the YAML contains literal block style indicator (|)
	assert.Contains(t, yamlStr, "private_key: |", "private_key should use literal block scalar style")

	// Verify we can unmarshal it back correctly
	var unmarshaled SnowflakeConnection
	err = yaml.Unmarshal(yamlData, &unmarshaled)
	require.NoError(t, err)

	// The unmarshaled private key should have proper newlines (header, content, footer on separate lines)
	assert.Contains(t, unmarshaled.PrivateKey, "-----BEGIN PRIVATE KEY-----\n", "header should be followed by newline")
	assert.Contains(t, unmarshaled.PrivateKey, "\n-----END PRIVATE KEY-----", "footer should be preceded by newline")

	// Content should not have spaces (they should be removed during normalization)
	lines := strings.Split(unmarshaled.PrivateKey, "\n")
	if len(lines) >= 2 {
		contentLine := lines[1]
		assert.NotContains(t, contentLine, " ", "content line should not contain spaces")
	}
}
