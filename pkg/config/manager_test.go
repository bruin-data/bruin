package config

import (
	"encoding/json"
	"path/filepath"
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

const testAWSRegion = "us-east-1"

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
	sharePointMaxFileSize := int64(104857600)
	sharePointMaxFiles := int64(10000)
	adaptyLookbackDays := 60

	devEnv := Environment{
		Connections: &Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
					ServiceAccountFile: servicefile,
					ProjectID:          "my-project",
				},
			},
			Snowflake: []SnowflakeConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn2"},
					Username:           "user",
					Password:           "pass",
					Account:            "account",
					Region:             "region",
					Role:               "role",
					Database:           "db",
					Schema:             "schema",
					Warehouse:          "wh",
				},
			},
			Postgres: []PostgresConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn3"},
					Host:               "somehost",
					Username:           "pguser",
					Password:           "pgpass",
					Database:           "pgdb",
					Schema:             "non_public_schema",
					Port:               5432,
					PoolMaxConns:       5,
					SslMode:            "require",
				},
			},
			RedShift: []RedshiftConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn4"},
					Host:               "someredshift",
					Username:           "rsuser",
					Password:           "rspass",
					Database:           "rsdb",
					Port:               5433,
					SslMode:            "disable",
				},
			},
			MsSQL: []MsSQLConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn5"},
					Host:               "somemssql",
					Username:           "msuser",
					Password:           "mspass",
					Database:           "mssqldb",
					Port:               1433,
				},
			},
			Databricks: []DatabricksConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn55"},
					Host:               "hostbricks",
					Path:               "sql",
					Token:              "aaaaaaaa",
					Port:               443,
				},
			},
			Synapse: []SynapseConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn6"},
					Host:               "somemsynapse",
					Username:           "syuser",
					Password:           "sypass",
					Database:           "sydb",
					Port:               1434,
				},
			},
			Mongo: []MongoConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn7"},
					Host:               "mongohost",
					Username:           "mongouser",
					Password:           "mongopass",
					Database:           "mongodb",
					Port:               27017,
				},
			},
			Couchbase: []CouchbaseConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "couchbase1"},
					Host:               "couchbasehost",
					Username:           "couchbaseuser",
					Password:           "couchbasepass",
					Bucket:             "mybucket",
					SSL:                false,
				},
			},
			Cursor: []CursorConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "cursor1"},
					APIKey:             "cursorapikey",
				},
			},
			MySQL: []MySQLConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn8"},
					Host:               "mysqlhost",
					Username:           "mysqluser",
					Password:           "mysqlpass",
					Database:           "mysqldb",
					Port:               3306,
				},
			},
			Vitess: []VitessConnection{
				{
					Name:     "conn8b",
					Host:     "vtgatehost",
					Username: "vitessuser",
					Password: "vitesspass",
					Database: "commerce",
					Port:     15306,
					GrpcPort: 15991,
				},
			},
			Planetscale: []PlanetScaleConnection{
				{
					Name:     "conn8c",
					Host:     "aws.connect.psdb.cloud",
					Username: "psuser",
					Password: "pspass",
					Database: "psdb",
					Port:     3306,
				},
			},
			Notion: []NotionConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn9"},
					APIKey:             "XXXXYYYYZZZZ",
				},
			},
			Allium: []AlliumConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "allium-1"},
					APIKey:             "allium-api-key",
				},
			},
			HANA: []HANAConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn10"},
					Host:               "hanahost",
					Username:           "hanauser",
					Password:           "hanapass",
					Database:           "hanadb",
					Port:               39013,
				},
			},
			Shopify: []ShopifyConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn11"},
					APIKey:             "shopifykey",
					URL:                "shopifyurl",
				},
			},
			Gorgias: []GorgiasConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn12"},
					APIKey:             "gorgiaskey",
					Domain:             "gorgiasurl",
					Email:              "gorgiasemail",
				},
			},
			G2: []G2Connection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn-g2"},
					APIToken:           "g2token",
				},
			},
			Klaviyo: []KlaviyoConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn15"},
					APIKey:             "klaviyokey",
				},
			},
			Adjust: []AdjustConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn16"},
					APIKey:             "adjustokey",
				},
			},
			Adapty: []AdaptyConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "adapty-1"},
					APIKey:             "adaptykey",
					LookbackDays:       &adaptyLookbackDays,
					Timezone:           "Europe/Istanbul",
				},
			},
			SurveyMonkey: []SurveyMonkeyConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "surveymonkey-default"},
					AccessToken:        "test-token-123",
				},
			},
			Anthropic: []AnthropicConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "anthropic-1"},
					APIKey:             "test-api-key",
				},
			},
			Hostaway: []HostawayConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "hostaway-1"},
					APIKey:             "test-api-key",
				},
			},
			Intercom: []IntercomConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "intercom-1"},
					AccessToken:        "test-access-token",
					Region:             "us",
				},
			},
			AthenaConnection: []AthenaConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn14"},
					AccessKey:          "athena_key",
					SecretKey:          "athena_secret",
					QueryResultsPath:   "s3://bucket/prefix",
					Region:             "us-west-2",
					Database:           "athena_db",
				},
			},
			AwsConnection: []AwsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn13"},
					SecretKey:          "awssecret",
					AccessKey:          "awskey",
				},
			},
			Generic: []GenericConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "key1"},
					Value:              "value1",
				},
				{
					ConnectionMetadata: ConnectionMetadata{Name: "key2"},
					Value:              "value2",
				},
			},
			FacebookAds: []FacebookAdsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn17"},
					AccessToken:        "Facebookkey",
					AccountID:          "Id123",
				},
			},
			Stripe: []StripeConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn18"},
					APIKey:             "stripekey",
				},
			},
			Paddle: []PaddleConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn-paddle"},
					APIKey:             "paddlekey",
				},
			},
			Chargebee: []ChargebeeConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn-chargebee"},
					Site:               "chargebeesite",
					APIKey:             "chargebeekey",
				},
			},
			Recurly: []RecurlyConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn-recurly"},
					APIKey:             "recurlykey",
					Region:             "us",
				},
			},
			GitLab: []GitLabConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn-gitlab"},
					AccessToken:        "gitlabtoken",
				},
			},
			Dune: []DuneConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "dune-1"},
					APIKey:             "dune-api-key-123",
				},
			},
			InfluxDB: []InfluxDBConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "influxdb-1"},
					Host:               "influxdb-host",
					Token:              "influxdb-token",
					Org:                "influxdb-org",
					Bucket:             "influxdb-bucket",
					Secure:             "true",
				},
			},
			Appsflyer: []AppsflyerConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn19"},
					APIKey:             "appsflyerkey",
				},
			},
			Kafka: []KafkaConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn20"},
					BootstrapServers:   "localhost:9093",
					GroupID:            "kafka123",
				},
			},
			RabbitMQ: []RabbitMQConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn21"},
					Host:               "localhost",
					Port:               5672,
					Username:           "guest",
					Password:           "guest",
				},
			},
			DuckDB: []DuckDBConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn20"},
					Path:               duckPath,
				},
			},

			ClickHouse: []ClickHouseConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn-clickhouse"},
					Host:               "clickhousehost",
					Port:               8123,
					Username:           "clickhouseuser",
					Password:           "clickhousepass",
					Database:           "clickhousedb",
					HTTPPort:           8124,
					Secure:             &clickhouseSecureValue,
				},
			},
			Hubspot: []HubspotConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn21"},
					APIKey:             "hubspotkey",
				},
			},
			GoogleSheets: []GoogleSheetsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn22"},
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
				},
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn22-1"},
					ServiceAccountFile: servicefile,
				},
			},
			Chess: []ChessConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn24"},
					Players:            []string{"FabianoCaruana", "Hikaru", "MagnusCarlsen", "GothamChess", "DanielNaroditsky", "AnishGiri", "Firouzja2003", "LevonAronian", "WesleySo", "GarryKasparov"},
				},
			},
			Airtable: []AirtableConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn23"},
					BaseID:             "123",
					AccessToken:        "accessKey",
				},
			},
			Mailchimp: []MailchimpConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn26"},
					APIKey:             "mailchimpKey",
					Server:             "us10",
				},
			},
			S3: []S3Connection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn25"},
					BucketName:         "my-bucket",
					PathToFile:         "/folder1/file.csv",
					AccessKeyID:        "123Key",
					SecretAccessKey:    "secretKey123",
				},
			},
			Slack: []SlackConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn26"},
					APIKey:             "slackkey",
				},
			},
			Socrata: []SocrataConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "socrata-test"},
					Domain:             "data.seattle.gov",
					AppToken:           "test_app_token",
				},
			},
			Asana: []AsanaConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "asana-workspace-1337"},
					AccessToken:        "access_token_pawn3d",
					WorkspaceID:        "1337",
				},
			},
			DynamoDB: []DynamoDBConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "dynamodb-432123"},
					AccessKeyID:        "access-key-786",
					SecretAccessKey:    "shhh,secret",
					Region:             "ap-south-1",
				},
			},
			Docebo: []DoceboConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "docebo-test"},
					BaseURL:            "https://mycompany.docebosaas.com",
					ClientID:           "test-client-id",
					ClientSecret:       "test-client-secret",
					Username:           "admin",
					Password:           "admin-password",
				},
			},
			Zendesk: []ZendeskConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn25"},
					APIToken:           "zendeskKey",
					Email:              "zendeskemail",
					Subdomain:          "zendeskUrl",
				},
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn25-1"},
					OAuthToken:         "zendeskToken",
					Subdomain:          "zendeskUrl",
				},
			},
			GoogleAds: []GoogleAdsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "googleads-0"},
					DeveloperToken:     "dev-0",
					CustomerID:         "1234567890",
					ServiceAccountJSON: `{"email": "no-reply@googleads-0.com"}`,
				},
			},
			TikTokAds: []TikTokAdsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "tiktokads-1"},
					AccessToken:        "access-token-123",
					AdvertiserIDs:      "advertiser-id-123,advertiser-id-456",
					Timezone:           "UTC",
				},
			},
			SnapchatAds: []SnapchatAdsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "snapchatads-1"},
					RefreshToken:       "refresh-token-123",
					ClientID:           "client-id-123",
					ClientSecret:       "client-secret-123",
					OrganizationID:     "org-id-123",
				},
			},
			GitHub: []GitHubConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "github-1"},
					Owner:              "owner-456",
					Repo:               "repo-456",
				},
				{
					ConnectionMetadata: ConnectionMetadata{Name: "github-2"},
					AccessToken:        "token-123",
					Owner:              "owner-456",
					Repo:               "repo-456",
				},
			},
			AppStore: []AppStoreConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "appstore-1"},
					IssuerID:           "issuer-id-123",
					KeyID:              "key-id-123",
					KeyPath:            "/path/to/key.pem",
				},
			},
			AppleAds: []AppleAdsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "appleads-1"},
					ClientID:           "SEARCHADS.client-id-123",
					TeamID:             "SEARCHADS.team-id-123",
					KeyID:              "key-id-123",
					OrgID:              "19371590",
					KeyPath:            "/path/to/key.pem",
				},
			},
			LinkedInAds: []LinkedInAdsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "linkedinads-1"},
					AccessToken:        "access-token-123",
					AccountIds:         "account-id-123,account-id-456",
				},
			},
			RevenueCat: []RevenueCatConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "revenuecat-1"},
					APIKey:             "rc_api_key_123",
					ProjectID:          "proj_123456789",
				},
			},
			Linear: []LinearConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "linear-1"},
					APIKey:             "api-key-123",
				},
			},
			GCS: []GCSConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "gcs-1"},
					ServiceAccountFile: "/path/to/service_account.json",
					BucketName:         "my-bucket",
					PathToFile:         "/folder1/file.csv",
					Layout:             "my_layout",
				},
			},
			Iceberg: []IcebergConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "iceberg-1"},
					CatalogName:        "analytics",
					Catalog: IcebergCatalog{
						Type:      IcebergCatalogGlue,
						CatalogID: "123456789012",
						Region:    testAWSRegion,
						Auth:      IcebergAuth{AccessKey: "AKIAEXAMPLE", SecretKey: "secret-123"},
					},
					Storage: IcebergStorage{
						Type: IcebergStorageS3,
						Path: "s3://my-lake/warehouse",
						Auth: IcebergAuth{AccessKey: "AKIAEXAMPLE", SecretKey: "secret-123"},
					},
				},
			},
			ApplovinMax: []ApplovinMaxConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "applovinmax-1"},
					APIKey:             "api-key-123",
				},
			},
			Personio: []PersonioConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "personio-1"},
					ClientID:           "client-id-123",
					ClientSecret:       "client-secret-123",
				},
			},
			Kinesis: []KinesisConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "kinesis-1"},
					AccessKeyID:        "aws-access-key-id-123",
					SecretAccessKey:    "aws-secret-access-key-123",
					Region:             testAWSRegion,
				},
			},
			Pipedrive: []PipedriveConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "pipedrive-1"},
					APIToken:           "token-123",
				},
			},
			Clickup: []ClickupConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "clickup-1"},
					APIToken:           "token_123",
				},
			},
			Jobtread: []JobtreadConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "jobtread-1"},
					GrantKey:           "grant_key_123",
					OrganizationID:     "org_123",
				},
			},
			Posthog: []PosthogConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "posthog-1"},
					PersonalAPIKey:     "phx_test123",
					ProjectID:          "12345",
				},
			},
			QuickBooks: []QuickBooksConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "quickbooks-1"},
					CompanyID:          "123456",
					ClientID:           "cid",
					ClientSecret:       "csecret",
					RefreshToken:       "rtoken",
				},
			},
			Pinterest: []PinterestConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "pinterest-1"},
					AccessToken:        "token",
				},
			},
			Mixpanel: []MixpanelConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "mixpanel-1"},
					Username:           "user-123",
					Password:           "secret-123",
					ProjectID:          "12345",
					Server:             "eu",
				},
			},
			Amplitude: []AmplitudeConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "amplitude-1"},
					APIKey:             "api-key-123",
					SecretKey:          "secret-key-123",
					Region:             "us",
				},
			},
			Fastspring: []FastspringConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "fastspring-1"},
					Username:           "user-123",
					Password:           "pass-123",
				},
			},
			Payrails: []PayrailsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "payrails-1"},
					ClientID:           "client-123",
					ClientSecret:       "secret-123",
					CertPath:           "/tmp/client.pem",
					KeyPath:            "/tmp/client.key",
				},
			},
			Wise: []WiseConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "wise-1"},
					APIKey:             "token-123",
				},
			},
			Trustpilot: []TrustpilotConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "trustpilot-1"},
					BusinessUnitID:     "unit123",
					APIKey:             "apikey",
				},
			},
			EMRServerless: []EMRServerlessConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "emr_serverless-test"},
					AccessKey:          "AKIAEXAMPLE",
					SecretKey:          "SECRETKEYEXAMPLE",
					Region:             "us-west-2",
					ApplicationID:      "emr-app_123",
					ExecutionRole:      "arn:aws:iam::123456789012:role/example_role",
					Workspace:          "s3://amzn-test-bucket/bruin-workspace/",
				},
			},
			GoogleAnalytics: []GoogleAnalyticsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "googleanalytics-1"},
					ServiceAccountFile: "path/to/service_account.json",
					PropertyID:         "12345",
				},
			},
			GSC: []GSCConnection{
				{
					Name:               "conn-gsc",
					ServiceAccountFile: "path/to/service_account.json",
					SiteURL:            "sc-domain:example.com",
				},
			},
			Frankfurter: []FrankfurterConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "frankfurter-1"},
				},
			},
			AppLovin: []AppLovinConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "applovin-1"},
					APIKey:             "key-123",
				},
			},
			Salesforce: []SalesforceConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "salesforce-1"},
					Username:           "username-123",
					Password:           "password-123",
					Token:              "token-123",
					AccessToken:        "access-token-123",
					Domain:             "mydomain.my.salesforce.com",
				},
			},
			SQLite: []SQLiteConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "sqlite-1"},
					Path:               "C:\\path\\to\\sqlite.db",
				},
			},
			DB2: []DB2Connection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "db2-default"},
					Username:           "username-123",
					Password:           "password-123",
					Host:               "host-123",
					Port:               "1234",
					Database:           "dbname-123",
				},
			},
			Oracle: []OracleConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "oracle-1"},
					Username:           "username-123",
					Password:           "password-123",
					Host:               "host-123",
					Port:               "1234",
					ServiceName:        "service-123",
				},
			},
			Phantombuster: []PhantombusterConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "phantombuster-1"},
					APIKey:             "api-key-123",
				},
			},
			Elasticsearch: []ElasticsearchConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "elasticsearch-1"},
					Username:           "username-123",
					Password:           "password-123",
					Host:               "host-123",
					Port:               9200,
					Secure:             "true",
					VerifyCerts:        "true",
				},
			},
			Spanner: []SpannerConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "spanner-1"},
					ProjectID:          "project-id-123",
					InstanceID:         "instance-id-123",
					Database:           "database-id-123",
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
				},
				{
					ConnectionMetadata: ConnectionMetadata{Name: "spanner-2"},
					ProjectID:          "project-id-123",
					InstanceID:         "instance-id-123",
					Database:           "database-id-123",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
			Solidgate: []SolidgateConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "solidgate-1"},
					SecretKey:          "secret-key-123",
					PublicKey:          "public-key-123",
				},
			},
			Square: []SquareConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "square-1"},
					AccessToken:        "EAAA-test-access-token",
					Environment:        "sandbox",
				},
			},
			Smartsheet: []SmartsheetConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "smartsheet-1"},
					AccessToken:        "access-token-123",
					SmartsheetID:       "1234567890",
				},
			},
			Attio: []AttioConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "attio-1"},
					APIKey:             "api-key-123",
				},
			},
			Sftp: []SFTPConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "sftp-1"},
					Host:               "sftp-host",
					Port:               22,
					Username:           "sftp-user",
					Password:           "sftp-password",
				},
			},
			ISOCPulse: []ISOCPulseConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "isoc_pulse-1"},
					Token:              "isoc-pulse-token-123",
				},
			},
			Zoom: []ZoomConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "zoom-1"},
					ClientID:           "zid",
					ClientSecret:       "zsecret",
					AccountID:          "accid",
				},
			},
			Fluxx: []FluxxConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "fluxx-1"},
					Instance:           "test-instance",
					ClientID:           "test-client-id",
					ClientSecret:       "test-client-secret",
				},
			},
			FundraiseUp: []FundraiseUpConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "fundraiseup-1"},
					APIKey:             "test-api-key",
				},
			},
			Fireflies: []FirefliesConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "fireflies-1"},
					APIKey:             "test-api-key",
				},
			},
			Trello: []TrelloConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "trello-1"},
					APIKey:             "test-api-key",
					Token:              "test-token",
				},
			},
			Jira: []JiraConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "jira-1"},
					Domain:             "company.atlassian.net",
					Email:              "user@company.com",
					APIToken:           "test-api-token",
				},
			},
			Monday: []MondayConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "monday-1"},
					APIToken:           "test-api-token",
				},
			},
			PlusVibeAI: []PlusVibeAIConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "plusvibeai-1"},
					APIKey:             "test-api-key",
					WorkspaceID:        "test-workspace-id",
				},
			},
			BruinCloud: []BruinCloudConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "bruin-1"},
					APIToken:           "test-api-token",
				},
			},
			Primer: []PrimerConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "primer-1"},
					APIKey:             "test-api-key",
				},
			},
			Indeed: []IndeedConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "indeed-1"},
					ClientID:           "test-client-id",
					ClientSecret:       "test-client-secret",
					EmployerID:         "test-employer-id",
				},
			},
			CustomerIo: []CustomerIoConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "customerio-1"},
					APIKey:             "test-api-key",
					Region:             "us",
				},
			},
			Sendgrid: []SendgridConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "sendgrid-1"},
					APIKey:             "test-api-key",
					OnBehalfOf:         "test-subuser",
				},
			},
			Twilio: []TwilioConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "twilio-1"},
					AccountSID:         "test-account-sid",
					AuthToken:          "test-auth-token",
					APIKey:             "test-api-key",
					APISecret:          "test-api-secret",
				},
			},
			Braze: []BrazeConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "braze-1"},
					APIKey:             "test-api-key",
					Endpoint:           "rest.iad-01.braze.com",
				},
			},
			RedditAds: []RedditAdsConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "redditads-1"},
					AccessToken:        "test-access-token",
					ClientID:           "test-client-id",
					ClientSecret:       "test-client-secret",
					RefreshToken:       "test-refresh-token",
				},
			},
			Espn: []EspnConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "espn-1"},
					Sport:              "football",
					League:             "nfl",
					Season:             "2026",
					Limit:              50,
				},
			},
			SharePoint: []SharePointConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "sharepoint-1"},
					TenantID:           "test-tenant-id",
					ClientID:           "test-client-id",
					ClientSecret:       "test-client-secret",
					Hostname:           "example.sharepoint.com",
					Site:               "sites/Example",
					Library:            "Documents",
					MaxFileSize:        &sharePointMaxFileSize,
					MaxFiles:           &sharePointMaxFiles,
					DownloadTimeout:    "30m",
				},
			},
			APIFootball: []APIFootballConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "apifootball-1"},
					APIKey:             "test-api-key",
					League:             "1",
					Season:             "2026",
				},
			},
			FootballData: []FootballDataConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "footballdata-1"},
					APIKey:             "test-api-key",
					Competition:        "WC",
					Season:             "2026",
				},
			},
			BallDontLie: []BallDontLieConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "balldontlie-1"},
					APIKey:             "test-api-key",
					Season:             "2026",
				},
			},
			StarRocks: []StarRocksConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "starrocks-1"},
					Host:               "localhost",
					Port:               9030,
					Username:           "root",
					Password:           "pass123",
					Database:           "analytics",
					Catalog:            "iceberg_catalog",
					SSL:                "true",
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
									ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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
					ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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

func TestLoadResolvesReadEmbedCredentialPaths(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	configPath := "/repo/.bruin.yml"
	yml := `default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-1
          project_id: proj
          service_account_file: creds/gcp.json
      google_sheets:
        - name: sheets-1
          service_account_file: creds/sheets.json
`
	require.NoError(t, afero.WriteFile(fs, configPath, []byte(yml), 0o644))

	cfg, err := LoadOrCreate(fs, configPath)
	require.NoError(t, err)
	absoluteConfigPath, err := filepath.Abs(configPath)
	require.NoError(t, err)
	configDir := filepath.Dir(absoluteConfigPath)

	// Both read+embed the file, so their relative paths must resolve against the
	// config directory (so masking reads the same file, independent of cwd).
	require.Len(t, cfg.SelectedEnvironment.Connections.GoogleCloudPlatform, 1)
	gcp := cfg.SelectedEnvironment.Connections.GoogleCloudPlatform[0].ServiceAccountFile
	assert.Equal(t, filepath.Join(configDir, "creds/gcp.json"), gcp)
	assert.True(t, filepath.IsAbs(gcp), "gcp path should be absolute: %s", gcp)

	require.Len(t, cfg.SelectedEnvironment.Connections.GoogleSheets, 1)
	sheets := cfg.SelectedEnvironment.Connections.GoogleSheets[0].ServiceAccountFile
	assert.Equal(t, filepath.Join(configDir, "creds/sheets.json"), sheets)
	assert.True(t, filepath.IsAbs(sheets), "sheets path should be absolute: %s", sheets)
}

func TestLoadDoesNotPrefixAnchoredCredentialPaths(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	configPath := "/repo/.bruin.yml"
	drivePath := `C:\Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json`
	rootedPath := `\Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json`
	slashRootedPath := "/Users/ColtonChilders/Documents/GCS Keys/plated-mesh.json"
	caPath := `C:\certs\ca.pem`
	certPath := `C:\certs\client.pem`
	keyPath := `C:\certs\client-key.pem`
	yml := `default_environment: default
environments:
  default:
    connections:
      snowflake:
        - name: sf-drive
          private_key_path: 'C:\certs\client-key.pem'
      mysql:
        - name: mysql-drive
          ssl_ca_path: 'C:\certs\ca.pem'
          ssl_cert_path: 'C:\certs\client.pem'
          ssl_key_path: 'C:\certs\client-key.pem'
      doris:
        - name: doris-drive
          ssl_ca_path: 'C:\certs\ca.pem'
          ssl_cert_path: 'C:\certs\client.pem'
          ssl_key_path: 'C:\certs\client-key.pem'
      vitess:
        - name: vitess-drive
          ssl_ca_path: 'C:\certs\ca.pem'
          ssl_cert_path: 'C:\certs\client.pem'
          ssl_key_path: 'C:\certs\client-key.pem'
      google_cloud_platform:
        - name: gcp-drive
          project_id: proj
          service_account_file: 'C:\Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json'
      google_sheets:
        - name: sheets-drive
          service_account_file: 'C:\Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json'
        - name: sheets-rooted
          service_account_file: '\Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json'
        - name: sheets-slash-rooted
          service_account_file: '/Users/ColtonChilders/Documents/GCS Keys/plated-mesh.json'
`
	require.NoError(t, afero.WriteFile(fs, configPath, []byte(yml), 0o644))

	cfg, err := LoadOrCreate(fs, configPath)
	require.NoError(t, err)

	require.Len(t, cfg.SelectedEnvironment.Connections.Snowflake, 1)
	assert.Equal(t, keyPath, cfg.SelectedEnvironment.Connections.Snowflake[0].PrivateKeyPath)

	require.Len(t, cfg.SelectedEnvironment.Connections.MySQL, 1)
	assert.Equal(t, caPath, cfg.SelectedEnvironment.Connections.MySQL[0].SslCaPath)
	assert.Equal(t, certPath, cfg.SelectedEnvironment.Connections.MySQL[0].SslCertPath)
	assert.Equal(t, keyPath, cfg.SelectedEnvironment.Connections.MySQL[0].SslKeyPath)

	require.Len(t, cfg.SelectedEnvironment.Connections.Doris, 1)
	assert.Equal(t, caPath, cfg.SelectedEnvironment.Connections.Doris[0].SslCaPath)
	assert.Equal(t, certPath, cfg.SelectedEnvironment.Connections.Doris[0].SslCertPath)
	assert.Equal(t, keyPath, cfg.SelectedEnvironment.Connections.Doris[0].SslKeyPath)

	require.Len(t, cfg.SelectedEnvironment.Connections.Vitess, 1)
	assert.Equal(t, caPath, cfg.SelectedEnvironment.Connections.Vitess[0].SslCaPath)
	assert.Equal(t, certPath, cfg.SelectedEnvironment.Connections.Vitess[0].SslCertPath)
	assert.Equal(t, keyPath, cfg.SelectedEnvironment.Connections.Vitess[0].SslKeyPath)

	require.Len(t, cfg.SelectedEnvironment.Connections.GoogleCloudPlatform, 1)
	assert.Equal(t, drivePath, cfg.SelectedEnvironment.Connections.GoogleCloudPlatform[0].ServiceAccountFile)

	require.Len(t, cfg.SelectedEnvironment.Connections.GoogleSheets, 3)
	assert.Equal(t, drivePath, cfg.SelectedEnvironment.Connections.GoogleSheets[0].ServiceAccountFile)
	assert.Equal(t, rootedPath, cfg.SelectedEnvironment.Connections.GoogleSheets[1].ServiceAccountFile)
	assert.Equal(t, slashRootedPath, cfg.SelectedEnvironment.Connections.GoogleSheets[2].ServiceAccountFile)
}

func TestIsAnchoredLocalPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "relative unix",
			path: "creds/service-account.json",
			want: false,
		},
		{
			name: "relative windows",
			path: `creds\service-account.json`,
			want: false,
		},
		{
			name: "windows drive absolute with backslashes",
			path: `C:\Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json`,
			want: true,
		},
		{
			name: "windows drive absolute with slashes",
			path: "C:/Users/ColtonChilders/Documents/GCS Keys/plated-mesh.json",
			want: true,
		},
		{
			name: "windows drive relative",
			path: `C:Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json`,
			want: false,
		},
		{
			name: "windows rooted current drive",
			path: `\Users\ColtonChilders\Documents\GCS Keys\plated-mesh.json`,
			want: true,
		},
		{
			name: "slash rooted",
			path: "/Users/ColtonChilders/Documents/GCS Keys/plated-mesh.json",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isAnchoredLocalPath(tt.path))
		})
	}
}

func TestHasAnchoredLocalPathPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "slash rooted",
			path: "/Users/x/key.json",
			want: true,
		},
		{
			name: "backslash rooted",
			path: `\Users\x\key.json`,
			want: true,
		},
		{
			name: "windows drive absolute with backslashes",
			path: `C:\Users\x\key.json`,
			want: true,
		},
		{
			name: "windows drive absolute with slashes",
			path: "C:/Users/x/key.json",
			want: true,
		},
		{
			name: "drive relative",
			path: `C:Users\x\key.json`,
			want: false,
		},
		{
			name: "relative",
			path: "Users/x/key.json",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hasAnchoredLocalPathPrefix(tt.path))
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
					ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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
					ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
		},
	}

	prodEnv := &Environment{
		Connections: &Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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
			name:     "Add Trello connection",
			envName:  "default",
			connType: "trello",
			connName: "trello-conn",
			creds: map[string]interface{}{
				"api_key": "test-api-key",
				"token":   "test-token",
			},
			expectedErr: false,
		},
		{
			name:     "Add Fabric connection",
			envName:  "default",
			connType: "fabric",
			connName: "fabric-conn",
			creds: map[string]interface{}{
				"host":          "myworkspace.datawarehouse.fabric.microsoft.com",
				"database":      "MyWarehouse",
				"client_id":     "test-client-id",
				"client_secret": "test-client-secret",
				"tenant_id":     "test-tenant-id",
			},
			expectedErr: false,
		},
		{
			name:     "Add SharePoint connection",
			envName:  "default",
			connType: "sharepoint",
			connName: "sharepoint-conn",
			creds: map[string]interface{}{
				"tenant_id":        "tenant-id",
				"client_id":        "client-id",
				"client_secret":    "client-secret",
				"hostname":         "example.sharepoint.com",
				"site":             "sites/Example",
				"library":          "Documents",
				"download_timeout": "30m",
			},
			expectedErr: false,
		},
		{
			name:     "Add Iceberg connection",
			envName:  "default",
			connType: "iceberg",
			connName: "iceberg-conn",
			creds: map[string]interface{}{
				"catalog_name": "analytics",
				"catalog": map[string]interface{}{
					"type":       "glue",
					"catalog_id": "123456789012",
					"region":     testAWSRegion,
					"auth": map[string]interface{}{
						"access_key": "AKIAEXAMPLE",
						"secret_key": "secret-123",
					},
				},
				"storage": map[string]interface{}{
					"type": "s3",
					"path": "s3://my-lake/warehouse",
				},
			},
			expectedErr: false,
		},
		{
			name:     "Add Adapty connection",
			envName:  "default",
			connType: "adapty",
			connName: "adapty-conn",
			creds: map[string]interface{}{
				"api_key":       "secret_live_123",
				"lookback_days": 15,
				"timezone":      "UTC",
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
				case "trello":
					assert.Len(t, env.Connections.Trello, 1)
					assert.Equal(t, tt.connName, env.Connections.Trello[0].Name)
					assert.Equal(t, tt.creds["api_key"], env.Connections.Trello[0].APIKey)
					assert.Equal(t, tt.creds["token"], env.Connections.Trello[0].Token)
				case "fabric":
					assert.Len(t, env.Connections.Fabric, 1)
					assert.Equal(t, tt.connName, env.Connections.Fabric[0].Name)
					assert.Equal(t, tt.creds["host"], env.Connections.Fabric[0].Host)
					assert.Equal(t, tt.creds["database"], env.Connections.Fabric[0].Database)
					assert.Equal(t, tt.creds["client_id"], env.Connections.Fabric[0].ClientID)
					assert.Equal(t, tt.creds["client_secret"], env.Connections.Fabric[0].ClientSecret)
					assert.Equal(t, tt.creds["tenant_id"], env.Connections.Fabric[0].TenantID)
				case "sharepoint":
					assert.Len(t, env.Connections.SharePoint, 1)
					assert.Equal(t, tt.connName, env.Connections.SharePoint[0].Name)
					assert.Equal(t, tt.creds["tenant_id"], env.Connections.SharePoint[0].TenantID)
					assert.Equal(t, tt.creds["client_id"], env.Connections.SharePoint[0].ClientID)
					assert.Equal(t, tt.creds["client_secret"], env.Connections.SharePoint[0].ClientSecret)
					assert.Equal(t, tt.creds["hostname"], env.Connections.SharePoint[0].Hostname)
					assert.Equal(t, tt.creds["site"], env.Connections.SharePoint[0].Site)
					assert.Equal(t, tt.creds["library"], env.Connections.SharePoint[0].Library)
					assert.Equal(t, tt.creds["download_timeout"], env.Connections.SharePoint[0].DownloadTimeout)
				case "iceberg":
					assert.Len(t, env.Connections.Iceberg, 1)
					assert.Equal(t, tt.connName, env.Connections.Iceberg[0].Name)
					assert.Equal(t, "analytics", env.Connections.Iceberg[0].CatalogName)
					assert.Equal(t, IcebergCatalogGlue, env.Connections.Iceberg[0].Catalog.Type)
					assert.Equal(t, "123456789012", env.Connections.Iceberg[0].Catalog.CatalogID)
					assert.Equal(t, IcebergStorageS3, env.Connections.Iceberg[0].Storage.Type)
					assert.Equal(t, "s3://my-lake/warehouse", env.Connections.Iceberg[0].Storage.Path)
				case "adapty":
					assert.Len(t, env.Connections.Adapty, 1)
					assert.Equal(t, tt.connName, env.Connections.Adapty[0].Name)
					assert.Equal(t, tt.creds["api_key"], env.Connections.Adapty[0].APIKey)
					require.NotNil(t, env.Connections.Adapty[0].LookbackDays)
					assert.Equal(t, tt.creds["lookback_days"], *env.Connections.Adapty[0].LookbackDays)
					assert.Equal(t, tt.creds["timezone"], env.Connections.Adapty[0].Timezone)
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
									{ConnectionMetadata: ConnectionMetadata{Name: "gcp-conn"}, ServiceAccountFile: "file.json", ProjectID: "project"},
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
									{ConnectionMetadata: ConnectionMetadata{Name: "aws-conn"}, AccessKey: "key", SecretKey: "secret"},
								},
							},
						},
					},
				}
			},
			expectedErr: false,
		},
		{
			name:     "Delete existing Fabric connection",
			envName:  "default",
			connName: "fabric-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{
						"default": {
							Connections: &Connections{
								Fabric: []FabricConnection{
									{ConnectionMetadata: ConnectionMetadata{Name: "fabric-conn"}, Host: "fabric.example", Database: "warehouse", ClientID: "client-id"},
								},
							},
						},
					},
				}
			},
			expectedErr: false,
		},
		{
			name:     "Delete existing Iceberg connection",
			envName:  "default",
			connName: "iceberg-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{
						"default": {
							Connections: &Connections{
								Iceberg: []IcebergConnection{
									{ConnectionMetadata: ConnectionMetadata{Name: "iceberg-conn"}, Catalog: IcebergCatalog{Type: IcebergCatalogGlue, Region: testAWSRegion}, Storage: IcebergStorage{Type: IcebergStorageS3, Path: "s3://my-lake/warehouse"}},
								},
							},
						},
					},
				}
			},
			expectedErr: false,
		},
		{
			name:     "Delete existing SharePoint connection",
			envName:  "default",
			connName: "sharepoint-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{
						"default": {
							Connections: &Connections{
								SharePoint: []SharePointConnection{
									{ConnectionMetadata: ConnectionMetadata{Name: "sharepoint-conn"}, TenantID: "tenant-id", ClientID: "client-id", ClientSecret: "secret", Hostname: "example.sharepoint.com", Site: "sites/Example"},
								},
							},
						},
					},
				}
			},
			expectedErr: false,
		},
		{
			name:     "Delete existing Adapty connection",
			envName:  "default",
			connName: "adapty-conn",
			setupConfig: func() *Config {
				return &Config{
					Environments: map[string]Environment{
						"default": {
							Connections: &Connections{
								Adapty: []AdaptyConnection{
									{ConnectionMetadata: ConnectionMetadata{Name: "adapty-conn"}, APIKey: "secret_live_123"},
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
				case "fabric-conn":
					assert.Empty(t, env.Connections.Fabric)
				case "sharepoint-conn":
					assert.Empty(t, env.Connections.SharePoint)
				case "adapty-conn":
					assert.Empty(t, env.Connections.Adapty)
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
								ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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
								ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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
								ConnectionMetadata: ConnectionMetadata{Name: "gcp-default"},
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
								ConnectionMetadata: ConnectionMetadata{Name: "gcp-default"},
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
								ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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
						Parameters: pipeline.ParameterMap{"source_connection": "conn3"},
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
								ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "conn2"},
							},
							{
								ConnectionMetadata: ConnectionMetadata{Name: "conn3"},
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
						Parameters: pipeline.ParameterMap{
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
								ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "conn2"},
							},
							{
								ConnectionMetadata: ConnectionMetadata{Name: "conn3"},
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
						Parameters: pipeline.ParameterMap{
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
								ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "conn2"},
							},
							{
								ConnectionMetadata: ConnectionMetadata{Name: "conn3"},
							},
						},
						Generic: []GenericConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "some_key"},
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
						Parameters: pipeline.ParameterMap{
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
			name: "explicit python connection missing",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypePython,
						Connection: "warehouse",
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "explicit python connection found",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						Postgres: []PostgresConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "warehouse"},
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypePython,
						Connection: "warehouse",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "explicit r connection missing",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeR,
						Connection: "warehouse",
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "explicit r connection found",
			config: Config{
				SelectedEnvironment: &Environment{
					Connections: &Connections{
						Postgres: []PostgresConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "warehouse"},
							},
						},
					},
				},
			},
			pipeline: pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Type:       pipeline.AssetTypeR,
						Connection: "warehouse",
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
								ConnectionMetadata: ConnectionMetadata{Name: "gcp-default"},
							},
						},
						Snowflake: []SnowflakeConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "snowflake-default"},
							},
							{
								ConnectionMetadata: ConnectionMetadata{Name: "conn3"},
							},
						},
						Generic: []GenericConnection{
							{
								ConnectionMetadata: ConnectionMetadata{Name: "some_key"},
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
						Parameters: pipeline.ParameterMap{
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

func TestCanRunTaskInstancesUsesTaskTypeConnectionNames(t *testing.T) {
	t.Parallel()

	sourceAsset := &pipeline.Asset{
		Name: "source",
		Type: pipeline.AssetTypeBigquerySource,
	}
	emptyConfig := Config{
		SelectedEnvironment: &Environment{Connections: &Connections{}},
	}

	t.Run("source main task is connectionless", func(t *testing.T) {
		t.Parallel()

		err := emptyConfig.CanRunTaskInstances(&pipeline.Pipeline{}, []scheduler.TaskInstance{
			&scheduler.AssetInstance{Asset: sourceAsset},
		})

		require.NoError(t, err)
	})

	t.Run("source check task requires primary connection", func(t *testing.T) {
		t.Parallel()

		err := emptyConfig.CanRunTaskInstances(&pipeline.Pipeline{}, []scheduler.TaskInstance{
			&scheduler.CustomCheckInstance{
				AssetInstance: &scheduler.AssetInstance{Asset: sourceAsset},
				Check:         &pipeline.CustomCheck{Name: "freshness"},
			},
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "gcp-default")
	})

	t.Run("source check task accepts primary connection", func(t *testing.T) {
		t.Parallel()

		conf := Config{
			SelectedEnvironment: &Environment{
				Connections: &Connections{
					GoogleCloudPlatform: []GoogleCloudPlatformConnection{
						{ConnectionMetadata: ConnectionMetadata{Name: "gcp-default"}},
					},
				},
			},
		}

		err := conf.CanRunTaskInstances(&pipeline.Pipeline{}, []scheduler.TaskInstance{
			&scheduler.CustomCheckInstance{
				AssetInstance: &scheduler.AssetInstance{Asset: sourceAsset},
				Check:         &pipeline.CustomCheck{Name: "freshness"},
			},
		})

		require.NoError(t, err)
	})
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
					{ConnectionMetadata: ConnectionMetadata{Name: "existing-conn"}},
				},
			},
			source: &Connections{},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "existing-conn"}},
				},
			},
			expectedErr: false,
		},
		{
			name: "merge multiple connection types",
			target: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "gcp1"}},
				},
				Snowflake: []SnowflakeConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "sf1"}},
				},
			},
			source: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "gcp2"}},
				},
				Postgres: []PostgresConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "pg1"}},
				},
			},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "gcp1"}},
					{ConnectionMetadata: ConnectionMetadata{Name: "gcp2"}},
				},
				Snowflake: []SnowflakeConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "sf1"}},
				},
				Postgres: []PostgresConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "pg1"}},
				},
			},
			expectedErr: false,
		},
		{
			name:   "merge into empty target",
			target: &Connections{},
			source: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "gcp1"}},
				},
				Snowflake: []SnowflakeConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "sf1"}},
				},
			},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "gcp1"}},
				},
				Snowflake: []SnowflakeConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "sf1"}},
				},
			},
			expectedErr: false,
		},
		{
			name: "merge connections with same name but different types",
			target: &Connections{
				Snowflake: []SnowflakeConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "prod-db"}},
				},
			},
			source: &Connections{
				Postgres: []PostgresConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "prod-db"}},
				},
			},
			want: &Connections{
				Snowflake: []SnowflakeConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "prod-db"}},
				},
				Postgres: []PostgresConnection{
					{ConnectionMetadata: ConnectionMetadata{Name: "prod-db"}},
				},
			},
			expectedErr: false,
		},
		{
			name: "merge development and production environments",
			target: &Connections{
				Snowflake: []SnowflakeConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "dev-db"},
						Username:           "dev_user",
						Password:           "dev_pass",
						Database:           "dev_db",
					},
				},
				Postgres: []PostgresConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "dev-pg"},
						Username:           "dev_pg_user",
						Password:           "dev_pg_pass",
						Database:           "dev_pg_db",
					},
				},
			},
			source: &Connections{
				Snowflake: []SnowflakeConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "prod-db"},
						Username:           "prod_user",
						Password:           "prod_pass",
						Database:           "prod_db",
					},
				},
				Postgres: []PostgresConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "prod-pg"},
						Username:           "prod_pg_user",
						Password:           "prod_pg_pass",
						Database:           "prod_pg_db",
					},
				},
			},
			want: &Connections{
				Snowflake: []SnowflakeConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "dev-db"},
						Username:           "dev_user",
						Password:           "dev_pass",
						Database:           "dev_db",
					},
					{
						ConnectionMetadata: ConnectionMetadata{Name: "prod-db"},
						Username:           "prod_user",
						Password:           "prod_pass",
						Database:           "prod_db",
					},
				},
				Postgres: []PostgresConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "dev-pg"},
						Username:           "dev_pg_user",
						Password:           "dev_pg_pass",
						Database:           "dev_pg_db",
					},
					{
						ConnectionMetadata: ConnectionMetadata{Name: "prod-pg"},
						Username:           "prod_pg_user",
						Password:           "prod_pg_pass",
						Database:           "prod_pg_db",
					},
				},
			},
			expectedErr: false,
		},
		{
			name:   "merge with all connection types",
			target: &Connections{},
			source: &Connections{
				AwsConnection:       []AwsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "aws1"}}},
				AthenaConnection:    []AthenaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "athena1"}}},
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gcp1"}}},
				Snowflake:           []SnowflakeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sf1"}}},
				Postgres:            []PostgresConnection{{ConnectionMetadata: ConnectionMetadata{Name: "pg1"}}},
				RedShift:            []RedshiftConnection{{ConnectionMetadata: ConnectionMetadata{Name: "rs1"}}},
				MsSQL:               []MsSQLConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mssql1"}}},
				Databricks:          []DatabricksConnection{{ConnectionMetadata: ConnectionMetadata{Name: "db1"}}},
				ADLS:                []ADLSConnection{{ConnectionMetadata: ConnectionMetadata{Name: "adls1"}}},
				Synapse:             []SynapseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "syn1"}}},
				Fabric:              []FabricConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fabric1"}}},
				Mongo:               []MongoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mongo1"}}},
				Cassandra:           []CassandraConnection{{ConnectionMetadata: ConnectionMetadata{Name: "cassandra1"}}},
				Couchbase:           []CouchbaseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "couchbase1"}}},
				CrateDB:             []CrateDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "cratedb1"}}},
				CSV:                 []CSVConnection{{ConnectionMetadata: ConnectionMetadata{Name: "csv1"}}},
				Cursor:              []CursorConnection{{ConnectionMetadata: ConnectionMetadata{Name: "cursor1"}}},
				MongoAtlas:          []MongoAtlasConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mongoatlas1"}}},
				MySQL:               []MySQLConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mysql1"}}},
				Notion:              []NotionConnection{{ConnectionMetadata: ConnectionMetadata{Name: "notion1"}}},
				Allium:              []AlliumConnection{{ConnectionMetadata: ConnectionMetadata{Name: "allium1"}}},
				HANA:                []HANAConnection{{ConnectionMetadata: ConnectionMetadata{Name: "hana1"}}},
				Hostaway:            []HostawayConnection{{ConnectionMetadata: ConnectionMetadata{Name: "hostaway1"}}},
				HTTP:                []HTTPConnection{{ConnectionMetadata: ConnectionMetadata{Name: "http1"}}},
				Shopify:             []ShopifyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "shopify1"}}},
				Gorgias:             []GorgiasConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gorgias1"}}},
				G2:                  []G2Connection{{ConnectionMetadata: ConnectionMetadata{Name: "g21"}}},
				Klaviyo:             []KlaviyoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "klaviyo1"}}},
				Adjust:              []AdjustConnection{{ConnectionMetadata: ConnectionMetadata{Name: "adjust1"}}},
				Adapty:              []AdaptyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "adapty1"}}},
				SurveyMonkey:        []SurveyMonkeyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "surveymonkey1"}}},
				Anthropic:           []AnthropicConnection{{ConnectionMetadata: ConnectionMetadata{Name: "anthropic1"}}},
				Generic:             []GenericConnection{{ConnectionMetadata: ConnectionMetadata{Name: "generic1"}}},
				FacebookAds:         []FacebookAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "facebookads1"}}},
				Stripe:              []StripeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "stripe1"}}},
				Paddle:              []PaddleConnection{{ConnectionMetadata: ConnectionMetadata{Name: "paddle1"}}},
				Chargebee:           []ChargebeeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "chargebee1"}}},
				Recurly:             []RecurlyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "recurly1"}}},
				GitLab:              []GitLabConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gitlab1"}}},
				Appsflyer:           []AppsflyerConnection{{ConnectionMetadata: ConnectionMetadata{Name: "appsflyer1"}}},
				Kafka:               []KafkaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "kafka1"}}},
				RabbitMQ:            []RabbitMQConnection{{ConnectionMetadata: ConnectionMetadata{Name: "rabbitmq1"}}},
				DuckDB:              []DuckDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "duckdb1"}}},
				MotherDuck:          []MotherduckConnection{{ConnectionMetadata: ConnectionMetadata{Name: "motherduck1"}}},
				ClickHouse:          []ClickHouseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "clickhouse1"}}},
				Hubspot:             []HubspotConnection{{ConnectionMetadata: ConnectionMetadata{Name: "hubspot1"}}},
				Intercom:            []IntercomConnection{{ConnectionMetadata: ConnectionMetadata{Name: "intercom1"}}},
				GitHub:              []GitHubConnection{{ConnectionMetadata: ConnectionMetadata{Name: "github1"}}},
				GoogleSheets:        []GoogleSheetsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "googlesheets1"}}},
				Chess:               []ChessConnection{{ConnectionMetadata: ConnectionMetadata{Name: "chess1"}}},
				Airtable:            []AirtableConnection{{ConnectionMetadata: ConnectionMetadata{Name: "airtable1"}}},
				Granola:             []GranolaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "granola1"}}},
				Zendesk:             []ZendeskConnection{{ConnectionMetadata: ConnectionMetadata{Name: "zendesk1"}}},
				Kalshi:              []KalshiConnection{{ConnectionMetadata: ConnectionMetadata{Name: "kalshi1"}}},
				TikTokAds:           []TikTokAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "tiktokads1"}}},
				SnapchatAds:         []SnapchatAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "snapchatads1"}}},
				S3:                  []S3Connection{{ConnectionMetadata: ConnectionMetadata{Name: "s31"}}},
				Slack:               []SlackConnection{{ConnectionMetadata: ConnectionMetadata{Name: "slack1"}}},
				Socrata:             []SocrataConnection{{ConnectionMetadata: ConnectionMetadata{Name: "socrata1"}}},
				Asana:               []AsanaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "asana1"}}},
				DynamoDB:            []DynamoDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "dynamodb1"}}},
				Docebo:              []DoceboConnection{{ConnectionMetadata: ConnectionMetadata{Name: "docebo1"}}},
				GoogleAds:           []GoogleAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "googleads1"}}},
				AppStore:            []AppStoreConnection{{ConnectionMetadata: ConnectionMetadata{Name: "appstore1"}}},
				AppleAds:            []AppleAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "appleads1"}}},
				LinkedInAds:         []LinkedInAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "linkedinads1"}}},
				RedditAds:           []RedditAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "redditads1"}}},
				Mailchimp:           []MailchimpConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mailchimp1"}}},
				Manifold:            []ManifoldConnection{{ConnectionMetadata: ConnectionMetadata{Name: "manifold1"}}},
				RevenueCat:          []RevenueCatConnection{{ConnectionMetadata: ConnectionMetadata{Name: "revenuecat1"}}},
				Linear:              []LinearConnection{{ConnectionMetadata: ConnectionMetadata{Name: "linear1"}}},
				GCS:                 []GCSConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gcs1"}}},
				SharePoint:          []SharePointConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sharepoint1"}}},
				ApplovinMax:         []ApplovinMaxConnection{{ConnectionMetadata: ConnectionMetadata{Name: "applovinmax1"}}},
				Personio:            []PersonioConnection{{ConnectionMetadata: ConnectionMetadata{Name: "personio1"}}},
				Kinesis:             []KinesisConnection{{ConnectionMetadata: ConnectionMetadata{Name: "kinesis1"}}},
				Pipedrive:           []PipedriveConnection{{ConnectionMetadata: ConnectionMetadata{Name: "pipedrive1"}}},
				Polymarket:          []PolymarketConnection{{ConnectionMetadata: ConnectionMetadata{Name: "polymarket1"}}},
				Mixpanel:            []MixpanelConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mixpanel1"}}},
				Amplitude:           []AmplitudeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "amplitude1"}}},
				Fastspring:          []FastspringConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fastspring1"}}},
				Payrails:            []PayrailsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "payrails1"}}},
				Clickup:             []ClickupConnection{{ConnectionMetadata: ConnectionMetadata{Name: "clickup1"}}},
				Jobtread:            []JobtreadConnection{{ConnectionMetadata: ConnectionMetadata{Name: "jobtread1"}}},
				Posthog:             []PosthogConnection{{ConnectionMetadata: ConnectionMetadata{Name: "posthog1"}}},
				Pinterest:           []PinterestConnection{{ConnectionMetadata: ConnectionMetadata{Name: "pinterest1"}}},
				Trustpilot:          []TrustpilotConnection{{ConnectionMetadata: ConnectionMetadata{Name: "trustpilot1"}}},
				QuickBooks:          []QuickBooksConnection{{ConnectionMetadata: ConnectionMetadata{Name: "quickbooks1"}}},
				Wise:                []WiseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "wise1"}}},
				Wistia:              []WistiaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "wistia1"}}},
				Zoom:                []ZoomConnection{{ConnectionMetadata: ConnectionMetadata{Name: "zoom1"}}},
				EMRServerless:       []EMRServerlessConnection{{ConnectionMetadata: ConnectionMetadata{Name: "emr1"}}},
				DataprocServerless:  []DataprocServerlessConnection{{ConnectionMetadata: ConnectionMetadata{Name: "dataproc1"}}},
				GoogleAnalytics:     []GoogleAnalyticsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "googleanalytics1"}}},
				GSC:                 []GSCConnection{{Name: "gsc1"}},
				AppLovin:            []AppLovinConnection{{ConnectionMetadata: ConnectionMetadata{Name: "applovin1"}}},
				Frankfurter:         []FrankfurterConnection{{ConnectionMetadata: ConnectionMetadata{Name: "frankfurter1"}}},
				Salesforce:          []SalesforceConnection{{ConnectionMetadata: ConnectionMetadata{Name: "salesforce1"}}},
				SQLite:              []SQLiteConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sqlite1"}}},
				DB2:                 []DB2Connection{{ConnectionMetadata: ConnectionMetadata{Name: "db21"}}},
				Oracle:              []OracleConnection{{ConnectionMetadata: ConnectionMetadata{Name: "oracle1"}}},
				Phantombuster:       []PhantombusterConnection{{ConnectionMetadata: ConnectionMetadata{Name: "phantombuster1"}}},
				Elasticsearch:       []ElasticsearchConnection{{ConnectionMetadata: ConnectionMetadata{Name: "elasticsearch1"}}},
				Solidgate:           []SolidgateConnection{{ConnectionMetadata: ConnectionMetadata{Name: "solidgate1"}}},
				Square:              []SquareConnection{{ConnectionMetadata: ConnectionMetadata{Name: "square1"}}},
				Spanner:             []SpannerConnection{{ConnectionMetadata: ConnectionMetadata{Name: "spanner1"}}},
				Smartsheet:          []SmartsheetConnection{{ConnectionMetadata: ConnectionMetadata{Name: "smartsheet1"}}},
				Attio:               []AttioConnection{{ConnectionMetadata: ConnectionMetadata{Name: "attio1"}}},
				Sftp:                []SFTPConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sftp1"}}},
				ISOCPulse:           []ISOCPulseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "isocpulse1"}}},
				InfluxDB:            []InfluxDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "influxdb1"}}},
				Tableau:             []TableauConnection{{ConnectionMetadata: ConnectionMetadata{Name: "tableau1"}}},
				QuickSight:          []QuickSightConnection{{ConnectionMetadata: ConnectionMetadata{Name: "quicksight1"}}},
				Trino:               []TrinoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "trino1"}}},
				StarRocks:           []StarRocksConnection{{ConnectionMetadata: ConnectionMetadata{Name: "starrocks1"}}},
				Fluxx:               []FluxxConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fluxx1"}}},
				Freshdesk:           []FreshdeskConnection{{ConnectionMetadata: ConnectionMetadata{Name: "freshdesk1"}}},
				FundraiseUp:         []FundraiseUpConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fundraiseup1"}}},
				Fireflies:           []FirefliesConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fireflies1"}}},
				Trello:              []TrelloConnection{{ConnectionMetadata: ConnectionMetadata{Name: "trello1"}}},
				Jira:                []JiraConnection{{ConnectionMetadata: ConnectionMetadata{Name: "jira1"}}},
				Monday:              []MondayConnection{{ConnectionMetadata: ConnectionMetadata{Name: "monday1"}}},
				PlusVibeAI:          []PlusVibeAIConnection{{ConnectionMetadata: ConnectionMetadata{Name: "plusvibeai1"}}},
				BruinCloud:          []BruinCloudConnection{{ConnectionMetadata: ConnectionMetadata{Name: "bruin1"}}},
				Primer:              []PrimerConnection{{ConnectionMetadata: ConnectionMetadata{Name: "primer1"}}},
				Indeed:              []IndeedConnection{{ConnectionMetadata: ConnectionMetadata{Name: "indeed1"}}},
				CustomerIo:          []CustomerIoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "customerio1"}}},
				Sendgrid:            []SendgridConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sendgrid1"}}},
				Twilio:              []TwilioConnection{{ConnectionMetadata: ConnectionMetadata{Name: "twilio1"}}},
				Braze:               []BrazeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "braze1"}}},
				Espn:                []EspnConnection{{ConnectionMetadata: ConnectionMetadata{Name: "espn1"}}},
				APIFootball:         []APIFootballConnection{{ConnectionMetadata: ConnectionMetadata{Name: "apifootball1"}}},
				FootballData:        []FootballDataConnection{{ConnectionMetadata: ConnectionMetadata{Name: "footballdata1"}}},
				BallDontLie:         []BallDontLieConnection{{ConnectionMetadata: ConnectionMetadata{Name: "balldontlie1"}}},
				Vertica:             []VerticaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "vertica1"}}},
				Dune:                []DuneConnection{{ConnectionMetadata: ConnectionMetadata{Name: "dune1"}}},
			},
			want: &Connections{
				AwsConnection:       []AwsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "aws1"}}},
				AthenaConnection:    []AthenaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "athena1"}}},
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gcp1"}}},
				Snowflake:           []SnowflakeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sf1"}}},
				Postgres:            []PostgresConnection{{ConnectionMetadata: ConnectionMetadata{Name: "pg1"}}},
				RedShift:            []RedshiftConnection{{ConnectionMetadata: ConnectionMetadata{Name: "rs1"}}},
				MsSQL:               []MsSQLConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mssql1"}}},
				Databricks:          []DatabricksConnection{{ConnectionMetadata: ConnectionMetadata{Name: "db1"}}},
				ADLS:                []ADLSConnection{{ConnectionMetadata: ConnectionMetadata{Name: "adls1"}}},
				Synapse:             []SynapseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "syn1"}}},
				Fabric:              []FabricConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fabric1"}}},
				Mongo:               []MongoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mongo1"}}},
				Cassandra:           []CassandraConnection{{ConnectionMetadata: ConnectionMetadata{Name: "cassandra1"}}},
				Couchbase:           []CouchbaseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "couchbase1"}}},
				CrateDB:             []CrateDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "cratedb1"}}},
				CSV:                 []CSVConnection{{ConnectionMetadata: ConnectionMetadata{Name: "csv1"}}},
				Cursor:              []CursorConnection{{ConnectionMetadata: ConnectionMetadata{Name: "cursor1"}}},
				MongoAtlas:          []MongoAtlasConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mongoatlas1"}}},
				MySQL:               []MySQLConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mysql1"}}},
				Notion:              []NotionConnection{{ConnectionMetadata: ConnectionMetadata{Name: "notion1"}}},
				Allium:              []AlliumConnection{{ConnectionMetadata: ConnectionMetadata{Name: "allium1"}}},
				HANA:                []HANAConnection{{ConnectionMetadata: ConnectionMetadata{Name: "hana1"}}},
				Hostaway:            []HostawayConnection{{ConnectionMetadata: ConnectionMetadata{Name: "hostaway1"}}},
				HTTP:                []HTTPConnection{{ConnectionMetadata: ConnectionMetadata{Name: "http1"}}},
				Shopify:             []ShopifyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "shopify1"}}},
				Gorgias:             []GorgiasConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gorgias1"}}},
				G2:                  []G2Connection{{ConnectionMetadata: ConnectionMetadata{Name: "g21"}}},
				Klaviyo:             []KlaviyoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "klaviyo1"}}},
				Adjust:              []AdjustConnection{{ConnectionMetadata: ConnectionMetadata{Name: "adjust1"}}},
				Adapty:              []AdaptyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "adapty1"}}},
				SurveyMonkey:        []SurveyMonkeyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "surveymonkey1"}}},
				Anthropic:           []AnthropicConnection{{ConnectionMetadata: ConnectionMetadata{Name: "anthropic1"}}},
				Generic:             []GenericConnection{{ConnectionMetadata: ConnectionMetadata{Name: "generic1"}}},
				FacebookAds:         []FacebookAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "facebookads1"}}},
				Stripe:              []StripeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "stripe1"}}},
				Paddle:              []PaddleConnection{{ConnectionMetadata: ConnectionMetadata{Name: "paddle1"}}},
				Chargebee:           []ChargebeeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "chargebee1"}}},
				Recurly:             []RecurlyConnection{{ConnectionMetadata: ConnectionMetadata{Name: "recurly1"}}},
				GitLab:              []GitLabConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gitlab1"}}},
				Appsflyer:           []AppsflyerConnection{{ConnectionMetadata: ConnectionMetadata{Name: "appsflyer1"}}},
				Kafka:               []KafkaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "kafka1"}}},
				RabbitMQ:            []RabbitMQConnection{{ConnectionMetadata: ConnectionMetadata{Name: "rabbitmq1"}}},
				DuckDB:              []DuckDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "duckdb1"}}},
				MotherDuck:          []MotherduckConnection{{ConnectionMetadata: ConnectionMetadata{Name: "motherduck1"}}},
				ClickHouse:          []ClickHouseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "clickhouse1"}}},
				Hubspot:             []HubspotConnection{{ConnectionMetadata: ConnectionMetadata{Name: "hubspot1"}}},
				Intercom:            []IntercomConnection{{ConnectionMetadata: ConnectionMetadata{Name: "intercom1"}}},
				GitHub:              []GitHubConnection{{ConnectionMetadata: ConnectionMetadata{Name: "github1"}}},
				GoogleSheets:        []GoogleSheetsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "googlesheets1"}}},
				Chess:               []ChessConnection{{ConnectionMetadata: ConnectionMetadata{Name: "chess1"}}},
				Airtable:            []AirtableConnection{{ConnectionMetadata: ConnectionMetadata{Name: "airtable1"}}},
				Granola:             []GranolaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "granola1"}}},
				Zendesk:             []ZendeskConnection{{ConnectionMetadata: ConnectionMetadata{Name: "zendesk1"}}},
				Kalshi:              []KalshiConnection{{ConnectionMetadata: ConnectionMetadata{Name: "kalshi1"}}},
				TikTokAds:           []TikTokAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "tiktokads1"}}},
				SnapchatAds:         []SnapchatAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "snapchatads1"}}},
				S3:                  []S3Connection{{ConnectionMetadata: ConnectionMetadata{Name: "s31"}}},
				Slack:               []SlackConnection{{ConnectionMetadata: ConnectionMetadata{Name: "slack1"}}},
				Socrata:             []SocrataConnection{{ConnectionMetadata: ConnectionMetadata{Name: "socrata1"}}},
				Asana:               []AsanaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "asana1"}}},
				DynamoDB:            []DynamoDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "dynamodb1"}}},
				Docebo:              []DoceboConnection{{ConnectionMetadata: ConnectionMetadata{Name: "docebo1"}}},
				GoogleAds:           []GoogleAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "googleads1"}}},
				AppStore:            []AppStoreConnection{{ConnectionMetadata: ConnectionMetadata{Name: "appstore1"}}},
				AppleAds:            []AppleAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "appleads1"}}},
				LinkedInAds:         []LinkedInAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "linkedinads1"}}},
				RedditAds:           []RedditAdsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "redditads1"}}},
				Mailchimp:           []MailchimpConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mailchimp1"}}},
				Manifold:            []ManifoldConnection{{ConnectionMetadata: ConnectionMetadata{Name: "manifold1"}}},
				RevenueCat:          []RevenueCatConnection{{ConnectionMetadata: ConnectionMetadata{Name: "revenuecat1"}}},
				Linear:              []LinearConnection{{ConnectionMetadata: ConnectionMetadata{Name: "linear1"}}},
				GCS:                 []GCSConnection{{ConnectionMetadata: ConnectionMetadata{Name: "gcs1"}}},
				SharePoint:          []SharePointConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sharepoint1"}}},
				ApplovinMax:         []ApplovinMaxConnection{{ConnectionMetadata: ConnectionMetadata{Name: "applovinmax1"}}},
				Personio:            []PersonioConnection{{ConnectionMetadata: ConnectionMetadata{Name: "personio1"}}},
				Kinesis:             []KinesisConnection{{ConnectionMetadata: ConnectionMetadata{Name: "kinesis1"}}},
				Pipedrive:           []PipedriveConnection{{ConnectionMetadata: ConnectionMetadata{Name: "pipedrive1"}}},
				Polymarket:          []PolymarketConnection{{ConnectionMetadata: ConnectionMetadata{Name: "polymarket1"}}},
				Mixpanel:            []MixpanelConnection{{ConnectionMetadata: ConnectionMetadata{Name: "mixpanel1"}}},
				Amplitude:           []AmplitudeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "amplitude1"}}},
				Fastspring:          []FastspringConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fastspring1"}}},
				Payrails:            []PayrailsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "payrails1"}}},
				Clickup:             []ClickupConnection{{ConnectionMetadata: ConnectionMetadata{Name: "clickup1"}}},
				Jobtread:            []JobtreadConnection{{ConnectionMetadata: ConnectionMetadata{Name: "jobtread1"}}},
				Posthog:             []PosthogConnection{{ConnectionMetadata: ConnectionMetadata{Name: "posthog1"}}},
				Pinterest:           []PinterestConnection{{ConnectionMetadata: ConnectionMetadata{Name: "pinterest1"}}},
				Trustpilot:          []TrustpilotConnection{{ConnectionMetadata: ConnectionMetadata{Name: "trustpilot1"}}},
				QuickBooks:          []QuickBooksConnection{{ConnectionMetadata: ConnectionMetadata{Name: "quickbooks1"}}},
				Wise:                []WiseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "wise1"}}},
				Wistia:              []WistiaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "wistia1"}}},
				Zoom:                []ZoomConnection{{ConnectionMetadata: ConnectionMetadata{Name: "zoom1"}}},
				EMRServerless:       []EMRServerlessConnection{{ConnectionMetadata: ConnectionMetadata{Name: "emr1"}}},
				DataprocServerless:  []DataprocServerlessConnection{{ConnectionMetadata: ConnectionMetadata{Name: "dataproc1"}}},
				GoogleAnalytics:     []GoogleAnalyticsConnection{{ConnectionMetadata: ConnectionMetadata{Name: "googleanalytics1"}}},
				GSC:                 []GSCConnection{{Name: "gsc1"}},
				AppLovin:            []AppLovinConnection{{ConnectionMetadata: ConnectionMetadata{Name: "applovin1"}}},
				Frankfurter:         []FrankfurterConnection{{ConnectionMetadata: ConnectionMetadata{Name: "frankfurter1"}}},
				Salesforce:          []SalesforceConnection{{ConnectionMetadata: ConnectionMetadata{Name: "salesforce1"}}},
				SQLite:              []SQLiteConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sqlite1"}}},
				DB2:                 []DB2Connection{{ConnectionMetadata: ConnectionMetadata{Name: "db21"}}},
				Oracle:              []OracleConnection{{ConnectionMetadata: ConnectionMetadata{Name: "oracle1"}}},
				Phantombuster:       []PhantombusterConnection{{ConnectionMetadata: ConnectionMetadata{Name: "phantombuster1"}}},
				Elasticsearch:       []ElasticsearchConnection{{ConnectionMetadata: ConnectionMetadata{Name: "elasticsearch1"}}},
				Solidgate:           []SolidgateConnection{{ConnectionMetadata: ConnectionMetadata{Name: "solidgate1"}}},
				Square:              []SquareConnection{{ConnectionMetadata: ConnectionMetadata{Name: "square1"}}},
				Spanner:             []SpannerConnection{{ConnectionMetadata: ConnectionMetadata{Name: "spanner1"}}},
				Smartsheet:          []SmartsheetConnection{{ConnectionMetadata: ConnectionMetadata{Name: "smartsheet1"}}},
				Attio:               []AttioConnection{{ConnectionMetadata: ConnectionMetadata{Name: "attio1"}}},
				Sftp:                []SFTPConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sftp1"}}},
				ISOCPulse:           []ISOCPulseConnection{{ConnectionMetadata: ConnectionMetadata{Name: "isocpulse1"}}},
				InfluxDB:            []InfluxDBConnection{{ConnectionMetadata: ConnectionMetadata{Name: "influxdb1"}}},
				Tableau:             []TableauConnection{{ConnectionMetadata: ConnectionMetadata{Name: "tableau1"}}},
				QuickSight:          []QuickSightConnection{{ConnectionMetadata: ConnectionMetadata{Name: "quicksight1"}}},
				Trino:               []TrinoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "trino1"}}},
				StarRocks:           []StarRocksConnection{{ConnectionMetadata: ConnectionMetadata{Name: "starrocks1"}}},
				Fluxx:               []FluxxConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fluxx1"}}},
				Freshdesk:           []FreshdeskConnection{{ConnectionMetadata: ConnectionMetadata{Name: "freshdesk1"}}},
				FundraiseUp:         []FundraiseUpConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fundraiseup1"}}},
				Fireflies:           []FirefliesConnection{{ConnectionMetadata: ConnectionMetadata{Name: "fireflies1"}}},
				Trello:              []TrelloConnection{{ConnectionMetadata: ConnectionMetadata{Name: "trello1"}}},
				Jira:                []JiraConnection{{ConnectionMetadata: ConnectionMetadata{Name: "jira1"}}},
				Monday:              []MondayConnection{{ConnectionMetadata: ConnectionMetadata{Name: "monday1"}}},
				PlusVibeAI:          []PlusVibeAIConnection{{ConnectionMetadata: ConnectionMetadata{Name: "plusvibeai1"}}},
				BruinCloud:          []BruinCloudConnection{{ConnectionMetadata: ConnectionMetadata{Name: "bruin1"}}},
				Primer:              []PrimerConnection{{ConnectionMetadata: ConnectionMetadata{Name: "primer1"}}},
				Indeed:              []IndeedConnection{{ConnectionMetadata: ConnectionMetadata{Name: "indeed1"}}},
				CustomerIo:          []CustomerIoConnection{{ConnectionMetadata: ConnectionMetadata{Name: "customerio1"}}},
				Sendgrid:            []SendgridConnection{{ConnectionMetadata: ConnectionMetadata{Name: "sendgrid1"}}},
				Twilio:              []TwilioConnection{{ConnectionMetadata: ConnectionMetadata{Name: "twilio1"}}},
				Braze:               []BrazeConnection{{ConnectionMetadata: ConnectionMetadata{Name: "braze1"}}},
				Espn:                []EspnConnection{{ConnectionMetadata: ConnectionMetadata{Name: "espn1"}}},
				APIFootball:         []APIFootballConnection{{ConnectionMetadata: ConnectionMetadata{Name: "apifootball1"}}},
				FootballData:        []FootballDataConnection{{ConnectionMetadata: ConnectionMetadata{Name: "footballdata1"}}},
				BallDontLie:         []BallDontLieConnection{{ConnectionMetadata: ConnectionMetadata{Name: "balldontlie1"}}},
				Vertica:             []VerticaConnection{{ConnectionMetadata: ConnectionMetadata{Name: "vertica1"}}},
				Dune:                []DuneConnection{{ConnectionMetadata: ConnectionMetadata{Name: "dune1"}}},
			},
			expectedErr: false,
		},
		{
			name: "merge existing GCP connections",
			target: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "gcp-dev"},
						ServiceAccountJSON: "{\"key\": \"dev-value\"}",
						ProjectID:          "dev-project",
					},
					{
						ConnectionMetadata: ConnectionMetadata{Name: "gcp-staging"},
						ServiceAccountJSON: "{\"key\": \"staging-value\"}",
						ProjectID:          "staging-project",
					},
				},
			},
			source: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "gcp-prod"},
						ServiceAccountJSON: "{\"key\": \"prod-value\"}",
						ProjectID:          "prod-project",
					},
				},
			},
			want: &Connections{
				GoogleCloudPlatform: []GoogleCloudPlatformConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "gcp-dev"},
						ServiceAccountJSON: "{\"key\": \"dev-value\"}",
						ProjectID:          "dev-project",
					},
					{
						ConnectionMetadata: ConnectionMetadata{Name: "gcp-staging"},
						ServiceAccountJSON: "{\"key\": \"staging-value\"}",
						ProjectID:          "staging-project",
					},
					{
						ConnectionMetadata: ConnectionMetadata{Name: "gcp-prod"},
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
						ConnectionMetadata: ConnectionMetadata{Name: "pg-dev"},
						Host:               "dev-host",
						Port:               5432,
						Username:           "dev-user",
						Password:           "dev-pass",
						Database:           "dev-db",
					},
				},
				MySQL: []MySQLConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "mysql-dev"},
						Host:               "dev-host",
						Port:               3306,
						Username:           "dev-user",
						Password:           "dev-pass",
						Database:           "dev-db",
					},
				},
			},
			source: &Connections{
				Postgres: []PostgresConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "pg-prod"},
						Host:               "prod-host",
						Port:               5432,
						Username:           "prod-user",
						Password:           "prod-pass",
						Database:           "prod-db",
					},
				},
				MySQL: []MySQLConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "mysql-prod"},
						Host:               "prod-host",
						Port:               3306,
						Username:           "prod-user",
						Password:           "prod-pass",
						Database:           "prod-db",
					},
				},
			},
			want: &Connections{
				Postgres: []PostgresConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "pg-dev"},
						Host:               "dev-host",
						Port:               5432,
						Username:           "dev-user",
						Password:           "dev-pass",
						Database:           "dev-db",
					},
					{
						ConnectionMetadata: ConnectionMetadata{Name: "pg-prod"},
						Host:               "prod-host",
						Port:               5432,
						Username:           "prod-user",
						Password:           "prod-pass",
						Database:           "prod-db",
					},
				},
				MySQL: []MySQLConnection{
					{
						ConnectionMetadata: ConnectionMetadata{Name: "mysql-dev"},
						Host:               "dev-host",
						Port:               3306,
						Username:           "dev-user",
						Password:           "dev-pass",
						Database:           "dev-db",
					},
					{
						ConnectionMetadata: ConnectionMetadata{Name: "mysql-prod"},
						Host:               "prod-host",
						Port:               3306,
						Username:           "prod-user",
						Password:           "prod-pass",
						Database:           "prod-db",
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
						ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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
							ConnectionMetadata: ConnectionMetadata{Name: "conn1"},
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

func TestLoadFromFileOrEnv_EnvironmentConfig(t *testing.T) {
	t.Setenv("BRUIN_CONFIG_FILE_CONTENT", `default_environment: prod
environments:
  prod:
    config:
      full_refresh_restricted: true
    connections:
      duckdb:
        - name: duckdb-default
          path: duckdb.db`)

	fs := afero.NewReadOnlyFs(afero.NewOsFs())
	got, err := LoadFromFileOrEnv(fs, "testdata/nonexistent.yml")
	require.NoError(t, err)

	require.NotNil(t, got.SelectedEnvironment.Config)
	assert.True(t, got.SelectedEnvironment.Config.RefreshRestricted)
	assert.True(t, got.Environments["prod"].Config.RefreshRestricted)
}

func TestLoadFromFileOrEnv_ExpandsEnvironmentVariablesBeforeDecode(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "placeholder-host.invalid")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_DB", "dummy")
	t.Setenv("POSTGRES_USER", "dummy_user")
	t.Setenv("POSTGRES_PASSWORD", "null")
	t.Setenv("DUCKLAKE_DATA_PATH", "s3://dummy/warehouse")
	t.Setenv("AWS_REGION", "auto")
	t.Setenv("R2_S3_ENDPOINT", "dummy.r2.cloudflarestorage.com")
	t.Setenv("AWS_ACCESS_KEY_ID", "access:key # hash")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "dummy_secret")

	fs := afero.NewMemMapFs()
	configPath := filepath.Join("repo", ".bruin.yml")
	require.NoError(t, fs.MkdirAll(filepath.Dir(configPath), 0o755))
	require.NoError(t, afero.WriteFile(fs, configPath, []byte(`default_environment: production
environments:
  production:
    connections:
      duckdb:
        - name: ducklake
          path: repro-ducklake.duckdb
          lakehouse:
            format: ducklake
            catalog:
              type: postgres
              host: "${POSTGRES_HOST}"
              port: "${POSTGRES_PORT}"
              database: "${POSTGRES_DB}"
              auth:
                username: "${POSTGRES_USER}"
                password: "${POSTGRES_PASSWORD}"
            storage:
              type: s3
              path: "${DUCKLAKE_DATA_PATH}"
              region: "${AWS_REGION}"
              endpoint: "${R2_S3_ENDPOINT}"
              url_style: path
              auth:
                access_key: ${AWS_ACCESS_KEY_ID}
                secret_key: "${AWS_SECRET_ACCESS_KEY}"
`), 0o644))

	got, err := LoadFromFileOrEnv(fs, configPath)
	require.NoError(t, err)

	require.Len(t, got.SelectedEnvironment.Connections.DuckDB, 1)
	conn := got.SelectedEnvironment.Connections.DuckDB[0]
	require.NotNil(t, conn.Lakehouse)

	assert.Equal(t, LakehouseFormatDuckLake, conn.Lakehouse.Format)
	assert.Equal(t, CatalogTypePostgres, conn.Lakehouse.Catalog.Type)
	assert.Equal(t, "placeholder-host.invalid", conn.Lakehouse.Catalog.Host)
	assert.Equal(t, 5433, conn.Lakehouse.Catalog.Port)
	assert.Equal(t, "dummy", conn.Lakehouse.Catalog.Database)
	assert.Equal(t, "dummy_user", conn.Lakehouse.Catalog.Auth.Username)
	assert.Equal(t, "null", conn.Lakehouse.Catalog.Auth.Password)
	assert.Equal(t, StorageTypeS3, conn.Lakehouse.Storage.Type)
	assert.Equal(t, "s3://dummy/warehouse", conn.Lakehouse.Storage.Path)
	assert.Equal(t, "auto", conn.Lakehouse.Storage.Region)
	assert.Equal(t, "dummy.r2.cloudflarestorage.com", conn.Lakehouse.Storage.Endpoint)
	assert.Equal(t, "path", conn.Lakehouse.Storage.URLStyle)
	assert.Equal(t, "access:key # hash", conn.Lakehouse.Storage.Auth.AccessKey)
	assert.Equal(t, "dummy_secret", conn.Lakehouse.Storage.Auth.SecretKey)
}

func TestLoadFromFileOrEnv_LeavesUnsetEnvironmentVariablesLiteral(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	configPath := filepath.Join("repo", ".bruin.yml")
	require.NoError(t, fs.MkdirAll(filepath.Dir(configPath), 0o755))
	require.NoError(t, afero.WriteFile(fs, configPath, []byte(`default_environment: production
environments:
  production:
    connections:
      postgres:
        - name: literal-env
          host: localhost
          username: bruin
          password: "secret-${BRUIN_TEST_UNSET_CONFIG_PASSWORD}"
          database: analytics
          port: 5432
`), 0o644))

	got, err := LoadFromFileOrEnv(fs, configPath)

	require.NoError(t, err)
	require.Len(t, got.SelectedEnvironment.Connections.Postgres, 1)
	conn := got.SelectedEnvironment.Connections.Postgres[0]
	assert.Equal(t, "secret-${BRUIN_TEST_UNSET_CONFIG_PASSWORD}", conn.Password)
	assert.Equal(t, 5432, conn.Port)
}

func TestSnowflakeConnection_MarshalYAML_PrivateKey(t *testing.T) {
	t.Parallel()

	privateKey := `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7ZtFJp7nR3sL9
Xy8kPq2jLmV9bN3oR6tUwVcXzA1bH2cY4eF5gH6jK7lM8nBvCxDsEwQrTyUiOp
-----END PRIVATE KEY-----`

	conn := SnowflakeConnection{
		ConnectionMetadata: ConnectionMetadata{Name: "test-snowflake"},
		Account:            "test-account",
		Username:           "test-user",
		Database:           "test-db",
		PrivateKey:         privateKey,
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
		ConnectionMetadata: ConnectionMetadata{Name: "test-snowflake"},
		Account:            "test-account",
		Username:           "test-user",
		Password:           "test-pass",
		Database:           "test-db",
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
		ConnectionMetadata: ConnectionMetadata{Name: "test-snowflake"},
		Account:            "test-account",
		Username:           "test-user",
		Database:           "test-db",
		PrivateKey:         singleLineKey,
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

func TestConnectionMetadata_MaxConcurrentAssetsYAMLParsing(t *testing.T) {
	t.Parallel()

	var conf Config
	err := yaml.Unmarshal([]byte(`
environments:
  default:
    connections:
      postgres:
        - name: flaky_source
          host: localhost
          username: bruin
          password: secret
          database: analytics
          max_concurrent_assets: 1
`), &conf)
	require.NoError(t, err)

	conn := conf.Environments["default"].Connections.Postgres[0]
	require.NotNil(t, conn.MaxConcurrentAssets)
	assert.Equal(t, "flaky_source", conn.Name)
	assert.Equal(t, 1, *conn.MaxConcurrentAssets)
}

func TestConnections_ConnectionConcurrencyLimits(t *testing.T) {
	t.Parallel()

	postgresLimit := 1
	snowflakeLimit := 2
	connections := &Connections{
		Postgres: []PostgresConnection{
			{
				ConnectionMetadata: ConnectionMetadata{
					Name:                "flaky_postgres",
					MaxConcurrentAssets: &postgresLimit,
				},
			},
			{
				ConnectionMetadata: ConnectionMetadata{Name: "unlimited_postgres"},
			},
		},
		Snowflake: []SnowflakeConnection{
			{
				ConnectionMetadata: ConnectionMetadata{
					Name:                "busy_snowflake",
					MaxConcurrentAssets: &snowflakeLimit,
				},
			},
		},
	}

	limits, err := connections.ConnectionConcurrencyLimits()
	require.NoError(t, err)
	assert.Equal(t, map[string]int{
		"flaky_postgres": 1,
		"busy_snowflake": 2,
	}, limits)
}

func TestConnections_ConnectionConcurrencyLimitsValidation(t *testing.T) {
	t.Parallel()

	var nilConnections *Connections
	limits, err := nilConnections.ConnectionConcurrencyLimits()
	require.NoError(t, err)
	assert.Empty(t, limits)

	zero := 0
	_, err = (&Connections{
		Postgres: []PostgresConnection{
			{
				ConnectionMetadata: ConnectionMetadata{
					Name:                "postgres",
					MaxConcurrentAssets: &zero,
				},
			},
		},
	}).ConnectionConcurrencyLimits()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be greater than 0")

	validLimit := 1
	_, err = (&Connections{
		Postgres: []PostgresConnection{
			{
				ConnectionMetadata: ConnectionMetadata{
					MaxConcurrentAssets: &validLimit,
				},
			},
		},
	}).ConnectionConcurrencyLimits()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty name")
}

func TestConfig_AddConnectionWithMaxConcurrentAssets(t *testing.T) {
	t.Parallel()

	conf := &Config{
		Environments: map[string]Environment{
			"default": {Connections: &Connections{}},
		},
	}

	err := conf.AddConnection("default", "flaky-secret", "generic", map[string]interface{}{
		"value":                 "secret-value",
		"max_concurrent_assets": float64(2),
	})
	require.NoError(t, err)

	conn := conf.Environments["default"].Connections.Generic[0]
	require.NotNil(t, conn.MaxConcurrentAssets)
	assert.Equal(t, 2, *conn.MaxConcurrentAssets)
}

func TestConfig_AddConnectionRejectsInvalidMaxConcurrentAssets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value interface{}
	}{
		{name: "zero", value: 0},
		{name: "negative", value: -1},
		{name: "fractional", value: 1.5},
		{name: "string", value: "not-an-int"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf := &Config{
				Environments: map[string]Environment{
					"default": {Connections: &Connections{}},
				},
			}

			err := conf.AddConnection("default", "bad-secret", "generic", map[string]interface{}{
				"value":                 "secret-value",
				"max_concurrent_assets": tt.value,
			})
			require.Error(t, err)
			assert.Empty(t, conf.Environments["default"].Connections.Generic)
		})
	}
}

func TestGetConnectionFieldsForTypeIncludesMaxConcurrentAssets(t *testing.T) {
	t.Parallel()

	fields := GetConnectionFieldsForType("postgres")
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}

	assert.Contains(t, names, "max_concurrent_assets")
	assert.NotContains(t, names, "name")
}

func TestConnectionsSchemaIncludesMaxConcurrentAssets(t *testing.T) {
	t.Parallel()

	schema, err := GetConnectionsSchema()
	require.NoError(t, err)
	assert.Contains(t, schema, "max_concurrent_assets")
}

func TestConnectionCustomMarshalersPreserveMaxConcurrentAssets(t *testing.T) {
	t.Parallel()

	limit := 2

	t.Run("gcp json", func(t *testing.T) {
		t.Parallel()
		payload := mustMarshalConnectionJSON(t, GoogleCloudPlatformConnection{
			ConnectionMetadata: ConnectionMetadata{Name: "gcp", MaxConcurrentAssets: &limit},
			ProjectID:          "project",
		})
		assertJSONMaxConcurrentAssets(t, payload, limit)
	})

	t.Run("gcp yaml", func(t *testing.T) {
		t.Parallel()
		data, err := yaml.Marshal(GoogleCloudPlatformConnection{
			ConnectionMetadata: ConnectionMetadata{Name: "gcp", MaxConcurrentAssets: &limit},
			ProjectID:          "project",
		})
		require.NoError(t, err)
		assert.Contains(t, string(data), "max_concurrent_assets: 2")
	})

	t.Run("athena yaml", func(t *testing.T) {
		t.Parallel()
		data, err := yaml.Marshal(AthenaConnection{
			ConnectionMetadata: ConnectionMetadata{Name: "athena", MaxConcurrentAssets: &limit},
			Database:           "analytics",
		})
		require.NoError(t, err)
		assert.Contains(t, string(data), "max_concurrent_assets: 2")
	})

	t.Run("snowflake json", func(t *testing.T) {
		t.Parallel()
		payload := mustMarshalConnectionJSON(t, SnowflakeConnection{
			ConnectionMetadata: ConnectionMetadata{Name: "snowflake", MaxConcurrentAssets: &limit},
			Account:            "account",
		})
		assertJSONMaxConcurrentAssets(t, payload, limit)
	})

	t.Run("snowflake yaml", func(t *testing.T) {
		t.Parallel()
		data, err := yaml.Marshal(SnowflakeConnection{
			ConnectionMetadata: ConnectionMetadata{Name: "snowflake", MaxConcurrentAssets: &limit},
			Account:            "account",
		})
		require.NoError(t, err)
		assert.Contains(t, string(data), "max_concurrent_assets: 2")
	})

	t.Run("generic json", func(t *testing.T) {
		t.Parallel()
		payload := mustMarshalConnectionJSON(t, GenericConnection{
			ConnectionMetadata: ConnectionMetadata{Name: "generic", MaxConcurrentAssets: &limit},
			Value:              "secret",
		})
		assertJSONMaxConcurrentAssets(t, payload, limit)
	})
}

func assertJSONMaxConcurrentAssets(t *testing.T, payload map[string]interface{}, want int) {
	t.Helper()

	got, ok := payload["max_concurrent_assets"].(float64)
	require.True(t, ok)
	assert.InDelta(t, float64(want), got, 0)
}

func mustMarshalConnectionJSON(t *testing.T, conn interface{}) map[string]interface{} {
	t.Helper()

	data, err := json.Marshal(conn)
	require.NoError(t, err)

	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &payload))
	return payload
}
