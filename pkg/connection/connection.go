package connection

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/adjust"
	"github.com/bruin-data/bruin/pkg/airtable"
	"github.com/bruin-data/bruin/pkg/allium"
	"github.com/bruin-data/bruin/pkg/anthropic"
	"github.com/bruin-data/bruin/pkg/applovin"
	"github.com/bruin-data/bruin/pkg/applovinmax"
	"github.com/bruin-data/bruin/pkg/appsflyer"
	"github.com/bruin-data/bruin/pkg/appstore"
	"github.com/bruin-data/bruin/pkg/asana"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/attio"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/bruin-data/bruin/pkg/chess"
	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/clickup"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/couchbase"
	"github.com/bruin-data/bruin/pkg/cursor"
	"github.com/bruin-data/bruin/pkg/databricks"
	dataprocserverless "github.com/bruin-data/bruin/pkg/dataproc_serverless"
	"github.com/bruin-data/bruin/pkg/db2"
	"github.com/bruin-data/bruin/pkg/docebo"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/dynamodb"
	"github.com/bruin-data/bruin/pkg/elasticsearch"
	"github.com/bruin-data/bruin/pkg/emr_serverless"
	"github.com/bruin-data/bruin/pkg/fabric_warehouse"
	"github.com/bruin-data/bruin/pkg/facebookads"
	"github.com/bruin-data/bruin/pkg/fireflies"
	"github.com/bruin-data/bruin/pkg/fluxx"
	"github.com/bruin-data/bruin/pkg/frankfurter"
	"github.com/bruin-data/bruin/pkg/freshdesk"
	"github.com/bruin-data/bruin/pkg/fundraiseup"
	"github.com/bruin-data/bruin/pkg/gcs"
	"github.com/bruin-data/bruin/pkg/github"
	"github.com/bruin-data/bruin/pkg/googleads"
	"github.com/bruin-data/bruin/pkg/googleanalytics"
	"github.com/bruin-data/bruin/pkg/gorgias"
	"github.com/bruin-data/bruin/pkg/gsheets"
	"github.com/bruin-data/bruin/pkg/hana"
	"github.com/bruin-data/bruin/pkg/hostaway"
	"github.com/bruin-data/bruin/pkg/hubspot"
	"github.com/bruin-data/bruin/pkg/indeed"
	"github.com/bruin-data/bruin/pkg/influxdb"
	"github.com/bruin-data/bruin/pkg/intercom"
	"github.com/bruin-data/bruin/pkg/isocpulse"
	"github.com/bruin-data/bruin/pkg/jira"
	"github.com/bruin-data/bruin/pkg/kafka"
	"github.com/bruin-data/bruin/pkg/kinesis"
	"github.com/bruin-data/bruin/pkg/klaviyo"
	"github.com/bruin-data/bruin/pkg/linear"
	"github.com/bruin-data/bruin/pkg/linkedinads"
	"github.com/bruin-data/bruin/pkg/mailchimp"
	"github.com/bruin-data/bruin/pkg/mixpanel"
	"github.com/bruin-data/bruin/pkg/monday"
	"github.com/bruin-data/bruin/pkg/mongo"
	mongoatlas "github.com/bruin-data/bruin/pkg/mongo_atlas"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/notion"
	"github.com/bruin-data/bruin/pkg/oracle"
	"github.com/bruin-data/bruin/pkg/personio"
	"github.com/bruin-data/bruin/pkg/phantombuster"
	"github.com/bruin-data/bruin/pkg/pinterest"
	"github.com/bruin-data/bruin/pkg/pipedrive"
	"github.com/bruin-data/bruin/pkg/plusvibeai"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/primer"
	"github.com/bruin-data/bruin/pkg/quickbooks"
	"github.com/bruin-data/bruin/pkg/revenuecat"
	"github.com/bruin-data/bruin/pkg/s3"
	"github.com/bruin-data/bruin/pkg/salesforce"
	"github.com/bruin-data/bruin/pkg/sftp"
	"github.com/bruin-data/bruin/pkg/shopify"
	"github.com/bruin-data/bruin/pkg/slack"
	"github.com/bruin-data/bruin/pkg/smartsheet"
	"github.com/bruin-data/bruin/pkg/snapchatads"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/bruin-data/bruin/pkg/socrata"
	"github.com/bruin-data/bruin/pkg/solidgate"
	"github.com/bruin-data/bruin/pkg/spanner"
	"github.com/bruin-data/bruin/pkg/sqlite"
	"github.com/bruin-data/bruin/pkg/stripe"
	"github.com/bruin-data/bruin/pkg/tableau"
	"github.com/bruin-data/bruin/pkg/tiktokads"
	"github.com/bruin-data/bruin/pkg/trino"
	"github.com/bruin-data/bruin/pkg/trustpilot"
	"github.com/bruin-data/bruin/pkg/wise"
	"github.com/bruin-data/bruin/pkg/zendesk"
	"github.com/bruin-data/bruin/pkg/zoom"
	"github.com/pkg/errors"
	"github.com/sourcegraph/conc"
)

type Manager struct {
	AllConnectionDetails map[string]any
	BigQuery             map[string]*bigquery.Client
	Snowflake            map[string]*snowflake.DB
	Postgres             map[string]*postgres.Client
	MsSQL                map[string]*mssql.DB
	Databricks           map[string]*databricks.DB
	FabricWarehouse      map[string]*fabric_warehouse.DB
	Mongo                map[string]*mongo.DB
	Couchbase            map[string]*couchbase.DB
	Cursor               map[string]*cursor.Client
	MongoAtlas           map[string]*mongoatlas.DB
	Mysql                map[string]*mysql.Client
	Notion               map[string]*notion.Client
	Allium               map[string]*allium.Client
	HANA                 map[string]*hana.Client
	Hostaway             map[string]*hostaway.Client
	Shopify              map[string]*shopify.Client
	Gorgias              map[string]*gorgias.Client
	Klaviyo              map[string]*klaviyo.Client
	Adjust               map[string]*adjust.Client
	Anthropic            map[string]*anthropic.Client
	Intercom             map[string]*intercom.Client
	Athena               map[string]*athena.DB
	Aws                  map[string]*config.AwsConnection
	FacebookAds          map[string]*facebookads.Client
	Stripe               map[string]*stripe.Client
	Appsflyer            map[string]*appsflyer.Client
	Kafka                map[string]*kafka.Client
	Airtable             map[string]*airtable.Client
	DuckDB               map[string]*duck.Client
	Hubspot              map[string]*hubspot.Client
	GoogleSheets         map[string]*gsheets.Client
	Chess                map[string]*chess.Client
	S3                   map[string]*s3.Client
	Slack                map[string]*slack.Client
	Socrata              map[string]*socrata.Client
	Asana                map[string]*asana.Client
	DynamoDB             map[string]*dynamodb.Client
	Docebo               map[string]*docebo.Client
	Zendesk              map[string]*zendesk.Client
	GoogleAds            map[string]*googleads.Client
	TikTokAds            map[string]*tiktokads.Client
	SnapchatAds          map[string]*snapchatads.Client
	GitHub               map[string]*github.Client
	AppStore             map[string]*appstore.Client
	LinkedInAds          map[string]*linkedinads.Client
	Mailchimp            map[string]*mailchimp.Client
	Linear               map[string]*linear.Client
	RevenueCat           map[string]*revenuecat.Client
	ClickHouse           map[string]*clickhouse.Client
	GCS                  map[string]*gcs.Client
	ApplovinMax          map[string]*applovinmax.Client
	Personio             map[string]*personio.Client
	Kinesis              map[string]*kinesis.Client
	Pipedrive            map[string]*pipedrive.Client
	Mixpanel             map[string]*mixpanel.Client
	Clickup              map[string]*clickup.Client
	Pinterest            map[string]*pinterest.Client
	Trustpilot           map[string]*trustpilot.Client
	QuickBooks           map[string]*quickbooks.Client
	Wise                 map[string]*wise.Client
	Zoom                 map[string]*zoom.Client
	Frankfurter          map[string]*frankfurter.Client
	Fluxx                map[string]*fluxx.Client
	Freshdesk            map[string]*freshdesk.Client
	FundraiseUp          map[string]*fundraiseup.Client
	Fireflies            map[string]*fireflies.Client
	Jira                 map[string]*jira.Client
	Monday               map[string]*monday.Client
	PlusVibeAI           map[string]*plusvibeai.Client
	BruinCloud           map[string]*bruincloud.Client
	Primer               map[string]*primer.Client
	Indeed               map[string]*indeed.Client
	EMRSeverless         map[string]*emr_serverless.Client
	DataprocServerless   map[string]*dataprocserverless.Client
	GoogleAnalytics      map[string]*googleanalytics.Client
	AppLovin             map[string]*applovin.Client
	Salesforce           map[string]*salesforce.Client
	SQLite               map[string]*sqlite.Client
	DB2                  map[string]*db2.Client
	Oracle               map[string]*oracle.Client
	Phantombuster        map[string]*phantombuster.Client
	Elasticsearch        map[string]*elasticsearch.Client
	Spanner              map[string]*spanner.Client
	Solidgate            map[string]*solidgate.Client
	Smartsheet           map[string]*smartsheet.Client
	Attio                map[string]*attio.Client
	Sftp                 map[string]*sftp.Client
	ISOCPulse            map[string]*isocpulse.Client
	InfluxDB             map[string]*influxdb.Client
	Tableau              map[string]*tableau.Client
	Trino                map[string]*trino.Client
	Generic              map[string]*config.GenericConnection
	mutex                sync.Mutex
	availableConnections map[string]any
}

func (m *Manager) GetConnection(name string) any {
	connection, ok := m.availableConnections[name]
	if !ok {
		return nil
	}
	return connection
}

func (m *Manager) GetConnectionDetails(name string) any {
	connection, ok := m.AllConnectionDetails[name]
	if !ok {
		return nil
	}
	return connection
}

func convertPKCS1ToPKCS8(privateKey string) string {
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return privateKey
	}

	if block.Type == "RSA PRIVATE KEY" {
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return privateKey
		}

		pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return privateKey
		}

		pkcs8Block := &pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: pkcs8Bytes,
		}

		return string(pem.EncodeToMemory(pkcs8Block))
	}

	return privateKey
}

func (m *Manager) AddBqConnectionFromConfig(connection *config.GoogleCloudPlatformConnection) error {
	m.mutex.Lock()
	if m.BigQuery == nil {
		m.BigQuery = make(map[string]*bigquery.Client)
	}
	m.mutex.Unlock()

	// Check if we have valid credentials configuration
	hasExplicitCredentials := len(connection.ServiceAccountFile) > 0 || len(connection.ServiceAccountJSON) > 0 || connection.GetCredentials() != nil

	if !hasExplicitCredentials && !connection.UseApplicationDefaultCredentials {
		return errors.New("credentials are required: provide either service_account_file, service_account_json, or enable use_application_default_credentials")
	}

	// Validate ServiceAccountFile if provided.
	if len(connection.ServiceAccountFile) > 0 {
		if err := validateServiceAccountFile(connection.ServiceAccountFile); err != nil {
			return err
		}
	}

	// Validate ServiceAccountJSON if provided.
	if len(connection.ServiceAccountJSON) > 0 {
		if err := validateServiceAccountJSON(connection.ServiceAccountJSON); err != nil {
			return err
		}
	}

	// Note: ADC validation is deferred - it will only happen when the client is actually used.
	// This allows pipelines without BigQuery assets to run even if ADC is not configured.
	db, err := bigquery.NewDB(&bigquery.Config{
		ProjectID:                        connection.ProjectID,
		CredentialsFilePath:              connection.ServiceAccountFile,
		CredentialsJSON:                  connection.ServiceAccountJSON,
		Credentials:                      connection.GetCredentials(),
		Location:                         connection.Location,
		UseApplicationDefaultCredentials: connection.UseApplicationDefaultCredentials,
	})
	if err != nil {
		return err
	}

	// Lock and store the new BigQuery client.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.BigQuery[connection.Name] = db
	m.availableConnections[connection.Name] = db
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddSfConnectionFromConfig(connection *config.SnowflakeConnection) error {
	m.mutex.Lock()
	if m.Snowflake == nil {
		m.Snowflake = make(map[string]*snowflake.DB)
	}
	m.mutex.Unlock()

	privateKey := ""

	// Prioritize the direct PrivateKey field over PrivateKeyPath
	if connection.PrivateKey != "" {
		privateKey = connection.PrivateKey
	} else if connection.PrivateKeyPath != "" {
		privateKeyBytes, err := os.ReadFile(connection.PrivateKeyPath)
		if err != nil {
			return err
		}
		privateKey = string(privateKeyBytes)
	}

	// Convert PKCS#1 to PKCS#8 if needed
	if privateKey != "" {
		privateKey = convertPKCS1ToPKCS8(privateKey)
	}

	db, err := snowflake.NewDB(&snowflake.Config{
		Account:    connection.Account,
		Username:   connection.Username,
		Password:   connection.Password,
		Region:     connection.Region,
		Role:       connection.Role,
		Database:   connection.Database,
		Schema:     connection.Schema,
		Warehouse:  connection.Warehouse,
		PrivateKey: privateKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Snowflake[connection.Name] = db
	m.availableConnections[connection.Name] = db
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAthenaConnectionFromConfig(connection *config.AthenaConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.Athena == nil {
		m.Athena = make(map[string]*athena.DB)
	}

	if connection.Profile != "" {
		if connection.AccessKey != "" || connection.SecretKey != "" {
			return errors.New("access_key and secret_key cannot be provided when profile is specified, you need specify either profile or access_key and secret_key")
		}
		err := connection.LoadCredentialsFromProfile(context.Background())
		if err != nil {
			return err
		}
	}

	m.Athena[connection.Name] = athena.NewDB(&athena.Config{
		Region:          connection.Region,
		OutputBucket:    connection.QueryResultsPath,
		AccessID:        connection.AccessKey,
		SecretAccessKey: connection.SecretKey,
		SessionToken:    connection.SessionToken,
		Database:        connection.Database,
	})
	m.availableConnections[connection.Name] = m.Athena[connection.Name]
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAwsConnectionFromConfig(connection *config.AwsConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.Aws == nil {
		m.Aws = make(map[string]*config.AwsConnection)
	}

	m.Aws[connection.Name] = connection
	m.availableConnections[connection.Name] = m.Aws[connection.Name]
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAlliumConnectionFromConfig(connection *config.AlliumConnection) error {
	m.mutex.Lock()
	if m.Allium == nil {
		m.Allium = make(map[string]*allium.Client)
	}
	m.mutex.Unlock()

	client, err := allium.NewClient(&allium.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Allium[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddPgConnectionFromConfig(ctx context.Context, connection *config.PostgresConnection) error {
	return m.addPgLikeConnectionFromConfig(ctx, connection, false)
}

func (m *Manager) AddRedshiftConnectionFromConfig(ctx context.Context, connection *config.RedshiftConnection) error {
	return m.addRedshiftConnectionFromConfig(ctx, connection)
}

func (m *Manager) addRedshiftConnectionFromConfig(ctx context.Context, connection *config.RedshiftConnection) error {
	m.mutex.Lock()
	if m.Postgres == nil {
		m.Postgres = make(map[string]*postgres.Client)
	}
	m.mutex.Unlock()

	var client *postgres.Client
	var err error
	client, err = postgres.NewClient(ctx, postgres.RedShiftConfig{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Database: connection.Database,
		Schema:   connection.Schema,
		SslMode:  connection.SslMode,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Postgres[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) addPgLikeConnectionFromConfig(ctx context.Context, connection *config.PostgresConnection, redshift bool) error {
	m.mutex.Lock()
	if m.Postgres == nil {
		m.Postgres = make(map[string]*postgres.Client)
	}
	m.mutex.Unlock()

	poolMaxConns := connection.PoolMaxConns
	if connection.PoolMaxConns == 0 {
		poolMaxConns = 10
	}

	var client *postgres.Client
	var err error
	if redshift {
		client, err = postgres.NewClient(ctx, postgres.RedShiftConfig{
			Username: connection.Username,
			Password: connection.Password,
			Host:     connection.Host,
			Port:     connection.Port,
			Database: connection.Database,
			Schema:   connection.Schema,
			SslMode:  connection.SslMode,
		})
	} else {
		client, err = postgres.NewClient(ctx, postgres.Config{
			Username:     connection.Username,
			Password:     connection.Password,
			Host:         connection.Host,
			Port:         connection.Port,
			Database:     connection.Database,
			Schema:       connection.Schema,
			PoolMaxConns: poolMaxConns,
			SslMode:      connection.SslMode,
		})
	}

	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Postgres[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddMsSQLConnectionFromConfig(connection *config.MsSQLConnection) error {
	m.mutex.Lock()
	if m.MsSQL == nil {
		m.MsSQL = make(map[string]*mssql.DB)
	}
	m.mutex.Unlock()

	client, err := mssql.NewDB(&mssql.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Database: connection.Database,
		Query:    connection.Options,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.MsSQL[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddSynapseSQLConnectionFromConfig(connection *config.SynapseConnection) error {
	m.mutex.Lock()
	if m.MsSQL == nil {
		m.MsSQL = make(map[string]*mssql.DB)
	}
	m.mutex.Unlock()

	client, err := mssql.NewDB(&mssql.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Database: connection.Database,
		Query:    connection.Options,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.MsSQL[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddFabricWarehouseConnectionFromConfig(connection *config.FabricWarehouseConnection) error {
	m.mutex.Lock()
	if m.FabricWarehouse == nil {
		m.FabricWarehouse = make(map[string]*fabric_warehouse.DB)
	}
	m.mutex.Unlock()

	client, err := fabric_warehouse.NewDB(&fabric_warehouse.Config{
		Username:                  connection.Username,
		Password:                  connection.Password,
		Host:                      connection.Host,
		Port:                      connection.Port,
		Database:                  connection.Database,
		Options:                   connection.Options,
		UseAzureDefaultCredential: connection.UseAzureDefaultCredential,
		ClientID:                  connection.ClientID,
		ClientSecret:              connection.ClientSecret,
		TenantID:                  connection.TenantID,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.FabricWarehouse[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddDatabricksConnectionFromConfig(connection *config.DatabricksConnection) error {
	m.mutex.Lock()
	if m.Databricks == nil {
		m.Databricks = make(map[string]*databricks.DB)
	}
	m.mutex.Unlock()

	client, err := databricks.NewDB(&databricks.Config{
		Token:        connection.Token,
		Host:         connection.Host,
		Path:         connection.Path,
		Port:         connection.Port,
		Catalog:      connection.Catalog,
		Schema:       connection.Schema,
		ClientID:     connection.ClientID,
		ClientSecret: connection.ClientSecret,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Databricks[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddMongoConnectionFromConfig(connection *config.MongoConnection) error {
	m.mutex.Lock()
	if m.Mongo == nil {
		m.Mongo = make(map[string]*mongo.DB)
	}
	m.mutex.Unlock()

	client, err := mongo.NewDB(&mongo.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Database: connection.Database,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Mongo[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddCouchbaseConnectionFromConfig(connection *config.CouchbaseConnection) error {
	m.mutex.Lock()
	if m.Couchbase == nil {
		m.Couchbase = make(map[string]*couchbase.DB)
	}
	m.mutex.Unlock()

	client, err := couchbase.NewDB(&couchbase.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Bucket:   connection.Bucket,
		SSL:      connection.SSL,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Couchbase[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddCursorConnectionFromConfig(connection *config.CursorConnection) error {
	m.mutex.Lock()
	if m.Cursor == nil {
		m.Cursor = make(map[string]*cursor.Client)
	}
	m.mutex.Unlock()

	client, err := cursor.NewClient(cursor.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Cursor[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddMongoAtlasConnectionFromConfig(connection *config.MongoAtlasConnection) error {
	m.mutex.Lock()
	if m.MongoAtlas == nil {
		m.MongoAtlas = make(map[string]*mongoatlas.DB)
	}
	m.mutex.Unlock()

	client, err := mongoatlas.NewClient(context.Background(), &mongoatlas.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Database: connection.Database,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.MongoAtlas[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddMySQLConnectionFromConfig(connection *config.MySQLConnection) error {
	m.mutex.Lock()
	if m.Mysql == nil {
		m.Mysql = make(map[string]*mysql.Client)
	}
	m.mutex.Unlock()

	client, err := mysql.NewClient(&mysql.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Database: connection.Database,
		Driver:   connection.Driver,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Mysql[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddNotionConnectionFromConfig(connection *config.NotionConnection) error {
	m.mutex.Lock()
	if m.Notion == nil {
		m.Notion = make(map[string]*notion.Client)
	}
	m.mutex.Unlock()

	client, err := notion.NewClient(&notion.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Notion[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddShopifyConnectionFromConfig(connection *config.ShopifyConnection) error {
	m.mutex.Lock()
	if m.Shopify == nil {
		m.Shopify = make(map[string]*shopify.Client)
	}
	m.mutex.Unlock()

	client, err := shopify.NewClient(&shopify.Config{
		APIKey: connection.APIKey,
		URL:    connection.URL,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Shopify[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddGorgiasConnectionFromConfig(connection *config.GorgiasConnection) error {
	m.mutex.Lock()
	if m.Gorgias == nil {
		m.Gorgias = make(map[string]*gorgias.Client)
	}
	m.mutex.Unlock()

	client, err := gorgias.NewClient(&gorgias.Config{
		APIKey: connection.APIKey,
		Domain: connection.Domain,
		Email:  connection.Email,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Gorgias[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddKlaviyoConnectionFromConfig(connection *config.KlaviyoConnection) error {
	m.mutex.Lock()
	if m.Klaviyo == nil {
		m.Klaviyo = make(map[string]*klaviyo.Client)
	}
	m.mutex.Unlock()

	client, err := klaviyo.NewClient(klaviyo.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Klaviyo[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAdjustConnectionFromConfig(connection *config.AdjustConnection) error {
	m.mutex.Lock()
	if m.Adjust == nil {
		m.Adjust = make(map[string]*adjust.Client)
	}
	m.mutex.Unlock()

	client, err := adjust.NewClient(adjust.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Adjust[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAnthropicConnectionFromConfig(connection *config.AnthropicConnection) error {
	m.mutex.Lock()
	if m.Anthropic == nil {
		m.Anthropic = make(map[string]*anthropic.Client)
	}
	m.mutex.Unlock()

	client, err := anthropic.NewClient(anthropic.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Anthropic[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddIntercomConnectionFromConfig(connection *config.IntercomConnection) error {
	m.mutex.Lock()
	if m.Intercom == nil {
		m.Intercom = make(map[string]*intercom.Client)
	}
	m.mutex.Unlock()

	client, err := intercom.NewClient(intercom.Config{
		AccessToken: connection.AccessToken,
		Region:      connection.Region,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Intercom[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddHANAConnectionFromConfig(connection *config.HANAConnection) error {
	m.mutex.Lock()
	if m.HANA == nil {
		m.HANA = make(map[string]*hana.Client)
	}
	m.mutex.Unlock()

	client, err := hana.NewClient(&hana.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Database: connection.Database,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.HANA[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddHostawayConnectionFromConfig(connection *config.HostawayConnection) error {
	m.mutex.Lock()
	if m.Hostaway == nil {
		m.Hostaway = make(map[string]*hostaway.Client)
	}
	m.mutex.Unlock()

	client, err := hostaway.NewClient(hostaway.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Hostaway[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddFacebookAdsConnectionFromConfig(connection *config.FacebookAdsConnection) error {
	m.mutex.Lock()
	if m.FacebookAds == nil {
		m.FacebookAds = make(map[string]*facebookads.Client)
	}
	m.mutex.Unlock()

	client, err := facebookads.NewClient(facebookads.Config{
		AccessToken: connection.AccessToken,
		AccountID:   connection.AccountID,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.FacebookAds[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddStripeConnectionFromConfig(connection *config.StripeConnection) error {
	m.mutex.Lock()
	if m.Stripe == nil {
		m.Stripe = make(map[string]*stripe.Client)
	}
	m.mutex.Unlock()

	client, err := stripe.NewClient(&stripe.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Stripe[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAppsflyerConnectionFromConfig(connection *config.AppsflyerConnection) error {
	m.mutex.Lock()
	if m.Appsflyer == nil {
		m.Appsflyer = make(map[string]*appsflyer.Client)
	}
	m.mutex.Unlock()

	client, err := appsflyer.NewClient(appsflyer.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Appsflyer[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddGoogleSheetsConnectionFromConfig(connection *config.GoogleSheetsConnection) error {
	m.mutex.Lock()
	if m.GoogleSheets == nil {
		m.GoogleSheets = make(map[string]*gsheets.Client)
	}
	m.mutex.Unlock()
	if len(connection.ServiceAccountFile) == 0 && len(connection.ServiceAccountJSON) == 0 {
		return errors.New("credentials are required: provide either service_account_file or service_account_json")
	}

	// Validate ServiceAccountFile if provided.
	if len(connection.ServiceAccountFile) > 0 {
		if err := validateServiceAccountFile(connection.ServiceAccountFile); err != nil {
			return err
		}
	}

	// Validate ServiceAccountJSON if provided.
	if len(connection.ServiceAccountJSON) > 0 {
		if err := validateServiceAccountJSON(connection.ServiceAccountJSON); err != nil {
			return err
		}
	}
	client, err := gsheets.NewClient(gsheets.Config{
		ServiceAccountFile: connection.ServiceAccountFile,
		ServiceAccountJSON: connection.ServiceAccountJSON,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.GoogleSheets[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddSpannerConnectionFromConfig(connection *config.SpannerConnection) error {
	m.mutex.Lock()
	if m.Spanner == nil {
		m.Spanner = make(map[string]*spanner.Client)
	}
	m.mutex.Unlock()

	if len(connection.ServiceAccountJSON) == 0 && len(connection.ServiceAccountFile) == 0 {
		return errors.New("credentials are required: provide either service account file or service account json")
	}

	client, err := spanner.NewClient(spanner.Config{
		ProjectID:          connection.ProjectID,
		InstanceID:         connection.InstanceID,
		Database:           connection.Database,
		ServiceAccountJSON: connection.ServiceAccountJSON,
		ServiceAccountFile: connection.ServiceAccountFile,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Spanner[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddSolidgateConnectionFromConfig(connection *config.SolidgateConnection) error {
	m.mutex.Lock()
	if m.Solidgate == nil {
		m.Solidgate = make(map[string]*solidgate.Client)
	}
	m.mutex.Unlock()

	client, err := solidgate.NewClient(solidgate.Config{
		SecretKey: connection.SecretKey,
		PublicKey: connection.PublicKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Solidgate[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddSmartsheetConnectionFromConfig(connection *config.SmartsheetConnection) error {
	m.mutex.Lock()
	if m.Smartsheet == nil {
		m.Smartsheet = make(map[string]*smartsheet.Client)
	}
	m.mutex.Unlock()

	client, err := smartsheet.NewClient(smartsheet.Config{
		AccessToken: connection.AccessToken,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Smartsheet[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddSftpConnectionFromConfig(connection *config.SFTPConnection) error {
	m.mutex.Lock()
	if m.Sftp == nil {
		m.Sftp = make(map[string]*sftp.Client)
	}
	m.mutex.Unlock()

	client, err := sftp.NewClient(sftp.Config{
		Host:     connection.Host,
		Port:     connection.Port,
		Username: connection.Username,
		Password: connection.Password,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Sftp[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddKafkaConnectionFromConfig(connection *config.KafkaConnection) error {
	m.mutex.Lock()
	if m.Kafka == nil {
		m.Kafka = make(map[string]*kafka.Client)
	}
	m.mutex.Unlock()

	client, err := kafka.NewClient(kafka.Config{
		BootstrapServers: connection.BootstrapServers,
		GroupID:          connection.GroupID,
		BatchSize:        connection.BatchSize,
		SaslMechanisms:   connection.SaslMechanisms,
		SaslUsername:     connection.SaslUsername,
		SaslPassword:     connection.SaslPassword,
		BatchTimeout:     connection.BatchTimeout,
	})
	if err != nil {
		return err
	}

	m.Kafka[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddDuckDBConnectionFromConfig(connection *config.DuckDBConnection) error {
	m.mutex.Lock()
	if m.DuckDB == nil {
		m.DuckDB = make(map[string]*duck.Client)
	}
	m.mutex.Unlock()

	client, err := duck.NewClient(duck.Config{
		Path: connection.Path,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.DuckDB[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddMotherduckConnectionFromConfig(connection *config.MotherduckConnection) error {
	m.mutex.Lock()
	if m.DuckDB == nil {
		m.DuckDB = make(map[string]*duck.Client)
	}
	m.mutex.Unlock()

	client, err := duck.NewClient(duck.MotherDuckConfig{
		Token:    connection.Token,
		Database: connection.Database,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.DuckDB[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddClickHouseConnectionFromConfig(connection *config.ClickHouseConnection) error {
	m.mutex.Lock()
	if m.ClickHouse == nil {
		m.ClickHouse = make(map[string]*clickhouse.Client)
	}
	m.mutex.Unlock()

	client, err := clickhouse.NewClient(&clickhouse.Config{
		Host:     connection.Host,
		Port:     connection.Port,
		Username: connection.Username,
		Password: connection.Password,
		Database: connection.Database,
		HTTPPort: connection.HTTPPort,
		Secure:   connection.Secure,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.ClickHouse[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddChessConnectionFromConfig(connection *config.ChessConnection) error {
	m.mutex.Lock()
	if m.Chess == nil {
		m.Chess = make(map[string]*chess.Client)
	}
	m.mutex.Unlock()
	client, err := chess.NewClient(chess.Config{
		Players: connection.Players,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Chess[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddHubspotConnectionFromConfig(connection *config.HubspotConnection) error {
	m.mutex.Lock()
	if m.Hubspot == nil {
		m.Hubspot = make(map[string]*hubspot.Client)
	}
	m.mutex.Unlock()

	client, err := hubspot.NewClient(hubspot.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Hubspot[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAirtableConnectionFromConfig(connection *config.AirtableConnection) error {
	m.mutex.Lock()
	if m.Airtable == nil {
		m.Airtable = make(map[string]*airtable.Client)
	}
	m.mutex.Unlock()
	client, err := airtable.NewClient(airtable.Config{
		BaseID:      connection.BaseID,
		AccessToken: connection.AccessToken,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Airtable[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddS3ConnectionFromConfig(connection *config.S3Connection) error {
	m.mutex.Lock()
	if m.S3 == nil {
		m.S3 = make(map[string]*s3.Client)
	}
	m.mutex.Unlock()
	client, err := s3.NewClient(s3.Config{
		BucketName:      connection.BucketName,
		PathToFile:      connection.PathToFile,
		AccessKeyID:     connection.AccessKeyID,
		SecretAccessKey: connection.SecretAccessKey,
		EndpointURL:     connection.EndpointURL,
		Layout:          connection.Layout,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.S3[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddSlackConnectionFromConfig(connection *config.SlackConnection) error {
	m.mutex.Lock()
	if m.Slack == nil {
		m.Slack = make(map[string]*slack.Client)
	}
	m.mutex.Unlock()
	client, err := slack.NewClient(slack.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Slack[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddSocrataConnectionFromConfig(connection *config.SocrataConnection) error {
	m.mutex.Lock()
	if m.Socrata == nil {
		m.Socrata = make(map[string]*socrata.Client)
	}
	m.mutex.Unlock()
	client, err := socrata.NewClient(&socrata.Config{
		Domain:   connection.Domain,
		AppToken: connection.AppToken,
		Username: connection.Username,
		Password: connection.Password,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Socrata[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddAsanaConnectionFromConfig(connection *config.AsanaConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Asana == nil {
		m.Asana = make(map[string]*asana.Client)
	}

	client, err := asana.NewClient(asana.Config{
		WorkspaceID: connection.WorkspaceID,
		AccessToken: connection.AccessToken,
	})
	if err != nil {
		return err
	}
	m.Asana[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddDynamoDBConnectionFromConfig(connection *config.DynamoDBConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.DynamoDB == nil {
		m.DynamoDB = make(map[string]*dynamodb.Client)
	}

	client, err := dynamodb.NewClient(dynamodb.Config{
		AccessKeyID:     connection.AccessKeyID,
		SecretAccessKey: connection.SecretAccessKey,
		Region:          connection.Region,
	})
	if err != nil {
		return err
	}
	m.DynamoDB[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddDoceboConnectionFromConfig(connection *config.DoceboConnection) error {
	m.mutex.Lock()
	if m.Docebo == nil {
		m.Docebo = make(map[string]*docebo.Client)
	}
	m.mutex.Unlock()

	client, err := docebo.NewClient(docebo.Config{
		BaseURL:      connection.BaseURL,
		ClientID:     connection.ClientID,
		ClientSecret: connection.ClientSecret,
		Username:     connection.Username,
		Password:     connection.Password,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Docebo[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddGoogleAdsConnectionFromConfig(connection *config.GoogleAdsConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.GoogleAds == nil {
		m.GoogleAds = make(map[string]*googleads.Client)
	}

	client, err := googleads.NewClient(googleads.Config{
		CustomerID:         connection.CustomerID,
		DeveloperToken:     connection.DeveloperToken,
		ServiceAccountFile: connection.ServiceAccountFile,
		ServiceAccountJSON: connection.ServiceAccountJSON,
		LoginCustomerID:    connection.LoginCustomerID,
	})
	if err != nil {
		return err
	}
	m.GoogleAds[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddZendeskConnectionFromConfig(connection *config.ZendeskConnection) error {
	m.mutex.Lock()
	if m.Zendesk == nil {
		m.Zendesk = make(map[string]*zendesk.Client)
	}
	m.mutex.Unlock()
	client, err := zendesk.NewClient(zendesk.Config{
		APIToken:   connection.APIToken,
		Email:      connection.Email,
		OAuthToken: connection.OAuthToken,
		Subdomain:  connection.Subdomain,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Zendesk[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddTikTokAdsConnectionFromConfig(connection *config.TikTokAdsConnection) error {
	m.mutex.Lock()
	if m.TikTokAds == nil {
		m.TikTokAds = make(map[string]*tiktokads.Client)
	}
	m.mutex.Unlock()
	client, err := tiktokads.NewClient(tiktokads.Config{
		AccessToken:   connection.AccessToken,
		AdvertiserIDs: connection.AdvertiserIDs,
		Timezone:      connection.Timezone,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.TikTokAds[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddSnapchatAdsConnectionFromConfig(connection *config.SnapchatAdsConnection) error {
	m.mutex.Lock()
	if m.SnapchatAds == nil {
		m.SnapchatAds = make(map[string]*snapchatads.Client)
	}
	m.mutex.Unlock()
	client, err := snapchatads.NewClient(snapchatads.Config{
		RefreshToken:   connection.RefreshToken,
		ClientID:       connection.ClientID,
		ClientSecret:   connection.ClientSecret,
		OrganizationID: connection.OrganizationID,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.SnapchatAds[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddGitHubConnectionFromConfig(connection *config.GitHubConnection) error {
	m.mutex.Lock()
	if m.GitHub == nil {
		m.GitHub = make(map[string]*github.Client)
	}
	m.mutex.Unlock()

	client, err := github.NewClient(github.Config{
		AccessToken: connection.AccessToken,
		Owner:       connection.Owner,
		Repo:        connection.Repo,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.GitHub[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAppStoreConnectionFromConfig(connection *config.AppStoreConnection) error {
	m.mutex.Lock()
	if m.AppStore == nil {
		m.AppStore = make(map[string]*appstore.Client)
	}
	m.mutex.Unlock()

	client, err := appstore.NewClient(appstore.Config{
		IssuerID: connection.IssuerID,
		KeyID:    connection.KeyID,
		KeyPath:  connection.KeyPath,
		Key:      connection.Key,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.AppStore[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddLinkedInAdsConnectionFromConfig(connection *config.LinkedInAdsConnection) error {
	m.mutex.Lock()
	if m.LinkedInAds == nil {
		m.LinkedInAds = make(map[string]*linkedinads.Client)
	}
	m.mutex.Unlock()
	client, err := linkedinads.NewClient(linkedinads.Config{
		AccessToken: connection.AccessToken,
		AccountIds:  connection.AccountIds,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.LinkedInAds[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddMailchimpConnectionFromConfig(connection *config.MailchimpConnection) error {
	m.mutex.Lock()
	if m.Mailchimp == nil {
		m.Mailchimp = make(map[string]*mailchimp.Client)
	}
	m.mutex.Unlock()
	client, err := mailchimp.NewClient(mailchimp.Config{
		APIKey: connection.APIKey,
		Server: connection.Server,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Mailchimp[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddRevenueCatConnectionFromConfig(connection *config.RevenueCatConnection) error {
	m.mutex.Lock()
	if m.RevenueCat == nil {
		m.RevenueCat = make(map[string]*revenuecat.Client)
	}
	m.mutex.Unlock()
	client, err := revenuecat.NewClient(revenuecat.Config{
		APIKey:    connection.APIKey,
		ProjectID: connection.ProjectID,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.RevenueCat[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddLinearConnectionFromConfig(connection *config.LinearConnection) error {
	m.mutex.Lock()
	if m.Linear == nil {
		m.Linear = make(map[string]*linear.Client)
	}
	m.mutex.Unlock()

	client, err := linear.NewClient(linear.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Linear[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddGCSConnectionFromConfig(connection *config.GCSConnection) error {
	m.mutex.Lock()
	if m.GCS == nil {
		m.GCS = make(map[string]*gcs.Client)
	}
	m.mutex.Unlock()

	client, err := gcs.NewClient(gcs.Config{
		ServiceAccountFile: connection.ServiceAccountFile,
		ServiceAccountJSON: connection.ServiceAccountJSON,
		BucketName:         connection.BucketName,
		PathToFile:         connection.PathToFile,
		Layout:             connection.Layout,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.GCS[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddPersonioConnectionFromConfig(connection *config.PersonioConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Personio == nil {
		m.Personio = make(map[string]*personio.Client)
	}

	m.Personio[connection.Name] = personio.NewClient(personio.Config{
		ClientID:     connection.ClientID,
		ClientSecret: connection.ClientSecret,
	})
	m.availableConnections[connection.Name] = m.Personio[connection.Name]
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddApplovinMaxConnectionFromConfig(connection *config.ApplovinMaxConnection) error {
	m.mutex.Lock()
	if m.ApplovinMax == nil {
		m.ApplovinMax = make(map[string]*applovinmax.Client)
	}
	m.mutex.Unlock()

	client, err := applovinmax.NewClient(applovinmax.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.ApplovinMax[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddAppLovinConnectionFromConfig(connection *config.AppLovinConnection) error {
	m.mutex.Lock()
	if m.AppLovin == nil {
		m.AppLovin = make(map[string]*applovin.Client)
	}
	m.mutex.Unlock()

	client, err := applovin.NewClient(applovin.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.AppLovin[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddPipedriveConnectionFromConfig(connection *config.PipedriveConnection) error {
	m.mutex.Lock()
	if m.Pipedrive == nil {
		m.Pipedrive = make(map[string]*pipedrive.Client)
	}
	m.mutex.Unlock()

	client, err := pipedrive.NewClient(pipedrive.Config{
		APIToken: connection.APIToken,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Pipedrive[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddClickupConnectionFromConfig(connection *config.ClickupConnection) error {
	m.mutex.Lock()
	if m.Clickup == nil {
		m.Clickup = make(map[string]*clickup.Client)
	}
	m.mutex.Unlock()

	client, err := clickup.NewClient(clickup.Config{
		APIToken: connection.APIToken,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Clickup[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddQuickBooksConnectionFromConfig(connection *config.QuickBooksConnection) error {
	m.mutex.Lock()
	if m.QuickBooks == nil {
		m.QuickBooks = make(map[string]*quickbooks.Client)
	}
	m.mutex.Unlock()

	client, err := quickbooks.NewClient(quickbooks.Config{
		CompanyID:    connection.CompanyID,
		ClientID:     connection.ClientID,
		ClientSecret: connection.ClientSecret,
		RefreshToken: connection.RefreshToken,
		Environment:  connection.Environment,
		MinorVersion: connection.MinorVersion,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.QuickBooks[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddPinterestConnectionFromConfig(connection *config.PinterestConnection) error {
	m.mutex.Lock()
	if m.Pinterest == nil {
		m.Pinterest = make(map[string]*pinterest.Client)
	}
	m.mutex.Unlock()

	client, err := pinterest.NewClient(pinterest.Config{
		AccessToken: connection.AccessToken,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Pinterest[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddTrustpilotConnectionFromConfig(connection *config.TrustpilotConnection) error {
	m.mutex.Lock()
	if m.Trustpilot == nil {
		m.Trustpilot = make(map[string]*trustpilot.Client)
	}
	m.mutex.Unlock()

	client, err := trustpilot.NewClient(trustpilot.Config{
		BusinessUnitID: connection.BusinessUnitID,
		APIKey:         connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Trustpilot[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddISOCPulseConnectionFromConfig(connection *config.ISOCPulseConnection) error {
	m.mutex.Lock()
	if m.ISOCPulse == nil {
		m.ISOCPulse = make(map[string]*isocpulse.Client)
	}
	m.mutex.Unlock()

	client, err := isocpulse.NewClient(isocpulse.Config{
		Token: connection.Token,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.ISOCPulse[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddInfluxDBConnectionFromConfig(connection *config.InfluxDBConnection) error {
	m.mutex.Lock()
	if m.InfluxDB == nil {
		m.InfluxDB = make(map[string]*influxdb.Client)
	}
	m.mutex.Unlock()

	client, err := influxdb.NewClient(influxdb.Config{
		Host:   connection.Host,
		Port:   connection.Port,
		Token:  connection.Token,
		Org:    connection.Org,
		Bucket: connection.Bucket,
		Secure: connection.Secure,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.InfluxDB[connection.Name] = client
	m.availableConnections[connection.Name] = client
	return nil
}

func (m *Manager) AddWiseConnectionFromConfig(connection *config.WiseConnection) error {
	m.mutex.Lock()
	if m.Wise == nil {
		m.Wise = make(map[string]*wise.Client)
	}
	m.mutex.Unlock()

	client, err := wise.NewClient(wise.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Wise[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddZoomConnectionFromConfig(connection *config.ZoomConnection) error {
	m.mutex.Lock()
	if m.Zoom == nil {
		m.Zoom = make(map[string]*zoom.Client)
	}
	m.mutex.Unlock()

	client, err := zoom.NewClient(zoom.Config{
		ClientID:     connection.ClientID,
		ClientSecret: connection.ClientSecret,
		AccountID:    connection.AccountID,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Zoom[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddMixpanelConnectionFromConfig(connection *config.MixpanelConnection) error {
	m.mutex.Lock()
	if m.Mixpanel == nil {
		m.Mixpanel = make(map[string]*mixpanel.Client)
	}
	m.mutex.Unlock()

	client, err := mixpanel.NewClient(mixpanel.Config{
		Username:  connection.Username,
		Password:  connection.Password,
		ProjectID: connection.ProjectID,
		Server:    connection.Server,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Mixpanel[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddGoogleAnalyticsConnectionFromConfig(connection *config.GoogleAnalyticsConnection) error {
	m.mutex.Lock()
	if m.GoogleAnalytics == nil {
		m.GoogleAnalytics = make(map[string]*googleanalytics.Client)
	}
	m.mutex.Unlock()

	client, err := googleanalytics.NewClient(googleanalytics.Config{
		ServiceAccountFile: connection.ServiceAccountFile,
		PropertyID:         connection.PropertyID,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.GoogleAnalytics[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddSalesforceConnectionFromConfig(connection *config.SalesforceConnection) error {
	m.mutex.Lock()
	if m.Salesforce == nil {
		m.Salesforce = make(map[string]*salesforce.Client)
	}
	m.mutex.Unlock()

	client, err := salesforce.NewClient(salesforce.Config{
		Username: connection.Username,
		Password: connection.Password,
		Token:    connection.Token,
		Domain:   connection.Domain,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Salesforce[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddSQLiteConnectionFromConfig(connection *config.SQLiteConnection) error {
	m.mutex.Lock()
	if m.SQLite == nil {
		m.SQLite = make(map[string]*sqlite.Client)
	}
	m.mutex.Unlock()

	client, err := sqlite.NewClient(sqlite.Config{
		Path: connection.Path,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.SQLite[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddOracleConnectionFromConfig(connection *config.OracleConnection) error {
	m.mutex.Lock()
	if m.Oracle == nil {
		m.Oracle = make(map[string]*oracle.Client)
	}
	m.mutex.Unlock()

	client, err := oracle.NewClient(oracle.Config{
		Username:     connection.Username,
		Password:     connection.Password,
		Host:         connection.Host,
		Port:         connection.Port,
		ServiceName:  connection.ServiceName,
		SID:          connection.SID,
		Role:         connection.Role,
		SSL:          connection.SSL,
		SSLVerify:    connection.SSLVerify,
		PrefetchRows: connection.PrefetchRows,
		TraceFile:    connection.TraceFile,
		Wallet:       connection.Wallet,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Oracle[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddKinesisConnectionFromConfig(connection *config.KinesisConnection) error {
	m.mutex.Lock()
	if m.Kinesis == nil {
		m.Kinesis = make(map[string]*kinesis.Client)
	}
	m.mutex.Unlock()

	client, err := kinesis.NewClient(kinesis.Config{
		AccessKeyID:     connection.AccessKeyID,
		SecretAccessKey: connection.SecretAccessKey,
		Region:          connection.Region,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Kinesis[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddDB2ConnectionFromConfig(connection *config.DB2Connection) error {
	m.mutex.Lock()
	if m.DB2 == nil {
		m.DB2 = make(map[string]*db2.Client)
	}
	m.mutex.Unlock()

	client, err := db2.NewClient(db2.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Database: connection.Database,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.DB2[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddPhantombusterConnectionFromConfig(connection *config.PhantombusterConnection) error {
	m.mutex.Lock()
	if m.Phantombuster == nil {
		m.Phantombuster = make(map[string]*phantombuster.Client)
	}
	m.mutex.Unlock()

	client, err := phantombuster.NewClient(phantombuster.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Phantombuster[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddAttioConnectionFromConfig(connection *config.AttioConnection) error {
	m.mutex.Lock()
	if m.Attio == nil {
		m.Attio = make(map[string]*attio.Client)
	}
	m.mutex.Unlock()

	client, err := attio.NewClient(attio.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Attio[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddElasticsearchConnectionFromConfig(connection *config.ElasticsearchConnection) error {
	m.mutex.Lock()
	if m.Elasticsearch == nil {
		m.Elasticsearch = make(map[string]*elasticsearch.Client)
	}
	m.mutex.Unlock()

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Username:    connection.Username,
		Password:    connection.Password,
		Host:        connection.Host,
		Port:        connection.Port,
		Secure:      connection.Secure,
		VerifyCerts: connection.VerifyCerts,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Elasticsearch[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddFrankfurterConnectionFromConfig(connection *config.FrankfurterConnection) error {
	m.mutex.Lock()
	if m.Frankfurter == nil {
		m.Frankfurter = make(map[string]*frankfurter.Client)
	}
	m.mutex.Unlock()

	client, err := frankfurter.NewClient(frankfurter.Config{})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Frankfurter[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddFluxxConnectionFromConfig(connection *config.FluxxConnection) error {
	m.mutex.Lock()
	if m.Fluxx == nil {
		m.Fluxx = make(map[string]*fluxx.Client)
	}
	m.mutex.Unlock()

	client, err := fluxx.NewClient(&fluxx.Config{
		Instance:     connection.Instance,
		ClientID:     connection.ClientID,
		ClientSecret: connection.ClientSecret,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Fluxx[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddFreshdeskConnectionFromConfig(connection *config.FreshdeskConnection) error {
	m.mutex.Lock()
	if m.Freshdesk == nil {
		m.Freshdesk = make(map[string]*freshdesk.Client)
	}
	m.mutex.Unlock()

	client, err := freshdesk.NewClient(freshdesk.Config{
		Domain: connection.Domain,
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Freshdesk[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddFundraiseUpConnectionFromConfig(connection *config.FundraiseUpConnection) error {
	m.mutex.Lock()
	if m.FundraiseUp == nil {
		m.FundraiseUp = make(map[string]*fundraiseup.Client)
	}
	m.mutex.Unlock()

	client, err := fundraiseup.NewClient(fundraiseup.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.FundraiseUp[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddFirefliesConnectionFromConfig(connection *config.FirefliesConnection) error {
	m.mutex.Lock()
	if m.Fireflies == nil {
		m.Fireflies = make(map[string]*fireflies.Client)
	}
	m.mutex.Unlock()

	client, err := fireflies.NewClient(fireflies.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Fireflies[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddJiraConnectionFromConfig(connection *config.JiraConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Jira == nil {
		m.Jira = make(map[string]*jira.Client)
	}

	client, err := jira.NewClient(jira.Config{
		Domain:   connection.Domain,
		Email:    connection.Email,
		APIToken: connection.APIToken,
	})
	if err != nil {
		return err
	}
	m.Jira[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddMondayConnectionFromConfig(connection *config.MondayConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Monday == nil {
		m.Monday = make(map[string]*monday.Client)
	}

	client, err := monday.NewClient(monday.Config{
		APIToken: connection.APIToken,
	})
	if err != nil {
		return err
	}
	m.Monday[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddPlusVibeAIConnectionFromConfig(connection *config.PlusVibeAIConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.PlusVibeAI == nil {
		m.PlusVibeAI = make(map[string]*plusvibeai.Client)
	}

	client, err := plusvibeai.NewClient(plusvibeai.Config{
		APIKey:      connection.APIKey,
		WorkspaceID: connection.WorkspaceID,
	})
	if err != nil {
		return err
	}
	m.PlusVibeAI[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddBruinCloudConnectionFromConfig(connection *config.BruinCloudConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.BruinCloud == nil {
		m.BruinCloud = make(map[string]*bruincloud.Client)
	}

	client, err := bruincloud.NewClient(bruincloud.Config{
		APIToken: connection.APIToken,
	})
	if err != nil {
		return err
	}
	m.BruinCloud[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddPrimerConnectionFromConfig(connection *config.PrimerConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Primer == nil {
		m.Primer = make(map[string]*primer.Client)
	}

	client, err := primer.NewClient(primer.Config{
		APIKey: connection.APIKey,
	})
	if err != nil {
		return err
	}
	m.Primer[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddIndeedConnectionFromConfig(connection *config.IndeedConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Indeed == nil {
		m.Indeed = make(map[string]*indeed.Client)
	}

	client, err := indeed.NewClient(indeed.Config{
		ClientID:     connection.ClientID,
		ClientSecret: connection.ClientSecret,
		EmployerID:   connection.EmployerID,
	})
	if err != nil {
		return err
	}
	m.Indeed[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddEMRServerlessConnectionFromConfig(connection *config.EMRServerlessConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.EMRSeverless == nil {
		m.EMRSeverless = make(map[string]*emr_serverless.Client)
	}
	client, err := emr_serverless.NewClient(emr_serverless.Config{
		AccessKey:     connection.AccessKey,
		SecretKey:     connection.SecretKey,
		ApplicationID: connection.ApplicationID,
		ExecutionRole: connection.ExecutionRole,
		Region:        connection.Region,
		Workspace:     connection.Workspace,
	})
	if err != nil {
		return err
	}
	m.EMRSeverless[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddDataprocServerlessConnectionFromConfig(connection *config.DataprocServerlessConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.DataprocServerless == nil {
		m.DataprocServerless = make(map[string]*dataprocserverless.Client)
	}
	client, err := dataprocserverless.NewClient(dataprocserverless.Config{
		ServiceAccountJSON:               connection.ServiceAccountJSON,
		ServiceAccountFile:               connection.ServiceAccountFile,
		UseApplicationDefaultCredentials: connection.UseApplicationDefaultCredentials,
		ProjectID:                        connection.ProjectID,
		Workspace:                        connection.Workspace,
		Region:                           connection.Region,
		ExecutionRole:                    connection.ExecutionRole,
		SubnetworkURI:                    connection.SubnetworkURI,
		NetworkTags:                      connection.NetworkTags,
		KmsKey:                           connection.KmsKey,
		StagingBucket:                    connection.StagingBucket,
		MetastoreService:                 connection.MetastoreService,
	})
	if err != nil {
		return err
	}
	m.DataprocServerless[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddGenericConnectionFromConfig(connection *config.GenericConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Generic == nil {
		m.Generic = make(map[string]*config.GenericConnection)
	}

	m.Generic[connection.Name] = connection
	m.availableConnections[connection.Name] = connection
	m.AllConnectionDetails[connection.Name] = connection
	return nil
}

func (m *Manager) AddTableauConnectionFromConfig(connection *config.TableauConnection) error {
	m.mutex.Lock()
	if m.Tableau == nil {
		m.Tableau = make(map[string]*tableau.Client)
	}
	m.mutex.Unlock()

	client, err := tableau.NewClient(tableau.Config{
		Name:                      connection.Name,
		Host:                      connection.Host,
		Username:                  connection.Username,
		Password:                  connection.Password,
		PersonalAccessTokenName:   connection.PersonalAccessTokenName,
		PersonalAccessTokenSecret: connection.PersonalAccessTokenSecret,
		SiteID:                    connection.SiteID,
		APIVersion:                connection.APIVersion,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Tableau[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

func (m *Manager) AddTrinoConnectionFromConfig(connection *config.TrinoConnection) error {
	m.mutex.Lock()
	if m.Trino == nil {
		m.Trino = make(map[string]*trino.Client)
	}
	m.mutex.Unlock()

	config := trino.Config{
		Username: connection.Username,
		Password: connection.Password,
		Host:     connection.Host,
		Port:     connection.Port,
		Catalog:  connection.Catalog,
		Schema:   connection.Schema,
	}

	client, err := trino.NewClient(config)
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Trino[connection.Name] = client
	m.availableConnections[connection.Name] = client
	m.AllConnectionDetails[connection.Name] = connection

	return nil
}

var envVarRegex = regexp.MustCompile(`\${([^}]+)}`)

func processConnections[T config.Named](connections []T, adder func(*T) error, wg *conc.WaitGroup, errList *[]error, mu *sync.Mutex) {
	if connections == nil {
		return
	}
	for i := range connections {
		conn := &connections[i]
		wg.Go(func() {
			// Check for environment variable placeholders in connection fields
			v := reflect.ValueOf(conn).Elem()
			for i := range v.NumField() {
				field := v.Field(i)

				// Only process string fields
				if field.Kind() != reflect.String {
					continue
				}
				if !field.CanSet() {
					continue
				}
				strValue := strings.TrimSpace(field.String())
				matches := envVarRegex.FindStringSubmatch(strValue)
				for len(matches) > 0 {
					envVarName := matches[1]
					envValue := os.Getenv(envVarName)
					strValue = strings.Replace(strValue, matches[0], envValue, 1)
					field.SetString(strValue)
					// Look for more matches after replacement
					matches = envVarRegex.FindStringSubmatch(strValue)
				}
			}

			err := adder(conn)
			if err != nil {
				mu.Lock()
				*errList = append(*errList, errors.Wrapf(err, "failed to add connection %q", (*conn).GetName()))
				mu.Unlock()
			}
		})
	}
}

// NewManagerFromConfig creates a connection Manager using a background context.
// Prefer NewManagerFromConfigWithContext to propagate cancellation and deadlines.
func NewManagerFromConfig(cm *config.Config) (config.ConnectionAndDetailsGetter, []error) {
	return NewManagerFromConfigWithContext(context.Background(), cm)
}

// NewManagerFromConfigWithContext creates a connection Manager using the provided context
// for any connection initializations that support context (e.g., Postgres/Redshift).
func NewManagerFromConfigWithContext(ctx context.Context, cm *config.Config) (config.ConnectionAndDetailsGetter, []error) {
	connectionManager := &Manager{}
	connectionManager.availableConnections = make(map[string]any)
	connectionManager.AllConnectionDetails = make(map[string]any)

	var wg conc.WaitGroup
	var errList []error
	var mu sync.Mutex

	processConnections(cm.SelectedEnvironment.Connections.AthenaConnection, connectionManager.AddAthenaConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.AwsConnection, connectionManager.AddAwsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.GoogleCloudPlatform, connectionManager.AddBqConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Snowflake, connectionManager.AddSfConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Postgres, func(conn *config.PostgresConnection) error {
		return connectionManager.AddPgConnectionFromConfig(ctx, conn)
	}, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.RedShift, func(conn *config.RedshiftConnection) error {
		return connectionManager.AddRedshiftConnectionFromConfig(ctx, conn)
	}, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.MsSQL, connectionManager.AddMsSQLConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Synapse, connectionManager.AddSynapseSQLConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.FabricWarehouse, connectionManager.AddFabricWarehouseConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Databricks, connectionManager.AddDatabricksConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Mongo, connectionManager.AddMongoConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Couchbase, connectionManager.AddCouchbaseConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Cursor, connectionManager.AddCursorConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.MongoAtlas, connectionManager.AddMongoAtlasConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.MySQL, connectionManager.AddMySQLConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Notion, connectionManager.AddNotionConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Allium, connectionManager.AddAlliumConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.HANA, connectionManager.AddHANAConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Hostaway, connectionManager.AddHostawayConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Shopify, connectionManager.AddShopifyConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Gorgias, connectionManager.AddGorgiasConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Klaviyo, connectionManager.AddKlaviyoConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Adjust, connectionManager.AddAdjustConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Anthropic, connectionManager.AddAnthropicConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Intercom, connectionManager.AddIntercomConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.FacebookAds, connectionManager.AddFacebookAdsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Stripe, connectionManager.AddStripeConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Appsflyer, connectionManager.AddAppsflyerConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Kafka, connectionManager.AddKafkaConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.GoogleSheets, connectionManager.AddGoogleSheetsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.DuckDB, connectionManager.AddDuckDBConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.MotherDuck, connectionManager.AddMotherduckConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.ClickHouse, connectionManager.AddClickHouseConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Hubspot, connectionManager.AddHubspotConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Chess, connectionManager.AddChessConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Airtable, connectionManager.AddAirtableConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.S3, connectionManager.AddS3ConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Slack, connectionManager.AddSlackConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Socrata, connectionManager.AddSocrataConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Asana, connectionManager.AddAsanaConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.DynamoDB, connectionManager.AddDynamoDBConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Docebo, connectionManager.AddDoceboConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Zendesk, connectionManager.AddZendeskConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.GoogleAds, connectionManager.AddGoogleAdsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.TikTokAds, connectionManager.AddTikTokAdsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.SnapchatAds, connectionManager.AddSnapchatAdsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.GitHub, connectionManager.AddGitHubConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.AppStore, connectionManager.AddAppStoreConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.LinkedInAds, connectionManager.AddLinkedInAdsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Mailchimp, connectionManager.AddMailchimpConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.RevenueCat, connectionManager.AddRevenueCatConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Linear, connectionManager.AddLinearConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.GCS, connectionManager.AddGCSConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Personio, connectionManager.AddPersonioConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.ApplovinMax, connectionManager.AddApplovinMaxConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Kinesis, connectionManager.AddKinesisConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Pipedrive, connectionManager.AddPipedriveConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Clickup, connectionManager.AddClickupConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Mixpanel, connectionManager.AddMixpanelConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Pinterest, connectionManager.AddPinterestConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Trustpilot, connectionManager.AddTrustpilotConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.QuickBooks, connectionManager.AddQuickBooksConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Wise, connectionManager.AddWiseConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Zoom, connectionManager.AddZoomConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.EMRServerless, connectionManager.AddEMRServerlessConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.DataprocServerless, connectionManager.AddDataprocServerlessConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.GoogleAnalytics, connectionManager.AddGoogleAnalyticsConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.AppLovin, connectionManager.AddAppLovinConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Frankfurter, connectionManager.AddFrankfurterConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Fluxx, connectionManager.AddFluxxConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Freshdesk, connectionManager.AddFreshdeskConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.FundraiseUp, connectionManager.AddFundraiseUpConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Fireflies, connectionManager.AddFirefliesConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Jira, connectionManager.AddJiraConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Monday, connectionManager.AddMondayConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.PlusVibeAI, connectionManager.AddPlusVibeAIConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.BruinCloud, connectionManager.AddBruinCloudConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Primer, connectionManager.AddPrimerConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Indeed, connectionManager.AddIndeedConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Salesforce, connectionManager.AddSalesforceConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.SQLite, connectionManager.AddSQLiteConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Oracle, connectionManager.AddOracleConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.DB2, connectionManager.AddDB2ConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Phantombuster, connectionManager.AddPhantombusterConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Elasticsearch, connectionManager.AddElasticsearchConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Spanner, connectionManager.AddSpannerConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Solidgate, connectionManager.AddSolidgateConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Smartsheet, connectionManager.AddSmartsheetConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Attio, connectionManager.AddAttioConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Sftp, connectionManager.AddSftpConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.ISOCPulse, connectionManager.AddISOCPulseConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.InfluxDB, connectionManager.AddInfluxDBConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Tableau, connectionManager.AddTableauConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Generic, connectionManager.AddGenericConnectionFromConfig, &wg, &errList, &mu)
	processConnections(cm.SelectedEnvironment.Connections.Trino, connectionManager.AddTrinoConnectionFromConfig, &wg, &errList, &mu)
	wg.Wait()
	return connectionManager, errList
}
