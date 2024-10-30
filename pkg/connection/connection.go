package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bruin-data/bruin/pkg/zendesk"
	"io/ioutil"
	"os"

	"sync"

	"github.com/bruin-data/bruin/pkg/adjust"
	"github.com/bruin-data/bruin/pkg/airtable"
	"github.com/bruin-data/bruin/pkg/appsflyer"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/databricks"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/facebookads"
	"github.com/bruin-data/bruin/pkg/gorgias"
	"github.com/bruin-data/bruin/pkg/gsheets"
	"github.com/bruin-data/bruin/pkg/hana"
	"github.com/bruin-data/bruin/pkg/hubspot"
	"github.com/bruin-data/bruin/pkg/kafka"
	"github.com/bruin-data/bruin/pkg/klaviyo"
	"github.com/bruin-data/bruin/pkg/mongo"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/notion"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/shopify"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/bruin-data/bruin/pkg/stripe"

	"github.com/pkg/errors"
	"github.com/sourcegraph/conc"
	"golang.org/x/exp/maps"
)

type Manager struct {
	BigQuery    map[string]*bigquery.Client
	Snowflake   map[string]*snowflake.DB
	Postgres    map[string]*postgres.Client
	MsSQL       map[string]*mssql.DB
	Databricks  map[string]*databricks.DB
	Mongo       map[string]*mongo.DB
	Mysql       map[string]*mysql.Client
	Notion      map[string]*notion.Client
	HANA        map[string]*hana.Client
	Shopify     map[string]*shopify.Client
	Gorgias     map[string]*gorgias.Client
	Klaviyo     map[string]*klaviyo.Client
	Adjust      map[string]*adjust.Client
	Athena      map[string]*athena.DB
	FacebookAds map[string]*facebookads.Client
	Stripe      map[string]*stripe.Client
	Appsflyer   map[string]*appsflyer.Client
	Kafka       map[string]*kafka.Client
	Airtable    map[string]*airtable.Client

	DuckDB       map[string]*duck.Client
	Hubspot      map[string]*hubspot.Client
	GoogleSheets map[string]*gsheets.Client
	Zendesk      map[string]*zendesk.Client
	mutex        sync.Mutex
}

func (m *Manager) GetConnection(name string) (interface{}, error) {
	availableConnectionNames := make([]string, 0)

	connBigQuery, err := m.GetBqConnectionWithoutDefault(name)
	if err == nil {
		return connBigQuery, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.BigQuery)...)

	connSnowflake, err := m.GetSfConnectionWithoutDefault(name)
	if err == nil {
		return connSnowflake, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Snowflake)...)

	connPostgres, err := m.GetPgConnectionWithoutDefault(name)
	if err == nil {
		return connPostgres, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Postgres)...)

	connMSSql, err := m.GetMsConnectionWithoutDefault(name)
	if err == nil {
		return connMSSql, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.MsSQL)...)

	connDatabricks, err := m.GetDatabricksConnectionWithoutDefault(name)
	if err == nil {
		return connDatabricks, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Databricks)...)

	connMongo, err := m.GetMongoConnectionWithoutDefault(name)
	if err == nil {
		return connMongo, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Mongo)...)

	connMysql, err := m.GetMySQLConnectionWithoutDefault(name)
	if err == nil {
		return connMysql, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Mysql)...)

	connNotion, err := m.GetNotionConnectionWithoutDefault(name)
	if err == nil {
		return connNotion, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Notion)...)

	connHANA, err := m.GetHANAConnectionWithoutDefault(name)
	if err == nil {
		return connHANA, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.HANA)...)

	connShopify, err := m.GetShopifyConnectionWithoutDefault(name)
	if err == nil {
		return connShopify, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Shopify)...)

	connGorgias, err := m.GetGorgiasConnectionWithoutDefault(name)
	if err == nil {
		return connGorgias, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Gorgias)...)

	connKlaviyo, err := m.GetKlaviyoConnectionWithoutDefault(name)
	if err == nil {
		return connKlaviyo, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Klaviyo)...)

	connAdjust, err := m.GetAdjustConnectionWithoutDefault(name)
	if err == nil {
		return connAdjust, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Adjust)...)

	athenaConnection, err := m.GetAthenaConnectionWithoutDefault(name)
	if err == nil {
		return athenaConnection, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Athena)...)

	connFacebookAds, err := m.GetFacebookAdsConnectionWithoutDefault(name)
	if err == nil {
		return connFacebookAds, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.FacebookAds)...)

	connStripe, err := m.GetStripeConnectionWithoutDefault(name)
	if err == nil {
		return connStripe, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Stripe)...)

	connAppsflyer, err := m.GetAppsflyerConnectionWithoutDefault(name)
	if err == nil {
		return connAppsflyer, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Appsflyer)...)

	connKafka, err := m.GetKafkaConnectionWithoutDefault(name)
	if err == nil {
		return connKafka, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Kafka)...)

	connDuckDB, err := m.GetDuckDBConnectionWithoutDefault(name)
	if err == nil {
		return connDuckDB, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.DuckDB)...)

	connHubspot, err := m.GetHubspotConnectionWithoutDefault(name)
	if err == nil {
		return connHubspot, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Hubspot)...)

	connGoogleSheets, err := m.GetGoogleSheetsConnectionWithoutDefault(name)
	if err == nil {
		return connGoogleSheets, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.GoogleSheets)...)

	connAirtable, err := m.GetAirtableConnectionWithoutDefault(name)
	if err == nil {
		return connAirtable, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Airtable)...)

	connZendesk, err := m.GetZendeskConnectionWithoutDefault(name)
	if err == nil {
		fmt.Println("err", err)
		return connZendesk, nil
	}
	availableConnectionNames = append(availableConnectionNames, maps.Keys(m.Zendesk)...)
	return nil, errors.Errorf("connection '%s' not found, available connection names are: %v", name, availableConnectionNames)
}

func (m *Manager) GetAthenaConnection(name string) (athena.Client, error) {
	db, err := m.GetAthenaConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetAthenaConnectionWithoutDefault("athena-default")
}

func (m *Manager) GetAthenaConnectionWithoutDefault(name string) (athena.Client, error) {
	if m.Athena == nil {
		return nil, errors.New("no Athena connections found")
	}

	db, ok := m.Athena[name]
	if !ok {
		return nil, errors.Errorf("Athena connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetDuckDBConnection(name string) (duck.DuckDBClient, error) {
	db, err := m.GetDuckDBConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetDuckDBConnectionWithoutDefault("duckdb-default")
}

func (m *Manager) GetDuckDBConnectionWithoutDefault(name string) (duck.DuckDBClient, error) {
	if m.DuckDB == nil {
		return nil, errors.New("no DuckDB connections found")
	}

	db, ok := m.DuckDB[name]
	if !ok {
		return nil, errors.Errorf("DuckDB connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetBqConnection(name string) (bigquery.DB, error) {
	return m.GetBqConnectionWithoutDefault(name)
}

func (m *Manager) GetBqConnectionWithoutDefault(name string) (bigquery.DB, error) {
	if m.BigQuery == nil {
		return nil, errors.New("no bigquery connections found")
	}

	db, ok := m.BigQuery[name]
	if !ok {
		return nil, errors.Errorf("bigquery connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetSfConnection(name string) (snowflake.SfClient, error) {
	db, err := m.GetSfConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetSfConnectionWithoutDefault("snowflake-default")
}

func (m *Manager) GetSfConnectionWithoutDefault(name string) (snowflake.SfClient, error) {
	if m.Snowflake == nil {
		return nil, errors.New("no snowflake connections found")
	}

	db, ok := m.Snowflake[name]
	if !ok {
		return nil, errors.Errorf("snowflake connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetPgConnection(name string) (postgres.PgClient, error) {
	db, err := m.GetPgConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetPgConnectionWithoutDefault("postgres-default")
}

func (m *Manager) GetPgConnectionWithoutDefault(name string) (postgres.PgClient, error) {
	if m.Postgres == nil {
		return nil, errors.New("no postgres/redshift connections found")
	}

	db, ok := m.Postgres[name]
	if !ok {
		return nil, errors.Errorf("postgres/redshift connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetMsConnection(name string) (mssql.MsClient, error) {
	db, err := m.GetMsConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetMsConnectionWithoutDefault("mssql-default")
}

func (m *Manager) GetMsConnectionWithoutDefault(name string) (mssql.MsClient, error) {
	if m.MsSQL == nil {
		return nil, errors.New("no mssql connections found")
	}

	db, ok := m.MsSQL[name]
	if !ok {
		return nil, errors.Errorf("mssql connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetDatabricksConnection(name string) (databricks.Client, error) {
	db, err := m.GetDatabricksConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetDatabricksConnectionWithoutDefault("databricks-default")
}

func (m *Manager) GetDatabricksConnectionWithoutDefault(name string) (databricks.Client, error) {
	if m.Databricks == nil {
		return nil, errors.New("no databricks connections found")
	}

	db, ok := m.Databricks[name]
	if !ok {
		return nil, errors.Errorf("databricks connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetMongoConnection(name string) (*mongo.DB, error) {
	db, err := m.GetMongoConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetMongoConnectionWithoutDefault("mongo-default")
}

func (m *Manager) GetMongoConnectionWithoutDefault(name string) (*mongo.DB, error) {
	if m.Mongo == nil {
		return nil, errors.New("no mongo connections found")
	}

	db, ok := m.Mongo[name]
	if !ok {
		return nil, errors.Errorf("mongo connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetMySQLConnection(name string) (*mysql.Client, error) {
	db, err := m.GetMySQLConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetMySQLConnectionWithoutDefault("mysql-default")
}

func (m *Manager) GetMySQLConnectionWithoutDefault(name string) (*mysql.Client, error) {
	if m.Mysql == nil {
		return nil, errors.New("no mysql connections found")
	}

	db, ok := m.Mysql[name]
	if !ok {
		return nil, errors.Errorf("mysql connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetNotionConnection(name string) (*notion.Client, error) {
	db, err := m.GetNotionConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetNotionConnectionWithoutDefault("notion-default")
}

func (m *Manager) GetNotionConnectionWithoutDefault(name string) (*notion.Client, error) {
	if m.Notion == nil {
		return nil, errors.New("no notion connections found")
	}

	db, ok := m.Notion[name]
	if !ok {
		return nil, errors.Errorf("notion connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetHANAConnection(name string) (*hana.Client, error) {
	db, err := m.GetHANAConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetHANAConnectionWithoutDefault("hana-default")
}

func (m *Manager) GetHANAConnectionWithoutDefault(name string) (*hana.Client, error) {
	if m.HANA == nil {
		return nil, errors.New("no hana connections found")
	}

	db, ok := m.HANA[name]
	if !ok {
		return nil, errors.Errorf("hana connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetShopifyConnection(name string) (*shopify.Client, error) {
	db, err := m.GetShopifyConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetShopifyConnectionWithoutDefault("shopify-default")
}

func (m *Manager) GetShopifyConnectionWithoutDefault(name string) (*shopify.Client, error) {
	if m.Shopify == nil {
		return nil, errors.New("no shopify connections found")
	}

	db, ok := m.Shopify[name]
	if !ok {
		return nil, errors.Errorf("shopify connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetKlaviyoConnection(name string) (*klaviyo.Client, error) {
	db, err := m.GetKlaviyoConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetKlaviyoConnectionWithoutDefault("klaviyo-default")
}

func (m *Manager) GetKlaviyoConnectionWithoutDefault(name string) (*klaviyo.Client, error) {
	if m.Klaviyo == nil {
		return nil, errors.New("no klaviyo connections found")
	}

	db, ok := m.Klaviyo[name]
	if !ok {
		return nil, errors.Errorf("klaviyo connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetAdjustConnection(name string) (*adjust.Client, error) {
	db, err := m.GetAdjustConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetAdjustConnectionWithoutDefault("adjust-default")
}

func (m *Manager) GetAdjustConnectionWithoutDefault(name string) (*adjust.Client, error) {
	if m.Adjust == nil {
		return nil, errors.New("no adjust connections found")
	}

	db, ok := m.Adjust[name]
	if !ok {
		return nil, errors.Errorf("adjust connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetStripeConnection(name string) (*stripe.Client, error) {
	db, err := m.GetStripeConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetStripeConnectionWithoutDefault("stripe-default")
}

func (m *Manager) GetStripeConnectionWithoutDefault(name string) (*stripe.Client, error) {
	if m.Stripe == nil {
		return nil, errors.New("no stripe connections found")
	}

	db, ok := m.Stripe[name]
	if !ok {
		return nil, errors.Errorf("stripe connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetGorgiasConnection(name string) (*gorgias.Client, error) {
	db, err := m.GetGorgiasConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetGorgiasConnectionWithoutDefault("gorgias-default")
}

func (m *Manager) GetGorgiasConnectionWithoutDefault(name string) (*gorgias.Client, error) {
	if m.Gorgias == nil {
		return nil, errors.New("no gorgias connections found")
	}

	db, ok := m.Gorgias[name]
	if !ok {
		return nil, errors.Errorf("hana gorgias not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetFacebookAdsConnection(name string) (*facebookads.Client, error) {
	db, err := m.GetFacebookAdsConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetFacebookAdsConnectionWithoutDefault("facebookads-default")
}

func (m *Manager) GetFacebookAdsConnectionWithoutDefault(name string) (*facebookads.Client, error) {
	if m.FacebookAds == nil {
		return nil, errors.New("no facebookads connections found")
	}

	db, ok := m.FacebookAds[name]
	if !ok {
		return nil, errors.Errorf("facebookads connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetAppsflyerConnection(name string) (*appsflyer.Client, error) {
	fmt.Println("Attempting to retrieve AppsFlyer connection with name:", name)
	db, err := m.GetAppsflyerConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetAppsflyerConnectionWithoutDefault("appsflyer-default")
}

func (m *Manager) GetAppsflyerConnectionWithoutDefault(name string) (*appsflyer.Client, error) {
	if m.Appsflyer == nil {
		return nil, errors.New("no appsflyer connections found")
	}

	db, ok := m.Appsflyer[name]
	if !ok {
		return nil, errors.Errorf("appsflyer connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetKafkaConnection(name string) (*kafka.Client, error) {
	db, err := m.GetKafkaConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetKafkaConnectionWithoutDefault("kafka-default")
}

func (m *Manager) GetKafkaConnectionWithoutDefault(name string) (*kafka.Client, error) {
	if m.Kafka == nil {
		return nil, errors.New("no kafka connections found")
	}

	db, ok := m.Kafka[name]
	if !ok {
		return nil, errors.Errorf("kafka connection not found for '%s'", name)
	}

	return db, nil
}

func (m *Manager) GetHubspotConnection(name string) (*hubspot.Client, error) {
	db, err := m.GetHubspotConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetHubspotConnectionWithoutDefault("hubspot-default")
}

func (m *Manager) GetHubspotConnectionWithoutDefault(name string) (*hubspot.Client, error) {
	if m.Hubspot == nil {
		return nil, errors.New("no Hubspot connections found")
	}
	db, ok := m.Hubspot[name]
	if !ok {
		return nil, errors.Errorf("hubspot connection not found for '%s'", name)
	}
	return db, nil
}

func (m *Manager) GetAirtableConnection(name string) (*airtable.Client, error) {
	db, err := m.GetAirtableConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetAirtableConnectionWithoutDefault("airtable-default")
}

func (m *Manager) GetAirtableConnectionWithoutDefault(name string) (*airtable.Client, error) {
	if m.Airtable == nil {
		return nil, errors.New("no airtable connections found")
	}
	db, ok := m.Airtable[name]
	if !ok {
		return nil, errors.Errorf("airtable connection not found for '%s'", name)
	}
	return db, nil
}

func (m *Manager) GetGoogleSheetsConnection(name string) (*gsheets.Client, error) {
	db, err := m.GetGoogleSheetsConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetGoogleSheetsConnectionWithoutDefault("google-sheets-default")
}

func (m *Manager) GetGoogleSheetsConnectionWithoutDefault(name string) (*gsheets.Client, error) {
	if m.GoogleSheets == nil {
		return nil, errors.New("no google sheets connections found")
	}
	db, ok := m.GoogleSheets[name]
	if !ok {
		return nil, errors.Errorf("google sheets connection not found for '%s'", name)
	}
	return db, nil
}

func (m *Manager) GetZendeskConnection(name string) (*zendesk.Client, error) {
	if m.Zendesk == nil {
		return nil, errors.New("no zendesk connections found")
	}
	db, ok := m.Zendesk[name]
	if !ok {
		return nil, errors.Errorf("zendesk connection not found for '%s'", name)
	}
	return db, nil
}

func (m *Manager) GetZendeskConnectionWithoutDefault(name string) (*zendesk.Client, error) {
	if m.Zendesk == nil {
		return nil, errors.New("no zendesk connections found")
	}
	db, ok := m.Zendesk[name]
	if !ok {
		return nil, errors.Errorf("zendesk connection not found for '%s'", name)
	}
	return db, nil
}

func (m *Manager) AddBqConnectionFromConfig(connection *config.GoogleCloudPlatformConnection) error {
	m.mutex.Lock()
	if m.BigQuery == nil {
		m.BigQuery = make(map[string]*bigquery.Client)
	}
	m.mutex.Unlock()

	if len(connection.ServiceAccountFile) == 0 && len(connection.ServiceAccountJSON) == 0 {
		return errors.New("at least one of service_account_file or service_account_json must be provided")
	}

	if len(connection.ServiceAccountFile) > 0 && connection.ServiceAccountFile != "" {

		file, err := ioutil.ReadFile(connection.ServiceAccountFile)
		if err == nil {
			return errors.Errorf("Please use service_account_file Instead  of  service_account_json ")
		}
		var js json.RawMessage
		err = json.Unmarshal(file, &js)
		if err != nil {
			return errors.Errorf("not a valid JSON in service account file at '%s'", connection.ServiceAccountFile)
		}
	}

	if len(connection.ServiceAccountJSON) > 0 && connection.ServiceAccountJSON != "" {

		_, err := os.Stat(connection.ServiceAccountJSON)
		if err == nil {
			return errors.New("please use service_account_file Instead  of service_account_json to define path ")
		}

		file, err := ioutil.ReadFile(connection.ServiceAccountJSON)
		if err != nil {
			file = []byte(connection.ServiceAccountJSON)
		}

		var js json.RawMessage
		err = json.Unmarshal(file, &js)
		if err != nil {
			return errors.New("not a valid JSON in service account json")
		}
	}

	db, err := bigquery.NewDB(&bigquery.Config{
		ProjectID:           connection.ProjectID,
		CredentialsFilePath: connection.ServiceAccountFile,
		CredentialsJSON:     connection.ServiceAccountJSON,
		Credentials:         connection.GetCredentials(),
		Location:            connection.Location,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.BigQuery[connection.Name] = db

	return nil
}

func (m *Manager) AddSfConnectionFromConfig(connection *config.SnowflakeConnection) error {
	m.mutex.Lock()
	if m.Snowflake == nil {
		m.Snowflake = make(map[string]*snowflake.DB)
	}
	m.mutex.Unlock()

	db, err := snowflake.NewDB(&snowflake.Config{
		Account:   connection.Account,
		Username:  connection.Username,
		Password:  connection.Password,
		Region:    connection.Region,
		Role:      connection.Role,
		Database:  connection.Database,
		Schema:    connection.Schema,
		Warehouse: connection.Warehouse,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Snowflake[connection.Name] = db

	return nil
}

func (m *Manager) AddAthenaConnectionFromConfig(connection *config.AthenaConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.Athena == nil {
		m.Athena = make(map[string]*athena.DB)
	}

	m.Athena[connection.Name] = athena.NewDB(&athena.Config{
		Region:          connection.Region,
		OutputBucket:    connection.QueryResultsPath,
		AccessID:        connection.AccessKey,
		SecretAccessKey: connection.SecretKey,
		Database:        connection.Database,
	})

	return nil
}

func (m *Manager) AddPgConnectionFromConfig(connection *config.PostgresConnection) error {
	return m.addPgLikeConnectionFromConfig(connection, false)
}

func (m *Manager) AddRedshiftConnectionFromConfig(connection *config.PostgresConnection) error {
	return m.addPgLikeConnectionFromConfig(connection, true)
}

func (m *Manager) addPgLikeConnectionFromConfig(connection *config.PostgresConnection, redshift bool) error {
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
		client, err = postgres.NewClient(context.TODO(), postgres.RedShiftConfig{
			Username:     connection.Username,
			Password:     connection.Password,
			Host:         connection.Host,
			Port:         connection.Port,
			Database:     connection.Database,
			Schema:       connection.Schema,
			PoolMaxConns: poolMaxConns,
			SslMode:      connection.SslMode,
		})
	} else {
		client, err = postgres.NewClient(context.TODO(), postgres.Config{
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
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.MsSQL[connection.Name] = client

	return nil
}

func (m *Manager) AddDatabricksConnectionFromConfig(connection *config.DatabricksConnection) error {
	m.mutex.Lock()
	if m.Databricks == nil {
		m.Databricks = make(map[string]*databricks.DB)
	}
	m.mutex.Unlock()

	client, err := databricks.NewDB(&databricks.Config{
		Token:   connection.Token,
		Host:    connection.Host,
		Path:    connection.Path,
		Port:    connection.Port,
		Catalog: connection.Catalog,
		Schema:  connection.Schema,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Databricks[connection.Name] = client

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

	return nil
}

func (m *Manager) AddAppsflyerConnectionFromConfig(connection *config.AppsflyerConnection) error {
	m.mutex.Lock()
	if m.Appsflyer == nil {
		m.Appsflyer = make(map[string]*appsflyer.Client)
	}
	m.mutex.Unlock()

	client, err := appsflyer.NewClient(appsflyer.Config{
		ApiKey: connection.ApiKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Appsflyer[connection.Name] = client

	return nil
}

func (m *Manager) AddGoogleSheetsConnectionFromConfig(connection *config.GoogleSheetsConnection) error {
	m.mutex.Lock()
	if m.GoogleSheets == nil {
		m.GoogleSheets = make(map[string]*gsheets.Client)
	}
	m.mutex.Unlock()

	client, err := gsheets.NewClient(gsheets.Config{
		CredentialsPath: connection.CredentialsPath,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.GoogleSheets[connection.Name] = client

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
		GroupId:          connection.GroupId,
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

	return nil
}

func (m *Manager) AddHubspotConnectionFromConfig(connection *config.HubspotConnection) error {
	m.mutex.Lock()
	if m.Hubspot == nil {
		m.Hubspot = make(map[string]*hubspot.Client)
	}
	m.mutex.Unlock()

	client, err := hubspot.NewClient(hubspot.Config{
		APIKey: connection.ApiKey,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Hubspot[connection.Name] = client

	return nil
}

func (m *Manager) AddAirtableConnectionFromConfig(connection *config.AirtableConnection) error {
	m.mutex.Lock()
	if m.Airtable == nil {
		m.Airtable = make(map[string]*airtable.Client)
	}
	m.mutex.Unlock()
	client, err := airtable.NewClient(airtable.Config{
		BaseId:      connection.BaseId,
		AccessToken: connection.AccessToken,
	})
	if err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Airtable[connection.Name] = client

	return nil
}

func (m *Manager) AddZendeskConnectionFromConfig(connection *config.ZendeskConnection) error {
	m.mutex.Lock()
	if m.Zendesk == nil {
		m.Zendesk = make(map[string]*zendesk.Client)
	}
	m.mutex.Unlock()
	client, err := zendesk.NewClient(zendesk.Config{
		ApiToken:   connection.ApiToken,
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

	return nil
}

func NewManagerFromConfig(cm *config.Config) (*Manager, []error) {
	connectionManager := &Manager{}

	var wg conc.WaitGroup

	var errList []error
	var mu sync.Mutex

	for _, conn := range cm.SelectedEnvironment.Connections.AthenaConnection {
		wg.Go(func() {
			err := connectionManager.AddAthenaConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add AWS connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.GoogleCloudPlatform {
		wg.Go(func() {
			err := connectionManager.AddBqConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add BigQuery connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Snowflake {
		wg.Go(func() {
			err := connectionManager.AddSfConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add Snowflake connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Postgres {
		wg.Go(func() {
			err := connectionManager.AddPgConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add Postgres connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.RedShift {
		wg.Go(func() {
			err := connectionManager.AddRedshiftConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add RedShift connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.MsSQL {
		wg.Go(func() {
			err := connectionManager.AddMsSQLConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add MsSQL connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Databricks {
		wg.Go(func() {
			err := connectionManager.AddDatabricksConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add Databricks connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Synapse {
		wg.Go(func() {
			err := connectionManager.AddMsSQLConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add Synapse connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Mongo {
		wg.Go(func() {
			err := connectionManager.AddMongoConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add Mongo connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.MySQL {
		wg.Go(func() {
			err := connectionManager.AddMySQLConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add mysql connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Notion {
		wg.Go(func() {
			err := connectionManager.AddNotionConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add notion connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Shopify {
		wg.Go(func() {
			err := connectionManager.AddShopifyConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add shopify connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Gorgias {
		wg.Go(func() {
			err := connectionManager.AddGorgiasConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add gorgias connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Klaviyo {
		wg.Go(func() {
			err := connectionManager.AddKlaviyoConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add klaviyo connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Adjust {
		wg.Go(func() {
			err := connectionManager.AddAdjustConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add adjust connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.FacebookAds {
		wg.Go(func() {
			err := connectionManager.AddFacebookAdsConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add facebookads connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Stripe {
		wg.Go(func() {
			err := connectionManager.AddStripeConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add stripe connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Appsflyer {
		wg.Go(func() {
			err := connectionManager.AddAppsflyerConnectionFromConfig(&conn)
			if err != nil {
				mu.Lock()
				errList = append(errList, errors.Wrapf(err, "failed to add appsflyer connection '%s'", conn.Name))
				mu.Unlock()
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Kafka {
		wg.Go(func() {
			err := connectionManager.AddKafkaConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add kafka connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.GoogleSheets {
		wg.Go(func() {
			err := connectionManager.AddGoogleSheetsConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add googlesheets connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.DuckDB {
		wg.Go(func() {
			err := connectionManager.AddDuckDBConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add duckdb connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Hubspot {
		wg.Go(func() {
			err := connectionManager.AddHubspotConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add hubspot connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Airtable {
		wg.Go(func() {
			err := connectionManager.AddAirtableConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add airtable connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Zendesk {
		wg.Go(func() {
			err := connectionManager.AddZendeskConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add zendesk connection '%s'", conn.Name))
			}
		})
	}
	wg.Wait()

	return connectionManager, errList
}
