package connection

import (
	"context"
	"github.com/bruin-data/bruin/pkg/athena"
	"sync"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/databricks"
	"github.com/bruin-data/bruin/pkg/gorgias"
	"github.com/bruin-data/bruin/pkg/hana"
	"github.com/bruin-data/bruin/pkg/mongo"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/notion"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/shopify"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/pkg/errors"
	"github.com/sourcegraph/conc"
	"golang.org/x/exp/maps"
)

type Manager struct {
	BigQuery   map[string]*bigquery.Client
	Snowflake  map[string]*snowflake.DB
	Postgres   map[string]*postgres.Client
	MsSQL      map[string]*mssql.DB
	Databricks map[string]*databricks.DB
	Mongo      map[string]*mongo.DB
	Mysql      map[string]*mysql.Client
	Notion     map[string]*notion.Client
	HANA       map[string]*hana.Client
	Shopify    map[string]*shopify.Client
	Gorgias    map[string]*gorgias.Client
	Aws        map[string]*athena.DB
	mutex      sync.Mutex
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

	return nil, errors.Errorf("connection '%s' not found, available connection names are: %v", name, availableConnectionNames)
}

func (m *Manager) GetAwsConnection(name string) (athena.Client, error) {
	db, err := m.GetAwsConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetAwsConnectionWithoutDefault("aws-default")
}

func (m *Manager) GetAwsConnectionWithoutDefault(name string) (athena.Client, error) {
	if m.Aws == nil {
		return nil, errors.New("no AWS connections found")
	}

	db, ok := m.Aws[name]
	if !ok {
		return nil, errors.Errorf("AWS connection not found for '%s'", name)
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
		return nil, errors.New("no postgres connections found")
	}

	db, ok := m.Postgres[name]
	if !ok {
		return nil, errors.Errorf("postgres connection not found for '%s'", name)
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

func (m *Manager) AddBqConnectionFromConfig(connection *config.GoogleCloudPlatformConnection) error {
	m.mutex.Lock()
	if m.BigQuery == nil {
		m.BigQuery = make(map[string]*bigquery.Client)
	}
	m.mutex.Unlock()

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

func (m *Manager) AddAwsConnectionFromConfig(connection *config.AwsConnection) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.Aws == nil {
		m.Aws = make(map[string]*athena.DB)
	}

	m.Aws[connection.Name] = athena.NewDB(&athena.Config{
		Region:          connection.Region,
		OutputBucket:    connection.QueryResultsPath,
		AccessID:        connection.AccessKey,
		SecretAccessKey: connection.SecretKey,
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
		Token: connection.Token,
		Host:  connection.Host,
		Path:  connection.Path,
		Port:  connection.Port,
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

func NewManagerFromConfig(cm *config.Config) (*Manager, error) {
	connectionManager := &Manager{}

	var wg conc.WaitGroup
	for _, conn := range cm.SelectedEnvironment.Connections.AwsConnection {
		wg.Go(func() {
			err := connectionManager.AddAwsConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add AWS connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.GoogleCloudPlatform {
		wg.Go(func() {
			err := connectionManager.AddBqConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add BigQuery connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Snowflake {
		wg.Go(func() {
			err := connectionManager.AddSfConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Snowflake connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Postgres {
		wg.Go(func() {
			err := connectionManager.AddPgConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Postgres connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.RedShift {
		wg.Go(func() {
			err := connectionManager.AddRedshiftConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add RedShift connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.MsSQL {
		wg.Go(func() {
			err := connectionManager.AddMsSQLConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add MsSQL connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Databricks {
		wg.Go(func() {
			err := connectionManager.AddDatabricksConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Databricks connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Synapse {
		wg.Go(func() {
			err := connectionManager.AddMsSQLConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Synapse connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Mongo {
		wg.Go(func() {
			err := connectionManager.AddMongoConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Mongo connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.MySQL {
		wg.Go(func() {
			err := connectionManager.AddMySQLConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add mysql connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Notion {
		wg.Go(func() {
			err := connectionManager.AddNotionConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add notion connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Shopify {
		wg.Go(func() {
			err := connectionManager.AddShopifyConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add shopify connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Gorgias {
		wg.Go(func() {
			err := connectionManager.AddGorgiasConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add gorgias connection '%s'", conn.Name))
			}
		})
	}

	wg.Wait()

	return connectionManager, nil
}
