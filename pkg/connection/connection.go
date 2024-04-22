package connection

import (
	"context"
	"sync"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/mongo"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/pkg/errors"
	"github.com/sourcegraph/conc"
)

type Manager struct {
	BigQuery  map[string]*bigquery.Client
	Snowflake map[string]*snowflake.DB
	Postgres  map[string]*postgres.Client
	MsSQL     map[string]*mssql.DB
	Mongo     map[string]*mongo.DB
	Mysql     map[string]*mysql.Client

	mutex sync.Mutex
}

func (m *Manager) GetConnection(name string) (interface{}, error) {
	connBigQuery, err := m.GetBqConnectionWithoutDefault(name)
	if err == nil {
		return connBigQuery, nil
	}

	connSnowflake, err := m.GetSfConnectionWithoutDefault(name)
	if err == nil {
		return connSnowflake, nil
	}

	connPostgres, err := m.GetPgConnectionWithoutDefault(name)
	if err == nil {
		return connPostgres, nil
	}

	connMSSql, err := m.GetMsConnectionWithoutDefault(name)
	if err == nil {
		return connMSSql, nil
	}

	connMongo, err := m.GetMongoConnectionWithoutDefault(name)
	if err == nil {
		return connMongo, nil
	}

	connMysql, err := m.GetMySQLConnectionWithoutDefault(name)
	if err == nil {
		return connMysql, nil
	}

	return nil, errors.New("connection not found")
}

func (m *Manager) GetBqConnection(name string) (bigquery.DB, error) {
	db, err := m.GetBqConnectionWithoutDefault(name)
	if err == nil {
		return db, nil
	}

	return m.GetBqConnectionWithoutDefault("gcp-default")
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
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Mysql[connection.Name] = client

	return nil
}

func NewManagerFromConfig(cm *config.Config) (*Manager, error) {
	connectionManager := &Manager{}

	var wg conc.WaitGroup
	for _, conn := range cm.SelectedEnvironment.Connections.GoogleCloudPlatform {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddBqConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add BigQuery connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Snowflake {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddSfConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Snowflake connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Postgres {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddPgConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Postgres connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.RedShift {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddRedshiftConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add RedShift connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.MsSQL {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddMsSQLConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add MsSQL connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Synapse {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddMsSQLConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Synapse connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.Mongo {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddMongoConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add Mongo connection '%s'", conn.Name))
			}
		})
	}

	for _, conn := range cm.SelectedEnvironment.Connections.MySQL {
		conn := conn
		wg.Go(func() {
			err := connectionManager.AddMySQLConnectionFromConfig(&conn)
			if err != nil {
				panic(errors.Wrapf(err, "failed to add mysql connection '%s'", conn.Name))
			}
		})
	}

	wg.Wait()

	return connectionManager, nil
}
