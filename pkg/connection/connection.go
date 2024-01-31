package connection

import (
	"sync"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/pkg/errors"
	"github.com/sourcegraph/conc"
)

type Manager struct {
	BigQuery  map[string]*bigquery.Client
	Snowflake map[string]*snowflake.DB

	mutex sync.Mutex
}

func (m *Manager) GetConnection(name string) (interface{}, error) {
	conn, err := m.GetBqConnectionWithoutDefault(name)
	if err == nil {
		return conn, nil
	}

	conn, err = m.GetSfConnectionWithoutDefault(name)
	if err == nil {
		return conn, nil
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
		return nil, errors.New("bigquery connection not found")
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
		return nil, errors.New("snowflake connection not found")
	}

	return db, nil
}

func (m *Manager) AddBqConnectionFromConfig(connection *config.GoogleCloudPlatformConnection) error {
	if m.BigQuery == nil {
		m.mutex.Lock()
		m.BigQuery = make(map[string]*bigquery.Client)
		m.mutex.Unlock()
	}

	db, err := bigquery.NewDB(&bigquery.Config{
		ProjectID:           connection.ProjectID,
		CredentialsFilePath: connection.ServiceAccountFile,
		CredentialsJSON:     connection.ServiceAccountJSON,
		Credentials:         connection.GetCredentials(),
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
	if m.Snowflake == nil {
		m.mutex.Lock()
		m.Snowflake = make(map[string]*snowflake.DB)
		m.mutex.Unlock()
	}

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

	wg.Wait()

	return connectionManager, nil
}
