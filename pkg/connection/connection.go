package connection

import (
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/pkg/errors"
)

type Manager struct {
	BigQuery  map[string]*bigquery.Client
	Snowflake map[string]*snowflake.DB
}

func (m *Manager) GetConnection(name string) (interface{}, error) {
	conn, err := m.GetBqConnection(name)
	if err == nil {
		return conn, nil
	}

	conn, err = m.GetSfConnection(name)
	if err == nil {
		return conn, nil
	}

	return nil, errors.New("connection not found")
}

func (m *Manager) GetBqConnection(name string) (bigquery.DB, error) {
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
		m.BigQuery = make(map[string]*bigquery.Client)
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

	m.BigQuery[connection.Name] = db

	return nil
}

func (m *Manager) AddSfConnectionFromConfig(connection *config.SnowflakeConnection) error {
	if m.Snowflake == nil {
		m.Snowflake = make(map[string]*snowflake.DB)
	}

	db, err := snowflake.NewDB(&snowflake.Config{
		Account:  connection.Account,
		Username: connection.Username,
		Password: connection.Password,
		Region:   connection.Region,
		Role:     connection.Role,
		Database: connection.Database,
		Schema:   connection.Schema,
	})
	if err != nil {
		return err
	}

	m.Snowflake[connection.Name] = db

	return nil
}

func NewManagerFromConfig(cm *config.Config) (*Manager, error) {
	connectionManager := &Manager{}
	for _, conn := range cm.SelectedEnvironment.Connections.GoogleCloudPlatform {
		conn := conn
		err := connectionManager.AddBqConnectionFromConfig(&conn)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to add BigQuery connection '%s'", conn.Name)
		}
	}
	for _, conn := range cm.SelectedEnvironment.Connections.Snowflake {
		conn := conn
		err := connectionManager.AddSfConnectionFromConfig(&conn)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to add Snowflake connection '%s'", conn.Name)
		}
	}

	return connectionManager, nil
}
