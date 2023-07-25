package connection

import (
	"errors"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
)

type Manager struct {
	BigQuery map[string]*bigquery.Client
}

func (m *Manager) GetConnection(name string) (interface{}, error) {
	return m.GetBqConnection(name)
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

func NewManagerFromConfig(cm *config.Config) (*Manager, error) {
	connectionManager := &Manager{}
	for _, conn := range cm.SelectedEnvironment.Connections.GoogleCloudPlatform {
		conn := conn
		err := connectionManager.AddBqConnectionFromConfig(&conn)
		if err != nil {
			return nil, err
		}
	}

	return connectionManager, nil
}
