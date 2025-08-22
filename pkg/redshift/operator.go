package redshift

import (
	"context"
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
)

type TableSensor = ansisql.TableSensor

type TableSensorConnectionGetter struct {
	conn config.ConnectionAndDetailsGetter
}

func (w *TableSensorConnectionGetter) GetConnection(name string) any {
	client, err := getTableSensorClient(context.Background(), name, w.conn)
	if err != nil {
		panic("Redshift table sensor requires ConnectionAndDetailsGetter interface")
	}
	return client
}

func NewTableSensor(conn config.ConnectionGetter, sensorMode string, extractor query.QueryExtractor) *TableSensor {
	connWithDetails, ok := conn.(config.ConnectionAndDetailsGetter)
	if !ok {
		panic("Redshift table sensor requires ConnectionAndDetailsGetter interface")
	}
	wrapper := &TableSensorConnectionGetter{conn: connWithDetails}
	return ansisql.NewTableSensor(wrapper, sensorMode, extractor)
}

// getTableSensorClient creates a Redshift-specific client for table sensor operations
// This is a wrapper that gets the client from the postgres package
func getTableSensorClient(ctx context.Context, connectionName string, conn config.ConnectionAndDetailsGetter) (ansisql.TableExistsChecker, error) {
	// Get the connection details
	connection := conn.GetConnection(connectionName)
	if connection == nil {
		return nil, fmt.Errorf("connection '%s' does not exist", connectionName)
	}

	// Get the connection details from the manager
	connectionDetails := conn.GetConnectionDetails(connectionName)
	if connectionDetails == nil {
		return nil, fmt.Errorf("connection details for '%s' not found", connectionName)
	}

	// Cast to RedshiftConnection to get the config
	redshiftConn, ok := connectionDetails.(*config.RedshiftConnection)
	if !ok {
		return nil, fmt.Errorf("connection '%s' is not a Redshift connection", connectionName)
	}

	// Create redshift.Config
	redshiftConfig := &Config{
		Username: redshiftConn.Username,
		Password: redshiftConn.Password,
		Host:     redshiftConn.Host,
		Port:     redshiftConn.Port,
		Database: redshiftConn.Database,
		Schema:   redshiftConn.Schema,
		SslMode:  redshiftConn.SslMode,
	}

	// Convert to postgres.RedShiftConfig and create client
	postgresConfig := redshiftConfig.ToPostgresConfig()
	client, err := postgres.NewClient(ctx, postgresConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redshift client: %w", err)
	}

	return client, nil
}
