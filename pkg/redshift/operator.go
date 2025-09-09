package redshift

import (
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
)

type TableSensor = ansisql.TableSensor

func NewTableSensor(conn config.ConnectionGetter, sensorMode string, extractor query.QueryExtractor) *TableSensor {
	wrapper := &TableSensorConnectionWrapper{conn: conn}
	return ansisql.NewTableSensor(wrapper, sensorMode, extractor)
}

type TableSensorConnectionWrapper struct {
	conn config.ConnectionGetter
}

func (w *TableSensorConnectionWrapper) GetConnection(name string) any {
	conn := w.conn.GetConnection(name)
	if conn == nil {
		return nil
	}

	postgresClient, ok := conn.(*postgres.Client)
	if !ok {
		return conn
	}

	return &TableSensorClient{PostgresClient: postgresClient}
}
